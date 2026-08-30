// Phase 5 -- does throughput actually scale with shard count?
//
// The second half of the Phase B gate. Sharding has been in the pipeline since
// Phase A, on the argument that it was cheaper to build in than to retrofit,
// and it has never been measured. This phase is the measurement.
//
// THE ONE DESIGN DECISION THAT DETERMINES WHETHER THIS PHASE MEANS ANYTHING
//
// Shards are worth it "when the sink, not the queue, is the bottleneck" --
// options.hpp says so at the `consumers` field, and the roadmap says so. Run
// this against the free-ish sink the other phases use and a single consumer
// already outruns everything the producers can offer: the shard-count column
// comes out flat, the phase reports a true fact, and the fact answers a
// question nobody asked. Worse, it would read as "sharding does not work".
//
// So the sink here has a deliberate fixed cost per record, which makes the
// CONSUMER the scarce resource. That is the only regime in which "throughput
// scales with shard count" is a claim rather than a category error, and it is
// the regime the option exists for. The cost is stated in the table so the
// number is never read as a general throughput figure -- it is not one, and the
// figures in phase 2 remain the ones to quote.
//
// Two consequences worth stating rather than discovering later:
//
//   - Scaling here BUYS cores. S shards means S consumer threads, so this is a
//     throughput-per-machine claim, not a throughput-per-core one. That is what
//     the option offers and it should not be reported as though it were free.
//   - The sweep is capped at what the machine can hold. Saturating producers
//     and spinning consumers each cost a core; past that the numbers describe
//     the OS scheduler, which is the same reason phase 2 caps its own sweep.

#include "harness.hpp"
#include "phases.hpp"

#include <tributary/pipeline.hpp>

#include <algorithm>
#include <cstdio>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace tributary::bench {
namespace {

constexpr std::uint32_t kShardCounts[] = {1, 2, 4};
constexpr std::uint32_t kProducers = 8;  // divisible by every shard count above
constexpr int kReps = 5;
constexpr std::int64_t kRunNs = 300'000'000;  // 0.3 s of saturating load per run
constexpr std::uint64_t kClockEvery = 256;    // records between clock reads

// Cost per record, in dependent multiply steps. Picked so one consumer's
// ceiling sits well below what eight saturating producers can offer, and well
// above the per-record cost of the queue itself -- if the queue dominated, the
// arms would differ by scheduler noise and nothing else.
constexpr std::uint32_t kSinkSpins = 24;

// A sink that costs a fixed, deliberate amount per record. See the header.
//
// A serial dependent chain, not a parallelisable one: out-of-order execution
// would otherwise hide most of the cost behind the loop, and the "cost" would
// vary with how much of the batch the core could keep in flight.
class costly_sink {
public:
    std::size_t write(std::span<const record> b) noexcept {
        std::uint64_t h = checksum_;
        for (const auto& r : b) {
            h ^= r.seq;
            for (std::uint32_t i = 0; i < kSinkSpins; ++i) h = (h ^ 0x9e3779b97f4a7c15ULL) * 0x100000001b3ULL;
        }
        checksum_ = h;
        records_ += b.size();
        return b.size();
    }

    [[nodiscard]] std::uint64_t records() const noexcept { return records_; }
    [[nodiscard]] std::uint64_t checksum() const noexcept { return checksum_; }

private:
    // Carried across calls and read at the end, so the chain cannot be dropped.
    std::uint64_t checksum_ = 0xcbf29ce484222325ULL;
    std::uint64_t records_ = 0;
};

struct run_result {
    double sustained_mps = 0.0;
    double offered_mps = 0.0;
    double accepted_pct = 0.0;
    double worst_share = 0.0;  // largest fraction of delivery any one shard did
};

run_result one_run(std::uint32_t shards) {
    options opt;
    opt.max_producers = kProducers;
    opt.consumers = shards;
    opt.scan = scan_policy::bitmap;
    // drop_newest: the consumer is deliberately the bottleneck here, so a spin
    // budget would just park every producer in a retry loop and the run would
    // measure the spin budget instead of the consumers' combined ceiling.
    opt.full = full_policy::drop_newest;

    pipeline<record> pipe(opt);

    std::vector<costly_sink> sinks(shards);
    std::vector<costly_sink*> ptrs;
    ptrs.reserve(shards);
    for (auto& s : sinks) ptrs.push_back(&s);
    pipe.start(std::span<costly_sink* const>(ptrs));

    std::vector<std::uint64_t> attempts(kProducers, 0);
    std::vector<std::int64_t> elapsed(kProducers, 0);
    std::vector<std::thread> ts;
    ts.reserve(kProducers);
    for (std::uint32_t p = 0; p < kProducers; ++p) {
        ts.emplace_back([&pipe, &attempts, &elapsed, p] {
            auto h = pipe.register_producer();
            if (!h) return;
            record r{};
            r.producer_id = h.id();
            std::uint64_t n = 0;
            const std::int64_t t0 = now_ns();
            std::int64_t dt = 0;
            do {
                for (std::uint64_t k = 0; k < kClockEvery; ++k) {
                    r.seq = static_cast<std::uint32_t>(++n);
                    h.push(r);
                }
                dt = now_ns() - t0;
            } while (dt < kRunNs);
            attempts[p] = n;
            elapsed[p] = dt;
        });
    }
    for (auto& t : ts) t.join();
    pipe.stop(stop_mode::drain);

    const auto st = pipe.snapshot();
    std::uint64_t offered = 0;
    std::int64_t wall = 0;
    for (std::uint32_t p = 0; p < kProducers; ++p) {
        offered += attempts[p];
        wall = std::max(wall, elapsed[p]);
    }
    const double secs = static_cast<double>(wall) / 1e9;

    std::uint64_t delivered = 0;
    std::uint64_t busiest = 0;
    for (const auto& s : sinks) {
        delivered += s.records();
        busiest = std::max(busiest, s.records());
    }

    run_result r;
    r.sustained_mps = static_cast<double>(st.pushed) / secs / 1e6;
    r.offered_mps = static_cast<double>(offered) / secs / 1e6;
    r.accepted_pct =
        offered > 0 ? static_cast<double>(st.pushed) / static_cast<double>(offered) * 100.0 : 0.0;
    r.worst_share =
        delivered > 0 ? static_cast<double>(busiest) / static_cast<double>(delivered) : 0.0;

    const std::string at = " at " + std::to_string(shards) + " shard(s)";
    // A throughput number from a run that lost track of its records is not a
    // throughput number.
    check(st.pushed + st.dropped == offered, "pushed + dropped == offered" + at);
    check(delivered == st.pushed, "the shards between them delivered every accepted record" + at);
    return r;
}

// Median per statistic, warm-up discarded. Not the statistics of one "median
// run" -- picking a run lets one noisy column drag the others with it.
run_result median_run(std::uint32_t shards) {
    (void)one_run(shards);  // warm-up: first touch, page faults, thread start

    std::vector<double> sustained;
    std::vector<double> offered;
    std::vector<double> accepted;
    std::vector<double> share;
    sustained.reserve(kReps);
    offered.reserve(kReps);
    accepted.reserve(kReps);
    share.reserve(kReps);

    for (int i = 0; i < kReps; ++i) {
        const run_result r = one_run(shards);
        sustained.push_back(r.sustained_mps);
        offered.push_back(r.offered_mps);
        accepted.push_back(r.accepted_pct);
        share.push_back(r.worst_share);
    }

    run_result m;
    m.sustained_mps = median_of(sustained);
    m.offered_mps = median_of(offered);
    m.accepted_pct = median_of(accepted);
    m.worst_share = median_of(share);
    return m;
}

}  // namespace

void bench_sharding() {
    constexpr std::size_t kLevels = std::size(kShardCounts);
    constexpr std::uint32_t kWidest = kShardCounts[kLevels - 1];

    std::printf("\n=== phase 5: sharded throughput ===\n");
    std::printf("%u saturating producers, sink cost %u dependent multiplies/record;"
                " %d reps of %.1fs, median per statistic\n\n",
                static_cast<unsigned>(kProducers), static_cast<unsigned>(kSinkSpins), kReps,
                static_cast<double>(kRunNs) / 1e9);

    // Producers and consumers each hold a core flat out. Measuring past that
    // reports the scheduler, not the library -- the same cap phase 2 applies.
    const unsigned cores = std::thread::hardware_concurrency();
    if (cores != 0 && cores < kProducers + kWidest + 1) {
        note("skipped: this machine has " + std::to_string(cores) + " cores and the widest arm " +
             "needs " + std::to_string(kProducers + kWidest + 1) +
             ". Past that the numbers describe the OS scheduler.");
        phase_verdict("phase 5: sharded throughput");
        return;
    }

    run_result r[kLevels];
    for (std::size_t i = 0; i < kLevels; ++i) r[i] = median_run(kShardCounts[i]);

    std::printf("shards  producers/shard | sustained M/s   vs 1 shard | offered M/s  accepted  busiest shard\n");
    std::printf("------------------------+---------------------------+--------------------------------------\n");
    for (std::size_t i = 0; i < kLevels; ++i) {
        const double vs1 = r[0].sustained_mps > 0.0 ? r[i].sustained_mps / r[0].sustained_mps : 0.0;
        std::printf("%-6u  %-15u |     %9.2f      %5.2fx |  %10.1f   %6.2f%%      %6.1f%%\n",
                    static_cast<unsigned>(kShardCounts[i]),
                    static_cast<unsigned>(kProducers / kShardCounts[i]), r[i].sustained_mps, vs1,
                    r[i].offered_mps, r[i].accepted_pct, r[i].worst_share * 100.0);
    }
    std::printf("\n");

    // --- the claim ----------------------------------------------------------

    // Each doubling of shards must buy at least 1.5x. Perfect scaling would be
    // 2x and will not happen -- the rings, the bitmap and the producers are
    // shared work that does not halve -- but a sink-bound system that gains
    // less than half of a doubling is not sharding in any useful sense.
    for (std::size_t i = 1; i < kLevels; ++i) {
        const double ratio =
            r[i - 1].sustained_mps > 0.0 ? r[i].sustained_mps / r[i - 1].sustained_mps : 0.0;
        check(ratio >= 1.5, std::to_string(kShardCounts[i - 1]) + " -> " +
                                std::to_string(kShardCounts[i]) + " shards scaled throughput (" +
                                std::to_string(ratio).substr(0, 4) + "x, want >= 1.50x)");
    }

    // Scaling measured on an unbalanced run would be measuring the imbalance.
    // Registration round-robins the starting shard, so with kProducers a
    // multiple of every shard count the split should be exact; anything far off
    // means the throughput ratios above are describing something else.
    for (std::size_t i = 0; i < kLevels; ++i) {
        const double fair = 1.0 / static_cast<double>(kShardCounts[i]);
        check_le(r[i].worst_share, fair * 1.35,
                 "work was spread across the shards at " + std::to_string(kShardCounts[i]) +
                     " shard(s)");
    }

    note("sustained M/s here is deliberately sink-bound and is NOT a throughput");
    note("figure for the library -- phase 2 has those. This phase varies exactly");
    note("one thing, shard count, against a sink slow enough for shards to matter.");
    note("Scaling buys cores: S shards means S consumer threads, so this is a");
    note("per-machine claim, not a per-core one.");

    phase_verdict("phase 5: sharded throughput");
}

}  // namespace tributary::bench
