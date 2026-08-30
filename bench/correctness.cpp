// Phase 1 -- correctness under stress.
//
// This phase reports no timings. It exists so that the numbers the other phases
// report come from a pipeline that was actually delivering records correctly
// while it was being measured. A fast pipeline that loses or reorders records is
// just a fast way to be wrong, and every failure mode here is silent: nothing
// crashes, nothing warns, the throughput looks fine.
//
// Each invariant below is one that breaking costs you data with no other signal.

#include "harness.hpp"
#include "phases.hpp"

#include <tributary/pipeline.hpp>

#include <atomic>
#include <cstdio>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace tributary::bench {
namespace {

constexpr std::uint32_t kProducers = 16;
constexpr std::uint32_t kPerProducer = 200'000;

// Producer churn: slots recycled through many waves.
constexpr std::uint32_t kWaves = 40;
constexpr std::uint32_t kWaveProducers = 8;
constexpr std::uint32_t kChurnSlots = 16;
constexpr std::uint32_t kChurnPerProducer = 2'000;

// This phase's sink, and only this phase's.
//
// Ordering is validated from inside the consumer thread, because that is the
// only place records are seen in the order the pipeline actually delivered them.
// The library deliberately has no per-record hook for this: a std::function on
// that path would be a cost every production user pays for a benchmark's sake.
class ordering_sink {
public:
    explicit ordering_sink(std::uint32_t max_producers)
        : last_(max_producers, 0), seen_(max_producers, 0) {}

    std::size_t write(std::span<const record> b) noexcept {
        for (const auto& r : b) {
            if (r.producer_id < last_.size()) {
                // A gap is legal -- that is a counted drop. Going backwards or
                // repeating a sequence number is not.
                if (seen_[r.producer_id] != 0 && r.seq <= last_[r.producer_id]) {
                    if (violations_ == 0) {
                        // Keep the first one. "N violations" says something
                        // broke; the offending triple says what.
                        bad_id_ = r.producer_id;
                        bad_seq_ = r.seq;
                        bad_prev_ = last_[r.producer_id];
                    }
                    ++violations_;
                }
                seen_[r.producer_id] = 1;
                last_[r.producer_id] = r.seq;
            } else {
                ++violations_;
            }
        }
        records_ += b.size();
        return b.size();
    }

    [[nodiscard]] std::uint64_t records() const noexcept { return records_; }
    [[nodiscard]] std::uint64_t violations() const noexcept { return violations_; }

    [[nodiscard]] std::string first_violation() const {
        if (violations_ == 0) return "none";
        return "slot " + std::to_string(bad_id_) + " went " + std::to_string(bad_prev_) + " -> " +
               std::to_string(bad_seq_);
    }

private:
    std::vector<std::uint64_t> last_;
    std::vector<std::uint8_t> seen_;  // not vector<bool>: no reason for a proxy here
    std::uint64_t records_ = 0;
    std::uint64_t violations_ = 0;
    std::uint32_t bad_id_ = 0;
    std::uint64_t bad_seq_ = 0;
    std::uint64_t bad_prev_ = 0;
};

void under_stress(scan_policy scan) {
    const char* name = (scan == scan_policy::bitmap) ? "bitmap" : "full-scan";
    const std::string tag = std::string(name) + ": ";

    options opt;
    opt.max_producers = 32;
    opt.scan = scan;
    // Spin first: this phase is about ordering and accounting, and a run that
    // drops 90% of its records exercises far less of the delivery path.
    opt.full = full_policy::spin_then_drop;

    pipeline<record> pipe(opt);
    ordering_sink sink(opt.max_producers);
    pipe.start(sink);

    // Every producer holds its slot until the last has registered. Without it
    // the early finishers' slots are recycled into the late starters, and a
    // recycled id restarts its sequence -- which this phase's whole purpose is
    // to report as a violation. See start_gate. (The churn phase below recycles
    // slots deliberately, and carries a generation tag for exactly that reason.)
    start_gate gate(kProducers);

    std::vector<std::thread> ts;
    ts.reserve(kProducers);
    for (std::uint32_t p = 0; p < kProducers; ++p) {
        ts.emplace_back([&pipe, &gate] {
            auto h = pipe.register_producer();
            gate.arrive_and_wait();
            if (!h) return;
            record r{};
            r.producer_id = h.id();
            for (std::uint32_t s = 1; s <= kPerProducer; ++s) {
                r.seq = s;
                h.push(r);
            }
        });
    }
    for (auto& t : ts) t.join();
    pipe.stop(stop_mode::drain);

    const auto st = pipe.snapshot();
    const std::uint64_t offered = std::uint64_t{kProducers} * kPerProducer;

    std::printf("  %-10s pushed=%llu dropped=%llu delivered=%llu probes/pass=%.2f writes=%llu\n",
                name, static_cast<unsigned long long>(st.pushed),
                static_cast<unsigned long long>(st.dropped),
                static_cast<unsigned long long>(sink.records()), st.probes_per_pass(),
                static_cast<unsigned long long>(st.bitmap_writes));

    check(sink.violations() == 0, tag + "per-producer order preserved");
    check(st.pushed + st.dropped == offered, tag + "pushed + dropped == offered, exactly");
    check(sink.records() == st.pushed, tag + "a drain stop lost nothing");
    check(st.consumed == st.pushed, tag + "the consumer's count agrees with the sink");
    check(pipe.drain_completed(), tag + "the drain finished inside its deadline");
    check(st.high_water <= opt.ring_capacity, tag + "ring depth never exceeded capacity");
    check(st.registration_failures == 0, tag + "every producer got a slot");
}

// Slots recycled through many waves.
//
// The assertion that matters is that every REGISTRATION SUCCEEDS. A churn test
// that only checks that registered producers' records survive cannot see the
// failure this guards: a consumer that never goes idle never publishes retired
// slots free, so new producers silently get nothing at all, and every such
// failure is a producer thread that produces no data and reports no error.
void churn() {
    options opt;
    opt.max_producers = kChurnSlots;
    opt.full = full_policy::spin_then_drop;

    pipeline<record> pipe(opt);
    ordering_sink sink(opt.max_producers);
    pipe.start(sink);

    // Sequence numbers are tagged with a registration counter, not with the
    // wave, and the difference is the whole subtlety of this test.
    //
    // producer_id is the SLOT index, and per-producer ordering is a guarantee
    // about one handle's lifetime -- not about a slot across successive
    // occupants. An untagged sequence restarts at 1 for the next occupant and
    // looks exactly like a reordering that never happened.
    //
    // Tagging by wave is not enough either: a slot is recycled *within* a wave
    // whenever a fast producer finishes and retires while a slower sibling is
    // still starting up, so two producers in the same wave share a slot and the
    // same wave tag. The tag has to be taken at the moment registration
    // succeeds. Successive occupants of a slot then always carry increasing
    // tags, because the later one could only claim the slot after the earlier
    // one retired.
    std::atomic<std::uint32_t> next_tag{0};
    std::uint64_t registered = 0;
    std::uint64_t expected_pushes = 0;
    for (std::uint32_t w = 0; w < kWaves; ++w) {
        std::vector<std::thread> ts;
        std::vector<std::uint64_t> got(kWaveProducers, 0);
        ts.reserve(kWaveProducers);
        for (std::uint32_t p = 0; p < kWaveProducers; ++p) {
            ts.emplace_back([&pipe, &got, &next_tag, p] {
                auto h = pipe.register_producer();
                if (!h) return;
                const std::uint32_t tag = next_tag.fetch_add(1, std::memory_order_relaxed);
                got[p] = 1;
                record r{};
                r.producer_id = h.id();
                for (std::uint32_t s = 1; s <= kChurnPerProducer; ++s) {
                    r.seq = tag * kChurnPerProducer + s;
                    h.push(r);
                }
                // Handle retires here, at scope exit, while the consumer may
                // still be mid-batch inside this ring.
            });
        }
        for (auto& t : ts) t.join();
        for (const auto g : got) registered += g;
        expected_pushes += kWaveProducers * std::uint64_t{kChurnPerProducer};
    }
    pipe.stop(stop_mode::drain);

    const auto st = pipe.snapshot();
    const std::uint64_t attempts = std::uint64_t{kWaves} * kWaveProducers;

    std::printf("  churn      %llu/%llu registrations, pushed=%llu dropped=%llu delivered=%llu\n",
                static_cast<unsigned long long>(registered),
                static_cast<unsigned long long>(attempts),
                static_cast<unsigned long long>(st.pushed),
                static_cast<unsigned long long>(st.dropped),
                static_cast<unsigned long long>(sink.records()));

    check(registered == attempts,
          "all " + std::to_string(attempts) + " registrations succeeded across " +
              std::to_string(kWaves) + " waves through " + std::to_string(kChurnSlots) + " slots");
    check(st.registration_failures == 0, "churn: no registration was refused");
    check(st.pushed + st.dropped == expected_pushes, "churn: pushed + dropped == offered");
    check(sink.records() == st.pushed, "churn: a retired producer's records still arrived");
    check(sink.violations() == 0,
          "churn: order held across slot reuse (" + std::to_string(sink.violations()) +
              " violations, first: " + sink.first_violation() + ")");
}

}  // namespace

void bench_correctness() {
    std::printf("\n=== phase 1: correctness under stress ===\n");
    std::printf("%u producers x %u records, both scan policies; then %u waves of %u through %u slots\n\n",
                static_cast<unsigned>(kProducers), static_cast<unsigned>(kPerProducer),
                static_cast<unsigned>(kWaves), static_cast<unsigned>(kWaveProducers),
                static_cast<unsigned>(kChurnSlots));

    under_stress(scan_policy::bitmap);
    under_stress(scan_policy::full_scan);
    churn();
    std::printf("\n");

    phase_verdict("phase 1: correctness under stress");
}

}  // namespace tributary::bench
