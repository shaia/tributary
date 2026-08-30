// Phase 2 -- what a push costs, and what happens when the consumer is outrun.
//
// These are two different questions and they need two different runs. Measuring
// them together is the trap this file exists to avoid:
//
//   push cost   measured on bursts that FIT, paced so the consumer keeps up.
//               A saturating loop cannot measure this. Once the rings are full
//               a failed try_push is a load and a compare -- several times
//               cheaper than a real push -- and at 16 producers against one
//               consumer the great majority of attempts fail. The column would
//               report the cost of NOT pushing, and would look impressively
//               fast doing it.
//   overload    measured saturating, which is the point: offered vs accepted vs
//               sustained is a statement about the drop policy, not about a push.
//
// The column that carries the design claim is `ns/push (no-signal)`: it should
// be essentially FLAT as producers are added. A shared tail would show the
// opposite curve, push cost climbing with producer count as the contended line
// migrates between cores. A regression here appears as slope, not as a worse
// absolute number, so compare the shape of the column.
//
// The two arms, and the honest caveat on the first:
//
//   no-signal  a bare fixed_channel per producer with one thread draining them.
//              The RING ALONE: no consumer scan, no staging buffer, no sink, and
//              no wakeup handshake. It is not "the pipeline minus a flag" --
//              signalling cannot be switched off, because a pipeline that does
//              not signal is not a pipeline, it is a queue that loses wakeups.
//   +bitmap    the real push path, ring plus handshake.
//
// The delta is what a correct wakeup costs. It is not free, and reporting it as
// though it were would be the more flattering lie.

#include "harness.hpp"
#include "phases.hpp"

#include <tributary/fixed_channel.hpp>
#include <tributary/pipeline.hpp>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <memory>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace tributary::bench {
namespace {

constexpr std::uint32_t kProducerCounts[] = {1, 4, 16, 32};
constexpr int kReps = 3;  // median of 3, warm-up discarded

// --- push cost --------------------------------------------------------------

constexpr std::size_t kBurst = 256;
constexpr std::int64_t kCostRunNs = 150'000'000;

// The TOTAL offered rate is held constant across the sweep, not the per-producer
// rate. Pacing per producer would offer 32x more work at the widest point than
// at the narrowest, saturate the rings there, and quietly turn this back into a
// measurement of the failed-push path -- the exact error this phase is built to
// avoid. Constant total means the consumer faces the same load at 1 producer as
// at 32, and every burst finds room.
constexpr double kTargetTotalPerSec = 64e6;

std::int64_t gap_for(std::uint32_t producers) noexcept {
    return static_cast<std::int64_t>(static_cast<double>(kBurst) *
                                     static_cast<double>(producers) / kTargetTotalPerSec * 1e9);
}

// The untimed gap between bursts. Yields rather than spinning on PAUSE, which
// is the opposite of what the pacer does elsewhere and deliberately so: at 32
// producers on 32 cores, 32 threads spinning flat out starve the single
// consumer, the rings fill, and "push cost" collapses back into the failed-push
// path -- the same error this phase exists to avoid, one level up. Precision
// does not matter here because the gap is explicitly not measured.
void idle_until(std::int64_t t_ns) noexcept {
    while (now_ns() < t_ns) std::this_thread::yield();
}
// Below this the ring was full during the measurement and the number is the
// failed-push path rather than the push path. Reported as a failure, because a
// silently invalid measurement is worse than a missing one.
constexpr double kMinSuccess = 0.99;

// --- overload ---------------------------------------------------------------

constexpr std::int64_t kSatRunNs = 200'000'000;
constexpr std::uint64_t kClockEvery = 1024;  // a clock read is ~25 ns vs a ~20 ns push
constexpr std::size_t kDrainBatch = 512;

// This phase's sink. Touches the batch so the compiler cannot elide producing
// it, but samples rather than hashing every byte -- a serial multiply chain over
// a full batch would make the SINK the bottleneck and every row would be
// reporting the checksum instead of the queue.
struct drain_sink {
    std::size_t write(std::span<const record> b) noexcept {
        const std::size_t stride = b.size() < 32 ? 1 : b.size() / 32;
        for (std::size_t i = 0; i < b.size(); i += stride)
            checksum = (checksum ^ b[i].seq) * 0x100000001b3ULL;
        records += b.size();
        return b.size();
    }
    std::uint64_t checksum = 0xcbf29ce484222325ULL;
    std::uint64_t records = 0;
};

struct cost_sample {
    std::uint64_t attempts = 0;
    std::uint64_t ok = 0;
    std::int64_t busy = 0;  // time inside the bursts only, never inside the gaps
};

// Timed bursts with untimed gaps. Timing a burst rather than each push keeps the
// ~25 ns clock read off a ~20 ns operation.
template <class Push>
cost_sample run_bursts(std::uint32_t id, std::int64_t gap_ns, Push push) {
    cost_sample s;
    record r{};
    r.producer_id = id;
    std::uint32_t seq = 0;
    const std::int64_t t_end = now_ns() + kCostRunNs;
    while (now_ns() < t_end) {
        const std::int64_t b0 = now_ns();
        for (std::size_t k = 0; k < kBurst; ++k) {
            r.seq = ++seq;
            if (push(r)) ++s.ok;
        }
        s.busy += now_ns() - b0;
        s.attempts += kBurst;
        // Untimed: lets the consumer drain so the next burst also fits.
        idle_until(b0 + gap_ns);
    }
    return s;
}

struct cost_result {
    double ns_per_push = 0.0;
    double success = 0.0;
};

cost_result summarise(const std::vector<cost_sample>& per) {
    std::vector<double> ns;
    ns.reserve(per.size());
    std::uint64_t attempts = 0;
    std::uint64_t ok = 0;
    for (const auto& s : per) {
        if (s.attempts == 0) continue;
        // Median across producers, not the mean: one descheduled thread should
        // not move the reported per-producer cost.
        ns.push_back(static_cast<double>(s.busy) / static_cast<double>(s.attempts));
        attempts += s.attempts;
        ok += s.ok;
    }
    cost_result r;
    r.ns_per_push = median_of(ns);
    r.success = attempts > 0 ? static_cast<double>(ok) / static_cast<double>(attempts) : 0.0;
    return r;
}

// --- arm A: the ring alone --------------------------------------------------

cost_result push_cost_raw(std::uint32_t producers) {
    std::vector<std::unique_ptr<fixed_channel<record>>> channels;
    channels.reserve(producers);
    for (std::uint32_t i = 0; i < producers; ++i)
        channels.push_back(std::make_unique<fixed_channel<record>>(4096));

    std::atomic<bool> running{true};
    std::thread drainer([&channels, &running] {
        std::vector<record> buf(kDrainBatch);
        while (running.load(std::memory_order_relaxed))
            for (auto& ch : channels) ch->pop_batch(buf.data(), buf.size());
        for (auto& ch : channels)
            while (ch->pop_batch(buf.data(), buf.size()) > 0) {
            }
    });

    std::vector<cost_sample> per(producers);
    std::vector<std::thread> ts;
    ts.reserve(producers);
    for (std::uint32_t p = 0; p < producers; ++p) {
        ts.emplace_back([&channels, &per, p, producers] {
            fixed_channel<record>& ch = *channels[p];
            per[p] = run_bursts(p, gap_for(producers),
                                [&ch](const record& r) { return ch.try_push(r); });
        });
    }
    for (auto& t : ts) t.join();
    running.store(false, std::memory_order_relaxed);
    drainer.join();

    return summarise(per);
}

// --- arm B: the real push path ----------------------------------------------

cost_result push_cost_pipeline(std::uint32_t producers) {
    options opt;
    opt.max_producers = producers;
    opt.scan = scan_policy::bitmap;
    // drop_newest: a spin budget would fold the consumer's stall into the
    // producer's measured cost, and this column is about the push itself.
    opt.full = full_policy::drop_newest;

    pipeline<record> pipe(opt);
    drain_sink sink;
    pipe.start(sink);

    std::vector<cost_sample> per(producers);
    std::vector<std::thread> ts;
    ts.reserve(producers);
    for (std::uint32_t p = 0; p < producers; ++p) {
        ts.emplace_back([&pipe, &per, p, producers] {
            auto h = pipe.register_producer();
            if (!h) return;
            per[p] = run_bursts(h.id(), gap_for(producers),
                                [&h](const record& r) { return h.push(r); });
        });
    }
    for (auto& t : ts) t.join();
    pipe.stop(stop_mode::drain);

    return summarise(per);
}

// --- overload ---------------------------------------------------------------

struct sat_result {
    double offered_mps = 0.0;
    double accepted_pct = 0.0;
    double sustained_mps = 0.0;
};

sat_result run_saturating(std::uint32_t producers) {
    options opt;
    opt.max_producers = producers;
    opt.scan = scan_policy::bitmap;
    opt.full = full_policy::drop_newest;

    pipeline<record> pipe(opt);
    drain_sink sink;
    pipe.start(sink);

    std::vector<std::uint64_t> attempts(producers, 0);
    std::vector<std::int64_t> elapsed(producers, 0);
    std::vector<std::thread> ts;
    ts.reserve(producers);
    for (std::uint32_t p = 0; p < producers; ++p) {
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
            } while (dt < kSatRunNs);
            attempts[p] = n;
            elapsed[p] = dt;
        });
    }
    for (auto& t : ts) t.join();
    pipe.stop(stop_mode::drain);

    const auto st = pipe.snapshot();
    std::uint64_t total = 0;
    std::int64_t wall = 0;
    for (std::uint32_t p = 0; p < producers; ++p) {
        total += attempts[p];
        wall = std::max(wall, elapsed[p]);
    }
    const double secs = static_cast<double>(wall) / 1e9;

    sat_result r;
    r.offered_mps = static_cast<double>(total) / secs / 1e6;
    r.accepted_pct =
        total > 0 ? static_cast<double>(st.pushed) / static_cast<double>(total) * 100.0 : 0.0;
    r.sustained_mps = static_cast<double>(st.pushed) / secs / 1e6;

    // Accounting is checked, not trusted: a throughput number from a run that
    // lost track of its records is not a throughput number.
    check(st.pushed + st.dropped == total,
          "pushed + dropped == offered at " + std::to_string(producers) + " producers");
    return r;
}

// Warm-up discarded (first touch, page faults, thread start), then median.
template <class Fn>
auto median_of_reps(Fn&& fn, std::uint32_t producers) {
    fn(producers);
    std::vector<decltype(fn(producers))> runs;
    runs.reserve(kReps);
    for (int i = 0; i < kReps; ++i) runs.push_back(fn(producers));
    return runs;
}

cost_result median_cost(cost_result (*fn)(std::uint32_t), std::uint32_t producers) {
    const auto runs = median_of_reps(fn, producers);
    std::vector<double> ns;
    std::vector<double> su;
    for (const auto& r : runs) {
        ns.push_back(r.ns_per_push);
        su.push_back(r.success);
    }
    cost_result m;
    m.ns_per_push = median_of(ns);
    m.success = median_of(su);
    return m;
}

sat_result median_sat(std::uint32_t producers) {
    const auto runs = median_of_reps(run_saturating, producers);
    std::vector<double> off;
    std::vector<double> acc;
    std::vector<double> sus;
    for (const auto& r : runs) {
        off.push_back(r.offered_mps);
        acc.push_back(r.accepted_pct);
        sus.push_back(r.sustained_mps);
    }
    sat_result m;
    m.offered_mps = median_of(off);
    m.accepted_pct = median_of(acc);
    m.sustained_mps = median_of(sus);
    return m;
}

}  // namespace

void bench_throughput() {
    constexpr std::size_t kLevels = std::size(kProducerCounts);

    std::printf("\n=== phase 2: throughput ===\n");
    std::printf("push cost: paced bursts of %zu, %.0f ms; overload: saturating, %.0f ms;"
                " warm-up discarded, median of %d\n\n",
                kBurst, static_cast<double>(kCostRunNs) / 1e6,
                static_cast<double>(kSatRunNs) / 1e6, kReps);

    const unsigned cores = std::thread::hardware_concurrency();
    if (cores != 0 && kProducerCounts[kLevels - 1] + 1 > cores)
        note("the widest point oversubscribes this machine; past that the numbers describe "
             "the OS scheduler rather than the queue");

    cost_result raw[kLevels];
    cost_result pipe[kLevels];
    sat_result sat[kLevels];
    for (std::size_t i = 0; i < kLevels; ++i) {
        raw[i] = median_cost(push_cost_raw, kProducerCounts[i]);
        pipe[i] = median_cost(push_cost_pipeline, kProducerCounts[i]);
        sat[i] = median_sat(kProducerCounts[i]);
    }

    std::printf("producers |   ns/push    ns/push |   offered  accepted   sustained\n");
    std::printf("          | no-signal    +bitmap |       M/s         %%         M/s\n");
    std::printf("----------+----------------------+--------------------------------\n");
    for (std::size_t i = 0; i < kLevels; ++i) {
        std::printf("%-9u | %9.1f %10.1f | %9.1f %8.1f%% %11.1f\n",
                    static_cast<unsigned>(kProducerCounts[i]), raw[i].ns_per_push,
                    pipe[i].ns_per_push, sat[i].offered_mps, sat[i].accepted_pct,
                    sat[i].sustained_mps);
    }
    std::printf("\n");

    // --- is the push-cost measurement even valid? ---------------------------
    //
    // Checked before anything is concluded from it. If the rings were full, the
    // ns/push columns are the failed-push path and mean nothing.
    for (std::size_t i = 0; i < kLevels; ++i) {
        const std::string at = " at " + std::to_string(kProducerCounts[i]) + " producers";
        check(raw[i].success >= kMinSuccess,
              "no-signal push cost measured on real pushes, not full rings" + at);
        check(pipe[i].success >= kMinSuccess,
              "+bitmap push cost measured on real pushes, not full rings" + at);
    }

    // --- the design claim ----------------------------------------------------
    //
    // Flatness from 4 producers up. The 1-producer figure is deliberately
    // excluded: a lone producer outruns nothing and spends its time on a ring
    // the consumer is actively draining, so it differs for a reason that has
    // nothing to do with contention. It is not the interesting case.
    // Deliberately ONE-SIDED. The claim is that push cost does not *climb* with
    // producer count; a shared tail would show it climbing as the contended line
    // migrates between cores. Cost falling as producers are added is not a
    // violation of that -- it is the absence of contention, and a symmetric
    // bound would fail the design for succeeding.
    double base = 0.0;
    double worst = 0.0;
    for (std::size_t i = 0; i < kLevels; ++i) {
        if (kProducerCounts[i] == 4) base = raw[i].ns_per_push;
        if (kProducerCounts[i] >= 4) worst = std::max(worst, raw[i].ns_per_push);
    }
    check_le(worst / base, 1.5,
             "ns/push (no-signal) does not climb from 4 to 32 producers -- the shape is the claim");

    for (std::size_t i = 0; i < kLevels; ++i)
        check(pipe[i].ns_per_push >= raw[i].ns_per_push,
              "the handshake costs something at " + std::to_string(kProducerCounts[i]) +
                  " producers, and the arms say so");

    note("no-signal is the RING ALONE: no consumer scan, no staging, no sink, no handshake.");
    note("It is a floor, not the pipeline with a feature switched off.");
    note("offered counts ATTEMPTS, including the cheap failures once a ring is full -- which");
    note("is why push cost is measured on a separate, paced run rather than from this one.");
    note("Past ~4 producers the single consumer is the ceiling and the surplus is dropped by");
    note("policy: the system behaving as designed, not failing.");

    phase_verdict("phase 2: throughput");
}

}  // namespace tributary::bench
