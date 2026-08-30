// Phase 3 -- overload, and the two ways a bounded pipeline can quietly stop
// being bounded.
//
// Backpressure is only backpressure if every link in the chain is bounded. A
// sink that buffers its own remainder converts a latency problem into a memory
// problem, and the memory problem arrives later, larger, and as an OOM kill. So
// this phase asserts that overload turns into *counted drops* rather than into
// growth, and that no producer ever waits on the consumer's worst stall.
//
// Producers are open-loop here for a reason that matters more in this phase than
// anywhere else: the sink is deliberately too slow, and a closed loop would let
// that slowness throttle the producers that are measuring it. The stall would
// then be invisible in exactly the run built to expose it.

#include "harness.hpp"
#include "phases.hpp"

#include <tributary/pipeline.hpp>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace tributary::bench {
namespace {

constexpr std::uint32_t kProducers = 8;
constexpr double kRatePerProducer = 200'000.0;  // records/s each, well past the sink
constexpr std::int64_t kRunNs = 400'000'000;    // 400 ms of overload
constexpr std::int64_t kStartDelayNs = 20'000'000;
constexpr std::int64_t kSinkDelayNs = 500'000;  // 500 us per batch
constexpr std::size_t kBatchCapacity = 512;     // with the delay, a ~1 M/s ceiling

// A push must never wait on the consumer's worst stall. This bound is loose on
// purpose -- it is testing for "unbounded", not for a tight latency target, and
// a tight bound here would fail on an unrelated scheduler hiccup.
constexpr std::int64_t kMaxPushNs = 50'000'000;  // 50 ms

// --- overload ---------------------------------------------------------------

// Deliberately slow. Stands in for a socket whose peer has stopped reading.
// Spins rather than sleeping: on Windows sleep_for rounds up to the system timer
// tick, which would make the delay 1-15 ms instead of the 500 us asked for.
class slow_sink {
public:
    std::size_t write(std::span<const record> b) noexcept {
        const std::int64_t until = now_ns() + kSinkDelayNs;
        while (now_ns() < until) TRIBUTARY_PAUSE();
        records_.fetch_add(b.size(), std::memory_order_relaxed);
        return b.size();
    }
    [[nodiscard]] std::uint64_t records() const noexcept {
        return records_.load(std::memory_order_relaxed);
    }

private:
    std::atomic<std::uint64_t> records_{0};
};

void overload() {
    options opt;
    opt.max_producers = kProducers;
    opt.batch_capacity = kBatchCapacity;
    // The policy with a bounded wait, which is the one worth stressing: it is
    // the only one that could block, so it is the one that has to prove it does
    // not block indefinitely.
    opt.full = full_policy::spin_then_drop;

    pipeline<record> pipe(opt);
    slow_sink sink;
    pipe.start(sink);

    const std::int64_t start = now_ns() + kStartDelayNs;
    const std::int64_t end = start + kRunNs;
    std::atomic<std::int64_t> worst_push{0};
    std::atomic<std::uint64_t> offered{0};

    std::vector<std::thread> ts;
    ts.reserve(kProducers);
    for (std::uint32_t p = 0; p < kProducers; ++p) {
        ts.emplace_back([&pipe, &worst_push, &offered, start, end] {
            auto h = pipe.register_producer();
            if (!h) return;
            const pacer pc(start, kRatePerProducer);
            std::int64_t worst = 0;
            std::uint64_t n = 0;
            for (std::uint64_t i = 0;; ++i) {
                const std::int64_t due = pc.due(i);
                if (due >= end) break;
                pacer::spin_until(due);
                record r{};
                r.producer_id = h.id();
                r.seq = static_cast<std::uint32_t>(i + 1);
                r.send_ns = due;
                // Timing every push costs two clock reads against a ~20 ns push.
                // Affordable only because this phase measures the maximum, not
                // the throughput, and the producers are paced at 5 us anyway.
                const std::int64_t t0 = now_ns();
                h.push(r);
                worst = std::max(worst, now_ns() - t0);
                ++n;
            }
            offered.fetch_add(n, std::memory_order_relaxed);
            // One relaxed max-merge per thread rather than per push.
            std::int64_t seen = worst_push.load(std::memory_order_relaxed);
            while (seen < worst &&
                   !worst_push.compare_exchange_weak(seen, worst, std::memory_order_relaxed)) {
            }
        });
    }
    for (auto& t : ts) t.join();

    const auto during = pipe.snapshot();

    // Abort must return promptly even with a wedged sink in the middle of a
    // batch. A shutdown that can hang is a shutdown that will.
    const std::int64_t t_stop = now_ns();
    pipe.stop(stop_mode::abort);
    const double stop_ms = static_cast<double>(now_ns() - t_stop) / 1e6;

    std::printf("  overload   offered=%llu pushed=%llu dropped=%llu (%.1f%%) high_water=%llu\n",
                static_cast<unsigned long long>(during.pushed + during.dropped),
                static_cast<unsigned long long>(during.pushed),
                static_cast<unsigned long long>(during.dropped), during.drop_fraction() * 100.0,
                static_cast<unsigned long long>(during.high_water));
    std::printf("  worst push %.3f ms, abort returned in %.3f ms\n",
                static_cast<double>(worst_push.load()) / 1e6, stop_ms);

    check(during.dropped > 0,
          "overload turned into counted drops (the sink was outrun, as intended)");
    // Accounting has to hold *under overload* specifically. This is where a
    // drop-counting bug hides: the counts only diverge when the drop path is
    // actually being taken, which is never in a healthy run.
    check(during.pushed + during.dropped == offered.load(),
          "pushed + dropped == offered, exactly, while dropping");
    check(during.high_water <= opt.ring_capacity,
          "memory stayed bounded: ring depth never exceeded capacity");
    check_le(static_cast<double>(worst_push.load()) / 1e6,
             static_cast<double>(kMaxPushNs) / 1e6,
             "no producer blocked indefinitely (worst push, ms)");
    check_le(stop_ms, 1000.0, "abort returned promptly (ms)");
}

// --- a stalled sink that later recovers -------------------------------------

// Refuses everything until opened, then accepts everything.
//
// This is the shape that strands a remainder. If flush is only reached when a
// drain pass moved records, a sink holding a partial write has nothing to drive
// a retry once the traffic stops -- and it stops precisely when the far side is
// in trouble. on_idle() is the hook that closes it.
class stalling_sink {
public:
    std::size_t write(std::span<const record> b) noexcept {
        if (!open_.load(std::memory_order_acquire)) {
            refusals_.fetch_add(1, std::memory_order_relaxed);
            return 0;
        }
        records_.fetch_add(b.size(), std::memory_order_relaxed);
        return b.size();
    }

    // Called before the consumer parks and on every park timeout. Reporting
    // progress here is what gets a recovered sink unstuck with no new records
    // arriving to drive another flush.
    bool on_idle() noexcept {
        idle_calls_.fetch_add(1, std::memory_order_relaxed);
        return open_.load(std::memory_order_acquire) &&
               refusals_.load(std::memory_order_relaxed) > 0;
    }

    void open() noexcept { open_.store(true, std::memory_order_release); }

    [[nodiscard]] std::uint64_t records() const noexcept {
        return records_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t refusals() const noexcept {
        return refusals_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t idle_calls() const noexcept {
        return idle_calls_.load(std::memory_order_relaxed);
    }

private:
    std::atomic<bool> open_{false};
    std::atomic<std::uint64_t> records_{0};
    std::atomic<std::uint64_t> refusals_{0};
    std::atomic<std::uint64_t> idle_calls_{0};
};

// Spin-waits for `pred`, bounded. Returns false on timeout rather than hanging
// the whole run on one stuck condition.
template <class Pred>
bool wait_for(Pred pred, std::int64_t timeout_ns) {
    const std::int64_t until = now_ns() + timeout_ns;
    while (now_ns() < until) {
        if (pred()) return true;
        std::this_thread::yield();
    }
    return pred();
}

void stalled_sink_recovers() {
    // Comfortably under ring_capacity. With the sink refusing everything the
    // ring cannot drain, so offering more than it holds would drop the surplus
    // by policy -- correct behaviour, but it would make this phase's assertion
    // untestable rather than failing it for the reason under test.
    constexpr std::uint32_t kRecords = 2'000;

    options opt;
    opt.max_producers = 2;
    pipeline<record> pipe(opt);
    stalling_sink sink;
    pipe.start(sink);
    check(kRecords < opt.ring_capacity, "the stall test offers less than one ring holds");

    std::uint64_t accepted = 0;
    {
        auto h = pipe.register_producer();
        record r{};
        r.producer_id = h.id();
        for (std::uint32_t s = 1; s <= kRecords; ++s) {
            r.seq = s;
            if (h.push(r)) ++accepted;
        }
    }

    const bool refused = wait_for([&sink] { return sink.refusals() > 0; }, 2'000'000'000);
    check(refused, "the stalled sink actually refused a batch (the setup is real)");
    check(sink.records() == 0, "nothing was delivered while the sink was refusing");

    // Wait for the consumer to actually reach its pre-park path before the sink
    // recovers. Without this it is usually still spinning when the sink opens,
    // the next ordinary flush succeeds, and the test passes green having never
    // exercised on_idle() at all -- which is the entire mechanism under test.
    const bool went_idle = wait_for([&sink] { return sink.idle_calls() > 0; }, 2'000'000'000);
    check(went_idle, "the consumer went idle holding a refused remainder");

    // The critical part: no further records are pushed from here. Recovery has
    // to be driven by on_idle() alone.
    const std::uint64_t idle_at_open = sink.idle_calls();
    sink.open();
    const bool drained =
        wait_for([&sink, accepted] { return sink.records() >= accepted; }, 5'000'000'000);

    std::printf("  recovery   refusals=%llu idle_calls=%llu delivered=%llu/%llu accepted\n",
                static_cast<unsigned long long>(sink.refusals()),
                static_cast<unsigned long long>(sink.idle_calls()),
                static_cast<unsigned long long>(sink.records()),
                static_cast<unsigned long long>(accepted));

    check(sink.idle_calls() > idle_at_open, "on_idle() kept being called after recovery");
    check(drained, "a recovered sink drained its remainder with no new records arriving");

    pipe.stop(stop_mode::drain);
}

}  // namespace

void bench_backpressure() {
    std::printf("\n=== phase 3: backpressure and shutdown ===\n");
    std::printf("%u producers at %.0fk rec/s each against a %.0f us/batch sink\n\n",
                static_cast<unsigned>(kProducers), kRatePerProducer / 1000.0,
                static_cast<double>(kSinkDelayNs) / 1000.0);

    overload();
    stalled_sink_recovers();
    std::printf("\n");

    phase_verdict("phase 3: backpressure and shutdown");
}

}  // namespace tributary::bench
