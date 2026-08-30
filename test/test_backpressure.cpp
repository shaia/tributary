// Overload and backpressure.
//
// "Producers must not block indefinitely" and "bounded memory" are the same
// requirement seen from two sides, and together they force the answer: the
// queue is bounded, so overload must have an explicit policy, and every policy
// must terminate.
//
// The partial-accept and stalling cases matter most. A sink that always takes
// everything never touches the retain/compact/re-offer path, which is where a
// backpressured pipeline actually lives.

#include "harness.hpp"
#include "sinks.hpp"

#include <tributary/pipeline.hpp>

#include <algorithm>
#include <chrono>
#include <thread>
#include <vector>

using namespace tributary;
using namespace tributary::test;

namespace {

void test_partial_accept() {
    std::printf("partial accept\n");

    // The sink takes at most 7 records per call, so almost every flush leaves a
    // remainder that must be compacted and re-offered.
    options opt;
    opt.max_producers = 4;
    opt.batch_capacity = 64;
    opt.full = full_policy::spin_then_drop;
    opt.push_spin = 100'000;  // effectively "wait": this test is about delivery, not drops

    pipeline<event> pipe(opt);
    partial_sink sink(7);
    pipe.start(sink);

    constexpr std::uint32_t kProducers = 4;
    constexpr std::uint32_t kPer = 20'000;
    std::vector<std::thread> ts;
    ts.reserve(kProducers);
    for (std::uint32_t p = 0; p < kProducers; ++p)
        ts.emplace_back([&pipe] {
            auto h = pipe.register_producer();
            if (!h) return;
            for (std::uint32_t s = 1; s <= kPer; ++s) h.push(make_event(h.id(), s));
        });
    for (auto& t : ts) t.join();
    pipe.stop(stop_mode::drain, std::chrono::seconds(30));

    const auto st = pipe.snapshot();
    std::printf("  pushed=%llu delivered=%llu short_writes=%llu backpressure_events=%llu\n",
                static_cast<unsigned long long>(st.pushed),
                static_cast<unsigned long long>(sink.records()),
                static_cast<unsigned long long>(sink.short_writes()),
                static_cast<unsigned long long>(st.sink_backpressure));

    check(sink.short_writes() > 0, "the sink really did accept short, exercising the retain path");
    check_eq(sink.records(), st.pushed, "a short-accepting sink still received every record");
    check(st.sink_backpressure > 0, "backpressure was counted, not invisible");
    check(st.high_water <= opt.ring_capacity, "memory stayed bounded under backpressure");
}

void test_stalled_sink_recovers() {
    std::printf("\nstalled sink recovery\n");

    // The defect this closes: a sink that refuses a batch and then recovers,
    // with no new records arriving to drive another flush. Without on_idle()
    // being called on the way to sleep, the remainder sits there forever.
    options opt;
    opt.max_producers = 2;
    opt.park_timeout = std::chrono::milliseconds(1);

    pipeline<event> pipe(opt);
    stalling_sink sink;
    pipe.start(sink);

    constexpr std::uint32_t kRecords = 500;
    {
        auto h = pipe.register_producer();
        check(h.valid(), "producer registered");
        for (std::uint32_t s = 1; s <= kRecords; ++s) h.push(make_event(h.id(), s));
    }

    // Let the consumer discover the refusal, drain what it can, and go quiet.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    check(sink.refusals() > 0, "the sink refused at least one batch");
    check_eq(sink.records(), 0, "nothing was delivered while the sink was closed");
    check(sink.idle_calls() > 0, "on_idle() was called while the pipeline was quiet");

    // Recover. Crucially, push nothing more: only on_idle() can get this
    // unstuck, which is exactly the case that used to strand the remainder.
    sink.open();

    bool delivered = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        if (sink.records() >= kRecords) {
            delivered = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    check(delivered, "a recovered sink drained without any new records arriving");

    pipe.stop(stop_mode::drain);
    check(sink.closes() > 0, "close() was called once the pipeline finished");
}

void test_overload_drops_rather_than_blocks() {
    std::printf("\noverload\n");

    // A deliberately slow sink drives the rings into overflow. The two numbers
    // that matter: memory stayed bounded, and no producer blocked indefinitely.
    options opt;
    opt.max_producers = 8;
    opt.ring_capacity = 256;
    opt.full = full_policy::drop_newest;

    pipeline<event> pipe(opt);
    slow_sink sink(std::chrono::microseconds(500));
    pipe.start(sink);

    constexpr std::uint32_t kProducers = 4;
    constexpr std::uint32_t kPer = 50'000;
    std::atomic<std::int64_t> worst_push_ns{0};

    std::vector<std::thread> ts;
    ts.reserve(kProducers);
    for (std::uint32_t p = 0; p < kProducers; ++p)
        ts.emplace_back([&] {
            auto h = pipe.register_producer();
            if (!h) return;
            std::int64_t worst = 0;
            for (std::uint32_t s = 1; s <= kPer; ++s) {
                const auto t0 = std::chrono::steady_clock::now();
                h.push(make_event(h.id(), s));
                const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    std::chrono::steady_clock::now() - t0)
                                    .count();
                worst = std::max(worst, dt);
            }
            std::int64_t prev = worst_push_ns.load(std::memory_order_relaxed);
            while (worst > prev &&
                   !worst_push_ns.compare_exchange_weak(prev, worst, std::memory_order_relaxed)) {
            }
        });
    for (auto& t : ts) t.join();
    pipe.stop(stop_mode::abort);

    const auto st = pipe.snapshot();
    std::printf("  pushed=%llu dropped=%llu (%.1f%%) high_water=%llu worst_push=%.1f us\n",
                static_cast<unsigned long long>(st.pushed),
                static_cast<unsigned long long>(st.dropped), 100.0 * st.drop_fraction(),
                static_cast<unsigned long long>(st.high_water),
                static_cast<double>(worst_push_ns.load()) / 1000.0);

    check(st.dropped > 0, "overload produced drops rather than blocking");
    check_eq(st.pushed + st.dropped, std::uint64_t{kProducers} * kPer,
             "every offered record was either accepted or counted as dropped");
    check(st.high_water <= opt.ring_capacity, "memory stayed bounded while overloaded");
    // Generous, because this figure is dominated by OS preemption rather than
    // by the queue -- the point is that it is bounded at all.
    check(worst_push_ns.load() < 500'000'000,
          "no producer blocked indefinitely (worst push under 500 ms)");
}

void test_drop_newest_is_immediate() {
    std::printf("\ndrop_newest\n");

    options opt;
    opt.max_producers = 1;
    opt.ring_capacity = 16;
    opt.full = full_policy::drop_newest;
    opt.own_threads = false;  // nothing drains, so the ring stays full

    pipeline<event> pipe(opt);
    counting_sink sink;
    pipe.start(sink);

    auto h = pipe.register_producer();
    std::uint64_t accepted = 0;
    const auto t0 = std::chrono::steady_clock::now();
    for (std::uint32_t s = 1; s <= 1000; ++s)
        if (h.push(make_event(h.id(), s))) ++accepted;
    const auto elapsed = std::chrono::steady_clock::now() - t0;

    check_eq(accepted, opt.ring_capacity, "exactly capacity records were accepted");
    check_eq(pipe.snapshot().dropped, 1000 - opt.ring_capacity, "the rest were counted as drops");
    check(elapsed < std::chrono::milliseconds(100),
          "drop_newest failed immediately instead of spinning");

    h.release();
    pipe.stop(stop_mode::abort);
}

}  // namespace

int main() {
    test_partial_accept();
    test_stalled_sink_recovers();
    test_overload_drops_rather_than_blocks();
    test_drop_newest_is_immediate();
    return summary("test_backpressure");
}
