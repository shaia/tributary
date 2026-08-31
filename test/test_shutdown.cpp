// Shutdown semantics.
//
// Two phases and two modes. Stop accepting first -- draining a queue that is
// still being filled is a race you can lose indefinitely -- then drain, then
// terminate the consumer. Everything is bounded by a deadline, because a wedged
// sink must never make shutdown hang forever.

#include "harness.hpp"
#include "sinks.hpp"

#include <tributary/pipeline.hpp>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace tributary;
using namespace tributary::test;

namespace {

void test_drain_loses_nothing() {
    std::printf("drain\n");

    options opt;
    opt.max_producers = 8;
    opt.full = full_policy::spin_then_drop;
    opt.push_spin = 100'000;

    pipeline<event> pipe(opt);
    ordering_sink sink(opt.max_producers);
    pipe.start(sink);

    constexpr std::uint32_t kProducers = 4;
    constexpr std::uint32_t kPer = 50'000;
    std::vector<std::thread> ts;
    ts.reserve(kProducers);
    for (std::uint32_t p = 0; p < kProducers; ++p)
        ts.emplace_back([&pipe] {
            auto h = pipe.register_producer();
            if (!h) return;
            for (std::uint32_t s = 1; s <= kPer; ++s) h.push(make_event(h.id(), s));
        });
    for (auto& t : ts) t.join();

    pipe.stop(stop_mode::drain);
    const auto st = pipe.snapshot();

    check(pipe.drain_completed(), "drain reported success");
    check_eq(sink.events(), st.pushed, "drain delivered every accepted event");
    check_eq(st.dropped, 0, "nothing was dropped at this rate");
}

void test_abort_is_prompt() {
    std::printf("\nabort\n");

    // A sink slow enough that a drain could not possibly finish. Abort must
    // return anyway, and promptly.
    options opt;
    opt.max_producers = 4;
    opt.ring_capacity = 4096;

    pipeline<event> pipe(opt);
    slow_sink sink(std::chrono::milliseconds(5));
    pipe.start(sink);

    auto h = pipe.register_producer();
    for (std::uint32_t s = 1; s <= 20'000; ++s) h.push(make_event(h.id(), s));
    h.release();

    const auto t0 = std::chrono::steady_clock::now();
    pipe.stop(stop_mode::abort);
    const auto elapsed = std::chrono::steady_clock::now() - t0;

    std::printf("  abort took %lld ms\n",
                static_cast<long long>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()));
    // Generous: abort still lets the in-flight batch finish, and that batch is
    // deliberately slow here. The assertion is that it does not wait for the
    // whole backlog, which at 5 ms per batch would be many seconds.
    check(elapsed < std::chrono::seconds(2), "abort returned without draining the backlog");
}

void test_drain_deadline_degrades_honestly() {
    std::printf("\ndrain deadline\n");

    options opt;
    opt.max_producers = 4;
    opt.ring_capacity = 4096;

    pipeline<event> pipe(opt);
    slow_sink sink(std::chrono::milliseconds(10));
    pipe.start(sink);

    auto h = pipe.register_producer();
    for (std::uint32_t s = 1; s <= 20'000; ++s) h.push(make_event(h.id(), s));
    h.release();

    // A deadline the drain cannot possibly meet.
    pipe.stop(stop_mode::drain, std::chrono::milliseconds(50));

    check(!pipe.drain_completed(),
          "a drain that missed its deadline says so rather than claiming success");
}

void test_producers_stop_being_accepted() {
    std::printf("\naccepting flag\n");

    options opt;
    opt.max_producers = 2;
    pipeline<event> pipe(opt);
    ordering_sink sink(opt.max_producers);
    pipe.start(sink);

    auto h = pipe.register_producer();
    check(h.push(make_event(h.id(), 1)), "push works while running");

    pipe.stop(stop_mode::drain);
    check(!h.push(make_event(h.id(), 2)), "push is refused after stop");
    check_eq(sink.events(), 1, "the event pushed before stop was delivered");

    h.release();
}

void test_stop_is_idempotent() {
    std::printf("\nidempotence\n");

    options opt;
    opt.max_producers = 2;
    pipeline<event> pipe(opt);
    counting_sink sink;

    pipe.stop(stop_mode::drain);  // never started
    check(true, "stop before start is a no-op");

    pipe.start(sink);
    pipe.stop(stop_mode::drain);
    pipe.stop(stop_mode::drain);
    pipe.stop(stop_mode::abort);
    check(true, "repeated stop calls are safe");
}

void test_destructor_stops_a_running_pipeline() {
    std::printf("\ndestructor\n");

    counting_sink sink;
    std::uint64_t pushed = 0;
    {
        options opt;
        opt.max_producers = 2;
        pipeline<event> pipe(opt);
        pipe.start(sink);
        auto h = pipe.register_producer();
        for (std::uint32_t s = 1; s <= 1000; ++s)
            if (h.push(make_event(h.id(), s))) ++pushed;
        h.release();
        // No explicit stop: the destructor must abort cleanly, join the
        // consumer, and not deadlock on the handle already being released.
    }
    check(pushed > 0, "events were pushed before destruction");
    check(true, "destroying a running pipeline did not hang or crash");
}

void test_external_driving() {
    std::printf("\nexternal driving (poll)\n");

    options opt;
    opt.max_producers = 4;
    opt.own_threads = false;
    pipeline<event> pipe(opt);
    ordering_sink sink(opt.max_producers);
    pipe.start(sink);

    constexpr std::uint32_t kEvents = 5000;
    auto h = pipe.register_producer();

    // Interleave pushing and polling on one thread, the way a single-threaded
    // reactor integration would.
    std::uint64_t pushed = 0;
    for (std::uint32_t s = 1; s <= kEvents; ++s) {
        if (h.push(make_event(h.id(), s))) ++pushed;
        if (s % 100 == 0) pipe.poll(0, sink);
    }
    h.release();

    while (pipe.poll(0, sink) > 0) {
    }
    pipe.close(0, sink);

    check_eq(pushed, kEvents, "nothing was dropped when polling kept up");
    check_eq(sink.events(), pushed, "poll() delivered every event");
    check_eq(sink.violations(), 0, "poll() preserved order");

    pipe.stop(stop_mode::abort);
}

}  // namespace

int main() {
    test_drain_loses_nothing();
    test_abort_is_prompt();
    test_drain_deadline_degrades_honestly();
    test_producers_stop_being_accepted();
    test_stop_is_idempotent();
    test_destructor_stops_a_running_pipeline();
    test_external_driving();
    return summary("test_shutdown");
}
