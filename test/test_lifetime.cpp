// Producer slots come and go. The ring must never be reused or destroyed while
// the consumer is still reading it, and a recycled slot must not erase its
// predecessor's accounting.
//
// This is the test that exercises the four-state slot protocol, and the one
// worth running under AddressSanitizer.

#include "harness.hpp"
#include "sinks.hpp"

#include <tributary/pipeline.hpp>

#include <atomic>
#include <thread>
#include <vector>

using namespace tributary;
using namespace tributary::test;

namespace {

void test_producer_churn() {
    std::printf("producer churn\n");

    // Deliberately fewer slots than total producers over the run, so slots are
    // recycled many times over while the consumer is concurrently draining them.
    constexpr int kWaves = 40;
    constexpr int kThreads = 8;
    constexpr std::uint32_t kPer = 500;

    options opt;
    opt.max_producers = 16;
    pipeline<event> pipe(opt);
    ordering_sink sink(opt.max_producers);
    pipe.start(sink);

    std::atomic<std::uint64_t> registered{0};
    for (int w = 0; w < kWaves; ++w) {
        std::vector<std::thread> ts;
        ts.reserve(kThreads);
        for (int t = 0; t < kThreads; ++t) {
            ts.emplace_back([&] {
                auto h = pipe.register_producer();
                if (!h) return;
                registered.fetch_add(1, std::memory_order_relaxed);
                for (std::uint32_t s = 1; s <= kPer; ++s) h.push(make_event(h.id(), s));
                // Handle destructs here, retiring the slot while the consumer
                // may still be mid-batch inside its ring.
            });
        }
        for (auto& t : ts) t.join();
    }
    pipe.stop(stop_mode::drain);

    const auto st = pipe.snapshot();
    std::printf("  %d waves x %d producers, registered=%llu pushed=%llu delivered=%llu\n", kWaves,
                kThreads, static_cast<unsigned long long>(registered.load()),
                static_cast<unsigned long long>(st.pushed),
                static_cast<unsigned long long>(sink.records()));

    check_eq(sink.records(), st.pushed, "every retired producer's records were still drained");
    check_eq(st.pushed + st.dropped, registered.load() * kPer,
             "accounting survived slot recycling (this is what folding retired counters buys)");
    check(pipe.drain_completed(), "drain finished within its deadline");

    // The regression guard for reclaim_if_pending(). Reclaiming only on the
    // idle path starves registration under sustained load: a busy consumer
    // never goes idle, so retired slots are never published free. That showed
    // up here as 144 of 320 registrations succeeding, and every one of those
    // failures is a producer thread that silently produced nothing.
    check_eq(registered.load(), std::uint64_t{kWaves} * kThreads,
             "every wave's producers got a slot; retired slots were reclaimed under load");
    check_eq(st.registration_failures, 0, "no registration was refused");

    // Ordering is per-producer-slot here, and a recycled slot restarts its
    // sequence at 1 -- so a violation would be a genuine reorder within one
    // producer's lifetime, not the recycle itself.
    std::printf("  (order violations across recycled slots: %llu)\n",
                static_cast<unsigned long long>(sink.violations()));
}

void test_retire_while_full() {
    std::printf("\nretirement with a backed-up ring\n");

    // A producer that fills its ring and then leaves. The consumer must drain
    // what it published before the slot can be reused, or those records vanish.
    options opt;
    opt.max_producers = 2;
    opt.ring_capacity = 64;
    opt.own_threads = false;  // no consumer running: the ring will genuinely fill
    pipeline<event> pipe(opt);
    counting_sink sink;
    pipe.start(sink);

    std::uint64_t pushed = 0;
    {
        auto h = pipe.register_producer();
        check(h.valid(), "producer registered");
        for (std::uint32_t s = 1; s <= 200; ++s)
            if (h.push(make_event(h.id(), s))) ++pushed;
    }  // retires here, with a full ring nobody has read

    check_eq(pushed, opt.ring_capacity, "the ring filled to exactly its capacity, then dropped");

    // Now drain by hand, the way a reactor integration would.
    std::size_t drained = 0;
    for (int i = 0; i < 100; ++i) drained += pipe.poll(0, sink);
    pipe.close(0, sink);

    check_eq(sink.records, pushed, "a retired producer's full ring was drained, not discarded");
    check(drained > 0, "poll() reported the work it did");

    pipe.stop(stop_mode::abort);
}

void test_slot_reuse_is_allocation_free() {
    std::printf("\nslot reuse\n");

    options opt;
    opt.max_producers = 1;
    pipeline<event> pipe(opt);
    ordering_sink sink(opt.max_producers);
    pipe.start(sink);

    // Take and release the same slot repeatedly. The ring is kept across
    // reuses -- its free-running indices are already consistent -- so this
    // should not allocate after the first pass. What we can assert portably is
    // that it stays correct.
    std::uint64_t total = 0;
    for (int round = 0; round < 200; ++round) {
        bool got = false;
        for (int retry = 0; retry < 5000 && !got; ++retry) {
            auto h = pipe.register_producer();
            if (!h) {
                std::this_thread::sleep_for(std::chrono::microseconds(50));
                continue;
            }
            got = true;
            for (std::uint32_t s = 1; s <= 10; ++s)
                if (h.push(make_event(h.id(), s))) ++total;
        }
        if (!got) {
            check(false, "slot came back for round " + std::to_string(round));
            break;
        }
    }
    pipe.stop(stop_mode::drain);

    check_eq(sink.records(), total, "200 register/retire rounds lost nothing");
    check_eq(pipe.snapshot().pushed, total, "counters survived 200 recycles");
}

}  // namespace

int main() {
    test_producer_churn();
    test_retire_while_full();
    test_slot_reuse_is_allocation_free();
    return summary("test_lifetime");
}
