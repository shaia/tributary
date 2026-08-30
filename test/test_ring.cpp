// The ring on its own: bounds, wrap, FIFO, and a two-thread handoff.
//
// Everything above this file assumes the ring is correct, so it gets tested in
// isolation first -- a pipeline-level ordering failure should never send you
// hunting through the bitmap when the bug is here.

#include "harness.hpp"

#include <tributary/fixed_channel.hpp>

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

using tributary::fixed_channel;
using namespace tributary::test;

namespace {

void test_bounds_and_fifo() {
    std::printf("bounds and FIFO\n");
    constexpr std::size_t kCap = 8;
    fixed_channel<event> ring(kCap);

    check_eq(ring.capacity(), kCap, "capacity is what was asked for");
    check(ring.empty_now(), "a fresh ring is empty");
    check_eq(ring.size_now(), 0, "a fresh ring has size 0");

    // Fill exactly to capacity. Free-running indices mean full and empty are
    // unambiguous without burning a slot, so all kCap must fit.
    for (std::size_t i = 0; i < kCap; ++i)
        check(ring.try_push(make_event(0, static_cast<std::uint32_t>(i + 1))),
              "push " + std::to_string(i) + " of a " + std::to_string(kCap) + "-slot ring");
    check(!ring.try_push(make_event(0, 999)), "the ring refuses when full, rather than growing");
    check_eq(ring.size_now(), kCap, "a full ring reports its full capacity");
    check_eq(ring.high_water(), kCap, "high water reached capacity and no further");

    event out[kCap]{};
    check_eq(ring.pop_batch(out, kCap), kCap, "one batch drains the whole ring");
    for (std::size_t i = 0; i < kCap; ++i)
        check_eq(out[i].seq, i + 1, "record " + std::to_string(i) + " came out in order");
    check(ring.empty_now(), "the ring is empty after a full drain");
}

void test_wrap() {
    std::printf("\nwrap\n");
    constexpr std::size_t kCap = 4;
    fixed_channel<event> ring(kCap);

    // Walk the write index all the way around several times, keeping the ring
    // partly full, so every push and pop straddles the mask boundary at some
    // point. pop_batch splits into two memcpys across the wrap; this is what
    // catches an off-by-one in that split.
    std::uint32_t next_push = 1;
    std::uint32_t next_pop = 1;
    for (int round = 0; round < 20; ++round) {
        for (int i = 0; i < 3; ++i)
            if (ring.try_push(make_event(0, next_push))) ++next_push;
        event out[2]{};
        const std::size_t n = ring.pop_batch(out, 2);
        for (std::size_t k = 0; k < n; ++k) {
            if (out[k].seq != next_pop) {
                check(false, "wrap kept FIFO order at round " + std::to_string(round));
                return;
            }
            ++next_pop;
        }
    }
    check(next_pop > 20, "the wrap test actually moved records (" + std::to_string(next_pop - 1) +
                             " through)");
    check(true, "wrap preserved FIFO order across " + std::to_string(next_push - 1) + " records");
}

void test_partial_drain() {
    std::printf("\npartial drain\n");
    fixed_channel<event> ring(16);
    for (std::uint32_t i = 1; i <= 10; ++i) ring.try_push(make_event(0, i));

    event out[4]{};
    check_eq(ring.pop_batch(out, 4), 4, "pop_batch honours max_items");
    check_eq(out[0].seq, 1, "partial drain starts at the head");
    check_eq(ring.size_now(), 6, "the rest stayed in the ring");
    check_eq(ring.pop_batch(out, 0), 0, "a zero-sized pop is a no-op");
    check_eq(ring.size_now(), 6, "a zero-sized pop did not advance the head");
}

// One producer, one consumer, no locks. The assertion that matters is not
// throughput but that nothing is lost, duplicated, or reordered.
void test_two_thread_handoff() {
    std::printf("\ntwo-thread handoff\n");
    constexpr std::uint32_t kRecords = 2'000'000;
    fixed_channel<event> ring(1024);

    std::atomic<std::uint64_t> received{0};
    std::atomic<std::uint64_t> order_violations{0};
    std::atomic<bool> done{false};

    std::thread consumer([&] {
        std::vector<event> buf(256);
        std::uint32_t expect = 1;
        while (true) {
            const std::size_t n = ring.pop_batch(buf.data(), buf.size());
            if (n == 0) {
                if (done.load(std::memory_order_acquire) && ring.empty_now()) break;
                TRIBUTARY_PAUSE();
                continue;
            }
            for (std::size_t i = 0; i < n; ++i) {
                if (buf[i].seq != expect) order_violations.fetch_add(1, std::memory_order_relaxed);
                ++expect;
            }
            received.fetch_add(n, std::memory_order_relaxed);
        }
    });

    for (std::uint32_t s = 1; s <= kRecords; ++s) {
        // Retry rather than drop: this test is about the ring never losing or
        // reordering, so the producer waits for room instead of exercising a
        // drop policy that belongs one layer up.
        while (!ring.try_push(make_event(0, s))) TRIBUTARY_PAUSE();
    }
    done.store(true, std::memory_order_release);
    consumer.join();

    check_eq(received.load(), kRecords, "every record crossed the ring exactly once");
    check_eq(order_violations.load(), 0, "records arrived in strict FIFO order");
    check(ring.high_water() <= ring.capacity(), "depth never exceeded capacity");
}

}  // namespace

int main() {
    test_bounds_and_fifo();
    test_wrap();
    test_partial_drain();
    test_two_thread_handoff();
    return summary("test_ring");
}
