#pragma once
//
// Minimal assertion harness. Deliberately no test framework: this library has
// no dependencies and neither do its tests, so a build is hermetic and CI needs
// nothing fetched.
//
// The exit code is derived from the failure count, so a violated invariant
// fails the build rather than merely the eye of whoever is reading the output.

#include <atomic>
#include <cstdio>
#include <cstdint>
#include <string>
#include <thread>

namespace tributary::test {

// A start gate for producer threads. Hold every handle until the last one
// exists.
//
// Without it a test is quietly not testing what it says. Threads spawned in a
// loop retire roughly in the order they started, the consumer publishes their
// slots free, and registration -- which takes the first free slot -- hands
// those same slots straight back to threads that have not started yet. The
// symptom is an ordering "violation" that is nothing of the sort: a recycled
// slot id restarts its per-producer sequence at 1, which is exactly what
// ordering_sink is built to flag.
//
// Both places this bit were found the same way and neither was visible here.
// On a 32-core developer machine sixteen producers all start before any
// finishes, so the race never opens; the first CI run, on a 4-core runner,
// failed test_pipeline immediately. That is why this is a shared type rather
// than a remembered convention.
//
// Slot recycling is worth testing deliberately -- test_lifetime.cpp does -- but
// not by accident, and not in the tests whose premise is N live producers.
class start_gate {
public:
    explicit start_gate(std::uint32_t parties) noexcept : parties_(parties) {}

    // Arrive unconditionally, whether or not registration succeeded: a thread
    // that returns early without arriving wedges every other thread here
    // instead of letting a check fail and say why.
    void arrive_and_wait() noexcept {
        arrived_.fetch_add(1, std::memory_order_release);
        while (arrived_.load(std::memory_order_acquire) < parties_) std::this_thread::yield();
    }

private:
    std::uint32_t parties_;
    std::atomic<std::uint32_t> arrived_{0};
};

inline int g_failures = 0;
inline int g_checks = 0;

inline void check(bool cond, const std::string& what) {
    ++g_checks;
    if (cond) {
        std::printf("  ok    %s\n", what.c_str());
        return;
    }
    ++g_failures;
    std::printf("  FAIL  %s\n", what.c_str());
}

template <class A, class B>
void check_eq(A a, B b, const std::string& what) {
    const auto ua = static_cast<std::uint64_t>(a);
    const auto ub = static_cast<std::uint64_t>(b);
    ++g_checks;
    if (ua == ub) {
        std::printf("  ok    %s\n", what.c_str());
        return;
    }
    ++g_failures;
    std::printf("  FAIL  %s  (%llu != %llu)\n", what.c_str(), static_cast<unsigned long long>(ua),
                static_cast<unsigned long long>(ub));
}

inline int summary(const char* suite) {
    std::printf("\n%s: %d checks, %d failed\n", suite, g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}

// The event every test uses. 32 bytes, trivially copyable, no indirection --
// the shape the fixed-size path is designed for, and the shape the benchmark
// numbers are quoted against.
struct event {
    std::uint32_t producer_id;
    std::uint32_t seq;  // per-producer, strictly increasing
    std::int64_t stamp_ns;
    std::byte payload[16];
};

static_assert(sizeof(event) == 32, "keep the test event small: two per cache line");

inline event make_event(std::uint32_t id, std::uint32_t seq, std::int64_t stamp = 0) {
    event e{};
    e.producer_id = id;
    e.seq = seq;
    e.stamp_ns = stamp;
    return e;
}

}  // namespace tributary::test
