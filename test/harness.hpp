#pragma once
//
// Minimal assertion harness. Deliberately no test framework: this library has
// no dependencies and neither do its tests, so a build is hermetic and CI needs
// nothing fetched.
//
// The exit code is derived from the failure count, so a violated invariant
// fails the build rather than merely the eye of whoever is reading the output.

#include <cstdio>
#include <cstdint>
#include <string>

namespace tributary::test {

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

// The record every test uses. 32 bytes, trivially copyable, no indirection --
// the shape the fixed-size path is designed for, and the shape the benchmark
// numbers are quoted against.
struct event {
    std::uint32_t producer_id;
    std::uint32_t seq;  // per-producer, strictly increasing
    std::int64_t stamp_ns;
    std::byte payload[16];
};

static_assert(sizeof(event) == 32, "keep the test record small: two per cache line");

inline event make_event(std::uint32_t id, std::uint32_t seq, std::int64_t stamp = 0) {
    event e{};
    e.producer_id = id;
    e.seq = seq;
    e.stamp_ns = stamp;
    return e;
}

}  // namespace tributary::test
