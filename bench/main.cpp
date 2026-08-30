// Runs the gate phases. The exit code is the verdict.

#include "harness.hpp"
#include "phases.hpp"

#include <cstdio>
#include <thread>

// A sanitized or debug binary produces numbers that are wrong by an order of
// magnitude while looking entirely plausible. Benchmarking one by accident is an
// easy mistake and a very confusing one, so say so loudly rather than letting
// the reader find out later, or never.
#if defined(__has_feature)
#  if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
#    define TRIBUTARY_BENCH_SANITIZED 1
#  endif
#endif
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#  define TRIBUTARY_BENCH_SANITIZED 1
#endif

int main() {
    std::printf("tributary bench -- Phase A gate\n");
    std::printf("hardware_concurrency: %u\n", std::thread::hardware_concurrency());

    bool suspect = false;
#ifndef NDEBUG
    std::printf("\n  *** WARNING: built without NDEBUG -- these are debug-build numbers ***\n");
    suspect = true;
#endif
#ifdef TRIBUTARY_BENCH_SANITIZED
    std::printf("\n  *** WARNING: sanitizer build -- throughput runs 10-40x low ***\n");
    suspect = true;
#endif
    if (suspect) {
        std::printf("  *** Reconfigure: -DCMAKE_BUILD_TYPE=Release, no TRIBUTARY_SANITIZER ***\n");
    }

    // Correctness first: if ordering or accounting is broken, the three phases
    // that follow are measuring the speed of something that does not work.
    tributary::bench::bench_correctness();
    tributary::bench::bench_throughput();
    tributary::bench::bench_backpressure();
    tributary::bench::bench_scan_ab();
    return tributary::bench::overall_verdict();
}
