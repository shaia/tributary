#pragma once
//
// Platform constants and the compile-time tuning surface.
//
// Everything here is baked into object layout, which is why it lives behind an
// ABI tag: two translation units compiled with different values of
// TRIBUTARY_CACHE_LINE disagree about the layout of a lock-free type, and that
// is memory corruption rather than a diagnostic. The inline namespace turns it
// into a link error instead.

#include <chrono>
#include <cstddef>

// --- cache line ------------------------------------------------------------
//
// Deliberately not std::hardware_destructive_interference_size: using it in a
// type's layout bakes a compiler-chosen constant into the ABI, and clang warns
// for exactly that reason. Override at configure time if your target disagrees.
#ifndef TRIBUTARY_CACHE_LINE
#  if defined(__APPLE__) && (defined(__aarch64__) || defined(__arm64__))
// Apple silicon pairs lines for prefetch; 128 is the effective destructive
// interference distance even though the line itself is 64.
#    define TRIBUTARY_CACHE_LINE 128
#  else
#    define TRIBUTARY_CACHE_LINE 64
#  endif
#endif

// --- ABI tag ---------------------------------------------------------------
#define TRIBUTARY_CAT_(a, b) a##b
#define TRIBUTARY_CAT(a, b) TRIBUTARY_CAT_(a, b)
#define TRIBUTARY_ABI TRIBUTARY_CAT(v1_cl, TRIBUTARY_CACHE_LINE)

// --- spin hint -------------------------------------------------------------
//
// Tells the core this is a spin-wait: de-pipelines the loop, cuts power, and on
// SMT yields issue slots to the sibling thread.
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#  include <immintrin.h>
#  define TRIBUTARY_PAUSE() _mm_pause()
#elif defined(__aarch64__) || defined(_M_ARM64)
#  if defined(_MSC_VER) && !defined(__clang__)
#    include <intrin.h>
#    define TRIBUTARY_PAUSE() __yield()
#  else
#    define TRIBUTARY_PAUSE() __asm__ __volatile__("yield" ::: "memory")
#  endif
#else
#  define TRIBUTARY_PAUSE() ((void)0)
#endif

// --- empty member elision --------------------------------------------------
//
// Lets a disabled stats counter occupy zero bytes rather than one, which
// matters because these sit inside carefully sized cache lines.
//
// Feature detection rather than a compiler test: clang targeting the MSVC ABI
// defines both _MSC_VER and __clang__, and *ignores* plain [[no_unique_address]]
// there because honouring it would break MSVC layout compatibility. The vendor
// spelling has to win whenever it is available.
#if defined(__has_cpp_attribute)
#  if __has_cpp_attribute(msvc::no_unique_address)
#    define TRIBUTARY_NO_UNIQUE_ADDRESS [[msvc::no_unique_address]]
#  elif __has_cpp_attribute(no_unique_address)
#    define TRIBUTARY_NO_UNIQUE_ADDRESS [[no_unique_address]]
#  else
#    define TRIBUTARY_NO_UNIQUE_ADDRESS
#  endif
#else
#  define TRIBUTARY_NO_UNIQUE_ADDRESS
#endif

namespace tributary {
inline namespace TRIBUTARY_ABI {

using clock_type = std::chrono::steady_clock;
using nanos      = std::chrono::nanoseconds;

// The compile-time knobs. Pass your own type with the same members to change
// them; every public template takes this as its last parameter.
struct default_traits {
    // Layout unit for false-sharing separation.
    static constexpr std::size_t cache_line = TRIBUTARY_CACHE_LINE;

    // When false, every counter in the library becomes an empty type and every
    // update becomes a no-op. Costs you all observability; buys back one
    // relaxed store per push. Measure before reaching for it — the counters are
    // how you find out the pipeline is dropping.
    static constexpr bool collect_stats = true;
};

// Stats compiled out. Useful for A/B measuring what observability costs.
struct no_stats_traits : default_traits {
    static constexpr bool collect_stats = false;
};

}  // namespace TRIBUTARY_ABI
}  // namespace tributary
