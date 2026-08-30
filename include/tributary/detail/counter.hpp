#pragma once
//
// Relaxed statistics counters that vanish when Traits::collect_stats is false.
//
// The distinction that matters here is single-writer vs multi-writer. Most of
// these counters have exactly one thread writing them, and for those a read-
// modify-write is pure waste: `lock xadd` is ~15-20 cycles against ~1 for a
// plain store, and on a push path measured at 18-29 ns that is not noise.
//
// Loads stay atomic and relaxed regardless, because an observer thread reads
// them for stats and a non-atomic read alongside a write is a data race.

#include "../traits.hpp"

#include <atomic>
#include <cstdint>

namespace tributary {
inline namespace TRIBUTARY_ABI {
namespace detail {

template <bool Enabled>
class counter {
public:
    // Multi-writer. Only for counters more than one thread increments.
    void add(std::uint64_t n = 1) noexcept { v_.fetch_add(n, std::memory_order_relaxed); }

    // Single-writer. The caller guarantees no other thread stores to this
    // counter for the duration of its ownership; other threads may load it.
    void bump(std::uint64_t n = 1) noexcept {
        v_.store(v_.load(std::memory_order_relaxed) + n, std::memory_order_relaxed);
    }

    // Single-writer running maximum.
    void raise_to(std::uint64_t n) noexcept {
        if (n > v_.load(std::memory_order_relaxed)) v_.store(n, std::memory_order_relaxed);
    }

    std::uint64_t get() const noexcept { return v_.load(std::memory_order_relaxed); }
    void store(std::uint64_t n) noexcept { v_.store(n, std::memory_order_relaxed); }

    // Used when a producer slot is recycled: hand the departing producer's total
    // to the lifetime accumulator so a reused slot does not erase its
    // predecessor's history.
    std::uint64_t take() noexcept { return v_.exchange(0, std::memory_order_relaxed); }

private:
    std::atomic<std::uint64_t> v_{0};
};

// Empty specialization: with TRIBUTARY_NO_UNIQUE_ADDRESS at the use site this
// occupies no storage, so disabling stats genuinely shrinks the hot cache line
// rather than merely skipping the writes.
// The signatures must mirror counter<true> exactly, including constness, so
// call sites are identical -- hence the suppressions rather than the "fixes".
// NOLINTBEGIN(readability-convert-member-functions-to-static)
template <>
class counter<false> {
public:
    void add(std::uint64_t /*n*/ = 1) noexcept {}
    void bump(std::uint64_t /*n*/ = 1) noexcept {}
    void raise_to(std::uint64_t /*n*/) noexcept {}
    std::uint64_t get() const noexcept { return 0; }
    void store(std::uint64_t /*n*/) noexcept {}
    std::uint64_t take() noexcept { return 0; }
};
// NOLINTEND(readability-convert-member-functions-to-static)

}  // namespace detail
}  // namespace TRIBUTARY_ABI
}  // namespace tributary
