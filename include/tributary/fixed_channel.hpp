#pragma once
//
// Bounded single-producer / single-consumer ring of fixed-size events.
//
// This is where the memory ordering lives. Two release/acquire pairs, one per
// direction, and no atomic read-modify-write on either side.
//
// Layout is as much of the design as the ordering is. Three groups:
//
//   producer line   tail_, the producer's cached view of head_, its private
//                   copy of the slot pointer and mask, and its own counters
//   consumer line   head_, its cached view of tail_, its own pointer and mask
//   storage         a separate over-aligned allocation
//
// The slot pointer and mask are immutable after construction and needed by both
// sides, so each side gets its own copy rather than sharing one. Duplicating 16
// bytes removes a third line from every operation; a shared read-only line
// would be free to read, but it would be a line, and both hot paths already fit
// in the lines they own.
//
// The counters live in the producer's line deliberately. The producer already
// takes that line exclusive on every publish, so a relaxed store next to tail_
// costs nothing. Putting them anywhere the consumer reads -- next to the slot
// state, say -- turns every push into an invalidation of a line the consumer
// polls, which is the exact false sharing this layout exists to prevent.

#include "detail/alloc.hpp"
#include "detail/counter.hpp"
#include "traits.hpp"

#include <algorithm>
#include <atomic>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <type_traits>

namespace tributary {
inline namespace TRIBUTARY_ABI {

template <class T, class Traits = default_traits>
class alignas(Traits::cache_line) fixed_channel {
    static_assert(std::is_trivially_copyable_v<T>,
                  "events travel by value through preallocated storage, so a push must be a "
                  "memcpy: no allocation, no indirection, no shared ownership on the hot path");
    static_assert(std::is_default_constructible_v<T>,
                  "ring storage is value-constructed once at registration");

public:
    using value_type = T;
    using batch_type = std::span<const T>;

    static constexpr std::size_t cache_line = Traits::cache_line;

    // capacity must be a power of two and at least 2; options::validate()
    // enforces this before any channel is built.
    explicit fixed_channel(std::size_t capacity) : storage_(capacity) {
        assert(capacity >= 2 && std::has_single_bit(capacity) &&
               "capacity must be a power of two so the mask replaces a modulo");
        p_slots_ = storage_.data();
        p_mask_ = capacity - 1;
        c_slots_ = storage_.data();
        c_mask_ = capacity - 1;
    }

    fixed_channel(const fixed_channel&) = delete;
    fixed_channel& operator=(const fixed_channel&) = delete;

    // --- producer side -----------------------------------------------------

    bool try_push(const T& value) noexcept {
        const std::uint64_t t = tail_.load(std::memory_order_relaxed);  // sole writer
        const std::uint64_t cap = p_mask_ + 1;
        if (t - cached_head_ == cap) {
            // Believed full. Only now pay for the consumer's line; acquire
            // pairs with the consumer's release store of head_, so a slot it
            // has finished reading is safe for us to overwrite.
            cached_head_ = head_.load(std::memory_order_acquire);
            if (t - cached_head_ == cap) return false;
        }
        p_slots_[t & p_mask_] = value;
        // Release: publishes the slot write to whoever acquires tail_.
        tail_.store(t + 1, std::memory_order_release);

        // After the publish, not before: these are relaxed and observer-only,
        // so there is no reason to delay the store the consumer is waiting on.
        // Both land in the line this thread just took exclusive.
        pushed_.bump();
        high_water_.raise_to(t + 1 - cached_head_);
        return true;
    }

    // Recorded by the pipeline once the full policy has actually given up, not
    // once per failed attempt -- a bounded spin must not count as N drops.
    void note_drop(std::uint64_t n = 1) noexcept { dropped_.bump(n); }

    // --- consumer side -----------------------------------------------------

    // Copies out at most max_items and frees their slots with a single release
    // store, so a 512-event drain costs one cross-core write, not 512.
    std::size_t pop_batch(T* out, std::size_t max_items) noexcept {
        if (max_items == 0) return 0;
        const std::uint64_t h = head_.load(std::memory_order_relaxed);  // sole writer
        if (h == cached_tail_) {
            cached_tail_ = tail_.load(std::memory_order_acquire);
            if (h == cached_tail_) return 0;
        }
        const std::size_t avail = static_cast<std::size_t>(cached_tail_ - h);
        const std::size_t n = std::min(max_items, avail);

        // Two contiguous runs rather than a masked element loop: T is trivially
        // copyable and the slots are contiguous, so this is two memcpys.
        const std::size_t cap = static_cast<std::size_t>(c_mask_) + 1;
        const std::size_t off = static_cast<std::size_t>(h & c_mask_);
        const std::size_t first = std::min(n, cap - off);
        std::memcpy(out, c_slots_ + off, first * sizeof(T));
        if (n > first) std::memcpy(out + first, c_slots_, (n - first) * sizeof(T));

        // Release: the slot reads above must not sink past this store, or the
        // producer could overwrite a slot we are still reading. This is the
        // pairing people forget -- it is not just a liveness hint.
        head_.store(h + n, std::memory_order_release);
        return n;
    }

    // Authoritative emptiness check for the consumer. Refreshes the cache, so
    // it is the right call to use after clearing an active-bitmap bit.
    bool empty_now() noexcept {
        cached_tail_ = tail_.load(std::memory_order_acquire);
        return head_.load(std::memory_order_relaxed) == cached_tail_;
    }

    // --- observer side -----------------------------------------------------

    // Safe from a thread that is neither the producer nor the consumer: it
    // reads only the two atomics, never the cached copies, which are private to
    // their owning thread and racy to read from anywhere else.
    std::uint64_t size_now() const noexcept {
        const std::uint64_t t = tail_.load(std::memory_order_acquire);
        const std::uint64_t h = head_.load(std::memory_order_acquire);
        return t - h;
    }

    std::size_t capacity() const noexcept { return static_cast<std::size_t>(c_mask_) + 1; }

    // Reading these pulls the producer's line to Shared, so the producer pays a
    // re-acquire on its next publish. Fine at monitoring frequencies; do not
    // put a stats scrape in a hot loop.
    std::uint64_t pushed() const noexcept { return pushed_.get(); }
    std::uint64_t dropped() const noexcept { return dropped_.get(); }
    std::uint64_t high_water() const noexcept { return high_water_.get(); }

    // Slot recycling: fold a departing producer's totals into lifetime figures
    // and reset, so a reused slot does not erase its predecessor's history.
    std::uint64_t take_pushed() noexcept { return pushed_.take(); }
    std::uint64_t take_dropped() noexcept { return dropped_.take(); }
    void reset_stats() noexcept {
        pushed_.store(0);
        dropped_.store(0);
        high_water_.store(0);
    }

private:
    using counter_t = detail::counter<Traits::collect_stats>;

    // --- producer line -----------------------------------------------------
    alignas(cache_line) std::atomic<std::uint64_t> tail_{0};
    std::uint64_t cached_head_{0};  // producer-private, non-atomic on purpose
    T* p_slots_{nullptr};           // immutable copy, so the consumer's line is never read
    std::uint64_t p_mask_{0};
    TRIBUTARY_NO_UNIQUE_ADDRESS counter_t pushed_;
    TRIBUTARY_NO_UNIQUE_ADDRESS counter_t dropped_;
    TRIBUTARY_NO_UNIQUE_ADDRESS counter_t high_water_;

    // --- consumer line -----------------------------------------------------
    alignas(cache_line) std::atomic<std::uint64_t> head_{0};
    std::uint64_t cached_tail_{0};  // consumer-private
    T* c_slots_{nullptr};
    std::uint64_t c_mask_{0};

    // --- cold --------------------------------------------------------------
    alignas(cache_line) detail::aligned_array<T, cache_line> storage_;
};

}  // namespace TRIBUTARY_ABI
}  // namespace tributary
