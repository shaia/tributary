#pragma once
//
// Over-aligned storage with a first-touch pass.
//
// First touch is the portable half of the NUMA story: both Linux's default
// policy and Windows place a page on the node of the thread that first writes
// it. Every allocation here is therefore made by the thread that will own the
// memory -- a producer registering its own ring -- and written once at
// construction so the placement is decided then rather than at some arbitrary
// later push. Explicit node binding is layered on top of this in numa.hpp; it
// does not replace it.
//
// Because binding is best-effort, an allocation here comes back one of two
// ways: bound (from numa::allocate, released with numa::deallocate) or ordinary
// (from ::operator new, released with ::operator delete). Which one it was is
// not recoverable from the pointer, so every owner below carries the flag. This
// is the entire reason aligned_deleter is stateful; getting it wrong frees
// mmap'd pages through operator delete, which is silent until it is not.

#include "../traits.hpp"
#include "numa.hpp"

#include <cstddef>
#include <memory>
#include <new>
#include <utility>

namespace tributary {
inline namespace TRIBUTARY_ABI {
namespace detail {

// The one place that decides between a node-bound allocation and an ordinary
// one. `bound` in the result is what the matching free needs; it is not a
// report of whether binding was *requested*, only of whether it happened.
struct raw_block {
    void* ptr;
    bool bound;
};

// Throws only what ::operator new throws. A failed *binding* is not a failure:
// it falls through to the ordinary path, because placement is a performance
// property and refusing to allocate over it would turn a tuning hint into an
// outage.
[[nodiscard]] inline raw_block raw_alloc(std::size_t bytes, std::align_val_t align) {
    if (const int node = numa::current_node(); node != numa::any_node) {
        if (void* bound = numa::allocate(bytes, align, node)) return {bound, true};
    }
    return {::operator new(bytes, align), false};
}

inline void raw_free(void* p, std::size_t bytes, std::align_val_t align, bool bound) noexcept {
    if (bound)
        numa::deallocate(p, bytes);
    else
        ::operator delete(p, align);
}

// Owning array of T with a caller-chosen alignment. Value-constructs every
// element, which both starts object lifetimes properly and touches every page.
template <class T, std::size_t Align>
class aligned_array {
public:
    aligned_array() = default;

    explicit aligned_array(std::size_t n) : n_(n) {
        if (n_ != 0) {
            const raw_block b = raw_alloc(n_ * sizeof(T), std::align_val_t{Align});
            p_ = static_cast<T*>(b.ptr);
            bound_ = b.bound;
            // Value-construction zeroes trivial types, so this is also the
            // first-touch pass. Doing it here means the pages are faulted in on
            // the registering thread rather than on some later hot-path push --
            // and, when the block is bound, faulted in under that policy.
            std::uninitialized_value_construct_n(p_, n_);
        }
    }

    aligned_array(aligned_array&& o) noexcept
        : n_(std::exchange(o.n_, 0)),
          p_(std::exchange(o.p_, nullptr)),
          bound_(std::exchange(o.bound_, false)) {}

    aligned_array& operator=(aligned_array&& o) noexcept {
        if (this != &o) {
            reset();
            n_ = std::exchange(o.n_, 0);
            p_ = std::exchange(o.p_, nullptr);
            // Must move with the pointer: freeing a bound block through
            // ::operator delete is undefined and survives testing.
            bound_ = std::exchange(o.bound_, false);
        }
        return *this;
    }

    aligned_array(const aligned_array&) = delete;
    aligned_array& operator=(const aligned_array&) = delete;
    ~aligned_array() { reset(); }

    T* data() const noexcept { return p_; }
    std::size_t size() const noexcept { return n_; }
    T& operator[](std::size_t i) const noexcept { return p_[i]; }

private:
    void reset() noexcept {
        if (p_) {
            std::destroy_n(p_, n_);
            raw_free(static_cast<void*>(p_), n_ * sizeof(T), std::align_val_t{Align}, bound_);
        }
        p_ = nullptr;
        n_ = 0;
        bound_ = false;
    }

    std::size_t n_{0};
    T* p_{nullptr};
    bool bound_{false};
};

// Single over-aligned object. C++17's aligned new would usually honour
// alignas(cache_line) on its own, but routing every such allocation through one
// place is what lets numa.hpp place node-bound pages without touching any call
// site -- which is exactly what raw_alloc now does.
//
// The bool is the deleter's whole state, and it is not optional: a bound block
// came from mmap or VirtualAllocExNuma and must go back the same way. sizeof(T)
// is a constant, so nothing else needs carrying, and aligned_ptr stays at two
// words. That matters -- one of these lives in producer_slot, whose layout
// decision 1 in docs/PLAN.md is about.
template <class T>
struct aligned_deleter {
    bool bound = false;

    void operator()(T* p) const noexcept {
        if (p) {
            p->~T();
            raw_free(static_cast<void*>(p), sizeof(T), std::align_val_t{alignof(T)}, bound);
        }
    }
};

template <class T>
using aligned_ptr = std::unique_ptr<T, aligned_deleter<T>>;

template <class T, class... Args>
aligned_ptr<T> make_aligned(Args&&... args) {
    const raw_block b = raw_alloc(sizeof(T), std::align_val_t{alignof(T)});
    try {
        return aligned_ptr<T>(new (b.ptr) T(std::forward<Args>(args)...), aligned_deleter<T>{b.bound});
    } catch (...) {
        raw_free(b.ptr, sizeof(T), std::align_val_t{alignof(T)}, b.bound);
        throw;
    }
}

}  // namespace detail
}  // namespace TRIBUTARY_ABI
}  // namespace tributary
