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

#include "../traits.hpp"

#include <cstddef>
#include <memory>
#include <new>
#include <utility>

namespace tributary {
inline namespace TRIBUTARY_ABI {
namespace detail {

// Owning array of T with a caller-chosen alignment. Value-constructs every
// element, which both starts object lifetimes properly and touches every page.
template <class T, std::size_t Align>
class aligned_array {
public:
    aligned_array() = default;

    explicit aligned_array(std::size_t n)
        : n_(n),
          p_(n == 0 ? nullptr
                    : static_cast<T*>(::operator new(n * sizeof(T), std::align_val_t{Align}))) {
        if (p_) {
            // Value-construction zeroes trivial types, so this is also the
            // first-touch pass. Doing it here means the pages are faulted in on
            // the registering thread rather than on some later hot-path push.
            std::uninitialized_value_construct_n(p_, n_);
        }
    }

    aligned_array(aligned_array&& o) noexcept
        : n_(std::exchange(o.n_, 0)), p_(std::exchange(o.p_, nullptr)) {}

    aligned_array& operator=(aligned_array&& o) noexcept {
        if (this != &o) {
            reset();
            n_ = std::exchange(o.n_, 0);
            p_ = std::exchange(o.p_, nullptr);
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
            ::operator delete(p_, std::align_val_t{Align});
        }
        p_ = nullptr;
        n_ = 0;
    }

    std::size_t n_{0};
    T* p_{nullptr};
};

// Single over-aligned object. C++17's aligned new would usually honour
// alignas(cache_line) on its own, but routing every such allocation through one
// place is what lets numa.hpp swap in node-bound pages later without touching
// any call site.
template <class T>
struct aligned_deleter {
    void operator()(T* p) const noexcept {
        if (p) {
            p->~T();
            ::operator delete(static_cast<void*>(p), std::align_val_t{alignof(T)});
        }
    }
};

template <class T>
using aligned_ptr = std::unique_ptr<T, aligned_deleter<T>>;

template <class T, class... Args>
aligned_ptr<T> make_aligned(Args&&... args) {
    void* raw = ::operator new(sizeof(T), std::align_val_t{alignof(T)});
    try {
        return aligned_ptr<T>(new (raw) T(std::forward<Args>(args)...));
    } catch (...) {
        ::operator delete(raw, std::align_val_t{alignof(T)});
        throw;
    }
}

}  // namespace detail
}  // namespace TRIBUTARY_ABI
}  // namespace tributary
