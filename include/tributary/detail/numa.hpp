#pragma once
//
// Explicit NUMA node binding for the pipeline's allocations.
//
// This is layered on top of first touch, not a replacement for it. First touch
// -- see alloc.hpp -- already places a ring on the node of the thread that
// registers it, and that is the right answer whenever a producer thread stays
// where it started. Explicit binding is for the cases first touch cannot reach:
// a registering thread that is not the thread that will push, a pool that
// migrates before the first event, or a deployment that wants every ring on
// one node regardless of who registered it.
//
// Best-effort, in the same sense as thread.hpp: every entry point here reports
// failure rather than throwing, and a failed binding falls back to the ordinary
// over-aligned allocation. A pipeline that refuses to start because a node is
// offline is worse than one that starts on the wrong node -- placement is a
// performance property, and no correctness argument in this library rides on
// it. The pipeline reports the failure once, at construction, through
// options::on_error, so "it silently ran unbound" is not a state you can end up
// in without being told.
//
// The node is ambient (a thread-local set by scoped_node) rather than a
// parameter. That is deliberate and worth the explanation, because a parameter
// is the obvious shape:
//
//   The allocation that matters most is fixed_channel::storage_, and it happens
//   *inside* the channel's constructor. Reaching it with a parameter means
//   widening the Channel contract from `(std::size_t capacity)` to
//   `(std::size_t capacity, int node)` -- and that contract is about to be
//   written down as a `channel_for` concept in Phase C. NUMA placement is a
//   property of the machine, not of a channel type; encoding it into the
//   concept every future channel must satisfy is the wrong seam. See
//   docs/PLAN.md, "The `Channel` parameter is not yet the seam this needs".
//
// Ambient state is only safe here because the scope is exactly one allocation
// on the calling thread: scoped_node sets a thread-local, the allocation runs,
// and the destructor restores the previous value. Nothing observes it across a
// thread boundary and nothing keeps it set past the call.

#include "../traits.hpp"

#include <cstddef>
#include <new>

#if defined(_WIN32)
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif
#  include <windows.h>
#elif defined(__linux__)
#  include <sys/mman.h>
#  include <sys/syscall.h>
#  include <unistd.h>
#endif

namespace tributary {
inline namespace TRIBUTARY_ABI {
namespace detail {
namespace numa {

// Matches options::numa_node's "leave it to first touch" value.
inline constexpr int any_node = -1;

// One unsigned long of node mask on Linux, and the same ceiling applied on
// Windows for consistency. Machines with more than 64 NUMA nodes exist; they
// need a multi-word mask and a processor-group-aware Windows path, which is the
// same boundary pin_to_cpu() draws at 64 CPUs and for the same reason.
inline constexpr int max_node = 63;

// Whether this build can bind at all. Compile-time: it says nothing about
// whether the running machine has more than one node.
[[nodiscard]] constexpr bool supported() noexcept {
#if defined(_WIN32) || (defined(__linux__) && defined(SYS_mbind))
    return true;
#else
    return false;
#endif
}

// Highest node number the running system reports, or -1 if that cannot be
// determined. Used only to reject an out-of-range options::numa_node up front,
// which is the difference between one message at construction and every ring
// silently landing unbound.
[[nodiscard]] inline int highest_node() noexcept {
#if defined(_WIN32)
    ULONG highest = 0;
    if (::GetNumaHighestNodeNumber(&highest) == 0) return -1;
    return static_cast<int>(highest);
#elif defined(__linux__) && defined(SYS_mbind)
    // No syscall reports the node count directly. get_mempolicy with
    // MPOL_F_MEMS_ALLOWED would, but parsing its mask to find the highest set
    // bit buys little: the binding itself already fails cleanly on a bad node,
    // and the caller falls back. Report "unknown" and let the range check be
    // the only up-front test.
    return -1;
#else
    return -1;
#endif
}

// A node this build could plausibly bind to. Deliberately permissive when
// highest_node() cannot tell us: refusing a node we merely failed to enumerate
// would turn a working configuration into a startup error.
[[nodiscard]] inline bool node_in_range(int node) noexcept {
    if (node < 0 || node > max_node) return false;
    const int highest = highest_node();
    return highest < 0 || node <= highest;
}

// Page/allocation granularity. The bound allocators below hand back memory
// aligned only to this, so an allocation needing more has to take the ordinary
// path. Every alignment this library asks for is a cache line (64 or 128) or
// alignof(T), all far below a page, so the check is a guard rather than a
// limitation in practice.
[[nodiscard]] inline std::size_t granularity() noexcept {
#if defined(_WIN32)
    SYSTEM_INFO si;
    ::GetSystemInfo(&si);
    return static_cast<std::size_t>(si.dwAllocationGranularity);
#elif defined(__linux__)
    const long page = ::sysconf(_SC_PAGESIZE);
    return page > 0 ? static_cast<std::size_t>(page) : std::size_t{4096};
#else
    return std::size_t{4096};
#endif
}

// Allocates `bytes` bound to `node`, or returns nullptr so the caller can fall
// back. Never throws, never partially succeeds: a non-null return is bound.
//
// The memory comes back zeroed on both platforms, which matters because the
// caller value-constructs over it -- the pages are faulted in by that pass, on
// the calling thread, under the policy set here.
// `align` is an align_val_t rather than a size_t so it cannot be transposed
// with `bytes` at a call site -- the two would otherwise be adjacent parameters
// of the same type, and swapping them compiles and misbehaves quietly.
[[nodiscard]] inline void* allocate(std::size_t bytes, std::align_val_t align, int node) noexcept {
    if (bytes == 0 || !node_in_range(node)) return nullptr;
    if (static_cast<std::size_t>(align) > granularity()) return nullptr;

#if defined(_WIN32)
    // VirtualAllocExNuma commits on the requested node; the pages themselves
    // are still faulted in lazily, which is what the caller's first-touch pass
    // does immediately afterwards.
    void* p = ::VirtualAllocExNuma(::GetCurrentProcess(), nullptr, bytes, MEM_RESERVE | MEM_COMMIT,
                                   PAGE_READWRITE, static_cast<DWORD>(node));
    return p;
#elif defined(__linux__) && defined(SYS_mbind)
    // Constants from <linux/mempolicy.h>, spelled out rather than included:
    // that header is kernel uapi and pulling it in would make a hermetic build
    // depend on which kernel headers happen to be installed.
    constexpr int mpol_bind = 2;

    void* p = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (p == MAP_FAILED) return nullptr;

    const unsigned long mask = 1UL << static_cast<unsigned>(node);
    // maxnode counts bits in the mask, not bytes.
    //
    // flags is 0 deliberately. MPOL_MF_MOVE would migrate pages that are
    // already faulted in, and MPOL_MF_STRICT would fail if any were on the
    // wrong node -- both are about *existing* pages, and this mapping has none
    // yet. Setting the policy on an untouched range is the whole point: the
    // caller's first-touch pass then faults every page in under it.
    const long rc = ::syscall(SYS_mbind, p, static_cast<unsigned long>(bytes), mpol_bind, &mask,
                              static_cast<unsigned long>(8 * sizeof(unsigned long)), 0U);
    if (rc != 0) {
        ::munmap(p, bytes);
        return nullptr;
    }
    return p;
#else
    (void)bytes;
    (void)align;
    (void)node;
    return nullptr;
#endif
}

// Releases memory from allocate(). `bytes` must be the same value that was
// passed in -- munmap is sized, unlike free -- which is why the callers in
// alloc.hpp carry the byte count alongside the pointer.
inline void deallocate(void* p, std::size_t bytes) noexcept {
    if (p == nullptr) return;
#if defined(_WIN32)
    (void)bytes;  // MEM_RELEASE takes the reservation as a whole
    ::VirtualFree(p, 0, MEM_RELEASE);
#elif defined(__linux__) && defined(SYS_mbind)
    ::munmap(p, bytes);
#else
    (void)p;
    (void)bytes;
#endif
}

// --- the ambient node --------------------------------------------------------

// thread_local, not a plain static: registration runs concurrently on every
// producer thread, and a shared global here would be a data race on the one
// piece of mutable state this header has.
[[nodiscard]] inline int& ambient_node_slot() noexcept {
    static thread_local int node = any_node;
    return node;
}

// The node allocations on this thread should bind to, or any_node.
[[nodiscard]] inline int current_node() noexcept {
    return ambient_node_slot();
}

// Sets the ambient node for the calling thread and restores the previous value
// on destruction. Nesting is honest rather than merely tolerated: the inner
// scope wins while it is open, and the outer one resumes after.
class scoped_node {
public:
    explicit scoped_node(int node) noexcept
        : slot_(ambient_node_slot()), previous_(ambient_node_slot()) {
        slot_ = node;
    }

    ~scoped_node() { slot_ = previous_; }

    scoped_node(const scoped_node&) = delete;
    scoped_node& operator=(const scoped_node&) = delete;
    scoped_node(scoped_node&&) = delete;
    scoped_node& operator=(scoped_node&&) = delete;

private:
    int& slot_;
    int previous_;
};

}  // namespace numa
}  // namespace detail
}  // namespace TRIBUTARY_ABI
}  // namespace tributary
