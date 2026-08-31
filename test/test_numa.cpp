// Explicit NUMA node binding.
//
// Placement is best-effort and no correctness argument in this library rides on
// it, so almost nothing here asserts that a binding *succeeded*. On a
// single-node machine, a kernel built without CONFIG_NUMA, or a platform with
// no binding call at all, falling back to first touch is the correct outcome
// rather than a failure -- and a test that demanded a successful bind would be
// asserting a property of the runner, not of this library.
//
// What is asserted is the half that genuinely is a correctness question: a
// bound block must go back the way it came. numa::allocate hands out mmap'd or
// VirtualAllocExNuma'd pages while ::operator new hands out ordinary ones, the
// two are indistinguishable from the pointer alone, and releasing one through
// the other's deallocator is undefined behaviour that a passing test will not
// notice on its own. That is what the flag on aligned_deleter and
// aligned_array exists for, and the end-to-end cases below are here to put the
// pairing under ASan and TSan in CI.

#include "harness.hpp"
#include "sinks.hpp"

#include <tributary/detail/numa.hpp>
#include <tributary/pipeline.hpp>

#include <cstdint>
#include <cstring>
#include <new>
#include <string>
#include <thread>
#include <vector>

using namespace tributary;
using namespace tributary::test;

namespace {

namespace numa = tributary::detail::numa;

void test_option_validation() {
    std::printf("option validation\n");

    // Only the shape is a configuration error. A node number that this machine
    // does not have is a fact about the machine, and degrades rather than
    // throwing -- see the fallback case below.
    options o;
    o.numa_node = -1;
    check(!o.validate().has_value(), "numa_node -1 (first touch) validates");
    o.numa_node = 0;
    check(!o.validate().has_value(), "numa_node 0 validates");
    o.numa_node = numa::max_node + 1;
    check(!o.validate().has_value(), "an out-of-range node is not a validation error");
    o.numa_node = -2;
    check(o.validate().has_value(), "a node below -1 is rejected");
}

void test_scoped_node_is_per_thread() {
    std::printf("ambient node\n");

    check_eq(numa::current_node(), numa::any_node, "the ambient node starts unset");
    {
        const numa::scoped_node outer{0};
        check_eq(numa::current_node(), 0, "scoped_node sets the ambient node");
        {
            const numa::scoped_node inner{1};
            check_eq(numa::current_node(), 1, "a nested scope wins while it is open");
        }
        check_eq(numa::current_node(), 0, "the outer scope resumes after the inner one closes");

        // This is the whole reason the slot is thread_local rather than a plain
        // static. register_producer() runs concurrently on every producer
        // thread and each one wraps its own ring allocation; a shared global
        // here would be both a data race and a way for one thread's placement
        // to land on another thread's memory.
        int seen = 99;
        std::thread t([&seen] { seen = numa::current_node(); });
        t.join();
        check_eq(seen, numa::any_node, "another thread does not observe this thread's node");
    }
    check_eq(numa::current_node(), numa::any_node, "the ambient node is restored on scope exit");
}

void test_allocate_round_trip() {
    std::printf("bound allocation\n");

    constexpr std::size_t kBytes = 64 * 1024;
    constexpr auto kAlign = std::align_val_t{64};

    std::printf("  (this build %s bind memory)\n", numa::supported() ? "can" : "cannot");

    if (void* p = numa::allocate(kBytes, kAlign, 0)) {
        // Write the whole range rather than trusting the mapping: a range that
        // is reserved but not committed, or bound to a node with no memory,
        // faults here where it says so instead of inside a producer's ring.
        std::memset(p, 0xA5, kBytes);
        const auto* bytes = static_cast<const unsigned char*>(p);
        check(bytes[0] == 0xA5 && bytes[kBytes - 1] == 0xA5,
              "bound memory is writable across its whole length");
        check((reinterpret_cast<std::uintptr_t>(p) % 64) == 0,
              "bound memory meets the requested alignment");
        numa::deallocate(p, kBytes);
        check(true, "bound memory released through numa::deallocate without fault");
    } else {
        // Not a failure. It is the documented outcome on a machine or kernel
        // that cannot bind, and the pipeline cases below still have to work.
        check(true, "node 0 could not be bound here; first-touch fallback is the documented path");
    }

    check(numa::allocate(kBytes, kAlign, numa::max_node + 1) == nullptr,
          "a node above the supported ceiling refuses rather than binding somewhere else");
    check(numa::allocate(0, kAlign, 0) == nullptr, "a zero-byte request refuses");
    check(numa::allocate(kBytes, kAlign, numa::any_node) == nullptr,
          "any_node refuses: placement there is first touch's job, not this function's");
    const auto coarse = std::align_val_t{numa::granularity() * 2};
    check(numa::allocate(kBytes, coarse, 0) == nullptr,
          "an alignment coarser than the granularity refuses rather than under-aligning");
}

// Runs a real pipeline end to end and returns nothing but its own assertions.
// The point is not the record count -- other suites cover delivery -- it is
// that every allocation the pipeline made (the slot array, each shard_state,
// and one ring per producer) is released through the deallocator that matches
// how it was obtained. Under ASan a mismatch is an immediate report; without
// it, this is the case that would corrupt the heap quietly.
void run_pipeline_with_node(int node, const std::string& label) {
    constexpr std::uint32_t kProducers = 4;
    constexpr std::uint32_t kPer = 5'000;

    int error_calls = 0;
    options opt;
    opt.max_producers = kProducers;
    opt.numa_node = node;
    opt.full = full_policy::spin_then_drop;
    opt.push_spin = 100'000;  // this case is about allocation, not about drops
    opt.on_error = [&error_calls](std::string_view) { ++error_calls; };

    pipeline<event> pipe(opt);
    ordering_sink sink(kProducers);
    pipe.start(sink);

    // Hold every producer until all of them exist: without the gate, threads
    // retire and their slots are recycled into threads that have not started,
    // which reads as an ordering violation that is nothing of the sort. See
    // harness.hpp.
    start_gate gate(kProducers);
    std::vector<std::thread> ts;
    ts.reserve(kProducers);
    for (std::uint32_t p = 0; p < kProducers; ++p)
        ts.emplace_back([&pipe, &gate] {
            auto h = pipe.register_producer();
            gate.arrive_and_wait();
            if (!h) return;
            for (std::uint32_t s = 1; s <= kPer; ++s) h.push(make_event(h.id(), s));
            h.release();
        });
    for (auto& t : ts) t.join();

    pipe.stop(stop_mode::drain);

    check_eq(sink.records(), kProducers * kPer, label + ": a drain stop delivered every record");
    check_eq(sink.violations(), 0, label + ": per-producer order preserved");

    // What on_error is expected to have said depends on *why* the node is
    // unusable, not merely on whether it is. A node this build cannot bind but
    // that is in range degrades silently by design -- the allocation simply
    // falls back -- so only the two ends are pinned down here.
    if (node == numa::any_node) {
        check_eq(error_calls, 0, label + ": first touch reports nothing");
    } else if (!numa::node_in_range(node)) {
        // Exactly once, at construction. Reporting per allocation would mean
        // one message per producer for a single misconfiguration.
        check_eq(error_calls, 1, label + ": an unusable node is reported once, at construction");
    }
}

void test_pipeline_first_touch() {
    std::printf("pipeline, first touch\n");
    run_pipeline_with_node(numa::any_node, "first touch");
}

void test_pipeline_bound_to_node_zero() {
    std::printf("pipeline, bound to node 0\n");
    // Node 0 exists wherever NUMA does, so on a machine that can bind at all
    // this is the case that actually exercises the bound allocate/free pair
    // through every owner in the pipeline.
    run_pipeline_with_node(0, "node 0");
}

void test_pipeline_falls_back_on_bad_node() {
    std::printf("pipeline, unusable node\n");
    // Above the ceiling on every platform, so node_in_range() rejects it
    // without asking the machine and the expectation is the same everywhere:
    // one report, then first touch, then a pipeline that works normally.
    run_pipeline_with_node(numa::max_node + 1, "unusable node");
}

}  // namespace

int main() {
    test_option_validation();
    test_scoped_node_is_per_thread();
    test_allocate_round_trip();
    test_pipeline_first_touch();
    test_pipeline_bound_to_node_zero();
    test_pipeline_falls_back_on_bad_node();
    return summary("test_numa");
}
