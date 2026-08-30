#pragma once
//
// What the pipeline says about itself.
//
// Every mechanism in this design can fail silently, so every mechanism is
// counted. The two that matter most are probes_per_pass (is the scan actually
// narrower than the producer count?) and bitmap_writes (is notification
// coalescing actually holding?). Both were invisible until the counters
// existed, and one of them was hiding a bug.

#include "traits.hpp"

#include <cstdint>

namespace tributary {
inline namespace TRIBUTARY_ABI {

struct stats {
    // Accounting. pushed + dropped == what callers offered, exactly. A drop is
    // never silent: a pipeline that quietly discards telemetry is worse than
    // one that fails loudly, because the loss is invisible precisely when the
    // system is in trouble and you most need the data.
    std::uint64_t pushed = 0;
    std::uint64_t dropped = 0;
    std::uint64_t consumed = 0;

    // Deepest any ring ever got. Proves memory stayed bounded.
    std::uint64_t high_water = 0;

    // Scan work. rings_probed / passes is the scan width the active bitmap
    // exists to shrink; rings_empty says how much of that probing found
    // nothing.
    std::uint64_t rings_probed = 0;
    std::uint64_t rings_empty = 0;
    std::uint64_t passes = 0;

    // Wakeup traffic. bitmap_writes counts writes to the shared bitmap line
    // over the run: coalescing is working only if this stays negligible next to
    // the event count. A notify per push would be a syscall per event, which is
    // categorically worse than the polling it replaces.
    std::uint64_t bitmap_writes = 0;
    std::uint64_t notifies = 0;
    std::uint64_t parks = 0;

    // Times a sink accepted less than it was offered. Rising means the far side
    // is the bottleneck and the rings are about to start dropping.
    std::uint64_t sink_backpressure = 0;

    // Registrations that found no free slot. Non-zero means callers are
    // silently unable to produce at all, which no other counter would show.
    std::uint64_t registration_failures = 0;

    double probes_per_pass() const noexcept {
        return passes ? static_cast<double>(rings_probed) / static_cast<double>(passes) : 0.0;
    }
    double empty_probe_ratio() const noexcept {
        return rings_probed ? static_cast<double>(rings_empty) / static_cast<double>(rings_probed)
                            : 0.0;
    }
    double drop_fraction() const noexcept {
        const std::uint64_t offered = pushed + dropped;
        return offered ? static_cast<double>(dropped) / static_cast<double>(offered) : 0.0;
    }
};

// Per-slot breakdown. A single global drop count tells you the pipeline is
// losing data; this tells you which producer overran, which is the question you
// actually have.
struct producer_stats {
    std::uint32_t id = 0;
    std::uint32_t shard = 0;
    bool active = false;
    bool retiring = false;
    std::uint64_t pushed = 0;
    std::uint64_t dropped = 0;
    std::uint64_t high_water = 0;
    std::uint64_t depth = 0;  // current occupancy, sampled
};

}  // namespace TRIBUTARY_ABI
}  // namespace tributary
