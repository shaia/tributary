#pragma once
//
// The runtime tuning surface, and the policies that make overload behaviour
// explicit rather than emergent.

#include "traits.hpp"

#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace tributary {
inline namespace TRIBUTARY_ABI {

// What a producer does when its ring is full.
//
// Both terminate. "Producers must not block indefinitely" is the requirement
// this library exists to satisfy, so the only question is how long we try,
// never whether we eventually give up.
//
// Overwrite-oldest is deliberately absent: it cannot be done safely in a plain
// SPSC ring, because the producer would overwrite a slot the consumer may be
// mid-read. It needs sequence-numbered slots and reader validation, which is a
// different data structure -- see DESIGN.md.
enum class full_policy : std::uint8_t {
    drop_newest,    // fail immediately, count it. Right when staleness beats stalling.
    spin_then_drop, // bounded spin first, for bursty traffic the consumer will absorb.
};

// drain is lossless but bounded by a deadline; abort discards and returns fast.
enum class stop_mode : std::uint8_t { drain, abort };

// How the consumer finds rings with work in them.
//
// This selects the SCAN only. The wakeup handshake -- producers setting their
// bit, the consumer clearing it on the way to sleep -- runs under both, so
// switching this varies exactly one thing: how many rings the consumer inspects
// per pass. That is what makes the two comparable in a benchmark; a version
// that also removed the handshake would be varying scan width and push cost
// together and could not attribute the difference to either.
enum class scan_policy : std::uint8_t {
    bitmap,     // O(active): iterate set bits only
    full_scan,  // O(registered): probe every non-free slot. The naive version,
                // kept because it is genuinely faster below a handful of
                // producers, where the bitmap's indirection buys nothing.
};

struct options {
    // --- capacity ---------------------------------------------------------

    // Producer slots. Memory is max_producers * ring_capacity * sizeof(T), but
    // rings are allocated on registration, so an unused slot costs nothing.
    std::uint32_t max_producers = 64;

    // Consumer shards. Each owns a disjoint, contiguous range of producer slots
    // and needs its own sink. More than one is worth it when the sink, not the
    // queue, is the bottleneck.
    std::uint32_t consumers = 1;

    // Per producer. Elements for a fixed-record pipeline, bytes for a
    // variable-length one. Power of two: the mask replaces a modulo.
    std::size_t ring_capacity = 4096;

    // Consumer staging buffer, in elements. One sink call covers up to this
    // many records across all of a shard's rings.
    std::size_t batch_capacity = 2048;

    // Fairness cap: events taken from one ring in one pass, so a hot producer
    // cannot monopolise a drain.
    std::size_t drain_batch = 512;

    // --- overload ---------------------------------------------------------

    full_policy full = full_policy::drop_newest;
    std::uint32_t push_spin = 200;  // spin_then_drop budget, in pause iterations

    // --- consumer scan ----------------------------------------------------

    scan_policy scan = scan_policy::bitmap;

    // --- consumer idling --------------------------------------------------

    std::uint32_t spin_before_park = 2000;

    // Missed-wakeup backstop. Always timed: a bounded worst case is worth more
    // than confidence that the wakeup path is perfect.
    nanos park_timeout{std::chrono::microseconds(200)};

    // --- threading --------------------------------------------------------

    // false hands the consumer loop back to you: call poll() from your own
    // reactor instead of the library owning a thread.
    bool own_threads = true;

    std::string thread_name = "tributary";

    // CPU id per shard. Empty leaves consumers unpinned. The tail-latency
    // story assumes the consumer does not migrate, so pin it in production.
    std::vector<int> pin_consumers_to;

    // -1 leaves placement to first touch by the registering thread, which is
    // the right default on both Linux and Windows.
    //
    // A node number binds every allocation the pipeline makes -- slots, shard
    // state, and each producer's ring -- to that node instead. Best-effort: if
    // the platform cannot bind, or the node does not exist, allocation falls
    // back to first touch and on_error is called once at construction. Nothing
    // in this library's correctness depends on placement.
    int numa_node = -1;

    // --- diagnostics ------------------------------------------------------

    // Cold path only: registration failures, sink exceptions escaping an
    // unsafe_sink adapter, a drain that missed its deadline. Never called from
    // the push path.
    std::function<void(std::string_view)> on_error;

    // Returns a message describing the first problem, or nullopt if usable.
    [[nodiscard]] std::optional<std::string> validate() const {
        if (max_producers == 0) return "max_producers must be at least 1";
        if (consumers == 0) return "consumers must be at least 1";
        if (consumers > max_producers) return "consumers must not exceed max_producers";
        if (ring_capacity < 2 || !std::has_single_bit(ring_capacity))
            return "ring_capacity must be a power of two and at least 2";
        if (batch_capacity == 0) return "batch_capacity must be at least 1";
        if (drain_batch == 0) return "drain_batch must be at least 1";
        if (!pin_consumers_to.empty() && pin_consumers_to.size() != consumers)
            return "pin_consumers_to must be empty or hold exactly one cpu id per consumer";
        if (park_timeout <= nanos::zero()) return "park_timeout must be positive";
        // Only the shape is checked here. Whether the node actually exists is a
        // property of the machine, not of the configuration, so it degrades to
        // first touch with an on_error report rather than refusing to start --
        // the same split thread pinning makes.
        if (numa_node < -1) return "numa_node must be -1 (first touch) or a node number";
        return std::nullopt;
    }
};

}  // namespace TRIBUTARY_ABI
}  // namespace tributary
