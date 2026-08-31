#pragma once
//
// The output contract.
//
// write() returns how many elements of the batch it accepted. Anything less
// than batch.size() is backpressure: the pipeline keeps the remainder and
// re-offers it, so the far side's slowness propagates back through the staging
// buffer, into the rings, and finally into the drop policy. Every link in that
// chain is bounded, which is what makes it backpressure rather than a deferred
// memory problem.
//
// A sink must never buffer an unbounded remainder of its own. That converts a
// latency problem into a memory problem, and the memory problem arrives later,
// larger, and as an OOM kill. Return a short count instead and let the rings
// absorb it -- they are sized for exactly that, and they have a drop policy
// when they run out.

#include "traits.hpp"

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <string_view>
#include <utility>

namespace tributary {
inline namespace TRIBUTARY_ABI {

// noexcept is required, and is not a formality. The consumer runs on its own
// thread with no handler above it, so an escaping exception is a
// std::terminate, not an error. Wrap a throwing sink in unsafe_sink.
template <class S, class Batch>
concept sink_for = requires(S& s, Batch b) {
    { s.write(b) } noexcept -> std::convertible_to<std::size_t>;
};

// Optional, detected at compile time.
//
// on_idle() is called before the consumer parks and again on every park
// timeout, so a sink holding a partially written remainder gets a chance to
// retry it even when no new events arrive. Without it a socket that returned
// EAGAIN would strand its remainder until the next event happened to show up
// -- which, on a pipeline that has just gone quiet, may be never.
//
// Return true if progress was made; the consumer will then retry the flush
// instead of sleeping.
template <class S>
concept has_on_idle = requires(S& s) {
    { s.on_idle() } noexcept -> std::convertible_to<bool>;
};

// Called once, on the consumer thread, after the final drain. A socket sink
// should ::shutdown(fd, SHUT_WR) here so the peer sees a clean EOF rather than
// a reset.
template <class S>
concept has_close = requires(S& s) {
    { s.close() } noexcept;
};

// ---------------------------------------------------------------------------
// Adapters
// ---------------------------------------------------------------------------

// Discards everything, accepts everything. Useful as a baseline: benchmark
// against this to find the pipeline's ceiling with the sink removed.
template <class Batch>
struct null_sink {
    std::size_t write(Batch b) noexcept {
        events += b.size();
        ++batches;
        return b.size();
    }
    std::uint64_t events = 0;
    std::uint64_t batches = 0;
};

// Wraps a sink that may throw, which the concept otherwise forbids.
//
// On an exception the batch is reported and treated as CONSUMED, not retried.
// Retrying a batch that just threw means retrying it forever, at whatever rate
// the consumer spins, with the rings filling behind it -- a livelock dressed up
// as durability. Dropping it loses data, but loses it visibly and at a bounded
// rate, and failed_events() says exactly how much.
template <class Inner>
class unsafe_sink {
public:
    explicit unsafe_sink(Inner& inner, std::function<void(std::string_view)> on_error = {})
        : inner_(&inner), on_error_(std::move(on_error)) {}

    template <class Batch>
    std::size_t write(Batch b) noexcept {
        try {
            return inner_->write(b);
        } catch (const std::exception& e) {
            report(e.what(), b.size());
        } catch (...) {
            report("sink threw a non-std exception", b.size());
        }
        return b.size();
    }

    bool on_idle() noexcept {
        if constexpr (has_on_idle<Inner>) {
            try {
                return inner_->on_idle();
            } catch (...) {
                report("sink threw from on_idle", 0);
            }
        }
        return false;
    }

    void close() noexcept {
        if constexpr (has_close<Inner>) {
            try {
                inner_->close();
            } catch (...) {
                report("sink threw from close", 0);
            }
        }
    }

    std::uint64_t failed_batches() const noexcept { return failed_batches_; }
    std::uint64_t failed_events() const noexcept { return failed_events_; }

private:
    void report(std::string_view what, std::size_t n) noexcept {
        ++failed_batches_;
        failed_events_ += n;
        if (on_error_) {
            // The handler is user code on the consumer thread. If it throws
            // too, there is nothing left to report to.
            try {
                on_error_(what);
            } catch (...) {  // NOLINT(bugprone-empty-catch)
            }
        }
    }

    Inner* inner_;
    std::function<void(std::string_view)> on_error_;
    std::uint64_t failed_batches_ = 0;
    std::uint64_t failed_events_ = 0;
};

}  // namespace TRIBUTARY_ABI
}  // namespace tributary
