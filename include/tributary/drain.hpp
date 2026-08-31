#pragma once
//
// The drain seam: how records get from a channel into a sink.
//
// Everything else in the consumer loop -- the slot lifetime protocol, the
// Dekker wakeup handshake, park/wake, reclaim_retired, the two-mode bounded
// shutdown -- is the same whatever a record is. This is the part that is not,
// and it is exactly the part that differs between record models:
//
//   staged      copy out of the ring into a contiguous buffer, then hand the
//               sink one batch spanning many rings. For a 32-byte record the
//               copy is ~1-2 ns and buys batching, so it is the right trade.
//   zero-copy   hand the sink the ring's own memory. For a 500-byte log line
//               the copy is real and there are fewer records per batch, so
//               cross-ring batching matters less than not copying. (Phase C.)
//
// Different regimes, different answers -- not an inconsistency. Splitting it
// here rather than forking basic_pipeline keeps one copy of the ordering
// arguments, which are the code that is hard to get right and the code that a
// sanitizer only exercises in CI.
//
// A strategy is consumer-local: exactly one thread ever touches one, and it
// synchronises nothing.

#include "channel.hpp"
#include "detail/counter.hpp"
#include "traits.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

namespace tributary {
inline namespace TRIBUTARY_ABI {

// The runtime knobs a drain strategy interprets, copied out of `options` so the
// consumer loop never reads that object -- it holds a std::string and a
// std::function and is several cold lines wide.
//
// Both fields are denominated in whatever the channel counts, which is why they
// live here rather than in the pipeline: `batch_capacity` has no meaning at all
// for a strategy that stages nothing, and `drain_batch` is a fairness cap in
// elements for a fixed-size channel and in bytes for a variable-length one.
struct drain_config {
    std::size_t batch_capacity;  // staging buffer size, in elements
    std::size_t drain_batch;     // taken from one ring in one pass
};

// Copy-out drain for a channel of fixed-size records.
template <class Channel, class Traits = default_traits>
class staged_drain {
    static_assert(staged_channel<Channel>,
                  "staged_drain copies records out with pop_batch(value_type*, n) into a buffer of "
                  "value_type; a channel without that member -- a zero-copy one, say -- needs its "
                  "own drain strategy passed as basic_pipeline's third argument");

public:
    using channel_type = Channel;
    using value_type = typename Channel::value_type;
    using batch_type = typename Channel::batch_type;

    explicit staged_drain(const drain_config& cfg) : drain_batch_(cfg.drain_batch) {
        stage_.resize(cfg.batch_capacity);
    }

    // Pull from one ring, flushing on the way if the buffer fills. Returns what
    // came out of the *ring* -- what the caller counts as consumed -- which is
    // not what the sink accepted.
    template <class S>
    std::size_t take(Channel& ch, S& sink) {
        std::size_t got = 0;
        while (got < drain_batch_) {
            if (staged_ == stage_.size()) {
                flush(sink);
                // The sink refused everything and the staging buffer is still
                // full. Stop pulling: this is backpressure doing its job, and
                // the records stay in the rings where the memory bound and the
                // drop policy already apply to them.
                if (staged_ == stage_.size()) break;
            }
            const std::size_t room = std::min(stage_.size() - staged_, drain_batch_ - got);
            const std::size_t n = ch.pop_batch(stage_.data() + staged_, room);
            if (n == 0) break;
            staged_ += n;
            got += n;
        }
        return got;
    }

    // One sink call per batch, so the syscall a socket sink makes is amortised
    // over up to batch_capacity records.
    //
    // Returns how many records the sink accepted, and the caller needs that
    // return value rather than a before/after comparison of the fields: every
    // exit path below leaves stage_head_ at 0, so an outside observer cannot
    // tell a short accept from a flat refusal by looking.
    template <class S>
    std::size_t flush(S& sink) {
        if (stage_head_ == staged_) {
            stage_head_ = 0;
            staged_ = 0;
            return 0;
        }
        batch_type b(stage_.data() + stage_head_, staged_ - stage_head_);
        const std::size_t n = std::min(sink.write(b), b.size());
        stage_head_ += n;

        if (stage_head_ == staged_) {
            stage_head_ = 0;
            staged_ = 0;
            return n;
        }

        // Short accept. Compact the remainder to the front so the next drain
        // has room at the tail; without this, staged stays pinned at capacity
        // and the rings stop draining even after the sink recovers.
        backpressure_.bump();
        if (stage_head_ > 0) {
            std::memmove(stage_.data(), stage_.data() + stage_head_,
                         (staged_ - stage_head_) * sizeof(value_type));
            staged_ -= stage_head_;
            stage_head_ = 0;
        }
        return n;
    }

    // True while the sink still owes us an accept. The pipeline's idle path
    // tests this to decide whether a partially written batch needs unsticking
    // before it parks -- the one place a stalled sink gets a retry with no new
    // records arriving to drive one.
    [[nodiscard]] bool pending() const noexcept { return staged_ > stage_head_; }

    [[nodiscard]] std::uint64_t backpressure() const noexcept { return backpressure_.get(); }

private:
    // Consumer-local, and sized to fit one cache line together with whatever the
    // shard puts next to it: 24 + 8 + 8 + 8 + 8. The buffer's capacity is
    // stage_.size(), not a fourth field, for that reason. Verify with the layout
    // dump after touching this, not by reading it.
    std::vector<value_type> stage_;
    std::size_t staged_{0};      // records written into stage_
    std::size_t stage_head_{0};  // records already accepted by the sink
    std::size_t drain_batch_;
    TRIBUTARY_NO_UNIQUE_ADDRESS detail::counter<Traits::collect_stats> backpressure_;
};

}  // namespace TRIBUTARY_ABI
}  // namespace tributary
