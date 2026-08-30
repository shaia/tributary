#pragma once
//
// Sinks that let the tests observe and misbehave.
//
// The ordering sink is the important one: it validates per-producer sequence
// monotonicity from inside the consumer thread, which is the only place the
// records are seen in the order the pipeline actually delivered them. The
// library deliberately has no per-record hook for this -- a std::function on
// that path would be a cost every production user pays for a test's benefit.

#include "harness.hpp"

#include <tributary/traits.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <span>
#include <thread>
#include <vector>

namespace tributary::test {

// Validates per-producer ordering and counts everything it is handed.
class ordering_sink {
public:
    explicit ordering_sink(std::uint32_t max_producers)
        : last_(max_producers, 0), seen_(max_producers, 0) {}

    std::size_t write(std::span<const event> b) noexcept {
        for (const auto& e : b) {
            if (e.producer_id < last_.size()) {
                // Gaps are legal -- that is a counted drop. Going backwards or
                // repeating is not.
                if (seen_[e.producer_id] != 0 && e.seq <= last_[e.producer_id]) ++violations_;
                seen_[e.producer_id] = 1;
                last_[e.producer_id] = e.seq;
            } else {
                ++violations_;
            }
        }
        records_ += b.size();
        ++batches_;
        return b.size();
    }

    std::uint64_t records() const noexcept { return records_; }
    std::uint64_t batches() const noexcept { return batches_; }
    std::uint64_t violations() const noexcept { return violations_; }

private:
    std::vector<std::uint64_t> last_;
    std::vector<std::uint8_t> seen_;  // not vector<bool>: no reason for a proxy reference here
    std::uint64_t records_ = 0;
    std::uint64_t batches_ = 0;
    std::uint64_t violations_ = 0;
};

// Accepts everything, keeps nothing. The baseline: benchmark against this to
// find the pipeline's ceiling with the sink removed.
struct counting_sink {
    std::size_t write(std::span<const event> b) noexcept {
        // Touch the batch so the compiler cannot elide producing it, but sample
        // rather than hashing every byte: a serial multiply chain over a 64 KiB
        // batch would make the *sink* the bottleneck and every measurement
        // would be reporting the checksum instead of the queue.
        const std::size_t stride = std::max<std::size_t>(1, b.size() / 32);
        for (std::size_t i = 0; i < b.size(); i += stride)
            checksum = (checksum ^ b[i].seq) * 0x100000001b3ULL;
        records += b.size();
        ++batches;
        return b.size();
    }
    std::uint64_t checksum = 0xcbf29ce484222325ULL;
    std::uint64_t records = 0;
    std::uint64_t batches = 0;
};

// Accepts at most `limit` records per call, so the pipeline has to retain,
// compact, and re-offer a remainder. This is the sink that exercises the whole
// backpressure path; a sink that always accepts everything never touches it.
class partial_sink {
public:
    explicit partial_sink(std::size_t limit) : limit_(limit) {}

    std::size_t write(std::span<const event> b) noexcept {
        const std::size_t n = std::min(limit_, b.size());
        records_ += n;
        ++batches_;
        if (n < b.size()) ++short_writes_;
        return n;
    }

    std::uint64_t records() const noexcept { return records_; }
    std::uint64_t short_writes() const noexcept { return short_writes_; }
    std::uint64_t batches() const noexcept { return batches_; }

private:
    std::size_t limit_;
    std::uint64_t records_ = 0;
    std::uint64_t batches_ = 0;
    std::uint64_t short_writes_ = 0;
};

// Refuses everything until unblocked, then accepts everything. Stands in for a
// socket whose peer has stopped reading and later resumes -- the case where a
// remainder would be stranded if the consumer never called on_idle().
class stalling_sink {
public:
    std::size_t write(std::span<const event> b) noexcept {
        ++batches_;
        if (!open_) {
            ++refusals_;
            return 0;
        }
        records_ += b.size();
        return b.size();
    }

    // Called before the consumer parks and on every park timeout. Reporting
    // progress here is what gets a recovered sink unstuck with no new records
    // arriving to drive a flush.
    bool on_idle() noexcept {
        ++idle_calls_;
        return open_ && refusals_ > 0;
    }

    void close() noexcept { ++closes_; }

    void open() noexcept { open_ = true; }

    std::uint64_t records() const noexcept { return records_; }
    std::uint64_t refusals() const noexcept { return refusals_; }
    std::uint64_t idle_calls() const noexcept { return idle_calls_; }
    std::uint64_t closes() const noexcept { return closes_; }

private:
    bool open_ = false;
    std::uint64_t records_ = 0;
    std::uint64_t batches_ = 0;
    std::uint64_t refusals_ = 0;
    std::uint64_t idle_calls_ = 0;
    std::uint64_t closes_ = 0;
};

// Deliberately slow, to drive the rings into overflow and exercise the drop
// policy. Stands in for a socket whose peer has stopped reading.
class slow_sink {
public:
    explicit slow_sink(std::chrono::nanoseconds per_batch) : delay_(per_batch) {}

    std::size_t write(std::span<const event> b) noexcept {
        records_ += b.size();
        ++batches_;
        const auto until = std::chrono::steady_clock::now() + delay_;
        while (std::chrono::steady_clock::now() < until) TRIBUTARY_PAUSE();
        return b.size();
    }

    std::uint64_t records() const noexcept { return records_; }
    std::uint64_t batches() const noexcept { return batches_; }

private:
    std::chrono::nanoseconds delay_;
    std::uint64_t records_ = 0;
    std::uint64_t batches_ = 0;
};

}  // namespace tributary::test
