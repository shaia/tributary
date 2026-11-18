// The extension points, exercised by something that is not fixed_channel.
//
// Every other suite runs the one channel and the one drain strategy the library
// ships, which means they would all still pass if the seams were welded shut.
// This one stands a deliberately trivial variable-length channel and a
// zero-copy drain up in their place and runs a real pipeline through them, so
// that "bytes_channel will fit through here" is something the build checks
// rather than something the design notes assert.
//
// The doubles are as small as they can be and still be honest. The channel is a
// non-wrapping byte buffer, not a ring: wrap, pad-to-wrap and frame headers are
// bytes_channel's problems, and none of them is what the seam is. What matters
// is that events reach the sink without a staging copy, that the producer
// writes into channel memory directly, and that a refused batch is retried from
// the idle path.

#include "harness.hpp"

#include <tributary/pipeline.hpp>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <thread>
#include <vector>

using namespace tributary;
using namespace tributary::test;

namespace {

// --- a variable-length channel ----------------------------------------------
//
// Satisfies channel_for and claimable_channel, and deliberately NOT
// staged_channel: it has no pop_batch and could not have one, since there is no
// fixed element to copy out. That combination is the whole point -- it is the
// shape bytes_channel will have.
//
// SPSC with the same two release/acquire pairs the real ring uses, because the
// pipeline will run this on two threads and a test double that races is worse
// than no test double at all.
class byte_channel {
public:
    using value_type = std::span<const std::byte>;
    using batch_type = std::span<const std::byte>;

    // What try_claim hands back. The pipeline never looks inside it; a real
    // channel would carry the frame offset here.
    class claim {
    public:
        claim() = default;
        explicit claim(std::span<std::byte> s) noexcept : span_(s) {}

        [[nodiscard]] bool valid() const noexcept { return !span_.empty(); }
        [[nodiscard]] std::span<std::byte> bytes() const noexcept { return span_; }

    private:
        std::span<std::byte> span_;
    };
    using claim_type = claim;

    explicit byte_channel(std::size_t capacity) : buf_(capacity) {}

    byte_channel(const byte_channel&) = delete;
    byte_channel& operator=(const byte_channel&) = delete;

    // --- producer side ------------------------------------------------------

    // Reserves without publishing. Nothing here is visible to the consumer
    // until commit stores write_ with release -- which is exactly why the
    // wakeup signal belongs in producer::commit and not here.
    claim_type try_claim(std::size_t n) noexcept {
        const std::size_t w = write_.load(std::memory_order_relaxed);  // sole writer
        if (n == 0 || w + n > buf_.size()) return {};
        return claim_type{std::span<std::byte>(buf_.data() + w, n)};
    }

    void commit(claim_type& /*c*/, std::size_t used) noexcept {
        const std::size_t w = write_.load(std::memory_order_relaxed);
        // Release: publishes the caller's writes into the claimed span.
        write_.store(w + used, std::memory_order_release);
        pushed_.fetch_add(1, std::memory_order_relaxed);
        const std::size_t depth = w + used - read_.load(std::memory_order_relaxed);
        if (depth > high_water_.load(std::memory_order_relaxed))
            high_water_.store(depth, std::memory_order_relaxed);
    }

    // The copying path, which channel_for requires of everyone. A caller with
    // bytes already in hand has nothing to gain from claiming.
    bool try_push(const value_type& v) noexcept {
        claim_type c = try_claim(v.size());
        if (!c.valid()) return false;
        std::memcpy(c.bytes().data(), v.data(), v.size());
        commit(c, v.size());
        return true;
    }

    void note_drop(std::uint64_t n = 1) noexcept { dropped_.fetch_add(n, std::memory_order_relaxed); }

    // --- consumer side ------------------------------------------------------

    // The zero-copy read: a span into the channel's own storage. Contiguous
    // because this double does not wrap.
    batch_type peek() noexcept {
        const std::size_t w = write_.load(std::memory_order_acquire);
        const std::size_t r = read_.load(std::memory_order_relaxed);  // sole writer
        return batch_type(buf_.data() + r, w - r);
    }

    // Release: the sink's reads of the peeked span must not sink past this.
    void release(std::size_t n) noexcept {
        read_.store(read_.load(std::memory_order_relaxed) + n, std::memory_order_release);
    }

    bool empty_now() noexcept {
        return read_.load(std::memory_order_relaxed) == write_.load(std::memory_order_acquire);
    }

    // --- observer side ------------------------------------------------------

    std::uint64_t size_now() const noexcept {
        const std::size_t w = write_.load(std::memory_order_acquire);
        const std::size_t r = read_.load(std::memory_order_acquire);
        return w - r;
    }
    std::uint64_t pushed() const noexcept { return pushed_.load(std::memory_order_relaxed); }
    std::uint64_t dropped() const noexcept { return dropped_.load(std::memory_order_relaxed); }
    std::uint64_t high_water() const noexcept { return high_water_.load(std::memory_order_relaxed); }

    std::uint64_t take_pushed() noexcept { return pushed_.exchange(0, std::memory_order_relaxed); }
    std::uint64_t take_dropped() noexcept { return dropped_.exchange(0, std::memory_order_relaxed); }
    void reset_stats() noexcept {
        pushed_.store(0, std::memory_order_relaxed);
        dropped_.store(0, std::memory_order_relaxed);
        high_water_.store(0, std::memory_order_relaxed);
    }

private:
    std::vector<std::byte> buf_;
    std::atomic<std::size_t> write_{0};
    std::atomic<std::size_t> read_{0};
    std::atomic<std::uint64_t> pushed_{0};
    std::atomic<std::uint64_t> dropped_{0};
    std::atomic<std::uint64_t> high_water_{0};
};

// --- a drain strategy that stages nothing ------------------------------------
//
// Hands the sink the channel's own memory and releases only what was accepted.
// Implements both rules drain.hpp states for a strategy with no buffer of its
// own, and exists mostly to prove those rules are implementable through the
// interface as it stands.
class direct_drain {
public:
    explicit direct_drain(const drain_config& cfg) : cap_(cfg.drain_batch) {}

    // Rule 2: once the sink has refused, offer it nothing more this pass. At
    // most one channel is ever owed, which is what keeps the bounded retry in
    // final_drain sufficient.
    template <class S>
    std::size_t take(byte_channel& ch, S& sink) {
        if (owed_ != nullptr) return 0;
        return offer(ch, sink);
    }

    // The retry, from the idle path and from shutdown. Rule 1: the channel came
    // from take(), because neither this signature nor pending()'s has one.
    template <class S>
    std::size_t flush(S& sink) {
        if (owed_ == nullptr) return 0;
        return offer(*owed_, sink);
    }

    [[nodiscard]] bool pending() const noexcept { return owed_ != nullptr; }
    [[nodiscard]] std::uint64_t backpressure() const noexcept { return backpressure_; }

private:
    template <class S>
    std::size_t offer(byte_channel& ch, S& sink) {
        std::span<const std::byte> b = ch.peek();
        if (b.empty()) {
            owed_ = nullptr;
            return 0;
        }
        if (b.size() > cap_) b = b.first(cap_);

        const std::size_t n = std::min(sink.write(b), b.size());
        ch.release(n);

        if (n == b.size()) {
            owed_ = nullptr;  // the sink is taking things again
        } else {
            ++backpressure_;
            owed_ = &ch;
        }
        return n;
    }

    byte_channel* owed_ = nullptr;
    std::size_t cap_;
    std::uint64_t backpressure_ = 0;
};

using byte_pipeline = basic_pipeline<byte_channel, default_traits, direct_drain>;

// --- the contract, at compile time ------------------------------------------

static_assert(channel_for<byte_channel>, "the double must meet the pipeline's own requirement");
static_assert(claimable_channel<byte_channel>, "and the producer-side one this suite exists for");
static_assert(!staged_channel<byte_channel>,
              "and must NOT meet the copy-out one -- if it did, this suite would be testing the "
              "staging path through a different channel rather than testing the seam");
static_assert(drain_for<direct_drain>, "a strategy with no buffer still answers pending()");
// The other half of the drain contract needs a sink type, so it is asserted
// below, once one exists.

// --- sinks -------------------------------------------------------------------

// Accepts everything, keeping the bytes so the test can assert the output is
// exactly the concatenation of what was committed.
struct alignas(default_traits::cache_line) recording_sink {
    std::size_t write(std::span<const std::byte> b) noexcept {
        seen.insert(seen.end(), b.begin(), b.end());
        ++batches;
        return b.size();
    }
    std::vector<std::byte> seen;
    std::uint64_t batches = 0;
};

// Refuses entirely until released, then accepts. on_idle() reports whether it
// has become willing, which is what lets a stalled sink recover with no new
// events arriving to drive another flush.
class stalling_byte_sink {
public:
    std::size_t write(std::span<const std::byte> b) noexcept {
        if (!open_.load(std::memory_order_acquire)) {
            ++refusals_;
            return 0;
        }
        seen_.insert(seen_.end(), b.begin(), b.end());
        return b.size();
    }

    bool on_idle() noexcept {
        ++idle_calls_;
        return false;  // no progress of its own; the pipeline retries the flush
    }

    void open() noexcept { open_.store(true, std::memory_order_release); }

    const std::vector<std::byte>& seen() const noexcept { return seen_; }
    std::uint64_t refusals() const noexcept { return refusals_; }
    std::uint64_t idle_calls() const noexcept { return idle_calls_; }

private:
    std::atomic<bool> open_{false};
    std::vector<std::byte> seen_;
    std::uint64_t refusals_ = 0;
    std::uint64_t idle_calls_ = 0;
};

// Accepts a bounded prefix of every batch, so every offer is a short accept.
struct alignas(default_traits::cache_line) partial_byte_sink {
    explicit partial_byte_sink(std::size_t per_call) : per_call_(per_call) {}

    std::size_t write(std::span<const std::byte> b) noexcept {
        const std::size_t n = std::min(per_call_, b.size());
        seen.insert(seen.end(), b.begin(), b.begin() + static_cast<std::ptrdiff_t>(n));
        return n;
    }

    std::vector<std::byte> seen;

private:
    std::size_t per_call_;
};

static_assert(drains_into<direct_drain, byte_channel, recording_sink>,
              "and the sink-dependent half, for a sink it is expected to drain into");
static_assert(sink_for<recording_sink, byte_channel::batch_type>,
              "the sink contract is unchanged by any of this: a batch of bytes is still a batch");

// --- helpers -----------------------------------------------------------------

// Length varies per event so the run is genuinely variable-length rather than a
// fixed record wearing a span.
std::vector<std::byte> payload_for(std::uint32_t i) {
    const std::size_t len = 1 + (i % 37);
    std::vector<std::byte> v(len);
    for (std::size_t k = 0; k < len; ++k)
        v[k] = static_cast<std::byte>((i + k) & 0xFF);
    return v;
}

std::vector<std::byte> expected_concatenation(std::uint32_t count) {
    std::vector<std::byte> all;
    for (std::uint32_t i = 0; i < count; ++i) {
        const auto p = payload_for(i);
        all.insert(all.end(), p.begin(), p.end());
    }
    return all;
}

options seam_options() {
    options opt;
    opt.max_producers = 4;
    opt.ring_capacity = 1U << 20;  // bytes; the double does not wrap, so size it to fit the run
    // Deliberately left at its default and deliberately unused: batch_capacity
    // is a staging buffer size and this strategy has no staging buffer. That it
    // can be ignored without consequence is part of what is being asserted.
    return opt;
}

// --- tests -------------------------------------------------------------------

// The whole path: reserve channel memory through the handle, write into it,
// publish, and have the consumer hand that same memory to the sink.
void test_claim_commit_round_trip() {
    std::printf("claim/commit round trip\n");
    constexpr std::uint32_t kEvents = 20'000;

    byte_pipeline pipe(seam_options());
    recording_sink sink;
    pipe.start(sink);

    std::uint64_t committed = 0;
    std::thread producer([&pipe, &committed] {
        auto h = pipe.register_producer();
        if (!h) return;
        for (std::uint32_t i = 0; i < kEvents; ++i) {
            const auto p = payload_for(i);
            auto c = h.try_claim(p.size());
            if (!c.valid()) continue;  // dropped, and counted by the channel
            // The point of the whole exercise: this writes into the channel's
            // storage, not into a buffer that is later copied there.
            std::memcpy(c.bytes().data(), p.data(), p.size());
            if (h.commit(c, p.size())) ++committed;
        }
    });
    producer.join();

    pipe.stop(stop_mode::drain);

    const auto expected = expected_concatenation(kEvents);
    check_eq(committed, kEvents, "every claim succeeded and was published");
    check(pipe.drain_completed(), "the drain finished inside its deadline");
    check_eq(sink.seen.size(), expected.size(), "every committed byte reached the sink");
    check(sink.seen == expected, "the sink's output is exactly the concatenation of what was "
                                 "committed, in order");
    check(sink.batches < kEvents, "the consumer batched across events rather than one call each ("
                                      + std::to_string(sink.batches) + " calls for "
                                      + std::to_string(kEvents) + " events)");
}

// A short accept on every offer. Nothing may be lost, and the sink must still
// see one uninterrupted stream.
void test_partial_accept_loses_nothing() {
    std::printf("\npartial accept\n");
    constexpr std::uint32_t kEvents = 2'000;

    options opt = seam_options();
    opt.own_threads = false;  // drive it by hand, so the assertions are not timing-dependent
    byte_pipeline pipe(opt);
    partial_byte_sink sink(7);  // smaller than most events, so every offer is short
    pipe.start(sink);

    {
        auto h = pipe.register_producer();
        check(h.valid(), "the producer got a slot");
        for (std::uint32_t i = 0; i < kEvents; ++i) {
            const auto p = payload_for(i);
            auto c = h.try_claim(p.size());
            if (!c.valid()) break;
            std::memcpy(c.bytes().data(), p.data(), p.size());
            h.commit(c, p.size());
        }
    }

    // Bounded: a strategy that failed to make progress would spin here forever
    // rather than failing, and a test that hangs says less than one that fails.
    int passes = 0;
    while (pipe.poll(0, sink) > 0 && ++passes < 1'000'000) {
    }
    pipe.stop(stop_mode::drain);
    while (pipe.poll(0, sink) > 0 && ++passes < 1'000'000) {
    }
    pipe.close(0, sink);

    const auto expected = expected_concatenation(kEvents);
    check(passes < 1'000'000, "the drain terminated rather than spinning");
    check_eq(sink.seen.size(), expected.size(), "a short accept on every offer still lost nothing");
    check(sink.seen == expected, "and the stream is still in order across the re-offers");

    const auto st = pipe.snapshot();
    check(st.sink_backpressure > 0, "backpressure was counted through the strategy's own counter");
}

// The failure decision 3 exists to prevent, through a strategy that holds no
// buffer: a sink that refuses and then recovers, with no new events arriving to
// drive another flush.
void test_stalled_sink_recovers() {
    std::printf("\nstalled sink recovers\n");
    constexpr std::uint32_t kEvents = 64;

    options opt = seam_options();
    byte_pipeline pipe(opt);
    stalling_byte_sink sink;
    pipe.start(sink);

    {
        auto h = pipe.register_producer();
        check(h.valid(), "the producer got a slot");
        for (std::uint32_t i = 0; i < kEvents; ++i) {
            const auto p = payload_for(i);
            auto c = h.try_claim(p.size());
            if (!c.valid()) break;
            std::memcpy(c.bytes().data(), p.data(), p.size());
            h.commit(c, p.size());
        }
    }

    // Let the consumer discover the refusal and settle into the idle path.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    check(sink.refusals() > 0, "the sink actually refused (the setup is real)");
    check(sink.seen().empty(), "nothing was delivered while it was refusing");
    check(sink.idle_calls() > 0, "on_idle() was reached while the strategy was owed a retry");

    // Nothing new is pushed after this point. Everything that arrives now
    // arrives because the idle path retried a batch it was still holding.
    sink.open();
    pipe.stop(stop_mode::drain);

    const auto expected = expected_concatenation(kEvents);
    check(pipe.drain_completed(), "the drain finished inside its deadline once the sink recovered");
    check_eq(sink.seen().size(), expected.size(),
             "a recovered sink drained its remainder with no new events arriving");
    check(sink.seen() == expected, "and the recovered stream is in order");
}

}  // namespace

int main() {
    test_claim_commit_round_trip();
    test_partial_accept_loses_nothing();
    test_stalled_sink_recovers();
    return summary("test_seam");
}
