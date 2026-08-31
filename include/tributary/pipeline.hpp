#pragma once
//
// The pipeline: a registry of per-producer rings and the consumers that drain
// them into sinks.
//
// Four protocols live here, and each is subtle enough to deserve naming:
//
//   producer lifetime   free -> claiming -> active -> retiring -> free, where
//                       only the consumer may publish free
//   wakeup handshake    a Dekker pair between active_set::signal() and reap(),
//                       carrying the design's only seq_cst fence
//   backpressure        a short accept from the sink is retained and re-offered,
//                       so slowness propagates into the rings and finally into
//                       the drop policy -- never into memory. This file owns
//                       that the remainder is held at all and that it is retried
//                       before the consumer parks; drain.hpp owns how it is held
//   shutdown            two phases, two modes, always bounded by a deadline
//
// All four are the same whatever an event is, which is why the second event
// model arrives as a Drain parameter rather than as a second copy of this file.
//
// LIFETIME CONTRACT: the pipeline must outlive every producer handle it hands
// out. A handle holds a raw pointer back and its destructor calls into the
// pipeline to retire its slot. This is deliberate -- a shared_ptr control block
// would be an atomic refcount on the push path, which is exactly the contended
// cache line this design exists to avoid.

#include "channel.hpp"
#include "detail/active_set.hpp"
#include "detail/alloc.hpp"
#include "detail/counter.hpp"
#include "detail/numa.hpp"
#include "detail/thread.hpp"
#include "drain.hpp"
#include "fixed_channel.hpp"
#include "options.hpp"
#include "sink.hpp"
#include "stats.hpp"
#include "traits.hpp"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace tributary {
inline namespace TRIBUTARY_ABI {

// Drain is the seam that admits a second event model. Everything this class
// writes down -- the four protocols above -- is the same for all of them; the
// copy-out into a staging buffer is not, so it lives behind this parameter
// rather than in the loop. See drain.hpp.
template <channel_for Channel, class Traits = default_traits,
          class Drain = staged_drain<Channel, Traits>>
class basic_pipeline {
public:
    using channel_type = Channel;
    using value_type = typename Channel::value_type;
    using batch_type = typename Channel::batch_type;
    using traits_type = Traits;
    using drain_type = Drain;

    static constexpr std::size_t cache_line = Traits::cache_line;

private:
    using counter_t = detail::counter<Traits::collect_stats>;
    using active_set_t = detail::active_set<Traits>;

    // free -> claiming (registrant CAS) -> active (registrant, after the ring
    // exists) -> retiring (handle destructor) -> free (consumer, after a final
    // drain).
    //
    // `claiming` is not decoration. Without it the registrant would publish
    // `active` before its ring existed, and a consumer probing between those
    // two moments would dereference a null channel.
    //
    // Only the consumer may publish `free`, and only after it has drained the
    // ring and seen it empty. A departing producer can mark itself `retiring`
    // but cannot release its own slot -- it has no way to know whether the
    // consumer is mid-batch inside its ring.
    enum class slot_state : std::uint8_t { free, claiming, active, retiring };

    // Consumer-read-mostly. The producer touches this line only at registration
    // and retirement; its per-push counters live inside the channel, in the
    // line it already owns. Putting them here instead would mean every push
    // invalidates a line the consumer polls -- the false sharing this
    // separation exists to prevent. Note that `alignas` on this struct only
    // separates slot i from slot i+1; the separation that matters is *within*
    // the slot, and it is this one. Verify with the layout dump, not by
    // reading the struct.
    struct alignas(cache_line) producer_slot {
        detail::aligned_ptr<Channel> channel;
        std::atomic<slot_state> state{slot_state::free};
        std::uint32_t shard{0};
    };

    // One per consumer. Each owns a contiguous, disjoint range of producer
    // slots, so no two shards ever share a bitmap word, a park mutex, or a
    // counter line.
    struct alignas(cache_line) shard_state {
        shard_state(std::uint32_t slot_count, std::uint32_t first, const drain_config& dc)
            : active(slot_count), begin(first), end(first + slot_count), count(slot_count),
              drain(dc) {}

        active_set_t active;
        std::uint32_t begin;
        std::uint32_t end;
        std::uint32_t count;

        // Consumer-written, deliberately on its own line. retire_pending is the
        // worst offender if misplaced: the consumer exchanges it -- an RMW,
        // which takes the line exclusive -- on idle passes.
        alignas(cache_line) std::atomic<bool> parked{false};
        std::atomic<bool> retire_pending{false};
        std::mutex park_mu;
        std::condition_variable park_cv;

        // Producer-written, and only when a producer actually wakes a sleeping
        // consumer. Its own line so that rare event does not invalidate the
        // consumer's counters.
        alignas(cache_line) TRIBUTARY_NO_UNIQUE_ADDRESS counter_t notifies;

        // Consumer-local. No other thread touches these. The strategy is 56
        // bytes and `rotate` is read and written on every pass, so the two share
        // a line and the drain's fields cost the loop no second one. `thread` is
        // touched at start and join only and can fall wherever it lands.
        alignas(cache_line) TRIBUTARY_NO_UNIQUE_ADDRESS Drain drain;
        std::uint32_t rotate{0};
        std::thread thread;

        alignas(cache_line) TRIBUTARY_NO_UNIQUE_ADDRESS counter_t consumed;
        TRIBUTARY_NO_UNIQUE_ADDRESS counter_t rings_probed;
        TRIBUTARY_NO_UNIQUE_ADDRESS counter_t rings_empty;
        TRIBUTARY_NO_UNIQUE_ADDRESS counter_t passes;
        TRIBUTARY_NO_UNIQUE_ADDRESS counter_t parks;
    };

    // Read-only after construction. Copied out of `options` so the consumer
    // loop never touches that object, which holds a std::string and a
    // std::function and is several cold lines wide.
    // What the drain interprets -- drain_batch, batch_capacity -- is not here:
    // it is denominated in whatever the channel counts and lives with the
    // strategy that reads it.
    struct hot_config {
        std::uint32_t spin_before_park;
        nanos park_timeout;
        scan_policy scan;
    };

public:
    // Move-only ticket for one producer slot.
    //
    // Everything the push path needs is cached here, so a push reads no
    // pipeline metadata beyond the single `accepting_` flag: not the options,
    // not the slot table, not the bitmap's header.
    class producer {
    public:
        producer() = default;
        ~producer() { release(); }

        producer(const producer&) = delete;
        producer& operator=(const producer&) = delete;

        producer(producer&& o) noexcept { steal(o); }
        producer& operator=(producer&& o) noexcept {
            if (this != &o) {
                release();
                steal(o);
            }
            return *this;
        }

        [[nodiscard]] bool valid() const noexcept { return pipe_ != nullptr; }
        explicit operator bool() const noexcept { return valid(); }

        std::uint32_t id() const noexcept { return id_; }
        std::uint32_t shard() const noexcept { return shard_; }

        // Returns false if the event was dropped -- the ring was full and the
        // overload policy gave up, or the pipeline has stopped accepting.
        // Always check it: the drop is counted, but only you can decide what
        // losing this particular event means.
        bool push(const value_type& v) noexcept {
            if (pipe_ == nullptr) return false;
            if (!pipe_->accepting_.load(std::memory_order_acquire)) return false;

            bool ok = ch_->try_push(v);
            if (!ok && full_ == full_policy::spin_then_drop) {
                // Bounded spin, then give up. Never an unbounded wait: coupling
                // a producer's latency to the consumer's worst stall is the
                // failure this library exists to prevent.
                for (std::uint32_t i = 0; i < push_spin_ && !ok; ++i) {
                    TRIBUTARY_PAUSE();
                    ok = ch_->try_push(v);
                }
            }
            if (!ok) {
                ch_->note_drop();
                return false;
            }

            // MUST follow the publish inside try_push. signal() opens with a
            // seq_cst fence for exactly that reason; see active_set.hpp.
            if (sh_->active.signal(tok_)) pipe_->wake_if_parked(*sh_);
            return true;
        }

        // Idempotent. Marks the slot retiring; the consumer releases it once it
        // has drained the ring.
        void release() noexcept {
            if (pipe_ == nullptr) return;
            pipe_->retire(id_, *sh_, tok_);
            pipe_ = nullptr;
            ch_ = nullptr;
            sh_ = nullptr;
        }

    private:
        friend class basic_pipeline;

        void steal(producer& o) noexcept {
            pipe_ = std::exchange(o.pipe_, nullptr);
            ch_ = std::exchange(o.ch_, nullptr);
            sh_ = std::exchange(o.sh_, nullptr);
            tok_ = o.tok_;
            id_ = o.id_;
            shard_ = o.shard_;
            full_ = o.full_;
            push_spin_ = o.push_spin_;
        }

        basic_pipeline* pipe_ = nullptr;
        Channel* ch_ = nullptr;
        shard_state* sh_ = nullptr;
        typename active_set_t::token tok_;
        std::uint32_t id_ = 0;
        std::uint32_t shard_ = 0;
        full_policy full_ = full_policy::drop_newest;
        std::uint32_t push_spin_ = 0;
    };

    explicit basic_pipeline(options opt) : opt_(std::move(opt)) {
        if (auto bad = opt_.validate()) throw std::invalid_argument("tributary: " + *bad);

        // Say once, here, that a requested node cannot be honoured. Binding is
        // best-effort everywhere below -- a failure falls back to first touch
        // rather than throwing -- and without this the only symptom would be
        // every ring quietly landing on the wrong node, which looks like
        // nothing at all until someone profiles remote memory traffic.
        if (opt_.numa_node != detail::numa::any_node && opt_.on_error) {
            if (!detail::numa::supported())
                opt_.on_error("numa_node was requested but this build cannot bind memory; "
                              "falling back to first touch");
            else if (!detail::numa::node_in_range(opt_.numa_node))
                opt_.on_error("numa_node is out of range on this machine; "
                              "falling back to first touch");
        }

        cfg_ = hot_config{opt_.spin_before_park, opt_.park_timeout, opt_.scan};
        const drain_config dc{opt_.batch_capacity, opt_.drain_batch};

        // Covers the slot array and every shard_state below. Rings are not
        // allocated here -- they are bound separately in commission(), on the
        // registering thread.
        const detail::numa::scoped_node place{opt_.numa_node};
        slots_ = detail::aligned_array<producer_slot, cache_line>(opt_.max_producers);

        // Contiguous slot ranges per shard, not interleaved: a shard's bitmap
        // words are then its own lines, and a consumer scanning them never
        // touches another shard's.
        const std::uint32_t per = opt_.max_producers / opt_.consumers;
        const std::uint32_t extra = opt_.max_producers % opt_.consumers;
        std::uint32_t first = 0;
        shards_.reserve(opt_.consumers);
        for (std::uint32_t s = 0; s < opt_.consumers; ++s) {
            const std::uint32_t n = per + (s < extra ? 1U : 0U);
            shards_.push_back(detail::make_aligned<shard_state>(n, first, dc));
            for (std::uint32_t i = first; i < first + n; ++i) slots_[i].shard = s;
            first += n;
        }
    }

    ~basic_pipeline() { stop(stop_mode::abort); }

    basic_pipeline(const basic_pipeline&) = delete;
    basic_pipeline& operator=(const basic_pipeline&) = delete;

    // --- registration ------------------------------------------------------

    // Call once per producer thread and keep the handle for that thread's
    // lifetime. Check valid(): a pipeline whose slots are all taken returns an
    // empty handle, and a caller that ignores that silently produces nothing.
    [[nodiscard]] producer register_producer() {
        const auto shard_count = static_cast<std::uint32_t>(shards_.size());
        const std::uint32_t start = next_shard_.fetch_add(1, std::memory_order_relaxed) % shard_count;
        for (std::uint32_t s = 0; s < shard_count; ++s) {
            shard_state& sh = *shards_[(start + s) % shard_count];
            for (std::uint32_t i = sh.begin; i < sh.end; ++i) {
                auto expected = slot_state::free;
                // acquire on success: everything the previous owner and the
                // consumer's release of this slot did is visible before we
                // reuse it.
                if (!slots_[i].state.compare_exchange_strong(expected, slot_state::claiming,
                                                             std::memory_order_acquire,
                                                             std::memory_order_relaxed))
                    continue;
                return commission(sh, i);
            }
        }
        registration_failures_.add();
        report("no free producer slot");
        return {};
    }

    // --- running -----------------------------------------------------------

    // Convenience for a single-shard pipeline.
    template <class S>
        requires sink_for<S, batch_type>
    void start(S& sink) {
        S* one[1] = {&sink};
        start(std::span<S* const>(one, 1));
    }

    // One sink per shard. The library imposes no thread-safety requirement on
    // your sink, which is why it will not share one across consumers.
    template <class S>
        requires sink_for<S, batch_type>
    void start(std::span<S* const> sinks) {
        if (sinks.size() != shards_.size())
            throw std::invalid_argument("tributary: start() needs exactly one sink per consumer");
        if (started_) throw std::logic_error("tributary: already started");

        started_ = true;
        drain_deadline_met_ = true;
        running_.store(true, std::memory_order_relaxed);
        accepting_.store(true, std::memory_order_release);
        if (!opt_.own_threads) return;  // the caller will drive run() or poll()

        for (std::uint32_t i = 0; i < shards_.size(); ++i) {
            shard_state* sh = shards_[i].get();
            S* sk = sinks[i];
            sh->thread = std::thread([this, sh, sk, i] { consume(*sh, *sk, i); });
        }
    }

    // Runs one shard's consumer loop on the calling thread until stop(). For
    // when you want to own the thread but keep the library's idling, parking
    // and shutdown behaviour.
    template <class S>
        requires sink_for<S, batch_type>
    void run(std::uint32_t shard, S& sink) {
        consume(*shards_[shard], sink, shard);
    }

    // One unit of consumer work, for driving from an existing reactor. Returns
    // the number of events drained. Never sleeps and never parks -- pacing is
    // the caller's business.
    //
    // After stop(), keep calling this until it returns 0 to complete the final
    // drain, then close(shard, sink).
    template <class S>
        requires sink_for<S, batch_type>
    std::size_t poll(std::uint32_t shard, S& sink) {
        shard_state& sh = *shards_[shard];
        const std::size_t n = drain_pass(sh, sink);
        sh.passes.bump();
        bump_rotate(sh);
        if (n > 0) return n;
        return idle_work(sh, sink) ? 1 : 0;
    }

    template <class S>
        requires sink_for<S, batch_type>
    void close(std::uint32_t shard, S& sink) {
        final_drain(*shards_[shard], sink);
    }

    // --- shutdown ----------------------------------------------------------

    // Idempotent. drain waits, bounded, for the rings to empty; abort does not.
    void stop(stop_mode mode, nanos deadline = std::chrono::seconds(5)) {
        if (!started_) return;

        // Producers see this and start failing pushes, so the rings can only
        // shrink from here. This must come first: draining a queue that is
        // still being filled is a race you can lose indefinitely.
        accepting_.store(false, std::memory_order_release);

        if (mode == stop_mode::drain) {
            const auto until = clock_type::now() + deadline;
            // Bounded: a wedged sink must never make shutdown hang forever.
            while (clock_type::now() < until && !all_quiesced()) {
                wake_all();
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
            drain_deadline_met_ = all_quiesced();
            if (!drain_deadline_met_) report("drain hit its deadline and degraded to an abort");
        }

        running_.store(false, std::memory_order_release);
        wake_all();
        for (auto& sh : shards_)
            if (sh->thread.joinable()) sh->thread.join();
        started_ = false;
    }

    // False if a drain hit its deadline and degraded to an abort.
    [[nodiscard]] bool drain_completed() const noexcept { return drain_deadline_met_; }

    // --- observability -----------------------------------------------------

    [[nodiscard]] stats snapshot() const {
        stats s;
        s.pushed = retired_pushed_.get();
        s.dropped = retired_dropped_.get();
        s.registration_failures = registration_failures_.get();

        for (std::uint32_t i = 0; i < slots_.size(); ++i) {
            const Channel* ch = slots_[i].channel.get();
            if (ch == nullptr) continue;
            s.pushed += ch->pushed();
            s.dropped += ch->dropped();
            s.high_water = std::max(s.high_water, ch->high_water());
        }
        for (const auto& sh : shards_) {
            s.consumed += sh->consumed.get();
            s.rings_probed += sh->rings_probed.get();
            s.rings_empty += sh->rings_empty.get();
            s.passes += sh->passes.get();
            s.parks += sh->parks.get();
            s.notifies += sh->notifies.get();
            s.sink_backpressure += sh->drain.backpressure();
            s.bitmap_writes += sh->active.writes();
        }
        return s;
    }

    void per_producer(std::vector<producer_stats>& out) const {
        out.clear();
        out.reserve(slots_.size());
        for (std::uint32_t i = 0; i < slots_.size(); ++i) {
            const producer_slot& sl = slots_[i];
            const auto st = sl.state.load(std::memory_order_acquire);
            producer_stats p;
            p.id = i;
            p.shard = sl.shard;
            p.active = (st == slot_state::active);
            p.retiring = (st == slot_state::retiring);
            if (const Channel* ch = sl.channel.get()) {
                p.pushed = ch->pushed();
                p.dropped = ch->dropped();
                p.high_water = ch->high_water();
                // size_now(), not the producer's cached hint: this runs on a
                // thread that is neither producer nor consumer, and the cached
                // indices are private to those threads.
                p.depth = ch->size_now();
            }
            out.push_back(p);
        }
    }

    [[nodiscard]] const options& config() const noexcept { return opt_; }
    [[nodiscard]] std::uint32_t consumer_count() const noexcept {
        return static_cast<std::uint32_t>(shards_.size());
    }

private:
    // --- registration internals -------------------------------------------

    producer commission(shard_state& sh, std::uint32_t i) {
        producer_slot& sl = slots_[i];
        try {
            // Allocated here, on the registering thread, so first touch places
            // the pages on this producer's NUMA node. An explicit
            // options::numa_node overrides that for the cases first touch
            // cannot reach -- see detail/numa.hpp -- and the guard has to wrap
            // the construction rather than the assignment, because the ring's
            // storage is allocated inside the channel's constructor.
            //
            // A recycled slot keeps its ring. The free-running indices are
            // already consistent -- head == tail was checked at reclaim -- so
            // reuse costs no allocation at all. It does mean a recycled ring
            // keeps its original node placement.
            if (!sl.channel) {
                const detail::numa::scoped_node place{opt_.numa_node};
                sl.channel = detail::make_aligned<Channel>(opt_.ring_capacity);
            }
        } catch (...) {
            sl.state.store(slot_state::free, std::memory_order_release);
            throw;
        }
        sl.channel->reset_stats();

        producer p;
        p.pipe_ = this;
        p.ch_ = sl.channel.get();
        p.sh_ = &sh;
        p.tok_ = sh.active.token_for(i - sh.begin);
        p.id_ = i;
        p.shard_ = sl.shard;
        p.full_ = opt_.full;
        p.push_spin_ = opt_.push_spin;

        // Release: publishes the channel pointer and the reset counters. The
        // consumer only dereferences channel after an acquire load that sees
        // active or retiring, which is what makes `claiming` load-bearing.
        sl.state.store(slot_state::active, std::memory_order_release);
        return p;
    }

    void retire(std::uint32_t id, shard_state& sh, const typename active_set_t::token& tok) noexcept {
        // Release so the consumer, on seeing retiring, also sees the final
        // pushes. The slot stays unusable until the consumer publishes free.
        slots_[id].state.store(slot_state::retiring, std::memory_order_release);
        sh.retire_pending.store(true, std::memory_order_release);
        // Make sure whatever is still in the ring gets drained rather than
        // waiting for the next unrelated producer to wake the consumer.
        if (sh.active.signal(tok)) wake_if_parked(sh);
        wake(sh);
    }

    // --- consumer ----------------------------------------------------------

    template <class S>
    void consume(shard_state& sh, S& sink, std::uint32_t index) {
        detail::set_thread_name(opt_.thread_name);
        if (!opt_.pin_consumers_to.empty()) {
            if (!detail::pin_to_cpu(opt_.pin_consumers_to[index]))
                report("could not pin a consumer thread; tail latency will be worse");
        }

        std::uint32_t idle = 0;
        while (running_.load(std::memory_order_acquire)) {
            const std::size_t n = drain_pass(sh, sink);
            sh.passes.bump();
            bump_rotate(sh);
            reclaim_if_pending(sh);
            if (n > 0) {
                idle = 0;
                continue;
            }

            // Everything below runs only on a pass that found no work, so none
            // of it is on the hot path.
            if (++idle < cfg_.spin_before_park) {
                TRIBUTARY_PAUSE();
                continue;
            }
            if (idle < cfg_.spin_before_park * 2) {
                std::this_thread::yield();
                continue;
            }
            if (idle_work(sh, sink)) {
                idle = 0;
                continue;
            }
            park(sh);
            idle = 0;
        }
        final_drain(sh, sink);
    }

    // O(active) when the bitmap is in use.
    template <class S>
    std::size_t drain_pass(shard_state& sh, S& sink) {
        if (cfg_.scan == scan_policy::full_scan) return drain_full(sh, sink);

        std::size_t total = 0;
        sh.active.visit(sh.rotate, [&](std::uint32_t local) {
            if (local >= sh.count) return;  // bit beyond this shard's slots
            sh.rings_probed.bump();
            const std::size_t n = drain_ring(sh, sink, sh.begin + local);
            if (n == 0) sh.rings_empty.bump();
            total += n;
        });
        if (total > 0) sh.drain.flush(sink);
        return total;
    }

    // O(registered). The naive version, kept both as a policy and as the
    // pre-park safety net below.
    template <class S>
    std::size_t drain_full(shard_state& sh, S& sink) {
        std::size_t total = 0;
        for (std::uint32_t k = 0; k < sh.count; ++k) {
            std::uint32_t off = k + sh.rotate;
            if (off >= sh.count) off -= sh.count;  // rotate is kept < count
            const std::uint32_t i = sh.begin + off;
            const auto st = slots_[i].state.load(std::memory_order_acquire);
            if (st == slot_state::free || st == slot_state::claiming) continue;
            sh.rings_probed.bump();
            const std::size_t n = drain_ring(sh, sink, i);
            if (n == 0) sh.rings_empty.bump();
            total += n;
        }
        if (total > 0) sh.drain.flush(sink);
        return total;
    }

    // The null check stays here rather than in the strategy: a slot can be
    // observed `claiming` with no ring yet, which is a fact about the lifetime
    // protocol above and not about how events are drained.
    template <class S>
    std::size_t drain_ring(shard_state& sh, S& sink, std::uint32_t i) {
        Channel* ch = slots_[i].channel.get();
        if (ch == nullptr) return 0;

        const std::size_t got = sh.drain.take(*ch, sink);
        sh.consumed.bump(got);
        return got;
    }

    // The idle path. Returns true if anything moved, in which case the caller
    // should go round again rather than sleeping.
    template <class S>
    bool idle_work(shard_state& sh, S& sink) {
        // Unconditional, not gated on retire_pending. A slot that finished
        // draining after the flag was consumed would otherwise stay retiring
        // forever with its bitmap bit set, and the consumer would spin instead
        // of ever parking.
        sh.retire_pending.store(false, std::memory_order_relaxed);
        reclaim_retired(sh);

        // The only place a sink holding a partially written batch gets unstuck
        // when no new events are arriving.
        if (sh.drain.pending()) {
            bool progressed = false;
            if constexpr (has_on_idle<S>) progressed = sink.on_idle();
            // Not short-circuited: the retry is the point of being here, so the
            // flush has to happen whatever on_idle() reported.
            const bool moved = sh.drain.flush(sink) > 0;
            if (moved || progressed) return true;
        } else if constexpr (has_on_idle<S>) {
            if (sink.on_idle()) return true;
        }

        // Clear stale bits only here, on the way to sleep -- not on every empty
        // pass. A lightly loaded pipeline goes briefly empty between arrivals,
        // and clearing there would have the consumer clear a bit the producer
        // immediately sets again: write churn on the shared line, precisely
        // proportional to throughput, which is the thing this design exists to
        // avoid. It also has to happen before park() checks the bitmap, or a
        // stale bit would keep the consumer spinning forever.
        sh.active.reap([this, &sh](std::uint32_t local) { return still_busy(sh.begin + local); });

        // Defense in depth: one unconditional full scan immediately before
        // sleeping. The bitmap protocol above is believed correct, but the cost
        // of being wrong is an event stranded in a ring nobody probes, and that
        // is not a failure worth being clever about. Going to sleep is the only
        // moment where being wrong becomes unbounded, and it is also the moment
        // an O(slots) scan is affordable.
        return drain_full(sh, sink) > 0;
    }

    // True if ring i still holds events, or its producer is retiring and the
    // slot has not been reclaimed yet.
    bool still_busy(std::uint32_t i) noexcept {
        producer_slot& sl = slots_[i];
        const auto st = sl.state.load(std::memory_order_acquire);
        if (st == slot_state::free || st == slot_state::claiming) return false;
        if (st == slot_state::retiring) return true;
        return sl.channel && !sl.channel->empty_now();
    }

    // Runs on every pass, including busy ones. The load is an acquire on a line
    // the consumer already owns -- an L1 hit -- and the O(slots) scan behind it
    // happens only when a producer has actually retired.
    //
    // Gating this on the idle path alone (the obvious placement, since the scan
    // is O(slots)) starves registration under sustained load: a consumer with
    // work to do never goes idle, so retired slots are never published free,
    // and new producers cannot register even though slots are logically
    // available. Measured on the churn test, 40 waves of 8 producers through 16
    // slots: 144 of 320 registrations succeeded before this, 320 after.
    void reclaim_if_pending(shard_state& sh) noexcept {
        if (!sh.retire_pending.load(std::memory_order_acquire)) return;
        // Clear before scanning, so a retirement that lands during the scan
        // sets the flag again and is picked up next pass rather than lost.
        sh.retire_pending.store(false, std::memory_order_relaxed);
        reclaim_retired(sh);
    }

    // Only the consumer publishes free, and only after the ring is empty.
    void reclaim_retired(shard_state& sh) noexcept {
        for (std::uint32_t i = sh.begin; i < sh.end; ++i) {
            producer_slot& sl = slots_[i];
            if (sl.state.load(std::memory_order_acquire) != slot_state::retiring) continue;
            if (sl.channel && !sl.channel->empty_now()) continue;

            // Roll the departing producer's counters into the lifetime totals
            // before the slot becomes reusable. Registration resets the
            // per-slot ones, so without this a recycled slot erases its
            // predecessor's history and the accounting stops adding up.
            if (sl.channel) {
                retired_pushed_.add(sl.channel->take_pushed());
                retired_dropped_.add(sl.channel->take_dropped());
            }
            sh.active.clear(i - sh.begin);
            sl.state.store(slot_state::free, std::memory_order_release);
        }
    }

    template <class S>
    void final_drain(shard_state& sh, S& sink) {
        // Whatever is published and reachable still goes out, so a drain
        // shutdown loses nothing. Full scan regardless of policy -- at
        // shutdown, correctness beats scan cost. Two passes because a ring
        // emptied in the first may have been refilled by a producer that had
        // not yet observed accepting_ == false.
        for (int pass = 0; pass < 2; ++pass) {
            while (drain_full(sh, sink) > 0) bump_rotate(sh);
        }

        // Give a backpressured sink a bounded number of chances at the tail
        // rather than dropping it at the very last step.
        for (int attempt = 0; attempt < 64 && sh.drain.pending(); ++attempt) {
            if constexpr (has_on_idle<S>) (void)sink.on_idle();
            sh.drain.flush(sink);
            if (sh.drain.pending()) std::this_thread::yield();
        }
        if (sh.drain.pending()) report("sink would not accept the final batch");

        reclaim_retired(sh);
        if constexpr (has_close<S>) sink.close();
    }

    // --- parking -----------------------------------------------------------

    void park(shard_state& sh) {
        std::unique_lock lk(sh.park_mu);
        sh.parked.store(true, std::memory_order_seq_cst);
        // Re-check after arming, or a producer that signalled between our last
        // scan and this store would find parked false, skip the notify, and
        // leave us asleep on an already-non-empty ring. Both sides are seq_cst
        // for exactly this: it is the same Dekker pair as the bitmap, one level
        // up.
        if (!sh.active.any() && running_.load(std::memory_order_acquire)) {
            sh.parks.bump();
            // Always timed: a bounded worst case beats trusting the wakeup path
            // to be perfect.
            sh.park_cv.wait_for(lk, cfg_.park_timeout);
        }
        sh.parked.store(false, std::memory_order_seq_cst);
    }

    // Called by the producer that took the shard from empty to non-empty. The
    // parked load is seq_cst -- the other half of park()'s handshake.
    void wake_if_parked(shard_state& sh) noexcept {
        if (!sh.parked.load(std::memory_order_seq_cst)) return;
        std::lock_guard lk(sh.park_mu);
        sh.park_cv.notify_one();
        sh.notifies.add();  // genuinely multi-writer, so an RMW is required
    }

    void wake(shard_state& sh) noexcept {
        std::lock_guard lk(sh.park_mu);
        sh.park_cv.notify_all();
    }

    void wake_all() noexcept {
        for (auto& sh : shards_) wake(*sh);
    }

    // --- helpers -----------------------------------------------------------

    static void bump_rotate(shard_state& sh) noexcept {
        sh.rotate = (sh.rotate + 1 < sh.count) ? sh.rotate + 1 : 0;
    }

    // Called from the stopping thread, which is neither producer nor consumer,
    // so it must use size_now(): the cached indices are private to their owning
    // threads and reading them here would be a race.
    bool all_quiesced() noexcept {
        for (std::uint32_t i = 0; i < slots_.size(); ++i) {
            const auto st = slots_[i].state.load(std::memory_order_acquire);
            if (st == slot_state::free || st == slot_state::claiming) continue;
            if (slots_[i].channel && slots_[i].channel->size_now() != 0) return false;
        }
        return true;
    }

    void report(const char* what) const noexcept {
        if (!opt_.on_error) return;
        try {
            opt_.on_error(what);
        } catch (...) {  // NOLINT(bugprone-empty-catch) -- nowhere left to report to
        }
    }

    // --- state -------------------------------------------------------------

    // Read by every producer on every push, written only at start and stop, so
    // this line sits Shared in every core and costs an L1 hit. Kept away from
    // anything a consumer writes -- see shard_state.
    alignas(cache_line) std::atomic<bool> accepting_{false};

    alignas(cache_line) std::atomic<bool> running_{false};

    alignas(cache_line) hot_config cfg_{};

    alignas(cache_line) TRIBUTARY_NO_UNIQUE_ADDRESS counter_t retired_pushed_;
    TRIBUTARY_NO_UNIQUE_ADDRESS counter_t retired_dropped_;
    TRIBUTARY_NO_UNIQUE_ADDRESS counter_t registration_failures_;
    std::atomic<std::uint32_t> next_shard_{0};

    options opt_;
    detail::aligned_array<producer_slot, cache_line> slots_;
    std::vector<detail::aligned_ptr<shard_state>> shards_;
    bool started_ = false;
    bool drain_deadline_met_ = true;
};

// --- the two event models -------------------------------------------------

// Fixed-size, trivially copyable events. A push is a memcpy into preallocated
// storage: no allocation, no indirection, no shared ownership on the hot path.
template <class T, class Traits = default_traits>
using pipeline = basic_pipeline<fixed_channel<T, Traits>, Traits>;

}  // namespace TRIBUTARY_ABI
}  // namespace tributary
