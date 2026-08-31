#pragma once
//
// "Which producers might have work" -- a read-mostly bitmap, so the consumer
// scans O(active) rings instead of O(registered).
//
// WHY A SHARED WORD NEED NOT BE A CONTENDED WORD
//
// A cache line *read* by 64 cores sits in Shared state in all 64 caches at once
// and costs each an L1 hit. Read sharing is genuinely free; cost appears only on
// writes, which require exclusive ownership and invalidate every other copy. So
// the design rule is not "avoid shared state", it is "make shared state
// read-mostly". A producer streaming millions of events per second sets its bit
// once and then never touches the line again.
//
// THE ASYMMETRY THAT MAKES IT WORK
//
// A set bit means "maybe non-empty". A false positive costs one wasted probe; a
// false negative loses a wakeup and strands a published event in a ring nobody
// looks at. So bits are set eagerly and cleared lazily -- only on the consumer's
// path to sleep, never after each drain. Clearing on every empty drain produces
// write churn proportional to throughput: a lightly loaded ring goes briefly
// empty between arrivals, the consumer clears, the producer immediately re-sets,
// and the line ping-pongs on every event. That is the contention this mechanism
// exists to avoid, reintroduced by the mechanism itself.
//
// TWO LEVELS
//
// Up to 64 producers is one word and there is no summary at all -- the common
// case pays nothing for the generalisation. Above that, a summary word carries
// one bit per leaf word. Correctness of the second level rests on one
// observation: reap() only clears a summary bit after reading its whole leaf
// word as zero, and a thread sees its own prior stores. So a set leaf bit can
// never have its summary cleared out from under it, and a producer whose leaf
// bit is already set need not look at the summary at all.

#include "../traits.hpp"
#include "alloc.hpp"
#include "counter.hpp"

#include <atomic>
#include <bit>
#include <cstdint>

namespace tributary {
inline namespace TRIBUTARY_ABI {
namespace detail {

template <class Traits>
class active_set {
public:
    static constexpr std::size_t cache_line = Traits::cache_line;
    static constexpr std::uint32_t bits_per_word = 64;

    // Everything signal() needs, cached in the producer's handle so the push
    // path reads no shared metadata line at all -- just its own leaf.
    struct token {
        std::atomic<std::uint64_t>* leaf = nullptr;
        std::atomic<std::uint64_t>* summary = nullptr;  // null when single-word
        std::uint64_t bit = 0;
        std::uint64_t word_bit = 0;
    };

    explicit active_set(std::uint32_t capacity)
        : words_((capacity + bits_per_word - 1) / bits_per_word), leaves_(words_) {}

    active_set(const active_set&) = delete;
    active_set& operator=(const active_set&) = delete;

    std::uint32_t words() const noexcept { return words_; }

    token token_for(std::uint32_t i) noexcept {
        const std::uint32_t w = i / bits_per_word;
        token t;
        t.leaf = &leaves_[w].bits;
        t.summary = (words_ == 1) ? nullptr : &summary_;
        t.bit = std::uint64_t{1} << (i % bits_per_word);
        t.word_bit = std::uint64_t{1} << w;
        return t;
    }

    // --- producer side -----------------------------------------------------

    // MUST be called after the producer's release publish. Returns true if this
    // call took the whole set from empty to non-empty, i.e. a parked consumer
    // may need waking. Only the producer that made that transition returns
    // true, so a burst across eight producers costs one notify, not eight.
    bool signal(const token& t) noexcept {
        // This fence is load-bearing and cannot be moved below the check.
        //
        // We have just published our tail with a release store, and are about
        // to decide, from the bitmap, that the consumer already knows about us.
        // x86 is TSO, which reorders exactly one pair -- StoreLoad -- so without
        // a barrier this load may execute before the publish drains the store
        // buffer. Then: we read a set bit, skip the fetch_or, and return; the
        // consumer concurrently sees the ring empty (our store not yet
        // visible), clears the bit, re-checks, still sees empty, and parks. The
        // event is now published with its bit clear, so the consumer will not
        // even probe this ring, and it sits there until this producer happens to
        // push again -- which it may never do.
        //
        // Release/acquire cannot express StoreLoad; this is the one ordering TSO
        // does not give away for free. Checking the bit first and fencing only
        // on the slow path -- the obvious optimisation -- reintroduces the bug
        // exactly, because it is the *unfenced check itself* that is unsound.
        std::atomic_thread_fence(std::memory_order_seq_cst);
        if (t.leaf->load(std::memory_order_relaxed) & t.bit) return false;

        const std::uint64_t prev_leaf = t.leaf->fetch_or(t.bit, std::memory_order_seq_cst);
        writes_.add();

        if (t.summary == nullptr) return prev_leaf == 0;  // single word: leaf is the summary

        // Some other producer in this word already had the set non-empty. Its
        // signal() set the summary bit, and reap() can only clear that bit after
        // reading the whole leaf word as zero -- which it is not, because our
        // bit and theirs are in it. The summary is therefore already correct.
        if (prev_leaf != 0) return false;

        // We took this word from empty, so the summary bit is ours to set.
        // Dekker one level up: our leaf fetch_or above is seq_cst (an RMW, hence
        // a full barrier) and precedes this store, while reap()'s summary
        // fetch_and is seq_cst and precedes its leaf load. Either reap sees our
        // leaf bit and re-sets the summary, or our fetch_or lands after its
        // clear. Both interleavings leave the summary set.
        const std::uint64_t prev_summary = t.summary->fetch_or(t.word_bit, std::memory_order_seq_cst);
        writes_.add();
        return prev_summary == 0;
    }

    // --- consumer side -----------------------------------------------------

    // Calls f(index) for each producer whose bit is set. `rotate` moves the
    // starting bit each pass: countr_zero scans from the LSB, so a fixed order
    // would drain producer 0 first every time and starve the high indices under
    // sustained overload.
    template <class F>
    void visit(std::uint32_t rotate, F&& f) {
        if (words_ == 1) {
            visit_word(leaves_[0].bits.load(std::memory_order_acquire), 0, rotate, f);
            return;
        }
        std::uint64_t s = summary_.load(std::memory_order_acquire);
        while (s != 0) {
            const auto w = static_cast<std::uint32_t>(std::countr_zero(s));
            s &= s - 1;
            visit_word(leaves_[w].bits.load(std::memory_order_acquire), w * bits_per_word, rotate,
                       f);
        }
    }

    // Idle path only. `still_busy(i)` must be an authoritative, cache-refreshing
    // check: true if ring i still holds events or its producer is retiring.
    template <class Q>
    void reap(Q&& still_busy) {
        for (std::uint32_t w = 0; w < words_; ++w) {
            std::atomic<std::uint64_t>& leaf = leaves_[w].bits;
            std::uint64_t m = leaf.load(std::memory_order_acquire);
            while (m != 0) {
                const auto b = static_cast<std::uint32_t>(std::countr_zero(m));
                m &= m - 1;
                const std::uint64_t bit = std::uint64_t{1} << b;
                if (still_busy((w * bits_per_word) + b)) continue;

                // The other half of the Dekker pair in signal(): clear first,
                // then look again. If a producer published before our clear, the
                // re-check sees its event; if it publishes after, it sees the
                // cleared bit and sets it itself. fetch_and is an RMW and so
                // already a full barrier -- no explicit fence needed here.
                leaf.fetch_and(~bit, std::memory_order_seq_cst);
                writes_.add();
                if (still_busy((w * bits_per_word) + b)) {
                    leaf.fetch_or(bit, std::memory_order_release);
                    writes_.add();
                }
            }
            if (words_ == 1) continue;
            maintain_summary(w, leaf);
        }
    }

    // Load half of the park handshake, hence seq_cst: the consumer arms
    // parked_, then reads this; the producer sets bits, then reads parked_.
    bool any() const noexcept {
        if (words_ == 1) return leaves_[0].bits.load(std::memory_order_seq_cst) != 0;
        return summary_.load(std::memory_order_seq_cst) != 0;
    }

    // Consumer-only, used when a producer slot is reclaimed. The summary is left
    // alone: a stale set summary bit is a false positive, which costs one wasted
    // word scan, and reap() will clear it.
    void clear(std::uint32_t i) noexcept {
        const std::uint32_t w = i / bits_per_word;
        leaves_[w].bits.fetch_and(~(std::uint64_t{1} << (i % bits_per_word)),
                                  std::memory_order_relaxed);
        writes_.add();
    }

    std::uint64_t writes() const noexcept { return writes_.get(); }

private:
    struct alignas(cache_line) leaf_word {
        std::atomic<std::uint64_t> bits{0};
    };

    template <class F>
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters) -- private, three call sites
    static void visit_word(std::uint64_t snapshot, std::uint32_t base, std::uint32_t rotate,
                           F& f) {
        const int r = static_cast<int>(rotate % bits_per_word);
        // Bit k of the rotated word is bit (k + r) mod 64 of the snapshot.
        std::uint64_t m = std::rotr(snapshot, r);
        while (m != 0) {
            const auto off = static_cast<std::uint32_t>(std::countr_zero(m));
            m &= m - 1;
            f(base + ((off + static_cast<std::uint32_t>(r)) % bits_per_word));
        }
    }

    void maintain_summary(std::uint32_t w, std::atomic<std::uint64_t>& leaf) noexcept {
        const std::uint64_t wbit = std::uint64_t{1} << w;
        if (leaf.load(std::memory_order_seq_cst) == 0) {
            summary_.fetch_and(~wbit, std::memory_order_seq_cst);
            writes_.add();
            // Same Dekker as the leaf level, one tier up.
            if (leaf.load(std::memory_order_seq_cst) != 0) {
                summary_.fetch_or(wbit, std::memory_order_release);
                writes_.add();
            }
            return;
        }
        // Repair. A producer preempted between its leaf fetch_or and its summary
        // fetch_or leaves a non-empty word with no summary bit, and visit()
        // would never look at it -- the events would wait for the park timeout.
        // Sweeping every word here, rather than only the ones the summary points
        // at, closes that window for the cost of an O(words) read on the idle
        // path. The load is relaxed and usually finds the bit already set, so
        // the common case writes nothing.
        if ((summary_.load(std::memory_order_relaxed) & wbit) == 0) {
            summary_.fetch_or(wbit, std::memory_order_release);
            writes_.add();
        }
    }

    using counter_t = counter<Traits::collect_stats>;

    // Read-only after construction, so this line sits Shared everywhere and is
    // never invalidated. The push path does not read it at all -- see token.
    std::uint32_t words_;

    // Written by producers, but only on empty -> non-empty word transitions.
    alignas(cache_line) std::atomic<std::uint64_t> summary_{0};

    // Cold: incremented only when a bit actually changes, which is what makes
    // this the counter that proves coalescing is holding.
    alignas(cache_line) TRIBUTARY_NO_UNIQUE_ADDRESS counter_t writes_;

    // One cache line per word, so producers in different words never share.
    aligned_array<leaf_word, cache_line> leaves_;
};

}  // namespace detail
}  // namespace TRIBUTARY_ABI
}  // namespace tributary
