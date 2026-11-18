#pragma once
//
// What the pipeline requires of a channel, stated rather than discovered.
//
// `basic_pipeline<Channel, ...>` took an unconstrained `class Channel` while
// sinks -- the easier of the two to get right -- were already constrained by
// `sink_for`. A channel missing one member produced a template error from
// somewhere inside the consumer loop, naming a line of pipeline.hpp and saying
// nothing at all about the channel.
//
// There are three concepts here rather than one, and the axis each splits on is
// the whole point of the seam:
//
//   channel_for        what the pipeline itself touches -- registration,
//                      retirement, the wakeup handshake, stats. Every one of
//                      those is the same whether events are fixed-size or
//                      variable-length, so this is the part no channel escapes.
//   staged_channel     + pop_batch, the *consumer* side. It copies out into a
//                      caller-provided array of `value_type`, which presumes
//                      both a staging buffer and a fixed element size.
//   claimable_channel  + try_claim/commit, the *producer* side. Reserve ring
//                      memory, write into it directly, publish.
//
// The last two are not opposites and a channel may satisfy either, both, or
// neither. They are separate because they are the two ends of the same
// avoid-a-copy argument, and a channel can want one end without the other.
// `fixed_channel` satisfies staged_channel only, and that is not a gap in it:
// for a 32-byte event the copy is ~1-2 ns and buys batching across many rings
// into one sink call, which is the better trade at that size.

#include "traits.hpp"

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <span>

namespace tributary {
inline namespace TRIBUTARY_ABI {

// A bounded SPSC channel the pipeline can own, register, drain and account for.
//
// noexcept throughout is not decoration. All of it runs on either the push path
// or the consumer thread, and the consumer has no handler above it -- an
// escaping exception there is a std::terminate, not an error.
template <class C>
concept channel_for = requires(C& ch, const C& cch, const typename C::value_type& v) {
    typename C::value_type;
    typename C::batch_type;

    // Capacity is the only construction parameter, deliberately. Ring storage
    // is allocated inside this constructor, on the registering thread, so first
    // touch places it; NUMA placement rides in ambiently rather than widening
    // this signature. See detail/numa.hpp for why that is the right seam.
    requires std::constructible_from<C, std::size_t>;

    // --- producer side: one thread, the one holding the handle --------------
    { ch.try_push(v) } noexcept -> std::same_as<bool>;
    { ch.note_drop() } noexcept;

    // --- consumer side: one thread, the shard's --------------------------
    { ch.empty_now() } noexcept -> std::same_as<bool>;

    // --- observer: a third thread scraping stats, hence const ---------------
    { cch.size_now() } noexcept -> std::convertible_to<std::uint64_t>;
    { cch.pushed() } noexcept -> std::convertible_to<std::uint64_t>;
    { cch.dropped() } noexcept -> std::convertible_to<std::uint64_t>;
    { cch.high_water() } noexcept -> std::convertible_to<std::uint64_t>;

    // --- slot recycling: consumer only, and only once the slot is retiring --
    { ch.take_pushed() } noexcept -> std::convertible_to<std::uint64_t>;
    { ch.take_dropped() } noexcept -> std::convertible_to<std::uint64_t>;
    { ch.reset_stats() } noexcept;
};

// Additionally drainable by copying out: the requirement `staged_drain` adds,
// and the one a variable-length channel will not satisfy.
template <class C>
concept staged_channel =
    channel_for<C> && requires(C& ch, typename C::value_type* out, std::size_t max_items) {
        { ch.pop_batch(out, max_items) } noexcept -> std::convertible_to<std::size_t>;
    };

// Additionally writable in place: reserve space, fill it, publish. This is what
// `producer::try_claim`/`commit` need, and the reason they are constrained --
// `try_push` copies a whole event in, which is the thing a 500-byte log line
// does not want to pay for twice.
//
// `claim_type` is the channel's own type rather than a bare span so it can carry
// back whatever `commit` needs to finish the publish -- a frame offset, the wrap
// state, a padding frame it had to emit first. The pipeline never looks inside
// it; it only hands it back.
//
// There is deliberately no `abort`. An uncommitted claim publishes nothing, so
// abandoning one costs the space until the next `try_claim` reuses it. A channel
// whose `try_claim` has side effects that need unwinding -- one that emits a
// pad-to-wrap frame eagerly, say -- is the case that would justify adding it,
// and none exists yet.
template <class C>
concept claimable_channel =
    channel_for<C> && requires(C& ch, std::size_t n, typename C::claim_type& cl) {
        typename C::claim_type;

        // Reserving must not publish anything: the release-store that makes the
        // event visible belongs in commit, because that is what the wakeup
        // handshake has to be sequenced after.
        { ch.try_claim(n) } noexcept -> std::same_as<typename C::claim_type>;

        // False when the reservation failed -- the caller must check, exactly as
        // it must check the bool from `push`.
        { cl.valid() } noexcept -> std::same_as<bool>;
        { cl.bytes() } noexcept -> std::same_as<std::span<std::byte>>;

        // `n` here is the count actually used, which may be less than was
        // claimed. Committing more than was claimed is the caller's bug.
        { ch.commit(cl, n) } noexcept;
    };

}  // namespace TRIBUTARY_ABI
}  // namespace tributary
