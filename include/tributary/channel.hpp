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
// The split between the two concepts below is the whole point of the seam, and
// it is not arbitrary: `channel_for` is what the pipeline itself touches --
// registration, retirement, the wakeup handshake, stats -- and every one of
// those is the same whether events are fixed-size or variable-length.
// `pop_batch` is the one member that is *not* channel-agnostic. It copies out
// into a caller-provided array of `value_type`, which presumes both a staging
// buffer and a fixed element size; a zero-copy channel hands the sink a span of
// its own memory instead and has no such member. So it belongs to the drain
// strategy's requirement, not the pipeline's. See drain.hpp.

#include "traits.hpp"

#include <concepts>
#include <cstddef>
#include <cstdint>

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

}  // namespace TRIBUTARY_ABI
}  // namespace tributary
