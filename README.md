# tributary

A header-only C++20 library for getting records from many producer threads to one consumer, fast,
without unbounded memory and without a producer ever blocking indefinitely.

Many streams joining one river.

```
producer 1 ──> bounded SPSC ring ──┐
producer 2 ──> bounded SPSC ring ──┤
producer 3 ──> bounded SPSC ring ──┼──> consumer ──> batch ──> sink
producer N ──> bounded SPSC ring ──┘
```

Each producer owns exactly one ring and writes only its own index. The consumer owns the read index.
There is no shared writable state on the push path except one read-mostly bitmap word, and a
streaming producer writes to that word **once** and then never again.

> **Status: in development.** The core is being built in phases; see
> [docs/PLAN.md](docs/PLAN.md) for the roadmap and the phase gates. Not yet released, not yet
> API-stable.

## Why not just use a mutex and a deque?

Often you should, and this library says so out loud. See [When not to use this](#when-not-to-use-this).

## Quickstart

```cpp
#include <tributary/pipeline.hpp>

struct event {                       // trivially copyable, small, no indirection
    std::uint32_t producer_id;
    std::uint32_t seq;
    std::int64_t  stamp_ns;
    std::array<std::byte, 16> payload;
};

tributary::options opt;
opt.max_producers  = 256;
opt.consumers      = 4;              // shards; one sink each
opt.ring_capacity  = 4096;           // per producer, power of two
opt.full           = tributary::full_policy::drop_newest;

tributary::pipeline<event> pipe(opt);

my_sink sinks[4];
my_sink* ptrs[4] = {&sinks[0], &sinks[1], &sinks[2], &sinks[3]};
pipe.start(std::span<my_sink* const>(ptrs, 4));

// On each producer thread:
auto p = pipe.register_producer();   // move-only RAII; retires on destruction
if (p) p.push(ev);                   // returns false if dropped -- always check

pipe.stop(tributary::stop_mode::drain);
```

Variable-length records use the same machinery with a different ring:

```cpp
tributary::byte_pipeline logs(opt);  // opt.ring_capacity is bytes here
auto p = logs.register_producer();
p.push(std::as_bytes(std::span(line)));
```

`namespace trib = tributary;` is the suggested short alias. The library does not define it — that is
your namespace to shorten, not ours.

## Writing a sink

```cpp
struct alignas(tributary::default_traits::cache_line) my_sink {
    // Return how many elements you accepted. Less than b.size() is
    // backpressure: the pipeline keeps the remainder and re-offers it.
    std::size_t write(std::span<const event> b) noexcept;

    // Optional. Called before the consumer sleeps and on every park timeout,
    // so a partially written remainder gets retried even when no new records
    // arrive. Without this a socket that returned EAGAIN strands its tail.
    bool on_idle() noexcept;

    // Optional. Called once after the final drain.
    void close() noexcept;
};
```

`write` **must** be `noexcept` — the consumer thread has no handler above it, so an escaping
exception is a `std::terminate`. Wrap a throwing sink in `tributary::unsafe_sink`.

The `alignas` is not decoration. Each shard's sink is written by its own consumer thread and by no
other, so two sinks sharing a cache line put two consumers in a write-write ping-pong on that line
for the life of the run — a scaling ceiling that no test fails on and that looks like "sharding just
doesn't help much". Aligning the sink type is what makes `my_sink sinks[4]` in the quickstart safe;
it is also why `start()` takes a span of *pointers* rather than a span of sinks, so the four need not
be contiguous at all.

A sink must never buffer an unbounded remainder of its own. That converts a latency problem into a
memory problem, and the memory problem arrives later, larger, and as an OOM kill. Return a short
count and let the rings absorb it — they are sized for exactly that, and they have a drop policy for
when they run out.

## What it guarantees

- **Per-producer ordering.** Not global ordering. That is the trade that buys the whole architecture.
- **`pushed + dropped == offered`, exactly.** Drops are counted per producer, never silent.
- **Bounded memory.** `max_producers × ring_capacity × sizeof(T)`, and rings are allocated on
  registration, so unused slots cost nothing.
- **Producers never block indefinitely.** Both overload policies terminate; the only question is how
  long they try.
- **A `drain` stop loses nothing**, bounded by a deadline. `drain_completed()` tells you if the
  deadline was met or the drain degraded to an abort.
- **No allocation on the hot path.** Ever.

## When not to use this

The honest list. This design is ~700 lines with a Dekker handshake, a four-state slot lifetime
protocol, and a `seq_cst` fence whose absence causes a rare, load-dependent stall. A mutex-protected
`std::deque` is ten lines and is obviously correct on inspection. That difference is a permanent
engineering cost paid by everyone who touches the code afterwards.

Prefer the mutex when:

- **Your event rate is moderate.** Below ~100k records/s the lock is essentially never contended and
  the entire justification evaporates. Most systems that think they need this do not.
- **You need global ordering.** Then per-producer rings are simply wrong, and merging the streams
  afterwards costs more than the lock ever did.
- **Your producer count is small**, or producers are numerous and short-lived — the registration
  protocol and `N × capacity` memory start to dominate.
- **Correctness matters more than throughput** — anything financial, medical, or safety-related. The
  mutex version can be reviewed by anyone; this one needs a reviewer fluent in the C++ memory model.

**Measure first.** The sophisticated answer is not automatically the right one.

## Design

[docs/DESIGN.md](docs/DESIGN.md) covers the memory ordering argument, the two-level bitmap proof, the
cache-line layout rules, and the failure modes. The short version:

- **One bounded SPSC ring per producer**, so there is no producer-producer contention to reason about
  and the memory model stays simple: two release/acquire pairs, one per direction, no atomic RMW on
  either side of the ring.
- **A read-mostly active bitmap** so the consumer scans O(active) rings rather than O(registered). A
  cache line *read* by 64 cores sits Shared in all 64 caches and costs each an L1 hit — read sharing
  is free, and only writes cost. The design rule is not "avoid shared state", it is "make shared
  state read-mostly".
- **Bits set eagerly, cleared lazily**, only on the consumer's path to sleep. Clearing after each
  empty drain produces write churn proportional to throughput, which is the exact contention the
  mechanism exists to remove.
- **One `seq_cst` fence**, on the producer side of the wakeup handshake. StoreLoad is the one
  ordering x86's TSO does not give away for free, and release/acquire cannot express it.

## Design notes

The ordering arguments, the bitmap proof, the layout invariants, and the cases where a mutex and a
deque are the better engineering choice are written up in [docs/DESIGN.md](docs/DESIGN.md). The
roadmap and the gate each phase has to pass are in [docs/PLAN.md](docs/PLAN.md).

## Requirements

C++20 (concepts, `std::span`, `<bit>`). Linux, Windows, and macOS on x86_64 and ARM64. No
dependencies.

## License

TBD.
