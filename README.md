# tributary

A header-only C++20 library for getting records from many producer threads to one or more consumers,
fast, without unbounded memory and without a producer ever blocking indefinitely.

Many streams joining one river.

- **Adding producer threads does not make pushing slower.** A push costs ~22–29 ns whether 4 threads
  are pushing or 32.
- **Idle producers cost the consumer almost nothing.** With 64 producers registered and only 8
  sending, the consumer checks ~8 rings per loop, not all 64.
- **Each doubling of consumers nearly doubles throughput.** 1.98× with two, 3.87× with four.

> **Status: in development.** Phases A and B are complete and their gates pass — Phase B's gate is a
> clean ThreadSanitizer run on both x86-64 and ARM64. Phase C (variable-length records) has not
> started. See [docs/PLAN.md](docs/PLAN.md) for the roadmap and the phase gates. Not yet released,
> not yet API-stable.

## What it's for

Hot paths that generate small records faster than one consumer can write them, where stalling a
producer is worse than losing a record:

- **Structured and application logging.** Every worker thread emits records; one thread owns the
  file or socket. Each thread's lines stay in order, and an overload burst is dropped and counted
  instead of turning your request handlers into a queue for the disk.
- **Metrics, tracing, and telemetry.** Counters and spans from every thread in the process,
  funnelled to one exporter. A sampling gap is acceptable; blocking a request to emit telemetry is
  not.
- **High-rate event ingestion.** Feed handlers, sensor streams, packet events — many independent
  sources, one processing thread, and a hard requirement that no producer ever waits on another.
- **Profiling and instrumentation.** Collecting samples from every thread without the collector
  perturbing the thing it is measuring.
- **Game and simulation loops.** Worker threads submitting events to a single consumer that drains
  them at a frame boundary.

The shape it fits: **many producers, small trivially-copyable records, per-producer ordering is
enough, and bounded loss beats unbounded latency.** If you need ordering *across* producers, or your
event rate is moderate, a mutex and a `std::deque` is the better engineering choice — see
[The trade you are making](#the-trade-you-are-making).

## How it works

```text
producer 1 ──> bounded SPSC ring ──┐
producer 2 ──> bounded SPSC ring ──┤
producer 3 ──> bounded SPSC ring ──┼──> consumer ──> batch ──> sink
producer N ──> bounded SPSC ring ──┘
```

Each producer owns exactly one ring and writes only its own index. The consumer owns the read index.
There is no shared writable state on the push path except one read-mostly bitmap word, and a
streaming producer writes to that word **once** and then never again.

## Quickstart

```cpp
#include <tributary/pipeline.hpp>

#include <cstddef>
#include <cstdint>
#include <span>
#include <thread>
#include <vector>

struct event {                       // trivially copyable, small, no indirection
    std::uint32_t producer_id;
    std::uint32_t seq;
    std::int64_t  stamp_ns;
    std::byte     payload[16];
};

// A sink is any type with a noexcept write() that returns how much it accepted.
struct alignas(tributary::default_traits::cache_line) counting_sink {
    std::size_t write(std::span<const event> b) noexcept {
        records += b.size();
        return b.size();             // accepted all of it
    }
    std::uint64_t records = 0;
};

int main() {
    tributary::options opt;
    opt.max_producers = 8;
    opt.ring_capacity = 4096;        // per producer, power of two

    tributary::pipeline<event> pipe(opt);

    counting_sink sink;
    pipe.start(sink);                // must precede any push

    std::vector<std::thread> producers;
    for (std::uint32_t p = 0; p < 4; ++p)
        producers.emplace_back([&pipe] {
            auto h = pipe.register_producer();  // move-only RAII; retires on destruction
            if (!h) return;                     // no free slot -- check it
            for (std::uint32_t s = 1; s <= 50'000; ++s)
                h.push(event{h.id(), s, 0, {}});  // false means dropped -- always check
        });
    for (auto& t : producers) t.join();

    pipe.stop(tributary::stop_mode::drain);     // lossless, bounded by a deadline

    const auto st = pipe.snapshot();
    // st.pushed + st.dropped == what was offered, exactly.
    return st.dropped == 0 ? 0 : 1;
}
```

`start()` is what makes the pipeline accept records, so it must come before any `push()` — a push
before `start()` or after `stop()` returns `false` and is counted, not queued.

`namespace trib = tributary;` is the suggested short alias. The library does not define it — that is
your namespace to shorten, not ours.

### More than one consumer

Set `consumers` and hand `start()` one sink pointer per shard. Each shard owns a disjoint,
contiguous range of producer slots, and a producer is bound to its shard for the life of the handle.

```cpp
opt.consumers = 4;
tributary::pipeline<event> pipe(opt);

counting_sink  sinks[4];
counting_sink* ptrs[4] = {&sinks[0], &sinks[1], &sinks[2], &sinks[3]};
pipe.start(std::span<counting_sink* const>(ptrs, 4));
```

Anything other than exactly one sink per consumer throws `std::invalid_argument`. Sharding is worth
it when the **sink**, not the queue, is the bottleneck — see the numbers below.

## Measured

> All figures: **32-core x86-64, clang 21, `-O2`, Release.** They are machine-specific and
> meaningless elsewhere. Reproduce with `./bin/tributary_bench`, which exits non-zero on any missed
> target.

**Push cost and throughput.** Each row is a separate run with that many producer threads pushing at
once. `no-signal` is the bare ring; `+bitmap` is the real push path, including the wakeup handshake.

```text
           |     ns/push     |     ns/push     |  sustained M/s
           |    no-signal    |     +bitmap     |
producers  | target     got  | target     got  | target     got
1          |   61.5    14.0  |   99.3    40.1  |    9.9    23.1
4          |   18.5    16.3  |   37.3    29.1  |  107.0   333.9
16         |   19.4    13.0  |   35.4    22.0  |  362.6   638.0
32         |   29.4    10.4  |   41.7    22.2  |  135.8   619.1
```

The column that matters is `ns/push (no-signal)`: **10–16 ns, and it does not rise as the producer
count goes from 4 to 32.** That is the whole design — a shared tail would show push cost *climbing*
as the contended line migrates between cores, so a flat cost is the evidence that there is nothing
being contended over. Read it as slope, not as an absolute.

**`+bitmap` costs ~9–26 ns/push on top.** That is the price of a correct wakeup handshake. It is not
free and is not reported here as though it were.

**Consumer scan width.** The consumer loops; each time round is a **pass**, and checking one
producer's ring for work is a **probe**. This run holds 8 producers sending at 100k rec/s each and
varies how many *idle* producers are also registered — the case the active bitmap exists for.

```text
                   |    p50 us     |  probes/pass  | bitmap writes
registered  scan   | target    got |  target   got | target    got
8           full   |    0.5   0.30 |     8.0  8.00 |    n/a     16
8           bitmap |    0.5   0.30 |     8.0  7.99 |      8     16
16          full   |    1.5   0.40 |    15.9 15.99 |    n/a     16
16          bitmap |    1.2   0.30 |     7.8  7.99 |      8     16
32          full   |    2.2   0.50 |    31.9 31.98 |    n/a     16
32          bitmap |    1.1   0.30 |     7.9  7.99 |      8     16
64          full   |    3.3   0.50 |    63.9 63.94 |    n/a     16
64          bitmap |    1.3   0.30 |     7.9  7.99 |      8     16
```

Read the `probes/pass` column. `full_scan` tracks the registered count (8 → 16 → 32 → 64), so every
idle producer is checked on every pass, forever. `bitmap` stays flat at ~8 — the number actually
sending — however many are registered. Latency follows: flat at 0.30 µs against a full scan rising
to 0.50.

**16 bitmap writes for 800k records** — two per producer. Before the lazy-clear fix, the same shape
of run wrote the bitmap **582,954** times. That single counter is how you know notification
coalescing still holds.

**Sharded throughput.**

```text
shards  producers/shard | sustained M/s   vs 1 shard | busiest shard
1       8               |         25.00       1.00x  |       100.0%
2       4               |         49.51       1.98x  |        50.8%
4       2               |         96.86       3.87x  |        25.9%
```

Two things this table is not:

- **The M/s figures are deliberately sink-bound and are not throughput figures for the library.**
  This phase uses a sink with a fixed per-record cost on purpose, because sharding only helps when
  the sink is the bottleneck; against a near-free sink one consumer already outruns eight producers
  and the column comes out flat. Read the *ratios*. The throughput table above has the throughput.
- **Sharding buys cores.** S shards means S consumer threads. This is a per-machine claim, not a
  per-core one.

**p99 and p99.9 are printed by the benchmark but deliberately not gated.** They swing several-fold
between runs in both directions from the same binary; treating them as a threshold would mean
chasing noise.

**ThreadSanitizer.** Run against `6184cf8`, clang 18.1.3, `RelWithDebInfo`:

```text
leg                 result                    slowest suites
linux-x64-release   6/6 passed  (control)
linux-x64-tsan      6/6 passed
linux-arm64-tsan    6/6 passed  28.9 s total  test_sharding 16.0 s, test_pipeline 11.4 s
```

Clean on the first real run, on both architectures. ARM64 is the leg that counts — see
[Design](#design).

## Writing a sink

```cpp
struct alignas(tributary::default_traits::cache_line) my_sink {
    // Return how many elements you accepted. Less than b.size() is
    // backpressure: the pipeline keeps the remainder and re-offers it.
    std::size_t write(std::span<const event> b) noexcept;

    // Optional. Called before the consumer sleeps and on every park timeout,
    // so a partially written remainder gets retried even when no new records
    // arrive. Without this a socket that returned EAGAIN strands its tail.
    // Return true if progress was made.
    bool on_idle() noexcept;

    // Optional. Called once, on the consumer thread, after the final drain.
    void close() noexcept;
};
```

Only `write` is required; `on_idle` and `close` are detected at compile time via the `has_on_idle`
and `has_close` concepts. Returning more than you were offered is clamped, not trusted.

`write` **must** be `noexcept` — the consumer thread has no handler above it, so an escaping
exception is a `std::terminate`. Wrap a throwing sink in `tributary::unsafe_sink`, which reports the
exception and treats the batch as consumed rather than retrying it forever.

The `alignas` is not decoration. Each shard's sink is written by its own consumer thread and by no
other, so two sinks sharing a cache line put two consumers in a write-write ping-pong on that line
for the life of the run — a scaling ceiling that no test fails on and that looks like "sharding just
doesn't help much". Aligning the sink type is what makes `my_sink sinks[4]` above safe; it is also
why `start()` takes a span of *pointers* rather than a span of sinks, so the four need not be
contiguous at all.

A sink must never buffer an unbounded remainder of its own. That converts a latency problem into a
memory problem, and the memory problem arrives later, larger, and as an OOM kill. Return a short
count and let the rings absorb it — they are sized for exactly that, and they have a drop policy for
when they run out.

`tributary::null_sink` accepts and discards everything; benchmark against it to find the pipeline's
ceiling with the sink removed. [test/sinks.hpp](test/sinks.hpp) has worked examples, including a
socket-shaped one implementing all three members.

## What it guarantees

- **Per-producer ordering.** Not global ordering. That is the trade that buys the whole
  architecture.
- **`pushed + dropped == offered`, exactly.** Drops are counted per producer, never silent.
- **Bounded memory.** `max_producers × ring_capacity × sizeof(T)`, and rings are allocated on
  registration, so unused slots cost nothing.
- **Producers never block indefinitely.** Both overload policies terminate; the only question is how
  long they try.
- **A `drain` stop loses nothing**, bounded by a deadline. `drain_completed()` tells you if the
  deadline was met or the drain degraded to an abort.
- **No allocation on the hot path.** Ever.
- **Failure is visible, not fatal.** A registration that finds no free slot returns an invalid
  handle and increments `registration_failures`; pushing through it returns `false` rather than
  crashing.

## Build and integration

Header-only. CMake ≥ 3.20, a C++20 compiler, and `Threads`. No other dependencies.

```cmake
find_package(tributary REQUIRED)
target_link_libraries(app PRIVATE tributary::tributary)
```

Or vendor it — tests and benchmarks turn themselves off when the project is not top-level:

```cmake
add_subdirectory(external/tributary)
target_link_libraries(app PRIVATE tributary::tributary)
```

| CMake option | Default | Meaning |
|---|---|---|
| `TRIBUTARY_BUILD_TESTS` | on when top-level | Build the test suite (7 CTest targets) |
| `TRIBUTARY_BUILD_BENCH` | on when top-level | Build `tributary_bench` |
| `TRIBUTARY_SANITIZER` | *(empty)* | `address`, `thread`, `undefined`, or `address,undefined`. Needs clang or gcc |
| `TRIBUTARY_CACHE_LINE` | *(empty)* | Override the cache-line size used for layout |

`TRIBUTARY_CACHE_LINE` is baked into the library's inline-namespace ABI tag, so linking a 64-byte
build against a 128-byte one is a **link error rather than silent memory corruption**.

Build and test:

```sh
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DTRIBUTARY_BUILD_TESTS=ON
cmake --build build && ctest --test-dir build --output-on-failure
```

The benchmark is deliberately **not** a CTest target — it runs for minutes and is the regression
guard, not a unit test. Run it explicitly before and after anything that touches the hot loop:

```sh
./bin/tributary_bench     # exit code is the verdict
```

## Driving the consumer yourself

By default the library owns a thread per shard. Set `own_threads = false` and it owns none, in
either of two shapes.

**You own the thread, the library owns the loop** — `run()` blocks until shutdown:

```cpp
opt.own_threads = false;
opt.consumers   = shards;
tributary::pipeline<event> pipe(opt);

std::vector<my_sink>  sinks(shards);
std::vector<my_sink*> ptrs;
for (auto& s : sinks) ptrs.push_back(&s);
pipe.start(std::span<my_sink* const>(ptrs.data(), ptrs.size()));

std::vector<std::thread> consumers;
for (std::uint32_t s = 0; s < shards; ++s)
    consumers.emplace_back([&pipe, &sinks, s] { pipe.run(s, sinks[s]); });

// ... producers run ...

pipe.stop(tributary::stop_mode::drain);
for (auto& t : consumers) t.join();   // stop() has no threads of its own to join here
```

**You own the loop** — `poll()` does one pass and returns what it drained, so it drops into an
existing reactor:

```cpp
opt.own_threads = false;
pipe.start(sink);

while (serving) {
    do_your_other_work();
    pipe.poll(0, sink);               // one shard, one pass
}

pipe.stop(tributary::stop_mode::drain);
while (pipe.poll(0, sink) > 0) { }    // finish the drain
pipe.close(0, sink);                  // gives the sink its close()
```

After `stop()`, keep calling `poll()` until it returns 0, then `close()`. That sequence is what
makes the final drain lossless when the library is not running the loop.

## Observability

Every mechanism in this design can fail silently, so every mechanism is counted. `snapshot()`
returns a `stats` for the whole pipeline; `per_producer()` fills a vector of `producer_stats` with
the per-slot breakdown.

The counters that answer operational questions:

| Field | Question it answers |
|---|---|
| `dropped`, `drop_fraction()` | Are we losing records, and what share? |
| `sink_backpressure` | Is the far side the bottleneck? Rising means the rings are about to drop |
| `registration_failures` | Are callers silently unable to produce at all? |
| `high_water` | How deep did any ring actually get? Proves memory stayed bounded |

And the two that say whether the design is still doing what it claims:

| Field | Healthy |
|---|---|
| `probes_per_pass()` | ≈ the *active* producer count, not the registered count |
| `bitmap_writes` | Negligible next to the record count — ~2 per producer, not per push |

`producer_stats` adds `id`, `shard`, `active`, `retiring`, `pushed`, `dropped`, `high_water`, and
sampled `depth` — because "the pipeline is dropping" is less useful than "producer 37 is dropping".

Reading per-producer counters pulls the producer's cache line to Shared. That is fine at monitoring
frequencies; do not put a stats scrape in a hot loop. If you want the counters gone entirely,
instantiate with `no_stats_traits` — they compile away to nothing and shrink the hot cache lines.

## Tuning

`options` is validated in the pipeline constructor, which throws `std::invalid_argument` on a bad
combination. `validate()` returns the same message without constructing, if you would rather check
first.

| Field | Default | Meaning |
|---|---|---|
| `max_producers` | `64` | Producer slots. Rings are allocated on registration, so an unused slot costs nothing |
| `consumers` | `1` | Consumer shards. Each needs its own sink |
| `ring_capacity` | `4096` | Per producer, in elements. Power of two — the mask replaces a modulo |
| `batch_capacity` | `2048` | Consumer staging buffer. One sink call covers up to this many records |
| `drain_batch` | `512` | Fairness cap: records taken from one ring in one pass |
| `full` | `drop_newest` | Or `spin_then_drop`. Both terminate; see below |
| `push_spin` | `200` | `spin_then_drop` budget, in pause iterations |
| `scan` | `bitmap` | Or `full_scan`, which is genuinely faster below a handful of producers |
| `spin_before_park` | `2000` | Idle passes before the consumer sleeps |
| `park_timeout` | `200 µs` | Missed-wakeup backstop. Always timed |
| `own_threads` | `true` | `false` hands the consumer loop back to you |
| `thread_name` | `"tributary"` | Applied to library-owned consumer threads |
| `pin_consumers_to` | *(empty)* | One CPU id per shard. Empty leaves consumers unpinned |
| `numa_node` | `-1` | `-1` is first touch. A node number binds every allocation the pipeline makes |
| `on_error` | *(empty)* | Cold path only — registration failures, sink exceptions, a missed drain deadline. **Never called from the push path** |

**Overload policy.** `drop_newest` fails immediately and counts it — right when staleness beats
stalling. `spin_then_drop` spins for `push_spin` first, for bursty traffic the consumer will absorb.
Overwrite-oldest is deliberately absent: it cannot be done safely in a plain SPSC ring, because the
producer would overwrite a slot the consumer may be mid-read. That needs sequence-numbered slots and
reader validation — a different data structure.

**Placement is best-effort.** NUMA binding and CPU pinning both degrade to the default and report
through `on_error` rather than refusing to start, because whether a node or a CPU exists is a
property of the machine, not of your configuration. Nothing in this library's correctness depends on
placement. The tail-latency story does assume the consumer does not migrate, so pin it in
production.

## Roadmap

- **Done.** Fixed-size records, per-producer SPSC rings, the active-bitmap wakeup handshake,
  consumer sharding, bounded two-mode shutdown, NUMA binding and consumer pinning, the drain seam.
- **Next (Phase C).** Variable-length records: a `bytes_channel`, zero-copy `try_claim`/`commit`,
  and frame-boundary partial release. `basic_pipeline`'s third template parameter is the seam this
  plugs into — the drain strategy — so the ordering arguments do not have to be written twice.
- **Later (Phase D).** `DESIGN.md`, MSVC and macOS in CI, the `TRIBUTARY_CACHE_LINE=128` build,
  clang-tidy and clang-format gates, and an `example/` built against the installed package.

The design arguments — memory ordering, the bitmap proof, the layout rules — currently live in
[docs/PLAN.md](docs/PLAN.md) and in the header comments, which are written to be read. `DESIGN.md`
will collect them.

---

# Engineering notes

## The trade you are making

This design is ~1,400 lines of code with a Dekker handshake, a four-state slot lifetime protocol,
and a `seq_cst` fence whose absence causes a rare, load-dependent stall. A mutex-protected
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

- **One bounded SPSC ring per producer**, so there is no producer-producer contention to reason
  about and the memory model stays simple: two release/acquire pairs, one per direction, no atomic
  RMW on either side of the ring.
- **A read-mostly active bitmap** so the consumer scans O(active) rings rather than O(registered). A
  cache line *read* by 64 cores sits Shared in all 64 caches and costs each an L1 hit — read sharing
  is free, and only writes cost. The design rule is not "avoid shared state", it is "make shared
  state read-mostly".
- **Bits set eagerly, cleared lazily**, only on the consumer's path to sleep. Clearing after each
  empty drain produces write churn proportional to throughput, which is the exact contention the
  mechanism exists to remove. The measured difference is 16 bitmap writes against 582,954.
- **One `seq_cst` fence**, on the producer side of the wakeup handshake. StoreLoad is the one
  ordering x86's TSO does not give away for free, and release/acquire cannot express it.

That last point is why **ARM64 is the CI leg that counts**. x86 is TSO and reorders exactly one
pair, StoreLoad — precisely the one the handshake exists to survive. A green x86 run is not evidence
that the ordering is correct; only aarch64 can say that.

## Platform support

**Tested in CI**, clang only: `linux-x64-release`, `linux-x64-tsan`, `linux-arm64-tsan`.

**Supported but not yet in CI:** Windows and macOS, MSVC and gcc, ASan/UBSan legs, and the
`TRIBUTARY_CACHE_LINE=128` build. These are Phase D. Treat them as "should work" rather than
"verified".

Known platform ceilings, all of them reported rather than silent:

- NUMA node numbers above 63 are not supported; machines with more nodes need a multi-word mask and
  a processor-group-aware Windows path.
- On Windows, pinning to a CPU id ≥ 64 fails — one processor group.
- **On macOS, CPU pinning is a no-op.** macOS exposes no portable hard affinity, and reporting
  failure through `on_error` is more honest than pretending it worked.
- NUMA binding is compiled in on Windows and on Linux with `mbind`; elsewhere `numa::supported()` is
  `false` and allocation falls back to first touch.

`numa.hpp` is implemented but has never run on a multi-node machine — single-node dev boxes and CI
runners exercise the code path and the allocator pairing, not the placement itself.

## Requirements

C++20 (concepts, `std::span`, `<bit>`). No dependencies, including in the tests.

## License

TBD.
