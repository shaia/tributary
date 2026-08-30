# tributary — roadmap

Approved roadmap. All paths are relative to this file, and the repository is self-contained: nothing
here — build, test, benchmark, or gate — depends on any sibling project.

## Context

The problem: many producer threads generate small records and push them to one consumer that batches
them to a sink. Very low latency, millions of records per second, minimal allocation, bounded memory,
producers must not block indefinitely, and order preserved **per producer only**.

That last clause is the one that grants rather than constrains. Global ordering could only be
established at a shared point of serialisation, and every shared point is a written cache line on the
push path. Per-producer ordering means no producer has to agree with another about anything, so the
answer is one bounded SPSC ring per producer, a single consumer draining them all, and a read-mostly
active-producer bitmap with a Dekker wakeup handshake so the consumer does not poll idle rings.

The ring, the slot lifetime protocol, the bitmap handshake, and the bounded two-mode shutdown are
domain-agnostic. What a library has to add on top is runtime sizing rather than a compile-time
capacity and record type, more than 64 producers, sharded consumers, variable-length records, and the
operational surface: an exception boundary, thread placement, NUMA, packaging, and TSan in CI.

---

## Four decisions that look like details and are not

Each of these is a shape the design deliberately does **not** have. Every one of them is silent when
wrong — no test fails, nothing crashes, the throughput is simply worse or the data is simply gone.
Document all four in `DESIGN.md`.

**1. Producer counters never share a line with the consumer's probe metadata.**
The tempting layout puts `ring` (8B), `state` (1B) and `pushed`/`dropped`/`high_water` (24B) together
in one slot struct — all inside the first 64-byte line. The producer writes those counters on *every
push*; the consumer reads `state` and dereferences `ring` on *every probe*, and under `full_scan`
reads `state` for every slot on every pass. The line ping-pongs between cores.

A trailing `pad[cache_line]` does not help, and reasoning about it is how the mistake survives review:
`alignas(64)` on the struct already separates slot *i* from slot *i+1*. The separation that matters is
*within* the slot.

→ Counters live next to `tail_` in the ring's producer-owned line, which the producer already holds
exclusive on every publish, so the write is free. `{channel, state}` is a consumer-read-mostly line.
Verified with `-Xclang -fdump-record-layouts`, not by inspection.

**2. Single-writer counters use a relaxed store, not an RMW.**
`pushed`/`dropped` have exactly one writer while the slot is `active` — the consumer's `exchange(0)`
in `reclaim_retired` only runs once the slot is `retiring` and the producer is gone. `fetch_add` would
be correct and needlessly expensive.

→ `detail::counter::bump()` is a relaxed load plus a relaxed store, keeping a `lock xadd`
(~15–20 cycles) out of an 18–29 ns push. `add()` remains for the genuinely multi-writer counters.

**3. A backpressured sink must never be able to strand its remainder.**
If `flush()` is reached only when a drain pass moved records, and returns early when nothing is
staged, then a sink holding a partial write has no way to retry it once the traffic stops. The
remainder sits there indefinitely — and it sits there precisely when the far side is in trouble.

→ `sink::write` returns an accepted count and the pipeline retains the remainder; an optional
`on_idle()` is called before parking and on every park timeout.
`test_backpressure.cpp::test_stalled_sink_recovers` holds this: a sink that refuses, then recovers,
with no new records arriving to drive another flush.

**4. Retired slots are reclaimed on every pass, not only when idle.**
Gating reclamation behind the idle path — after an `if (n > 0) continue;` — means it never runs on a
busy pass. A consumer with work to do never goes idle, so retired slots are never published `free` and
new producers cannot register even though slots are logically available. Every such failure is a
producer thread that silently produces nothing at all.

Measured on the churn test (40 waves of 8 producers through 16 slots): **144 of 320 registrations
succeeded**. A churn test that only asserts *registered* producers' records survive will never catch
this — it has to assert that registration itself succeeds.

→ `reclaim_if_pending()` runs on **every** pass. Its gate is an acquire load of a line the consumer
already owns — an L1 hit — and the O(slots) scan behind it happens only when a producer has actually
retired. 320 of 320 after, and the churn test got 2.5× faster. Regression-guarded in
`test_lifetime.cpp`.

---

## The two genuinely new pieces

### Two-level active bitmap (`detail/active_set.hpp`) — done

Up to 64 producers is one word with no summary at all. Above that, a summary word carries one bit per
leaf word, and there is one `active_set` **per consumer shard** so shards never share a written line.

The hot path stays at one contended line even with >64 producers. The reason is worth keeping written
down, because it is the part that is easy to break later:

- **Producer fast path reads only its leaf word** — `fence(seq_cst); if (leaf & bit) return;`.
- **The producer never checks the summary.** If it saw its leaf bit set, then by the Dekker argument
  the consumer's post-`fetch_and` re-check must observe the ring non-empty and re-set the leaf bit.
  `reap()` only clears a summary bit after reading `leaves_[w] == 0`, and a thread sees its own prior
  stores — so the summary cannot be cleared out from under a set leaf bit.
- **Slow path** (`prev_leaf == 0`): unconditional `summary_.fetch_or`. Dekker one level up — the leaf
  `fetch_or` is `seq_cst` (an RMW, hence a full barrier) before the summary op, and `reap()`'s
  summary `fetch_and` is `seq_cst` before its leaf load. Either reap sees the leaf bit and re-sets the
  summary, or the `fetch_or` lands after the clear.
- **Summary repair.** A producer preempted between its leaf and summary writes leaves a non-empty word
  with no summary bit, which `visit()` would never look at. `reap()` sweeps *every* word rather than
  only the ones the summary points at, closing that window for an O(words) read on the idle path.
  The alternative is to let the park timeout cover it, which turns a cheap idle-path read into a
  latency spike whose cause is invisible.
- **Notify** only when the whole shard went empty → non-empty. This is what keeps bitmap writes
  proportional to producer count rather than to throughput.

### Variable-length records (`bytes_channel`, `record.hpp`) — Phase C

A bip-style ring with free-running byte counters over a power-of-two buffer, and **pad-to-wrap**: when
a record won't fit before the wrap point, the producer emits a padding frame filling the tail and
writes the record at offset 0. No record straddles the wrap, so `peek()` returns a contiguous,
frame-aligned span with **zero copy** — the sink writes ring memory directly and there is no staging
buffer on this path at all.

```cpp
struct record_header {              // 8 bytes
    std::uint32_t frame_length;     // total incl. header, multiple of 8, >= 8
    std::uint16_t type;             // frame_type::data | frame_type::padding
    std::uint16_t slack;            // 0..7; payload_length = frame_length - 8 - slack
};
```

Padding frames reach the sink as an explicit, documented wire element (as Aeron does), which keeps
`peek()` O(1) instead of a header walk. `for_each_record(span, fn)` skips them for in-process
decoders.

Partial accept rounds the sink's accepted count **down to a frame boundary** and releases only that,
re-offering the rest. The sink never buffers, so memory stays bounded by the rings — this is what
makes backpressure end-to-end rather than deferred into a `pending_` vector.

**Why fixed and bytes channels differ:** for 32-byte records the staging copy is ~1–2 ns and buys
batching across many rings into one sink call. For a 500-byte log line the copy is real and there are
fewer records per batch, so cross-ring batching matters less than avoiding the copy. Different
regimes, different answers — not an inconsistency, and say so in `DESIGN.md`.

#### The `Channel` parameter is not yet the seam this needs

`basic_pipeline<Channel, Traits>` looks like the extension point that will admit `bytes_channel`. It
is not one yet, and finding that out at the start of Phase C means choosing under pressure between
forking the pipeline and threading `if constexpr` through five methods. Decide it before then.

**What the pipeline requires of `Channel` today, none of it stated.** The parameter is an
unconstrained `class Channel`, while sinks — the easier of the two to get right — are constrained by
`sink_for`. The actual requirement is a `(std::size_t capacity)` constructor, the typedefs
`value_type` and `batch_type`, and eleven members: `try_push`, `note_drop`, `pop_batch`, `empty_now`,
`size_now`, `pushed`, `dropped`, `high_water`, `take_pushed`, `take_dropped`, `reset_stats`. A channel
missing one of them produces a template error from inside `drain_ring`, which says nothing about the
channel.

**Why `bytes_channel` cannot satisfy it.** Two independent reasons, either one fatal on its own:

- `drain_ring` copies out of the ring into `shard_state::stage`, a `std::vector<value_type>`, and
  `flush` compacts it with a `memmove` sized by `sizeof(value_type)`. The design above is zero-copy
  with **no staging buffer on that path at all**, and the Phase C gate is "no staging copy in the
  profile". The staging model is not an implementation detail of the consumer loop; it is the record
  model, spelled into it.
- `flush` advances `stage_head` by whatever count the sink returned. For a byte batch that count can
  land mid-frame — exactly what the design above forbids, since a partial accept must round **down to
  a frame boundary** before anything is released.

Note also that `options::batch_capacity` is documented in elements and has no meaning at all for a
channel with no staging buffer, while `ring_capacity` is already documented as elements-or-bytes.

**Where the split actually falls.** Channel-agnostic, and worth writing exactly once: the slot
lifetime protocol, the Dekker wakeup handshake, park/wake, `reclaim_retired`, the two-mode bounded
shutdown, stats aggregation. Fixed-channel-specific: `shard_state::stage`/`staged`/`stage_head`,
`drain_ring`, `flush`, and the meaning of `batch_capacity`. The first group is most of `pipeline.hpp`
and it is precisely the code the "things that are easy to get wrong" list is about — a fork would
duplicate the `seq_cst` fence ordering, the consumer-only publish of `free`, the unconditional
pre-park `reclaim_retired`, and the `claiming` intermediate state, and would then have to keep two
copies of each correct under a sanitizer that only runs in CI.

→ Either extract the drain path into a `drain_strategy<Channel>` policy — staging for
`fixed_channel`, zero-copy for `bytes_channel` — and add a `channel_for` concept that states the
requirement above, or fork `basic_pipeline` and accept the duplication. Record which, and why, in
`DESIGN.md`.

**The gate on doing it.** Either way this is a behaviour-preserving refactor of the measured hot loop,
so it is gated on Phase A's throughput phase re-run before and after: `ns/push` unchanged and still
flat from 4 to 32 producers. Read it as slope, not as an absolute number — the same way the Phase A
target is read, and for the same reason. Do it while `fixed_channel` is still the only channel: a
refactor is far easier to prove free when only one instantiation has to keep passing.

---

## Phases

Each ends at a gate that must pass before the next starts. Check in with the user at each gate.

### Phase A — core, fixed-size, single consumer

- [x] `traits.hpp`, `detail/alloc.hpp`, `detail/counter.hpp`
- [x] `fixed_channel<T>` — runtime capacity; `mask_`/`slots_` duplicated into both the producer and
      consumer lines (read-only, 16 bytes, removes a third line from every operation)
- [x] `detail/active_set.hpp`
- [x] `sink.hpp` — accepted-count concept, `on_idle`/`close` detection, `unsafe_sink` (decision 3)
- [x] `detail/thread.hpp`
- [x] `pipeline.hpp` — slot registry, consumer loop, stop modes, stats (decisions 1, 2 and 4)
- [x] Rings allocated at `register_producer`, not eagerly — an eager constructor commits
      `max_producers × ring_capacity × sizeof(T)` up front, 8 MiB at the defaults, whether or not the
      producers ever appear
- [x] Tests: ring unit + 2-thread stress, ordering/accounting, lifetime churn, backpressure, shutdown
      — 5 suites, all passing, zero warnings, ASan clean
- [x] **Gate:** `bench/` built and its four phases meeting the targets below — **81 checks, 0 failed**

#### Phase A gate targets

Four self-checking phases in one binary, non-zero exit on any violated invariant. The numeric targets
below are for **this machine — 32-core x86-64, clang 21, `-O2`, Release** — and are meaningless on
other hardware; re-establish them before reading a result on anything else.

**1. Correctness under stress.** 16 producers × 200k records, run under both scan policies. Asserts
per-producer sequence numbers arrive strictly increasing (gaps only where a drop was counted),
`pushed + dropped == offered` exactly, a `drain` stop loses nothing, and ring depth never exceeded
capacity. Plus producer churn: 40 waves × 8 producers recycled through the slots, asserting both that
every record from a retired producer is still drained **and that all 320 registrations succeed**.
Pass/fail only — no timing target.

**2. Throughput.** Push cost and overload are measured by **two separate runs**, because they cannot
share one. Push cost comes from paced bursts that fit; overload comes from a saturating loop. Warm-up
discarded, median of 3.

```text
           |     ns/push     |     ns/push     |  sustained M/s
           |    no-signal    |     +bitmap     |
producers  | target     got  | target     got  | target     got
1          |   61.5    14.0  |   99.3    40.1  |    9.9    23.1
4          |   18.5    16.3  |   37.3    29.1  |  107.0   333.9
16         |   19.4    13.0  |   35.4    22.0  |  362.6   638.0
32         |   29.4    10.4  |   41.7    22.2  |  135.8   619.1
```

**Every target beaten**, and sustained throughput by 3–4.5× at 16 and 32 producers.

The column that matters is `ns/push (no-signal)`: **10–16 ns, and it does not climb from 4 to 32
producers.** That is the whole design — a shared tail would show push cost *climbing* as the
contended line migrates between cores. Read it as slope, not as an absolute number, and read it
**one-sided**: cost *falling* as producers are added is the absence of contention, not a regression,
and a symmetric bound would fail the design for succeeding. (The 1-producer figure differs because a
lone producer outruns nothing and spends its time on a ring the consumer is actively draining; it is
not the interesting case.)

`+bitmap` costs ~9–26 ns/push on top: the price of a correct wakeup handshake. It is not free and
should not be reported as though it were.

Two methodology notes, both load-bearing:

- **Push cost cannot be measured on a saturating run.** Once the rings are full a failed `try_push`
  is a load and a compare — several times cheaper than a real push — and at 16 producers against one
  consumer most attempts fail. The column would then report the cost of *not* pushing, and would look
  impressively fast doing it. Measured here on paced bursts instead, with the success rate asserted
  above 99% so an invalid measurement fails rather than misleads.
- **`offered`/`accepted%` are deliberately absent from the table above.** The saturating run counts
  *attempts*, cheap failures included, so its offered figure (~4900 M/s at 32 producers) is not on the
  same yardstick as the target's 767.6 and comparing them would be meaningless. The pair still
  describes the drop policy correctly — 12.5% accepted at 32 producers, the single consumer being the
  ceiling and the surplus dropped by policy, the system behaving as designed — and the benchmark
  prints both.

**3. Backpressure and shutdown** against a deliberately slow sink: drops occur rather than blocking,
memory stays bounded, no producer blocks indefinitely, `abort` returns promptly, and a stalled sink
that later recovers still drains. Pass/fail only.

**4. Scan A/B.** 8 active producers at 100k rec/s each, varying how many idle producers are also
registered.

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

`got` is 8 active producers at 100k rec/s, 5 reps of 1 s, median per statistic, drop-free throughout.
**Every p50 target is beaten, by 3–5×**, and probes/pass matches to two decimals.

Three things are asserted, and one is deliberately not:

- `probes/pass` for `full_scan` tracks registration (8 → 16 → 32 → 64) while `bitmap` stays flat at
  ~8 regardless. This is the follow-up's claim, measured rather than assumed.
- **16 bitmap writes for 800k records** — two per producer, against a run that offered 800,000.
  Coalescing is the mechanism; this number is how you know it still holds. Before the lazy-clear fix
  the same shape of run wrote the bitmap **582,954** times.

  Two notes on this column. The count is **two** per producer, not one: a streaming producer sets its
  bit once, the consumer clears it on its way to sleep, and the producer sets it again on the next
  record. That is coalescing working, not leaking. And it is **non-zero under `full_scan` too** — the
  `n/a` above — because `scan_policy` here selects the scan *only* and the wakeup handshake runs
  under both. A target of `0` there would presume a design where `full_scan` also drops the
  handshake, which would vary two things at once and is exactly what this A/B refuses to do.
- `full_scan` p50 rises with registration (0.30 → 0.40 → 0.50 → 0.50 µs) while `bitmap` stays flat at
  0.30 throughout. p50 is the column that reproduces across runs, to within one 0.1 µs tick.
- **p99 and p99.9 are not a gate criterion and must not be made one.** Observed directly on two
  consecutive runs of the finished benchmark: `bitmap` p99 at 64 registered went **9.8 µs → 38.0 µs**
  while `full_scan` p99 at 8 registered went **28.3 µs → 7.3 µs**. Roughly 4×, in opposite
  directions, from the same binary minutes apart. The tail here is dominated by OS preemption and
  park/wake latency, both far larger than the scan cost being removed. On this machine, at this
  scale, the bitmap is a median-latency and CPU-efficiency optimisation, not a tail-latency one.
  Report the columns, draw no conclusion from them.

Sharding, `poll()`/`run()` and thread placement landed early with the pipeline (they were cheaper to
build in than to retrofit), but are only lightly tested — Phase B owns proving them, and now does:
`test/test_sharding.cpp` and `bench/sharding.cpp`.

### Phase B — production integration surface

- [x] Two-level `active_set` exercised with >64 producers
- [x] Sharded consumers: contiguous slot ranges, per-shard bitmap and park state on their own lines
- [x] `own_threads == false` + `poll()` / `run()`
- [ ] `detail/numa.hpp` (`VirtualAllocExNuma` / `mbind`), first-touch as the portable default
- [x] `options::validate()` wired, `on_error` wired
- [x] Tests: missed-wakeup stress, >64 producers, shard disjointness, externally-driven mode —
      `test/test_sharding.cpp`
- **Gate:** TSan clean on Linux x86_64 **and ARM64**; sharded throughput scales with shard count

#### Two deviations from the plan as written, recorded rather than left to be inferred

**1. The TSan CI legs were pulled forward from Phase D.** This gate cannot be read from a development
machine here — clang rejects `-fsanitize=thread` for `x86_64-pc-windows-msvc` — so for this phase CI
is not a packaging concern, it is the only instrument that can read the gate at all.
`.github/workflows/ci.yml` therefore exists early and holds **three legs only**: Linux x86-64 Release
as a control, and TSan on x86-64 and ARM64. Everything else Phase D lists — clang-tidy and
clang-format gates, MSVC, macOS, the `TRIBUTARY_CACHE_LINE=128` build, the installed-`example/` smoke
test — is still Phase D and still unwritten.

**2. `detail/numa.hpp` is deferred.** It is the one item here with no correctness argument riding on
it: placement is a runtime property of the machine, `detail::make_aligned` is already the single
choke point it plugs into, and no part of the gate depends on it. Phase B is **not** complete until
it lands or is explicitly moved.

#### Sharded throughput — the half of the gate that can be measured locally

`bench/sharding.cpp`, phase 5, on the same 32-core x86-64 / clang 21 / `-O2` Release machine as the
Phase A targets:

```text
shards  producers/shard | sustained M/s   vs 1 shard | busiest shard
1       8               |         25.00       1.00x  |       100.0%
2       4               |         49.51       1.98x  |        50.8%
4       2               |         96.86       3.87x  |        25.9%
```

Gated at **≥1.5× per doubling**. Perfect 2× was never available — the rings, the bitmap and the
producers are shared work that does not halve — so a symmetric target would fail the design for being
imperfect rather than for being wrong. `busiest shard` is reported beside it because scaling measured
on an unbalanced run would be measuring the imbalance.

The sink cost is the load-bearing choice here, and it is the same methodological rule as the scan
A/B's one level up: **measure the mechanism in the regime it exists for.** Sharding helps when the
*sink*, not the queue, is the bottleneck. Against the near-free sink the other phases use, one
consumer already outruns eight saturating producers, the shard column comes out flat, and the phase
reports a true number that reads as "sharding does not work". Phase 5's sink therefore carries a
deliberate fixed per-record cost — which is also why its M/s figures are **not** throughput figures
for the library. Phase 2 has those.

### Phase C — variable-length records

- Decide the drain seam **first**, while `fixed_channel` is still the only channel —
  `drain_strategy<Channel>` plus a `channel_for` concept, or a fork of `basic_pipeline`. See "The
  `Channel` parameter is not yet the seam this needs" above. Gated on an unchanged throughput phase.
- `record.hpp`, `bytes_channel`, zero-copy `try_claim`/`commit`, frame-boundary partial release
- Tests: wrap-with-padding, exact payload round-trip, zero-length and maximum-size records, and a
  partial-accept sink asserting the output is exactly the concatenation of what was pushed
- **Gate:** ASan + TSan clean; no staging copy in the profile

### Phase D — packaging, CI, docs

- Install rules, export set, `tributary-config.cmake`, generated `version.hpp`
- `example/` built against the **installed** package as a CI smoke test
- CI: ubuntu-24.04 (gcc-14, clang) Release/ASan+UBSan/TSan; ubuntu-24.04-arm Release/TSan;
  windows-2022 (MSVC, clang-cl) Release/ASan; macos-14 Release/TSan plus a
  `TRIBUTARY_CACHE_LINE=128` build; clang-tidy and clang-format gates
- `DESIGN.md` — ordering arguments, the bitmap proof, the four decisions above, and "when the mutex
  wins". A library must say when it is the wrong choice.

---

## Non-goals

Multi-consumer *per ring* (the SPSC discipline is the design), global ordering across producers,
overwrite-oldest (needs sequence-numbered slots and reader validation — a different data structure),
and serialization/schema. Callers own their wire format above the frame header.

### Generics deliberately not added

The library is already generic where genericity pays: `fixed_channel<T, Traits>`,
`basic_pipeline<Channel, Traits>`, `Traits::collect_stats` compiling every counter down to nothing,
and duck-typed sinks behind `sink_for`/`has_on_idle`/`has_close`. The candidates below were considered
and rejected, and the one seam that *is* missing is the `Channel` drain path above — not any of these.

- **`scan_policy`/`full_policy` as template parameters.** The scan branch runs once per drain pass,
  amortised over up to `drain_batch` records; the full-policy branch is reached only after a push has
  already failed. Both are perfectly predicted. The price is two instantiations of the consumer loop —
  icache pressure on the exact loop being optimised — plus the loss of runtime reconfiguration and of
  the single-binary alternating A/B the benchmark methodology requires.
- **Compile-time `ring_capacity`/`drain_batch`.** Would fold the mask to a constant, but runtime
  sizing is a stated purpose of this library (see Context). Giving up a design goal to save a mask op
  is the wrong direction.
- **Allocator or NUMA placement as a template parameter.** `detail::make_aligned` is already the
  single choke point through which `numa.hpp` can swap in node-bound pages without touching a call
  site. Placement is a runtime property of the machine; `options::numa_node` is the right axis.
- **Type-erasing the sink behind a virtual `write`.** The indirect call amortises over a whole batch,
  so it is nearly free — but what it buys is a compile firewall and ABI stability, and this is a
  header-only library whose ABI tag already turns a mismatch into a link error. Nothing to buy.
- **Heterogeneous sinks per shard.** `start()` currently requires one sink *type* across all shards.
  A real but small generalisation with no known caller; leave it open rather than building it
  speculatively.
