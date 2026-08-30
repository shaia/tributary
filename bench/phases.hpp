#pragma once
//
// The gate phases, one per translation unit.
//
// One TU each so a hang or a crash names the phase without bisecting, and so
// each phase's argument stands on its own. No phase shares a pipeline, a sink,
// or a fixture with another: a failure then names one thing.
//
// Each phase prints its own table and registers its own pass/fail through the
// harness. Targets are in docs/PLAN.md under "Phase A gate targets".

namespace tributary::bench {

// 1. Per-producer ordering, exact accounting, lossless drain, and slot reuse
//    under churn. Reports no timings: it is what makes the other phases' numbers
//    worth reading.
void bench_correctness();

// 2. Offered vs sustained rate, and what the wakeup handshake costs a producer.
void bench_throughput();

// 3. Overload against a deliberately slow sink, and shutdown.
void bench_backpressure();

// 4. Full scan vs active bitmap, at a fixed active-producer count and a varying
//    number of registered-but-idle producers. The phase that measures the
//    bitmap's actual claim.
void bench_scan_ab();

// 5. Does throughput scale with consumer shard count? Measured against a sink
//    with a deliberate per-record cost, because that is the only regime in
//    which sharding is meant to help -- see the file's header.
void bench_sharding();

}  // namespace tributary::bench
