// Phase 4 -- full scan vs active bitmap.
//
// The claim under test: with a handful of active producers among many
// registered, the consumer should stop looking at the idle rings. `probes/pass`
// is that claim stated as a number.
//
// The A/B is clean here by construction. `scan_policy` selects the SCAN only --
// the wakeup handshake runs under both policies -- so the two arms differ in
// scan width and nothing else. Do not "simplify" this by disabling the
// handshake in the full_scan arm: that would vary scan width and push cost
// together and the result would be unattributable to either.

#include "harness.hpp"
#include "phases.hpp"

#include <tributary/pipeline.hpp>

#include <cstdio>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace tributary::bench {
namespace {

// Eight active producers among many registered: the case the bitmap exists for.
constexpr std::uint32_t kActive = 8;
constexpr double kRatePerProducer = 100'000.0;  // records/s, each
constexpr std::uint32_t kRegistered[] = {8, 16, 32, 64};
constexpr int kReps = 5;
constexpr std::int64_t kRunNs = 1'000'000'000;     // 1 s of paced load per run
constexpr std::int64_t kStartDelayNs = 50'000'000; // every thread registers before t0

// Sample 1 in 4. A clock read is ~25 ns; recording every record would make the
// instrument a measurable share of the consumer's budget, and the measurement
// would then be partly reporting itself.
constexpr std::size_t kSampleEvery = 4;

// This phase's sink, and only this phase's.
//
// One clock read per write() call rather than per record: a batch is drained
// together, so one arrival time for the batch is the honest granularity, and it
// keeps the instrument off the path being measured.
class latency_sink {
public:
    explicit latency_sink(std::size_t reserve) { samples_.reserve(reserve); }

    std::size_t write(std::span<const record> b) noexcept {
        const std::int64_t arrived = now_ns();
        // The phase counter carries across batch boundaries, making this a true
        // 1-in-N over the whole stream. Striding within each batch instead
        // (`i += kSampleEvery`) emits at least one sample per batch, and under
        // this load the consumer drains batches of one to three records -- so
        // that spelling silently becomes ~100% sampling, overruns the buffer,
        // and puts a clock read back on the path it was meant to stay off.
        for (const auto& r : b) {
            if (++since_ < kSampleEvery) continue;
            since_ = 0;
            // Never allocate on a measured path. If the preallocated buffer
            // fills, stop sampling and report it -- growing it here would put a
            // malloc inside the consumer loop under measurement, and would bias
            // the percentiles toward the start of the run.
            if (samples_.size() < samples_.capacity())
                samples_.push_back(arrived - r.send_ns);
            else
                ++unsampled_;
        }
        records_ += b.size();
        return b.size();
    }

    std::vector<std::int64_t>& samples() noexcept { return samples_; }
    [[nodiscard]] std::uint64_t records() const noexcept { return records_; }
    [[nodiscard]] std::uint64_t unsampled() const noexcept { return unsampled_; }

private:
    std::vector<std::int64_t> samples_;
    std::size_t since_ = 0;  // records seen since the last sample, across batches
    std::uint64_t records_ = 0;
    std::uint64_t unsampled_ = 0;
};

struct run_result {
    double p50_us = 0.0;
    double p99_us = 0.0;
    double p999_us = 0.0;
    double probes_per_pass = 0.0;
    double bitmap_writes = 0.0;
    double drop_pct = 0.0;
    double unsampled = 0.0;
};

run_result one_run(scan_policy scan, std::uint32_t registered) {
    options opt;
    opt.max_producers = registered;
    opt.scan = scan;
    // drop_newest, not spin_then_drop: a spin budget would add producer-side
    // spinning into every latency sample. The offered rate is far below the
    // consumer's ceiling, and drops are asserted to be zero.
    opt.full = full_policy::drop_newest;

    pipeline<record> pipe(opt);

    const double expected = static_cast<double>(kActive) * kRatePerProducer *
                            (static_cast<double>(kRunNs) / 1e9);
    latency_sink sink(static_cast<std::size_t>(expected / kSampleEvery * 1.25));
    pipe.start(sink);

    // Registered but silent: the slots a full-scan consumer must probe on every
    // pass and a bitmap consumer must not. Declared after `pipe` so the handles
    // retire before the pipeline they point into is destroyed.
    std::vector<decltype(pipe)::producer> idle;
    idle.reserve(registered - kActive);
    for (std::uint32_t i = 0; i < registered - kActive; ++i)
        idle.push_back(pipe.register_producer());

    const std::int64_t start = now_ns() + kStartDelayNs;
    const std::int64_t end = start + kRunNs;

    std::vector<std::thread> ts;
    ts.reserve(kActive);
    for (std::uint32_t p = 0; p < kActive; ++p) {
        ts.emplace_back([&pipe, start, end] {
            auto h = pipe.register_producer();
            if (!h) return;
            const pacer pc(start, kRatePerProducer);
            for (std::uint64_t i = 0;; ++i) {
                const std::int64_t due = pc.due(i);
                if (due >= end) break;
                pacer::spin_until(due);
                record r{};
                r.producer_id = h.id();
                r.seq = static_cast<std::uint32_t>(i + 1);
                r.send_ns = due;  // the time it was DUE, not the time it was sent
                h.push(r);
            }
        });
    }
    for (auto& t : ts) t.join();
    pipe.stop(stop_mode::drain);

    const auto st = pipe.snapshot();
    const auto pct = compute(sink.samples());

    run_result r;
    r.p50_us = pct.p50 / 1000.0;
    r.p99_us = pct.p99 / 1000.0;
    r.p999_us = pct.p999 / 1000.0;
    r.probes_per_pass = st.probes_per_pass();
    r.bitmap_writes = static_cast<double>(st.bitmap_writes);
    r.drop_pct = st.drop_fraction() * 100.0;
    r.unsampled = static_cast<double>(sink.unsampled());
    return r;
}

// Median per statistic across repetitions -- not the statistics of one "median
// run". Picking a run lets a single noisy percentile drag every other column.
run_result median_run(scan_policy scan, std::uint32_t registered) {
    std::vector<double> p50;
    std::vector<double> p99;
    std::vector<double> p999;
    std::vector<double> probes;
    std::vector<double> writes;
    std::vector<double> drops;
    std::vector<double> unsampled;
    p50.reserve(kReps);
    p99.reserve(kReps);
    p999.reserve(kReps);
    probes.reserve(kReps);
    writes.reserve(kReps);
    drops.reserve(kReps);
    unsampled.reserve(kReps);

    for (int i = 0; i < kReps; ++i) {
        const run_result r = one_run(scan, registered);
        p50.push_back(r.p50_us);
        p99.push_back(r.p99_us);
        p999.push_back(r.p999_us);
        probes.push_back(r.probes_per_pass);
        writes.push_back(r.bitmap_writes);
        drops.push_back(r.drop_pct);
        unsampled.push_back(r.unsampled);
    }

    run_result m;
    m.p50_us = median_of(p50);
    m.p99_us = median_of(p99);
    m.p999_us = median_of(p999);
    m.probes_per_pass = median_of(probes);
    m.bitmap_writes = median_of(writes);
    m.drop_pct = median_of(drops);
    m.unsampled = median_of(unsampled);
    return m;
}

}  // namespace

void bench_scan_ab() {
    constexpr std::size_t kLevels = std::size(kRegistered);

    std::printf("\n=== phase 4: scan A/B ===\n");
    std::printf("%u active producers at %.0fk rec/s each; %d reps of %.1fs, median per statistic\n\n",
                static_cast<unsigned>(kActive), kRatePerProducer / 1000.0, kReps,
                static_cast<double>(kRunNs) / 1e9);

    run_result full[kLevels];
    run_result bmp[kLevels];
    for (std::size_t i = 0; i < kLevels; ++i) {
        full[i] = median_run(scan_policy::full_scan, kRegistered[i]);
        bmp[i] = median_run(scan_policy::bitmap, kRegistered[i]);
    }

    std::printf("registered  scan   |   p50 us   p99 us  p99.9 us | probes/pass  bmp-writes   drop%%\n");
    std::printf("-------------------+---------------------------+-------------------------------\n");
    const auto row = [](std::uint32_t reg, const char* name, const run_result& r) {
        std::printf("%-10u  %-6s |  %7.2f  %7.2f   %7.2f |  %10.2f  %10.0f  %5.2f%%\n",
                    static_cast<unsigned>(reg), name, r.p50_us, r.p99_us, r.p999_us,
                    r.probes_per_pass, r.bitmap_writes, r.drop_pct);
    };
    for (std::size_t i = 0; i < kLevels; ++i) {
        row(kRegistered[i], "full", full[i]);
        row(kRegistered[i], "bitmap", bmp[i]);
    }
    std::printf("\n");

    // --- what the mechanism must do -----------------------------------------

    for (std::size_t i = 0; i < kLevels; ++i) {
        const std::string at = " at " + std::to_string(kRegistered[i]) + " registered";

        // The bitmap's whole claim: scan width tracks ACTIVE producers, not
        // registered ones. Flat across the sweep or the mechanism is not working.
        check_le(bmp[i].probes_per_pass, kActive + 2,
                 "bitmap probes only the active producers" + at);

        // Coalescing. A notify per record would be a wakeup per record, which is
        // categorically worse than the polling it replaces.
        check_le(bmp[i].bitmap_writes, 4.0 * kActive, "bitmap writes stayed negligible" + at);

        check_le(bmp[i].drop_pct, 0.0, "bitmap dropped nothing" + at);
        check_le(full[i].drop_pct, 0.0, "full scan dropped nothing" + at);

        check_le(bmp[i].unsampled, 0.0, "latency buffer held every sample" + at);
    }

    // Full scan tracks registration; the bitmap does not. Compared at the widest
    // point, which is where the two policies are meant to diverge most.
    const run_result& f_last = full[kLevels - 1];
    const run_result& b_last = bmp[kLevels - 1];
    check(f_last.probes_per_pass > 4.0 * b_last.probes_per_pass,
          "at " + std::to_string(kRegistered[kLevels - 1]) +
              " registered the bitmap scan is >4x narrower than a full scan");
    check(full[kLevels - 1].probes_per_pass > full[0].probes_per_pass * 2.0,
          "full-scan probes/pass grows with registration, as it must");

    // --- what is reported but deliberately not asserted ----------------------

    note("p99/p99.9 are printed, not gated: they swing several-fold between runs in");
    note("both directions. The tail here is dominated by OS preemption and park/wake,");
    note("both far larger than the scan cost being removed. On this machine the bitmap");
    note("is a median-latency and CPU-efficiency win, not a tail-latency one.");
    note("bmp-writes is non-zero under BOTH policies by design: scan_policy selects the");
    note("scan only, and the wakeup handshake runs under full_scan too.");

    phase_verdict("phase 4: scan A/B");
}

}  // namespace tributary::bench
