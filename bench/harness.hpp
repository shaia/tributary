#pragma once
//
// The measurement rig.
//
// Every rule encoded here exists because violating it produces a number that
// looks fine and is wrong. The two that do the most work:
//
//   open loop     a record's send time is decided before the run starts, so a
//                 producer that falls behind reports its lateness instead of
//                 quietly sliding its own schedule
//   preallocated  nothing on a measured path allocates, locks, or grows
//
// No dependencies here either. A benchmark that needs a package fetched is a
// benchmark that stops running.

#include <tributary/traits.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace tributary::bench {

// The record every phase moves. 32 bytes, trivially copyable, no indirection --
// the shape the fixed-size path is designed for.
//
// Deliberately its own type rather than the test suite's `event`: the two rigs
// are independent, and the 32-byte shape is the only thing they have to agree
// on. That agreement is what the static_assert is for.
struct record {
    std::uint32_t producer_id;
    std::uint32_t seq;    // per-producer, strictly increasing
    std::int64_t send_ns; // INTENDED send time -- never a clock read, see pacer
    std::byte payload[16];
};

static_assert(sizeof(record) == 32, "keep the record small: two per cache line");

// --- clock -----------------------------------------------------------------
//
// Inline because it is called from spin loops. A read is ~25 ns, which is the
// entire reason latency is sampled rather than recorded per record.
inline std::int64_t now_ns() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               clock_type::now().time_since_epoch())
        .count();
}

// --- open-loop pacing -------------------------------------------------------

// Record `i` is due at `start + i * period`, fixed before the run begins.
//
// A producer that falls behind does **not** slide its schedule: it stamps the
// time the record was *due*, so the resulting latency includes the lateness.
// That is the whole point. In a closed loop -- where the next send is timed
// from the previous completion -- a stalled consumer throttles its own
// producers and the stall disappears from the measurement. That is coordinated
// omission, and it always lies in the flattering direction.
class pacer {
public:
    pacer(std::int64_t start_ns, double rate_per_sec) noexcept
        : start_(start_ns), period_ns_(rate_per_sec > 0.0 ? 1e9 / rate_per_sec : 0.0) {}

    [[nodiscard]] std::int64_t due(std::uint64_t i) const noexcept {
        return start_ + static_cast<std::int64_t>(static_cast<double>(i) * period_ns_);
    }

    // Spin, never sleep_for. On Windows sleep_for rounds up to the system timer
    // tick (~1-15 ms); at a 10 us period that quantises the schedule into
    // uselessness and every latency number becomes a report on the timer.
    static void spin_until(std::int64_t t_ns) noexcept {
        while (now_ns() < t_ns) TRIBUTARY_PAUSE();
    }

private:
    std::int64_t start_;
    double period_ns_;
};

// --- percentiles ------------------------------------------------------------
//
// Percentiles, never means: the entire claim is about tails, and a mean
// averages away exactly the events under discussion.

struct percentiles {
    double p50 = 0.0;   // nanoseconds
    double p99 = 0.0;
    double p999 = 0.0;
    std::size_t n = 0;
};

// Sorts `samples` in place. Nearest-rank, so every reported value is a value
// that actually occurred rather than an interpolation between two that did not.
percentiles compute(std::vector<std::int64_t>& samples_ns);

// Median across repetitions, taken **per statistic** rather than by picking one
// "median run". Picking a run lets one noisy percentile drag every unrelated
// column with it.
double median_of(std::vector<double> xs);

// --- verdict ----------------------------------------------------------------
//
// The exit code is derived from the failure count, so a violated invariant
// fails the run rather than merely the eye of whoever is reading the output.

void check(bool ok, const std::string& what);
void check_le(double got, double limit, const std::string& what);

// Printed, never asserted. For the numbers that are real observations but whose
// run-to-run variance makes a threshold meaningless -- see the p99 discussion in
// scan_ab.cpp.
void note(const std::string& what);

void phase_verdict(const char* phase);
int overall_verdict();

}  // namespace tributary::bench
