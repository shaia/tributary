#include "harness.hpp"

#include <algorithm>
#include <cstdio>

namespace tributary::bench {
namespace {

int g_checks = 0;
int g_failures = 0;
int g_phase_checks = 0;
int g_phase_failures = 0;

}  // namespace

percentiles compute(std::vector<std::int64_t>& samples_ns) {
    percentiles r;
    r.n = samples_ns.size();
    if (samples_ns.empty()) return r;

    std::sort(samples_ns.begin(), samples_ns.end());

    // Nearest-rank: index floor(p * n), clamped. Reports a sample that actually
    // occurred instead of interpolating between two that did not.
    const auto at = [&samples_ns](double p) {
        auto i = static_cast<std::size_t>(p * static_cast<double>(samples_ns.size()));
        if (i >= samples_ns.size()) i = samples_ns.size() - 1;
        return static_cast<double>(samples_ns[i]);
    };

    r.p50 = at(0.50);
    r.p99 = at(0.99);
    r.p999 = at(0.999);
    return r;
}

double median_of(std::vector<double> xs) {
    if (xs.empty()) return 0.0;
    std::sort(xs.begin(), xs.end());
    const std::size_t mid = xs.size() / 2;
    // Even count: average the two middles rather than favouring one side, so a
    // 2- or 4-rep run is not silently biased.
    if (xs.size() % 2 == 0) return (xs[mid - 1] + xs[mid]) / 2.0;
    return xs[mid];
}

void check(bool ok, const std::string& what) {
    ++g_checks;
    ++g_phase_checks;
    if (ok) {
        std::printf("  ok    %s\n", what.c_str());
        return;
    }
    ++g_failures;
    ++g_phase_failures;
    std::printf("  FAIL  %s\n", what.c_str());
}

void check_le(double got, double limit, const std::string& what) {
    ++g_checks;
    ++g_phase_checks;
    if (got <= limit) {
        std::printf("  ok    %s  (%.2f <= %.2f)\n", what.c_str(), got, limit);
        return;
    }
    ++g_failures;
    ++g_phase_failures;
    std::printf("  FAIL  %s  (%.2f > %.2f)\n", what.c_str(), got, limit);
}

void note(const std::string& what) { std::printf("  note  %s\n", what.c_str()); }

void phase_verdict(const char* phase) {
    std::printf("\n%s: %d checks, %d failed\n", phase, g_phase_checks, g_phase_failures);
    g_phase_checks = 0;
    g_phase_failures = 0;
}

int overall_verdict() {
    std::printf("\n%s\n", "================================================================");
    std::printf("bench: %d checks, %d failed\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}

}  // namespace tributary::bench
