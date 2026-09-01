// The invariants that make the throughput numbers worth reading. If ordering or
// accounting is wrong, a fast pipeline is just a fast way to be wrong.

#include "harness.hpp"
#include "sinks.hpp"

#include <tributary/pipeline.hpp>

#include <string>
#include <thread>
#include <vector>

using namespace tributary;
using namespace tributary::test;

namespace {

// --- the drain contract -----------------------------------------------------
//
// `Drain` is basic_pipeline's third parameter, and an unconstrained one would
// fail from wherever the consumer loop first touched it -- the exact defect the
// seam was extracted to remove, reintroduced one level up. It splits in two
// because half of it cannot be checked until a sink type is in hand.
//
// The negative controls are the half that carries the weight: a concept nothing
// can fail says the same thing about staged_drain as a correct one would.

// Everything `drain_for` asks for and nothing else -- no take/flush, so it is
// what a strategy looks like when someone has written the shape but not yet the
// drain itself.
// NOLINTBEGIN(readability-convert-member-functions-to-static)
struct shape_only_drain {
    explicit shape_only_drain(const drain_config& /*cfg*/) {}
    bool pending() const noexcept { return false; }
    std::uint64_t backpressure() const noexcept { return 0; }
};
// NOLINTEND(readability-convert-member-functions-to-static)

// Constructible from anything but a drain_config, which is how a strategy that
// forgot the pipeline builds it in place would look.
struct unconfigurable_drain : shape_only_drain {
    unconfigurable_drain() : shape_only_drain(drain_config{0, 0}) {}
};

using test_channel = fixed_channel<event>;
using test_drain = staged_drain<test_channel>;

static_assert(drain_for<test_drain>, "staged_drain must satisfy the class-scope half");
static_assert(drains_into<test_drain, test_channel, counting_sink>,
              "and the sink-dependent half, for a sink it is expected to drain into");
static_assert(drain_for<shape_only_drain>,
              "take/flush belong to the sink-dependent half, so a strategy without them still "
              "satisfies what the pipeline can check without a sink");
static_assert(!drains_into<shape_only_drain, test_channel, counting_sink>,
              "and fails the moment a sink is named -- which is start(), not the consumer loop");
static_assert(!drain_for<unconfigurable_drain>,
              "the pipeline constructs the strategy in place from a drain_config; a type it cannot "
              "build that way is rejected at the class, so the contract is not vacuous");

void test_correctness(scan_policy scan) {
    const char* name = (scan == scan_policy::bitmap) ? "bitmap" : "full-scan";
    std::printf("\ncorrectness under stress [%s]\n", name);

    constexpr std::uint32_t kProducers = 16;
    constexpr std::uint32_t kPerProducer = 200'000;

    options opt;
    opt.max_producers = 32;
    opt.scan = scan;
    // Spin first: this test is about ordering and accounting, and a run with
    // 90% drops exercises far less of the delivery path.
    opt.full = full_policy::spin_then_drop;

    pipeline<event> pipe(opt);
    ordering_sink sink(opt.max_producers);
    pipe.start(sink);

    // Every producer holds its slot until the last one has registered. Without
    // this the early finishers' slots are recycled into the late starters and
    // the recycled ids restart their sequence at 1, which reads as an ordering
    // violation and is not one. See start_gate.
    start_gate gate(kProducers);

    std::vector<std::thread> producers;
    producers.reserve(kProducers);
    for (std::uint32_t p = 0; p < kProducers; ++p) {
        producers.emplace_back([&pipe, &gate] {
            auto h = pipe.register_producer();
            gate.arrive_and_wait();
            if (!h) return;
            for (std::uint32_t s = 1; s <= kPerProducer; ++s) h.push(make_event(h.id(), s));
        });
    }
    for (auto& t : producers) t.join();
    pipe.stop(stop_mode::drain);

    const auto st = pipe.snapshot();
    const std::uint64_t offered = std::uint64_t{kProducers} * kPerProducer;
    const std::string tag = std::string(name) + ": ";

    std::printf("  pushed=%llu dropped=%llu delivered=%llu probes/pass=%.2f bitmap_writes=%llu\n",
                static_cast<unsigned long long>(st.pushed),
                static_cast<unsigned long long>(st.dropped),
                static_cast<unsigned long long>(sink.events()), st.probes_per_pass(),
                static_cast<unsigned long long>(st.bitmap_writes));

    check_eq(sink.violations(), 0, tag + "per-producer order preserved");
    check_eq(st.pushed + st.dropped, offered, tag + "every push accounted for (pushed + dropped)");
    check_eq(sink.events(), st.pushed, tag + "drain shutdown lost nothing");
    check_eq(st.consumed, st.pushed, tag + "consumer counter agrees with the sink");
    check(pipe.drain_completed(), tag + "drain finished within its deadline");
    check(st.high_water <= opt.ring_capacity, tag + "ring depth stayed bounded");
    check_eq(st.registration_failures, 0, tag + "all producers got a slot");

    // The mechanism check, not just the outcome: coalescing is working only if
    // writes to the shared line stay negligible next to the event count.
    check(st.bitmap_writes < offered / 1000,
          tag + "bitmap writes stayed negligible (" + std::to_string(st.bitmap_writes) + " for " +
              std::to_string(offered) + " events)");
}

void test_scan_width() {
    std::printf("\nscan width\n");

    // Eight active producers among many registered: the case the bitmap exists
    // for. Registered-but-idle handles hold slots a full-scan consumer must
    // probe on every pass.
    constexpr std::uint32_t kActive = 8;
    constexpr std::uint32_t kRegistered = 64;

    auto run = [](scan_policy scan) {
        options opt;
        opt.max_producers = kRegistered;
        opt.scan = scan;
        pipeline<event> pipe(opt);
        counting_sink sink;

        std::vector<decltype(pipe)::producer> idle;
        idle.reserve(kRegistered - kActive);
        pipe.start(sink);
        for (std::uint32_t i = 0; i < kRegistered - kActive; ++i)
            idle.push_back(pipe.register_producer());

        // Same reason as test_correctness, but the symptom here would be a
        // measurement rather than a failure: recycled slots mean fewer
        // producers active at once, so probes/pass would come out flatteringly
        // low and the assertion below would pass without meaning it.
        start_gate gate(kActive);

        std::vector<std::thread> ts;
        ts.reserve(kActive);
        for (std::uint32_t p = 0; p < kActive; ++p)
            ts.emplace_back([&pipe, &gate] {
                auto h = pipe.register_producer();
                gate.arrive_and_wait();
                if (!h) return;
                for (std::uint32_t s = 1; s <= 50'000; ++s) h.push(make_event(h.id(), s));
            });
        for (auto& t : ts) t.join();
        pipe.stop(stop_mode::drain);
        return pipe.snapshot();
    };

    const auto full = run(scan_policy::full_scan);
    const auto bmp = run(scan_policy::bitmap);

    std::printf("  full-scan probes/pass=%.2f   bitmap probes/pass=%.2f\n", full.probes_per_pass(),
                bmp.probes_per_pass());

    // This is the follow-up question's actual claim, asserted rather than
    // asserted-about: with 8 of 64 producers active, the bitmap must inspect
    // dramatically fewer rings per pass.
    check(bmp.probes_per_pass() < full.probes_per_pass() / 2,
          "bitmap scan is at least twice as narrow as a full scan");
    check(bmp.probes_per_pass() <= kActive + 1,
          "bitmap probes at most the active producers per pass");
}

void test_registration_limits() {
    std::printf("\nregistration limits\n");

    options opt;
    opt.max_producers = 4;
    pipeline<event> pipe(opt);
    counting_sink sink;
    pipe.start(sink);

    std::vector<decltype(pipe)::producer> held;
    for (int i = 0; i < 4; ++i) {
        held.push_back(pipe.register_producer());
        check(held.back().valid(), "slot " + std::to_string(i) + " registered");
    }

    auto overflow = pipe.register_producer();
    check(!overflow.valid(), "registering past capacity returns an invalid handle");
    check(!overflow.push(make_event(0, 1)), "an invalid handle refuses pushes rather than crashing");
    check_eq(pipe.snapshot().registration_failures, 1,
             "the failed registration was counted, not silent");

    // Releasing one must make a slot available again -- but only after the
    // consumer has published it free, which is why this retries.
    held.pop_back();
    bool reacquired = false;
    for (int i = 0; i < 2000 && !reacquired; ++i) {
        auto p = pipe.register_producer();
        reacquired = p.valid();
        if (!reacquired) std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    check(reacquired, "a released slot becomes reusable");

    held.clear();
    pipe.stop(stop_mode::drain);
}

void test_move_semantics() {
    std::printf("\nhandle move semantics\n");

    options opt;
    opt.max_producers = 4;
    pipeline<event> pipe(opt);
    ordering_sink sink(opt.max_producers);
    pipe.start(sink);

    auto a = pipe.register_producer();
    check(a.valid(), "handle acquired");
    const std::uint32_t id = a.id();

    auto b = std::move(a);
    check(!a.valid(), "a moved-from handle is invalid");  // NOLINT(bugprone-use-after-move)
    check(b.valid(), "the moved-to handle is valid");
    check_eq(b.id(), id, "the moved-to handle kept the slot id");
    check(b.push(make_event(b.id(), 1)), "the moved-to handle can push");
    check(!a.push(make_event(0, 1)), "the moved-from handle cannot push");

    // Move-assign over a live handle must retire the one being overwritten,
    // not leak its slot.
    auto c = pipe.register_producer();
    const std::uint32_t c_id = c.id();
    c = std::move(b);
    check_eq(c.id(), id, "move-assign took over the source's slot");

    bool freed = false;
    for (int i = 0; i < 2000 && !freed; ++i) {
        std::vector<producer_stats> per;
        pipe.per_producer(per);
        freed = !per[c_id].active && !per[c_id].retiring;
        if (!freed) std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    check(freed, "move-assign released the overwritten handle's slot");

    pipe.stop(stop_mode::drain);
}

}  // namespace

int main() {
    test_correctness(scan_policy::bitmap);
    test_correctness(scan_policy::full_scan);
    test_scan_width();
    test_registration_limits();
    test_move_semantics();
    return summary("test_pipeline");
}
