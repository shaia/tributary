// Phase B: the integration surface Phase A built and never ran.
//
// Two substantial code paths shipped with Phase A without a single test
// executing them. `opt.consumers` was never set above 1, so sharded consumers
// -- disjoint slot ranges, per-shard bitmap and park state, one sink each --
// have never run. And `opt.max_producers` never exceeded 64, so `active_set`
// always had exactly one word and its entire second level was dead code: the
// summary word, visit()'s summary loop, and maintain_summary()'s repair sweep.
//
// Both are argued for carefully in comments and neither was ever observed to
// work. That is the gap this suite closes. The wide cases deliberately push the
// ACTIVE producers into high words rather than settling for a large
// max_producers, because registration is linear from slot 0: a test that just
// asks for 200 slots and starts 96 threads exercises words 0 and 1 and leaves
// the interesting part -- a partially-valid final word -- untouched.
//
// The missed-wakeup test is the one to read first. It is the invariant whose
// failure mode is rare, load-dependent, and invisible in a normal test, and it
// is invisible for a specific reason: park_timeout puts a 200 us ceiling on
// being wrong, so a lost wakeup looks like a latency blip rather than a bug.
// Raising the timeout to five seconds is what converts it into a test failure.

#include "harness.hpp"
#include "sinks.hpp"

#include <tributary/pipeline.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace tributary;
using namespace tributary::test;

namespace {

// Wire up N shards with one ordering_sink each and hand them to start(). The
// library will not share a sink across consumers -- it imposes no thread-safety
// requirement on sinks, so it cannot -- which makes this the shape every
// sharded test needs.
template <class Pipe>
std::vector<ordering_sink*> shard_sinks(std::vector<ordering_sink>& out, Pipe& pipe,
                                        std::uint32_t max_producers) {
    out.reserve(pipe.consumer_count());
    for (std::uint32_t s = 0; s < pipe.consumer_count(); ++s) out.emplace_back(max_producers);
    std::vector<ordering_sink*> ptrs;
    ptrs.reserve(out.size());
    for (auto& s : out) ptrs.push_back(&s);
    return ptrs;
}

// One slot per producer thread, written only by that thread, so recording what
// each got is not itself a race.
struct registration {
    std::uint32_t id = 0;
    std::uint32_t shard = 0;
    bool ok = false;
};

// Run `count` producer threads, each pushing `per` events through its own
// handle, and report what each one was given.
//
// THE GATE IS LOAD-BEARING, and it is not about timing. Measured on the first
// draft of this suite: 96 producers cycled through 12 slots and never left leaf
// word 0, so the two-level bitmap this file exists to test was never reached,
// and the ordering assertions failed for a reason that had nothing to do with
// ordering. See start_gate in harness.hpp for the mechanism -- the same defect
// was hiding in test_pipeline.cpp, where only CI's 4-core runner exposed it.
//
// file-local, and every call site names both counts in a kProducers /
// kPerProducer constant rather than passing a bare literal.
template <class Pipe>
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
std::vector<registration> run_producers(Pipe& pipe, std::uint32_t count, std::uint32_t per) {
    std::vector<registration> regs(count);
    start_gate gate(count);
    std::vector<std::thread> ts;
    ts.reserve(count);
    for (std::uint32_t p = 0; p < count; ++p) {
        ts.emplace_back([&pipe, &regs, &gate, p, per] {
            auto h = pipe.register_producer();
            if (h) regs[p] = registration{h.id(), h.shard(), true};
            gate.arrive_and_wait();
            if (!h) return;
            for (std::uint32_t s = 1; s <= per; ++s) h.push(make_event(h.id(), s));
        });
    }
    for (auto& t : ts) t.join();
    return regs;
}

// Every producer held its slot for the whole run, so the ids must be distinct.
// Asserting it is what keeps the barrier above from rotting silently: if it
// ever stops working, the ordering failures it causes look like a library bug.
bool ids_are_distinct(const std::vector<registration>& regs, std::uint32_t max_producers) {
    std::vector<int> seen(max_producers, 0);
    for (const auto& r : regs) {
        if (!r.ok) continue;
        if (r.id >= max_producers || seen[r.id] != 0) return false;
        seen[r.id] = 1;
    }
    return true;
}

// --- the two-level bitmap --------------------------------------------------

void test_wide_bitmap(scan_policy scan) {
    const char* name = (scan == scan_policy::bitmap) ? "bitmap" : "full-scan";
    std::printf("\n>64 producers, two-level bitmap [%s]\n", name);

    constexpr std::uint32_t kProducers = 96;  // spans leaf words 0 and 1
    constexpr std::uint32_t kPerProducer = 10'000;

    options opt;
    opt.max_producers = 200;  // ceil(200/64) = 4 leaf words, so a summary exists
    opt.ring_capacity = 1024;
    opt.scan = scan;
    opt.full = full_policy::spin_then_drop;

    pipeline<event> pipe(opt);
    ordering_sink sink(opt.max_producers);
    pipe.start(sink);

    const auto regs = run_producers(pipe, kProducers, kPerProducer);
    pipe.stop(stop_mode::drain);

    const auto st = pipe.snapshot();
    const std::uint64_t offered = std::uint64_t{kProducers} * kPerProducer;
    const std::string tag = std::string(name) + ": ";

    std::uint32_t highest = 0;
    for (const auto& r : regs)
        if (r.ok) highest = std::max(highest, r.id);

    std::printf("  producers=%u highest slot=%u pushed=%llu dropped=%llu delivered=%llu\n",
                kProducers, highest, static_cast<unsigned long long>(st.pushed),
                static_cast<unsigned long long>(st.dropped),
                static_cast<unsigned long long>(sink.events()));

    check(highest >= 64, tag + "producers actually reached past the first leaf word");
    check(ids_are_distinct(regs, opt.max_producers), tag + "every producer held its own slot");
    check_eq(st.registration_failures, 0, tag + "all 96 producers got a slot");
    check_eq(sink.violations(), 0, tag + "per-producer order preserved across words");
    check_eq(st.pushed + st.dropped, offered, tag + "every push accounted for");
    check_eq(sink.events(), st.pushed, tag + "drain shutdown lost nothing");
    check(pipe.drain_completed(), tag + "drain finished within its deadline");
    check(st.high_water <= opt.ring_capacity, tag + "ring depth stayed bounded");

    // The mechanism, not just the outcome. Coalescing has to survive the second
    // level: the summary fetch_or is an extra write per empty->non-empty word
    // transition, and if it ever ran per event instead this is where it shows.
    check(st.bitmap_writes < offered / 1000,
          tag + "bitmap writes stayed negligible with a summary word (" +
              std::to_string(st.bitmap_writes) + " for " + std::to_string(offered) + ")");
}

void test_high_leaf_words() {
    std::printf("\nactive producers in high leaf words\n");

    // Registration is linear from slot 0, so the only way to put ACTIVE
    // producers in words 2 and 3 is to park idle handles in front of them.
    // Those handles never push, so they never set a bit -- the bitmap arm sees
    // exactly the eight active bits, and they live where nothing has looked
    // before: word 2 (bits 190-191 of slot space) through word 3, whose final
    // word is only partially valid (200 slots, so bits 192-199 of 256).
    constexpr std::uint32_t kIdle = 190;
    constexpr std::uint32_t kActive = 8;
    constexpr std::uint32_t kPerProducer = 20'000;

    options opt;
    opt.max_producers = 200;
    opt.ring_capacity = 256;  // 198 rings, so keep them small
    opt.full = full_policy::spin_then_drop;

    pipeline<event> pipe(opt);
    ordering_sink sink(opt.max_producers);
    pipe.start(sink);

    std::vector<decltype(pipe)::producer> idle;
    idle.reserve(kIdle);
    for (std::uint32_t i = 0; i < kIdle; ++i) idle.push_back(pipe.register_producer());
    check(idle.back().valid(), "the 190 placeholder handles all registered");

    const auto regs = run_producers(pipe, kActive, kPerProducer);
    std::uint32_t lowest = opt.max_producers;
    for (const auto& r : regs)
        if (r.ok) lowest = std::min(lowest, r.id);

    idle.clear();
    pipe.stop(stop_mode::drain);

    const auto st = pipe.snapshot();
    const std::uint64_t offered = std::uint64_t{kActive} * kPerProducer;

    std::printf("  active producers occupied slots %u..%u (leaf words %u..%u)\n", lowest,
                lowest + kActive - 1, lowest / 64, (lowest + kActive - 1) / 64);

    check(lowest >= 128, "the active producers really did land in leaf word 2 or above");
    check_eq(sink.violations(), 0, "per-producer order preserved in the high words");
    check_eq(st.pushed + st.dropped, offered, "every push accounted for");

    // The claim under test: an event published against a bit in a high leaf
    // word is found. If the summary were cleared out from under a set leaf bit,
    // or if visit() skipped a word the summary did not point at, these events
    // would reach the sink only via the park timeout -- or, at shutdown, only
    // via final_drain's unconditional full scan. Asserting the drain is
    // lossless does not distinguish those, so the timing assert below does.
    check_eq(sink.events(), st.pushed, "nothing was stranded in a high-word ring");
}

// --- sharded consumers -----------------------------------------------------

void test_sharded_delivery() {
    std::printf("\nsharded consumers: delivery and slot disjointness\n");

    constexpr std::uint32_t kShards = 4;
    constexpr std::uint32_t kProducers = 16;
    constexpr std::uint32_t kPerProducer = 20'000;

    options opt;
    opt.max_producers = 64;  // 16 slots per shard
    opt.consumers = kShards;
    opt.ring_capacity = 1024;
    opt.full = full_policy::spin_then_drop;

    pipeline<event> pipe(opt);
    check_eq(pipe.consumer_count(), kShards, "the pipeline built one shard per requested consumer");

    std::vector<ordering_sink> sinks;
    auto ptrs = shard_sinks(sinks, pipe, opt.max_producers);
    pipe.start(std::span<ordering_sink* const>(ptrs));

    const auto regs = run_producers(pipe, kProducers, kPerProducer);
    pipe.stop(stop_mode::drain);

    const auto st = pipe.snapshot();
    const std::uint64_t offered = std::uint64_t{kProducers} * kPerProducer;

    std::uint64_t delivered = 0;
    std::uint64_t violations = 0;
    for (auto& s : sinks) {
        delivered += s.events();
        violations += s.violations();
    }
    std::printf("  shards=%u delivered=%llu of pushed=%llu\n", kShards,
                static_cast<unsigned long long>(delivered),
                static_cast<unsigned long long>(st.pushed));

    check(ids_are_distinct(regs, opt.max_producers), "every producer held its own slot");
    check_eq(st.pushed + st.dropped, offered, "every push accounted for");
    check_eq(delivered, st.pushed, "the shards between them delivered everything");
    check_eq(violations, 0, "per-producer order preserved within each shard");

    // Disjointness, which is the actual invariant. An event must reach exactly
    // one shard's sink: two would mean the slot ranges overlap, zero would mean
    // a slot belongs to no shard at all. Counting totals cannot see either --
    // an overlap that double-delivers and a gap that drops would cancel.
    bool disjoint = true;
    bool covered = true;
    for (const auto& r : regs) {
        if (!r.ok) continue;
        std::uint32_t seen_by = 0;
        for (auto& s : sinks) seen_by += s.saw(r.id) ? 1U : 0U;
        if (seen_by > 1) disjoint = false;
        if (seen_by == 0) covered = false;
    }
    check(disjoint, "no producer's events reached two shards");
    check(covered, "every registered producer's events reached a shard");

    // The handle's cached shard is what the push path uses to pick a bitmap and
    // a park state, so a disagreement with the slot table would send wakeups to
    // a consumer that does not own the ring.
    std::vector<producer_stats> per;
    pipe.per_producer(per);
    bool agree = true;
    std::vector<int> used(kShards, 0);
    for (const auto& r : regs) {
        if (!r.ok) continue;
        if (per[r.id].shard != r.shard) agree = false;
        if (r.shard < kShards) used[r.shard] = 1;
    }
    check(agree, "each handle's cached shard matches the slot table");
    check(std::find(used.begin(), used.end(), 0) == used.end(),
          "registration spread producers over every shard");
}

void test_wide_and_sharded() {
    std::printf("\n>64 producers AND sharded\n");

    // 200 slots over 3 shards is 67/67/66, so shard 0 owns slots 0..66: a
    // two-word active_set whose second word has three valid bits. That is the
    // combination where the `local >= sh.count` guard in drain_pass and the
    // `sh.begin + local` translation in reap have to both be right, and until
    // now neither has ever been exercised with a non-zero begin.
    constexpr std::uint32_t kShards = 3;
    constexpr std::uint32_t kProducers = 48;
    constexpr std::uint32_t kPerProducer = 10'000;

    options opt;
    opt.max_producers = 200;
    opt.consumers = kShards;
    opt.ring_capacity = 512;
    opt.full = full_policy::spin_then_drop;

    pipeline<event> pipe(opt);
    std::vector<ordering_sink> sinks;
    auto ptrs = shard_sinks(sinks, pipe, opt.max_producers);
    pipe.start(std::span<ordering_sink* const>(ptrs));

    const auto regs = run_producers(pipe, kProducers, kPerProducer);
    pipe.stop(stop_mode::drain);

    const auto st = pipe.snapshot();
    const std::uint64_t offered = std::uint64_t{kProducers} * kPerProducer;

    std::uint64_t delivered = 0;
    std::uint64_t violations = 0;
    for (auto& s : sinks) {
        delivered += s.events();
        violations += s.violations();
    }

    std::uint32_t highest = 0;
    for (const auto& r : regs)
        if (r.ok) highest = std::max(highest, r.id);
    std::printf("  shards=%u highest slot=%u delivered=%llu\n", kShards, highest,
                static_cast<unsigned long long>(delivered));

    check(highest >= 64, "producers landed beyond a single leaf word");
    check(ids_are_distinct(regs, opt.max_producers), "every producer held its own slot");
    check_eq(st.pushed + st.dropped, offered, "every push accounted for");
    check_eq(delivered, st.pushed, "sharded delivery over multiple words lost nothing");
    check_eq(violations, 0, "per-producer order preserved");

    bool disjoint = true;
    for (const auto& r : regs) {
        if (!r.ok) continue;
        std::uint32_t seen_by = 0;
        for (auto& s : sinks) seen_by += s.saw(r.id) ? 1U : 0U;
        if (seen_by != 1) disjoint = false;
    }
    check(disjoint, "slot ownership stayed disjoint with a multi-word bitmap per shard");
}

// --- the wakeup handshake --------------------------------------------------

void test_no_missed_wakeup(scan_policy scan) {
    const char* name = (scan == scan_policy::bitmap) ? "bitmap" : "full-scan";
    std::printf("\nmissed wakeup under repeated parking [%s]\n", name);

    // The whole design of this test is in these two lines. spin_before_park=1
    // makes the consumer park after two idle passes, so almost every event
    // below arrives at a sleeping consumer -- the only state in which a lost
    // wakeup is unbounded. park_timeout=5s removes the backstop that normally
    // hides the failure: at the default 200 us a missed wakeup costs 200 us and
    // looks like scheduler noise, which is exactly why this bug is invisible in
    // an ordinary test. Here it costs five seconds and fails the deadline.
    options opt;
    opt.max_producers = 200;
    opt.ring_capacity = 256;
    opt.scan = scan;
    opt.spin_before_park = 1;
    opt.park_timeout = std::chrono::seconds(5);

    pipeline<event> pipe(opt);
    signalling_sink sink;
    pipe.start(sink);

    // Push the active handles into a high leaf word again: the summary is the
    // half of the handshake with the newer argument behind it.
    constexpr std::uint32_t kIdle = 130;
    constexpr std::uint32_t kHandles = 4;
    constexpr std::uint32_t kRounds = 300;

    std::vector<decltype(pipe)::producer> idle;
    idle.reserve(kIdle);
    for (std::uint32_t i = 0; i < kIdle; ++i) idle.push_back(pipe.register_producer());

    std::vector<decltype(pipe)::producer> handles;
    handles.reserve(kHandles);
    for (std::uint32_t i = 0; i < kHandles; ++i) handles.push_back(pipe.register_producer());
    check(handles.back().valid(), "the wakeup handles registered in a high leaf word");

    // A deadline far below park_timeout: anything slower than this could only
    // be the park timeout firing, which means the notification was lost.
    const auto budget = std::chrono::milliseconds(500);
    std::uint32_t late = 0;
    std::uint32_t asleep = 0;
    std::uint64_t sent = 0;

    for (std::uint32_t r = 0; r < kRounds; ++r) {
        auto& h = handles[r % kHandles];

        // Wait until the consumer is genuinely asleep before publishing.
        //
        // This gate is the difference between a test and a formality. `parks`
        // is bumped inside park(), after the Dekker re-check of the bitmap and
        // while holding park_mu, so a push that follows an observed bump is a
        // push at a consumer committed to sleeping -- the only state in which a
        // lost wakeup is unbounded rather than a scheduling blip. Without the
        // gate the event lands on a consumer that is still spinning and the
        // wakeup path is never used: the first draft of this test recorded two
        // parks across three hundred rounds and passed, proving nothing.
        //
        // snapshot() is O(slots) and far too heavy for a hot path; here it runs
        // on the observing thread, which has nothing else to do. Reading these
        // counters from a third thread is sound by construction -- counter::get
        // is a relaxed atomic load for exactly this case.
        const std::uint64_t parks_before = pipe.snapshot().parks;
        const auto park_by = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (pipe.snapshot().parks == parks_before &&
               std::chrono::steady_clock::now() < park_by)
            std::this_thread::yield();
        if (pipe.snapshot().parks == parks_before) continue;  // never settled; not a wakeup failure
        ++asleep;

        const std::uint64_t before = sink.delivered();
        if (!h.push(make_event(h.id(), r + 1))) continue;
        ++sent;

        const auto until = std::chrono::steady_clock::now() + budget;
        while (sink.delivered() == before && std::chrono::steady_clock::now() < until)
            std::this_thread::yield();
        if (sink.delivered() == before) ++late;
    }

    handles.clear();
    idle.clear();
    pipe.stop(stop_mode::drain);

    const auto st = pipe.snapshot();
    std::printf("  rounds=%llu at-a-parked-consumer=%u parks=%llu notifies=%llu late=%u\n",
                static_cast<unsigned long long>(sent), asleep,
                static_cast<unsigned long long>(st.parks),
                static_cast<unsigned long long>(st.notifies), late);

    const std::string tag = std::string(name) + ": ";
    check_eq(late, 0, tag + "every event woke the consumer well inside the park timeout");
    check_eq(sink.delivered(), sent, tag + "no event was left stranded in a ring");

    // Without this the test can pass by never testing anything -- see the gate
    // above. It is the assertion that the other two mean what they say.
    check(asleep >= kRounds - (kRounds / 10),
          tag + "the consumer was genuinely parked for nearly every round (" +
              std::to_string(asleep) + " of " + std::to_string(kRounds) + ")");
}

// --- externally driven -----------------------------------------------------

void test_run_on_caller_threads() {
    std::printf("\nexternal driving (run) with two shards\n");

    constexpr std::uint32_t kShards = 2;
    constexpr std::uint32_t kProducers = 8;
    constexpr std::uint32_t kPerProducer = 20'000;

    options opt;
    opt.max_producers = 16;
    opt.consumers = kShards;
    opt.own_threads = false;  // we own the consumer threads, the library keeps the loop
    opt.full = full_policy::spin_then_drop;

    pipeline<event> pipe(opt);
    std::vector<ordering_sink> sinks;
    auto ptrs = shard_sinks(sinks, pipe, opt.max_producers);
    pipe.start(std::span<ordering_sink* const>(ptrs));

    std::vector<std::thread> consumers;
    consumers.reserve(kShards);
    for (std::uint32_t s = 0; s < kShards; ++s)
        consumers.emplace_back([&pipe, &sinks, s] { pipe.run(s, sinks[s]); });

    run_producers(pipe, kProducers, kPerProducer);

    // stop() has no threads of its own to join here; it drops accepting_, waits
    // for the rings to quiesce, then drops running_ so our run() loops fall out
    // through their final drain. Joining them is ours to do.
    pipe.stop(stop_mode::drain);
    for (auto& t : consumers) t.join();

    const auto st = pipe.snapshot();
    std::uint64_t delivered = 0;
    std::uint64_t violations = 0;
    for (auto& s : sinks) {
        delivered += s.events();
        violations += s.violations();
    }

    check_eq(st.pushed + st.dropped, std::uint64_t{kProducers} * kPerProducer,
             "run(): every push accounted for");
    check_eq(delivered, st.pushed, "run() on caller threads delivered everything");
    check_eq(violations, 0, "run() preserved per-producer order");
    check(pipe.drain_completed(), "run(): the drain met its deadline");
}

void test_multi_shard_poll() {
    std::printf("\nexternal driving (poll) across shards\n");

    constexpr std::uint32_t kShards = 2;
    constexpr std::uint32_t kEvents = 5000;

    options opt;
    opt.max_producers = 4;
    opt.consumers = kShards;
    opt.own_threads = false;

    pipeline<event> pipe(opt);
    std::vector<ordering_sink> sinks;
    auto ptrs = shard_sinks(sinks, pipe, opt.max_producers);
    pipe.start(std::span<ordering_sink* const>(ptrs));

    // One handle per shard, so a single-threaded reactor has to service both.
    // Draining only shard 0 would leave shard 1's events in their ring, which
    // is the failure a single-shard poll test cannot see.
    std::vector<decltype(pipe)::producer> handles;
    for (std::uint32_t i = 0; i < kShards; ++i) handles.push_back(pipe.register_producer());
    check(handles[0].shard() != handles[1].shard(),
          "round-robin registration put the two handles on different shards");

    std::uint64_t pushed = 0;
    for (std::uint32_t s = 1; s <= kEvents; ++s) {
        for (auto& h : handles)
            if (h.push(make_event(h.id(), s))) ++pushed;
        if (s % 100 == 0)
            for (std::uint32_t sh = 0; sh < kShards; ++sh) pipe.poll(sh, sinks[sh]);
    }
    handles.clear();

    for (std::uint32_t sh = 0; sh < kShards; ++sh) {
        while (pipe.poll(sh, sinks[sh]) > 0) {
        }
        pipe.close(sh, sinks[sh]);
    }

    std::uint64_t delivered = 0;
    std::uint64_t violations = 0;
    for (auto& s : sinks) {
        delivered += s.events();
        violations += s.violations();
    }

    check_eq(pushed, std::uint64_t{kEvents} * kShards, "nothing dropped while polling kept up");
    check_eq(delivered, pushed, "poll() delivered every event from every shard");
    check_eq(violations, 0, "poll() preserved order across shards");

    pipe.stop(stop_mode::abort);
}

// --- the diagnostics surface -----------------------------------------------

void test_option_validation() {
    std::printf("\noption validation\n");

    // Rejection has to happen in the constructor. An invalid option that is
    // merely clamped or ignored produces a pipeline that works and is silently
    // not the one that was asked for.
    auto rejects = [](const options& o, const std::string& what) {
        bool threw = false;
        try {
            pipeline<event> p(o);
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        check(threw, "rejected: " + what);
    };

    options base;
    base.max_producers = 4;

    auto with = [&base](auto&& mutate) {
        options o = base;
        mutate(o);
        return o;
    };

    rejects(with([](options& o) { o.max_producers = 0; }), "max_producers == 0");
    rejects(with([](options& o) { o.consumers = 0; }), "consumers == 0");
    rejects(with([](options& o) { o.consumers = 8; }), "consumers > max_producers");
    rejects(with([](options& o) { o.ring_capacity = 1; }), "ring_capacity below 2");
    rejects(with([](options& o) { o.ring_capacity = 3000; }), "ring_capacity not a power of two");
    rejects(with([](options& o) { o.batch_capacity = 0; }), "batch_capacity == 0");
    rejects(with([](options& o) { o.drain_batch = 0; }), "drain_batch == 0");
    rejects(with([](options& o) { o.pin_consumers_to = {0, 1}; }),
            "one cpu id per consumer, or none");
    rejects(with([](options& o) { o.park_timeout = nanos::zero(); }), "park_timeout == 0");

    // The control. Without it, a validate() that rejected everything would pass
    // all nine assertions above.
    std::string rejected;
    try {
        pipeline<event> p(base);
    } catch (const std::invalid_argument& e) {
        rejected = e.what();
    }
    check(rejected.empty(), "a valid options set still constructs (" + rejected + ")");
}

void test_on_error_reports() {
    std::printf("\non_error reporting\n");

    // report() runs on whichever thread hit the problem -- the registering
    // thread here, the stopping thread and the consumer thread below -- so the
    // counter has to be atomic. A plain int would be a data race in the second
    // case, and the TSan legs would say so.
    std::atomic<int> calls{0};

    {
        options opt;
        opt.max_producers = 1;
        opt.on_error = [&calls](std::string_view) { calls.fetch_add(1, std::memory_order_relaxed); };

        pipeline<event> pipe(opt);
        counting_sink sink;
        pipe.start(sink);

        auto held = pipe.register_producer();
        check(held.valid(), "the only slot was taken");
        auto denied = pipe.register_producer();
        check(!denied.valid(), "registering past capacity failed");
        check(calls.load() >= 1, "on_error was told about the failed registration");
        check_eq(pipe.snapshot().registration_failures, 1, "and it was counted too");

        held.release();
        pipe.stop(stop_mode::abort);
    }

    calls.store(0);
    {
        options opt;
        opt.max_producers = 4;
        opt.ring_capacity = 4096;
        opt.on_error = [&calls](std::string_view) { calls.fetch_add(1, std::memory_order_relaxed); };

        pipeline<event> pipe(opt);
        slow_sink sink(std::chrono::milliseconds(10));
        pipe.start(sink);

        auto h = pipe.register_producer();
        for (std::uint32_t s = 1; s <= 20'000; ++s) h.push(make_event(h.id(), s));
        h.release();

        pipe.stop(stop_mode::drain, std::chrono::milliseconds(50));

        check(!pipe.drain_completed(), "the missed drain deadline was reported honestly");
        check(calls.load() >= 1, "on_error was told the drain degraded to an abort");
    }
}

}  // namespace

int main() {
    test_wide_bitmap(scan_policy::bitmap);
    test_wide_bitmap(scan_policy::full_scan);
    test_high_leaf_words();
    test_sharded_delivery();
    test_wide_and_sharded();
    test_no_missed_wakeup(scan_policy::bitmap);
    test_no_missed_wakeup(scan_policy::full_scan);
    test_run_on_caller_threads();
    test_multi_shard_poll();
    test_option_validation();
    test_on_error_reports();
    return summary("test_sharding");
}
