/**
 * test_shardmap_benchmark.cpp
 *
 * Micro-benchmarks for ShardMap raw operations.
 * Tests across map sizes and shard configurations.
 *
 * Run with:
 *   ./shardmap_benchmarks                          # all benchmarks
 *   ./shardmap_benchmarks "[get]"                  # get latency only
 *   ./shardmap_benchmarks "[scale]"                # scaling tests only
 *   ./shardmap_benchmarks "[throughput]"            # multi-threaded only
 *   ./shardmap_benchmarks "[lock]"                 # lock overhead only
 *   BENCH_SAMPLES=1000 ./shardmap_benchmarks       # more samples
 */

#include <catch2/catch_test_macros.hpp>

#include <jcailloux/shardmap/ShardMap.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <latch>
#include <numeric>
#include <random>
#include <sched.h>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace jcailloux::shardmap;

// =============================================================================
// Benchmark environment setup (runs before main via static init)
// =============================================================================
//
// BENCH_PIN_CPU=N  — pin main thread to core N (default: no pinning)
// Automatically checks CPU governor and warns if not "performance".
//

static const bool bench_env_ready = [] {
    if (auto* env = std::getenv("BENCH_PIN_CPU")) {
        int core = std::atoi(env);
        cpu_set_t mask;
        CPU_ZERO(&mask);
        CPU_SET(core, &mask);
        if (sched_setaffinity(0, sizeof(mask), &mask) == 0) {
            std::fprintf(stderr, "  [bench] pinned to CPU %d\n", core);
        } else {
            std::fprintf(stderr, "  [bench] WARNING: failed to pin to CPU %d\n", core);
        }
    }

    int cpu = 0;
    if (auto* env = std::getenv("BENCH_PIN_CPU")) cpu = std::atoi(env);
    std::string path = "/sys/devices/system/cpu/cpu" + std::to_string(cpu) + "/cpufreq/scaling_governor";
    if (std::ifstream gov(path); gov.is_open()) {
        std::string g;
        std::getline(gov, g);
        if (g == "performance") {
            std::fprintf(stderr, "  [bench] CPU governor: performance\n");
        } else {
            std::fprintf(stderr,
                "  [bench] WARNING: CPU governor is '%s', not 'performance'\n"
                "          Run: sudo cpupower frequency-set -g performance\n", g.c_str());
        }
    }

    for (auto* turbo_path : {
        "/sys/devices/system/cpu/intel_pstate/no_turbo",
        "/sys/devices/system/cpu/cpufreq/boost"
    }) {
        if (std::ifstream f(turbo_path); f.is_open()) {
            int val = 0;
            f >> val;
            bool turbo_on = (std::string(turbo_path).find("no_turbo") != std::string::npos)
                            ? (val == 0) : (val == 1);
            if (turbo_on) {
                std::fprintf(stderr,
                    "  [bench] WARNING: turbo boost is ON (frequency varies with temperature)\n"
                    "          Disable: echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo\n"
                    "              or: echo 0 | sudo tee /sys/devices/system/cpu/cpufreq/boost\n");
            } else {
                std::fprintf(stderr, "  [bench] turbo boost: disabled\n");
            }
            break;
        }
    }

    return true;
}();

// =============================================================================
// Benchmark engine
// =============================================================================

static constexpr int WARMUP = 50;

static int benchSamples() {
    static int n = [] {
        if (auto* env = std::getenv("BENCH_SAMPLES"))
            if (int v = std::atoi(env); v > 0) return v;
        return 500;
    }();
    return n;
}

using Clock = std::chrono::steady_clock;

struct BenchResult {
    std::string name;
    double median_ns;
    double p99_ns;
    double mean_ns;
    double min_ns;
    double max_ns;
};

static BenchResult computeStats(const std::string& name, std::vector<double>& times_ns) {
    std::sort(times_ns.begin(), times_ns.end());
    auto n = static_cast<int>(times_ns.size());
    double median = times_ns[n / 2];
    double p99 = times_ns[static_cast<int>(n * 0.99)];
    double mean = std::accumulate(times_ns.begin(), times_ns.end(), 0.0) / n;
    return {name, median, p99, mean, times_ns.front(), times_ns.back()};
}

static std::string fmtNs(double ns) {
    std::ostringstream out;
    out << std::fixed;
    if (ns < 1000)           out << std::setprecision(0) << ns << " ns";
    else if (ns < 1'000'000) out << std::setprecision(1) << ns / 1000 << " us";
    else                     out << std::setprecision(2) << ns / 1'000'000 << " ms";
    return out.str();
}

static std::string formatTable(const std::string& title,
                               const std::vector<BenchResult>& results) {
    size_t max_name = 0;
    for (const auto& r : results)
        max_name = std::max(max_name, r.name.size());
    max_name += 2;

    auto w = static_cast<int>(max_name + 55);
    auto bar = std::string(w, '-');

    std::ostringstream out;
    out << "\n  " << bar << "\n"
        << "  " << title << " (" << benchSamples() << " samples)\n"
        << "  " << bar << "\n"
        << "  " << std::left << std::setw(static_cast<int>(max_name)) << ""
        << std::right
        << std::setw(10) << "median"
        << std::setw(12) << "p99"
        << std::setw(10) << "mean"
        << std::setw(10) << "min"
        << std::setw(10) << "max" << "\n"
        << "  " << bar << "\n";

    for (const auto& r : results) {
        out << "  " << std::left << std::setw(static_cast<int>(max_name)) << r.name
            << std::right
            << std::setw(10) << fmtNs(r.median_ns)
            << std::setw(12) << fmtNs(r.p99_ns)
            << std::setw(10) << fmtNs(r.mean_ns)
            << std::setw(10) << fmtNs(r.min_ns)
            << std::setw(10) << fmtNs(r.max_ns) << "\n";
    }

    out << "  " << bar;
    return out.str();
}

template<typename T>
static void doNotOptimize(const T& val) {
    asm volatile("" : : "r,m"(val) : "memory");
}

// =============================================================================
// Metadata type matching relais's EntityCacheMetadata
// =============================================================================

struct BenchMetadata {
    std::atomic<int64_t> expiration_rep{0};

    BenchMetadata() = default;
    explicit BenchMetadata(int64_t v) : expiration_rep(v) {}
    BenchMetadata(const BenchMetadata& o)
        : expiration_rep(o.expiration_rep.load(std::memory_order_relaxed)) {}
    BenchMetadata& operator=(const BenchMetadata& o) {
        expiration_rep.store(o.expiration_rep.load(std::memory_order_relaxed),
                            std::memory_order_relaxed);
        return *this;
    }
};

// =============================================================================
// Helper: populate a map with N entries (random keys for realistic distribution)
// =============================================================================

template<typename Map>
static void populate(Map& map, int count) {
    // Use a fixed-seed RNG for reproducible key distribution
    std::mt19937_64 rng(12345);
    for (int i = 0; i < count; ++i) {
        auto key = static_cast<int64_t>(rng());
        map.put(key, std::make_shared<int>(i), BenchMetadata{i * 1000});
    }
}

/// Populate and return a vector of existing keys for lookup benchmarks
template<typename Map>
static std::vector<int64_t> populateAndCollectKeys(Map& map, int count) {
    std::mt19937_64 rng(12345);  // Same seed as populate()
    std::vector<int64_t> keys;
    keys.reserve(count);
    for (int i = 0; i < count; ++i) {
        auto key = static_cast<int64_t>(rng());
        keys.push_back(key);
        map.put(key, std::make_shared<int>(i), BenchMetadata{i * 1000});
    }
    return keys;
}

// =============================================================================
// Generic benchmark runner: get latency for a given config and map size
// =============================================================================

template<ShardMapConfig Cfg>
static BenchResult benchGet(const std::string& label, int map_size) {
    using Map = ShardMap<int64_t, std::shared_ptr<int>, BenchMetadata, Cfg>;
    Map map;
    auto keys = populateAndCollectKeys(map, map_size);

    // Pick a key from the middle of the key set
    const int64_t key = keys[map_size / 2];
    const int N = benchSamples();

    for (int i = 0; i < WARMUP; ++i)
        doNotOptimize(map.get(key));

    std::vector<double> times(N);
    for (int i = 0; i < N; ++i) {
        auto t0 = Clock::now();
        auto v = map.get(key);
        auto t1 = Clock::now();
        doNotOptimize(v);
        times[i] = std::chrono::duration<double, std::nano>(t1 - t0).count();
    }

    return computeStats(label, times);
}

template<ShardMapConfig Cfg>
static BenchResult benchGetCallback(const std::string& label, int map_size) {
    using Map = ShardMap<int64_t, std::shared_ptr<int>, BenchMetadata, Cfg>;
    Map map;
    auto keys = populateAndCollectKeys(map, map_size);

    const int64_t key = keys[map_size / 2];
    const int N = benchSamples();

    for (int i = 0; i < WARMUP; ++i) {
        doNotOptimize(map.get(key, [](const auto&, const BenchMetadata& meta) {
            doNotOptimize(meta.expiration_rep.load(std::memory_order_relaxed));
            return GetAction::Accept;
        }));
    }

    std::vector<double> times(N);
    for (int i = 0; i < N; ++i) {
        auto t0 = Clock::now();
        auto v = map.get(key, [](const auto&, const BenchMetadata& meta) {
            doNotOptimize(meta.expiration_rep.load(std::memory_order_relaxed));
            return GetAction::Accept;
        });
        auto t1 = Clock::now();
        doNotOptimize(v);
        times[i] = std::chrono::duration<double, std::nano>(t1 - t0).count();
    }

    return computeStats(label, times);
}

// #############################################################################
//
//  1. GET latency — scaling by map size (fixed 8 shards)
//
// #############################################################################

TEST_CASE("Benchmark: get latency by map size", "[benchmark][get][scale]")
{
    constexpr auto cfg8 = ShardMapConfig{.shard_count_log2 = 3};
    std::vector<BenchResult> results;

    results.push_back(benchGet<cfg8>("1K entries", 1'000));
    results.push_back(benchGet<cfg8>("10K entries", 10'000));
    results.push_back(benchGet<cfg8>("100K entries", 100'000));
    results.push_back(benchGet<cfg8>("1M entries", 1'000'000));
    results.push_back(benchGet<cfg8>("2M entries", 2'000'000));

    WARN(formatTable("get() latency by map size (8 shards)", results));
}

// #############################################################################
//
//  2. GET latency — shard count comparison at large scale
//
// #############################################################################

TEST_CASE("Benchmark: get latency by shard count (large map)", "[benchmark][get][scale]")
{
    static constexpr int MAP_SIZE = 2'000'000;
    std::vector<BenchResult> results;

    results.push_back(benchGet<ShardMapConfig{.shard_count_log2 = 2}>("4 shards", MAP_SIZE));
    results.push_back(benchGet<ShardMapConfig{.shard_count_log2 = 3}>("8 shards", MAP_SIZE));
    results.push_back(benchGet<ShardMapConfig{.shard_count_log2 = 4}>("16 shards", MAP_SIZE));
    results.push_back(benchGet<ShardMapConfig{.shard_count_log2 = 5}>("32 shards", MAP_SIZE));
    results.push_back(benchGet<ShardMapConfig{.shard_count_log2 = 6}>("64 shards", MAP_SIZE));

    WARN(formatTable("get() latency by shard count (2M entries)", results));
}

// #############################################################################
//
//  3. GET with callback — scaling by map size
//
// #############################################################################

TEST_CASE("Benchmark: get(callback) latency by map size", "[benchmark][get][callback][scale]")
{
    constexpr auto cfg8 = ShardMapConfig{.shard_count_log2 = 3};
    std::vector<BenchResult> results;

    results.push_back(benchGetCallback<cfg8>("1K entries", 1'000));
    results.push_back(benchGetCallback<cfg8>("10K entries", 10'000));
    results.push_back(benchGetCallback<cfg8>("100K entries", 100'000));
    results.push_back(benchGetCallback<cfg8>("1M entries", 1'000'000));
    results.push_back(benchGetCallback<cfg8>("2M entries", 2'000'000));

    WARN(formatTable("get(callback) latency by map size (8 shards)", results));
}

// #############################################################################
//
//  4. PUT latency — scaling by map size
//
// #############################################################################

TEST_CASE("Benchmark: put latency by map size", "[benchmark][put][scale]")
{
    const int N = benchSamples();
    constexpr auto cfg8 = ShardMapConfig{.shard_count_log2 = 3};
    std::vector<BenchResult> results;

    auto bench_put = [&](const std::string& label, int map_size) {
        using Map = ShardMap<int64_t, std::shared_ptr<int>, BenchMetadata, cfg8>;
        Map map;
        auto keys = populateAndCollectKeys(map, map_size);

        const int64_t key = keys[map_size / 2];
        auto val = std::make_shared<int>(999);

        for (int i = 0; i < WARMUP; ++i)
            map.put(key, val, BenchMetadata{i});

        std::vector<double> times(N);
        for (int i = 0; i < N; ++i) {
            auto t0 = Clock::now();
            map.put(key, val, BenchMetadata{i});
            auto t1 = Clock::now();
            times[i] = std::chrono::duration<double, std::nano>(t1 - t0).count();
        }

        results.push_back(computeStats(label, times));
    };

    bench_put("1K entries", 1'000);
    bench_put("10K entries", 10'000);
    bench_put("100K entries", 100'000);
    bench_put("1M entries", 1'000'000);
    bench_put("2M entries", 2'000'000);

    WARN(formatTable("put() latency by map size (8 shards)", results));
}

// #############################################################################
//
//  5. Random-key get — realistic scattered access pattern
//
// #############################################################################

template<ShardMapConfig Cfg>
static BenchResult benchGetRandom(const std::string& label, int map_size) {
    using Map = ShardMap<int64_t, std::shared_ptr<int>, BenchMetadata, Cfg>;
    Map map;
    auto keys = populateAndCollectKeys(map, map_size);

    const int N = benchSamples();
    std::mt19937 rng(99);

    for (int i = 0; i < WARMUP; ++i)
        doNotOptimize(map.get(keys[rng() % keys.size()]));

    std::vector<double> times(N);
    for (int i = 0; i < N; ++i) {
        auto key = keys[rng() % keys.size()];
        auto t0 = Clock::now();
        auto v = map.get(key);
        auto t1 = Clock::now();
        doNotOptimize(v);
        times[i] = std::chrono::duration<double, std::nano>(t1 - t0).count();
    }

    return computeStats(label, times);
}

TEST_CASE("Benchmark: random-key get by map size", "[benchmark][get][random][scale]")
{
    constexpr auto cfg8 = ShardMapConfig{.shard_count_log2 = 3};
    std::vector<BenchResult> results;

    results.push_back(benchGetRandom<cfg8>("1K entries", 1'000));
    results.push_back(benchGetRandom<cfg8>("10K entries", 10'000));
    results.push_back(benchGetRandom<cfg8>("100K entries", 100'000));
    results.push_back(benchGetRandom<cfg8>("1M entries", 1'000'000));
    results.push_back(benchGetRandom<cfg8>("2M entries", 2'000'000));

    WARN(formatTable("get() random key by map size (8 shards)", results));
}

TEST_CASE("Benchmark: random-key get by shard count (large map)", "[benchmark][get][random][scale]")
{
    static constexpr int MAP_SIZE = 2'000'000;
    std::vector<BenchResult> results;

    results.push_back(benchGetRandom<ShardMapConfig{.shard_count_log2 = 2}>("4 shards", MAP_SIZE));
    results.push_back(benchGetRandom<ShardMapConfig{.shard_count_log2 = 3}>("8 shards", MAP_SIZE));
    results.push_back(benchGetRandom<ShardMapConfig{.shard_count_log2 = 4}>("16 shards", MAP_SIZE));
    results.push_back(benchGetRandom<ShardMapConfig{.shard_count_log2 = 5}>("32 shards", MAP_SIZE));
    results.push_back(benchGetRandom<ShardMapConfig{.shard_count_log2 = 6}>("64 shards", MAP_SIZE));

    WARN(formatTable("get() random key by shard count (2M entries)", results));
}

// #############################################################################
//
//  7. Multi-threaded throughput
//     Aligned with relais benchmark conditions for overhead comparison.
//
// #############################################################################

// =============================================================================
// Throughput helpers
// =============================================================================

static std::string fmtDuration(double us) {
    std::ostringstream out;
    out << std::fixed;
    if (us < 1.0)            out << std::setprecision(0) << us * 1000 << " ns";
    else if (us < 1'000)     out << std::setprecision(1) << us << " us";
    else if (us < 1'000'000) out << std::setprecision(2) << us / 1'000 << " ms";
    else                     out << std::setprecision(2) << us / 1'000'000 << " s";
    return out.str();
}

static std::string fmtOps(double ops) {
    std::ostringstream out;
    out << std::fixed;
    if (ops >= 1'000'000)  out << std::setprecision(1) << ops / 1'000'000 << "M ops/s";
    else if (ops >= 1'000) out << std::setprecision(1) << ops / 1'000 << "K ops/s";
    else                   out << std::setprecision(0) << ops << " ops/s";
    return out.str();
}

/// Run N threads x ops, synchronized with latches. Returns wall time (work only).
/// Each thread is pinned to a separate CPU core for true parallelism.
template<typename Fn>
static auto measureParallel(int num_threads, int ops_per_thread, Fn&& fn) {
    std::latch ready{num_threads};
    std::latch go{1};
    std::vector<std::jthread> threads;
    threads.reserve(num_threads);

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            cpu_set_t mask;
            CPU_ZERO(&mask);
            CPU_SET(i % std::thread::hardware_concurrency(), &mask);
            sched_setaffinity(0, sizeof(mask), &mask);

            ready.count_down();
            go.wait();
            fn(i, ops_per_thread);
        });
    }

    ready.wait();
    auto t0 = Clock::now();
    go.count_down();
    for (auto& t : threads) t.join();
    return Clock::now() - t0;
}

struct DurationResult {
    Clock::duration elapsed;
    int64_t total_ops;
};

/// Duration-based benchmark duration (configurable via BENCH_DURATION_S env var).
static int benchDurationSeconds() {
    static int n = [] {
        if (auto* env = std::getenv("BENCH_DURATION_S"))
            if (int v = std::atoi(env); v > 0) return v;
        return 10;
    }();
    return n;
}

/// Run N threads for a fixed duration. Each thread loops until `running` is set to false.
/// fn(int tid, std::atomic<bool>& running) must return the number of ops performed.
template<typename Fn>
static DurationResult measureDuration(int num_threads, Fn&& fn) {
    std::latch ready{num_threads};
    std::latch go{1};
    std::atomic<bool> running{true};
    std::vector<std::atomic<int64_t>> ops_counts(num_threads);
    std::vector<std::jthread> threads;
    threads.reserve(num_threads);

    for (int i = 0; i < num_threads; ++i) {
        ops_counts[i].store(0);
        threads.emplace_back([&, i]() {
            cpu_set_t mask;
            CPU_ZERO(&mask);
            CPU_SET(i % std::thread::hardware_concurrency(), &mask);
            sched_setaffinity(0, sizeof(mask), &mask);

            ready.count_down();
            go.wait();
            ops_counts[i].store(fn(i, running), std::memory_order_relaxed);
        });
    }

    ready.wait();
    auto t0 = Clock::now();
    go.count_down();
    std::this_thread::sleep_for(std::chrono::seconds(benchDurationSeconds()));
    running.store(false, std::memory_order_relaxed);
    for (auto& t : threads) t.join();
    auto elapsed = Clock::now() - t0;

    int64_t total = 0;
    for (auto& c : ops_counts) total += c.load(std::memory_order_relaxed);
    return {elapsed, total};
}

/// Format a multi-threaded throughput measurement (fixed ops).
static std::string formatThroughput(
        const std::string& label, int threads, int ops_per_thread,
        Clock::duration elapsed) {
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
    auto total_ops = static_cast<int64_t>(threads) * ops_per_thread;
    auto ops_per_sec = (us > 0) ? total_ops * 1'000'000.0 / us : 0.0;
    auto avg_us = (total_ops > 0) ? static_cast<double>(us) / total_ops : 0.0;

    auto bar = std::string(50, '-');
    std::ostringstream out;
    out << "\n"
        << "  " << bar << "\n"
        << "  " << label << "\n"
        << "  " << bar << "\n"
        << "  threads:      " << threads << "\n"
        << "  ops/thread:   " << ops_per_thread << "\n"
        << "  total ops:    " << total_ops << "\n"
        << "  wall time:    " << fmtDuration(static_cast<double>(us)) << "\n"
        << "  throughput:   " << fmtOps(ops_per_sec) << "\n"
        << "  avg latency:  " << fmtDuration(avg_us) << "\n"
        << "  " << bar;
    return out.str();
}

/// Format a duration-based throughput measurement.
static std::string formatDurationThroughput(
        const std::string& label, int threads,
        const DurationResult& result) {
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(result.elapsed).count();
    auto ops_per_sec = (us > 0) ? result.total_ops * 1'000'000.0 / us : 0.0;
    auto avg_us = (result.total_ops > 0) ? static_cast<double>(us) / result.total_ops : 0.0;

    auto bar = std::string(50, '-');
    std::ostringstream out;
    out << "\n"
        << "  " << bar << "\n"
        << "  " << label << "\n"
        << "  " << bar << "\n"
        << "  threads:      " << threads << "\n"
        << "  duration:     " << std::fixed << std::setprecision(2)
                              << static_cast<double>(us) / 1'000'000 << " s\n"
        << "  total ops:    " << result.total_ops << "\n"
        << "  throughput:   " << fmtOps(ops_per_sec) << "\n"
        << "  avg latency:  " << fmtDuration(avg_us) << "\n"
        << "  " << bar;
    return out.str();
}

// =============================================================================
// 7a. Fixed-ops throughput (comparable with relais benchmark)
// =============================================================================

TEST_CASE("Benchmark: multi-threaded throughput (fixed ops)", "[benchmark][throughput]")
{
    static constexpr int THREADS = 6;
    static constexpr int OPS = 2'000'000;
    static constexpr int RUNS = 3;
    static constexpr int NUM_KEYS = 64;
    static constexpr auto cfg = ShardMapConfig{.shard_count_log2 = 3};

    using Map = ShardMap<int64_t, std::shared_ptr<int>, BenchMetadata, cfg>;
    Map map;

    // Populate with NUM_KEYS entries (same seed as relais benchmark)
    std::mt19937_64 rng(12345);
    std::vector<int64_t> ids;
    ids.reserve(NUM_KEYS);
    for (int i = 0; i < NUM_KEYS; ++i) {
        auto key = static_cast<int64_t>(rng());
        ids.push_back(key);
        map.put(key, std::make_shared<int>(i), BenchMetadata{i * 1000});
    }

    SECTION("get -- single key (contention)") {
        auto key = ids[0];
        Clock::duration best = Clock::duration::max();
        for (int r = 0; r < RUNS; ++r) {
            auto elapsed = measureParallel(THREADS, OPS, [&](int, int n) {
                for (int j = 0; j < n; ++j) {
                    doNotOptimize(map.get(key));
                }
            });
            best = std::min(best, elapsed);
        }
        WARN(formatThroughput("get -- single key (contention)", THREADS, OPS, best));
    }

    SECTION("get -- distributed keys (parallel)") {
        Clock::duration best = Clock::duration::max();
        for (int r = 0; r < RUNS; ++r) {
            auto elapsed = measureParallel(THREADS, OPS, [&](int tid, int n) {
                for (int j = 0; j < n; ++j) {
                    doNotOptimize(map.get(ids[(tid * n + j) % NUM_KEYS]));
                }
            });
            best = std::min(best, elapsed);
        }
        WARN(formatThroughput("get -- distributed keys (parallel)", THREADS, OPS, best));
    }

    SECTION("get(callback) -- distributed keys") {
        Clock::duration best = Clock::duration::max();
        for (int r = 0; r < RUNS; ++r) {
            auto elapsed = measureParallel(THREADS, OPS, [&](int tid, int n) {
                for (int j = 0; j < n; ++j) {
                    doNotOptimize(map.get(ids[(tid * n + j) % NUM_KEYS],
                        [](const auto&, const BenchMetadata& meta) {
                            doNotOptimize(meta.expiration_rep.load(std::memory_order_relaxed));
                            return GetAction::Accept;
                        }));
                }
            });
            best = std::min(best, elapsed);
        }
        WARN(formatThroughput("get(callback) -- distributed keys", THREADS, OPS, best));
    }

    SECTION("mixed read/write -- distributed (75R/25W)") {
        auto template_val = std::make_shared<int>(999);

        Clock::duration best = Clock::duration::max();
        for (int r = 0; r < RUNS; ++r) {
            auto elapsed = measureParallel(THREADS, OPS, [&](int tid, int n) {
                std::mt19937 trng(tid * 42 + 7);
                for (int j = 0; j < n; ++j) {
                    auto kid = ids[(tid * n + j) % NUM_KEYS];
                    if (trng() % 4 != 0) {
                        // 75% read
                        doNotOptimize(map.get(kid));
                    } else {
                        // 25% write: invalidate + put
                        map.invalidate(kid);
                        map.put(kid, template_val, BenchMetadata{j});
                    }
                }
            });
            best = std::min(best, elapsed);
        }
        WARN(formatThroughput("mixed read/write -- distributed (75R/25W)", THREADS, OPS, best));
    }
}

// =============================================================================
// 7b. Duration-based throughput (sustained measurement)
// =============================================================================

TEST_CASE("Benchmark: multi-threaded throughput (duration)", "[benchmark][throughput][duration]")
{
    static constexpr int THREADS = 6;
    static constexpr int NUM_KEYS = 64;
    static constexpr auto cfg = ShardMapConfig{.shard_count_log2 = 3};

    using Map = ShardMap<int64_t, std::shared_ptr<int>, BenchMetadata, cfg>;
    Map map;

    std::mt19937_64 rng(12345);
    std::vector<int64_t> ids;
    ids.reserve(NUM_KEYS);
    for (int i = 0; i < NUM_KEYS; ++i) {
        auto key = static_cast<int64_t>(rng());
        ids.push_back(key);
        map.put(key, std::make_shared<int>(i), BenchMetadata{i * 1000});
    }

    SECTION("get -- single key (contention)") {
        auto key = ids[0];
        auto result = measureDuration(THREADS, [&](int, std::atomic<bool>& running) -> int64_t {
            int64_t ops = 0;
            while (running.load(std::memory_order_relaxed)) {
                doNotOptimize(map.get(key));
                ++ops;
            }
            return ops;
        });
        WARN(formatDurationThroughput("get -- single key (contention) [duration]", THREADS, result));
    }

    SECTION("get -- distributed keys (parallel)") {
        auto result = measureDuration(THREADS, [&](int tid, std::atomic<bool>& running) -> int64_t {
            int64_t ops = 0;
            while (running.load(std::memory_order_relaxed)) {
                doNotOptimize(map.get(ids[(tid * 1000000 + ops) % NUM_KEYS]));
                ++ops;
            }
            return ops;
        });
        WARN(formatDurationThroughput("get -- distributed keys (parallel) [duration]", THREADS, result));
    }

    SECTION("get(callback) -- distributed keys") {
        auto result = measureDuration(THREADS, [&](int tid, std::atomic<bool>& running) -> int64_t {
            int64_t ops = 0;
            while (running.load(std::memory_order_relaxed)) {
                doNotOptimize(map.get(ids[(tid * 1000000 + ops) % NUM_KEYS],
                    [](const auto&, const BenchMetadata& meta) {
                        doNotOptimize(meta.expiration_rep.load(std::memory_order_relaxed));
                        return GetAction::Accept;
                    }));
                ++ops;
            }
            return ops;
        });
        WARN(formatDurationThroughput("get(callback) -- distributed keys [duration]", THREADS, result));
    }

    SECTION("mixed read/write -- distributed (75R/25W)") {
        auto template_val = std::make_shared<int>(999);

        auto result = measureDuration(THREADS, [&](int tid, std::atomic<bool>& running) -> int64_t {
            std::mt19937 trng(tid * 42 + 7);
            int64_t ops = 0;
            while (running.load(std::memory_order_relaxed)) {
                auto kid = ids[(tid * 1000000 + ops) % NUM_KEYS];
                if (trng() % 4 != 0) {
                    doNotOptimize(map.get(kid));
                } else {
                    map.invalidate(kid);
                    map.put(kid, template_val, BenchMetadata{static_cast<int64_t>(ops)});
                }
                ++ops;
            }
            return ops;
        });
        WARN(formatDurationThroughput("mixed read/write -- distributed (75R/25W) [duration]", THREADS, result));
    }
}

// #############################################################################
//
//  6. Lock overhead isolation
//
// #############################################################################

TEST_CASE("Benchmark: lock overhead (empty op under lock)", "[benchmark][lock]")
{
    const int N = benchSamples();
    std::vector<BenchResult> results;

    // SpinRWLock shared lock cost
    {
        SpinRWLock lock;
        for (int i = 0; i < WARMUP; ++i) {
            lock.lock_shared();
            lock.unlock_shared();
        }

        std::vector<double> times(N);
        for (int i = 0; i < N; ++i) {
            auto t0 = Clock::now();
            lock.lock_shared();
            lock.unlock_shared();
            auto t1 = Clock::now();
            times[i] = std::chrono::duration<double, std::nano>(t1 - t0).count();
        }
        results.push_back(computeStats("SpinRWLock shared", times));
    }

    // SpinRWLock exclusive lock cost
    {
        SpinRWLock lock;
        for (int i = 0; i < WARMUP; ++i) {
            lock.lock();
            lock.unlock();
        }

        std::vector<double> times(N);
        for (int i = 0; i < N; ++i) {
            auto t0 = Clock::now();
            lock.lock();
            lock.unlock();
            auto t1 = Clock::now();
            times[i] = std::chrono::duration<double, std::nano>(t1 - t0).count();
        }
        results.push_back(computeStats("SpinRWLock exclusive", times));
    }

    // std::shared_mutex shared lock cost
    {
        std::shared_mutex mtx;
        for (int i = 0; i < WARMUP; ++i) {
            mtx.lock_shared();
            mtx.unlock_shared();
        }

        std::vector<double> times(N);
        for (int i = 0; i < N; ++i) {
            auto t0 = Clock::now();
            mtx.lock_shared();
            mtx.unlock_shared();
            auto t1 = Clock::now();
            times[i] = std::chrono::duration<double, std::nano>(t1 - t0).count();
        }
        results.push_back(computeStats("shared_mutex shared", times));
    }

    // std::mutex lock cost
    {
        std::mutex mtx;
        for (int i = 0; i < WARMUP; ++i) {
            mtx.lock();
            mtx.unlock();
        }

        std::vector<double> times(N);
        for (int i = 0; i < N; ++i) {
            auto t0 = Clock::now();
            mtx.lock();
            mtx.unlock();
            auto t1 = Clock::now();
            times[i] = std::chrono::duration<double, std::nano>(t1 - t0).count();
        }
        results.push_back(computeStats("std::mutex", times));
    }

    WARN(formatTable("Lock overhead (uncontended, single-threaded)", results));
}