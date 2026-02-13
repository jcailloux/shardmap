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
//  7. Multi-threaded read throughput — small vs large maps
//
// #############################################################################

TEST_CASE("Benchmark: multi-threaded read throughput", "[benchmark][throughput]")
{
    static constexpr int THREADS = 8;
    static constexpr int OPS = 50'000;

    auto bench_mt = [&]<ShardMapConfig Cfg>(const std::string& label, int map_size) {
        using Map = ShardMap<int64_t, std::shared_ptr<int>, BenchMetadata, Cfg>;
        Map map;
        auto keys = populateAndCollectKeys(map, map_size);

        std::latch start{THREADS};
        std::vector<std::jthread> threads;
        threads.reserve(THREADS);

        auto t0 = Clock::now();
        for (int i = 0; i < THREADS; ++i) {
            threads.emplace_back([&, i]() {
                std::mt19937 rng(i * 42 + 7);
                start.arrive_and_wait();
                for (int j = 0; j < OPS; ++j) {
                    auto key = keys[rng() % keys.size()];
                    doNotOptimize(map.get(key));
                }
            });
        }
        for (auto& t : threads) t.join();
        auto elapsed = Clock::now() - t0;

        auto total_ops = THREADS * OPS;
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
        auto ops_sec = (us > 0) ? total_ops * 1'000'000.0 / us : 0.0;
        auto avg_ns = (total_ops > 0) ? static_cast<double>(us) * 1000.0 / total_ops : 0.0;

        std::ostringstream out;
        out << std::fixed;
        if (ops_sec >= 1'000'000) out << std::setprecision(1) << ops_sec / 1'000'000 << "M ops/s";
        else                       out << std::setprecision(1) << ops_sec / 1'000 << "K ops/s";
        out << ", avg " << std::setprecision(0) << avg_ns << " ns/op";

        WARN("  " << label << ": " << out.str());
    };

    SECTION("8 shards, varying map size") {
        constexpr auto cfg = ShardMapConfig{.shard_count_log2 = 3};
        bench_mt.operator()<cfg>("  1K entries", 1'000);
        bench_mt.operator()<cfg>("100K entries", 100'000);
        bench_mt.operator()<cfg>("  1M entries", 1'000'000);
        bench_mt.operator()<cfg>("  2M entries", 2'000'000);
    }

    SECTION("2M entries, varying shard count") {
        bench_mt.operator()<ShardMapConfig{.shard_count_log2 = 2}>("  4 shards", 2'000'000);
        bench_mt.operator()<ShardMapConfig{.shard_count_log2 = 3}>("  8 shards", 2'000'000);
        bench_mt.operator()<ShardMapConfig{.shard_count_log2 = 4}>(" 16 shards", 2'000'000);
        bench_mt.operator()<ShardMapConfig{.shard_count_log2 = 6}>(" 64 shards", 2'000'000);
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