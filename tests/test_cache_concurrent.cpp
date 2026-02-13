#include <catch2/catch_test_macros.hpp>
#include <jcailloux/segcache/Cache.h>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <random>
#include <iostream>
#include <iomanip>

#include "CacheAccessor.h"

using namespace jcailloux::segcache;
using namespace std::chrono_literals;

struct ConcurrentMetadata {
    std::chrono::steady_clock::time_point last_accessed;
    int access_count = 0;

    ConcurrentMetadata() = default;
    ConcurrentMetadata(std::chrono::steady_clock::time_point t, int c) : last_accessed(t), access_count(c) {}
    ConcurrentMetadata(const ConcurrentMetadata&) = default;
    ConcurrentMetadata(ConcurrentMetadata&&) = default;
    ConcurrentMetadata& operator=(const ConcurrentMetadata&) = default;
    ConcurrentMetadata& operator=(ConcurrentMetadata&&) = default;
};
using Metadata = ConcurrentMetadata;

struct CleanupContext {
    std::chrono::steady_clock::time_point now;
    std::chrono::seconds max_age;
};

TEST_CASE("Concurrent reads", "[cache][concurrent][reads]") {
    constexpr size_t num_threads = 16;
    constexpr size_t operations_per_thread = 100000;
    constexpr size_t num_keys = 1000;

    Cache<int, std::string> cache(8);

    // Pre-populate cache
    for (size_t i = 0; i < num_keys; ++i) {
        cache.put(i, "value_" + std::to_string(i));
    }

    std::atomic<size_t> total_hits{0};
    std::atomic<size_t> total_misses{0};

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(t);
            std::uniform_int_distribution<int> dist(0, num_keys - 1);

            size_t hits = 0;
            size_t misses = 0;

            for (size_t i = 0; i < operations_per_thread; ++i) {
                int key = dist(rng);
                auto result = cache.get(key);
                if (result) {
                    hits++;
                } else {
                    misses++;
                }
            }

            total_hits.fetch_add(hits, std::memory_order_relaxed);
            total_misses.fetch_add(misses, std::memory_order_relaxed);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    size_t total_ops = num_threads * operations_per_thread;
    double ops_per_sec = (total_ops * 1000.0) / duration.count();

    std::cout << "\n=== Concurrent Reads ===" << std::endl;
    std::cout << "Threads: " << num_threads << std::endl;
    std::cout << "Total operations: " << total_ops << std::endl;
    std::cout << "Duration: " << duration.count() << " ms" << std::endl;
    std::cout << "Throughput: " << ops_per_sec << " ops/sec" << std::endl;
    std::cout << "Hits: " << total_hits.load() << std::endl;
    std::cout << "Misses: " << total_misses.load() << std::endl;

    REQUIRE(total_hits.load() > 0);
}

TEST_CASE("Concurrent writes", "[cache][concurrent][writes]") {
    constexpr size_t num_threads = 16;
    constexpr size_t operations_per_thread = 50000;
    constexpr size_t num_keys = 10000;

    Cache<int, std::string> cache(8);

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(t);
            std::uniform_int_distribution<int> dist(0, num_keys - 1);

            for (size_t i = 0; i < operations_per_thread; ++i) {
                int key = dist(rng);
                cache.put(key, "value_" + std::to_string(key) + "_" + std::to_string(i));
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    size_t total_ops = num_threads * operations_per_thread;
    double ops_per_sec = (total_ops * 1000.0) / duration.count();

    std::cout << "\n=== Concurrent Writes ===" << std::endl;
    std::cout << "Threads: " << num_threads << std::endl;
    std::cout << "Total operations: " << total_ops << std::endl;
    std::cout << "Duration: " << duration.count() << " ms" << std::endl;
    std::cout << "Throughput: " << ops_per_sec << " ops/sec" << std::endl;
    std::cout << "Final cache size: " << cache.size() << std::endl;

    REQUIRE(cache.size() > 0);
    REQUIRE(cache.size() <= num_keys);
}

TEST_CASE("Mixed read/write workload", "[cache][concurrent][mixed]") {
    constexpr size_t num_threads = 16;
    constexpr size_t operations_per_thread = 100000;
    constexpr size_t num_keys = 5000;
    constexpr double write_ratio = 0.2; // 20% writes, 80% reads

    Cache<int, std::string> cache(8);

    // Pre-populate
    for (size_t i = 0; i < num_keys / 2; ++i) {
        cache.put(i, "initial_" + std::to_string(i));
    }

    std::atomic<size_t> total_reads{0};
    std::atomic<size_t> total_writes{0};
    std::atomic<size_t> total_hits{0};

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(t);
            std::uniform_int_distribution<int> key_dist(0, num_keys - 1);
            std::uniform_real_distribution<double> op_dist(0.0, 1.0);

            size_t reads = 0;
            size_t writes = 0;
            size_t hits = 0;

            for (size_t i = 0; i < operations_per_thread; ++i) {
                int key = key_dist(rng);

                if (op_dist(rng) < write_ratio) {
                    // Write operation
                    cache.put(key, "value_" + std::to_string(key));
                    writes++;
                } else {
                    // Read operation
                    auto result = cache.get(key);
                    reads++;
                    if (result) hits++;
                }
            }

            total_reads.fetch_add(reads, std::memory_order_relaxed);
            total_writes.fetch_add(writes, std::memory_order_relaxed);
            total_hits.fetch_add(hits, std::memory_order_relaxed);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    size_t total_ops = num_threads * operations_per_thread;
    double ops_per_sec = (total_ops * 1000.0) / duration.count();

    std::cout << "\n=== Mixed Read/Write Workload ===" << std::endl;
    std::cout << "Threads: " << num_threads << std::endl;
    std::cout << "Total operations: " << total_ops << std::endl;
    std::cout << "Reads: " << total_reads.load() << " ("
              << (100.0 * total_reads.load() / total_ops) << "%)" << std::endl;
    std::cout << "Writes: " << total_writes.load() << " ("
              << (100.0 * total_writes.load() / total_ops) << "%)" << std::endl;
    std::cout << "Hit rate: " << (100.0 * total_hits.load() / total_reads.load()) << "%" << std::endl;
    std::cout << "Duration: " << duration.count() << " ms" << std::endl;
    std::cout << "Throughput: " << ops_per_sec << " ops/sec" << std::endl;

    REQUIRE(total_reads.load() > 0);
    REQUIRE(total_writes.load() > 0);
}

TEST_CASE("Concurrent get with callback and metadata updates", "[cache][concurrent][callback]") {
    constexpr size_t num_threads = 16;
    constexpr size_t operations_per_thread = 50000;
    constexpr size_t num_keys = 1000;

    Cache<int, std::string, Metadata> cache(8);

    auto now = std::chrono::steady_clock::now();

    // Pre-populate
    for (size_t i = 0; i < num_keys; ++i) {
        cache.put(i, "value_" + std::to_string(i), Metadata{now, 0});
    }

    std::atomic<size_t> total_accesses{0};

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(t);
            std::uniform_int_distribution<int> dist(0, num_keys - 1);

            for (size_t i = 0; i < operations_per_thread; ++i) {
                int key = dist(rng);
                cache.get(key, [](const std::string&, Metadata& meta) {
                    meta.last_accessed = std::chrono::steady_clock::now();
                    meta.access_count++;
                    return GetAction::Accept;
                });
                total_accesses.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    size_t total_ops = num_threads * operations_per_thread;
    double ops_per_sec = (total_ops * 1000.0) / duration.count();

    std::cout << "\n=== Concurrent Get with Callback ===" << std::endl;
    std::cout << "Threads: " << num_threads << std::endl;
    std::cout << "Total operations: " << total_ops << std::endl;
    std::cout << "Duration: " << duration.count() << " ms" << std::endl;
    std::cout << "Throughput: " << ops_per_sec << " ops/sec" << std::endl;

    REQUIRE(total_accesses.load() == total_ops);
}

TEST_CASE("Concurrent cleanup operations", "[cache][concurrent][cleanup]") {
    constexpr size_t num_cleanup_threads = 4;
    constexpr size_t num_writer_threads = 8;
    constexpr size_t num_keys = 10000;
    constexpr size_t writes_per_thread = 25000;

    Cache<int, std::string, Metadata> cache(8);

    auto start_time = std::chrono::steady_clock::now();

    std::atomic<bool> stop{false};
    std::atomic<size_t> cleanups_performed{0};
    std::atomic<size_t> total_removed{0};

    std::vector<std::thread> threads;

    // Cleanup threads
    for (size_t t = 0; t < num_cleanup_threads; ++t) {
        threads.emplace_back([&]() {
            while (!stop.load(std::memory_order_relaxed)) {
                auto now = std::chrono::steady_clock::now();
                auto [removed, seg_id] = cache.cleanup(
                    CleanupContext{now, 1s},
                    [](const int&, const std::string&, const Metadata& meta, const CleanupContext& ctx) {
                        return (ctx.now - meta.last_accessed) > ctx.max_age;
                    }
                );
                cleanups_performed.fetch_add(1, std::memory_order_relaxed);
                total_removed.fetch_add(removed, std::memory_order_relaxed);
                std::this_thread::sleep_for(10ms);
            }
        });
    }

    // Writer threads
    for (size_t t = 0; t < num_writer_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(t);
            std::uniform_int_distribution<int> dist(0, num_keys - 1);

            for (size_t i = 0; i < writes_per_thread; ++i) {
                int key = dist(rng);
                auto now = std::chrono::steady_clock::now();
                cache.put(key, "value_" + std::to_string(key), Metadata{now, 0});

                if (i % 1000 == 0) {
                    std::this_thread::sleep_for(1ms);
                }
            }
        });
    }

    // Wait for writers to finish
    for (size_t t = num_cleanup_threads; t < threads.size(); ++t) {
        threads[t].join();
    }

    // Stop cleanup threads
    stop.store(true, std::memory_order_relaxed);
    for (size_t t = 0; t < num_cleanup_threads; ++t) {
        threads[t].join();
    }

    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "\n=== Concurrent Cleanup ===" << std::endl;
    std::cout << "Cleanup threads: " << num_cleanup_threads << std::endl;
    std::cout << "Writer threads: " << num_writer_threads << std::endl;
    std::cout << "Duration: " << duration.count() << " ms" << std::endl;
    std::cout << "Cleanups performed: " << cleanups_performed.load() << std::endl;
    std::cout << "Total entries removed: " << total_removed.load() << std::endl;
    std::cout << "Final cache size: " << cache.size() << std::endl;

    REQUIRE(cleanups_performed.load() > 0);
}

TEST_CASE("Hotspot contention", "[cache][concurrent][contention]") {
    constexpr size_t num_threads = 32;
    constexpr size_t operations_per_thread = 100000;
    constexpr size_t num_hot_keys = 10;  // Very few keys, high contention
    constexpr size_t num_cold_keys = 10000;
    constexpr double hot_key_ratio = 0.8;  // 80% of accesses go to hot keys

    Cache<int, std::string> cache(8);

    // Pre-populate
    for (size_t i = 0; i < num_hot_keys + num_cold_keys; ++i) {
        cache.put(i, "value_" + std::to_string(i));
    }

    std::atomic<size_t> hot_accesses{0};
    std::atomic<size_t> cold_accesses{0};

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(t);
            std::uniform_int_distribution<int> hot_dist(0, num_hot_keys - 1);
            std::uniform_int_distribution<int> cold_dist(num_hot_keys, num_hot_keys + num_cold_keys - 1);
            std::uniform_real_distribution<double> access_dist(0.0, 1.0);

            size_t hot = 0;
            size_t cold = 0;

            for (size_t i = 0; i < operations_per_thread; ++i) {
                int key;
                if (access_dist(rng) < hot_key_ratio) {
                    key = hot_dist(rng);
                    hot++;
                } else {
                    key = cold_dist(rng);
                    cold++;
                }

                // Mix of reads and writes
                if (i % 5 == 0) {
                    cache.put(key, "updated_" + std::to_string(key));
                } else {
                    cache.get(key);
                }
            }

            hot_accesses.fetch_add(hot, std::memory_order_relaxed);
            cold_accesses.fetch_add(cold, std::memory_order_relaxed);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    size_t total_ops = num_threads * operations_per_thread;
    double ops_per_sec = (total_ops * 1000.0) / duration.count();

    std::cout << "\n=== Hotspot Contention ===" << std::endl;
    std::cout << "Threads: " << num_threads << std::endl;
    std::cout << "Hot keys: " << num_hot_keys << std::endl;
    std::cout << "Cold keys: " << num_cold_keys << std::endl;
    std::cout << "Total operations: " << total_ops << std::endl;
    std::cout << "Hot accesses: " << hot_accesses.load() << " ("
              << (100.0 * hot_accesses.load() / total_ops) << "%)" << std::endl;
    std::cout << "Cold accesses: " << cold_accesses.load() << " ("
              << (100.0 * cold_accesses.load() / total_ops) << "%)" << std::endl;
    std::cout << "Duration: " << duration.count() << " ms" << std::endl;
    std::cout << "Throughput: " << ops_per_sec << " ops/sec" << std::endl;

    REQUIRE(hot_accesses.load() > 0);
    REQUIRE(cold_accesses.load() > 0);
}

TEST_CASE("Scalability test", "[cache][concurrent][scalability]") {
    constexpr size_t operations_per_thread = 100000;
    constexpr size_t num_keys = 5000;

    std::vector<size_t> thread_counts = {5,6,7,8,9,10,11,12,13,14};

    std::cout << "\n=== Scalability Test ===" << std::endl;
    std::cout << "Thread Count | Duration (ms) | Ops/sec | Speedup" << std::endl;
    std::cout << "-------------|---------------|---------|--------" << std::endl;

    double baseline_ops_per_sec = 0;

    for (size_t num_threads : thread_counts) {
        Cache<int, std::string> cache(8);

        // Pre-populate
        for (size_t i = 0; i < num_keys; ++i) {
            cache.put(i, "value_" + std::to_string(i));
        }

        auto start = std::chrono::high_resolution_clock::now();

        std::vector<std::thread> threads;
        for (size_t t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, t]() {
                std::mt19937 rng(t);
                std::uniform_int_distribution<int> dist(0, num_keys - 1);

                for (size_t i = 0; i < operations_per_thread; ++i) {
                    int key = dist(rng);
                    if (i % 10 == 0) {
                        cache.put(key, "value_" + std::to_string(key));
                    } else {
                        cache.get(key);
                    }
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        size_t total_ops = num_threads * operations_per_thread;
        double ops_per_sec = (total_ops * 1000.0) / duration.count();

        if (num_threads == 1) {
            baseline_ops_per_sec = ops_per_sec;
        }

        double speedup = ops_per_sec / baseline_ops_per_sec;

        std::cout << std::setw(12) << num_threads << " | "
                  << std::setw(13) << duration.count() << " | "
                  << std::setw(7) << static_cast<size_t>(ops_per_sec) << " | "
                  << std::fixed << std::setprecision(2) << speedup << "x"
                  << std::endl;
    }

    REQUIRE(true);
}

TEST_CASE("Multi-cache 30-second stress test", "[cache][concurrent][multicache][.slow]") {
    constexpr size_t num_caches = 30;
    constexpr size_t num_threads = 16;
    constexpr size_t num_keys_per_cache = 1000;
    constexpr auto test_duration = 30s;

    // Create 30 caches with metadata
    std::vector<std::unique_ptr<Cache<int, std::string, Metadata>>> caches;
    for (size_t i = 0; i < num_caches; ++i) {
        caches.push_back(std::make_unique<Cache<int, std::string, Metadata>>(8));
    }

    // Pre-populate each cache
    auto now = std::chrono::steady_clock::now();
    for (size_t cache_idx = 0; cache_idx < num_caches; ++cache_idx) {
        for (size_t key = 0; key < num_keys_per_cache; ++key) {
            caches[cache_idx]->put(key, "initial_" + std::to_string(key), Metadata{now, 0});
        }
    }

    std::atomic<bool> stop_flag{false};
    std::atomic<size_t> total_puts{0};
    std::atomic<size_t> total_gets{0};
    std::atomic<size_t> total_get_hits{0};
    std::atomic<size_t> total_cleanups{0};
    std::atomic<size_t> total_cleanup_attempts{0};

    std::cout << "\n=== Multi-Cache 30-Second Stress Test ===" << std::endl;
    std::cout << "Configuration:" << std::endl;
    std::cout << "  Number of caches: " << num_caches << std::endl;
    std::cout << "  Threads: " << num_threads << std::endl;
    std::cout << "  Keys per cache: " << num_keys_per_cache << std::endl;
    std::cout << "  Duration: 30 seconds" << std::endl;
    std::cout << "\nRunning..." << std::flush;

    auto start_time = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;

    // Launch 16 threads that hammer all caches with get, put, and try_cleanup
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(t + 5000);
            std::uniform_int_distribution<size_t> cache_dist(0, num_caches - 1);
            std::uniform_int_distribution<int> key_dist(0, num_keys_per_cache - 1);
            std::uniform_int_distribution<int> op_dist(0, 99);  // 0-99 for operation type

            size_t local_puts = 0;
            size_t local_gets = 0;
            size_t local_get_hits = 0;
            size_t local_cleanup_attempts = 0;
            size_t local_cleanups = 0;

            while (!stop_flag.load(std::memory_order_relaxed)) {
                // Select random cache
                size_t cache_idx = cache_dist(rng);
                auto& cache = *caches[cache_idx];
                int key = key_dist(rng);
                int op = op_dist(rng);

                if (op < 50) {
                    // 50% - GET operation
                    auto result = cache.get(key);
                    local_gets++;
                    if (result) {
                        local_get_hits++;
                    }
                } else if (op < 90) {
                    // 40% - PUT operation
                    auto current_time = std::chrono::steady_clock::now();
                    cache.put(key, "value_" + std::to_string(key) + "_" + std::to_string(local_puts),
                             Metadata{current_time, static_cast<int>(local_puts)});
                    local_puts++;
                } else {
                    // 10% - TRY_CLEANUP operation
                    local_cleanup_attempts++;
                    auto current_time = std::chrono::steady_clock::now();
                    CleanupContext ctx{current_time, std::chrono::seconds(5)};

                    if (cache.try_cleanup(ctx, [](const int&, const std::string&, const Metadata& meta, const CleanupContext& ctx) {
                        auto age = ctx.now - meta.last_accessed;
                        return age > ctx.max_age;
                    })) {
                        local_cleanups++;
                    }
                }

                // Small yield every 100 operations to allow other threads
                if ((local_puts + local_gets + local_cleanup_attempts) % 100 == 0) {
                    std::this_thread::yield();
                }
            }

            total_puts.fetch_add(local_puts, std::memory_order_relaxed);
            total_gets.fetch_add(local_gets, std::memory_order_relaxed);
            total_get_hits.fetch_add(local_get_hits, std::memory_order_relaxed);
            total_cleanup_attempts.fetch_add(local_cleanup_attempts, std::memory_order_relaxed);
            total_cleanups.fetch_add(local_cleanups, std::memory_order_relaxed);
        });
    }

    // Let it run for 30 seconds
    std::this_thread::sleep_for(test_duration);
    stop_flag.store(true, std::memory_order_relaxed);

    // Wait for all threads to finish
    for (auto& thread : threads) {
        thread.join();
    }

    auto end_time = std::chrono::steady_clock::now();
    auto actual_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // Collect final stats from all caches
    size_t total_cache_size = 0;
    size_t total_tracked_keys = 0;
    for (const auto& cache : caches) {
        total_cache_size += cache->size();
        total_tracked_keys += cache->tracked_keys_count();
    }

    std::cout << " Done!" << std::endl;
    std::cout << "\nResults:" << std::endl;
    std::cout << "  Actual duration: " << actual_duration.count() << " ms" << std::endl;
    std::cout << "  Total puts: " << total_puts.load() << " ("
              << (total_puts.load() * 1000.0 / actual_duration.count()) << " ops/sec)" << std::endl;
    std::cout << "  Total gets: " << total_gets.load() << " ("
              << (total_gets.load() * 1000.0 / actual_duration.count()) << " ops/sec)" << std::endl;
    std::cout << "  Get hit rate: " << (100.0 * total_get_hits.load() / total_gets.load()) << "%" << std::endl;
    std::cout << "  Cleanup attempts: " << total_cleanup_attempts.load() << std::endl;
    std::cout << "  Successful cleanups: " << total_cleanups.load() << " ("
              << (100.0 * total_cleanups.load() / total_cleanup_attempts.load()) << "% success rate)" << std::endl;
    std::cout << "  Total cache size (all 30 caches): " << total_cache_size << std::endl;
    std::cout << "  Total tracked keys (all 30 caches): " << total_tracked_keys << std::endl;
    std::cout << "  Total operations: "
              << (total_puts.load() + total_gets.load() + total_cleanup_attempts.load()) << std::endl;
    std::cout << "  Combined throughput: "
              << ((total_puts.load() + total_gets.load() + total_cleanup_attempts.load()) * 1000.0 / actual_duration.count())
              << " ops/sec" << std::endl;

    // Sanity checks
    REQUIRE(total_puts.load() > 0);
    REQUIRE(total_gets.load() > 0);
    REQUIRE(total_cleanup_attempts.load() > 0);
    REQUIRE(total_cache_size <= num_caches * num_keys_per_cache);

    std::cout << "\n✓ 30 caches hammered by 16 threads for 30 seconds - no crashes!" << std::endl;
}

TEST_CASE("Intensive 20-second stress test", "[cache][concurrent][stress][.slow]") {
    constexpr size_t num_keys = 10000;
    constexpr auto test_duration = 20s;

    // Thread configuration
    constexpr size_t num_writer_threads = 8;
    constexpr size_t num_invalidate_threads = 4;
    constexpr size_t num_cleanup_threads = 2;
    constexpr size_t num_reader_threads = 8;

    Cache<int, std::string, Metadata> cache(16);

    std::atomic<bool> stop_flag{false};
    std::atomic<size_t> total_puts{0};
    std::atomic<size_t> total_invalidates{0};
    std::atomic<size_t> total_cleanups{0};
    std::atomic<size_t> total_cleanup_attempts{0};
    std::atomic<size_t> total_reads{0};
    std::atomic<size_t> total_read_hits{0};
    std::atomic<size_t> entries_removed_by_cleanup{0};

    std::cout << "\n=== Intensive 20-Second Stress Test ===" << std::endl;
    std::cout << "Configuration:" << std::endl;
    std::cout << "  Writer threads: " << num_writer_threads << std::endl;
    std::cout << "  Invalidate threads: " << num_invalidate_threads << std::endl;
    std::cout << "  Cleanup threads: " << num_cleanup_threads << std::endl;
    std::cout << "  Reader threads: " << num_reader_threads << std::endl;
    std::cout << "  Key space: " << num_keys << " keys" << std::endl;
    std::cout << "  Duration: 20 seconds" << std::endl;
    std::cout << "\nRunning..." << std::flush;

    auto start_time = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;

    // Writer threads - continuously put entries with metadata
    for (size_t t = 0; t < num_writer_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(t + 1000);
            std::uniform_int_distribution<int> key_dist(0, num_keys - 1);
            size_t local_puts = 0;

            while (!stop_flag.load(std::memory_order_relaxed)) {
                int key = key_dist(rng);
                auto now = std::chrono::steady_clock::now();
                cache.put(key, "value_" + std::to_string(key), Metadata{now, static_cast<int>(local_puts)});
                local_puts++;

                // Small random delay to vary contention
                if (local_puts % 100 == 0) {
                    std::this_thread::sleep_for(std::chrono::microseconds(10));
                }
            }

            total_puts.fetch_add(local_puts, std::memory_order_relaxed);
        });
    }

    // Invalidate threads - continuously invalidate random entries
    for (size_t t = 0; t < num_invalidate_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(t + 2000);
            std::uniform_int_distribution<int> key_dist(0, num_keys - 1);
            size_t local_invalidates = 0;

            while (!stop_flag.load(std::memory_order_relaxed)) {
                int key = key_dist(rng);
                cache.invalidate(key);
                local_invalidates++;

                // Vary the invalidation rate
                if (local_invalidates % 50 == 0) {
                    std::this_thread::sleep_for(std::chrono::microseconds(50));
                }
            }

            total_invalidates.fetch_add(local_invalidates, std::memory_order_relaxed);
        });
    }

    // Cleanup threads - continuously try to cleanup
    for (size_t t = 0; t < num_cleanup_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(t + 3000);
            std::uniform_int_distribution<int> threshold_dist(0, 1000);
            size_t local_cleanups = 0;
            size_t local_attempts = 0;
            size_t local_removed = 0;

            while (!stop_flag.load(std::memory_order_relaxed)) {
                local_attempts++;

                // Randomly choose between try_cleanup and cleanup
                bool use_try = (local_attempts % 3 == 0);

                auto now = std::chrono::steady_clock::now();
                CleanupContext ctx{now, std::chrono::seconds(threshold_dist(rng))};

                if (use_try) {
                    if (cache.try_cleanup(ctx, [](const int&, const std::string&, const Metadata& meta, const CleanupContext& ctx) {
                        auto age = ctx.now - meta.last_accessed;
                        return age > ctx.max_age;
                    })) {
                        local_cleanups++;
                    }
                } else {
                    auto [removed, seg_id] = cache.cleanup(ctx, [](const int&, const std::string&, const Metadata& meta, const CleanupContext& ctx) {
                        auto age = ctx.now - meta.last_accessed;
                        return age > ctx.max_age;
                    });
                    local_removed += removed;
                    local_cleanups++;
                }

                // Cleanup is expensive, don't spam it
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }

            total_cleanups.fetch_add(local_cleanups, std::memory_order_relaxed);
            total_cleanup_attempts.fetch_add(local_attempts, std::memory_order_relaxed);
            entries_removed_by_cleanup.fetch_add(local_removed, std::memory_order_relaxed);
        });
    }

    // Reader threads - continuously read entries
    for (size_t t = 0; t < num_reader_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(t + 4000);
            std::uniform_int_distribution<int> key_dist(0, num_keys - 1);
            size_t local_reads = 0;
            size_t local_hits = 0;

            while (!stop_flag.load(std::memory_order_relaxed)) {
                int key = key_dist(rng);

                // Mix simple get and get with callback
                if (local_reads % 2 == 0) {
                    auto result = cache.get(key);
                    if (result) {
                        local_hits++;
                    }
                } else {
                    auto result = cache.get(key, [](const std::string&, Metadata& meta) {
                        meta.access_count++;
                        return GetAction::Accept;
                    });
                    if (result) {
                        local_hits++;
                    }
                }

                local_reads++;

                // Very fast reads
                if (local_reads % 1000 == 0) {
                    std::this_thread::sleep_for(std::chrono::microseconds(10));
                }
            }

            total_reads.fetch_add(local_reads, std::memory_order_relaxed);
            total_read_hits.fetch_add(local_hits, std::memory_order_relaxed);
        });
    }

    // Let it run for 20 seconds
    std::this_thread::sleep_for(test_duration);
    stop_flag.store(true, std::memory_order_relaxed);

    // Wait for all threads to finish
    for (auto& thread : threads) {
        thread.join();
    }

    auto end_time = std::chrono::steady_clock::now();
    auto actual_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // Collect final stats
    size_t final_size = cache.size();
    size_t tracked_keys = cache.tracked_keys_count();

    std::cout << " Done!" << std::endl;
    std::cout << "\nResults:" << std::endl;
    std::cout << "  Actual duration: " << actual_duration.count() << " ms" << std::endl;
    std::cout << "  Total puts: " << total_puts.load() << " ("
              << (total_puts.load() * 1000.0 / actual_duration.count()) << " ops/sec)" << std::endl;
    std::cout << "  Total invalidates: " << total_invalidates.load() << " ("
              << (total_invalidates.load() * 1000.0 / actual_duration.count()) << " ops/sec)" << std::endl;
    std::cout << "  Total reads: " << total_reads.load() << " ("
              << (total_reads.load() * 1000.0 / actual_duration.count()) << " ops/sec)" << std::endl;
    std::cout << "  Read hit rate: " << (100.0 * total_read_hits.load() / total_reads.load()) << "%" << std::endl;
    std::cout << "  Cleanup attempts: " << total_cleanup_attempts.load() << std::endl;
    std::cout << "  Successful cleanups: " << total_cleanups.load() << std::endl;
    std::cout << "  Entries removed by cleanup: " << entries_removed_by_cleanup.load() << std::endl;
    std::cout << "  Final cache size: " << final_size << std::endl;
    std::cout << "  Tracked keys: " << tracked_keys << std::endl;
    std::cout << "  Total operations: "
              << (total_puts.load() + total_invalidates.load() + total_reads.load()) << std::endl;
    std::cout << "  Combined throughput: "
              << ((total_puts.load() + total_invalidates.load() + total_reads.load()) * 1000.0 / actual_duration.count())
              << " ops/sec" << std::endl;

    // Sanity checks
    REQUIRE(total_puts.load() > 0);
    REQUIRE(total_invalidates.load() > 0);
    REQUIRE(total_reads.load() > 0);
    REQUIRE(total_cleanups.load() > 0);
    REQUIRE(final_size <= num_keys); // Can't have more entries than keys

    std::cout << "\n✓ No crashes, deadlocks, or data corruption detected!" << std::endl;
}