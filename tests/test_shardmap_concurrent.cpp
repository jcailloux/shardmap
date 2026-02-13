#include <catch2/catch_test_macros.hpp>
#include <jcailloux/shardmap/ShardMap.h>
#include <atomic>
#include <memory>
#include <thread>
#include <vector>

using namespace jcailloux::shardmap;

static constexpr int NUM_THREADS = 8;
static constexpr int OPS_PER_THREAD = 10'000;

// =============================================================================
// Concurrent reads
// =============================================================================

TEST_CASE("ShardMap: concurrent reads", "[shardmap][concurrent]") {
    ShardMap<int64_t, int> map;

    // Pre-populate
    for (int64_t i = 0; i < 1000; ++i) {
        map.put(i, static_cast<int>(i));
    }

    std::atomic<size_t> hits{0};
    std::vector<std::jthread> threads;

    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&map, &hits] {
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int64_t key = i % 1000;
                auto result = map.get(key);
                if (result.has_value()) {
                    ++hits;
                }
            }
        });
    }

    threads.clear();  // join all
    REQUIRE(hits == NUM_THREADS * OPS_PER_THREAD);
}

// =============================================================================
// Concurrent writes
// =============================================================================

TEST_CASE("ShardMap: concurrent writes", "[shardmap][concurrent]") {
    ShardMap<int64_t, int> map;

    std::vector<std::jthread> threads;

    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&map, t] {
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int64_t key = t * OPS_PER_THREAD + i;
                map.put(key, static_cast<int>(key));
            }
        });
    }

    threads.clear();
    REQUIRE(map.size() == NUM_THREADS * OPS_PER_THREAD);
}

// =============================================================================
// Concurrent mixed reads and writes
// =============================================================================

TEST_CASE("ShardMap: concurrent mixed read/write", "[shardmap][concurrent]") {
    ShardMap<int64_t, int> map;

    // Pre-populate with 500 entries
    for (int64_t i = 0; i < 500; ++i) {
        map.put(i, static_cast<int>(i));
    }

    std::atomic<size_t> reads{0};
    std::atomic<size_t> writes{0};
    std::vector<std::jthread> threads;

    // Readers
    for (int t = 0; t < NUM_THREADS / 2; ++t) {
        threads.emplace_back([&map, &reads] {
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int64_t key = i % 1000;
                (void)map.get(key);
                ++reads;
            }
        });
    }

    // Writers
    for (int t = 0; t < NUM_THREADS / 2; ++t) {
        threads.emplace_back([&map, &writes, t] {
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int64_t key = 500 + t * OPS_PER_THREAD + i;
                map.put(key, static_cast<int>(key));
                ++writes;
            }
        });
    }

    threads.clear();
    REQUIRE(reads == (NUM_THREADS / 2) * OPS_PER_THREAD);
    REQUIRE(writes == (NUM_THREADS / 2) * OPS_PER_THREAD);
}

// =============================================================================
// Concurrent reads with callback (shared lock)
// =============================================================================

struct AtomicMeta {
    std::atomic<int64_t> counter;
    AtomicMeta() : counter(0) {}
    AtomicMeta(int64_t v) : counter(v) {}
    AtomicMeta(const AtomicMeta& o) : counter(o.counter.load(std::memory_order_relaxed)) {}
    AtomicMeta& operator=(const AtomicMeta& o) {
        counter.store(o.counter.load(std::memory_order_relaxed), std::memory_order_relaxed);
        return *this;
    }
    AtomicMeta(AtomicMeta&& o) noexcept : counter(o.counter.load(std::memory_order_relaxed)) {}
    AtomicMeta& operator=(AtomicMeta&& o) noexcept {
        counter.store(o.counter.load(std::memory_order_relaxed), std::memory_order_relaxed);
        return *this;
    }
};

TEST_CASE("ShardMap: concurrent get with callback (atomic metadata mutation)", "[shardmap][concurrent]") {
    ShardMap<int64_t, int, AtomicMeta> map;

    // Single entry accessed by all threads
    map.put(int64_t{1}, 42, AtomicMeta{0});

    std::vector<std::jthread> threads;

    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&map] {
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                map.get(int64_t{1}, [](const int& val, AtomicMeta& meta) {
                    meta.counter.fetch_add(1, std::memory_order_relaxed);
                    return GetAction::Accept;
                });
            }
        });
    }

    threads.clear();

    // Verify all increments were applied
    map.get(int64_t{1}, [](const int&, AtomicMeta& meta) {
        REQUIRE(meta.counter.load() == NUM_THREADS * OPS_PER_THREAD);
        return GetAction::Accept;
    });
}

// =============================================================================
// Concurrent read + invalidate
// =============================================================================

TEST_CASE("ShardMap: concurrent read and invalidate", "[shardmap][concurrent]") {
    ShardMap<int64_t, int> map;

    for (int64_t i = 0; i < 1000; ++i) {
        map.put(i, static_cast<int>(i));
    }

    std::atomic<bool> done{false};
    std::atomic<size_t> invalidated{0};

    std::vector<std::jthread> threads;

    // Readers
    for (int t = 0; t < NUM_THREADS - 1; ++t) {
        threads.emplace_back([&map, &done] {
            while (!done.load(std::memory_order_relaxed)) {
                for (int64_t i = 0; i < 1000; ++i) {
                    (void)map.get(i);  // May or may not find entry
                }
            }
        });
    }

    // Invalidator
    threads.emplace_back([&map, &done, &invalidated] {
        for (int64_t i = 0; i < 1000; ++i) {
            if (map.invalidate(i)) {
                ++invalidated;
            }
        }
        done.store(true, std::memory_order_relaxed);
    });

    threads.clear();
    REQUIRE(invalidated == 1000);
    REQUIRE(map.empty());
}

// =============================================================================
// Hotspot contention (many threads, same keys)
// =============================================================================

TEST_CASE("ShardMap: hotspot contention", "[shardmap][concurrent]") {
    ShardMap<int64_t, int> map;

    // 10 hot keys
    for (int64_t i = 0; i < 10; ++i) {
        map.put(i, static_cast<int>(i));
    }

    std::atomic<size_t> total_ops{0};
    std::vector<std::jthread> threads;

    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&map, &total_ops, t] {
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int64_t key = i % 10;
                if (i % 5 == 0) {
                    map.put(key, t * 1000 + i);
                } else {
                    (void)map.get(key);
                }
                ++total_ops;
            }
        });
    }

    threads.clear();
    REQUIRE(total_ops == NUM_THREADS * OPS_PER_THREAD);
    REQUIRE(map.size() == 10);
}
