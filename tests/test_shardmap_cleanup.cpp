#include <catch2/catch_test_macros.hpp>
#include <jcailloux/shardmap/ShardMap.h>
#include <atomic>
#include <thread>
#include <vector>

using namespace jcailloux::shardmap;

struct ExpMeta {
    int64_t expiration;
};

struct CleanupCtx {
    int64_t now;
};

// =============================================================================
// Basic cleanup
// =============================================================================

TEST_CASE("ShardMap: try_cleanup removes expired entries", "[shardmap][cleanup]") {
    constexpr auto cfg = ShardMapConfig{}.with_shard_count_log2(2);  // 4 shards for predictability
    ShardMap<int64_t, int, ExpMeta, cfg> map;

    // Insert entries with varying expiration
    for (int64_t i = 0; i < 100; ++i) {
        map.put(i, static_cast<int>(i), ExpMeta{.expiration = i % 2 == 0 ? 50 : 200});
    }

    REQUIRE(map.size() == 100);

    CleanupCtx ctx{.now = 100};

    // Cleanup predicate: remove if expired (expiration < now)
    // Using 3-param metadata-only version for efficient phase-2 evaluation
    auto should_remove = [](const int64_t&, const ExpMeta& meta, const CleanupCtx& ctx) {
        return meta.expiration < ctx.now;
    };

    // Run cleanup on all shards
    size_t total_removed = map.full_cleanup(ctx, should_remove);

    // All even-indexed entries (expiration=50) should be removed
    REQUIRE(total_removed == 50);
    REQUIRE(map.size() == 50);

    // Verify remaining entries are the odd ones
    for (int64_t i = 0; i < 100; ++i) {
        if (i % 2 == 0) {
            REQUIRE_FALSE(map.get(i).has_value());
        } else {
            REQUIRE(map.get(i).has_value());
        }
    }
}

TEST_CASE("ShardMap: try_cleanup is non-blocking", "[shardmap][cleanup]") {
    ShardMap<int64_t, int, ExpMeta> map;
    map.put(1, 42, ExpMeta{.expiration = 0});

    CleanupCtx ctx{.now = 100};
    auto should_remove = [](const int64_t&, const ExpMeta& meta, const CleanupCtx& ctx) {
        return meta.expiration < ctx.now;
    };

    auto result1 = map.try_cleanup(ctx, should_remove);
    REQUIRE(result1.has_value());
}

TEST_CASE("ShardMap: partial cleanup advances through shards", "[shardmap][cleanup]") {
    constexpr auto cfg = ShardMapConfig{}.with_shard_count_log2(2);  // 4 shards
    ShardMap<int64_t, int, ExpMeta, cfg> map;

    // Insert many entries to cover all shards
    for (int64_t i = 0; i < 100; ++i) {
        map.put(i, static_cast<int>(i), ExpMeta{.expiration = 0});
    }

    CleanupCtx ctx{.now = 100};
    auto should_remove = [](const int64_t&, const ExpMeta& meta, const CleanupCtx& ctx) {
        return meta.expiration < ctx.now;
    };

    // Run 4 partial cleanups (one per shard)
    size_t total_removed = 0;
    for (int i = 0; i < 4; ++i) {
        auto result = map.cleanup(ctx, should_remove);
        total_removed += result.removed;
    }

    REQUIRE(total_removed == 100);
    REQUIRE(map.empty());
}

// =============================================================================
// Cleanup with value-based predicate (4-param)
// =============================================================================

TEST_CASE("ShardMap: cleanup with value-based predicate", "[shardmap][cleanup]") {
    constexpr auto cfg = ShardMapConfig{}.with_shard_count_log2(2);
    ShardMap<int64_t, int, ExpMeta, cfg> map;

    for (int64_t i = 0; i < 50; ++i) {
        map.put(i, static_cast<int>(i * 10), ExpMeta{.expiration = 0});
    }

    CleanupCtx ctx{.now = 100};

    // Remove only entries where value >= 250
    auto should_remove = [](const int64_t&, const int& value, const ExpMeta&, const CleanupCtx&) {
        return value >= 250;
    };

    size_t removed = map.full_cleanup(ctx, should_remove);

    // Values 0,10,20,...,240 kept (25 entries). Values 250,260,...,490 removed (25 entries).
    REQUIRE(removed == 25);
    REQUIRE(map.size() == 25);
}

// =============================================================================
// Cleanup with shard_id parameter (5-param)
// =============================================================================

TEST_CASE("ShardMap: cleanup with shard_id parameter", "[shardmap][cleanup]") {
    constexpr auto cfg = ShardMapConfig{}.with_shard_count_log2(2);
    ShardMap<int64_t, int, ExpMeta, cfg> map;

    for (int64_t i = 0; i < 50; ++i) {
        map.put(i, static_cast<int>(i), ExpMeta{.expiration = 0});
    }

    CleanupCtx ctx{.now = 100};

    // Verify shard_id is passed correctly
    auto should_remove = [](const int64_t&, const int&, const ExpMeta&,
                           const CleanupCtx&, uint8_t shard_id) {
        // Only remove entries in shard 0
        return shard_id == 0;
    };

    size_t removed = map.full_cleanup(ctx, should_remove);
    REQUIRE(removed > 0);  // At least some entries in shard 0
    REQUIRE(map.size() > 0);  // Entries in other shards survive
}

// =============================================================================
// TOCTOU protection: entry refreshed between phases
// =============================================================================

TEST_CASE("ShardMap: cleanup TOCTOU — refreshed entry is kept", "[shardmap][cleanup]") {
    constexpr auto cfg = ShardMapConfig{}.with_shard_count_log2(2);
    ShardMap<int64_t, int, ExpMeta, cfg> map;

    // Insert entry that will be expired
    map.put(int64_t{1}, 42, ExpMeta{.expiration = 50});

    CleanupCtx ctx{.now = 100};

    // This test verifies the re-check in phase 3:
    // If between phase 1 (copy) and phase 3 (erase), the entry is refreshed,
    // the re-check should see the new expiration and keep the entry.
    //
    // We can't easily inject between phases in a unit test, but we can verify
    // that a non-expired entry is correctly kept.

    // Refresh the entry with new expiration BEFORE cleanup
    map.put(int64_t{1}, 42, ExpMeta{.expiration = 200});

    auto should_remove = [](const int64_t&, const ExpMeta& meta, const CleanupCtx& ctx) {
        return meta.expiration < ctx.now;
    };

    size_t removed = map.full_cleanup(ctx, should_remove);
    REQUIRE(removed == 0);
    REQUIRE(map.contains(1));
}

// =============================================================================
// Concurrent cleanup + reads
// =============================================================================

TEST_CASE("ShardMap: concurrent cleanup with reads", "[shardmap][cleanup][concurrent]") {
    constexpr auto cfg = ShardMapConfig{}.with_shard_count_log2(3);  // 8 shards
    ShardMap<int64_t, int, ExpMeta, cfg> map;

    // Populate: half expired, half valid
    for (int64_t i = 0; i < 1000; ++i) {
        map.put(i, static_cast<int>(i), ExpMeta{.expiration = i % 2 == 0 ? 0 : 200});
    }

    CleanupCtx ctx{.now = 100};
    auto should_remove = [](const int64_t&, const ExpMeta& meta, const CleanupCtx& ctx) {
        return meta.expiration < ctx.now;
    };

    std::atomic<bool> done{false};
    std::atomic<size_t> read_count{0};

    std::vector<std::jthread> threads;

    // Concurrent readers
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&map, &done, &read_count] {
            while (!done.load(std::memory_order_relaxed)) {
                for (int64_t i = 0; i < 1000; ++i) {
                    (void)map.get(i);
                    ++read_count;
                }
            }
        });
    }

    // Cleanup thread
    threads.emplace_back([&map, &ctx, &should_remove, &done] {
        map.full_cleanup(ctx, should_remove);
        done.store(true, std::memory_order_relaxed);
    });

    threads.clear();

    REQUIRE(map.size() == 500);
    REQUIRE(read_count > 0);
}

// =============================================================================
// Concurrent cleanup + writes
// =============================================================================

TEST_CASE("ShardMap: concurrent cleanup with writes", "[shardmap][cleanup][concurrent]") {
    constexpr auto cfg = ShardMapConfig{}.with_shard_count_log2(3);
    ShardMap<int64_t, int, ExpMeta, cfg> map;

    for (int64_t i = 0; i < 500; ++i) {
        map.put(i, static_cast<int>(i), ExpMeta{.expiration = 0});
    }

    CleanupCtx ctx{.now = 100};
    auto should_remove = [](const int64_t&, const ExpMeta& meta, const CleanupCtx& ctx) {
        return meta.expiration < ctx.now;
    };

    std::atomic<bool> cleanup_done{false};
    std::vector<std::jthread> threads;

    // Writer: adds new entries while cleanup runs
    threads.emplace_back([&map, &cleanup_done] {
        int64_t key = 10'000;
        while (!cleanup_done.load(std::memory_order_relaxed)) {
            map.put(key++, 999, ExpMeta{.expiration = 999});
        }
    });

    // Cleanup
    threads.emplace_back([&map, &ctx, &should_remove, &cleanup_done] {
        map.full_cleanup(ctx, should_remove);
        cleanup_done.store(true, std::memory_order_relaxed);
    });

    threads.clear();

    // Original 500 expired entries should be removed
    for (int64_t i = 0; i < 500; ++i) {
        REQUIRE_FALSE(map.contains(i));
    }
    // New entries written during cleanup should survive
    REQUIRE(map.size() > 0);
}
