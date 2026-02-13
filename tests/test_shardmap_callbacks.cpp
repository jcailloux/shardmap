#include <catch2/catch_test_macros.hpp>
#include <jcailloux/shardmap/ShardMap.h>
#include <atomic>
#include <memory>

using namespace jcailloux::shardmap;

// =============================================================================
// Metadata with atomic fields (for shared-lock mutation)
// =============================================================================

struct TTLMetadata {
    std::atomic<int64_t> cached_at_rep;

    TTLMetadata() : cached_at_rep(0) {}
    TTLMetadata(int64_t v) : cached_at_rep(v) {}

    // Copy constructor (required by flat_hash_map): load atomically
    TTLMetadata(const TTLMetadata& other)
        : cached_at_rep(other.cached_at_rep.load(std::memory_order_relaxed)) {}
    TTLMetadata& operator=(const TTLMetadata& other) {
        cached_at_rep.store(other.cached_at_rep.load(std::memory_order_relaxed),
                           std::memory_order_relaxed);
        return *this;
    }
    TTLMetadata(TTLMetadata&& other) noexcept
        : cached_at_rep(other.cached_at_rep.load(std::memory_order_relaxed)) {}
    TTLMetadata& operator=(TTLMetadata&& other) noexcept {
        cached_at_rep.store(other.cached_at_rep.load(std::memory_order_relaxed),
                           std::memory_order_relaxed);
        return *this;
    }
};

// =============================================================================
// Get with callback
// =============================================================================

TEST_CASE("ShardMap: get with callback — Accept", "[shardmap][callbacks]") {
    ShardMap<int64_t, int, TTLMetadata> map;
    map.put(1, 42, TTLMetadata{100});

    auto result = map.get(1, [](const int& value, TTLMetadata& meta) {
        REQUIRE(value == 42);
        REQUIRE(meta.cached_at_rep.load() == 100);
        return GetAction::Accept;
    });

    REQUIRE(result.has_value());
    REQUIRE(*result == 42);
    REQUIRE(map.contains(1));  // Entry still present
}

TEST_CASE("ShardMap: get with callback — Reject", "[shardmap][callbacks]") {
    ShardMap<int64_t, int, TTLMetadata> map;
    map.put(1, 42, TTLMetadata{100});

    auto result = map.get(1, [](const int&, TTLMetadata&) {
        return GetAction::Reject;
    });

    REQUIRE_FALSE(result.has_value());
    REQUIRE(map.contains(1));  // Entry still present
}

TEST_CASE("ShardMap: get with callback — Invalidate", "[shardmap][callbacks]") {
    ShardMap<int64_t, int, TTLMetadata> map;
    map.put(1, 42, TTLMetadata{100});

    auto result = map.get(1, [](const int&, TTLMetadata&) {
        return GetAction::Invalidate;
    });

    REQUIRE_FALSE(result.has_value());
    REQUIRE_FALSE(map.contains(1));  // Entry erased
}

TEST_CASE("ShardMap: get with callback — metadata mutation under shared lock", "[shardmap][callbacks]") {
    ShardMap<int64_t, int, TTLMetadata> map;
    map.put(1, 42, TTLMetadata{100});

    // Refresh TTL (atomic mutation under shared lock)
    auto result = map.get(1, [](const int& value, TTLMetadata& meta) {
        meta.cached_at_rep.store(200, std::memory_order_relaxed);
        return GetAction::Accept;
    });

    REQUIRE(result.has_value());

    // Verify the TTL was updated
    map.get(1, [](const int&, TTLMetadata& meta) {
        REQUIRE(meta.cached_at_rep.load() == 200);
        return GetAction::Accept;
    });
}

TEST_CASE("ShardMap: get with callback — const metadata", "[shardmap][callbacks]") {
    ShardMap<int64_t, int, TTLMetadata> map;
    map.put(1, 42, TTLMetadata{100});

    // Read-only callback (const Metadata&)
    auto result = map.get(1, [](const int& value, const TTLMetadata& meta) {
        REQUIRE(meta.cached_at_rep.load() == 100);
        return GetAction::Accept;
    });

    REQUIRE(result.has_value());
}

TEST_CASE("ShardMap: get with callback — not found", "[shardmap][callbacks]") {
    ShardMap<int64_t, int, TTLMetadata> map;

    auto result = map.get(999, [](const int&, TTLMetadata&) -> GetAction {
        FAIL("Callback should not be called for missing key");
        return GetAction::Accept;
    });

    REQUIRE_FALSE(result.has_value());
}

// =============================================================================
// Get with callback — shared_ptr value (nullable)
// =============================================================================

TEST_CASE("ShardMap: get callback with shared_ptr value", "[shardmap][callbacks]") {
    ShardMap<int64_t, std::shared_ptr<const std::string>, TTLMetadata> map;
    map.put(1, std::make_shared<const std::string>("hello"), TTLMetadata{100});

    auto result = map.get(1, [](const std::shared_ptr<const std::string>& val, TTLMetadata& meta) {
        REQUIRE(*val == "hello");
        meta.cached_at_rep.store(200, std::memory_order_relaxed);
        return GetAction::Accept;
    });

    REQUIRE(result != nullptr);
    REQUIRE(*result == "hello");
}

// =============================================================================
// Get with callback — shard_id parameter
// =============================================================================

TEST_CASE("ShardMap: get with callback receives shard_id", "[shardmap][callbacks]") {
    ShardMap<int64_t, int, TTLMetadata> map;
    map.put(1, 42, TTLMetadata{100});

    uint8_t received_shard_id = 255;
    auto result = map.get(1, [&](const int& value, TTLMetadata& meta, uint8_t shard_id) {
        REQUIRE(value == 42);
        received_shard_id = shard_id;
        return GetAction::Accept;
    });

    REQUIRE(result.has_value());
    REQUIRE(*result == 42);
    // shard_id should be deterministic and within range [0, shard_count)
    REQUIRE(received_shard_id < map.shard_count());
}

TEST_CASE("ShardMap: get callback shard_id is consistent", "[shardmap][callbacks]") {
    ShardMap<int64_t, int, TTLMetadata> map;
    map.put(42, 100, TTLMetadata{1});

    // Call get twice: shard_id must be the same (deterministic from key)
    uint8_t sid1 = 255, sid2 = 255;

    map.get(42, [&](const int&, TTLMetadata&, uint8_t sid) {
        sid1 = sid;
        return GetAction::Accept;
    });
    map.get(42, [&](const int&, TTLMetadata&, uint8_t sid) {
        sid2 = sid;
        return GetAction::Accept;
    });

    REQUIRE(sid1 == sid2);
}

// =============================================================================
// Conditional invalidate
// =============================================================================

TEST_CASE("ShardMap: conditional invalidate", "[shardmap][callbacks]") {
    ShardMap<int64_t, int, TTLMetadata> map;
    map.put(1, 42, TTLMetadata{100});
    map.put(2, 99, TTLMetadata{50});

    // Only invalidate entries with cached_at < 75
    auto pred = [](const int&, const TTLMetadata& meta) {
        return meta.cached_at_rep.load(std::memory_order_relaxed) < 75;
    };

    REQUIRE_FALSE(map.invalidate(1, pred));  // 100 >= 75, kept
    REQUIRE(map.invalidate(2, pred));         // 50 < 75, erased

    REQUIRE(map.contains(1));
    REQUIRE_FALSE(map.contains(2));
}
