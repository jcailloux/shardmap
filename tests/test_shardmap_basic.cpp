#include <catch2/catch_test_macros.hpp>
#include <jcailloux/shardmap/ShardMap.h>
#include <memory>
#include <string>
#include <utility>

using namespace jcailloux::shardmap;

// =============================================================================
// Basic get/put/invalidate
// =============================================================================

TEST_CASE("ShardMap: put and get basic types", "[shardmap][basic]") {
    ShardMap<int64_t, int> map;

    SECTION("get on empty map returns default") {
        auto result = map.get(42);
        REQUIRE_FALSE(result.has_value());
    }

    SECTION("put then get returns value") {
        map.put(1, 100);
        auto result = map.get(1);
        REQUIRE(result.has_value());
        REQUIRE(*result == 100);
    }

    SECTION("put returns true for new key, false for existing") {
        REQUIRE(map.put(1, 100));
        REQUIRE_FALSE(map.put(1, 200));

        auto result = map.get(1);
        REQUIRE(*result == 200);  // Value updated
    }

    SECTION("multiple keys") {
        for (int64_t i = 0; i < 1000; ++i) {
            map.put(i, static_cast<int>(i * 10));
        }

        REQUIRE(map.size() == 1000);

        for (int64_t i = 0; i < 1000; ++i) {
            auto result = map.get(i);
            REQUIRE(result.has_value());
            REQUIRE(*result == static_cast<int>(i * 10));
        }
    }
}

TEST_CASE("ShardMap: put with metadata", "[shardmap][basic]") {
    struct Meta {
        int64_t expiration;
    };

    ShardMap<int64_t, int, Meta> map;

    map.put(1, 42, Meta{.expiration = 1000});
    auto result = map.get(1);
    REQUIRE(result.has_value());
    REQUIRE(*result == 42);
}

TEST_CASE("ShardMap: nullable types return directly", "[shardmap][basic]") {
    SECTION("shared_ptr") {
        ShardMap<int64_t, std::shared_ptr<const std::string>> map;

        auto result = map.get(1);
        REQUIRE(result == nullptr);

        map.put(1, std::make_shared<const std::string>("hello"));
        result = map.get(1);
        REQUIRE(result != nullptr);
        REQUIRE(*result == "hello");
    }

    SECTION("optional") {
        ShardMap<int64_t, std::optional<int>> map;

        auto result = map.get(1);
        REQUIRE_FALSE(result.has_value());

        map.put(1, std::optional<int>(42));
        result = map.get(1);
        REQUIRE(result.has_value());
        REQUIRE(*result == 42);
    }

    SECTION("raw pointer") {
        ShardMap<int64_t, const int*> map;

        auto result = map.get(1);
        REQUIRE(result == nullptr);

        static const int val = 42;
        map.put(1, &val);
        result = map.get(1);
        REQUIRE(result == &val);
    }
}

// =============================================================================
// Invalidation
// =============================================================================

TEST_CASE("ShardMap: invalidate", "[shardmap][basic]") {
    ShardMap<int64_t, int> map;
    map.put(1, 100);
    map.put(2, 200);

    SECTION("unconditional invalidate") {
        REQUIRE(map.invalidate(1));
        REQUIRE_FALSE(map.get(1).has_value());
        REQUIRE(map.get(2).has_value());
    }

    SECTION("invalidate non-existent key returns false") {
        REQUIRE_FALSE(map.invalidate(999));
    }

    SECTION("conditional invalidate") {
        // Only invalidate if value > 150
        REQUIRE_FALSE(map.invalidate(1, [](const int& v, const auto&) { return v > 150; }));
        REQUIRE(map.get(1).has_value());  // Kept

        REQUIRE(map.invalidate(2, [](const int& v, const auto&) { return v > 150; }));
        REQUIRE_FALSE(map.get(2).has_value());  // Erased
    }
}

// =============================================================================
// Introspection
// =============================================================================

TEST_CASE("ShardMap: size, empty, contains", "[shardmap][basic]") {
    ShardMap<int64_t, int> map;

    REQUIRE(map.empty());
    REQUIRE(map.size() == 0);
    REQUIRE_FALSE(map.contains(1));

    map.put(1, 100);
    REQUIRE_FALSE(map.empty());
    REQUIRE(map.size() == 1);
    REQUIRE(map.contains(1));

    map.invalidate(1);
    REQUIRE(map.empty());
    REQUIRE(map.size() == 0);
    REQUIRE_FALSE(map.contains(1));
}

// =============================================================================
// Configuration
// =============================================================================

TEST_CASE("ShardMap: shard_count reflects config", "[shardmap][basic]") {
    SECTION("default 8 shards") {
        ShardMap<int64_t, int> map;
        REQUIRE(map.shard_count() == 8);
    }

    SECTION("custom shard count") {
        constexpr auto cfg = ShardMapConfig{}.with_shard_count_log2(4);  // 16 shards
        ShardMap<int64_t, int, std::monostate, cfg> map;
        REQUIRE(map.shard_count() == 16);
    }
}

// =============================================================================
// String keys
// =============================================================================

TEST_CASE("ShardMap: non-integral key types", "[shardmap][basic]") {
    ShardMap<std::string, int> map;

    map.put(std::string("hello"), 1);
    map.put(std::string("world"), 2);

    REQUIRE(map.get("hello").has_value());
    REQUIRE(*map.get("hello") == 1);
    REQUIRE(*map.get("world") == 2);
}

TEST_CASE("ShardMap: short string keys", "[shardmap][basic]") {
    ShardMap<std::string, int> map;

    // Empty key (n == 0 branch)
    map.put(std::string(""), 0);
    REQUIRE(map.get("").has_value());
    REQUIRE(*map.get("") == 0);

    // 1-byte key (0 < n < 4 branch)
    map.put(std::string("a"), 1);
    REQUIRE(*map.get("a") == 1);

    // 2-byte key
    map.put(std::string("ab"), 2);
    REQUIRE(*map.get("ab") == 2);

    // 3-byte key
    map.put(std::string("abc"), 3);
    REQUIRE(*map.get("abc") == 3);

    // 4-byte key (n >= 4 branch)
    map.put(std::string("abcd"), 4);
    REQUIRE(*map.get("abcd") == 4);

    REQUIRE(map.size() == 5);
}

TEST_CASE("ShardMap: string key distribution", "[shardmap][basic]") {
    ShardMap<std::string, int> map;

    constexpr int N = 800;
    for (int i = 0; i < N; ++i) {
        map.put("key_" + std::to_string(i), i);
    }

    REQUIRE(map.size() == N);

    for (int i = 0; i < N; ++i) {
        auto result = map.get("key_" + std::to_string(i));
        REQUIRE(result.has_value());
        REQUIRE(*result == i);
    }
}

// =============================================================================
// Pair/tuple keys (recursive fast_shard_hash)
// =============================================================================

TEST_CASE("ShardMap: pair key type", "[shardmap][basic]") {
    using Key = std::pair<int, std::string>;
    ShardMap<Key, int> map;

    map.put(Key{1, "alpha"}, 10);
    map.put(Key{2, "beta"}, 20);
    map.put(Key{1, "beta"}, 30);

    REQUIRE(map.get(Key{1, "alpha"}).has_value());
    REQUIRE(*map.get(Key{1, "alpha"}) == 10);
    REQUIRE(*map.get(Key{2, "beta"}) == 20);
    REQUIRE(*map.get(Key{1, "beta"}) == 30);
    REQUIRE_FALSE(map.get(Key{3, "gamma"}).has_value());
}

// =============================================================================
// Custom struct key (absl::Hash fallback)
// =============================================================================

namespace {
struct Point {
    double x, y;

    bool operator==(const Point&) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const Point& p) {
        return H::combine(std::move(h), p.x, p.y);
    }
};
}  // namespace

TEST_CASE("ShardMap: custom struct key type", "[shardmap][basic]") {
    ShardMap<Point, int> map;

    map.put(Point{1.0, 2.0}, 10);
    map.put(Point{3.0, 4.0}, 20);

    REQUIRE(map.get(Point{1.0, 2.0}).has_value());
    REQUIRE(*map.get(Point{1.0, 2.0}) == 10);
    REQUIRE(*map.get(Point{3.0, 4.0}) == 20);
    REQUIRE_FALSE(map.get(Point{5.0, 6.0}).has_value());
}
