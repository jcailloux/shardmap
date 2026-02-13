#include <catch2/catch_test_macros.hpp>
#include <jcailloux/segcache/Cache.h>
#include <string>
#include <memory>
#include <optional>

using namespace jcailloux::segcache;

struct Metadata {
    int value = 0;
    int access_count = 0;
};

struct CleanupContext {
    int threshold;
};

TEST_CASE("Nullable types return unwrapped values", "[cache][nullable]") {
    SECTION("shared_ptr returns directly") {
        Cache<int, std::shared_ptr<std::string>> cache(4);
        auto ptr = std::make_shared<std::string>("hello");
        cache.put(1, ptr);

        auto result = cache.get(1);
        REQUIRE(std::is_same_v<decltype(result), std::shared_ptr<std::string>>);
        REQUIRE(result != nullptr);
        REQUIRE(*result == "hello");

        // Not found returns nullptr
        auto not_found = cache.get(999);
        REQUIRE(not_found == nullptr);
    }

    SECTION("optional returns directly") {
        Cache<int, std::optional<std::string>> cache(4);
        cache.put(1, std::optional<std::string>("test"));
        cache.put(2, std::nullopt);

        auto result = cache.get(1);
        REQUIRE(std::is_same_v<decltype(result), std::optional<std::string>>);
        REQUIRE(result.has_value());
        REQUIRE(*result == "test");

        // Stored nullopt returns nullopt
        auto stored_null = cache.get(2);
        REQUIRE_FALSE(stored_null.has_value());

        // Not found also returns nullopt
        auto not_found = cache.get(999);
        REQUIRE_FALSE(not_found.has_value());
    }

    SECTION("raw pointer returns directly") {
        Cache<int, std::string*> cache(4);
        std::string value = "raw";
        cache.put(1, &value);

        auto result = cache.get(1);
        REQUIRE(std::is_same_v<decltype(result), std::string*>);
        REQUIRE(result != nullptr);
        REQUIRE(*result == "raw");

        // Not found returns nullptr
        auto not_found = cache.get(999);
        REQUIRE(not_found == nullptr);
    }
}

TEST_CASE("Get callback signature variations", "[cache][get][callback]") {
    SECTION("Callback can modify value and metadata") {
        Cache<int, std::string, Metadata> cache(4);
        cache.put(1, "value", Metadata{10, 0});

        auto result = cache.get(1, [](std::string& val, Metadata& meta) {
            val = "modified";
            meta.value = 20;
            meta.access_count++;
            return GetAction::Accept;
        });

        REQUIRE(result);
        REQUIRE(*result == "modified");

        // Verify modifications persisted
        result = cache.get(1, [](const std::string& val, const Metadata& meta) {
            REQUIRE(val == "modified");
            REQUIRE(meta.value == 20);
            REQUIRE(meta.access_count == 1);
            return GetAction::Accept;
        });
    }

    SECTION("Callback with const value, mutable metadata") {
        Cache<int, std::string, Metadata> cache(4);
        cache.put(2, "value", Metadata{10, 0});

        auto result = cache.get(2, [](const std::string& val, Metadata& meta) {
            REQUIRE(val == "value");
            meta.access_count++;
            return GetAction::Accept;
        });

        REQUIRE(result);
        REQUIRE(*result == "value");

        // Verify metadata change persisted
        result = cache.get(2, [](const std::string&, const Metadata& meta) {
            REQUIRE(meta.access_count == 1);
            return GetAction::Accept;
        });
    }

    SECTION("Callback with mutable value, const metadata") {
        Cache<int, std::string, Metadata> cache(4);
        cache.put(3, "value", Metadata{10, 0});

        auto result = cache.get(3, [](std::string& val, const Metadata& meta) {
            REQUIRE(meta.value == 10);
            val = "updated";
            return GetAction::Accept;
        });

        REQUIRE(result);
        REQUIRE(*result == "updated");

        // Verify value change persisted
        result = cache.get(3);
        REQUIRE(*result == "updated");
    }

    SECTION("Callback with all const parameters") {
        Cache<int, std::string, Metadata> cache(4);
        cache.put(4, "value", Metadata{10, 0});

        auto result = cache.get(4, [](const std::string& val, const Metadata& meta) {
            REQUIRE(val == "value");
            REQUIRE(meta.value == 10);
            return GetAction::Accept;
        });

        REQUIRE(result);
        REQUIRE(*result == "value");
    }
}

TEST_CASE("GetAction sequences", "[cache][get][sequences]") {
    SECTION("Multiple Reject actions preserve entry") {
        Cache<int, std::string, Metadata> cache(4);
        cache.put(10, "value", Metadata{5, 0});
        auto result = cache.get(10, [](const std::string&, const Metadata& meta) {
            return meta.value < 10 ? GetAction::Reject : GetAction::Accept;
        });
        REQUIRE_FALSE(result);
        REQUIRE(cache.contains(10));

        result = cache.get(10, [](const std::string&, const Metadata& meta) {
            return meta.value < 10 ? GetAction::Reject : GetAction::Accept;
        });
        REQUIRE_FALSE(result);
        REQUIRE(cache.contains(10));
    }

    SECTION("Reject then Accept after metadata change") {
        Cache<int, std::string, Metadata> cache(4);
        cache.put(1, "value", Metadata{5, 0});

        auto result = cache.get(1, [](const std::string&, const Metadata& meta) {
            return meta.access_count >= 3 ? GetAction::Accept : GetAction::Reject;
        });
        REQUIRE_FALSE(result);

        // Update metadata
        cache.get(1, [](const std::string&, Metadata& meta) {
            meta.access_count = 3;
            return GetAction::Reject;
        });

        // Now Accept
        result = cache.get(1, [](const std::string&, const Metadata& meta) {
            return meta.access_count >= 3 ? GetAction::Accept : GetAction::Reject;
        });
        REQUIRE(result);
    }

    SECTION("Accept then Invalidate") {
        Cache<int, std::string, Metadata> cache(4);
        cache.put(30, "value", Metadata{5, 0});
        auto result = cache.get(30, [](const std::string&, const Metadata&) {
            return GetAction::Accept;
        });
        REQUIRE(result);
        REQUIRE(cache.contains(30));

        result = cache.get(30, [](const std::string&, const Metadata&) {
            return GetAction::Invalidate;
        });
        REQUIRE_FALSE(result);
        REQUIRE_FALSE(cache.contains(30));
    }

    SECTION("Update metadata then check and invalidate") {
        Cache<int, std::string, Metadata> cache(4);
        cache.put(40, "value", Metadata{5, 0});

        auto result = cache.get(40, [](const std::string&, Metadata& meta) {
            meta.access_count++;
            return meta.access_count > 2 ? GetAction::Invalidate : GetAction::Accept;
        });
        REQUIRE(result);
        REQUIRE(cache.contains(40));

        result = cache.get(40, [](const std::string&, Metadata& meta) {
            meta.access_count++;
            return meta.access_count > 2 ? GetAction::Invalidate : GetAction::Accept;
        });
        REQUIRE(result);
        REQUIRE(cache.contains(40));

        result = cache.get(40, [](const std::string&, Metadata& meta) {
            meta.access_count++;
            return meta.access_count > 2 ? GetAction::Invalidate : GetAction::Accept;
        });
        REQUIRE_FALSE(result);
        REQUIRE_FALSE(cache.contains(40));
    }
}

TEST_CASE("Cleanup edge cases", "[cache][cleanup]") {
    SECTION("All keys match predicate - remove all") {
        Cache<int, int, Metadata> cache(4);
        for (int i = 1; i <= 100; i++) {
            cache.put(i, i, Metadata{i, 0});
        }

        REQUIRE(cache.size() == 100);

        size_t total_removed = cache.full_cleanup(
            CleanupContext{1000},
            [](const int&, const int&, const Metadata& meta, const CleanupContext& ctx) {
                return meta.value < ctx.threshold;  // All match
            }
        );

        REQUIRE(total_removed == 100);
        REQUIRE(cache.size() == 0);
        REQUIRE(cache.empty());
    }

    SECTION("No keys match predicate - keep all") {
        Cache<int, int, Metadata> cache(4);
        for (int i = 1; i <= 100; i++) {
            cache.put(i, i, Metadata{i, 0});
        }

        REQUIRE(cache.size() == 100);

        size_t total_removed = cache.full_cleanup(
            CleanupContext{0},
            [](const int&, const int&, const Metadata& meta, const CleanupContext& ctx) {
                return meta.value < ctx.threshold;  // None match
            }
        );

        REQUIRE(total_removed == 0);
        REQUIRE(cache.size() == 100);
    }

    SECTION("Cleanup after invalidation - tracked but not in cache") {
        Cache<int, int, Metadata> cache(4);
        for (int i = 1; i <= 100; i++) {
            cache.put(i, i, Metadata{i, 0});
        }

        // Invalidate half the entries
        for (int i = 1; i <= 50; i++) {
            cache.invalidate(i);
        }

        REQUIRE(cache.size() == 50);
        REQUIRE(cache.tracked_keys_count() == 100);  // Still tracked

        // Cleanup should handle missing keys gracefully
        size_t total_removed = cache.full_cleanup(
            CleanupContext{1000},
            [](const int&, const int&, const Metadata&, const CleanupContext&) {
                return true;  // Remove all
            }
        );

        REQUIRE(total_removed == 50);  // Only 50 were in cache
        REQUIRE(cache.size() == 0);
    }
}

TEST_CASE("Segment count variations", "[cache][segments]") {
    SECTION("Single segment") {
        Cache<int, std::string> cache(1);
        // Note: minimum segment count is enforced to 2 in Cache constructor
        REQUIRE(cache.num_segments() == 2);

        for (int i = 0; i < 100; i++) {
            cache.put(i, "value");
        }

        REQUIRE(cache.size() == 100);

        // cleanup() processes one segment (which is all of them)
        size_t removed = cache.cleanup(
            std::monostate{},
            [](const int&, const std::string&, const std::monostate&, const std::monostate&) {
                return true;
            }
        ).removed;

        // With rolling buffer, single segment means 2 containers total (segment + buffer)
        // After one cleanup, roughly half should be removed
        REQUIRE(removed > 0);
    }

    SECTION("Large segment count") {
        Cache<int, std::string> cache(16);
        REQUIRE(cache.num_segments() == 16);

        for (int i = 0; i < 1000; i++) {
            cache.put(i, "value");
        }

        REQUIRE(cache.size() == 1000);
        REQUIRE(cache.tracked_keys_count() == 1000);

        // With 16 segments + 1 buffer = 17 containers
        // Each cleanup processes ~1000/17 ≈ 59 keys
        size_t removed = cache.cleanup(
            std::monostate{},
            [](const int&, const std::string&, const std::monostate&, const std::monostate&) {
                return true;
            }
        ).removed;

        REQUIRE(removed > 0);
        REQUIRE(removed < 1000);

        // full_cleanup should remove all
        size_t total_removed = removed + cache.full_cleanup(
            std::monostate{},
            [](const int&, const std::string&, const std::monostate&, const std::monostate&) {
                return true;
            }
        );

        REQUIRE(total_removed == 1000);
        REQUIRE(cache.empty());
    }

    SECTION("Minimum segment count is enforced") {
        Cache<int, std::string> cache(0);
        REQUIRE(cache.num_segments() >= 2);  // Minimum is 2, not 1
    }
}

TEST_CASE("Conditional invalidate sequences", "[cache][invalidate]") {
    Cache<int, std::string, Metadata> cache(4);
    cache.put(1, "value1", Metadata{5, 0});
    cache.put(2, "value2", Metadata{15, 0});

    SECTION("Invalidate with false predicate, then true predicate") {
        // First attempt - predicate returns false
        bool removed = cache.invalidate(1, [](const std::string&, const Metadata& meta) {
            return meta.value > 10;  // false for key 1
        });

        REQUIRE_FALSE(removed);
        REQUIRE(cache.contains(2));

        // Second attempt - predicate returns true
        removed = cache.invalidate(1, [](const std::string&, const Metadata& meta) {
            return meta.value < 10;  // true for key 1
        });

        REQUIRE(removed);
        REQUIRE_FALSE(cache.contains(1));
    }

    SECTION("Multiple conditional invalidations") {
        bool removed = cache.invalidate(1, [](const std::string&, const Metadata& meta) {
            return meta.value < 10;
        });
        REQUIRE(removed);

        removed = cache.invalidate(2, [](const std::string&, const Metadata& meta) {
            return meta.value > 10;
        });
        REQUIRE(removed);

        REQUIRE(cache.empty());
    }
}

TEST_CASE("Cleanup with metadata patterns", "[cache][cleanup][metadata]") {
    struct SessionMetadata {
        int priority;
        int access_count;
        bool expired;
    };

    Cache<int, std::string, SessionMetadata> cache(4);

    // Insert entries with various metadata
    cache.put(1, "low_active", SessionMetadata{1, 10, false});
    cache.put(2, "high_active", SessionMetadata{10, 20, false});
    cache.put(3, "low_expired", SessionMetadata{2, 5, true});
    cache.put(4, "high_expired", SessionMetadata{9, 30, true});

    SECTION("Cleanup removes low priority expired entries") {
        struct Ctx { int min_priority; };

        size_t removed = cache.full_cleanup(
            Ctx{5},
            [](const int&, const std::string&, const SessionMetadata& meta, const Ctx& ctx) {
                return meta.expired && meta.priority < ctx.min_priority;
            }
        );

        REQUIRE(removed == 1);  // Only key 3
        REQUIRE_FALSE(cache.contains(3));
        REQUIRE(cache.contains(1));
        REQUIRE(cache.contains(2));
        REQUIRE(cache.contains(4));
    }

    SECTION("Cleanup removes all expired") {
        size_t removed = cache.full_cleanup(
            std::monostate{},
            [](const int&, const std::string&, const SessionMetadata& meta, const std::monostate&) {
                return meta.expired;
            }
        );

        REQUIRE(removed == 2);  // Keys 3 and 4
        REQUIRE(cache.size() == 2);
        REQUIRE(cache.contains(1));
        REQUIRE(cache.contains(2));
    }

    SECTION("Cleanup based on access count threshold") {
        struct Ctx { int min_access; };

        size_t removed = cache.full_cleanup(
            Ctx{15},
            [](const int&, const std::string&, const SessionMetadata& meta, const Ctx& ctx) {
                return meta.access_count < ctx.min_access;
            }
        );

        REQUIRE(removed == 2);  // Keys 1 and 3
        REQUIRE(cache.contains(2));
        REQUIRE(cache.contains(4));
    }
}

TEST_CASE("Move semantics", "[cache][move]") {
    SECTION("Put with move") {
        Cache<int, std::string> cache(4);
        std::string value = "move_me";
        cache.put(1, std::move(value));

        auto result = cache.get(1);
        REQUIRE(result);
        REQUIRE(*result == "move_me");
    }

    SECTION("Put with move and metadata") {
        Cache<int, std::string, Metadata> cache(4);
        std::string value = "move_with_meta";
        cache.put(1, std::move(value), Metadata{42, 0});

        auto result = cache.get(1, [](const std::string&, const Metadata& meta) {
            REQUIRE(meta.value == 42);
            return GetAction::Accept;
        });
        REQUIRE(result);
        REQUIRE(*result == "move_with_meta");
    }
}

TEST_CASE("Empty introspection", "[cache][empty]") {
    Cache<int, std::string> cache(4);

    SECTION("Empty returns true for new cache") {
        REQUIRE(cache.empty());
        REQUIRE(cache.size() == 0);
    }

    SECTION("Empty returns false after insertion") {
        cache.put(1, "value");
        REQUIRE_FALSE(cache.empty());
        REQUIRE(cache.size() == 1);
    }

    SECTION("Empty returns true after removing all entries") {
        cache.put(1, "value");
        cache.invalidate(1);
        REQUIRE(cache.empty());
        REQUIRE(cache.size() == 0);
    }
}