#include <catch2/catch_test_macros.hpp>
#include <jcailloux/segcache/Cache.h>
#include <string>

#include "CacheAccessor.h"
#include "KeyTrackerAccessor.h"

using namespace jcailloux::segcache;
using namespace std::chrono_literals;

struct Metadata { int value; };
struct CleanupContext { int value; };

TEST_CASE("Cache basic operations", "[cache][basic]") {
    Cache<int, std::string> cache(4);

    auto& key_tracker = test::CacheAccessor<int, std::string, std::monostate>::getKeyTracker(cache);
    auto& segments = test::KeyTrackerAccessor<int>::getSegments(key_tracker);
    REQUIRE(segments.size() == 4);

    SECTION("Empty cache") {
        REQUIRE(cache.size() == 0);
        REQUIRE_FALSE(cache.get(1));
    }

    SECTION("Insertions increment size") {
        Cache<int, std::string> cache(4);
        cache.put(1, "value1");
        REQUIRE(cache.size() == 1);
        cache.put(2, "value2");
        REQUIRE(cache.size() == 2);
        cache.put(3, "value3");
        REQUIRE(cache.size() == 3);
        cache.put(4, "value4");
        REQUIRE(cache.size() == 4);
    }

    cache.put(1, "value1");
    cache.put(2, "value2");
    cache.put(3, "value3");
    cache.put(4, "value4");
    cache.put(5, "value5");

    SECTION("Contains returns true (false) for inserted (absent) values") {
        REQUIRE(cache.contains(5));
        REQUIRE_FALSE(cache.contains(6));
    }

    SECTION("Retrieve a value") {
        auto result = cache.get(5);
        REQUIRE(result);
        REQUIRE(*result == "value5");
        result = cache.get(1);
        REQUIRE(result);
        REQUIRE(*result == "value1");
        result = cache.get(3);
        REQUIRE(result);
        REQUIRE(*result == "value3");
        result = cache.get(2);
        REQUIRE(result);
        REQUIRE(*result == "value2");
        result = cache.get(4);
        REQUIRE(result);
        REQUIRE(*result == "value4");
    }

    SECTION("Get returns nullopt for non-existent key") {
        auto result = cache.get(999);
        REQUIRE_FALSE(result);
    }

    const size_t initial_size = cache.size();

    SECTION("Invalidation removes existing entries") {
        bool removed = cache.invalidate(1);
        REQUIRE(removed);
        REQUIRE(cache.size() == initial_size - 1);
        auto result = cache.get(1);
        REQUIRE_FALSE(result);

        removed = cache.invalidate(2);
        REQUIRE(removed);
        REQUIRE(cache.size() == initial_size - 2);
        result = cache.get(2);
        REQUIRE_FALSE(result);
    }

    SECTION("Invalidate returns false for non-existent key") {
        bool removed = cache.invalidate(999);
        REQUIRE_FALSE(removed);
    }

    SECTION("Update existing key") {
        cache.put(1, "old");
        cache.put(1, "new");

        REQUIRE(cache.size() == initial_size);
        REQUIRE(*cache.get(1) == "new");
        REQUIRE(cache.tracked_keys_count() == 5);
    }

    SECTION("Nullable types aren't wrapped in an optional") {
        Cache<int, std::shared_ptr<std::string>> ptr_cache(4);
        std::shared_ptr<std::string> ptr = std::make_shared<std::string>("hello");
        ptr_cache.put(0, ptr);
        auto result = ptr_cache.get(0);
        REQUIRE(std::is_same_v<decltype(result), std::shared_ptr<std::string>>);
    }
}

TEST_CASE("Cache with metadata", "[cache][metadata]") {
    Cache<int, std::string, Metadata> cache(4);
    cache.put(1, "value1", {1});
    cache.put(2, "value2", {1});
    cache.put(3, "value3", {1});
    cache.put(4, "value4", {1});
    cache.put(5, "value5", {1});

    SECTION("Put updates both value and metadata") {
        cache.put(1, "new_value", {2});
        auto result = cache.get(1);
        REQUIRE(result);
        REQUIRE(result.value() == "new_value");
        cache.get(1, [](const std::string&, Metadata& meta) {
            REQUIRE(meta.value == 2);
            return GetAction::Accept;
        });
    }

    SECTION("Get with callback can update metadata") {
        auto result = cache.get(1, [](const std::string&, Metadata& meta) {
            meta.value += 5;
            return GetAction::Accept;
        });

        REQUIRE(result);
        REQUIRE(result.value() == "value1");

        result = cache.get(1, [](const std::string&, const Metadata& meta) {
            REQUIRE(meta.value == 6);
            return GetAction::Accept;
        });
        REQUIRE(result);
        REQUIRE(result.value() == "value1");
    }

    SECTION("Get with callback can reject entry") {
        auto result = cache.get(1, [](const std::string&, const Metadata& meta) {
            return meta.value < 5 ? GetAction::Reject : GetAction::Accept;
        });

        REQUIRE_FALSE(result);
        result = cache.get(1);
        REQUIRE(result);
        REQUIRE(*result == "value1");
    }

    SECTION("Get with callback can invalidate entry") {
        auto result = cache.get(1, [](const std::string&, const Metadata& meta) {
            return meta.value < 5 ? GetAction::Invalidate : GetAction::Accept;
        });

        REQUIRE_FALSE(result);
        result = cache.get(1);
    }

    SECTION("Get with callback on non-existent entry") {
        auto result = cache.get(999, [](const std::string&, const Metadata& meta) {
            return meta.value < 5 ? GetAction::Invalidate : GetAction::Accept;
        });

        REQUIRE_FALSE(result);
    }
}

TEST_CASE("Cache conditional invalidate", "[cache][invalidate]") {
    struct Metadata {
        int priority;
    };

    Cache<int, std::string, Metadata> cache(4);
    cache.put(1, "low", Metadata{1});
    cache.put(2, "high", Metadata{10});
    const size_t initial_size = cache.size();

    SECTION("Invalidate with callback removes matching entries") {
        bool removed = cache.invalidate(1, [](const std::string&, const Metadata& meta) {
            return meta.priority < 5;
        });

        REQUIRE(removed);
        REQUIRE(cache.size() == initial_size - 1);
        REQUIRE_FALSE(cache.get(1));
    }

    SECTION("Invalidate with callback keeps non-matching entries") {
        bool removed = cache.invalidate(2, [](const std::string&, const Metadata& meta) {
            return meta.priority < 5;
        });

        REQUIRE_FALSE(removed);
        REQUIRE(cache.size() == initial_size);
        REQUIRE(cache.get(2));
    }

    SECTION("Invalidate with callback on non-existent entry") {
        bool removed = cache.invalidate(999, [](const std::string&, const Metadata& meta) {
            return meta.priority < 5;
        });

        REQUIRE_FALSE(removed);
        REQUIRE(cache.size() == initial_size);
        REQUIRE(cache.get(2));
    }
}

TEST_CASE("Round-robin with rotating cleanup buffer", "[cache][keytracker]") {
    const size_t nb_segments = 3;
    const size_t nb_entries = 1000;
    Cache<int, int, Metadata> cache(nb_segments);
    for (int i = 1; i <= nb_entries; i++) {
        cache.put(i, i, {i});
    }
    auto& key_tracker = test::CacheAccessor<int, int, Metadata>::getKeyTracker(cache);
    auto& segments = test::KeyTrackerAccessor<int>::getSegments(key_tracker);

    SECTION("Balanced key distribution") {
        REQUIRE(key_tracker.total_tracked() == nb_entries);
        REQUIRE(cache.tracked_keys_count() == nb_entries);

        size_t total_in_segments = 0;
        for (const auto& segment_ptr : segments) {
            const size_t nb_keys = segment_ptr->keys.size();
            total_in_segments += nb_keys;
            REQUIRE((nb_entries / nb_segments) - 1 <= nb_keys);
            REQUIRE(nb_keys <= (nb_entries / nb_segments) + 1);
        }

        REQUIRE(total_in_segments == nb_entries);
        REQUIRE(total_in_segments == key_tracker.total_tracked());
    }

    SECTION("Buffer rotation") {
        REQUIRE(test::KeyTrackerAccessor<int>::getCleanupBuffer(key_tracker).empty());

        for (int i = 0; i <= nb_segments; i++) {
            cache.cleanup(CleanupContext{0}, [](const int&, const int&, const Metadata& meta, const CleanupContext& ctx){return false;});
            size_t smallest_segment_size = SIZE_MAX;
            size_t biggest_segment_size = 0;
            for (const auto &segment_ptr : segments) {
                if (segment_ptr->keys.size() < smallest_segment_size) {
                    smallest_segment_size = segment_ptr->keys.size();
                }
                if (segment_ptr->keys.size() > biggest_segment_size) {
                    biggest_segment_size = segment_ptr->keys.size();
                }
            }
            REQUIRE(biggest_segment_size + 1 >= nb_entries / nb_segments);

            if (i < nb_segments) {
                REQUIRE(test::KeyTrackerAccessor<int>::getCleanupBuffer(key_tracker).size() + 1 >= nb_entries / nb_segments);
                REQUIRE(smallest_segment_size == 0);
            } else {
                REQUIRE(test::KeyTrackerAccessor<int>::getCleanupBuffer(key_tracker).empty());
                REQUIRE(smallest_segment_size + 1 >= nb_entries / nb_segments);
            }
        }
    }
}

TEST_CASE("Cache cleanup", "[cache][cleanup]") {
    constexpr size_t nb_segments = 3;
    constexpr size_t nb_entries = 10000;
    Cache<int, int, Metadata> cache(nb_segments);

    SECTION("Cleanup empty cache") {
        size_t total_removed = cache.cleanup(
            CleanupContext{0},
            [](const int&, const int&, const Metadata& meta, const CleanupContext& ctx) {
                return true;
            }
        ).removed;
        REQUIRE(total_removed == 0);
        REQUIRE(cache.size() == 0);
        total_removed = cache.full_cleanup(
            CleanupContext{0},
            [](const int&, const int&, const Metadata& meta, const CleanupContext& ctx) {
                return true;
            }
        );
        REQUIRE(total_removed == 0);
        REQUIRE(cache.size() == 0);
    }

    for (int i = 1; i <= nb_entries; i++) {
        cache.put(i, i, {i});
    }

    SECTION("Cleanup removes invalid entries") {
        size_t total_removed = 0;
        for (int i = 0; i < nb_segments+1; i++) {
            total_removed += cache.cleanup(
                CleanupContext{nb_entries / 4},
                [](const int&, const int&, const Metadata& meta, const CleanupContext& ctx) {
                    return meta.value <= ctx.value;
                }
            ).removed;
        }

        REQUIRE(total_removed == nb_entries / 4);
        REQUIRE(cache.size() == nb_entries - (nb_entries / 4));
        REQUIRE_FALSE(cache.get(nb_entries / 4));
        REQUIRE(cache.get((nb_entries / 4) + 1));
    }

    SECTION("Try cleanup politely skips") {
        int cleanup_started = 0;
        auto partial_cleanup = [&cache, &cleanup_started]() {
            if (cache.try_cleanup(CleanupContext{0}, [](const int&, const int&, const Metadata& meta, const CleanupContext& ctx){return false;})) {
                cleanup_started += 1;
            }
        };
        std::thread t1(partial_cleanup);
        std::thread t2(partial_cleanup);
        std::thread t3(partial_cleanup);

        t1.join();
        t2.join();
        t3.join();

        REQUIRE(cleanup_started == 1);
    }

    SECTION("Cleanup waits for other cleanup processes") {
        size_t total_removed = 0;
        auto partial_cleanup = [&cache, &total_removed]() {
            total_removed += cache.cleanup(CleanupContext{0}, [](const int&, const int&, const Metadata& meta, const CleanupContext& ctx){return true;}).removed;
        };
        std::thread t1(partial_cleanup);
        std::thread t2(partial_cleanup);
        std::thread t3(partial_cleanup);

        t1.join();
        t2.join();
        t3.join();

        REQUIRE(total_removed == nb_entries);
    }

    SECTION("Full cleanup covers it all") {
        // Fake cleanup to rotate the buffer
        cache.cleanup(CleanupContext{0}, [](const int&, const int&, const Metadata& meta, const CleanupContext& ctx){return false;});
        REQUIRE(cache.size() == nb_entries);
        // Fill the segment swapped with the buffer
        for (int i = nb_entries+1; i <= 2*nb_entries; i++) {
            cache.put(i,i,{i});
        }
        size_t total_removed = cache.full_cleanup(CleanupContext{0}, [](const int&, const int&, const Metadata& meta, const CleanupContext& ctx){return true;});
        REQUIRE(total_removed == 2*nb_entries);
        REQUIRE(cache.empty());
    }
}