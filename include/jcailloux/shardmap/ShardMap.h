#ifndef JCX_SHARDMAP_SHARD_MAP_H
#define JCX_SHARDMAP_SHARD_MAP_H

#include <absl/container/flat_hash_map.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <shared_mutex>
#include <type_traits>
#include <utility>
#include <vector>

// Platform-specific pause instruction for spin loops
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
    #include <emmintrin.h>
    #define JCX_SPIN_PAUSE() _mm_pause()
#elif defined(__aarch64__) || defined(_M_ARM64)
    #define JCX_SPIN_PAUSE() asm volatile("yield" ::: "memory")
#else
    #define JCX_SPIN_PAUSE() ((void)0)
#endif

namespace jcailloux::shardmap {

// =============================================================================
// Type traits
// =============================================================================

template<typename T>
struct is_nullable : std::false_type {};

template<typename T>
struct is_nullable<std::optional<T>> : std::true_type {};

template<typename T>
struct is_nullable<std::shared_ptr<T>> : std::true_type {};

template<typename T>
struct is_nullable<std::unique_ptr<T>> : std::true_type {};

template<typename T>
struct is_nullable<T*> : std::true_type {};

template<typename T>
concept Nullable = is_nullable<std::remove_cv_t<T>>::value;

// =============================================================================
// Enums and result types
// =============================================================================

enum class GetAction : uint8_t {
    Accept,      // Return the value, keep entry
    Reject,      // Return "not found", keep entry
    Invalidate   // Return "not found", erase entry (deferred under exclusive lock)
};

struct CleanupResult {
    size_t removed;
    uint8_t shard_id;
};

// =============================================================================
// Configuration
// =============================================================================

struct ShardMapConfig {
    uint8_t shard_count_log2 = 3;   // 2^3 = 8 shards
    size_t initial_capacity = 0;    // 0 = let Abseil decide per shard

    consteval ShardMapConfig with_shard_count_log2(uint8_t v) const {
        auto c = *this; c.shard_count_log2 = v; return c;
    }
    consteval ShardMapConfig with_initial_capacity(size_t v) const {
        auto c = *this; c.initial_capacity = v; return c;
    }

    constexpr auto operator<=>(const ShardMapConfig&) const = default;
};

inline constexpr ShardMapConfig DefaultShardMapConfig{};

// =============================================================================
// SpinRWLock — lightweight reader-writer lock
// =============================================================================
//
// Single atomic<uint32_t>: bit 31 = writer flag, bits 0-30 = reader count.
// Fast path for readers: single fetch_add (~5ns uncontended).
// Writer-preference: once a writer sets the flag, new readers back off.

class SpinRWLock {
    std::atomic<uint32_t> state_{0};

    static constexpr uint32_t kWriterBit  = 1U << 31;
    static constexpr uint32_t kReaderMask = kWriterBit - 1;

public:
    void lock_shared() noexcept {
        // Optimistic: increment reader count first (single atomic op)
        uint32_t prev = state_.fetch_add(1, std::memory_order_acquire);
        if (__builtin_expect((prev & kWriterBit) == 0, 1))
            return;  // Fast path: no writer, done

        // Slow path: writer present, undo and spin
        state_.fetch_sub(1, std::memory_order_release);
        for (;;) {
            while (state_.load(std::memory_order_relaxed) & kWriterBit)
                JCX_SPIN_PAUSE();
            prev = state_.fetch_add(1, std::memory_order_acquire);
            if ((prev & kWriterBit) == 0)
                return;
            state_.fetch_sub(1, std::memory_order_release);
        }
    }

    void unlock_shared() noexcept {
        state_.fetch_sub(1, std::memory_order_release);
    }

    void lock() noexcept {
        // Phase 1: atomically set writer bit (immune to reader count changes)
        // fetch_or always succeeds — unlike CAS, it can't livelock from readers
        while (state_.fetch_or(kWriterBit, std::memory_order_acquire) & kWriterBit) {
            // Another writer has the bit; wait for release then retry
            while (state_.load(std::memory_order_relaxed) & kWriterBit)
                JCX_SPIN_PAUSE();
        }
        // Phase 2: wait for existing readers to drain
        while ((state_.load(std::memory_order_acquire) & kReaderMask) != 0) {
            JCX_SPIN_PAUSE();
        }
    }

    void unlock() noexcept {
        // Clear only the writer bit — preserves transient reader counts
        // from readers between fetch_add and fetch_sub in lock_shared()
        state_.fetch_and(~kWriterBit, std::memory_order_release);
    }
};

// =============================================================================
// ShardMap
// =============================================================================

template<
    typename K,
    typename V,
    typename Metadata = std::monostate,
    ShardMapConfig Config = DefaultShardMapConfig
>
class ShardMap {
public:
    using GetResult = std::conditional_t<Nullable<V>, V, std::optional<V>>;

    static consteval size_t shard_count() { return kShardCount; }

private:
    static constexpr size_t kShardCount = size_t{1} << Config.shard_count_log2;
    static constexpr size_t kShardMask  = kShardCount - 1;

    struct Entry {
        V value;
        [[no_unique_address]] Metadata metadata;

        Entry() = default;
        Entry(const Entry&) = default;
        Entry& operator=(const Entry&) = default;
        Entry(Entry&&) = default;
        Entry& operator=(Entry&&) = default;

        template<typename VV, typename MM>
        Entry(VV&& v, MM&& m)
            : value(std::forward<VV>(v)), metadata(std::forward<MM>(m)) {}
    };

    // Cache-line aligned shard to prevent false sharing
    static constexpr size_t kCacheLineSize = 64;

    struct alignas(kCacheLineSize) Shard {
        mutable SpinRWLock mutex;
        absl::flat_hash_map<K, Entry> map;

        Shard() {
            if constexpr (Config.initial_capacity > 0) {
                map.reserve(Config.initial_capacity / kShardCount);
            }
        }
    };

    std::array<Shard, kShardCount> shards_;
    std::atomic<uint8_t> cleanup_cursor_{0};
    std::atomic<bool> cleanup_in_progress_{false};

    // =========================================================================
    // Shard selection
    // =========================================================================

    static uint8_t shard_id_for(const K& key) {
        if constexpr (std::is_integral_v<K>) {
            // Fibonacci hash: multiply + shift, ~1ns for integers
            return static_cast<uint8_t>(
                (static_cast<uint64_t>(key) * 11400714819323198485ULL)
                >> (64 - Config.shard_count_log2)
            );
        } else {
            return static_cast<uint8_t>(
                absl::Hash<K>{}(key) >> (64 - Config.shard_count_log2)
            );
        }
    }

    Shard& shard_for(const K& key) {
        return shards_[shard_id_for(key)];
    }

    const Shard& shard_for(const K& key) const {
        return shards_[shard_id_for(key)];
    }

public:
    ShardMap() = default;
    ~ShardMap() = default;

    ShardMap(const ShardMap&) = delete;
    ShardMap& operator=(const ShardMap&) = delete;
    ShardMap(ShardMap&&) = delete;
    ShardMap& operator=(ShardMap&&) = delete;

    // =========================================================================
    // Get operations
    // =========================================================================

    /// Simple get: shared lock, maximum concurrency.
    [[nodiscard]] GetResult get(const K& key) const {
        const auto& shard = shard_for(key);
        __builtin_prefetch(&shard, 0, 3);
        std::shared_lock lock(shard.mutex);

        auto it = shard.map.find(key);
        if (it == shard.map.end()) {
            return GetResult{};
        }
        return it->second.value;
    }

    /// Get with callback: SHARED LOCK for callback execution.
    /// Callback receives (const V&, Metadata&) or (const V&, const Metadata&).
    /// Optionally receives shard_id: (const V&, Metadata&, uint8_t shard_id).
    /// If metadata fields are mutated, the user must use atomic types.
    ///
    /// If callback returns Invalidate: shared unlock → exclusive lock → erase(key).
    /// The invalidation is deferred but the get returns "not found" immediately.
    ///
    /// Note: benign TOCTOU — between shared unlock and exclusive lock, a concurrent
    /// put() may have refreshed the entry. The erase will remove the fresh value,
    /// causing one unnecessary cache miss. This is self-healing (next access re-caches).
    template<typename Callback>
    GetResult get(const K& key, Callback&& callback) {
        const uint8_t sid = shard_id_for(key);
        auto& shard = shards_[sid];
        __builtin_prefetch(&shard, 0, 3);
        GetResult result{};
        bool needs_invalidation = false;

        {
            std::shared_lock lock(shard.mutex);
            auto it = shard.map.find(key);
            if (it == shard.map.end()) {
                return result;
            }

            GetAction action;
            if constexpr (std::is_invocable_v<Callback, const V&, Metadata&, uint8_t>) {
                action = callback(std::as_const(it->second.value), it->second.metadata, sid);
            } else {
                action = callback(std::as_const(it->second.value), it->second.metadata);
            }

            switch (action) {
                case GetAction::Accept:
                    result = it->second.value;
                    break;
                case GetAction::Reject:
                    break;
                case GetAction::Invalidate:
                    needs_invalidation = true;
                    break;
            }
        } // shared lock released

        if (needs_invalidation) {
            std::unique_lock lock(shard.mutex);
            shard.map.erase(key);
        }

        return result;
    }

    // =========================================================================
    // Put operations
    // =========================================================================

    /// Simple put with metadata. Returns true if new key was inserted.
    template<typename KK, typename VV>
    bool put(KK&& key, VV&& value, Metadata metadata) {
        auto& shard = shard_for(key);
        __builtin_prefetch(&shard, 1, 3);
        std::unique_lock lock(shard.mutex);

        auto [it, inserted] = shard.map.insert_or_assign(
            std::forward<KK>(key),
            Entry{std::forward<VV>(value), std::move(metadata)}
        );
        return inserted;
    }

    /// Put without metadata (only when Metadata = std::monostate).
    template<typename KK, typename VV>
        requires std::same_as<Metadata, std::monostate>
    bool put(KK&& key, VV&& value) {
        return put(std::forward<KK>(key), std::forward<VV>(value), std::monostate{});
    }

    // =========================================================================
    // Invalidation
    // =========================================================================

    /// Unconditional invalidate. Returns true if key existed.
    bool invalidate(const K& key) {
        auto& shard = shard_for(key);
        __builtin_prefetch(&shard, 1, 3);
        std::unique_lock lock(shard.mutex);
        return shard.map.erase(key) > 0;
    }

    /// Conditional invalidate with predicate.
    /// Predicate signature: bool(const V&, const Metadata&)
    /// Returns true if key existed AND predicate returned true (entry erased).
    template<typename Predicate>
    bool invalidate(const K& key, Predicate&& predicate) {
        auto& shard = shard_for(key);
        __builtin_prefetch(&shard, 1, 3);
        std::unique_lock lock(shard.mutex);

        auto it = shard.map.find(key);
        if (it == shard.map.end()) {
            return false;
        }

        if (predicate(std::as_const(it->second.value), std::as_const(it->second.metadata))) {
            shard.map.erase(it);
            return true;
        }
        return false;
    }

    // =========================================================================
    // Cleanup (two-phase: evaluate under shared lock, erase per-key)
    // =========================================================================

    /// Try cleanup of one shard (non-blocking).
    /// Returns the shard_id that was cleaned, or nullopt if cleanup is already in progress.
    ///
    /// ShouldRemove signatures:
    ///   bool(const K&, const Metadata&, const Context&)                         — metadata-only
    ///   bool(const K&, const V&, const Metadata&, const Context&)               — with value
    ///   bool(const K&, const V&, const Metadata&, const Context&, uint8_t sid)  — with shard id
    template<typename Context, typename ShouldRemove>
    std::optional<uint8_t> try_cleanup(const Context& ctx, ShouldRemove&& should_remove) {
        if (bool expected = false; !cleanup_in_progress_.compare_exchange_strong(
                expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
            return std::nullopt;
        }

        auto result = partial_cleanup_impl(ctx, should_remove);
        cleanup_in_progress_.store(false, std::memory_order_release);
        return result.shard_id;
    }

    /// Cleanup one shard (blocking, waits if cleanup in progress).
    template<typename Context, typename ShouldRemove>
    CleanupResult cleanup(const Context& ctx, ShouldRemove&& should_remove) {
        bool expected = false;
        while (!cleanup_in_progress_.compare_exchange_weak(
                expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
            expected = false;
            JCX_SPIN_PAUSE();
        }

        auto result = partial_cleanup_impl(ctx, should_remove);
        cleanup_in_progress_.store(false, std::memory_order_release);
        return result;
    }

    /// Full cleanup (blocking, processes all shards).
    template<typename Context, typename ShouldRemove>
    size_t full_cleanup(const Context& ctx, ShouldRemove&& should_remove) {
        bool expected = false;
        while (!cleanup_in_progress_.compare_exchange_weak(
                expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
            expected = false;
            JCX_SPIN_PAUSE();
        }

        size_t total_removed = 0;
        for (size_t i = 0; i < kShardCount; ++i) {
            total_removed += partial_cleanup_impl(ctx, should_remove).removed;
        }

        cleanup_in_progress_.store(false, std::memory_order_release);
        return total_removed;
    }

    // =========================================================================
    // Introspection
    // =========================================================================

    [[nodiscard]] size_t size() const {
        size_t total = 0;
        for (const auto& shard : shards_) {
            std::shared_lock lock(shard.mutex);
            total += shard.map.size();
        }
        return total;
    }

    [[nodiscard]] bool empty() const {
        for (const auto& shard : shards_) {
            std::shared_lock lock(shard.mutex);
            if (!shard.map.empty()) return false;
        }
        return true;
    }

    [[nodiscard]] bool contains(const K& key) const {
        const auto& shard = shard_for(key);
        std::shared_lock lock(shard.mutex);
        return shard.map.contains(key);
    }

private:
    // =========================================================================
    // Two-phase cleanup implementation
    // =========================================================================

    template<typename Context, typename ShouldRemove>
    CleanupResult partial_cleanup_impl(const Context& ctx, ShouldRemove& should_remove) {
        const uint8_t sid = cleanup_cursor_.fetch_add(1, std::memory_order_relaxed)
                            & static_cast<uint8_t>(kShardMask);
        auto& shard = shards_[sid];

        // Phase 1: Evaluate predicate under shared lock, collect only keys to remove.
        // All predicate signatures are evaluated here (reads are not blocked).
        std::vector<K> keys_to_remove;

        {
            std::shared_lock lock(shard.mutex);
            keys_to_remove.reserve(shard.map.size() / 4);  // Heuristic: ~25% removal

            for (const auto& [key, entry] : shard.map) {
                bool should_erase = false;

                if constexpr (std::is_invocable_v<ShouldRemove, const K&, const V&, const Metadata&, const Context&, uint8_t>) {
                    should_erase = should_remove(key, entry.value, entry.metadata, ctx, sid);
                } else if constexpr (std::is_invocable_v<ShouldRemove, const K&, const Metadata&, const Context&>) {
                    should_erase = should_remove(key, entry.metadata, ctx);
                } else {
                    should_erase = should_remove(key, entry.value, entry.metadata, ctx);
                }

                if (should_erase) {
                    keys_to_remove.push_back(key);
                }
            }
        } // shared lock released

        // Phase 2: Erase collected keys under per-key exclusive lock.
        // Benign TOCTOU: a concurrent put() between phases may have refreshed
        // an entry. The erase causes one unnecessary cache miss (self-healing).
        size_t removed = 0;
        for (const K& key : keys_to_remove) {
            std::unique_lock lock(shard.mutex);
            if (shard.map.erase(key)) {
                ++removed;
            }
        }

        return {removed, sid};
    }
};

} // namespace jcailloux::shardmap

#endif // JCX_SHARDMAP_SHARD_MAP_H
