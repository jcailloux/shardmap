# shardmap

A thread-safe, header-only in-memory cache for C++23 with sharded storage, customizable metadata, and flexible invalidation logic.

## Features

- **Thread-safe** — concurrent reads and writes via per-shard reader-writer locks
- **Customizable metadata** — attach any metadata structure to cached entries (atomic fields for shared-lock mutation)
- **Flexible invalidation** — custom predicates for get, invalidate, and cleanup operations
- **Zero runtime overhead** — all callbacks are template parameters for full inlining
- **Compile-time configuration** — shard count and capacity via structural NTTP aggregate
- **Two-phase cleanup** — evaluate under shared lock, erase under exclusive lock (reads never blocked during cleanup)

## Requirements

- C++23 compiler (GCC 13+, Clang 18+)
- Abseil (Swiss Table `flat_hash_map`, fetched automatically via CMake)

## Quick Start

### Simple cache without metadata

```cpp
#include <jcailloux/shardmap/ShardMap.h>

using namespace jcailloux::shardmap;

// Create a sharded map with default config (8 shards)
ShardMap<std::string, MyData> cache;

// Store value (returns true if key was newly inserted)
cache.put("user:123", userData);

// Retrieve (returns std::optional<MyData>)
if (auto result = cache.get("user:123")) {
    // Use *result
}

// Invalidate (returns true if key existed)
cache.invalidate("user:123");
```

### Cache with custom metadata

Metadata fields that are mutated inside `get()` callbacks must use atomic types, because the callback runs under a **shared lock** (multiple readers can execute concurrently).

```cpp
struct TtlMetadata {
    std::atomic<int64_t> expires_at_ns;

    TtlMetadata() : expires_at_ns(0) {}
    TtlMetadata(int64_t v) : expires_at_ns(v) {}

    // Copy/move constructors required by flat_hash_map
    TtlMetadata(const TtlMetadata& o)
        : expires_at_ns(o.expires_at_ns.load(std::memory_order_relaxed)) {}
    TtlMetadata& operator=(const TtlMetadata& o) {
        expires_at_ns.store(o.expires_at_ns.load(std::memory_order_relaxed),
                            std::memory_order_relaxed);
        return *this;
    }
};

ShardMap<std::string, MyData, TtlMetadata> cache;

// Store with metadata
auto now = Clock::now().time_since_epoch().count();
cache.put("user:123", userData, TtlMetadata{now + ttl_ns});

// Get with expiration check — callback runs under shared lock
auto result = cache.get("user:123", [now](const MyData&, TtlMetadata& meta) {
    if (meta.expires_at_ns.load(std::memory_order_relaxed) < now)
        return GetAction::Invalidate;
    return GetAction::Accept;
});
```

## API Reference

### Template Parameters

```cpp
template<
    typename K,
    typename V,
    typename Metadata = std::monostate,
    ShardMapConfig Config = {}
>
class ShardMap;
```

- `K`: Key type (any hashable type)
- `V`: Value type
- `Metadata`: Custom metadata type (default: `std::monostate` for no metadata)
- `Config`: Compile-time configuration (shard count, initial capacity)

### Configuration

```cpp
struct ShardMapConfig {
    uint8_t shard_count_log2 = 3;   // 2^3 = 8 shards
    size_t initial_capacity = 0;    // 0 = let Abseil decide per shard

    consteval ShardMapConfig with_shard_count_log2(uint8_t v) const;
    consteval ShardMapConfig with_initial_capacity(size_t v) const;
};
```

Configuration is a structural aggregate passed as a Non-Type Template Parameter (NTTP). Use fluent `consteval` methods to customize:

```cpp
// Default: 8 shards, Abseil decides capacity
ShardMap<int, std::string> cache;

// Custom: 16 shards, pre-allocated for 10,000 entries
constexpr auto cfg = ShardMapConfig{}
    .with_shard_count_log2(4)       // 2^4 = 16 shards
    .with_initial_capacity(10'000); // ~625 per shard

ShardMap<int, std::string, std::monostate, cfg> cache;
```

### Constructor

```cpp
ShardMap() = default;  // No arguments — configuration is compile-time via Config NTTP
```

### Core Operations

#### Get

```cpp
// Simple get — shared lock, maximum concurrency
GetResult get(const K& key) const;

// Get with callback — shared lock for callback execution
// Callback: GetAction(const V&, Metadata&)
// Callback: GetAction(const V&, const Metadata&)
// Callback: GetAction(const V&, Metadata&, uint8_t shard_id)
template<typename Callback>
GetResult get(const K& key, Callback&& callback);
```

**GetAction options:**
- `Accept` — return the value, keep entry in cache
- `Reject` — return "not found", keep entry in cache
- `Invalidate` — return "not found", erase entry (deferred under exclusive lock)

**Important:** The callback runs under a **shared lock**. Multiple `get()` calls on different keys (or even the same key) execute concurrently. If the callback mutates metadata fields, those fields **must be atomic types** (e.g., `std::atomic<int64_t>`).

**Usage examples:**

```cpp
// Refresh TTL on access (sliding window)
cache.get(key, [now](const auto&, TtlMetadata& meta) {
    meta.expires_at_ns.store(now + ttl_ns, std::memory_order_relaxed);
    return GetAction::Accept;
});

// Check expiration and invalidate if needed
cache.get(key, [now](const auto&, const TtlMetadata& meta) {
    return meta.expires_at_ns.load(std::memory_order_relaxed) < now
        ? GetAction::Invalidate : GetAction::Accept;
});

// Use shard_id for per-shard statistics
cache.get(key, [&counters](const auto&, auto& meta, uint8_t shard_id) {
    counters[shard_id].fetch_add(1, std::memory_order_relaxed);
    return GetAction::Accept;
});
```

#### Put

```cpp
// With metadata — returns true if key was newly inserted, false if updated
template<typename KK, typename VV>
bool put(KK&& key, VV&& value, Metadata metadata);

// Without metadata (only if Metadata = std::monostate)
template<typename KK, typename VV>
bool put(KK&& key, VV&& value);
```

#### Invalidate

```cpp
// Unconditional — returns true if key existed
bool invalidate(const K& key);

// Conditional — returns true if key existed AND predicate returned true
// Predicate: bool(const V&, const Metadata&)
template<typename Predicate>
bool invalidate(const K& key, Predicate&& predicate);
```

### Cleanup Operations

Cleanup uses a **two-phase approach**: first, it evaluates the removal predicate under a shared lock (reads continue unblocked), then erases matching entries under per-key exclusive locks.

```cpp
// Try cleanup (non-blocking) — processes one shard
// Returns the shard_id that was cleaned, or nullopt if cleanup is already in progress
template<typename Context, typename ShouldRemove>
std::optional<uint8_t> try_cleanup(const Context& ctx, ShouldRemove&& should_remove);

// Cleanup (blocking) — waits for any in-progress cleanup, then processes one shard
template<typename Context, typename ShouldRemove>
CleanupResult cleanup(const Context& ctx, ShouldRemove&& should_remove);
// CleanupResult { size_t removed; uint8_t shard_id; }

// Full cleanup (blocking) — processes all shards
template<typename Context, typename ShouldRemove>
size_t full_cleanup(const Context& ctx, ShouldRemove&& should_remove);
```

**ShouldRemove signatures** (three overloads, auto-detected via `std::is_invocable_v`):

| Signature | Use case |
|-----------|----------|
| `bool(const K&, const Metadata&, const Context&)` | Metadata-only check (most efficient) |
| `bool(const K&, const V&, const Metadata&, const Context&)` | Value-based check |
| `bool(const K&, const V&, const Metadata&, const Context&, uint8_t shard_id)` | Per-shard logic |

**Usage example:**

```cpp
struct CleanupContext {
    int64_t now;
};

// Periodic cleanup — metadata-only predicate (most efficient)
cache.cleanup(
    CleanupContext{now},
    [](const auto& key, const TtlMetadata& meta, const CleanupContext& ctx) {
        return meta.expires_at_ns.load(std::memory_order_relaxed) < ctx.now;
    }
);
```

### Introspection

```cpp
static consteval size_t shard_count();               // Number of shards (compile-time)
[[nodiscard]] size_t size() const;                    // Total entries across all shards
[[nodiscard]] bool empty() const;                     // True if no entries
[[nodiscard]] bool contains(const K& key) const;      // Check if key exists
```

### Return Types

For `get()` operations, the return type depends on the value type `V`:

| Value type | Return type | "Not found" value |
|------------|-------------|-------------------|
| `T` (non-nullable) | `std::optional<T>` | `std::nullopt` |
| `std::shared_ptr<T>` | `std::shared_ptr<T>` | `nullptr` |
| `std::optional<T>` | `std::optional<T>` | `std::nullopt` |
| `T*` | `T*` | `nullptr` |

This avoids redundant wrapping like `std::optional<std::shared_ptr<T>>`.

**Note on move-only types:** `get()` copies the value out of the map. Move-only types like `std::unique_ptr<T>` cannot be retrieved this way. Use `std::shared_ptr<T>` instead, or use the callback-based `get()` to inspect the value in place.

## Usage Patterns

### TTL-based cache with periodic cleanup

```cpp
struct TtlMetadata {
    std::atomic<int64_t> expires_at_ns;

    TtlMetadata() : expires_at_ns(0) {}
    TtlMetadata(int64_t v) : expires_at_ns(v) {}
    TtlMetadata(const TtlMetadata& o)
        : expires_at_ns(o.expires_at_ns.load(std::memory_order_relaxed)) {}
    TtlMetadata& operator=(const TtlMetadata& o) {
        expires_at_ns.store(o.expires_at_ns.load(std::memory_order_relaxed),
                            std::memory_order_relaxed);
        return *this;
    }
};

ShardMap<int, std::string, TtlMetadata> cache;

// Put with expiration
auto now = steady_clock::now().time_since_epoch().count();
cache.put(42, "value", TtlMetadata{now + ttl_ns});

// Get with automatic invalidation of expired entries
auto value = cache.get(42, [now](const auto&, TtlMetadata& meta) {
    auto exp = meta.expires_at_ns.load(std::memory_order_relaxed);
    if (exp < now)
        return GetAction::Invalidate;
    // Optional: refresh TTL on access (sliding window)
    meta.expires_at_ns.store(now + ttl_ns, std::memory_order_relaxed);
    return GetAction::Accept;
});

// Periodic cleanup (e.g., from a timer callback)
struct Ctx { int64_t now; };
cache.cleanup(Ctx{now}, [](const auto&, const TtlMetadata& meta, const Ctx& ctx) {
    return meta.expires_at_ns.load(std::memory_order_relaxed) < ctx.now;
});
```

### Cache-aside pattern

```cpp
std::optional<Entity> fetchEntity(const Key& key) {
    // Try cache first
    if (auto cached = cache.get(key)) {
        return cached;
    }

    // Cache miss — fetch from backend
    auto entity = backend.fetch(key);
    if (entity) {
        cache.put(key, *entity, createMetadata());
    }
    return entity;
}
```

### With shared_ptr (nullable type)

```cpp
ShardMap<int, std::shared_ptr<Entity>> cache;

cache.put(123, std::make_shared<Entity>(...));

// Returns std::shared_ptr<Entity> directly (not wrapped in optional)
std::shared_ptr<Entity> entity = cache.get(123);
if (entity) {
    // Use entity
}
```

## Thread Safety

All public methods are thread-safe. The implementation uses:
- **Per-shard SpinRWLock** — readers run concurrently (shared lock), writers take exclusive lock
- **Cache-line aligned shards** — prevents false sharing between shard locks
- **Shared-lock callbacks** — `get()` callbacks execute under shared lock; metadata mutations must use atomic types
- **Deferred invalidation** — `GetAction::Invalidate` releases shared lock, then takes exclusive lock to erase
- **Atomic cleanup coordination** — prevents concurrent cleanup operations

## Zero-Overhead Design

- **Template callbacks** — all predicates and callbacks are template parameters, fully inlined by the compiler
- **No virtual functions** — zero virtual dispatch overhead
- **No `std::function`** — no type erasure or heap allocation for callbacks
- **`[[no_unique_address]]`** — zero size overhead when `Metadata = std::monostate`
- **NTTP configuration** — shard count and capacity resolved at compile time

## Further Reading

For implementation details, the two-phase cleanup mechanism, and tuning guidance, see [INTERNALS.md](INTERNALS.md).

## License

MIT License — see [LICENSE](LICENSE) file.