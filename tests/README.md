# shardmap Tests

This directory contains unit tests for the shardmap library.

## Structure

```
tests/
├── test_shardmap_basic.cpp       # Basic get/put/invalidate, nullable types, config
├── test_shardmap_callbacks.cpp   # Get with callback (Accept/Reject/Invalidate), metadata mutation, shard_id
├── test_shardmap_concurrent.cpp  # Concurrent stress tests (reads, writes, mixed, hotspot)
├── test_shardmap_cleanup.cpp     # Cleanup operations (two-phase, TOCTOU, concurrent cleanup+reads/writes)
├── test_shardmap_benchmark.cpp   # Performance benchmarks (separate binary, not in CTest)
└── README.md                     # This file
```

## Building and Running Tests

### From the shardmap directory

```bash
# Configure the project with tests enabled (default)
cmake -B build -DCMAKE_BUILD_TYPE=Debug

# Build
cmake --build build

# Run all tests via CTest
ctest --test-dir build --output-on-failure

# Or run the test executable directly
./build/shardmap_tests
```

### Disabling tests

```bash
cmake -B build -DSHARDMAP_BUILD_TESTS=OFF
```

### Running benchmarks

Benchmarks are a separate binary with `-O2 -DNDEBUG`, not included in CTest:

```bash
cmake --build build
./build/shardmap_benchmarks
```

## Adding New Tests

1. Create a new `test_shardmap_*.cpp` file in this directory
2. Include Catch2 and the ShardMap header:
   ```cpp
   #include <catch2/catch_test_macros.hpp>
   #include <jcailloux/shardmap/ShardMap.h>
   ```
3. Write your tests using Catch2 macros (`TEST_CASE`, `SECTION`, `REQUIRE`, etc.)
4. Add the file to `CMakeLists.txt` in the `shardmap_tests` sources list

## Test Example

```cpp
TEST_CASE("ShardMap: my new test", "[shardmap][feature]") {
    ShardMap<int64_t, std::string> map;

    SECTION("Description of tested behavior") {
        map.put(1, "value");
        REQUIRE(map.size() == 1);
    }
}
```

## Available Tags

**Main categories:**
- `[shardmap]` — All ShardMap tests (present on every test)
- `[basic]` — Basic CRUD operations, nullable types, configuration
- `[callbacks]` — Get/invalidate with callbacks, metadata mutation, shard_id parameter
- `[cleanup]` — Cleanup mechanism (two-phase, TOCTOU, predicate overloads)
- `[concurrent]` — Concurrency stress tests

**Examples:**
```bash
./shardmap_tests                          # Run all tests
./shardmap_tests [basic]                  # Run only basic tests
./shardmap_tests [concurrent]             # Run all concurrency tests
./shardmap_tests [cleanup]                # Run cleanup tests
./shardmap_tests ~[concurrent]            # Run everything except concurrent tests
./shardmap_tests "[shardmap][callbacks]"  # Run callback tests
```