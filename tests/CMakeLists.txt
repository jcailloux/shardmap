# =============================================================================
# Catch2
# =============================================================================

if(NOT TARGET Catch2::Catch2WithMain)
    include(FetchContent)
    FetchContent_Declare(Catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2.git
        GIT_TAG        v3.12.0
    )
    FetchContent_MakeAvailable(Catch2)
endif()

# =============================================================================
# Test executable
# =============================================================================

add_executable(shardmap_tests
    test_shardmap_basic.cpp
    test_shardmap_callbacks.cpp
    test_shardmap_concurrent.cpp
    test_shardmap_cleanup.cpp
)

target_link_libraries(shardmap_tests
    PRIVATE
        jcailloux::shardmap
        Catch2::Catch2WithMain
)

list(APPEND CMAKE_MODULE_PATH ${catch2_SOURCE_DIR}/extras)
include(CTest)
include(Catch)
catch_discover_tests(shardmap_tests)

# =============================================================================
# Benchmark executable (not registered in CTest)
# =============================================================================

add_executable(shardmap_benchmarks
    test_shardmap_benchmark.cpp
)

target_link_libraries(shardmap_benchmarks
    PRIVATE
        jcailloux::shardmap
        Catch2::Catch2WithMain
)

target_compile_options(shardmap_benchmarks PRIVATE -O2 -DNDEBUG)
