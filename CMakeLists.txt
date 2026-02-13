cmake_minimum_required(VERSION 3.20)
project(shardmap LANGUAGES CXX)

add_library(shardmap INTERFACE)
add_library(jcailloux::shardmap ALIAS shardmap)

target_include_directories(shardmap INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_compile_features(shardmap INTERFACE cxx_std_23)

# =============================================================================
# Dependencies
# =============================================================================

include(FetchContent)

# abseil-cpp — Swiss Table hash map (flat_hash_map)
FetchContent_Declare(abseil-cpp
    GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
    GIT_TAG        20240722.0
)

FetchContent_GetProperties(abseil-cpp)
if(NOT abseil-cpp_POPULATED)
    set(ABSL_PROPAGATE_CXX_STD ON)
    set(ABSL_BUILD_TESTING OFF)
    set(ABSL_INSTALL OFF)
    FetchContent_MakeAvailable(abseil-cpp)
endif()

target_link_libraries(shardmap INTERFACE absl::flat_hash_map)

# =============================================================================
# Tests
# =============================================================================

option(SHARDMAP_BUILD_TESTS "Build tests" ON)

if(SHARDMAP_BUILD_TESTS)
    enable_testing()

    if(NOT TARGET Catch2::Catch2WithMain)
        include(FetchContent)
        FetchContent_Declare(Catch2
            GIT_REPOSITORY https://github.com/catchorg/Catch2.git
            GIT_TAG        v3.12.0
        )
        FetchContent_MakeAvailable(Catch2)
    endif()

    # Test executable
    add_executable(shardmap_tests
        tests/test_shardmap_basic.cpp
        tests/test_shardmap_callbacks.cpp
        tests/test_shardmap_concurrent.cpp
        tests/test_shardmap_cleanup.cpp
    )

    target_link_libraries(shardmap_tests
        PRIVATE
            jcailloux::shardmap
            Catch2::Catch2WithMain
    )

    # Discover tests for CTest
    list(APPEND CMAKE_MODULE_PATH ${catch2_SOURCE_DIR}/extras)
    include(CTest)
    include(Catch)
    catch_discover_tests(shardmap_tests)

    # Benchmark executable (separate binary, not in CTest)
    add_executable(shardmap_benchmarks
        tests/test_shardmap_benchmark.cpp
    )

    target_link_libraries(shardmap_benchmarks
        PRIVATE
            jcailloux::shardmap
            Catch2::Catch2WithMain
    )

    target_compile_options(shardmap_benchmarks PRIVATE -O2 -DNDEBUG)
endif()