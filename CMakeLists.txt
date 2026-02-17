cmake_minimum_required(VERSION 3.20)
project(shardmap VERSION 1.0.1 LANGUAGES CXX)

# =============================================================================
# Library target (header-only)
# =============================================================================

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

# Prefer system-installed Abseil (>= 20240116); fall back to FetchContent
find_package(absl 20240116 QUIET)
if(NOT absl_FOUND)
    include(FetchContent)
    FetchContent_Declare(abseil-cpp
        GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
        GIT_TAG        20260107.1
    )
    set(ABSL_PROPAGATE_CXX_STD ON)
    set(ABSL_BUILD_TESTING OFF)
    set(ABSL_INSTALL OFF)
    FetchContent_MakeAvailable(abseil-cpp)
endif()

target_link_libraries(shardmap INTERFACE absl::flat_hash_map)

# =============================================================================
# Install (opt-in; requires system-installed Abseil for find_package support)
# =============================================================================

option(SHARDMAP_INSTALL "Generate install rules" OFF)

if(SHARDMAP_INSTALL)
    include(GNUInstallDirs)
    include(CMakePackageConfigHelpers)

    install(TARGETS shardmap EXPORT shardmapTargets)

    install(DIRECTORY include/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )

    install(EXPORT shardmapTargets
        FILE shardmapTargets.cmake
        NAMESPACE jcailloux::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/shardmap
    )

    configure_package_config_file(
        cmake/shardmapConfig.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/shardmapConfig.cmake
        INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/shardmap
    )

    write_basic_package_version_file(
        ${CMAKE_CURRENT_BINARY_DIR}/shardmapConfigVersion.cmake
        COMPATIBILITY SameMajorVersion
    )

    install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/shardmapConfig.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/shardmapConfigVersion.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/shardmap
    )
endif()

# =============================================================================
# Tests
# =============================================================================

if(PROJECT_IS_TOP_LEVEL)
    option(SHARDMAP_BUILD_TESTS "Build tests and benchmarks" ON)
else()
    option(SHARDMAP_BUILD_TESTS "Build tests and benchmarks" OFF)
endif()

if(SHARDMAP_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
