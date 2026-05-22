cmake_minimum_required(VERSION 3.20)

# ----------------------------------------------------------------------
# Project definition
# ----------------------------------------------------------------------
project(knot
    VERSION     0.1.0
    DESCRIPTION "Distributed KV store with Raft + LSM"
    LANGUAGES   CXX
)

# ----------------------------------------------------------------------
# C++ standard
# ----------------------------------------------------------------------
set(CMAKE_CXX_STANDARD          20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS        OFF)   # use -std=c++20, not -std=gnu++20

# ----------------------------------------------------------------------
# Build defaults
# ----------------------------------------------------------------------
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

# Export compile_commands.json for clangd / IDE integration
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Warnings
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# ----------------------------------------------------------------------
# Dependencies (resolved by vcpkg)
# ----------------------------------------------------------------------
find_package(spdlog CONFIG REQUIRED)
find_package(fmt    CONFIG REQUIRED)

# Will add as we use them in later weeks:
# find_package(Boost CONFIG REQUIRED COMPONENTS system)
# find_package(Protobuf CONFIG REQUIRED)
# find_package(GTest CONFIG REQUIRED)

# ----------------------------------------------------------------------
# Targets
# ----------------------------------------------------------------------

# knotd — the server daemon
add_executable(knotd
    src/server/main.cpp
)
target_include_directories(knotd PRIVATE include)
target_link_libraries(knotd PRIVATE
    spdlog::spdlog
    fmt::fmt
)

# ----------------------------------------------------------------------
# Install (optional, defaults are fine for now)
# ----------------------------------------------------------------------
install(TARGETS knotd RUNTIME DESTINATION bin)
