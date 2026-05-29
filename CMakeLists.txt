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
find_package(GTest  CONFIG REQUIRED)

find_package(Boost CONFIG REQUIRED COMPONENTS system)
find_package(Protobuf CONFIG REQUIRED)

# ----------------------------------------------------------------------
# Targets
# ----------------------------------------------------------------------

# knot_storage — storage engine static library (WAL, MemTable, SSTable, ...)
add_library(knot_storage STATIC
    src/common/crc32.cpp
    src/storage/wal.cpp
    src/storage/memtable.cpp
    src/storage/sstable.cpp
    src/storage/engine.cpp
)
target_include_directories(knot_storage PUBLIC include)
target_compile_features(knot_storage PUBLIC cxx_std_20)

# knot_raft — Raft consensus static library (log, transport, node)
add_library(knot_raft STATIC
    src/raft/log.cpp
    src/raft/transport.cpp
    src/raft/node.cpp
    src/raft/persister.cpp
    src/raft/wire.cpp
    src/raft/tcp_transport.cpp
    src/raft/command.cpp
)
target_include_directories(knot_raft PUBLIC include)
target_compile_features(knot_raft PUBLIC cxx_std_20)

# knot_proto — generated protobuf C++ for inter-node RPCs (Day 14)
set(PROTO_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto_gen)
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})
protobuf_generate(
    LANGUAGE cpp
    OUT_VAR  PROTO_SRCS
    PROTOS   ${CMAKE_CURRENT_SOURCE_DIR}/proto/knot.proto
    IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/proto
    PROTOC_OUT_DIR ${PROTO_GEN_DIR}
)

add_library(knot_proto STATIC ${PROTO_SRCS})
target_include_directories(knot_proto PUBLIC ${PROTO_GEN_DIR})
target_link_libraries(knot_proto PUBLIC protobuf::libprotobuf)
target_compile_features(knot_proto PUBLIC cxx_std_20)

target_link_libraries(knot_raft PUBLIC knot_proto Boost::system)

# knot_server — client-facing RPC server (Day 16)
add_library(knot_server STATIC
    src/server/client_server.cpp
)
target_include_directories(knot_server PUBLIC include)
target_link_libraries(knot_server PUBLIC knot_raft knot_storage Boost::system)
target_compile_features(knot_server PUBLIC cxx_std_20)

# knotd — the server daemon
add_executable(knotd
    src/server/main.cpp
)
target_include_directories(knotd PRIVATE include)
target_link_libraries(knotd PRIVATE
    knot_storage
    knot_raft
    knot_server
    spdlog::spdlog
    fmt::fmt
)

add_executable(knotctl src/cli/knotctl.cpp)
target_link_libraries(knotctl PRIVATE
    knot_proto
    Boost::system
)
target_include_directories(knotctl PRIVATE include)
target_compile_features(knotctl PRIVATE cxx_std_20)

# ----------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------
enable_testing()
include(GoogleTest)

add_executable(wal_test tests/unit/wal_test.cpp)
target_link_libraries(wal_test PRIVATE
    knot_storage
    GTest::gtest
    GTest::gtest_main
)
gtest_discover_tests(wal_test)

add_executable(memtable_test tests/unit/memtable_test.cpp)
target_link_libraries(memtable_test PRIVATE
    knot_storage
    GTest::gtest
    GTest::gtest_main
)
gtest_discover_tests(memtable_test)

add_executable(sstable_test tests/unit/sstable_test.cpp)
target_link_libraries(sstable_test PRIVATE
    knot_storage
    GTest::gtest
    GTest::gtest_main
)
gtest_discover_tests(sstable_test)

add_executable(engine_test tests/unit/engine_test.cpp)
target_link_libraries(engine_test PRIVATE
    knot_storage
    GTest::gtest
    GTest::gtest_main
)
gtest_discover_tests(engine_test)

add_executable(raft_log_test tests/unit/raft_log_test.cpp)
target_link_libraries(raft_log_test PRIVATE
    knot_raft
    GTest::gtest
    GTest::gtest_main
)
gtest_discover_tests(raft_log_test)

add_executable(raft_transport_test tests/unit/raft_transport_test.cpp)
target_link_libraries(raft_transport_test PRIVATE
    knot_raft
    GTest::gtest
    GTest::gtest_main
)
gtest_discover_tests(raft_transport_test)

add_executable(raft_node_test tests/unit/raft_node_test.cpp)
target_link_libraries(raft_node_test PRIVATE
    knot_raft
    GTest::gtest
    GTest::gtest_main
)
gtest_discover_tests(raft_node_test)

add_executable(raft_persister_test tests/unit/raft_persister_test.cpp)
target_link_libraries(raft_persister_test PRIVATE
    knot_raft
    GTest::gtest
    GTest::gtest_main
)
gtest_discover_tests(raft_persister_test)

add_executable(raft_wire_test tests/unit/raft_wire_test.cpp)
target_link_libraries(raft_wire_test PRIVATE
    knot_raft
    GTest::gtest
    GTest::gtest_main
)
gtest_discover_tests(raft_wire_test)

add_executable(raft_tcp_test tests/unit/raft_tcp_test.cpp)
target_link_libraries(raft_tcp_test PRIVATE
    knot_raft
    GTest::gtest
    GTest::gtest_main
)
gtest_discover_tests(raft_tcp_test)

add_executable(raft_command_test tests/unit/raft_command_test.cpp)
target_link_libraries(raft_command_test PRIVATE
    knot_raft
    GTest::gtest
    GTest::gtest_main
)
gtest_discover_tests(raft_command_test)

add_executable(raft_client_test tests/unit/raft_client_test.cpp)
target_link_libraries(raft_client_test PRIVATE
    knot_server
    GTest::gtest
    GTest::gtest_main
)
gtest_discover_tests(raft_client_test)

add_executable(raft_e2e_test tests/unit/raft_e2e_test.cpp)
target_link_libraries(raft_e2e_test PRIVATE
    knot_server
    knot_raft
    GTest::gtest
    GTest::gtest_main
)
gtest_discover_tests(raft_e2e_test)

# ----------------------------------------------------------------------
# Install (optional, defaults are fine for now)
# ----------------------------------------------------------------------
install(TARGETS knotd RUNTIME DESTINATION bin)
