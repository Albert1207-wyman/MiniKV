cmake_minimum_required(VERSION 3.10)

project(MiniKV CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror=return-type -g"
)

find_package(Threads REQUIRED)

# --------------------------------------------------------------------------
# MiniKV Storage Engine
#
# 网络层：
#   kv_server.cpp
#   kv_router.cpp
#
# Raft：
#   raft.cpp
#
# 不属于 libminikv，避免将网络层和存储引擎耦合。
# --------------------------------------------------------------------------

file(GLOB MINIKV_SOURCES
    "${PROJECT_SOURCE_DIR}/src/*.cpp"
)

list(REMOVE_ITEM
    MINIKV_SOURCES
    "${PROJECT_SOURCE_DIR}/src/kv_server.cpp"
    "${PROJECT_SOURCE_DIR}/src/kv_router.cpp"
    "${PROJECT_SOURCE_DIR}/src/raft.cpp"
)

add_library(minikv STATIC
    ${MINIKV_SOURCES}
)

target_include_directories(
    minikv
    PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(
    minikv
    PUBLIC
    Threads::Threads
)

# --------------------------------------------------------------------------
# Build Information
# --------------------------------------------------------------------------

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Unknown")
endif()

execute_process(
    COMMAND git rev-parse --short HEAD
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    OUTPUT_VARIABLE MINIKV_GIT_HASH
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
)

if(NOT MINIKV_GIT_HASH)
    set(MINIKV_GIT_HASH "unknown")
endif()

# --------------------------------------------------------------------------
# Single-node / Raft KV Server
# --------------------------------------------------------------------------

add_executable(
    kv_server
    src/kv_server.cpp
    src/raft.cpp
    tools/kv_server_main.cpp
)

target_include_directories(
    kv_server
    PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(
    kv_server
    PRIVATE
    minikv
    Threads::Threads
)

# --------------------------------------------------------------------------
# KV Client
# --------------------------------------------------------------------------

add_executable(
    kv_client
    tools/kv_client.cpp
)

target_include_directories(
    kv_client
    PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(
    kv_client
    PRIVATE
    Threads::Threads
)

# --------------------------------------------------------------------------
# KV Benchmark
# --------------------------------------------------------------------------

add_executable(
    kv_bench
    tools/kv_bench.cpp
)

target_include_directories(
    kv_bench
    PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(
    kv_bench
    PRIVATE
    Threads::Threads
)

# --------------------------------------------------------------------------
# Shard Router
# --------------------------------------------------------------------------

add_executable(
    kv_router
    src/kv_router.cpp
    tools/kv_router_main.cpp
)

target_include_directories(
    kv_router
    PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(
    kv_router
    PRIVATE
    Threads::Threads
)

# --------------------------------------------------------------------------
# Standalone Raft Core Test
# --------------------------------------------------------------------------

add_executable(
    raft_core
    src/raft.cpp
    tools/raft_main.cpp
)

target_include_directories(
    raft_core
    PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(
    raft_core
    PRIVATE
    Threads::Threads
)