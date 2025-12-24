cmake_minimum_required(VERSION 3.10)
project(TitanKV)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O2 -g")

# --- 引入 Google CRC32C ---
include(FetchContent)

# --- 本地集成 google/crc32c ---
set(CRC32C_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(CRC32C_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(CRC32C_USE_GLOG OFF CACHE BOOL "" FORCE)

# 这里的路径指向你解压的文件夹
add_subdirectory(third_party/crc32c)
# -------------------------

# 引入 GoogleTest
enable_testing()
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# 包含子项目
add_subdirectory(core)
