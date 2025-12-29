# TitanKV/CMakeLists.txt

cmake_minimum_required(VERSION 3.10)
project(TitanKV)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -O2 -g")

# --- 关键修改：配置 crc32c 选项 ---
# 关闭 crc32c 自带的测试、基准测试和安装，只编译库本身
set(CRC32C_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(CRC32C_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(CRC32C_INSTALL OFF CACHE BOOL "" FORCE)
set(CRC32C_USE_GLOG OFF CACHE BOOL "" FORCE)

# 引入第三方库
add_subdirectory(third_party/crc32c)
add_subdirectory(third_party/liburing)

# GTest 配置 (保持不变)
enable_testing()
find_package(GTest REQUIRED)

add_subdirectory(core)