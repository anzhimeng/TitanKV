cmake_minimum_required(VERSION 3.10)
project(liburing C)

# 开启 GNU 特性 (io_uring 依赖)
add_definitions(-D_GNU_SOURCE)

# 指定源文件
set(LIBURING_SOURCES
    src/queue.c
    src/register.c
    src/setup.c
    src/syscall.c
    src/version.c
)

add_library(uring STATIC ${LIBURING_SOURCES})

# 【关键修复】使用 ${CMAKE_CURRENT_SOURCE_DIR} 指定绝对路径
# 这样无论在哪个目录下编译，都能准确定位到 src/include
target_include_directories(uring PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/src/include
)