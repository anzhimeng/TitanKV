# 收集源文件
file(GLOB_RECURSE SOURCES "src/*.cc")

# 头文件路径
include_directories(include)

# 生成静态库 libtitankv_core.a
add_library(titankv_core ${SOURCES})

target_include_directories(titankv_core PUBLIC
    # 1. 公共头文件目录 (外部用户使用，如 include/titankv/db.h)
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    
    # 2. 【关键修复】内部源码目录 (内部模块互相引用，如 util/coding.h)
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# 【关键】链接 crc32c 库
# FetchContent_MakeAvailable 后，会暴露一个名为 crc32c 的 target
target_link_libraries(titankv_core PUBLIC crc32c)

# --- 单元测试 ---
# 发现所有测试文件
file(GLOB_RECURSE TEST_SOURCES "tests/*.cc")

foreach(test_source ${TEST_SOURCES})
        get_filename_component(test_name ${test_source} NAME_WE)
        add_executable(${test_name} ${test_source})
	   target_link_libraries(${test_name} titankv_core GTest::GTest GTest::Main pthread)
        add_test(NAME ${test_name} COMMAND ${test_name})
endforeach()
