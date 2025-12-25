set(CORE_SOURCES
    # 核心实现
    src/db_impl.cc
    
    # 存储组件
    src/blob/blob_file.cc
    src/blob/blob_format.cc
    src/blob/blob_store.cc
    
    # LSM 组件
    src/lsm/block.cc
    src/lsm/block_builder.cc
    src/lsm/dbformat.cc
    src/lsm/memtable.cc
    src/lsm/table.cc
    src/lsm/table_builder.cc
    src/lsm/table_cache.cc
    src/lsm/version_edit.cc
    src/lsm/version_set.cc
    
    # 工具组件
    src/util/arena.cc
    src/util/coding.cc
    src/util/crc32c.cc
    src/util/env.cc
    src/util/filename.cc
    src/util/status.cc
    
    # 日志组件
    src/wal/log_reader.cc
    src/wal/log_writer.cc

    # 对外接口
    src/api/titan_c.cc
)

# 2. 生成静态库
add_library(titankv STATIC ${CORE_SOURCES})

# 3. 设置头文件路径
target_include_directories(titankv PUBLIC include)
target_include_directories(titankv PRIVATE src)

# --- 关键修改：链接 crc32c ---
# 使用 PRIVATE，因为 crc32c 的头文件只在 .cc 实现中用到，对外头文件(titan_db.h)不依赖它
target_link_libraries(titankv PRIVATE crc32c)

# 4. 测试部分 (保持不变，但记得链接 pthread)
if(BUILD_TESTING)
    file(GLOB TEST_SOURCES "tests/*.cc")
    foreach(test_source ${TEST_SOURCES})
        get_filename_component(test_name ${test_source} NAME_WE)
        add_executable(${test_name} ${test_source})
        # 链接你的库 (titankv 已经包含了 crc32c 的符号)
        target_link_libraries(${test_name} titankv GTest::GTest GTest::Main pthread)
        add_test(NAME ${test_name} COMMAND ${test_name})
    endforeach()
endif()

