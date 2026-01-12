# 1. 定义源文件列表
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
    src/lsm/filter_block.cc
    src/lsm/merging_iterator.cc
    src/lsm/two_level_iterator.cc
    src/lsm/mvcc_reader.cc
    
    # 工具组件
    src/util/arena.cc
    src/util/coding.cc
    src/util/crc32c.cc
    src/util/env.cc
    src/util/filename.cc
    src/util/status.cc
    src/util/hash.cc
    src/util/bloom.cc
    src/util/write_batch.cc
    
    # 日志组件
    src/wal/log_reader.cc
    src/wal/log_writer.cc

    # 对外接口
    src/api/titan_c.cc
    
    # 异步 IO
    src/util/io_uring_executor.cc
    src/util/cache.cc
)

# 2. 生成静态库
add_library(titankv STATIC ${CORE_SOURCES})

# 3. 设置头文件路径
# PUBLIC: 外部使用者可以看到 include 目录
target_include_directories(titankv PUBLIC include)
# PRIVATE: 库内部可以看到 src 目录
target_include_directories(titankv PRIVATE src)

# 链接依赖库
# 注意：liburing (uring) 和 crc32c 是私有依赖
target_link_libraries(titankv PRIVATE crc32c uring)

# 4. 自动构建所有测试
# 移除 if(BUILD_TESTING) 以确保总是编译测试，或者确保你在根目录开启了它
file(GLOB TEST_SOURCES "tests/*.cc")

foreach(test_source ${TEST_SOURCES})
    # 获取文件名作为目标名 (例如 db_test.cc -> db_test)
    get_filename_component(test_name ${test_source} NAME_WE)
    
    # 创建可执行文件
    add_executable(${test_name} ${test_source})
    
    # 【关键修复】让测试代码能引用 src 目录下的内部头文件
    target_include_directories(${test_name} PRIVATE src include)
    
    # 【关键修复】链接所有必要的库
    # titankv: 核心库
    # GTest: 测试框架
    # pthread: 线程支持
    # uring: io_uring 支持 (io_uring_test 需要)
    # crc32c: 校验支持
    target_link_libraries(${test_name} titankv GTest::GTest GTest::Main pthread uring crc32c)
    
    # 注册到 CTest
    add_test(NAME ${test_name} COMMAND ${test_name})
endforeach()

# 【注意】删除了底部手动添加的 io_uring_test，因为上面的循环已经自动处理了它