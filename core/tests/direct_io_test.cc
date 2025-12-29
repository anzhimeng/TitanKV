#include "gtest/gtest.h"
#include "blob/blob_store.h"
#include "util/io_uring_executor.h"
#include <filesystem>

using namespace titankv;

TEST(DirectIOTest, AlignmentCheck) {
    std::string dbname = "/tmp/titankv_direct_test";
    std::filesystem::remove_all(dbname);
    
    Options options;
    options.use_direct_io = true; // 开启 Direct IO

    IoUringExecutor executor;
    BlobStore store(dbname, options, &executor);

    // 1. 写入 (Append 不受 Direct IO 读的影响，通常还是 Buffered IO 写)
    // 写入一个变长数据，使得 Offset 肯定不是 4096 对齐的
    BlobIndex idx1, idx2;
    store.Add("k1", "v1", &idx1); // offset 0
    store.Add("k2", "v2", &idx2); // offset 0 + 12 + 2 + 2 = 16 (未对齐)

    // 2. 读取未对齐的数据
    // 如果对齐逻辑写错了，这里会报 IO Error
    std::string val;
    Status s = store.Get(idx2, &val);
    
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(val, "v2");

    std::filesystem::remove_all(dbname);
}