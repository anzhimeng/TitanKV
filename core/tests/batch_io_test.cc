#include "gtest/gtest.h"
#include "blob/blob_store.h"
#include "util/io_uring_executor.h"
#include <vector>
#include <string>
#include <filesystem>

using namespace titankv;

TEST(BatchIOTest, MultiGetPerformance) {
    // 1. Setup
    std::string dbname = "/tmp/titankv_batch_test";
    std::filesystem::remove_all(dbname);
    
    IoUringExecutor executor(1024); // 大队列
    
    // 【关键修复】创建 Options 并传入
    Options options;
    BlobStore store(dbname, options, &executor);

    const int K = 100;
    std::vector<BlobIndex> indices;
    
    // 2. Write Data
    for (int i = 0; i < K; ++i) {
        BlobIndex idx;
        std::string val(4096, 'a' + (i % 26)); // 4KB
        store.Add("key", val, &idx);
        indices.push_back(idx);
    }

    // 3. MultiGet
    std::vector<std::string> values;
    std::vector<Status> statuses;
    
    store.MultiGet(indices, &values, &statuses);

    // 4. Verify
    for (int i = 0; i < K; ++i) {
        ASSERT_TRUE(statuses[i].ok());
        ASSERT_GT(values[i].size(), 4096); 
    }
    
    std::filesystem::remove_all(dbname);
}