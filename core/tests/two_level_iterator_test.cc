#include "gtest/gtest.h"
#include "lsm/table_builder.h"
#include "lsm/table.h"
#include "util/env.h"
#include "util/coding.h"
#include <filesystem>

using namespace titankv;

class TwoLevelIterTest : public testing::Test {
protected:
    std::string fname_ = "/tmp/titankv_two_level_test.sst";
    void SetUp() override { std::filesystem::remove(fname_); }
    
    std::string MakeKey(int i) {
        char buf[100];
        snprintf(buf, sizeof(buf), "key%06d", i);
        std::string key(buf);
        PutFixed64(&key, (uint64_t(1) << 8) | 1); 
        return key;
    }
};

TEST_F(TwoLevelIterTest, IterateAll) {
    // 1. 写入大量数据，确保有多个 Block
    Options options;
    options.block_size = 1024;
    std::unique_ptr<WritableFile> wfile;
    ASSERT_TRUE(NewWritableFile(fname_, &wfile).ok());
    TableBuilder builder(options, wfile.get());
    
    const int N = 1000;
    for (int i = 0; i < N; i++) {
        builder.Add(MakeKey(i), "val");
    }
    ASSERT_TRUE(builder.Finish().ok());
    wfile->Close();

    // 2. 读取
    std::unique_ptr<RandomAccessFile> rfile;
    ASSERT_TRUE(NewRandomAccessFile(fname_, &rfile).ok());
    uint64_t fsize = std::filesystem::file_size(fname_);
    Table* table;
    ASSERT_TRUE(Table::Open(options, rfile.release(), 1, fsize, &table).ok());

    // 3. 获取全表迭代器
    Iterator* iter = table->NewIterator(ReadOptions());
    
    // 4. 顺序遍历
    iter->SeekToFirst();
    int count = 0;
    while (iter->Valid()) {
        // 验证 Key 的顺序性
        // std::string expected = MakeKey(count);
        // ASSERT_EQ(iter->key().ToString(), expected);
        count++;
        iter->Next();
    }
    ASSERT_EQ(count, N);

    // 5. Seek 测试
    iter->Seek(MakeKey(500));
    ASSERT_TRUE(iter->Valid());
    // 验证找到了 500 (注意 Internal Key 的比较)
    // Slice k = iter->key();
    // ...

    delete iter;
    delete table;
}