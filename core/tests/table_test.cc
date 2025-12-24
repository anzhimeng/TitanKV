#include "gtest/gtest.h"
#include "lsm/table_builder.h"
#include "lsm/table.h"
#include "util/env.h"
#include "util/coding.h"
#include <filesystem>
#include <vector>
#include <string>

using namespace titankv;

class TableBuilderTest : public testing::Test {
protected:
    std::string fname_;
    Options options_;

    void SetUp() override {
        fname_ = "/tmp/titankv_table_test.sst";
        std::filesystem::remove(fname_);
        options_.block_size = 1024;
        options_.block_restart_interval = 16;
    }

    void TearDown() override {
        // std::filesystem::remove(fname_);
    }

    // 【关键辅助函数】构造 InternalKey (UserKey + 8字节 Tag)
    std::string MakeKey(int i) {
        char buf[100];
        snprintf(buf, sizeof(buf), "key%06d", i);
        std::string key(buf);
        // 追加 8 字节 Tag (SeqNum=1, Type=Value)
        // 简单起见，直接追加 8 个 \x00 或者固定值，只要长度对就行
        // 真正的 InternalKeyComparator 需要 DecodeFixed64，所以我们Encode一下
        PutFixed64(&key, (uint64_t(1) << 8) | 1); 
        return key;
    }
};

TEST_F(TableBuilderTest, SimpleBuild) {
    std::unique_ptr<WritableFile> file;
    ASSERT_TRUE(NewWritableFile(fname_, &file).ok());

    TableBuilder builder(options_, file.get());

    // 使用长 Value 确保触发 Block 切分
    std::string long_val(100, 'v');

    for (int i = 0; i < 100; i++) {
        builder.Add(MakeKey(i), long_val);
    }

    ASSERT_TRUE(builder.Finish().ok());
    
    uint64_t file_size = builder.FileSize();
    ASSERT_GT(file_size, 0);
    file->Close();

    uintmax_t actual_size = std::filesystem::file_size(fname_);
    ASSERT_EQ(file_size, actual_size);
    
    // 预期：100 * (Key~20B + Val100B) = 12KB 左右
    // 考虑到 Block Overhead，应该大于 10000
    ASSERT_GT(actual_size, 10000);
}

TEST_F(TableBuilderTest, EmptyBuild) {
    std::unique_ptr<WritableFile> file;
    ASSERT_TRUE(NewWritableFile(fname_, &file).ok());

    TableBuilder builder(options_, file.get());
    ASSERT_TRUE(builder.Finish().ok());
    
    file->Close();
    uintmax_t size = std::filesystem::file_size(fname_);
    ASSERT_GT(size, 0);
}

TEST_F(TableBuilderTest, BuildAndRead) {
    // 1. Build
    {
        std::unique_ptr<WritableFile> file;
        ASSERT_TRUE(NewWritableFile(fname_, &file).ok());
        
        TableBuilder builder(options_, file.get());

        // 写入 1000 个 KV
        for (int i = 0; i < 1000; i++) {
            builder.Add(MakeKey(i), "val"); 
        }

        ASSERT_TRUE(builder.Finish().ok());
        file->Close(); 
    }

    // 2. Read
    std::unique_ptr<RandomAccessFile> raf;
    ASSERT_TRUE(NewRandomAccessFile(fname_, &raf).ok());
    
    uint64_t file_size = std::filesystem::file_size(fname_);
    Table* table;
    ASSERT_TRUE(Table::Open(options_, raf.get(), file_size, &table).ok());

    struct Context {
        bool found;
        std::string value;
    } ctx;
    
    // Callback 接收到的 k 也是 InternalKey，但我们这里只关心 Value
    auto callback = [](void* arg, const Slice& k, const Slice& v) {
        (void)k;
        Context* c = static_cast<Context*>(arg);
        c->found = true;
        c->value = v.ToString();
    };

    // Case A: 查存在的 Key (key000500)
    ctx.found = false;
    // 必须用 MakeKey 构造 InternalKey 进行查找
    table->InternalGet(ReadOptions(), MakeKey(500), &ctx, callback);
    ASSERT_TRUE(ctx.found);
    ASSERT_EQ(ctx.value, "val");

    // Case B: 查不存在的 Key (key999999)
    ctx.found = false;
    table->InternalGet(ReadOptions(), MakeKey(999999), &ctx, callback);
    ASSERT_FALSE(ctx.found);

    delete table;
}