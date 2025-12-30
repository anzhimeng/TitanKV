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

    std::string MakeKey(int i) {
        char buf[100];
        snprintf(buf, sizeof(buf), "key%06d", i);
        std::string key(buf);
        PutFixed64(&key, (uint64_t(1) << 8) | 1); 
        return key;
    }
};

TEST_F(TableBuilderTest, SimpleBuild) {
    std::unique_ptr<WritableFile> file;
    ASSERT_TRUE(NewWritableFile(fname_, &file).ok());

    TableBuilder builder(options_, file.get());

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
    
    // 【关键修复】传入 file_number = 1
    ASSERT_TRUE(Table::Open(options_, raf.get(), 1, file_size, &table).ok());

    struct Context {
        bool found;
        std::string value;
    } ctx;
    
    auto callback = [](void* arg, const Slice& k, const Slice& v) {
        (void)k;
        Context* c = static_cast<Context*>(arg);
        c->found = true;
        c->value = v.ToString();
    };

    // Case A: 查存在的 Key
    ctx.found = false;
    table->InternalGet(ReadOptions(), MakeKey(500), &ctx, callback);
    ASSERT_TRUE(ctx.found);
    ASSERT_EQ(ctx.value, "val");

    // Case B: 查不存在的 Key
    ctx.found = false;
    table->InternalGet(ReadOptions(), MakeKey(999999), &ctx, callback);
    ASSERT_FALSE(ctx.found);

    delete table;
}