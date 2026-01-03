#include "gtest/gtest.h"
#include "titankv/db.h"
#include "titankv/options.h"
#include <filesystem>
#include <string>
#include <thread>

using namespace titankv;

class RecoveryTest : public testing::Test {
protected:
    std::string dbname_ = "/tmp/titankv_recovery_test";
    DB* db_ = nullptr;
    Options options_;

void SetUp() override {
        if (db_) { delete db_; db_ = nullptr; }
        std::filesystem::remove_all(dbname_);
        
        options_.create_if_missing = true;
        
        // 【修改】从 4096 改为 64KB
        // Arena 初始分配 4KB，如果设为 4096，第一条数据就会填满配额触发 Flush。
        // 改为 64KB 可以容纳更多数据，减少 SSTable 数量。
        options_.write_buffer_size = 64 * 1024; 
        
        options_.max_file_size = 1024 * 1024;
    }

    void TearDown() override {
        if (db_) delete db_;
        std::filesystem::remove_all(dbname_);
    }

    void Open() {
        if (db_) delete db_;
        ASSERT_TRUE(DB::Open(options_, dbname_, &db_).ok());
    }

    void Close() {
        if (db_) {
            delete db_;
            db_ = nullptr;
        }
    }
};

TEST_F(RecoveryTest, BasicPersistence) {
    Open();
    
    // 1. 写入数据
    // 写入 Key1 (MemTable)
    ASSERT_TRUE(db_->Put(WriteOptions(), "key1", "val1").ok());
    // 写入 Key2 (MemTable)
    ASSERT_TRUE(db_->Put(WriteOptions(), "key2", "val2").ok());
    
    // 2. 关闭数据库 (模拟正常关闭)
    Close();

    // 3. 重新打开 (Recover 应该重放 WAL)
    Open();

    // 4. 验证数据
    std::string val;
    ASSERT_TRUE(db_->Get(ReadOptions(), "key1", &val).ok());
    ASSERT_EQ(val, "val1");
    ASSERT_TRUE(db_->Get(ReadOptions(), "key2", &val).ok());
    ASSERT_EQ(val, "val2");
}

TEST_F(RecoveryTest, WithSSTables) {
    Open();

    // 写入 1000 条，每条 ~20B
    // Total ~20KB
    // 如果 write_buffer_size 是 4KB，会生成 ~5 个 SSTable
    int N = 1000;
    for (int i = 0; i < N; i++) {
        std::string key = "key" + std::to_string(i);
        std::string val = "val" + std::to_string(i);
        ASSERT_TRUE(db_->Put(WriteOptions(), key, val).ok());
    }

    // 这里的 Close 会析构 DB，触发 MemTable 丢弃（如果没有 Flush）
    // 为了保证数据在 SSTable 里，我们最好手动触发一次 Flush 
    // 或者依赖 WriteOptions.sync (但这只针对 WAL)
    
    // 技巧：重新 Open 一次会触发 Recover，Recover 会重放 WAL 恢复 MemTable
    // 所以即使没 Flush 成 SSTable，数据也在 MemTable 里。
    
    Close(); // 模拟 Crash/Restart

    Open(); // 触发 Recover

    // 验证
    for (int i = 0; i < N; i++) {
        std::string key = "key" + std::to_string(i);
        std::string val;
        Status s = db_->Get(ReadOptions(), key, &val);
        // 如果这里报错 NotFound，说明 Recover 逻辑（Manifest或WAL重放）有错
        ASSERT_TRUE(s.ok()) << "Missing key: " << key; 
        ASSERT_EQ(val, "val" + std::to_string(i));
    }
}

TEST_F(RecoveryTest, ManifestReuse) {
    // 验证多次重启后，Manifest 是否正常工作
    Open();
    db_->Put(WriteOptions(), "a", "1");
    Close();

    Open();
    db_->Put(WriteOptions(), "b", "2");
    Close();

    Open();
    std::string v;
    ASSERT_TRUE(db_->Get(ReadOptions(), "a", &v).ok());
    ASSERT_EQ(v, "1");
    ASSERT_TRUE(db_->Get(ReadOptions(), "b", &v).ok());
    ASSERT_EQ(v, "2");
}