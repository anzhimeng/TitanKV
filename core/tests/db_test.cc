#include "gtest/gtest.h"
#include "titankv/db.h"
#include <filesystem>
#include <string>
#include <vector>
#include <thread>

using namespace titankv;

class DBTest : public testing::Test {
protected:
    std::string dbname_;
    DB* db_;
    Options options_;

    void SetUp() override {
        dbname_ = "/tmp/titankv_week1_test";
        // 清理旧环境
        std::filesystem::remove_all(dbname_);
        
        options_.create_if_missing = true;
        // 设置一个较小的 Blob 阈值，方便测试 KV 分离
        options_.min_blob_size = 100; // 100 Bytes 以上就分离
        
        ASSERT_TRUE(DB::Open(options_, dbname_, &db_).ok());
    }

    void TearDown() override {
        delete db_;
        // 暂时不删目录，方便排错，或者手动删
        std::filesystem::remove_all(dbname_);
    }

    // 辅助：重启数据库
    void Reopen() {
        delete db_;
        db_ = nullptr;
        ASSERT_TRUE(DB::Open(options_, dbname_, &db_).ok());
    }
};

// 1. 测试小 Value (内联到 LSM)
TEST_F(DBTest, PutGetSmall) {
    std::string key = "small_key";
    std::string val = "small_val"; // < 100 bytes
    
    ASSERT_TRUE(db_->Put(WriteOptions(), key, val).ok());
    
    std::string res;
    ASSERT_TRUE(db_->Get(ReadOptions(), key, &res).ok());
    ASSERT_EQ(val, res);
}

// 2. 测试大 Value (分离到 BlobStore)
TEST_F(DBTest, PutGetBlob) {
    std::string key = "blob_key";
    std::string val(1024, 'x'); // 1KB > 100 bytes
    
	Status s = db_->Put(WriteOptions(), key, val);
	ASSERT_TRUE(s.ok()) << s.ToString(); // 如果失败，打印错误详情
    
    // 验证：
    // 1. Get 能读回来
    std::string res;
    ASSERT_TRUE(db_->Get(ReadOptions(), key, &res).ok());
    ASSERT_EQ(val, res);
    
    // 2. 检查磁盘上是否有 .blob 文件
    bool blob_exists = false;
    for (const auto& entry : std::filesystem::directory_iterator(dbname_ + "/blob")) {
        if (entry.path().extension() == ".blob") {
            blob_exists = true;
            // 文件大小应该至少是 1024 + header overhead
            ASSERT_GT(entry.file_size(), 1024);
        }
    }
    ASSERT_TRUE(blob_exists) << "Blob file should exist for large value";
}

// 3. 测试持久化恢复 (Recovery)
TEST_F(DBTest, Recovery) {
    std::string k1 = "key_small";
    std::string v1 = "val_small";
    std::string k2 = "key_blob";
    std::string v2(2000, 'y'); // Large blob

    Status s = db_->Put(WriteOptions(), k1, v1);
    ASSERT_TRUE(s.ok()) << s.ToString(); 
    Status s2 = db_->Put(WriteOptions(), k2, v2);
    ASSERT_TRUE(s2.ok()) << s2.ToString(); // 如果失败，打印错误详情

    // 模拟重启
    Reopen();

    std::string res;
    // 读小值
    ASSERT_TRUE(db_->Get(ReadOptions(), k1, &res).ok());
    ASSERT_EQ(v1, res);
    
    // 读大值 (需要去 BlobStore 读)
    ASSERT_TRUE(db_->Get(ReadOptions(), k2, &res).ok());
    ASSERT_EQ(v2, res);
}

// 4. 测试混合写入与删除 (覆盖写)
TEST_F(DBTest, Overwrite) {
    std::string key = "key_overwrite";
    
    // 1. Write Blob
    std::string v1(500, 'a');
    db_->Put(WriteOptions(), key, v1);
    
    // 2. Overwrite with Small
    std::string v2 = "short";
    db_->Put(WriteOptions(), key, v2);
    
    std::string res;
    db_->Get(ReadOptions(), key, &res);
    ASSERT_EQ(v2, res); // 应该是新的值

    // 3. 重启后依然是新的
    Reopen();
    db_->Get(ReadOptions(), key, &res);
    ASSERT_EQ(v2, res);
}

TEST_F(DBTest, FlushAndRead) {
    // 1. 设置很小的 MemTable 阈值 (1KB)，方便触发 Flush
    options_.write_buffer_size = 1024;
    
    // 关闭并用新配置重新打开
    delete db_;
    std::filesystem::remove_all(dbname_);
    ASSERT_TRUE(DB::Open(options_, dbname_, &db_).ok());

    // 2. 写入数据，确保超过 1KB
    std::string key1 = "flushed_key1";
    std::string val1(500, 'a'); // ~500 B
    std::string key2 = "flushed_key2";
    std::string val2(600, 'b'); // ~600 B (500+600 > 1024)
    
    // 第一次 Put，数据在 MemTable
    ASSERT_TRUE(db_->Put(WriteOptions(), key1, val1).ok());
    
    // 第二次 Put，应该会触发 MakeRoomForWrite -> Flush
    // key1 会被刷到 SSTable，key2 会在新 MemTable 里
    ASSERT_TRUE(db_->Put(WriteOptions(), key2, val2).ok());
    
    // 3. 验证 SSTable 文件已生成
    bool sst_exists = false;
    for (const auto& entry : std::filesystem::directory_iterator(dbname_)) {
        if (entry.path().extension() == ".sst") {
            sst_exists = true;
            break;
        }
    }
    ASSERT_TRUE(sst_exists) << "SSTable should be created after flush";
    
    // 4. 读取验证
    std::string res;
    
    // Case A: 读取被 Flush 掉的 Key (key1)
    // 这一步会走 Get -> Mem(miss) -> Imm(miss) -> SSTable(hit) 路径
    ASSERT_TRUE(db_->Get(ReadOptions(), key1, &res).ok());
    ASSERT_EQ(val1, res);
    
    // Case B: 读取还在新 MemTable 里的 Key (key2)
    ASSERT_TRUE(db_->Get(ReadOptions(), key2, &res).ok());
    ASSERT_EQ(val2, res);
    
    // 5. 重启后验证 (数据都在 SSTable 里)
    Reopen();

    ASSERT_TRUE(db_->Get(ReadOptions(), key1, &res).ok());
    ASSERT_EQ(val1, res);
    ASSERT_TRUE(db_->Get(ReadOptions(), key2, &res).ok());
    ASSERT_EQ(val2, res);
}

TEST_F(DBTest, Delete) {
    std::string key = "key_to_delete";
    std::string val = "value_content";

    // 1. Put
    ASSERT_TRUE(db_->Put(WriteOptions(), key, val).ok());
    
    // 2. Get (应该存在)
    std::string res;
    ASSERT_TRUE(db_->Get(ReadOptions(), key, &res).ok());
    ASSERT_EQ(res, val);

    // 3. Delete
    ASSERT_TRUE(db_->Delete(WriteOptions(), key).ok());

    // 4. Get (应该 NotFound)
    Status s = db_->Get(ReadOptions(), key, &res);
    ASSERT_TRUE(s.IsNotFound());
    
    // 5. 重启后依然 NotFound
    Reopen();
    s = db_->Get(ReadOptions(), key, &res);
    ASSERT_TRUE(s.IsNotFound());
}

TEST_F(DBTest, TriggerCompaction) {
    // 1. 极端配置：1KB 就 Flush
    options_.write_buffer_size = 1024; 
    
    // 关闭旧 DB，用新配置打开
    delete db_;
    std::filesystem::remove_all(dbname_);
    ASSERT_TRUE(DB::Open(options_, dbname_, &db_).ok());

    // 2. 写入数据，制造 5 个 SSTable
    // L0 触发阈值是 4 个文件。我们写 5 个文件，Score 应该是 1.25，必定触发。
    // 每个文件 1KB，所以总共写 6KB 数据足够了。
    
    for (int i = 0; i < 6; i++) {
        // 每次写入 1.5KB，确保填满一个 MemTable 并触发 Flush
        std::string key = "key_" + std::to_string(i);
        std::string val(1500, 'v'); 
        
        ASSERT_TRUE(db_->Put(WriteOptions(), key, val).ok());
        
        // 稍微 sleep 一下，让后台线程有机会运行
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // 3. 等待 Compaction 发生
    // 此时 L0 应该有 6 个文件。Score = 1.5。
    // BGWork 应该会 Pick 其中一个进行合并。
    
    fprintf(stderr, "Waiting for compaction...\n");
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    // 4. 验证 (通过日志观察，或者检查文件数)
    // 这种测试主要靠看日志，或者通过 GetProperty 接口（还没实现）
    // 这里我们只要不 Crash 就算过。
}

