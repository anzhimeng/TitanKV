#include "gtest/gtest.h"
#include "titankv/db.h"
#include <filesystem>
#include <string>
#include <vector>

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