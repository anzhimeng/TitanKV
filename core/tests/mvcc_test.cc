#include "gtest/gtest.h"
#include "util/coding.h"
#include "titankv/db.h"
#include "lsm/mvcc_reader.h"
#include "util/filename.h"
#include <string>
#include "titankv/db_impl.h"
#include <filesystem>

using namespace titankv;

class MvccIntegrationTest : public testing::Test {
protected:
    std::string dbname_ = "/tmp/titankv_mvcc_test";
    DB* db_;
    Options options_;

    void SetUp() override {
        std::filesystem::remove_all(dbname_);
        options_.create_if_missing = true;
        ASSERT_TRUE(DB::Open(options_, dbname_, &db_).ok());
    }

    void TearDown() override {
        delete db_;
        std::filesystem::remove_all(dbname_);
    }
};

// 验证 CF 隔离：不同 CF 的同名 Key 互不影响
TEST_F(MvccIntegrationTest, CFIsolation) {
    // 1. 在 Default CF 写入 (TS=100)
    ASSERT_TRUE(db_->PutCF(kCFDefault, "key1", "val_d", 100).ok());
    
    // 2. 在 Write CF 写入 (TS=100)
    ASSERT_TRUE(db_->PutCF(kCFWrite, "key1", "val_w", 100).ok());
    
    // 3. 在 Lock CF 写入 (无 TS)
    ASSERT_TRUE(db_->PutCF(kCFLock, "key1", "val_l", 0).ok());
    
    // 4. 读取验证
    std::string val;
    // 读 Default
    ASSERT_TRUE(db_->GetCF(kCFDefault, "key1", &val, 100).ok());
    ASSERT_EQ(val, "val_d");
    
    // 读 Write
    ASSERT_TRUE(db_->GetCF(kCFWrite, "key1", &val, 100).ok());
    ASSERT_EQ(val, "val_w");
    
    // 读 Lock
    ASSERT_TRUE(db_->GetCF(kCFLock, "key1", &val, 0).ok());
    ASSERT_EQ(val, "val_l");
}

TEST_F(MvccIntegrationTest, CheckConflict) {
    DBImpl* impl = dynamic_cast<DBImpl*>(db_);
    ASSERT_NE(impl, nullptr);

    // 1. Write a committed version at TS=100
    ASSERT_TRUE(db_->PutCF(kCFWrite, "key_c", "write_info_100", 100).ok());

    // 2. Check Conflict with StartTS=90 (Older than 100)
    std::vector<std::string> keys = {"key_c"};
    Status s = impl->CheckConflict(keys, 90);
    ASSERT_TRUE(s.IsIOError());
    ASSERT_EQ(s.ToString(), "IO error: Write conflict");

    // 3. Check Conflict with StartTS=110 (Newer than 100)
    s = impl->CheckConflict(keys, 110);
    ASSERT_TRUE(s.ok());

    // 4. Check Lock Conflict
    ASSERT_TRUE(db_->PutCF(kCFLock, "key_c", "lock_info", 0).ok());
    s = impl->CheckConflict(keys, 110);
    ASSERT_TRUE(s.IsIOError());
    ASSERT_EQ(s.ToString(), "IO error: Key is locked");
}

// 验证 MVCC 排序：最新的版本应该先被读到 (Seek)
TEST_F(MvccIntegrationTest, VersionOrder) {
    // 写入三个版本
    ASSERT_TRUE(db_->PutCF(kCFWrite, "key_v", "v10", 10).ok());
    ASSERT_TRUE(db_->PutCF(kCFWrite, "key_v", "v20", 20).ok());
    ASSERT_TRUE(db_->PutCF(kCFWrite, "key_v", "v30", 30).ok());
    
    // 使用 MvccReader 进行 Seek
    MvccReader reader(db_, 25); // Snapshot = 25
    
    uint64_t commit_ts;
    std::string write_info;
    
    // 应该找到 TS <= 25 的最新版本，即 TS=20
    Status s = reader.SeekWrite("key_v", &commit_ts, &write_info);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(commit_ts, 20);
    ASSERT_EQ(write_info, "v20");
    
    // 如果 Snapshot = 5，应该 NotFound (最小是 10)
    MvccReader reader2(db_, 5);
    s = reader2.SeekWrite("key_v", &commit_ts, &write_info);
    ASSERT_TRUE(s.IsNotFound());
}

TEST(MvccTest, EncodingOrder) {
    // 模拟两个版本: TS=100 (New), TS=90 (Old)
    // 期望: Encode(100) < Encode(90)
    
    std::string k1 = EncodeMvccKey(kCFWrite, "key_a", 100);
    std::string k2 = EncodeMvccKey(kCFWrite, "key_a", 90);
    
    // 验证顺序
    ASSERT_LT(k1, k2); // 字节序比较
    
    // 验证不同 Key
    std::string k3 = EncodeMvccKey(kCFWrite, "key_b", 200);
    ASSERT_LT(k1, k3); // "key_a" < "key_b"
}

TEST(MvccTest, Decode) {
    std::string encoded = EncodeMvccKey(kCFDefault, "my_key", 12345);
    
    uint64_t ts;
    Slice user_key = DecodeMvccKey(encoded, &ts);
    
    ASSERT_EQ(user_key.ToString(), "my_key");
    ASSERT_EQ(ts, 12345);
}

TEST(MvccTest, CFIsolation) {
    // Default vs Write vs Lock
    std::string d = EncodeMvccKey(kCFDefault, "key", 100);
    std::string l = EncodeLockKey("key");
    std::string w = EncodeMvccKey(kCFWrite, "key", 100);
    
    // 验证 CF 前缀生效
    // 'd' < 'l' < 'w'
    ASSERT_LT(d, l);
    ASSERT_LT(l, w);
}