#include "gtest/gtest.h"
#include "lsm/memtable.h"
#include "lsm/dbformat.h"
#include "blob/blob_format.h"
#include "titankv/status.h"
#include <string>

using namespace titankv;

class MemTableTest : public testing::Test {
protected:
    InternalKeyComparator cmp_;
    MemTable* mem_;

    MemTableTest() : cmp_() {
        mem_ = new MemTable(cmp_);
        mem_->Ref();
    }

    ~MemTableTest() override {
        mem_->Unref();
    }

    // 辅助函数：模拟生成一个 BlobIndex 的序列化字符串
    std::string MakeBlobIndex(uint32_t file_id, uint64_t offset, uint64_t size) {
        BlobIndex index;
        index.file_id = file_id;
        index.offset = offset;
        index.size = size;
        std::string val;
        index.EncodeTo(&val);
        return val;
    }
};

// 1. 基础读写测试
TEST_F(MemTableTest, SimpleAddGet) {
    std::string val = MakeBlobIndex(1, 100, 50);
    
    // 写入: Seq=1, Type=Put, Key="key1"
    mem_->Add(1, kTypeValue, "key1", val);

    // 读取: 使用 Seq=2 进行查找 (应该能读到 Seq=1 的数据)
    LookupKey lkey("key1", 2);
    std::string res_val;
    Status s;
    
    ASSERT_TRUE(mem_->Get(lkey, &res_val, &s));
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(res_val, val);
}

// 2. MVCC 多版本覆盖测试
TEST_F(MemTableTest, OverwriteAndSnapshot) {
    // 写入版本 1 (Seq=10)
    std::string val1 = MakeBlobIndex(1, 100, 50);
    mem_->Add(10, kTypeValue, "key_mvcc", val1);

    // 写入版本 2 (Seq=20) - 覆盖
    std::string val2 = MakeBlobIndex(2, 200, 50);
    mem_->Add(20, kTypeValue, "key_mvcc", val2);

    // Case A: 读取最新 (Seq=30) -> 应该读到 Seq=20 的数据
    {
        LookupKey lkey("key_mvcc", 30);
        std::string res;
        Status s;
        ASSERT_TRUE(mem_->Get(lkey, &res, &s));
        ASSERT_EQ(res, val2);
    }

    // Case B: 快照读 (Seq=15) -> 应该读到 Seq=10 的数据 (历史穿越)
    // 因为 15 < 20，所以 Seq=20 对我不可见
    {
        LookupKey lkey("key_mvcc", 15);
        std::string res;
        Status s;
        ASSERT_TRUE(mem_->Get(lkey, &res, &s));
        ASSERT_EQ(res, val1);
    }
}

// 3. 删除测试 (Tombstone)
TEST_F(MemTableTest, Delete) {
    std::string val = MakeBlobIndex(1, 100, 50);
    
    // 1. 写入数据 (Seq=5)
    mem_->Add(5, kTypeValue, "key_del", val);

    // 2. 写入删除标记 (Seq=10)
    mem_->Add(10, kTypeDeletion, "key_del", "");

    // 3. 读取 (Seq=15) -> 应该读到 Deletion，返回 NotFound
    {
        LookupKey lkey("key_del", 15);
        std::string res;
        Status s;
        // MemTable::Get 返回 true 表示找到了 key entry
        // 但 s.IsNotFound() 表示这个 entry 是一个删除标记
        ASSERT_TRUE(mem_->Get(lkey, &res, &s)); 
        ASSERT_TRUE(s.IsNotFound());
    }

    // 4. 快照读 (Seq=8) -> 应该读到 Seq=5 的数据 (因为它还没被删)
    {
        LookupKey lkey("key_del", 8);
        std::string res;
        Status s;
        ASSERT_TRUE(mem_->Get(lkey, &res, &s));
        ASSERT_TRUE(s.ok());
        ASSERT_EQ(res, val);
    }
}

// 4. 不存在的 Key
TEST_F(MemTableTest, NotFound) {
    LookupKey lkey("key_not_exist", 100);
    std::string res;
    Status s;
    ASSERT_FALSE(mem_->Get(lkey, &res, &s)); // 返回 false 表示 SkipList 里没这 Key
}