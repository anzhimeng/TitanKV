#include "gtest/gtest.h"
#include "lsm/merging_iterator.h"
#include "lsm/dbformat.h"
#include "util/coding.h" 
#include <vector>
#include <string>
#include <algorithm>

using namespace titankv;

// 【新增】辅助函数：构造合法的 Internal Key
std::string MakeKey(const std::string& user_key, uint64_t seq = 1) {
    std::string key = user_key;
    // 追加 8 字节 Tag (SeqNum << 8 | Type)
    PutFixed64(&key, (seq << 8) | kTypeValue);
    return key;
}

// 【新增】辅助函数：从 Internal Key 提取 User Key 用于验证
std::string ExtractUser(const Slice& internal_key) {
    assert(internal_key.size() >= 8);
    return std::string(internal_key.data(), internal_key.size() - 8);
}

class TestIterator : public Iterator {
public:
    TestIterator(const std::vector<std::string>& keys) : keys_(keys), index_(0) {
        std::sort(keys_.begin(), keys_.end());
    }
    bool Valid() const override { return index_ < keys_.size(); }
    void SeekToFirst() override { index_ = 0; }
    void SeekToLast() override { if (!keys_.empty()) index_ = keys_.size() - 1; }
    
    void Seek(const Slice& target) override {
        // 简单模拟 Seek
        auto it = std::lower_bound(keys_.begin(), keys_.end(), target.ToString());
        index_ = std::distance(keys_.begin(), it);
    }
    
    void Next() override { index_++; }
    void Prev() override { if (index_ > 0) index_--; }
    
    Slice key() const override { return Slice(keys_[index_]); }
    Slice value() const override { return Slice("val"); }
    Status status() const override { return Status::OK(); }

private:
    std::vector<std::string> keys_;
    size_t index_;
};

TEST(MergingIteratorTest, BasicMerge) {
    InternalKeyComparator cmp;
    
    // 构造 3 个有序序列 (使用合法的 Internal Key)
    // List 1: 1, 4, 7
    std::vector<std::string> k1 = {MakeKey("1"), MakeKey("4"), MakeKey("7")};
    // List 2: 2, 5, 8
    std::vector<std::string> k2 = {MakeKey("2"), MakeKey("5"), MakeKey("8")};
    // List 3: 3, 6, 9
    std::vector<std::string> k3 = {MakeKey("3"), MakeKey("6"), MakeKey("9")};

    Iterator* children[3];
    children[0] = new TestIterator(k1);
    children[1] = new TestIterator(k2);
    children[2] = new TestIterator(k3);

    Iterator* iter = NewMergingIterator(&cmp, children, 3);
    
    iter->SeekToFirst();
    for (int i = 1; i <= 9; i++) {
        ASSERT_TRUE(iter->Valid());
        // 验证 User Key 部分
        ASSERT_EQ(ExtractUser(iter->key()), std::to_string(i));
        iter->Next();
    }
    ASSERT_FALSE(iter->Valid());

    delete iter;
}

TEST(MergingIteratorTest, EmptyAndOverlap) {
    InternalKeyComparator cmp;
    
    // List 1: 1, 3, 5
    std::vector<std::string> k1 = {MakeKey("1"), MakeKey("3"), MakeKey("5")};
    // List 2: (Empty)
    std::vector<std::string> k2 = {};
    // List 3: 1, 4, 5 (Overlapping keys)
    std::vector<std::string> k3 = {MakeKey("1"), MakeKey("4"), MakeKey("5")};

    Iterator* children[3];
    children[0] = new TestIterator(k1);
    children[1] = new TestIterator(k2);
    children[2] = new TestIterator(k3);

    Iterator* iter = NewMergingIterator(&cmp, children, 3);
    
    // Expected User Keys: 1, 1, 3, 4, 5, 5
    std::vector<std::string> expected = {"1", "1", "3", "4", "5", "5"};
    
    iter->SeekToFirst();
    for (const auto& expect : expected) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(ExtractUser(iter->key()), expect);
        iter->Next();
    }
    ASSERT_FALSE(iter->Valid());

    delete iter;
}