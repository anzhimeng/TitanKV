#include "gtest/gtest.h"
#include "lsm/skiplist.h"
#include "util/arena.h"
#include <set>

using namespace titankv;

// 简单的 Int 比较器
struct IntComparator {
    int operator()(const uint64_t& a, const uint64_t& b) const {
        if (a < b) return -1;
        if (a > b) return +1;
        return 0;
    }
};

TEST(SkipListTest, InsertAndContains) {
    Arena arena;
    IntComparator cmp;
    SkipList<uint64_t, IntComparator> list(cmp, &arena);

    ASSERT_TRUE(!list.Contains(10));
    list.Insert(10);
    ASSERT_TRUE(list.Contains(10));
    ASSERT_TRUE(!list.Contains(20));
    
    list.Insert(20);
    list.Insert(5);
    
    ASSERT_TRUE(list.Contains(10));
    ASSERT_TRUE(list.Contains(5));
    ASSERT_TRUE(list.Contains(20));
}

TEST(SkipListTest, Iterator) {
    Arena arena;
    IntComparator cmp;
    SkipList<uint64_t, IntComparator> list(cmp, &arena);
    
    // 插入 0, 10, 20 ... 90
    for (int i = 0; i < 100; i+=10) {
        list.Insert(i);
    }

    auto iter = list.NewIterator();
    
    // Test Seek
    iter->Seek(50);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), 50);

    // Test Seek non-existent (should land on next)
    iter->Seek(55);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), 60);

    // Test Next
    iter->SeekToFirst();
    ASSERT_EQ(iter->key(), 0);
    iter->Next();
    ASSERT_EQ(iter->key(), 10);

    // Test SeekToLast
    iter->SeekToLast();
    ASSERT_EQ(iter->key(), 90);
    
    delete iter;
}