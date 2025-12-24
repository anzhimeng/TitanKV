#include <gtest/gtest.h>
#include "util/coding.h"
#include <vector>

using namespace titankv;

class CodingTest : public testing::Test {};

// --- Fixed32 / Fixed64 测试 ---

TEST_F(CodingTest, Fixed32) {
    std::string s;
    for (uint32_t v = 0; v < 100000; v++) {
        PutFixed32(&s, v);
    }

    const char* p = s.data();
    for (uint32_t v = 0; v < 100000; v++) {
        uint32_t actual = DecodeFixed32(p);
        ASSERT_EQ(v, actual);
        p += sizeof(uint32_t);
    }
}

TEST_F(CodingTest, Fixed64) {
    std::string s;
    uint64_t values[] = {0, 100, 0xFFFFFFFF, 0xFFFFFFFFFFFFFFFFull};
    for (uint64_t v : values) {
        PutFixed64(&s, v);
    }

    const char* p = s.data();
    for (uint64_t v : values) {
        uint64_t actual = DecodeFixed64(p);
        ASSERT_EQ(v, actual);
        p += sizeof(uint64_t);
    }
}

// --- Varint32 / Varint64 测试 ---

TEST_F(CodingTest, Varint32) {
    std::string s;
    // 测试边界值：
    // < 128 (1 byte)
    // < 16384 (2 bytes)
    // ...
    std::vector<uint32_t> values = {0, 1, 127, 128, 16383, 16384, (1u << 31), 0xFFFFFFFF};
    
    for (uint32_t v : values) {
        PutVarint32(&s, v);
    }

    Slice input(s);
    for (uint32_t v : values) {
        uint32_t actual;
        ASSERT_TRUE(GetVarint32(&input, &actual));
        ASSERT_EQ(v, actual);
    }
    ASSERT_TRUE(input.empty()); // 确保所有字节都被消费了
}

TEST_F(CodingTest, Varint64) {
    // 构造一些跨越不同字节长度的数
    std::vector<uint64_t> values;
    uint64_t v = 1;
    for (int i = 0; i < 64; i++) {
        values.push_back(v);
        values.push_back(v - 1);
        v <<= 1;
    }
    values.push_back(0);
    values.push_back(0xFFFFFFFFFFFFFFFFull);

    std::string s;
    for (uint64_t val : values) {
        PutVarint64(&s, val);
    }

    Slice input(s);
    for (uint64_t val : values) {
        uint64_t actual;
        ASSERT_TRUE(GetVarint64(&input, &actual));
        ASSERT_EQ(val, actual);
    }
    ASSERT_TRUE(input.empty());
}

TEST_F(CodingTest, VarintLength) {
    ASSERT_EQ(1, VarintLength(0));
    ASSERT_EQ(1, VarintLength(127));
    ASSERT_EQ(2, VarintLength(128));
    ASSERT_EQ(2, VarintLength(16383));
    ASSERT_EQ(3, VarintLength(16384));
    ASSERT_EQ(5, VarintLength(0xFFFFFFFF));
    ASSERT_EQ(10, VarintLength(0xFFFFFFFFFFFFFFFFull));
}

// 验证 Slice 指针移动
TEST_F(CodingTest, SliceAdvance) {
    std::string s;
    PutVarint32(&s, 500); // 500 > 128, 占 2 字节
    PutFixed32(&s, 999);  // 占 4 字节
    
    Slice input(s);
    ASSERT_EQ(6, input.size()); // 2 + 4

    uint32_t v1;
    ASSERT_TRUE(GetVarint32(&input, &v1));
    ASSERT_EQ(500, v1);
    
    ASSERT_EQ(4, input.size()); // 应该只剩 4 字节
    
    uint32_t v2 = DecodeFixed32(input.data());
    ASSERT_EQ(999, v2);
}