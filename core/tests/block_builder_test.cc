#include "gtest/gtest.h"
#include "lsm/block_builder.h"
#include "util/coding.h"

using namespace titankv;

TEST(BlockBuilderTest, SimpleEncode) {
    Options options;
    options.block_restart_interval = 16;
    BlockBuilder builder(&options);

    builder.Add("apple", "red");
    builder.Add("apricot", "orange");

    Slice block = builder.Finish();
    
    // 验证逻辑：手动解析一下生成的 buffer
    // 1. Entry 1: apple
    // shared=0, non_shared=5, val_len=3, "apple", "red"
    // Varint(0)=1B, Varint(5)=1B, Varint(3)=1B, Key=5B, Val=3B -> Total 11B
    
    // 2. Entry 2: apricot
    // shared=2 ("ap"), non_shared=5 ("ricot"), val_len=6 ("orange")
    // Varint(2)=1B, Varint(5)=1B, Varint(6)=1B, Key=5B, Val=6B -> Total 14B
    
    // 3. Restart Array
    // Restart[0] = 0 (4B)
    // Restart Count = 1 (4B)
    
    // Total Size = 11 + 14 + 4 + 4 = 33 Bytes
    
    ASSERT_EQ(block.size(), 33);
}

TEST(BlockBuilderTest, RestartPoints) {
    Options options;
    options.block_restart_interval = 2; // 每 2 个 Key 重启一次
    BlockBuilder builder(&options);

    builder.Add("key1", "v1"); // Restart 0
    builder.Add("key2", "v2");
    builder.Add("key3", "v3"); // Restart 1 (Offset should be calculated)
    
    Slice block = builder.Finish();
    
    // 读取最后的 count
    const char* p = block.data() + block.size() - 4;
    uint32_t num_restarts = DecodeFixed32(p);
    
    ASSERT_EQ(num_restarts, 2);
}