#include <gtest/gtest.h>
#include "blob/blob_format.h"
#include "util/coding.h"
#include <string>

using namespace titankv;

class BlobFormatTest : public testing::Test {};

// 1. 测试内存索引 (Varint)
TEST_F(BlobFormatTest, BlobIndexRoundTrip) {
    BlobIndex original;
    original.file_id = 123;
    original.offset = 1024 * 1024;
    original.size = 500;

    std::string encoded;
    original.EncodeTo(&encoded);

    // Varint 编码检查：123(1B) + 1M(3B) + 500(2B) = 6 Bytes
    // 具体长度取决于 Varint 实现，不强求，只验证编解码一致性
    ASSERT_GT(encoded.size(), 0);

    BlobIndex decoded;
    Slice input(encoded);
    Status s = decoded.DecodeFrom(&input);

    ASSERT_TRUE(s.ok());
    ASSERT_EQ(original.file_id, decoded.file_id);
    ASSERT_EQ(original.offset, decoded.offset);
    ASSERT_EQ(original.size, decoded.size);
    ASSERT_TRUE(input.empty());
}

// 2. 测试磁盘 Header (Fixed)
TEST_F(BlobFormatTest, BlobRecordHeaderRoundTrip) {
    BlobRecordHeader header;
    header.crc = 0xDEADBEEF;
    header.size = 100;     // Value Len
    header.key_size = 10;  // Key Len

    char buf[BlobRecordHeader::kHeaderSize];
    header.EncodeTo(buf);

    // 验证定长编码
    ASSERT_EQ(BlobRecordHeader::kHeaderSize, 12);

    BlobRecordHeader decoded;
    Slice input(buf, sizeof(buf));
    Status s = decoded.DecodeFrom(&input);

    ASSERT_TRUE(s.ok());
    ASSERT_EQ(header.crc, decoded.crc);
    ASSERT_EQ(header.size, decoded.size);
    ASSERT_EQ(header.key_size, decoded.key_size);
    ASSERT_TRUE(input.empty()); // Header 应该被消耗完
}

TEST_F(BlobFormatTest, BlobRecordHeaderSanityCheck) {
    BlobRecordHeader header;
    header.crc = 0;
    header.size = 200 * 1024 * 1024; // 200MB (Over limit)
    header.key_size = 5;

    char buf[BlobRecordHeader::kHeaderSize];
    header.EncodeTo(buf);

    BlobRecordHeader decoded;
    Slice input(buf, sizeof(buf));
    Status s = decoded.DecodeFrom(&input);

    ASSERT_TRUE(s.IsCorruption());
    ASSERT_EQ(s.ToString(), "Corruption: BlobRecordHeader has invalid sizes");
}

// 3. 测试完整的 Record 解析 (模拟从磁盘读取)
TEST_F(BlobFormatTest, ParseFullBlobRecord) {
    std::string key = "user:1001";
    std::string val = "This is a large value payload...";
    
    // 构造 Header
    BlobRecordHeader header;
    header.crc = 0; // 暂时不校验
    header.key_size = key.size();
    header.size = val.size();

    // 模拟磁盘数据 buffer
    std::string disk_data;
    
    // 1. 写入 Header
    char header_buf[BlobRecordHeader::kHeaderSize];
    header.EncodeTo(header_buf);
    disk_data.append(header_buf, sizeof(header_buf));

    // 2. 写入 Key 和 Value
    disk_data.append(key);
    disk_data.append(val);
    
    // 3. 模拟追加了一些垃圾数据 (Parse 应该只读取它需要的部分)
    disk_data.append("garbage data");

    // 开始解析
    Slice input(disk_data);
    ParsedBlobRecord result;
    
    Status s = ParseBlobRecord(&input, &result);
    ASSERT_TRUE(s.ok());

    // 验证字段
    ASSERT_EQ(result.header.key_size, key.size());
    ASSERT_EQ(result.header.size, val.size());
    ASSERT_EQ(result.key.ToString(), key);
    ASSERT_EQ(result.value.ToString(), val);

    // 验证指针移动
    // input 应该指向 garbage data 的开始
    ASSERT_EQ(input.ToString(), "garbage data");
}

TEST_F(BlobFormatTest, ParseIncompleteRecord) {
    BlobRecordHeader header;
    header.crc = 0;
    header.key_size = 10;
    header.size = 10; // 总共需要 20 字节 Payload

    char buf[BlobRecordHeader::kHeaderSize];
    header.EncodeTo(buf);

    std::string disk_data;
    disk_data.append(buf, sizeof(buf));
    disk_data.append("12345"); // 只提供了 5 字节，不够

    Slice input(disk_data);
    ParsedBlobRecord result;
    Status s = ParseBlobRecord(&input, &result);

    ASSERT_TRUE(s.IsCorruption());
    ASSERT_EQ(s.ToString(), "Corruption: BlobRecord data too short");
}