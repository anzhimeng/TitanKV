#pragma once

#include <string>
#include <cstdint>
#include "titankv/slice.h"
#include "titankv/status.h"

namespace titankv {

// 定义 Header 长度常量 (CRC:4 + KeyLen:4 + ValLen:4)
constexpr uint32_t kHeaderSize = 12;

// ==========================================
// 1. BlobIndex (In-Memory)
// ==========================================
struct BlobIndex {
    uint32_t file_id = 0;
    uint64_t offset = 0;
    uint64_t size = 0;

    void EncodeTo(std::string* dst) const;
    Status DecodeFrom(Slice* input);
    std::string ToString() const;

    // 【新增】相等比较操作符
    bool operator==(const BlobIndex& other) const {
        return file_id == other.file_id &&
               offset == other.offset &&
               size == other.size;
    }
};

// ==========================================
// 2. BlobRecordHeader (On-Disk)
// ==========================================
struct BlobRecordHeader {
    uint32_t crc = 0;
    uint32_t size = 0;      // Value size
    uint32_t key_size = 0;  // Key size

    static const size_t kHeaderSize = 12; // 4 + 4 + 4

    void EncodeTo(char* dst) const;
    Status DecodeFrom(Slice* input);
};

// ==========================================
// 3. Helper Structures (用于读取和 GC)
// ==========================================

// 用于持有解析后的 Key/Value (只是 Slice，不拥有内存)
struct ParsedBlobRecord {
    Slice key;
    Slice value;
    BlobRecordHeader header;
};

// 从 Slice 中解析完整的 BlobRecord (Header + Key + Value)
// 输入: input 指向 Header 开始的位置
// 输出: result 填充 Key/Value Slice，input 指针会移动到 Record 之后
Status ParseBlobRecord(Slice* input, ParsedBlobRecord* result);

} // namespace titankv
