#pragma once
namespace titankv {
namespace log {

enum RecordType {
  // Zero is reserved for preallocated files
  kZeroType = 0,
  kFullType = 1,   // 记录完整地在一个 Block 中
  kFirstType = 2,  // 记录的第一部分
  kMiddleType = 3, // 记录的中间部分
  kLastType = 4    // 记录的最后一部分
};

static const int kMaxRecordType = kLastType;
static const int kBlockSize = 32768; // 32KB
static const int kHeaderSize = 4 + 2 + 1; // CRC(4) + Len(2) + Type(1)

} // namespace log
} // namespace titankv