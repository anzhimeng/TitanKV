#include "wal/log_writer.h"
#include <cstring>
#include "util/coding.h"
#include "util/crc32c.h"

namespace titankv {
namespace log {

// 预先初始化 CRC 表，避免每次写入都重复计算 Type 的 CRC
static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char c = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&c, 1);
  }
}

Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // 标记：这是不是该逻辑记录的第一个分片？
  // 逻辑记录被切分时，第一个分片是 First，中间是 Middle，最后是 Last
  // 如果没被切分，则是 Full
  bool begin = true;

  Status s;
  do {
    // 1. 计算当前 Block 剩余空间
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);

    // 2. 如果剩余空间连 Header (7字节) 都放不下
    // 则填充 0，并开启新 Block
    if (leftover < kHeaderSize) {
      if (leftover > 0) {
        // 填充 \x00
        // 注意：kZeroType = 0，Reader 读到 Type=0 会跳过
        static const char kZeros[kHeaderSize] = {0};
        s = dest_->Append(Slice(kZeros, leftover));
        if (!s.ok()) {
          return s;
        }
      }
      block_offset_ = 0;
    }

    // 3. 计算本次能写入多少 Payload
    // 此时 block_offset_ 必然是新 Block 的开始，或者剩余空间足够放 Header
    // kBlockSize - block_offset_ 计算的是当前 Block 的有效剩余空间
    // 再减去 kHeaderSize 就是能放数据的空间
    const int avail = kBlockSize - block_offset_ - kHeaderSize;
    
    // 本次实际写入长度：取 min(剩余数据量, Block可用容量)
    const size_t fragment_length = (left < static_cast<size_t>(avail)) ? left : avail;

    // 4. 判定物理记录类型
    RecordType type;
    const bool end = (left == fragment_length); // 如果这次能写完，end 为 true

    if (begin && end) {
      type = kFullType;   // 既是头又是尾 -> 完整记录
    } else if (begin) {
      type = kFirstType;  // 是头但不是尾 -> 第一段
    } else if (end) {
      type = kLastType;   // 不是头但是尾 -> 最后一段
    } else {
      type = kMiddleType; // 既不是头也不是尾 -> 中间段
    }

    // 5. 发射物理记录到文件
    s = EmitPhysicalRecord(type, ptr, fragment_length);
    
    // 6. 更新指针和状态
    ptr += fragment_length;
    left -= fragment_length;
    begin = false; // 下一次循环肯定不是开头了

    // 如果出错，立即停止
    if (!s.ok()) {
      return s;
    }

  } while (left > 0); // 只要还有数据没写完，就继续切分

  return Status::OK();
}

Status Writer::EmitPhysicalRecord(RecordType type, const char* ptr, size_t length) {
  // 断言长度合法 (2字节长度只能表示 65535)
  // 由于 kBlockSize = 32768，fragment_length 永远不会超过 2字节
  assert(length <= 0xffff);
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  // 1. 构造 Header (7 Bytes)
  // Format: Checksum (4), Length (2), Type (1)
  char buf[kHeaderSize];

  // Length (Little Endian)
  buf[4] = static_cast<char>(length & 0xff);
  buf[5] = static_cast<char>(length >> 8);

  // Type
  buf[6] = static_cast<char>(type);

  // CRC Calculation
  // 为了效率，先取预计算的 Type CRC，再 Extend 数据 CRC
  uint32_t crc = crc32c::Extend(type_crc_[type], ptr, length);
  crc = crc32c::Mask(crc); // Mask 防止 CRC 数据本身看起来像某个特征码

  // Put CRC (Little Endian)
  EncodeFixed32(buf, crc);

  // 2. 写入 Header
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    // 3. 写入 Payload
    s = dest_->Append(Slice(ptr, length));
    if (s.ok()) {
      // 4. 刷新到 OS Cache
      // 注意：这里没有调用 Sync()，因为 Sync 太慢。
      // 我们依赖 WriteOptions.sync 决定是否在 Batch 结束后调用 Sync。
      // 但这里必须 Flush，确保数据进入内核缓冲区。
      s = dest_->Flush();
    }
  }

  // 5. 更新 Block 偏移量
  block_offset_ += kHeaderSize + length;
  return s;
}

}  // namespace log
}  // namespace titankv