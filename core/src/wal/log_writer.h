#pragma once

#include <cstdint>
#include "titankv/slice.h"
#include "titankv/status.h"
#include "util/env.h"
#include "wal/log_format.h"

namespace titankv {
namespace log {

class Writer {
 public:
  // 创建一个 Writer，写入到 dest 文件
  // dest 必须由调用者负责释放
  explicit Writer(WritableFile* dest);

  // 写入一条逻辑记录
  // 数据会被自动切分为一个或多个物理 Fragment
  Status AddRecord(const Slice& slice);

 private:
  WritableFile* dest_;
  int block_offset_;  // 当前 Block 内的写入偏移量 (0 ~ kBlockSize-1)

  // 预先计算好的类型 CRC，用于加速计算
  // crc32c(type)
  uint32_t type_crc_[kMaxRecordType + 1];

  // 发射物理记录
  // type: 记录类型 (Full, First, Middle, Last)
  // ptr: 数据指针
  // length: 数据长度
  Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

  // 禁止拷贝
  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;
};

}  // namespace log
}  // namespace titankv