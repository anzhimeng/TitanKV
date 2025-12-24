#pragma once

#include <cstdint>
#include <string>
#include "titankv/slice.h"
#include "titankv/status.h"
#include "util/env.h"
#include "wal/log_format.h"

namespace titankv {
namespace log {

class Reporter {
 public:
  virtual ~Reporter() = default;
  virtual void Corruption(size_t bytes, const Status& status) = 0;
};

class Reader {
 public:
  // file: 顺序文件接口
  // reporter: 错误报告回调 (可为 nullptr)
  // checksum: 是否开启 CRC 校验
  // initial_offset: 从哪个物理位置开始读取日志
  Reader(SequentialFile* file, Reporter* reporter, bool checksum,
         uint64_t initial_offset);

  ~Reader();

  bool ReadRecord(Slice* record, std::string* scratch);

  uint64_t LastRecordOffset();

 private:
  SequentialFile* const file_;
  Reporter* const reporter_;
  bool const checksum_;
  char* const backing_store_;
  Slice buffer_;
  bool eof_;

  // 记录上一条成功读取的记录的起始偏移量
  uint64_t last_record_offset_;
  // 当前 buffer 结束位置在文件中的物理偏移量
  uint64_t end_of_buffer_offset_;
  // 用户要求的初始偏移量
  uint64_t const initial_offset_;
  // 是否处于“重新同步”状态 (寻找 initial_offset 之后的第一条有效记录)
  bool resyncing_;

  enum {
    kEof = kMaxRecordType + 1,
    kBadRecord = kMaxRecordType + 2
  };

  // 跳过 initial_offset 之前的 Block
  bool SkipToInitialBlock();
  
  // 读取物理记录，处理 Block 边界和 CRC
  unsigned int ReadPhysicalRecord(Slice* result);

  void ReportCorruption(uint64_t bytes, const char* reason);
  void ReportDrop(uint64_t bytes, const Status& reason);
};

} // namespace log
} // namespace titankv