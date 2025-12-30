#pragma once

#include <memory>
#include "util/env.h"
#include "blob/blob_format.h"
#include "titankv/status.h"
#include "titankv/slice.h"

namespace titankv {

class BlobWriter {
 public:
  explicit BlobWriter(std::unique_ptr<WritableFile> file);

  Status AddRecord(const Slice& key, const Slice& value);

  uint64_t FileSize() const { return file_size_; }

 private:
  std::unique_ptr<WritableFile> file_;
  uint64_t file_size_;
};

// 在 BlobWriter 下面添加 BlobFileIterator
class BlobFileIterator {
 public:
  // file: 必须是打开的 RandomAccessFile
  // file_number: 仅用于构造 BlobIndex
  // file_size: 文件大小，用于判断结束
  BlobFileIterator(RandomAccessFile* file, uint64_t file_number, uint64_t file_size)
      : file_(file), 
        file_number_(file_number), 
        file_size_(file_size),
        current_offset_(0),
        valid_(false) {}

  // 移动到下一个 Record
  void Next();
  
  // 检查当前是否有效
  bool Valid() const { return valid_; }

  // 获取当前 Record 的 Key/Value
  Slice key() const { return current_record_.key; }
  Slice value() const { return current_record_.value; }

  // 获取当前 Record 的 BlobIndex (用于和 LSM 对比)
  BlobIndex GetBlobIndex() const;
  
  Status status() const { return status_; }

 private:
  RandomAccessFile* file_;
  uint64_t file_number_;
  uint64_t file_size_;
  uint64_t current_offset_;
  uint64_t record_offset_;
  
  bool valid_;
  Status status_;
  
  // 缓存当前的 Key/Value 数据 (持有内存)
  std::string buffer_; 
  ParsedBlobRecord current_record_; // 指向 buffer_
};

}