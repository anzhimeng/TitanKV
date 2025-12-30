#pragma once
#include <cstddef>
#include <cstdint>
#include "titankv/slice.h"
#include "titankv/status.h"
#include "lsm/dbformat.h" // 包含 Comparator

namespace titankv {

class Iterator {
 public:
  virtual ~Iterator() = default;
  virtual bool Valid() const = 0;
  virtual void SeekToFirst() = 0;
  virtual void SeekToLast() = 0;
  virtual void Seek(const Slice& target) = 0;
  virtual void Next() = 0;
  virtual void Prev() = 0;
  virtual Slice key() const = 0;
  virtual Slice value() const = 0;
  virtual Status status() const { return Status::OK(); }
};


struct BlockContents {
  Slice data;           // 实际数据
  bool heap_allocated;  // 是否需要 delete[] data.data()
};

class Block {
 public:
  // 初始化 Block，contents 包含数据+Restart数组
  explicit Block(const BlockContents& contents);

  ~Block();

  size_t size() const { return size_; }
  const char* data() const { return data_; } 
  
  // 创建迭代器
  Iterator* NewIterator(const UserKeyComparator* comparator);

 private:
  const char* data_;
  size_t size_;
  uint32_t restart_offset_; // Restart 数组的起始偏移量
  bool owned_;              // 是否拥有 data_ 的所有权

  // 禁止拷贝
  Block(const Block&) = delete;
  Block& operator=(const Block&) = delete;
};

} // namespace titankv