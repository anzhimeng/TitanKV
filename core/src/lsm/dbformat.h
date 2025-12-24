#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include "titankv/slice.h"
#include "util/coding.h"

namespace titankv {

typedef uint64_t SequenceNumber;

enum ValueType : unsigned char {
  kTypeDeletion = 0x0,
  kTypeValue = 0x1
};

static const SequenceNumber kMaxSequenceNumber = ((0x1ull << 56) - 1);

inline uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  return (seq << 8) | t;
}

class LookupKey {
 public:
  LookupKey(const Slice& user_key, SequenceNumber sequence);

  ~LookupKey();

  // 返回完整的 MemTable Key (包含 Varint 长度前缀)
  Slice memtable_key() const { return Slice(start_, end_ - start_); }

  // 返回 Internal Key (UserKey + Tag, 不含长度前缀)
  Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }

  // 返回 User Key
  Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }

 private:
  const char* start_;  // 内存块起始位置 (Varint Len 开始)
  const char* kstart_; // 【新增】Key 数据起始位置 (User Key 开始)
  const char* end_;    // 结束位置
  char space_[200]; 
};

class InternalKeyComparator {
 public:
  int Compare(const Slice& a, const Slice& b) const;
  int user_key_compare(const Slice& a, const Slice& b) const;
  const char* Name() const { return "titankv.InternalKeyComparator"; }
};

} // namespace titankv