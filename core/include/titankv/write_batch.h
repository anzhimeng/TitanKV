#pragma once
#include <string>
#include <vector>
#include "titankv/slice.h"

namespace titankv {

class WriteBatch {
 public:
  WriteBatch() = default;
  ~WriteBatch() = default;

  void Put(const Slice& key, const Slice& value);
  void Delete(const Slice& key);
  void Clear();

  // 序列化后的数据 (用于写入 WAL)
  // 格式: [Count] [Type KeyLen Key ValLen Val] ...
  std::string Encode() const;
  
  // 内部访问 (供 DBImpl 使用)
  struct Entry {
      char type; // kTypeValue or kTypeDeletion
      std::string key;
      std::string value;
  };
  const std::vector<Entry>& entries() const { return entries_; }

 private:
  std::vector<Entry> entries_;
};

} // namespace titankv