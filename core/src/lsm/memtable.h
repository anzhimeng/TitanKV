#pragma once

#include "lsm/dbformat.h"
#include "lsm/skiplist.h"
#include "util/arena.h"
#include "titankv/status.h"
#include <string>

namespace titankv {

class MemTable {
 public:
  // MemTable 也是引用计数的，因为 Flush 到磁盘时可能有读请求在访问它
  MemTable(const InternalKeyComparator& comparator);
  
  // 禁止拷贝
  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  // 增加引用计数
  void Ref() { ++refs_; }
  
  // 减少引用计数，为 0 时自杀
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  // 估算内存占用 (用于决定是否 Flush)
  size_t ApproximateMemoryUsage();

  // 核心写接口
  // value 通常是 BlobIndex 的序列化数据
  void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value);

  // 核心读接口
  // 如果找到，status 设为 OK，value 设为找到的值
  // 如果找到的是删除标记，status 设为 NotFound
  bool Get(const LookupKey& key, std::string* value, Status* s);

  // 迭代器 (用于 Flush)
  // Iterator* NewIterator();

 private:
  // 私有析构，只能通过 Unref 删除
  ~MemTable();

  // SkipList 需要一个比较器适配器
  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    
    // SkipList 里的 Key 是带有长度前缀的 (Varint Len + InternalKey)
    int operator()(const char* a, const char* b) const;
  };

  typedef SkipList<const char*, KeyComparator> Table;

  InternalKeyComparator comparator_;
  int refs_;
  Arena arena_;
  Table table_;
};

} // namespace titankv