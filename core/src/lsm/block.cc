#include "lsm/block.h"
#include "util/coding.h"
#include <vector>
#include <algorithm>

namespace titankv {

// 辅助函数：解析 Helper
static inline const char* DecodeEntry(const char* p, const char* limit,
                                      uint32_t* shared, uint32_t* non_shared,
                                      uint32_t* value_length) {
  if (limit - p < 3) return nullptr; // 至少 3 个字节
  *shared = static_cast<uint8_t>(p[0]);
  *non_shared = static_cast<uint8_t>(p[1]);
  *value_length = static_cast<uint8_t>(p[2]);
  if ((*shared | *non_shared | *value_length) < 128) {
    // 快速路径：所有长度都 < 128 (1字节)
    p += 3;
  } else {
    // 慢速路径：完整的 Varint 解析
    if ((p = GetVarint32Ptr(p, limit, shared)) == nullptr) return nullptr;
    if ((p = GetVarint32Ptr(p, limit, non_shared)) == nullptr) return nullptr;
    if ((p = GetVarint32Ptr(p, limit, value_length)) == nullptr) return nullptr;
  }
  return p;
}

Block::Block(const BlockContents& contents)
    : data_(contents.data.data()),
      size_(contents.data.size()),
      owned_(contents.heap_allocated) {
  if (size_ < sizeof(uint32_t)) {
    size_ = 0;  // 错误：数据太小
  } else {
    // 读取最后的 restart_count
    // restart_array 位于: total_size - sizeof(uint32) - (count * 4)
    size_t max_restarts_allowed = (size_ - sizeof(uint32_t)) / sizeof(uint32_t);
    uint32_t num_restarts = DecodeFixed32(data_ + size_ - sizeof(uint32_t));
    if (num_restarts > max_restarts_allowed) {
      size_ = 0; // 错误：restart count 太大
    } else {
      restart_offset_ = size_ - (1 + num_restarts) * sizeof(uint32_t);
    }
  }
}

Block::~Block() {
  if (owned_) {
    delete[] data_;
  }
}

// --- Block Iterator ---

class BlockIterator : public Iterator {
 public:
  BlockIterator(const UserKeyComparator* comparator, const char* data,
                uint32_t restart_offset, uint32_t num_restarts)
      : comparator_(comparator),
        data_(data),
        restart_offset_(restart_offset),
        num_restarts_(num_restarts),
        current_(restart_offset_),
        restart_index_(num_restarts_) {
    assert(num_restarts_ > 0);
  }

  bool Valid() const override { return current_ < restart_offset_; }
  
  Slice key() const override {
    assert(Valid());
    return key_;
  }
  
  Slice value() const override {
    assert(Valid());
    return value_;
  }

  void Next() override {
    assert(Valid());
    ParseNextKey();
  }

  void Prev() override {
    // 简单实现 Prev：二分查找找到比当前 key 小的最后一个
    // 这里的实现比较复杂，Week 2 Day 3 可以先留空或者抛出 NotSupported
    // 为了编译通过，我们先 Seek 到当前 Key 之前
    // 实际生产中 Prev 效率较低，通常通过 Restart Point 回溯实现
    const Slice target = key();
    Seek(target); // 这是一个 stub，实际 Prev 逻辑很长
    // 暂时留空或者后续补充
  }

  void Seek(const Slice& target) override {
    // 1. 二分查找 Restart Points
    uint32_t left = 0;
    uint32_t right = num_restarts_ - 1;
    
    while (left < right) {
      uint32_t mid = (left + right + 1) / 2;
      uint32_t region_offset = GetRestartPoint(mid);
      
      uint32_t shared, non_shared, val_len;
      const char* key_ptr = DecodeEntry(data_ + region_offset, data_ + restart_offset_,
                                        &shared, &non_shared, &val_len);
      if (key_ptr == nullptr || shared != 0) {
        CorruptionError();
        return;
      }
      
      Slice mid_key(key_ptr, non_shared);
      if (comparator_->Compare(mid_key, target) < 0) {
        left = mid;
      } else {
        right = mid - 1;
      }
    }

    SeekToRestartPoint(left);
    
    while (true) {
      if (!Valid())
      {
      	fprintf(stderr, "[BlockIter] Seek Hit End. Target: %s\n", target.ToString().c_str());
      	return;
      }
      fprintf(stderr, "[BlockIter] Scanning Key: %s\n", key_.c_str());
      if (comparator_->Compare(key_, target) >= 0) {
        return;
      }
      ParseNextKey();
    }
  }

  void SeekToFirst() override {
    SeekToRestartPoint(0);
    ParseNextKey();
  }

  void SeekToLast() override {
    SeekToRestartPoint(num_restarts_ - 1);
    while (Valid() && NextEntryOffset() < restart_offset_) {
      ParseNextKey();
    }
  }

 private:
  const UserKeyComparator* comparator_;
  const char* data_;
  uint32_t restart_offset_;
  uint32_t num_restarts_;
  
  uint32_t current_;
  uint32_t restart_index_;
  std::string key_;
  Slice value_;

  uint32_t GetRestartPoint(uint32_t index) {
    assert(index < num_restarts_);
    return DecodeFixed32(data_ + restart_offset_ + index * sizeof(uint32_t));
  }

  void SeekToRestartPoint(uint32_t index) {
    key_.clear();
    restart_index_ = index;
    uint32_t offset = GetRestartPoint(index);
    current_ = offset;
    value_ = Slice(data_ + offset, 0); 
  }

  void ParseNextKey() {
    current_ = NextEntryOffset();
    const char* p = data_ + current_;
    const char* limit = data_ + restart_offset_; 
    if (p >= limit) {
      current_ = restart_offset_;
      restart_index_ = num_restarts_;
      return;
    }

    uint32_t shared, non_shared, value_length;
    p = DecodeEntry(p, limit, &shared, &non_shared, &value_length);
    if (p == nullptr || key_.size() < shared) {
      CorruptionError();
      return;
    }

    key_.resize(shared);
    key_.append(p, non_shared);
    value_ = Slice(p + non_shared, value_length);
  }
  
  uint32_t NextEntryOffset() const {
    return (value_.data() + value_.size()) - data_;
  }

  void CorruptionError() {
    current_ = restart_offset_;
    restart_index_ = num_restarts_;
    key_.clear();
    value_ = Slice(data_ + restart_offset_, 0);
  }
};

Iterator* Block::NewIterator(const UserKeyComparator* comparator) {
  if (size_ < sizeof(uint32_t)) {
    return nullptr;
  }
  uint32_t num_restarts = DecodeFixed32(data_ + size_ - sizeof(uint32_t));
  if (num_restarts == 0) {
    return nullptr;
  }
  
  // 这里 new 的是 BlockIterator，但返回类型是 Iterator* (多态)
  return new BlockIterator(comparator, data_, restart_offset_, num_restarts);
}

} // namespace titankv