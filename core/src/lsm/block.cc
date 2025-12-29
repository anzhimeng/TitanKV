#include "lsm/block.h"
#include "util/coding.h"
#include <vector>
#include <algorithm>

namespace titankv {

// 辅助函数：解析 Helper
static inline const char* DecodeEntry(const char* p, const char* limit,
                                      uint32_t* shared, uint32_t* non_shared,
                                      uint32_t* value_length) {
  if (limit - p < 3) return nullptr;
  *shared = static_cast<uint8_t>(p[0]);
  *non_shared = static_cast<uint8_t>(p[1]);
  *value_length = static_cast<uint8_t>(p[2]);
  if ((*shared | *non_shared | *value_length) < 128) {
    p += 3;
  } else {
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
    size_ = 0; 
  } else {
    size_t max_restarts_allowed = (size_ - sizeof(uint32_t)) / sizeof(uint32_t);
    uint32_t num_restarts = DecodeFixed32(data_ + size_ - sizeof(uint32_t));
    if (num_restarts > max_restarts_allowed) {
      size_ = 0; 
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

  // 【完善】实现了真正的 Prev 逻辑
  void Prev() override {
    assert(Valid());

    // 扫描直到 current_ 之前的位置
    const uint32_t original = current_;
    while (GetRestartPoint(restart_index_) >= original) {
      if (restart_index_ == 0) {
        // 到头了
        current_ = restart_offset_;
        restart_index_ = num_restarts_;
        return;
      }
      restart_index_--;
    }

    SeekToRestartPoint(restart_index_);
    
    // 线性扫描直到下一个就是 original
    while (NextEntryOffset() < original) {
      ParseNextKey();
    }
  }

  void Seek(const Slice& target) override {
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
      if (!Valid()) return;
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
    // 【关键修复】确保 NextEntryOffset 计算正确
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
  
  return new BlockIterator(comparator, data_, restart_offset_, num_restarts);
}

} // namespace titankv