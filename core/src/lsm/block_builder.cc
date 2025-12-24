#include "lsm/block_builder.h"
#include "util/coding.h"
#include <algorithm>
#include <cassert>

namespace titankv {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options), restarts_(), counter_(0), finished_(false) {
  assert(options_->block_restart_interval >= 1);
  restarts_.push_back(0); // 第一个 Key 总是重启点
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
  // Raw Data + Restart Array + Restart Count
  return buffer_.size() + restarts_.size() * sizeof(uint32_t) + sizeof(uint32_t);
}

Slice BlockBuilder::Finish() {
  // 追加 Restart Points 数组
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }
  // 追加 Restart Points 数量
  PutFixed32(&buffer_, restarts_.size());
  
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_piece(last_key_);
  assert(!finished_);
  assert(counter_ <= options_->block_restart_interval);
  
  // 简单检查顺序 (生产环境应该用 Comparator)
  // assert(buffer_.empty() || key.compare(last_key_piece) > 0);

  size_t shared = 0;
  
  // 1. 计算共享前缀
  // 如果 counter_ < interval，说明在两个重启点之间，可以做前缀压缩
  if (counter_ < options_->block_restart_interval) {
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
    // 达到间隔，强制 restart
    // shared = 0，存储完整 Key
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }

  const size_t non_shared = key.size() - shared;

  // 2. 写入 Entry
  // 格式: <shared><non_shared><value_len><key_delta><value>
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());
  
  // 写入 Key 后缀
  buffer_.append(key.data() + shared, non_shared);
  
  // 写入 Value
  buffer_.append(value.data(), value.size());

  // 3. 更新状态
  // 更新 last_key_ 为当前 key
  // 优化：只更新变动的部分
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);
  
  counter_++;
}

} // namespace titankv