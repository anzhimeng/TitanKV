#pragma once
#include <string>
#include <cstdint>
#include "titankv/slice.h"
#include "titankv/status.h"
#include "util/coding.h"

namespace titankv {

// BlockHandle 是一个指向文件的指针：偏移量 + 长度
// 编码后用于 Index Block，指向 Data Block
class BlockHandle {
 public:
  BlockHandle();

  // 偏移量
  uint64_t offset() const { return offset_; }
  void set_offset(uint64_t offset) { offset_ = offset; }

  // 大小
  uint64_t size() const { return size_; }
  void set_size(uint64_t size) { size_ = size; }

  // 序列化/反序列化 (Varint64)
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

  // 最大编码长度 (Varint64 * 2)
  enum { kMaxEncodedLength = 10 + 10 };

 private:
  uint64_t offset_;
  uint64_t size_;
};

// Footer 位于 SSTable 文件的末尾 (固定长度 48 字节)
// 它包含指向 Metaindex Block 和 Index Block 的 Handle
class Footer {
 public:
  Footer() {}

  // 这里的 Handle 指向 Index Block (存 Key -> DataBlock 的索引)
  const BlockHandle& index_handle() const { return index_handle_; }
  void set_index_handle(const BlockHandle& h) { index_handle_ = h; }

  // 序列化/反序列化
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

  // 编码后的固定长度
  // 2 * BlockHandle::kMaxEncodedLength + 8 (Magic Number)
  // 为了简单，LevelDB 实际上使用了 Padding 凑整，这里我们也简化处理
  // 实际上我们用定长编码 Footer，方便 Seek 到文件末尾读取
  enum { kEncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8 };

 private:
  BlockHandle index_handle_;
  // BlockHandle metaindex_handle_; // 暂时不需要 Filter Block，先留空
};

// Magic Number (用于校验文件类型)
static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;

// --- Implementation ---

inline BlockHandle::BlockHandle() : offset_(~static_cast<uint64_t>(0)), size_(0) {}

inline void BlockHandle::EncodeTo(std::string* dst) const {
  // Varint 编码节省索引空间
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

inline Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
    return Status::OK();
  }
  return Status::Corruption("bad block handle");
}

inline void Footer::EncodeTo(std::string* dst) const {
  const size_t original_size = dst->size();
  index_handle_.EncodeTo(dst);
  // Padding 到固定长度，方便读取
  dst->resize(original_size + 2 * BlockHandle::kMaxEncodedLength); 
  PutFixed64(dst, kTableMagicNumber);
}

inline Status Footer::DecodeFrom(Slice* input) {
  const char* magic_ptr = input->data() + kEncodedLength - 8;
  const uint64_t magic = DecodeFixed64(magic_ptr);
  if (magic != kTableMagicNumber) {
    return Status::Corruption("not an sstable (bad magic number)");
  }

  Status result = index_handle_.DecodeFrom(input);
  if (result.ok()) {
    // 忽略 Padding
  }
  return result;
}

} // namespace titankv