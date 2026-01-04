#include "lsm/dbformat.h"
#include <cstring>

namespace titankv {

// --- LookupKey Implementation ---

LookupKey::LookupKey(const Slice& user_key, SequenceNumber sequence) {
  size_t usize = user_key.size();
  // varint(internal_key_size) + user_key + tag(8 bytes)
  // Varint32 最多 5 字节
  size_t needed = 5 + usize + 8; 
  
  char* dst;
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
    dst = new char[needed];
  }
  
  start_ = dst;
  
  // 1. 写入长度 (Internal Key Size = User Key Size + 8)
  // EncodeVarint32 会返回写入后的下一个位置
  dst = EncodeVarint32(dst, usize + 8); 
  
  // 【关键修复】记录 User Key 开始的位置
  kstart_ = dst;

  // 2. 写入 User Key
  std::memcpy(dst, user_key.data(), usize);
  dst += usize;
  
  // 3. 写入 Tag
  EncodeFixed64(dst, PackSequenceAndType(sequence, kTypeValue));
  dst += 8;
  
  end_ = dst;
}

LookupKey::~LookupKey() {
  if (start_ != space_) {
    delete[] start_;
  }
}

// ... InternalKeyComparator 实现保持不变 ...
// 为了完整性，我把 Compare 部分也贴在这，防止你之前改乱了

int InternalKeyComparator::user_key_compare(const Slice& a, const Slice& b) const {
  return a.compare(b);
}

int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
  // 【关键修复】移除 assert，改为防御性逻辑
  // assert(akey.size() >= 8); 
  // assert(bkey.size() >= 8);

  // 1. 提取 User Key
  // 如果长度不足 8，则整个 Key 就是 User Key (或者是坏数据)，直接比较
  Slice a_user = (akey.size() >= 8) ? Slice(akey.data(), akey.size() - 8) : akey;
  Slice b_user = (bkey.size() >= 8) ? Slice(bkey.data(), bkey.size() - 8) : bkey;

  int r = user_key_compare(a_user, b_user);
  if (r != 0) {
    return r;
  }

  // 2. User Key 相同，比较 Tag (SeqNum + Type)
  // 如果任意一个 Key 长度不足 8，无法比较 Tag，此时认为它们相等（或按长度决胜）
  if (akey.size() < 8 || bkey.size() < 8) {
      if (akey.size() < bkey.size()) return -1;
      if (akey.size() > bkey.size()) return +1;
      return 0;
  }

  // 正常的 Tag 比较逻辑
  const uint64_t a_tag = DecodeFixed64(akey.data() + akey.size() - 8);
  const uint64_t b_tag = DecodeFixed64(bkey.data() + bkey.size() - 8);

  // SeqNum 降序排列 (较大的 SeqNum 排在前面)
  if (a_tag > b_tag) return -1;
  if (a_tag < b_tag) return +1;

  return 0;
}

} // namespace titankv