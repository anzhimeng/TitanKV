#include "lsm/version_edit.h"
#include "util/coding.h"

namespace titankv {

void VersionEdit::Clear() {
  has_log_number_ = false;
  log_number_ = 0;
  has_next_file_number_ = false;
  next_file_number_ = 0;
  deleted_files_.clear();
  new_files_.clear();
}

void VersionEdit::AddFile(int level, uint64_t file, uint64_t file_size,
                          const Slice& smallest, const Slice& largest) {
  FileMetaData f;
  f.file_number = file;
  f.file_size = file_size;
  // Slice 转 string 存储
  f.smallest = smallest.ToString(); 
  f.largest = largest.ToString();
  new_files_.push_back(std::make_pair(level, f));
}
void VersionEdit::EncodeTo(std::string* dst) const {
  if (has_log_number_) {
    PutVarint32(dst, kLogNumber);
    PutVarint64(dst, log_number_);
  }
  if (has_next_file_number_) {
    PutVarint32(dst, kNextFileNumber);
    PutVarint64(dst, next_file_number_);
  }
  for (const auto& del : deleted_files_) {
    PutVarint32(dst, kDeletedFile);
    PutVarint32(dst, del.first);  // Level
    PutVarint64(dst, del.second); // FileNum
  }

  
  for (const auto& nf : new_files_) {
    PutVarint32(dst, kNewFile);
    PutVarint32(dst, nf.first); // Level
    PutVarint64(dst, nf.second.file_number);
    PutVarint64(dst, nf.second.file_size);
    
    // 写入 Smallest / Largest (带长度前缀)
    Slice smallest(nf.second.smallest);
    PutVarint32(dst, smallest.size());
    dst->append(smallest.data(), smallest.size());
    
    Slice largest(nf.second.largest);
    PutVarint32(dst, largest.size());
    dst->append(largest.data(), largest.size());
  }
}

Status VersionEdit::DecodeFrom(const Slice& src) {
  Clear();
  Slice input = src;
  const char* msg = nullptr;
  uint32_t tag;
  
  // 循环解析直到结束
  while (msg == nullptr && GetVarint32(&input, &tag)) {
    switch (tag) {
      case kLogNumber:
        if (GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {
          msg = "log number";
        }
        break;
      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;
      // 【新增】反序列化删除记录
      case kDeletedFile: {
          uint32_t level;
          uint64_t file_num;
          if (GetVarint32(&input, &level) && GetVarint64(&input, &file_num)) {
              DeleteFile(level, file_num);
          } else {
              msg = "deleted file";
          }
          break;
      }
      case kNewFile: {
        uint32_t level;
        FileMetaData f;
        uint64_t number;
        uint64_t file_size;
        Slice smallest, largest;
        if (GetVarint32(&input, &level) &&
            GetVarint64(&input, &number) &&
            GetVarint64(&input, &file_size) &&
            GetLengthPrefixedSlice(&input, &smallest) && // 需实现
            GetLengthPrefixedSlice(&input, &largest)) {
            
            f.file_number = number;
            f.file_size = file_size;
            f.smallest = smallest.ToString();
            f.largest = largest.ToString();
            new_files_.push_back(std::make_pair(level, f));
        } else {
            msg = "new-file entry";
        }
        break;
      }
      default:
        msg = "unknown tag";
        break;
    }
  }

  if (msg == nullptr && !input.empty()) {
    msg = "invalid tag";
  }

  if (msg != nullptr) {
    return Status::Corruption("VersionEdit", msg);
  }
  return Status::OK();
}

} // namespace titankv