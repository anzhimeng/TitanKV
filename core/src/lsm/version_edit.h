#pragma once
#include <vector>
#include <string>
#include <set>
#include <utility>
#include "lsm/dbformat.h"
#include "titankv/status.h"

namespace titankv {

class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() = default;

  void Clear();

  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }

  // 添加新文件 (Level, FileNum, FileSize, Smallest, Largest)
  void AddFile(int level, uint64_t file, uint64_t file_size,
               const Slice& smallest, const Slice& largest);
               
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  // Getter (简单起见直接公开或者友元)
  friend class VersionSet;

 private:
  // Tag 用于序列化
  enum Tag {
    kLogNumber = 1,
    kNextFileNumber = 2,
    kLastSequence = 3,
    kNewFile = 4,
    kDeletedFile = 5,
  };

  bool has_log_number_;
  uint64_t log_number_;

  bool has_next_file_number_;
  uint64_t next_file_number_;
  // 【新增】记录被删除的文件 <level, file_number>
  std::set<std::pair<int, uint64_t>> deleted_files_;
  // <level, file_meta>
  std::vector<std::pair<int, FileMetaData>> new_files_;
};

} // namespace titankv