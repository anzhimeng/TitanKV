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
  // 【新增】设置 LastSequence
  void SetLastSequence(uint64_t seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }

  // 添加新文件 (Level, FileNum, FileSize, Smallest, Largest)
  void AddFile(int level, uint64_t file, uint64_t file_size,
               const Slice& smallest, const Slice& largest);
               
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }
  // 【新增】检查文件是否被标记删除
  bool IsDeleted(int level, uint64_t file_number) const {
      return deleted_files_.count(std::make_pair(level, file_number)) > 0;
  }

  // 【新增】暴露新文件列表供 DBImpl 清理 pending_outputs_
  const std::vector<std::pair<int, FileMetaData>>& GetNewFiles() const {
      return new_files_;
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

  // 【新增】LastSequence 成员
  bool has_last_sequence_;
  uint64_t last_sequence_;
  // 【新增】记录被删除的文件 <level, file_number>
  std::set<std::pair<int, uint64_t>> deleted_files_;
  // <level, file_meta>
  std::vector<std::pair<int, FileMetaData>> new_files_;
};

} // namespace titankv