#pragma once
#include <string>
#include <vector>
#include <memory>
#include <mutex>        
#include "lsm/dbformat.h"
#include "titankv/options.h"
#include "titankv/status.h"
#include "util/env.h"     
#include "wal/log_writer.h" 
namespace titankv {

class VersionEdit;

struct FileMetaData {
  uint32_t refs = 0;
  int allowed_seeks = 1 << 30; // 用于读放大优化，暂不使用
  uint64_t file_number = 0;
  uint64_t file_size = 0;
  
  // 该文件中最小和最大的 InternalKey
  // 用于读请求过滤：如果 target < smallest 或 target > largest，直接跳过此文件
  std::string smallest;
  std::string largest;

  FileMetaData() {}
};

// 简单的 Version 类，管理 L0 文件列表
// 生产环境会有 Level 1~6，Day 4 我们只实现 L0 (Tiering 模式)
class Version {
 public:
  // 追加新文件到 L0
  void AddFile(int level, uint64_t file_number, uint64_t file_size,
               const Slice& smallest, const Slice& largest) {
      // Day 4 忽略 level，全部视为 L0
      (void)level;
      FileMetaData* f = new FileMetaData();
      f->file_number = file_number;
      f->file_size = file_size;
      f->smallest = smallest.ToString();
      f->largest = largest.ToString();
      files_.push_back(f);
  }

  void Ref() { ++refs_; }
  void Unref() {
      assert(refs_ >= 1);
      --refs_;
      if (refs_ == 0) delete this;
  }
  const std::vector<FileMetaData*>& GetFiles() const { return files_; }

 private:
  friend class VersionSet;
  ~Version() {
      for (auto* f : files_) delete f;
  }
  std::vector<FileMetaData*> files_;
  int refs_ = 0;
};

class VersionSet {
 public:
  VersionSet(const std::string& dbname, const Options& options);
  ~VersionSet();

  // 这里的 LogAndApply 是核心
  // 将 edit 应用到当前版本，生成新版本，并写入 MANIFEST
  Status LogAndApply(VersionEdit* edit, std::mutex* mu);

  // 恢复
  Status Recover(bool* save_manifest);

  Version* current() const { return current_; }
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }
  uint64_t NewFileNumber() { return next_file_number_++; }
  uint64_t LogNumber() const { return log_number_; }

 private:
  std::string dbname_;
  const Options options_;
  uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  uint64_t log_number_;

  Version* current_; // 当前版本 (Link List Head)
  
  // Manifest 写入器
  std::unique_ptr<WritableFile> manifest_file_;
  std::unique_ptr<log::Writer> manifest_log_;

  // 辅助：将 edit 应用到 base 生成新 Version
  class Builder; // 前置声明
  void AppendVersion(Version* v);
};

} // namespace titankv