#pragma once
#include <string>
#include <vector>
#include <memory>
#include <mutex>        
#include <atomic>
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

class Version {
 public:
  Version() : refs_(0) {} // 初始化

  // 追加新文件到 L0
  void AddFile(int level, uint64_t file_number, uint64_t file_size,
               const Slice& smallest, const Slice& largest) {
      (void)level;
      FileMetaData* f = new FileMetaData();
      f->file_number = file_number;
      f->file_size = file_size;
      f->smallest = smallest.ToString();
      f->largest = largest.ToString();
      files_.push_back(f);
  }

  // 【关键修复】析构函数必须是私有的，或者确保只有 Unref 能调用
  // 这里保持原样，但在 Unref 中 delete
  ~Version() {
      for (auto* f : files_) delete f;
  }

  // 【关键修复】使用原子操作
  void Ref() {
      refs_.fetch_add(1, std::memory_order_relaxed);
  }

  // 【关键修复】使用原子操作
  void Unref() {
      // fetch_sub 返回修改前的值。如果修改前是 1，减完就是 0。
      if (refs_.fetch_sub(1, std::memory_order_release) == 1) {
          std::atomic_thread_fence(std::memory_order_acquire);
          delete this;
      }
  }

  const std::vector<FileMetaData*>& GetFiles() const { return files_; }

 private:
  friend class VersionSet;
  
  std::vector<FileMetaData*> files_;
  
  // 【关键修复】从 int 改为 std::atomic<int>
  std::atomic<int> refs_; 
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