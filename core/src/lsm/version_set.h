#pragma once
#include <vector>
#include <string>
#include <atomic>
#include <functional>
#include <mutex>         
#include <memory>      
#include "titankv/slice.h"
#include "titankv/status.h"
#include "titankv/options.h"
#include "lsm/dbformat.h" 
#include "lsm/version_edit.h"
#include "lsm/two_level_iterator.h"
#include "util/env.h"   
#include "wal/log_writer.h"

namespace titankv {

class TableCache;
class VersionSet;
class Compaction;

class Version {
 public:
  explicit Version(VersionSet* vset) : vset_(vset), refs_(0) {}

  ~Version(); // 在 .cc 中实现

  // 【修改】只保留声明，去掉了花括号里的实现
  void AddFile(int level, uint64_t file_number, uint64_t file_size,
               const Slice& smallest, const Slice& largest);

  // 核心读取接口
  Status Get(const ReadOptions& options, const LookupKey& key, std::string* val,
             bool* found, TableCache* table_cache,
             std::function<Status(const Slice&, std::string*)> blob_getter);

  void Ref() { refs_.fetch_add(1, std::memory_order_relaxed); }
  
  void Unref() {
      if (refs_.fetch_sub(1, std::memory_order_release) == 1) {
          std::atomic_thread_fence(std::memory_order_acquire);
          delete this;
      }
  }

  // GetFiles 现在需要 level 参数
  const std::vector<FileMetaData*>& GetFiles(int level) const {
      return files_[level];
  }
  // 计算每一层的 Score
  void Finalize();

  // 访问器
  double compaction_score() const { return compaction_score_; }
  int compaction_level() const { return compaction_level_; }

  void GetOverlappingInputs(
  	 int level,
      const Slice& begin,
      const Slice& end,
      std::vector<FileMetaData*>* inputs);
  // 检查 level+1 层及以下是否包含 user_key
  bool OverlapInLevel(int level, const Slice& user_key, const Slice& internal_key) const;

 private:
  friend class VersionSet;
  VersionSet* vset_; 
  std::vector<FileMetaData*> files_[kNumLevels];
  std::atomic<int> refs_;
  double compaction_score_ = -1;
  int compaction_level_ = -1;
};

class VersionSet {
public:
    VersionSet(const std::string& dbname, const Options& options);
    ~VersionSet();

    Status LogAndApply(VersionEdit* edit, std::mutex* mu);
    Status Recover(bool* save_manifest);

    Version* current() const { return current_; }
    uint64_t ManifestFileNumber() const { return manifest_file_number_; }
    uint64_t NewFileNumber() { return next_file_number_++; }
    uint64_t LogNumber() const { return log_number_; }
    
    const InternalKeyComparator* icmp() const { return &icmp_; }
    Iterator* MakeInputIterator(Compaction* c, TableCache* table_cache, const ReadOptions& options);

    Compaction* PickCompaction();
    void AddLiveFiles(std::set<uint64_t>* live);

private:
    std::string dbname_;
    const Options options_;
    uint64_t next_file_number_;
    uint64_t manifest_file_number_;
    uint64_t log_number_;
    
    InternalKeyComparator icmp_; // 必须有这个

    Version* current_;
    
    std::unique_ptr<WritableFile> manifest_file_;
    std::unique_ptr<log::Writer> manifest_log_;

    // 记录每一层上一次合并结束的 Key (Largest Key)
    std::string compact_pointer_[kNumLevels];  

    class Builder;
    void AppendVersion(Version* v);
};

    Iterator* NewLevelIterator(const InternalKeyComparator& icmp,
	                      TableCache* table_cache,
	                      const std::vector<FileMetaData*>& files,
	                      const ReadOptions& options);

} // namespace titankv