#include "lsm/version_set.h"
#include "lsm/version_edit.h"
#include "util/filename.h"
#include "wal/log_reader.h"
#include <cstdio>

namespace titankv {

VersionSet::VersionSet(const std::string& dbname, const Options& options)
    : dbname_(dbname),
      options_(options),
      next_file_number_(2),
      manifest_file_number_(0),
      log_number_(0),
      current_(new Version()) {
  current_->Ref();
}

VersionSet::~VersionSet() {
  current_->Unref();
}

void VersionSet::AppendVersion(Version* v) {
  v->Ref(); // 先增加新版本的引用
  
  // 原子替换 current_
  // 注意：current_ 是普通指针，标准 C++ 对普通指针赋值是原子的（在 x64 上），
  // 但为了绝对安全，可以用 std::atomic_exchange 或者手动加锁保护 current_ 的切换。
  // 考虑到我们在 LogAndApply 里持有 mutex_，其实写是安全的。
  
  // 关键问题是读：Get 里的 current_ = versions_->current() 是无锁的。
  
  Version* old = current_;
  current_ = v; // 切换指针
  
  // 延迟释放旧版本
  old->Unref(); 
}

Status VersionSet::LogAndApply(VersionEdit* edit, std::mutex* mu) {

  (void)mu; 
  // 1. 设置 Edit 中的 next_file_number (防止重启后冲突)
  if (edit->has_log_number_) {
      assert(edit->log_number_ >= log_number_);
      assert(edit->log_number_ < next_file_number_);
  } else {
      edit->SetLogNumber(log_number_);
  }
  
  if (!edit->has_next_file_number_) {
      edit->SetNextFile(next_file_number_);
  }

  // 2. 创建新 Version
  // 简化逻辑：新 Version = 旧 Version + 新文件
  // (生产环境这里需要处理 Compaction 的删除逻辑，Week 2 我们只处理 AddFile)
  Version* v = new Version();
  
  // 2.1 拷贝旧文件 (L0)
  for (auto* f : current_->GetFiles()) {
      // 深拷贝还是浅拷贝元数据？通常浅拷贝指针，但要处理引用计数
      // Day 5 简化：深拷贝 FileMetaData
      FileMetaData* new_f = new FileMetaData(*f);
      v->files_.push_back(new_f);
  }
  
  // 2.2 应用新文件
  for (const auto& kv : edit->new_files_) {
      // kv.second 是 FileMetaData
      FileMetaData* new_f = new FileMetaData(kv.second);
      v->files_.push_back(new_f);
  }

  // 3. 写入 MANIFEST
  std::string record;
  edit->EncodeTo(&record);
  
  // 如果 Manifest 文件还没创建，创建一个新的
  if (!manifest_log_) {
      std::string fname = ManifestFileName(dbname_, manifest_file_number_);
      // ... Create file ... (这里略写，逻辑同 DBImpl)
      // 实际上我们可能需要 rolling manifest
      // Day 5 简化：假设已经有了，或者在这里创建
  }
  
  // Status s = manifest_log_->AddRecord(record);
  // if (s.ok()) s = manifest_file_->Sync();
  // if (!s.ok()) { delete v; return s; }

  // 4. 更新 Current 状态
  AppendVersion(v);
  log_number_ = edit->log_number_;
  
  return Status::OK();
}

} // namespace titankv