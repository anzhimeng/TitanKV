#include "lsm/version_set.h"
#include "lsm/version_edit.h"
#include "lsm/table_cache.h"
#include "util/filename.h"
#include "lsm/two_level_iterator.h"
#include "util/coding.h"
#include "blob/blob_format.h"
#include <algorithm>

namespace titankv {

// --- Version Implementation ---

Version::~Version() {
  for (int level = 0; level < kNumLevels; level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      delete f;
    }
  }
}

void Version::AddFile(int level, uint64_t file_number, uint64_t file_size,
                      const Slice& smallest, const Slice& largest) {
  assert(level < kNumLevels);
  FileMetaData* f = new FileMetaData();
  f->file_number = file_number;
  f->file_size = file_size;
  f->smallest = smallest.ToString();
  f->largest = largest.ToString();
  // 【关键】推入对应 Level 的 vector
  files_[level].push_back(f);
}

static int FindFile(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    FileMetaData* f = files[mid];
    if (icmp.Compare(f->largest, key) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return right;
}

Status Version::Get(const ReadOptions& options, const LookupKey& k, std::string* val,
                    bool* found, TableCache* table_cache,
                    std::function<Status(const Slice&, std::string*)> blob_getter) {
  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();
  const InternalKeyComparator* ucmp = vset_->icmp();
  Status s;

  // 1. 搜索 L0 (从新到旧)
  const std::vector<FileMetaData*>& l0_files = files_[0];
  for (auto it = l0_files.rbegin(); it != l0_files.rend(); ++it) {
    FileMetaData* f = *it;
    
    // 【关键修复】只有当 smallest/largest 有效时，才进行范围过滤
    // 如果是从 Recover 恢复的，这两个字段可能是空的，此时我们必须查文件
    if (!f->smallest.empty() && !f->largest.empty()) {
        if (ucmp->user_key_compare(user_key, ExtractUserKey(f->smallest)) < 0 ||
            ucmp->user_key_compare(user_key, ExtractUserKey(f->largest)) > 0) {
          continue;
        }
    }

    struct Context {
        std::string* val;
        bool found;
    } ctx {val, false};
    
    auto callback = [](void* arg, const Slice& k, const Slice& v) {
        (void)k;
        Context* c = static_cast<Context*>(arg);
        c->found = true;
        *(c->val) = v.ToString();
    };

    s = table_cache->Get(options, f->file_number, f->file_size, ikey, &ctx, callback);
    if (!s.ok()) return s;
    
    if (ctx.found) {
        BlobIndex b_index;
        Slice input(*val);
        if (b_index.DecodeFrom(&input).ok() && input.empty()) {
             s = blob_getter(input, val); 
        }
        *found = true;
        return Status::OK();
    }
  }

  // 2. 搜索 L1 ~ L6
  for (int level = 1; level < kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // 【修改】修复 size_t 和 int 比较警告
    size_t index = FindFile(*ucmp, files_[level], ikey);
    
    if (index >= num_files) continue;

    FileMetaData* f = files_[level][index];
    if (ucmp->Compare(ikey, f->smallest) < 0) {
        continue;
    }

    struct Context {
        std::string* val;
        bool found;
    } ctx {val, false};
    
    auto callback = [](void* arg, const Slice& k, const Slice& v) {
        (void)k;
        Context* c = static_cast<Context*>(arg);
        c->found = true;
        *(c->val) = v.ToString();
    };
    
    s = table_cache->Get(options, f->file_number, f->file_size, ikey, &ctx, callback);
    if (!s.ok()) return s;

    if (ctx.found) {
        *found = true;
        return Status::OK();
    }
  }

  *found = false;
  return Status::OK();
}

// --- VersionSet Implementation ---

VersionSet::VersionSet(const std::string& dbname, const Options& options)
    : dbname_(dbname),
      options_(options),
      next_file_number_(2),
      manifest_file_number_(0),
      log_number_(0),
      current_(new Version(this)) {
  current_->Ref();
}

VersionSet::~VersionSet() {
  current_->Unref();
}

void VersionSet::AppendVersion(Version* v) {
  v->Ref();
  Version* old = current_;
  current_ = v;
  old->Unref();
}

Status VersionSet::LogAndApply(VersionEdit* edit, std::mutex* mu) {
  (void)mu; // 消除 unused parameter 警告

  if (edit->has_log_number_) {
      log_number_ = edit->log_number_;
  }
  if (!edit->has_next_file_number_) {
      edit->SetNextFile(next_file_number_);
  } else {
      next_file_number_ = edit->next_file_number_;
  }

  // 1. 创建新 Version
  Version* v = new Version(this);
  
  // 2. 复制旧 Version 的文件 (所有 Level)
  // 【关键修复】现在需要遍历所有 Level
  for (int level = 0; level < kNumLevels; level++) {
      const auto& files = current_->GetFiles(level);
      for (size_t i = 0; i < files.size(); i++) {
          FileMetaData* f = files[i];
          // 增加引用计数或深拷贝？这里做深拷贝简化管理
          FileMetaData* new_f = new FileMetaData(*f);
          v->files_[level].push_back(new_f);
      }
  }
  
  // 3. 应用变更 (AddFile)
  // 【关键修复】VersionEdit 中的新文件带有 Level 信息，要加到对应 Level
  for (const auto& kv : edit->new_files_) {
      int level = kv.first; // 获取 Level
      const FileMetaData& meta = kv.second;
      
      FileMetaData* new_f = new FileMetaData(meta);
      // 添加到对应层级
      v->files_[level].push_back(new_f);
  }

  // TODO: 这里应该对 L1+ 的文件进行排序 (Sort by smallest key)
  // 也就是 v->files_[level] 需要 sort。Day 3 暂时忽略，假设 AddFile 是有序的或者只做 L0。

  // 4. 更新 Current
  AppendVersion(v);
  
  return Status::OK();
}

Status VersionSet::Recover(bool* save_manifest) {
    (void)save_manifest;
    return Status::OK();
}

class LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp), flist_(flist), index_(flist->size()) {}

  bool Valid() const override {
    return index_ < flist_->size();
  }

  void Seek(const Slice& target) override {
    // 二分查找第一个 largest >= target 的文件
    // 这与 Version::Get 里的逻辑类似
    // std::lower_bound 使用 < 比较，找到第一个不满足 (a < b) 的元素，即 a >= b
    // 我们的比较规则是：如果 file->largest < target，则 file 小于 target
    
    // 自定义比较器
    auto comp = [&](FileMetaData* f, const Slice& k) {
        return icmp_.Compare(f->largest, k) < 0;
    };
    
    auto it = std::lower_bound(flist_->begin(), flist_->end(), target, comp);
    index_ = std::distance(flist_->begin(), it);
  }

  void SeekToFirst() override { index_ = 0; }
  
  void SeekToLast() override {
    if (flist_->empty()) {
      index_ = 0;
    } else {
      index_ = flist_->size() - 1;
    }
  }

  void Next() override {
    assert(Valid());
    index_++;
  }

  void Prev() override {
    if (index_ == 0) {
      index_ = flist_->size(); // Invalid
    } else {
      index_--;
    }
  }

  Slice key() const override {
    assert(Valid());
    return (*flist_)[index_]->largest; // Index Iterator 的 Key 必须是边界 Key
  }

  Slice value() const override {
    assert(Valid());
    // 编码 file_number 和 file_size
    // 我们使用 16 字节的 buffer
    EncodeFixed64(value_buf_, (*flist_)[index_]->file_number);
    EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }

  Status status() const override { return Status::OK(); }

 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* flist_;
  uint32_t index_;
  mutable char value_buf_[16]; // 用于存储编码后的 value
};

// 回调函数：根据 handle_value 打开 SSTable
static Iterator* GetFileIterator(void* arg, const ReadOptions& options, const Slice& handle_value) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  
  if (handle_value.size() != 16) {
      return nullptr; // Error
  }
  
  uint64_t file_number = DecodeFixed64(handle_value.data());
  uint64_t file_size = DecodeFixed64(handle_value.data() + 8);
  
  return cache->NewIterator(options, file_number, file_size);
}

// 暴露给外部的工厂方法
Iterator* NewLevelIterator(const InternalKeyComparator& icmp,
                           TableCache* table_cache,
                           const std::vector<FileMetaData*>& files,
                           const ReadOptions& options) {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(icmp, &files), // Index Iterator
      &GetFileIterator,                       // Block Function
      table_cache,                            // arg
      options);
}


} // namespace titankv