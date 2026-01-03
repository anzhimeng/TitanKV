#include "lsm/version_set.h"
#include "lsm/version_edit.h"
#include "lsm/table_cache.h"
#include "lsm/compaction.h"
#include "lsm/merging_iterator.h"
#include "util/filename.h"
#include "util/coding.h"
#include "blob/blob_format.h"
#include "wal/log_reader.h" 
#include <algorithm>
#include <fstream>
#include <set>
#include <map>

namespace titankv {

// --- Version Implementation ---

static double MaxBytesForLevel(int level) {
    double result = 10.0 * 1048576.0;
    while (level > 1) {
        result *= 10;
        level--;
    }
    return result;
}

static uint64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
    uint64_t sum = 0;
    for (auto* f : files) sum += f->file_size;
    return sum;
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

// 原子更新 CURRENT 文件：先写临时文件，再 rename
static Status SetCurrentFile(const std::string& dbname, uint64_t descriptor_number) {
  std::string manifest = ManifestFileName(dbname, descriptor_number);
  // CURRENT 文件内容只包含 Manifest 的文件名（不含路径）
  // 例如: MANIFEST-000005\n
  std::string content = manifest.substr(dbname.size() + 1) + "\n";
  
  std::string tmp = TempFileName(dbname, descriptor_number);
  Status s = WriteStringToFile(tmp, content);
  if (s.ok()) {
      if (rename(tmp.c_str(), CurrentFileName(dbname).c_str()) != 0) {
          s = Status::IOError("Rename failed", tmp);
          std::remove(tmp.c_str());
      }
  } else {
      std::remove(tmp.c_str());
  }
  return s;
}

// --- Version::Builder ---
// 一个 Helper 类，用于将 VersionEdit 应用到 Base Version 上，生成 New Version
class VersionSet::Builder {
 private:
  // 按照 (Smallest Key, FileNum) 排序
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;
    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) return r < 0;
      return f1->file_number < f2->file_number;
    }
  };

  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
  };

  VersionSet* vset_;
  Version* base_;
  LevelState levels_[kNumLevels];

 public:
  Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base) {
    base_->Ref();
    BySmallestKey cmp;
    cmp.internal_comparator = vset_->icmp();
    for (int level = 0; level < kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
      for (auto* f : *added) {
        // 这里的引用计数逻辑比较复杂。
        // 在标准实现中，VersionEdit 里的 NewFile 初始 ref=1。
        // Builder 只是借用。真正转移所有权是在 SaveTo 里的 v->files_.push_back
        // 如果 Builder 析构了但没生成 Version，理论上需要减少引用。
        // 为简化，我们假设 FileMetaData 是纯数据结构，由 Version 统一管理生命周期。
        if (f->refs <= 0) delete f; // 防御性清理
      }
      delete added;
    }
    base_->Unref();
  }

  // 应用增量变更 (可多次调用)
  void Apply(VersionEdit* edit) {
    // 1. 更新删除集合
    for (const auto& del : edit->deleted_files_) {
        int level = del.first;
        uint64_t number = del.second;
        
        // 维护 deleted_files 集合
        levels_[level].deleted_files.insert(number);
        
        // 如果这个文件是刚才在同一个 Builder 里 Add 的，直接从 added_files 移除
        // 这种情况在 Recover 过程中可能出现（先 Add 后 Delete）
        auto& added = *levels_[level].added_files;
        for (auto it = added.begin(); it != added.end(); ) {
            if ((*it)->file_number == number) {
                delete *it; // 【新增】释放内存！
                it = added.erase(it);
            } else {
                ++it;
            }
        }
    }

    // 2. 更新新增文件
    for (const auto& nf : edit->new_files_) {
        int level = nf.first;
        FileMetaData* f = new FileMetaData(nf.second);
        f->refs = 1;

        // 如果之前标记了删除该文件（文件号复用？极少见），先取消删除标记
        levels_[level].deleted_files.erase(f->file_number);
        levels_[level].added_files->insert(f);
    }
  }

  // 将 Base + Delta 合并到新 Version v 中
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = vset_->icmp();
    
    for (int level = 0; level < kNumLevels; level++) {
      // 1. 合并 Base 文件
      const std::vector<FileMetaData*>& base_files = base_->GetFiles(level);
      auto base_iter = base_files.begin();
      auto base_end = base_files.end();
      
      const FileSet* added = levels_[level].added_files;
      
      v->files_[level].reserve(base_files.size() + added->size());
      
      // 归并排序：将 base_files 和 added_files 有序合并
      for (const auto& added_file : *added) {
        // 把 base 中小于 added_file 的先加进去
        for (auto bpos = std::upper_bound(base_iter, base_end, added_file, cmp);
             base_iter != bpos; ++base_iter) {
             MaybeAddFile(v, level, *base_iter);
        }
        // 加入 added_file
        MaybeAddFile(v, level, added_file);
      }
      
      // 加入剩余的 base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }
    }
  }

  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    // 如果在删除列表中，则丢弃
    if (levels_[level].deleted_files.count(f->file_number) > 0) {
      return;
    }
    
    // 这里的 f 可能是 base 里的（深拷贝），也可能是 added 里的（新 new 的）
    // 为了统一管理，我们在 Version 中存储副本
    FileMetaData* new_f = new FileMetaData(*f);
    new_f->refs = 1;
    v->files_[level].push_back(new_f);
  }
};

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

void Version::Finalize() {
    // 寻找分数最高的层
    int best_level = -1;
    double best_score = -1;

    for (int level = 0; level < kNumLevels - 1; level++) {
        double score;
        if (level == 0) {
            // L0 仍然按文件数：4 个
            score = files_[0].size() / 4.0;
        } else {
            const uint64_t level_bytes = TotalFileSize(files_[level]);
            score = static_cast<double>(level_bytes) / MaxBytesForLevel(level);
        }

        if (score > best_score) {
            best_score = score;
            best_level = level;
        }
    }

    compaction_level_ = best_level;
    compaction_score_ = best_score;
}


void Version::GetOverlappingInputs(
    int level,
    const Slice& begin,
    const Slice& end,
    std::vector<FileMetaData*>* inputs) {
    
    inputs->clear();
    Slice user_begin, user_end;
    if (!begin.empty()) user_begin = ExtractUserKey(begin);
    if (!end.empty()) user_end = ExtractUserKey(end);

    // 【关键修复】直接使用成员变量 vset_ 获取 Comparator
    const InternalKeyComparator* ucmp = vset_->icmp();
    
    // 【关键修复】直接使用成员变量 files_ 获取文件列表
    const std::vector<FileMetaData*>& current_files = files_[level];

    for (size_t i = 0; i < current_files.size(); i++) {
        FileMetaData* f = current_files[i];
        const Slice file_start = f->smallest;
        const Slice file_limit = f->largest;
        
        // 健壮性检查
        if (file_start.size() < 8 || file_limit.size() < 8) {
            inputs->push_back(f);
            continue;
        }

        const Slice file_u_start = ExtractUserKey(file_start);
        const Slice file_u_limit = ExtractUserKey(file_limit);

        if (!end.empty() && ucmp->user_key_compare(user_end, file_u_start) < 0) {
            // file is completely after range
        } else if (!begin.empty() && ucmp->user_key_compare(user_begin, file_u_limit) > 0) {
            // file is completely before range
        } else {
            inputs->push_back(f);
        }
    }
}

bool Version::OverlapInLevel(int level, const Slice& user_key, const Slice& internal_key) const {
  const InternalKeyComparator* ucmp = vset_->icmp();
  
  // 遍历 level 及更下层
  for (; level < kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = files_[level];
    
    // 因为 L1+ 是有序不重叠的，我们可以用二分查找加速
    // 这里为了逻辑简单，先写通用检查（L0和L1+都适用）
    // 检查是否有文件范围覆盖了 user_key
    for (const auto* f : files) {
      if (ucmp->user_key_compare(user_key, ExtractUserKey(f->smallest)) >= 0 &&
          ucmp->user_key_compare(user_key, ExtractUserKey(f->largest)) <= 0) {
        return true;
      }
    }
  }
  return false;
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
  for (int i = 0; i < kNumLevels; i++) {
    compact_pointer_[i] = ""; // 初始为空，表示从头开始
  }
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

// 核心：使用 Builder 应用 Edit，并写入 Manifest
Status VersionSet::LogAndApply(VersionEdit* edit, std::mutex* mu) {
  // 1. 设置 Edit 的全局状态
  if (edit->has_log_number_) {
      assert(edit->log_number_ >= log_number_);
      assert(edit->log_number_ < next_file_number_);
  } else {
      edit->SetLogNumber(log_number_);
  }

  if (!edit->has_next_file_number_) {
      edit->SetNextFile(next_file_number_);
  }
  // 【新增】更新 LastSequence
  if (edit->has_last_sequence_) {
      assert(edit->last_sequence_ >= last_sequence_);
      last_sequence_ = edit->last_sequence_;
  } else {
      // 每次写 Manifest 都带上当前的 Sequence，防止回滚
      edit->SetLastSequence(last_sequence_);
  }
  // 2. 利用 Builder 构建新版本 (Current + Edit -> New)
  Version* v = new Version(this);
  {
      Builder builder(this, current_);
      builder.Apply(edit);
      builder.SaveTo(v);
  }
  
  // 更新辅助信息
  v->Finalize(); // 计算 Score
  // 更新 compact_pointer (省略代码，保持原样)

  // 3. 写入 MANIFEST (持久化)
  std::string record;
  
  if (!manifest_log_) {
      // --- 情况 A: 需要创建新 Manifest 文件 (如重启后首次写入) ---
      // 我们不直接写 edit，而是要把当前的“全量状态”先写进去，以此作为 Base。
      // 否则如果删除了旧 Manifest，只靠这个增量 edit 是无法恢复数据的。
      
      uint64_t new_manifest_file_number = NewFileNumber();
      std::string fname = ManifestFileName(dbname_, new_manifest_file_number);
      std::unique_ptr<WritableFile> file;
      Status s = NewWritableFile(fname, &file);
      if (!s.ok()) { delete v; return s; }
      
      manifest_file_ = std::move(file);
      manifest_log_ = std::make_unique<log::Writer>(manifest_file_.get());
      
      // >> 关键点：构造快照 Edit <<
      // 这个 snapshot_edit 包含了 current_ 的所有文件
      VersionEdit snapshot_edit;
      snapshot_edit.SetLogNumber(log_number_);
      snapshot_edit.SetNextFile(next_file_number_);
      
      for (int level = 0; level < kNumLevels; level++) {
          const auto& files = current_->GetFiles(level);
          for (const auto* f : files) {
              // 把当前存活的所有文件都加到 snapshot_edit 中
              snapshot_edit.AddFile(level, f->file_number, f->file_size, 
                                    Slice(f->smallest), Slice(f->largest));
          }
      }
      
      // 先写入全量快照
      std::string snapshot_record;
      snapshot_edit.EncodeTo(&snapshot_record);
      s = manifest_log_->AddRecord(snapshot_record);
      if (!s.ok()) { delete v; return s; }

      // 再写入本次的增量 edit
      edit->EncodeTo(&record);
      s = manifest_log_->AddRecord(record);
      if (!s.ok()) { delete v; return s; }
      
      // 更新 CURRENT 指向这个新文件
      s = SetCurrentFile(dbname_, new_manifest_file_number);
      if (!s.ok()) { delete v; return s; }
      
      manifest_file_number_ = new_manifest_file_number;

  } else {
      // --- 情况 B: 追加到现有 Manifest ---
      edit->EncodeTo(&record);
      Status s = manifest_log_->AddRecord(record);
      if (!s.ok()) { delete v; return s; }
  }

  // 4. Sync 刷盘
  Status s = manifest_file_->Sync();
  if (!s.ok()) { delete v; return s; }

  // 5. 内存切换 (Install New Version)
  AppendVersion(v);
  log_number_ = edit->log_number_;
  next_file_number_ = edit->next_file_number_;

  return Status::OK();
}
// core/src/lsm/version_set.cc

Status VersionSet::Recover(bool* save_manifest) {
  struct LogReporter : public log::Reporter {
    Status* status;
    void Corruption(size_t bytes, const Status& s) override {
      if (this->status->ok()) *this->status = s;
    }
  };

  std::string current_file = CurrentFileName(dbname_);
  std::ifstream in(current_file);
  if (!in) {
      return Status::NotFound("CURRENT not found");
  }
  
  std::string manifest_name;
  std::getline(in, manifest_name);
  if (manifest_name.empty() || manifest_name.back() == '\r') {
      if (!manifest_name.empty()) manifest_name.pop_back();
  }
  if (manifest_name.empty()) return Status::Corruption("CURRENT empty");
  
  std::string manifest_path = dbname_ + "/" + manifest_name;
  std::unique_ptr<SequentialFile> file;
  Status s = NewSequentialFile(manifest_path, &file);
  if (!s.ok()) return s;

  LogReporter reporter;
  reporter.status = &s;
  log::Reader reader(file.get(), &reporter, true, 0);
  
  Slice record;
  std::string scratch;
  VersionEdit edit;
  
  // 临时状态变量
  uint64_t next_file = 0;
  uint64_t log_num = 0;
  bool have_log_number = false;
  bool have_next_file = false;
  
  // 【关键修复】定义缺失的变量
  bool have_last_sequence = false;
  uint64_t last_seq = 0;

  { 
      Version* base = new Version(this);
      Builder builder(this, base); 
      
      while (reader.ReadRecord(&record, &scratch) && s.ok()) {
          if (edit.DecodeFrom(record).ok()) {
              if (edit.has_log_number_) {
                  log_num = edit.log_number_;
                  have_log_number = true;
              }
              if (edit.has_next_file_number_) {
                  next_file = edit.next_file_number_;
                  have_next_file = true;
              }
              // 【关键修复】现在变量已定义，可以赋值了
              if (edit.has_last_sequence_) {
                  last_seq = edit.last_sequence_;
                  have_last_sequence = true;
              }
              
              builder.Apply(&edit);
          } else {
              s = Status::Corruption("Manifest record decode failed");
          }
      }

      if (s.ok()) {
          if (!have_next_file) {
              s = Status::Corruption("no meta-nextfile entry in descriptor");
          } else if (!have_log_number) {
              s = Status::Corruption("no meta-lognumber entry in descriptor");
          }

          if (s.ok()) {
              Version* v = new Version(this);
              builder.SaveTo(v);
              v->Finalize();
              AppendVersion(v);
              
              manifest_file_number_ = next_file; 
              next_file_number_ = next_file + 1;
              log_number_ = log_num;
              
              // 【关键修复】恢复 last_sequence_
              if (have_last_sequence) {
                  last_sequence_ = last_seq;
              }
              
              if (manifest_name.length() > 9) { 
                   manifest_file_number_ = std::stoull(manifest_name.substr(9));
              }
              
              if (save_manifest) *save_manifest = true;
          }
      }
      
  } 

  return s;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
    // 遍历当前 Version 的所有层级
    for (int level = 0; level < kNumLevels; level++) {
        const std::vector<FileMetaData*>& files = current_->GetFiles(level);
        for (const auto* f : files) {
            live->insert(f->file_number);
        }
    }
}

Iterator* VersionSet::MakeInputIterator(Compaction* c, TableCache* table_cache, const ReadOptions& options) {
    // 0. 准备迭代器列表
    std::vector<Iterator*> list;
    
    // 1. 处理 inputs_[0] (源层)
    int level = c->level();
    const std::vector<FileMetaData*>& files_0 = *c->inputs(0);
    
    if (level == 0) {
        // L0 文件之间可能有重叠，必须为每个文件创建一个独立的 Iterator
        for (size_t i = 0; i < files_0.size(); i++) {
            Iterator* iter = table_cache->NewIterator(options, 
                                                      files_0[i]->file_number, 
                                                      files_0[i]->file_size);
            // 【关键修复】检查空指针
            if (iter != nullptr) {
                list.push_back(iter);
            } else {
                // 如果打开失败（极其罕见），记录日志但不要崩溃
                fprintf(stderr, "[Compaction] Failed to open L0 file #%lu\n", files_0[i]->file_number);
            }
        }
    } else {
        // L1+ 文件不重叠，创建一个 LevelIterator 即可
        // CreateTwoLevelIterator 是我们在 Day 4 实现的 NewLevelIterator
        // 请确认你的 Day 4 代码里叫什么名字，这里假设叫 NewLevelIterator
        list.push_back(NewLevelIterator(icmp_, table_cache, files_0, options));
    }

    // 2. 处理 inputs_[1] (目标层，Level + 1)
    const std::vector<FileMetaData*>& files_1 = *c->inputs(1);
    if (!files_1.empty()) {
        // 目标层一定是不重叠的 (L1+)，所以直接用 LevelIterator
        list.push_back(NewLevelIterator(icmp_, table_cache, files_1, options));
    }

    // 3. 将所有迭代器合并为一个
    return NewMergingIterator(&icmp_, &list[0], list.size());
}

// core/src/lsm/version_set.cc

// core/src/lsm/version_set.cc

Compaction* VersionSet::PickCompaction() {
    Version* current = current_;
    const bool size_compaction = (current->compaction_score() >= 1);
    
    if (!size_compaction) return nullptr;

    int level = current->compaction_level();
    
    // [修改] 将 'current' 传入构造函数
    Compaction* c = new Compaction(&options_, level, current);

    // 1. 初始选择
    if (current->GetFiles(level).empty()) {
        delete c;
        return nullptr;
    }
    FileMetaData* f = current->GetFiles(level)[0];
    c->inputs(0)->push_back(f);

    // 2. L0 激进合并策略
    if (level == 0) {
        c->inputs(0)->clear(); 
        const auto& l0_files = current->GetFiles(0);
        for (auto* f : l0_files) {
            c->inputs(0)->push_back(f);
        }
        fprintf(stderr, "[Pick] Aggressive L0: picked %lu files\n", c->inputs(0)->size());
    }

    // 3. 挑选 Input[1]
    if (c->inputs(0)->empty()) {
        delete c;
        return nullptr;
    }

    Slice smallest = (*c->inputs(0))[0]->smallest;
    Slice largest = (*c->inputs(0))[0]->largest;
    const InternalKeyComparator* icmp = &icmp_;

    for (size_t i = 1; i < c->inputs(0)->size(); ++i) {
        FileMetaData* f = (*c->inputs(0))[i];
        if (f->smallest.size() >= 8 && smallest.size() >= 8 && icmp->Compare(f->smallest, smallest) < 0) smallest = f->smallest;
        if (f->largest.size() >= 8 && largest.size() >= 8 && icmp->Compare(f->largest, largest) > 0) largest = f->largest;
    }

    current->GetOverlappingInputs(level + 1, smallest, largest, c->inputs(1));
    return c;
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