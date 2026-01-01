#include "lsm/version_set.h"
#include "lsm/version_edit.h"
#include "lsm/table_cache.h"
#include "lsm/merging_iterator.h"
#include "util/filename.h"
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

// 辅助：计算某一层文件的总大小
static uint64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
    uint64_t sum = 0;
    for (auto* f : files) sum += f->file_size;
    return sum;
}

// 辅助：获取某层的大小限制
static double MaxBytesForLevel(int level) {
    // L1: 10MB, L2: 100MB...
    double result = 10.0 * 1048576.0;
    while (level > 1) {
        result *= 10;
        level--;
    }
    return result;
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
  // 【新增】更新 compact_pointer_
  // 逻辑：本次 Compaction 涉及的最大的 Key，就是下一次的起点
  for (size_t i = 0; i < edit->new_files_.size(); i++) {
      int level = edit->new_files_[i].first;
      const FileMetaData& f = edit->new_files_[i].second;
        
      // 简单策略：取新生成文件的 Largest Key
      // 实际上应该取 Input 文件的 Largest Key，但这里简化处理
      compact_pointer_[level] = f.largest;
  }

  // 4. 更新 Current
  v->Finalize(); // 【新增】计算分数
  AppendVersion(v);
  return Status::OK();
}

Status VersionSet::Recover(bool* save_manifest) {
    (void)save_manifest;
    return Status::OK();
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
            list.push_back(iter);
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

Compaction* VersionSet::PickCompaction() {
    Version* current = current_;
    const bool size_compaction = (current->compaction_score() >= 1);
    
    if (!size_compaction) {
        return nullptr;
    }

    int level = current->compaction_level();
    const std::vector<FileMetaData*>& files = current->GetFiles(level);

    // 1. 寻找第一个 largest > compact_pointer_[level] 的文件
    // 也就是接续上一次的工作
    FileMetaData* f = nullptr;
    for (size_t i = 0; i < files.size(); i++) {
        if (compact_pointer_[level].empty() || 
            icmp_.Compare(files[i]->largest, Slice(compact_pointer_[level])) > 0) {
            f = files[i];
            break;
        }
    }

    // 如果所有文件都比 pointer 小（说明扫完了一圈），这就回绕到第一个文件
    if (f == nullptr && !files.empty()) {
        f = files[0];
    }
    
    if (f == nullptr) return nullptr;

    
    Compaction* c = new Compaction(&options_, level);

    // 1. 挑选 Input[0]
    if (current->GetFiles(level).empty()) {
        delete c;
        return nullptr;
    }
    c->inputs(0)->push_back(f);

    // L0 特殊处理：扩展重叠
    if (level == 0) {
        Slice min = f->smallest;
        Slice max = f->largest;
        
        // 【修复 1】如果种子文件的 Key 无效，不进行扩展，仅 Compact 它自己
        if (min.size() >= 8 && max.size() >= 8) {
            std::vector<FileMetaData*> l0_inputs;
            current->GetOverlappingInputs(0, min, max, &l0_inputs);
            *(c->inputs(0)) = l0_inputs;
        }
    }

    // 2. 挑选 Input[1] (Level + 1)
    // 计算 Input[0] 的整体范围
    if (c->inputs(0)->empty()) { // 防御性检查
        delete c;
        return nullptr;
    }

    FileMetaData* f0 = (*c->inputs(0))[0];
    Slice smallest = f0->smallest;
    Slice largest = f0->largest;

    // 【修复 2】如果初始范围无效，直接返回（只做 Input[0] 的整理，不合并到下一层）
    // 这可以防止 Crash，并允许系统逐渐通过 Compaction 修正元数据（如果有读取逻辑补全的话）
    if (smallest.size() < 8 || largest.size() < 8) {
        // 这里的 return c 意味着只 Compact inputs[0]，不拉取 inputs[1]
        // 这在 L0->L0 的 Trivial Move 或者 Self-Compaction 是合法的
        return c; 
    }

    for (size_t i = 1; i < c->inputs(0)->size(); ++i) {
        FileMetaData* f = (*c->inputs(0))[i];
        
        // 【修复 3】只比较合法的 Key
        if (f->smallest.size() >= 8 && f->largest.size() >= 8) {
            if (icmp_.Compare(f->smallest, smallest) < 0) smallest = f->smallest;
            if (icmp_.Compare(f->largest, largest) > 0) largest = f->largest;
        }
    }

    // 去 Next Level 找重叠
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