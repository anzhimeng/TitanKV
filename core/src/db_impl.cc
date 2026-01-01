#include "titankv/db_impl.h"
#include "wal/log_reader.h"
#include "util/coding.h"
#include "lsm/table_builder.h"
#include "lsm/version_edit.h"
#include "lsm/compaction.h"
#include "util/filename.h"
#include "util/cache.h"
#include <filesystem>
#include <iostream>
#include <algorithm>
#include <vector>

namespace titankv {

// --- 静态工厂方法 ---
Status DB::Open(const Options& options, const std::string& name, DB** dbptr) {
  *dbptr = nullptr;

  // 1. 检查/创建目录
  if (!std::filesystem::exists(name)) {
    if (options.create_if_missing) {
      std::filesystem::create_directories(name);
    } else {
      return Status::InvalidArgument("DB directory does not exist");
    }
  }

  // 2. 创建实例
  DBImpl* impl = new DBImpl(options, name);

  // 3. 执行恢复
  Status s = impl->Recover();
  if (!s.ok()) {
    delete impl;
    return s;
  }

  *dbptr = impl;
  return Status::OK();
}

// --- 构造与析构 ---

DBImpl::DBImpl(const Options& options, const std::string& dbname)
    : dbname_(dbname),
      options_(options),
      mem_(nullptr),
      blob_store_(nullptr),
      last_sequence_(0),
      imm_(nullptr),
      table_cache_(nullptr),
      versions_(nullptr),
      bg_running_(true)
{
  // 1. 初始化 io_uring
  // 生产环境可以做成配置项，这里直接启用
  try {
      uring_executor_ = std::make_unique<IoUringExecutor>(256); // 队列深度 256
      fprintf(stderr, "[DBImpl] io_uring enabled.\n");
  } catch (...) {
      fprintf(stderr, "[DBImpl] io_uring init failed, fallback to sync IO.\n");
      uring_executor_ = nullptr;
  }

  // 如果外部没传 Cache，初始化一个默认的
  if (options_.block_cache == nullptr) {
  options_.block_cache.reset(NewLRUCache(8 * 1024 * 1024)); // 8MB
  }

  if (options_.filter_policy == nullptr) {
    options_.filter_policy = NewBloomFilterPolicy(10);
  }


  InternalKeyComparator cmp;
  mem_ = new MemTable(cmp);
  mem_->Ref();


  // 初始化组件
  blob_store_ = new BlobStore(dbname_ + "/blob", options, uring_executor_.get());

  table_cache_ = new TableCache(dbname_, options_);
  versions_ = new VersionSet(dbname_, options_);
  // 启动后台线程
  bg_thread_ = std::thread(&DBImpl::BGWork, this);
}

DBImpl::~DBImpl() {
  // 1. 停止后台线程
  {
    std::lock_guard<std::mutex> lock(bg_mutex_);
    bg_running_ = false;
    bg_cv_.notify_all();
  }
  if (bg_thread_.joinable()) {
    bg_thread_.join();
  }
  if (mem_) mem_->Unref();
  if (imm_) imm_->Unref();
  delete blob_store_;
  delete table_cache_;
  delete versions_;
  // log_ 和 logfile_ 是 unique_ptr，自动释放
}

// --- 核心恢复逻辑 (Crash Recovery) ---

Status DBImpl::Recover() {
  // 1. 扫描目录下的所有文件
  std::vector<std::string> filenames;
  for (const auto& entry : std::filesystem::directory_iterator(dbname_)) {
      filenames.push_back(entry.path().filename().string());
  }

  std::vector<uint64_t> sst_files;
  uint64_t max_log_number = 0;
  bool has_log = false;

  for (const auto& fname : filenames) {
      uint64_t number;
      // 解析 SST 文件: 000001.sst
      if (fname.length() > 4 && fname.substr(fname.length() - 4) == ".sst") {
          number = std::stoull(fname.substr(0, fname.length() - 4));
          sst_files.push_back(number);
      } 
      // 解析 Log 文件: 000003.log
      else if (fname.length() > 4 && fname.substr(fname.length() - 4) == ".log") {
          try {
            number = std::stoull(fname.substr(0, fname.length() - 4));
            if (number > max_log_number) {
                max_log_number = number;
                has_log = true;
            }
          } catch (...) { 
              // 兼容旧逻辑：如果只有 wal.log，把它当作 log number 0
              if (fname == "wal.log") {
                  if (!has_log) max_log_number = 0; 
                  has_log = true; 
              }
          }
      }
  }

  // 2. 恢复 VersionSet (将磁盘上的 SSTable 加入内存元数据)
  // 为了简单，我们手动构建一个 VersionEdit 把所有 SSTable 加进去
  VersionEdit edit;
  for (uint64_t num : sst_files) {
      std::string fname = TableFileName(dbname_, num);
      uint64_t fsize = std::filesystem::file_size(fname);
      // Smallest/Largest 暂时留空，Get 时会全盘扫描这些文件（功能正确，性能稍差）
      edit.AddFile(0, num, fsize, Slice(""), Slice(""));
      
      // 更新文件编号计数器，防止冲突
      while (versions_->NewFileNumber() <= num); 
  }
  // 应用到 VersionSet
  versions_->LogAndApply(&edit, &mutex_);

  // 更新 next_file_number_ 超过 max_log_number
  while (versions_->NewFileNumber() <= max_log_number);

  // 3. 回放最新的 WAL
  if (!has_log && !std::filesystem::exists(dbname_ + "/wal.log")) {
      return Status::OK(); // 新库
  }

  std::string log_path;
  if (max_log_number > 0) {
      log_path = LogFileName(dbname_, max_log_number);
  } else {
      log_path = dbname_ + "/wal.log";
  }

  std::unique_ptr<SequentialFile> file;
  Status s = NewSequentialFile(log_path, &file);
  if (!s.ok()) return s; // 如果打开失败，可能是文件不存在，视为 OK

  log::Reader reader(file.get(), nullptr, true, 0);
  Slice record;
  std::string scratch;
  
  while (reader.ReadRecord(&record, &scratch)) {
    Slice input = record;
    if (input.size() < 1) continue;

    char type_char = input[0];
    input.remove_prefix(1);
    ValueType type = static_cast<ValueType>(type_char);

    uint32_t key_len;
    if (!GetVarint32(&input, &key_len)) continue;
    Slice key(input.data(), key_len);
    input.remove_prefix(key_len);

    uint32_t val_len;
    if (!GetVarint32(&input, &val_len)) continue;
    Slice value(input.data(), val_len);
    
    // 恢复 Sequence
    last_sequence_++;
    mem_->Add(last_sequence_, type, key, value);
  }

  // 4. 准备新的写入环境
  // 恢复完成后，开启一个新的 Log 文件用于后续写入
  uint64_t new_log_num = versions_->NewFileNumber();
  std::unique_ptr<WritableFile> lfile;
  s = NewWritableFile(LogFileName(dbname_, new_log_num), &lfile);
  if (!s.ok()) return s;
  
  logfile_ = std::move(lfile);
  log_ = std::make_unique<log::Writer>(logfile_.get());
  
  // 记录 LogNumber 变更
  VersionEdit log_edit;
  log_edit.SetLogNumber(new_log_num);
  versions_->LogAndApply(&log_edit, &mutex_);

  return Status::OK();
}

// --- 写入路径 ---

Status DBImpl::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // 1. 检查 MemTable 空间 (可能触发 Flush)
  Status s = MakeRoomForWrite(false);
  if (!s.ok()) return s;

  // 2. 写入
  return WriteLocked(opt, kTypeValue, key, value);
}

Status DBImpl::Delete(const WriteOptions& opt, const Slice& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  Status s = MakeRoomForWrite(false);
  if (!s.ok()) return s;

  return WriteLocked(opt, kTypeDeletion, key, Slice());
}

Status DBImpl::WriteLocked(const WriteOptions& opt, ValueType type, const Slice& key, const Slice& value) {
  Status s;
  std::string value_to_store;

    // 【模拟垃圾检测逻辑】
  if (type == kTypeValue && options_.simulate_garbage_generation) { // 仅 Put 时检查
      std::string old_val_idx;
      // 1. 尝试读取旧索引
      Status s = GetLSMValue(key, &old_val_idx);
      
      if (s.ok()) {
          BlobIndex old_idx;
          Slice input(old_val_idx);
          if (old_idx.DecodeFrom(&input).ok()) {
              // 2. 只有解码成功才通知
              blob_store_->NotifyGarbage(old_idx.file_id, old_idx.size);
              
              // 【新增调试】看看是否真的进来了 (生产环境需删除)
              // fprintf(stderr, "[DEBUG] Marked garbage for Key: %s, File: %u\n", 
              //         key.ToString().c_str(), old_idx.file_number);
          } else {
              // fprintf(stderr, "[DEBUG] Found key %s but decode BlobIndex failed\n", key.ToString().c_str());
          }
      } else {
          // 没找到旧值？可能是 GetLSMValue 没查到 SSTable
          // fprintf(stderr, "[DEBUG] Key %s not found in LSM during overwrite.\n", key.ToString().c_str());
      }
  }

  // 1. 键值分离逻辑
  if (type == kTypeValue && value.size() >= options_.min_blob_size) {
    BlobIndex b_index;
    s = blob_store_->Add(key, value, &b_index);
    if (!s.ok()) return s;
    b_index.EncodeTo(&value_to_store);
  } else {
    value_to_store.assign(value.data(), value.size());
  }

  // 2. 构造 WAL 条目
  std::string log_record = EncodeLogRecord(type, key, value_to_store);
  
  // 如果 log_ 为空（新库首次写入），初始化它
  if (!log_) {
    uint64_t new_log_num = versions_->NewFileNumber();
    std::unique_ptr<WritableFile> lfile;
    s = NewWritableFile(LogFileName(dbname_, new_log_num), &lfile);
    if (!s.ok()) return s;
    
    logfile_ = std::move(lfile);
    log_ = std::make_unique<log::Writer>(logfile_.get());
    
    VersionEdit edit;
    edit.SetLogNumber(new_log_num);
    versions_->LogAndApply(&edit, &mutex_);
  }

  // 3. 写入 WAL
  s = log_->AddRecord(log_record);
  if (!s.ok()) return s;

  if (opt.sync) {
    s = logfile_->Sync();
    if (!s.ok()) return s;
  }

  // 4. 写入 MemTable
  last_sequence_++;
  mem_->Add(last_sequence_, type, key, value_to_store);

  return Status::OK();
}

Status DBImpl::MakeRoomForWrite(bool force) {
    bool allow_switch = (force || mem_->ApproximateMemoryUsage() >= options_.write_buffer_size);
    if (!allow_switch) {
        return Status::OK();
    }

    // 1. 处理旧的 Immutable (如果有)
    if (imm_ != nullptr) {
        VersionEdit edit;
        Status s = WriteLevel0Table(imm_, &edit);
        if (!s.ok()) return s;
        
        s = versions_->LogAndApply(&edit, &mutex_);
        if (!s.ok()) return s;

        imm_->Unref();
        imm_ = nullptr;
    }

    // 2. 切换 MemTable & WAL
    imm_ = mem_;
    mem_ = new MemTable(InternalKeyComparator());
    mem_->Ref();

    // 切换 Log
    logfile_.reset();
    log_.reset();

    uint64_t new_log_number = versions_->NewFileNumber();
    std::string log_fname = LogFileName(dbname_, new_log_number);
    Status s = NewWritableFile(log_fname, &logfile_);
    if (!s.ok()) return s;
    
    log_ = std::make_unique<log::Writer>(logfile_.get());

    // 应用 LogNumber 变更
    VersionEdit edit;
    edit.SetLogNumber(new_log_number);
    s = versions_->LogAndApply(&edit, &mutex_);
    if (!s.ok()) return s;

    // 3. Flush 刚刚切换出来的 imm_ (同步 Flush)
    {
        VersionEdit imm_edit;
        s = WriteLevel0Table(imm_, &imm_edit);
        if (!s.ok()) return s;
        
        s = versions_->LogAndApply(&imm_edit, &mutex_);
        if (!s.ok()) return s;
        
        imm_->Unref();
        imm_ = nullptr;
    }
    bg_cv_.notify_one(); 
    return Status::OK();
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit) {
  // 1. 准备文件
  uint64_t file_number = versions_->NewFileNumber();
  std::string fname = TableFileName(dbname_, file_number);
  
  std::unique_ptr<WritableFile> file;
  Status s = NewWritableFile(fname, &file);
  if (!s.ok()) return s;

  TableBuilder builder(options_, file.get());

  // 2. 遍历 MemTable 写入 Builder
  Iterator* iter = mem->NewIterator();
  iter->SeekToFirst();

  if (!iter->Valid()) {
    delete iter;
    builder.Abandon();
    return Status::OK();
  }
  
  std::string smallest, largest;
  bool first = true;
  
  for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      if (first) {
          smallest = key.ToString();
          first = false;
      }
      largest = key.ToString();
      builder.Add(key, iter->value());
  }
  delete iter;

  // 3. 完成构建
  s = builder.Finish();
  if (!s.ok()) {
      builder.Abandon(); // 【关键修复】显式放弃
      return s;
  }
  
  uint64_t file_size = builder.FileSize();
  s = file->Close();
  if (!s.ok()) return s;

  if (s.ok()) {
      // 记录新文件到 VersionEdit
      edit->AddFile(0, file_number, file_size, Slice(smallest), Slice(largest));
      // fprintf(stderr, "[DBImpl] Flushed SSTable #%lu. Entries: %lu, Size: %lu\n", 
              // file_number, builder.NumEntries(), file_size);
  }
  return s;
}

// --- 读取路径 ---

Status DBImpl::Get(const ReadOptions& opt, const Slice& key, std::string* value) {
  SequenceNumber snapshot = last_sequence_.load(std::memory_order_acquire);
  LookupKey lkey(key, snapshot);
  Status s;

  // 1. 查 MemTable
  if (mem_->Get(lkey, value, &s)) {
      if (s.IsNotFound()) return s;
      return ResolveBlobIndex(value); // 封装 Blob 读取逻辑
  }

  // 2. 查 Immutable
  if (imm_ != nullptr) {
      if (imm_->Get(lkey, value, &s)) {
          if (s.IsNotFound()) return s;
          return ResolveBlobIndex(value);
      }
  }

  // 3. 查 Version (L0 - L6)
  Version* current;
  {
      std::lock_guard<std::mutex> lock(mutex_);
      current = versions_->current();
      current->Ref();
  }

  bool found = false;
  // 定义 BlobGetter 回调
  auto blob_getter = [&](const Slice& blob_idx_slice, std::string* val_out) -> Status {
      // 这里的 blob_idx_slice 是编码后的 BlobIndex
      // 我们其实不需要传 slice，因为 Version::Get 内部已经有了 string value
      // 但为了配合上面的接口设计，这里做个适配
      // 实际上最好把 ResolveBlobIndex 逻辑放到 Version::Get 外面，Version只负责返回 Raw Value
      // 但为了利用 Version::Get 内部的 found 判断，我们在外面做 Resolve 比较好。
      return Status::OK();
  };

  // 调用 Version::Get 获取 Raw Value
  s = current->Get(opt, lkey, value, &found, table_cache_, blob_getter);
  
  current->Unref();

  if (s.ok() && found) {
      // 找到了 Raw Value，处理 Blob
      return ResolveBlobIndex(value);
  }

  return Status::NotFound("Key not found");
}

std::string DBImpl::EncodeLogRecord(ValueType type, const Slice& key, const Slice& value) {
  std::string dst;
  dst.push_back(static_cast<char>(type));
  PutVarint32(&dst, key.size());
  dst.append(key.data(), key.size());
  PutVarint32(&dst, value.size());
  dst.append(value.data(), value.size());
  return dst;
}

Status DBImpl::GetLSMValue(const Slice& key, std::string* val_buf) {
  SequenceNumber snapshot = last_sequence_.load(std::memory_order_acquire);
  LookupKey lkey(key, snapshot);
  Status s;

  // 1. 查 MemTable
  if (mem_->Get(lkey, val_buf, &s)) return s;

  // 2. 查 Immutable
  if (imm_ != nullptr) {
    if (imm_->Get(lkey, val_buf, &s)) return s;
  }

  // 3. 查 Version (L0 - L6)
  Version* current;
  {
      std::lock_guard<std::mutex> lock(mutex_);
      current = versions_->current();
      current->Ref();
  }

  bool found = false;
  
  // 定义一个“空”的 BlobGetter
  // 因为 GetLSMValue 的目的就是获取 Raw Value (BlobIndex)，不需要去读 Blob
  auto noop_blob_getter = [&](const Slice& idx, std::string* val) {
      return Status::OK(); // 什么都不做，保留 val 中的 BlobIndex 字符串
  };

  // 复用 Version::Get 的多层查找逻辑
  s = current->Get(ReadOptions(), lkey, val_buf, &found, table_cache_, noop_blob_getter);
  
  current->Unref();

  if (s.ok() && found) {
      return Status::OK();
  }

  return Status::NotFound("Key not found");
}

// 【新增】实现 ResolveBlobIndex
Status DBImpl::ResolveBlobIndex(std::string* value) {
    BlobIndex b_index;
    Slice input(*value);
    // 尝试解码，如果成功且无剩余数据，说明是 BlobIndex
    if (b_index.DecodeFrom(&input).ok() && input.empty()) {
        return blob_store_->Get(b_index, value);
    }
    // 否则是普通 Value，直接返回 OK
    return Status::OK();
}


Status DBImpl::FinishGC(const std::vector<GCRecord>& gc_records) {
    // 这一步需要加锁，确保原子写入 WriteBatch
    // 但查询 GetLSMValue 是否需要加锁？
    // 为了保证 Check-And-Set 的原子性，我们在检查期间持有锁。
    std::lock_guard<std::mutex> lock(mutex_);

    int success_count = 0;
    
    for (const auto& rec : gc_records) {
        std::string current_val_str;
        
        // 1. Check: 查询当前 LSM 中的值
        Status s = GetLSMValue(rec.key, &current_val_str);
        
        bool can_update = false;
        
        if (s.ok()) {
            // 解析当前值是否为 BlobIndex
            BlobIndex current_index;
            Slice input(current_val_str);
            if (current_index.DecodeFrom(&input).ok()) {
                // 2. Compare: 比较是否等于 GC 前的旧索引
                if (current_index == rec.old_index) {
                    can_update = true;
                }
            }
        }
        
        // 3. Set: 如果匹配，则更新为新索引
        if (can_update) {
            std::string new_val_str;
            rec.new_index.EncodeTo(&new_val_str);
            
            // 复用 WriteLocked 写入 WAL 和 MemTable
            // 注意：这里我们是在循环里多次调用 WriteLocked，这会产生多条 WAL 日志。
            // 生产环境通常使用 WriteBatch 一次性写入。
            // 但为了复用现有逻辑，循环写是可以接受的（只是性能稍差）。
            WriteLocked(WriteOptions(), kTypeValue, rec.key, new_val_str);
            success_count++;
        } else {
            // 冲突！用户已经更新或删除了该 Key，放弃回填。
            // 新写入 BlobStore 的数据变成了垃圾（浪费了一点空间），下次 GC 会清理它。
            // fprintf(stderr, "[GC] Conflict detected for key: %s\n", rec.key.c_str());
        }
    }
    
    fprintf(stderr, "[GC] Rewrite finished. Success: %d/%lu\n", 
            success_count, gc_records.size());
            
    return Status::OK();
}

Status DBImpl::GarbageCollect() {
    // 1. 判活回调：查询 LSM 确认 BlobIndex 是否有效
    // 注意：RunGC 运行在锁外（耗时操作），所以回调里需要加锁或处理并发
    // 为了简单，我们让 RunGC 先做物理搬运（不查 LSM，或者只查简单的 Bloom），
    // 真正的强一致性检查放在 FinishGC 的 CAS 阶段。
    
    // 但 RunGC 需要一个回调来决定“这个 Key 是否值得搬运”。
    // 如果我们不在 RunGC 阶段查 LSM，就会搬运所有数据（包括已删除的），这就变成了 Full Compaction，效率低但正确。
    // 为了更高效，我们在 RunGC 里也查一次 LSM (无锁或短锁)。
    
    auto is_valid_cb = [&](const Slice& key, const BlobIndex& old_index) {
        std::string val;
        // 这里调用 GetLSMValue 需要注意锁的问题。
        // 如果 GetLSMValue 访问 MemTable，MemTable 是无锁读的，安全。
        // 如果访问 VersionSet，VersionSet 有 Ref 计数，安全。
        // 所以在锁外调用 GetLSMValue 是安全的。
        Status s = GetLSMValue(key, &val);
        if (!s.ok()) return false;
        
        BlobIndex current_index;
        Slice input(val);
        if (current_index.DecodeFrom(&input).ok()) {
            return current_index == old_index;
        }
        return false;
    };

    // 2. 执行物理搬运 (耗时，无锁)
    std::vector<GCRecord> records;
    Status s = blob_store_->RunGC(is_valid_cb, &records);
    if (!s.ok()) return s; // 没找到需要 GC 的文件

    options_.statistics->gc_run_count++;

    // 3. 执行索引回填
    if (!records.empty()) {
        s = FinishGC(records);
        
        // 【新增】更新统计信息
        if (s.ok()) {
            options_.statistics->gc_run_count++;
            options_.statistics->gc_keys_moved += records.size();
            // 这里还可以统计回收了多少字节，需要 RunGC 返回更多信息，暂略
        }
    }
    
    return s;
}

// core/src/db_impl.cc

Status DBImpl::DoCompactionWork(Compaction* c) {
  // 1. 构建输入迭代器 (Week 1 产物)
  Iterator* input = versions_->MakeInputIterator(c, table_cache_, ReadOptions());
  input->SeekToFirst();
  
  Status status;
  std::string current_user_key;
  bool has_current_user_key = false;
  
  // 临时状态
  TableBuilder* builder = nullptr;
  std::unique_ptr<WritableFile> file;
  uint64_t current_output_file_number = 0;
  Compaction::OutputFile current_output;

  // --- Helper: 开启新文件 ---
  auto OpenOutputFile = [&]() -> Status {
      current_output_file_number = versions_->NewFileNumber();
      std::string fname = TableFileName(dbname_, current_output_file_number);
      Status s = NewWritableFile(fname, &file);
      if (!s.ok()) return s;
      
      builder = new TableBuilder(GetOptions(), file.get());
      
      current_output.number = current_output_file_number;
      current_output.smallest.clear();
      current_output.largest.clear();
      return Status::OK();
  };

  // --- Helper: 完成当前文件 ---
  auto FinishOutputFile = [&]() -> Status {
      if (builder == nullptr) return Status::OK();
      
      Status s = builder->Finish();
      if (s.ok()) {
          uint64_t fsize = builder->FileSize();
          s = file->Close();
          if (s.ok()) {
              current_output.file_size = fsize;
              c->AddOutputFile(current_output);
              fprintf(stderr, "[Compaction] Generated table #%lu@L%d, entries: %lu\n", 
                      current_output.number, c->level() + 1, builder->NumEntries());
          }
      }
      delete builder;
      builder = nullptr;
      file.reset();
      return s;
  };

  // 2. 遍历输入流
  const InternalKeyComparator* icmp = versions_->icmp();
  
  for (; input->Valid(); input->Next()) {
      Slice key = input->key();
      Slice user_key = ExtractUserKey(key);

      // --- 核心丢弃规则 ---
      bool drop = false;
      
      if (has_current_user_key && 
          icmp->user_key_compare(user_key, Slice(current_user_key)) == 0) {
          // 规则 1: 相同的 User Key，只保留第一个 (SeqNum 最大的)
          // 既然遇到了相同的 Key，说明当前这个是旧版本，直接丢弃
          drop = true;
      } 
      else {
          // 遇到了新 Key
          current_user_key = user_key.ToString();
          has_current_user_key = true;
          
          // 解析 Tag 判断是否是删除标记
          uint64_t tag = DecodeFixed64(key.data() + key.size() - 8);
          ValueType type = static_cast<ValueType>(tag & 0xff);

          if (type == kTypeDeletion) {
              // 规则 2: 如果是 Tombstone，且 Base Level 没有该 Key，则丢弃
              // "Base Level" 指的是 Compaction 目标层及以下
              if (!versions_->current()->OverlapInLevel(c->level() + 2, user_key, key)) {
                  drop = true;
              }
          }
      }

      // --- Blob GC 联动 ---
      if (drop) {
          // 如果我们要丢弃这条记录，且它是一个有效的 BlobIndex，通知 BlobStore
          Slice val = input->value();
          BlobIndex blob_idx;
          // 尝试解码，如果是 BlobIndex 则通知
          Slice input_slice(val);
          if (blob_idx.DecodeFrom(&input_slice).ok() && input_slice.empty()) {
               blob_store_->NotifyGarbage(blob_idx.file_id, blob_idx.size);
          }
          continue; // 跳过写入
      }

      // --- 写入 Output ---
      if (builder == nullptr) {
          status = OpenOutputFile();
          if (!status.ok()) break;
          current_output.smallest = key.ToString();
      }
      current_output.largest = key.ToString();
      builder->Add(key, input->value());

      // 检查文件大小是否超限
      if (builder->FileSize() >= c->MaxOutputFileSize()) {
          status = FinishOutputFile();
          if (!status.ok()) break;
      }
  }

  if (status.ok()) {
      status = input->status();
  }
  delete input;
  input = nullptr;

  // 结束最后一个文件
  if (status.ok() && builder != nullptr) {
      status = FinishOutputFile();
  }
  
  if (builder != nullptr) delete builder; // 清理异常情况

  // 3. 提交变更 (Install)
  if (status.ok()) {
      // 构造 VersionEdit
      VersionEdit edit;
      c->AddToEdit(&edit); // 记录 Delete Inputs, Add Outputs
      
      // 原子更新 Version
      // 注意：DoCompactionWork 是在锁外执行的，但 LogAndApply 需要锁
      std::lock_guard<std::mutex> l(mutex_);
      status = versions_->LogAndApply(&edit, &mutex_);
      
      // 更新完 Version 后，我们需要手动清理 obsolete files (旧文件)
      // 在工业级实现中，通常由 DeleteObsoleteFiles() 统一处理
      // 这里简化：VersionEdit 会导致旧 Version 引用计数下降，
      // 但物理文件删除目前只能靠下一次重启或者专门的清理线程。
      // Day 2 暂不实现物理删除 SST，只做逻辑删除。
  }

  return status;
}

// 后台循环
void DBImpl::BGWork() {
    while (true) {
        // 使用 unique_lock，以便手动 unlock
        std::unique_lock<std::mutex> lock(mutex_);
        
        if (!bg_running_) break;

        // 1. 检查 Flush (简化)
        if (imm_ != nullptr) {
            // ... (Flush logic) ...
            MakeRoomForWrite(false); // 假设这里处理
        }

        // 2. 尝试 Compaction
        versions_->current()->Finalize();
        Compaction* c = versions_->PickCompaction();

        if (c != nullptr) {
            Status s;
            
            // 【关键分支】
            if (c->IsTrivialMove()) {
                // --- Trivial Move (持有锁执行) ---
                // 不需要 IO，直接更新 Version
                std::shared_ptr<VersionEdit> edit = std::make_shared<VersionEdit>();
                FileMetaData* f = c->inputs(0)->at(0);
                
                // 1. 删除旧层文件
                edit->DeleteFile(c->level(), f->file_number);
                // 2. 添加到新层 (直接复用元数据)
                edit->AddFile(c->level() + 1, f->file_number, f->file_size, 
                              Slice(f->smallest), Slice(f->largest));
                
                // 应用变更
                s = versions_->LogAndApply(edit.get(), &mutex_);
                
                fprintf(stderr, "[Compaction] Trivial Move: File #%lu L%d -> L%d. Status: %s\n",
                        f->file_number, c->level(), c->level()+1, s.ToString().c_str());
                
                // Trivial Move 很快，锁一直持有没问题
                
            } else {
                // --- Normal Compaction (释放锁执行) ---
                fprintf(stderr, "[Compaction] Picked Level %d -> %d. Input0: %d, Input1: %d\n",
                        c->level(), c->level()+1, 
                        c->num_input_files(0), c->num_input_files(1));
                
                // 【关键】释放锁，允许前台 Put/Get 继续运行
                lock.unlock();
                
                // 执行耗时操作
                s = DoCompactionWork(c);
                
                // 重新获取锁 (为了下一次循环检测或 wait)
                lock.lock();
                
                if (!s.ok()) {
                     fprintf(stderr, "[Compaction] Failed: %s\n", s.ToString().c_str());
                } else {
                     fprintf(stderr, "[Compaction] Success!\n");
                }
            }
            
            delete c;
            // 做了任务，这轮不睡，直接 continue 检查还有没有
            continue;
            
        } else {
            // 没有 Compaction 任务，释放锁去做 GC
            lock.unlock();
            
            GarbageCollect();
            
            // 重新加锁准备睡眠
            lock.lock();
            // 再次检查退出标志
            if (!bg_running_) break;
            
            // 睡眠等待 (释放锁)
            bg_cv_.wait_for(lock, std::chrono::seconds(5));
        }
    }
}

} // namespace titankv