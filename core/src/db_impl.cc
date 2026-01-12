#include "titankv/db_impl.h"
#include "wal/log_reader.h"
#include "util/coding.h"
#include "lsm/table_builder.h"
#include "lsm/version_edit.h"
#include "lsm/table.h"          
#include "lsm/merging_iterator.h" 
#include "lsm/table_cache.h"
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
  impl->StartBackgroundThread();
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
  //bg_thread_ = std::thread(&DBImpl::BGWork, this);
}

void DBImpl::StartBackgroundThread() {
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
  // 1. 创建目录 (保持不变)
  if (!std::filesystem::exists(dbname_)) {
    if (options_.create_if_missing) {
      std::filesystem::create_directories(dbname_);
    } else {
      return Status::InvalidArgument("DB directory does not exist");
    }
  }

  // 2. 恢复 VersionSet (保持不变)
  bool save_manifest = false;
  Status s = versions_->Recover(&save_manifest);
  if (!s.ok() && !s.IsNotFound()) return s;

  // 3. 扫描 WAL 文件 (保持不变)
  uint64_t min_log = versions_->LogNumber();
  std::vector<std::string> filenames;
  for (const auto& entry : std::filesystem::directory_iterator(dbname_)) {
      filenames.push_back(entry.path().filename().string());
  }
  
  std::vector<uint64_t> logs;
  for (const auto& fname : filenames) {
      uint64_t number;
      if (fname.length() > 4 && fname.substr(fname.length() - 4) == ".log") {
          try {
             number = std::stoull(fname.substr(0, fname.length() - 4));
             if (number >= min_log) logs.push_back(number);
          } catch (...) { 
             if (fname == "wal.log" && min_log == 0) logs.push_back(0);
          }
      }
  }
  std::sort(logs.begin(), logs.end());

  // 4. 重放 WAL (保持不变)
  for (uint64_t log_num : logs) {
      std::string log_path = (log_num == 0 && std::filesystem::exists(dbname_ + "/wal.log")) ? 
                             dbname_ + "/wal.log" : LogFileName(dbname_, log_num);
      
      std::unique_ptr<SequentialFile> file;
      s = NewSequentialFile(log_path, &file);
      if (!s.ok()) continue;

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
        
        // 恢复 Sequence (取最大值)
        if (versions_->LastSequence() > last_sequence_) {
            last_sequence_ = versions_->LastSequence(); 
        }
        last_sequence_++; 
        mem_->Add(last_sequence_, type, key, value);
      }
  }

  // =========================================================
  // 【关键修复】5. 准备新环境：Flush 重放的数据 + 切换 Log
  // =========================================================
  
  uint64_t new_log_number = versions_->NewFileNumber();
  VersionEdit edit;
  
  // 如果 MemTable 里有重放的数据，必须先 Flush 到磁盘！
  if (mem_->ApproximateMemoryUsage() > 0) {
      uint64_t file_num = 0;
      // 调用 WriteLevel0Table 生成 SSTable
      // 注意：此时 BGWork 可能也在运行，需要加锁保护 pending_outputs_ (WriteLevel0Table 内部没加锁？)
      // 但 WriteLevel0Table 会操作 pending_outputs_，它是非线程安全的集合？
      // 在 DBImpl::Recover 运行时，理论上外部不会有并发 Put/Get，但 BGWork 线程已经启动了。
      // 为了安全，我们加个锁调用 WriteLevel0Table
      
      {
          std::lock_guard<std::mutex> l(mutex_);
          s = WriteLevel0Table(mem_, &edit, &file_num);
      }
      
      if (!s.ok()) return s;

      // 移除保护 (因为 WriteLevel0Table 加了 insert，但没 erase)
      if (file_num > 0) {
          std::lock_guard<std::mutex> l(mutex_);
          pending_outputs_.erase(file_num);
      }
      
      // Flush 完后，旧 MemTable 的使命结束了，换个新的
      mem_->Unref();
      mem_ = new MemTable(InternalKeyComparator());
      mem_->Ref();
  }
  
  // 创建新 Log 文件
  std::unique_ptr<WritableFile> lfile;
  s = NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
  if (!s.ok()) return s;
  
  logfile_ = std::move(lfile);
  log_ = std::make_unique<log::Writer>(logfile_.get());
  
  // 更新 Manifest：
  // 1. 设置新的 LogNumber (表示之前的 Log 都作废了)
  // 2. 记录刚才 Flush 的新文件 (edit 里如果有的话)
  // 3. 记录 LastSequence
  edit.SetLogNumber(new_log_number);
  edit.SetLastSequence(last_sequence_);
  
  // 原子应用
  {
      std::lock_guard<std::mutex> l(mutex_);
      s = versions_->LogAndApply(&edit, &mutex_);
  }

  return s;
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
    
    // Flow Control 逻辑 (保持不变)
    {
        if (versions_->current() != nullptr) {
             int l0_files = versions_->current()->GetFiles(0).size();
             const int kL0_Stop = 12;
             while (l0_files >= kL0_Stop && bg_running_) {
                 bg_cv_.notify_all(); 
                 mutex_.unlock();
                 std::this_thread::sleep_for(std::chrono::milliseconds(10));
                 mutex_.lock();
                 l0_files = versions_->current()->GetFiles(0).size();
             }
             const int kL0_Slow = 8;
             if (l0_files >= kL0_Slow) {
                 bg_cv_.notify_all(); 
                 mutex_.unlock();
                 std::this_thread::sleep_for(std::chrono::milliseconds(1));
                 mutex_.lock();
             }
        }
    }

    if (!allow_switch) {
        return Status::OK();
    }

    // 1. 如果有残留的 imm_，先刷盘
    if (imm_ != nullptr) {
        VersionEdit edit;
        uint64_t file_num = 0;
        Status s = WriteLevel0Table(imm_, &edit, &file_num);
        if (!s.ok()) return s;
        
        s = versions_->LogAndApply(&edit, &mutex_);
        if (file_num > 0) pending_outputs_.erase(file_num);
        if (!s.ok()) return s;

        imm_->Unref();
        imm_ = nullptr;
    }

    // 2. 切换 MemTable & WAL
    // ---------------------------------------------------------
    imm_ = mem_;
    mem_ = new MemTable(InternalKeyComparator());
    mem_->Ref();

    // 切换 Log 文件
    // 获取新编号，但暂时不写入 Manifest 的 LogNumber
    uint64_t new_log_number = versions_->NewFileNumber();
    std::unique_ptr<WritableFile> lfile;
    Status s = NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
    if (!s.ok()) return s;
    
    logfile_ = std::move(lfile);
    log_ = std::make_unique<log::Writer>(logfile_.get());

    // 【关键修改】这里不再调用 LogAndApply 更新 LogNumber！
    // 此时 Manifest 依然指向旧 Log。如果现在 Crash，Recover 会重放旧 Log (imm_) 和新 Log (mem_)，数据安全。
    // ---------------------------------------------------------

    // 3. Flush 刚刚切换出来的 imm_
    {
        VersionEdit imm_edit;
        
        // 【关键修改】在 Flush 成功后的 Edit 中，顺便更新 LogNumber
        // 这表示：imm_ 已经落盘为 SST 了，旧 Log 可以废弃了，Log 起点推进到 new_log_number
        imm_edit.SetLogNumber(new_log_number); 

        uint64_t file_num = 0;
        s = WriteLevel0Table(imm_, &imm_edit, &file_num);
        if (!s.ok()) return s;
        
        // 原子提交：增加新 SST 文件 + 推进 LogNumber
        s = versions_->LogAndApply(&imm_edit, &mutex_);
        
        if (file_num > 0) pending_outputs_.erase(file_num);
        if (!s.ok()) return s;
        
        imm_->Unref();
        imm_ = nullptr;
    }
    
    bg_cv_.notify_one(); 
    return Status::OK();
}

// 【修改】增加 file_number 输出参数
Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit, uint64_t* file_number) {
  // 1. 分配文件号
  *file_number = versions_->NewFileNumber();
  
  // 2. 加入保护 (开始保护)
  pending_outputs_.insert(*file_number);
  
  std::string fname = TableFileName(dbname_, *file_number);
  std::unique_ptr<WritableFile> file;
  Status s = NewWritableFile(fname, &file);
  
  // 注意：如果打开失败，需要立即移除保护，否则该 ID 永远泄露在 pending_outputs_ 中
  if (!s.ok()) {
      pending_outputs_.erase(*file_number);
      return s;
  }

  // 3. 构建 Table
  TableBuilder builder(options_, file.get());
  Iterator* iter = mem->NewIterator();
  iter->SeekToFirst();
  
  if (!iter->Valid()) {
      // 空表处理
      delete iter;
      builder.Abandon();
      pending_outputs_.erase(*file_number); // 移除保护
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

  // 4. 完成写入
  s = builder.Finish();
  if (s.ok()) {
      s = file->Close();
  }

  if (s.ok() && builder.NumEntries() > 0) {
      uint64_t file_size = builder.FileSize();
      edit->AddFile(0, *file_number, file_size, Slice(smallest), Slice(largest));
      fprintf(stderr, "[DBImpl] Flushed SSTable #%lu. Entries: %lu, Size: %lu\n", 
              *file_number, builder.NumEntries(), file_size);
      // 【关键修复】将当前 Sequence 写入 Edit
      // 这样 Manifest 就会记录下“此时此刻系统已经写到了哪个 Sequence”
      edit->SetLastSequence(last_sequence_);
      
      fprintf(stderr, "[DBImpl] Flushed SSTable #%lu. Entries: %lu, Seq: %lu\n", 
              *file_number, builder.NumEntries(), last_sequence_.load());
      // 【关键修复】这里绝对不要 erase pending_outputs_！
      // 文件虽然写完了，但还没进 Version，必须继续保护！
  } else {
      // 失败或空文件：清理
      std::filesystem::remove(fname);
      pending_outputs_.erase(*file_number); // 移除保护
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

// core/src/db_impl.cc

Status DBImpl::DoCompactionWork(Compaction* c) {
    fprintf(stderr, "[Compaction] START Level %d -> %d\n", c->level(), c->level()+1);
    // 【新增调试】打印 Input 文件号
    fprintf(stderr, "[Compaction] Doing L%d->L%d. Input0: ", c->level(), c->level()+1);
    for (auto* f : *c->inputs(0)) fprintf(stderr, "%lu ", f->file_number);
    fprintf(stderr, "\n");
    // 1. 构建输入迭代器
    Iterator* input = versions_->MakeInputIterator(c, table_cache_, ReadOptions());
    input->SeekToFirst();
    
    Status status;
    std::string current_user_key;
    bool has_current_user_key = false;
    
    // 【关键调试】打印阈值
    uint64_t limit = c->MaxOutputFileSize();
    fprintf(stderr, "[Compaction] Start. Limit: %lu bytes. Level: %d\n", limit, c->level());
    
    TableBuilder* builder = nullptr;
    std::unique_ptr<WritableFile> file;
    uint64_t current_output_file_number = 0;
    Compaction::OutputFile current_output;
    
    // 【关键】手动计数器
    uint64_t current_file_bytes = 0;
    // 【新增】记录本次生成的所有文件号
    std::vector<uint64_t> produced_files;
    auto OpenOutputFile = [&]() -> Status {
        mutex_.lock();
        current_output_file_number = versions_->NewFileNumber();
        pending_outputs_.insert(current_output_file_number);
        mutex_.unlock();
        
	   produced_files.push_back(current_output_file_number);
        std::string fname = TableFileName(dbname_, current_output_file_number);
        Status s = NewWritableFile(fname, &file);
        if (!s.ok()) return s;
        
        Options builder_opts = options_; 
        // 强制 Block 大小 4KB，避免过早 Flush
        builder_opts.block_size = 4 * 1024;
        
        builder = new TableBuilder(builder_opts, file.get());
        
        current_output.number = current_output_file_number;
        current_output.smallest.clear();
        current_output.largest.clear();
        
        // 重置计数器
        current_file_bytes = 0; 
        return Status::OK();
    };

    auto FinishOutputFile = [&]() -> Status {
        if (builder == nullptr) return Status::OK();
        
        Status s = builder->Finish();
        if (s.ok()) {
            uint64_t fsize = builder->FileSize();
            s = file->Close();
            if (s.ok()) {
                current_output.file_size = fsize;
                c->AddOutputFile(current_output);
                // 打印生成的统计信息
                fprintf(stderr, "[Compaction] Generated #%lu@L%d. Entries: %lu. Size: %lu. (Limit: %lu)\n", 
                        current_output.number, c->level() + 1, builder->NumEntries(), fsize, limit);
            }
        }
        delete builder;
        builder = nullptr;
        file.reset();

        mutex_.lock();
        pending_outputs_.erase(current_output.number); 
        mutex_.unlock();

        return s;
    };

    const InternalKeyComparator* icmp = versions_->icmp();
    
    for (; input->Valid(); input->Next()) {
        Slice key = input->key();
        // 【关键修复】防御性检查：如果 Input Key 非法，直接跳过！
        // 这能防止脏数据扩散到新文件
        if (key.size() < 8) {
            fprintf(stderr, "[Compaction] Error: Skipped invalid key (len=%lu)\n", key.size());
            continue;
        }
        Slice user_key = ExtractUserKey(key);

        bool drop = false;
        
        if (has_current_user_key && 
            icmp->user_key_compare(user_key, Slice(current_user_key)) == 0) {
            drop = true;
            BlobIndex blob_idx;
            Slice val_input = input->value();
            if (blob_idx.DecodeFrom(&val_input).ok()) {
                 blob_store_->NotifyGarbage(blob_idx.file_id, blob_idx.size);
            }
        } else {
            current_user_key = user_key.ToString();
            has_current_user_key = true;
            
            uint64_t tag = DecodeFixed64(key.data() + key.size() - 8);
            ValueType type = static_cast<ValueType>(tag & 0xff);

            if (type == kTypeDeletion) {
                if (!versions_->current()->OverlapInLevel(c->level() + 2, user_key, key)) {
                    drop = true;
                }
            }
        }

        if (!drop) {
            if (builder == nullptr) {
                status = OpenOutputFile();
                if (!status.ok()) break;
                current_output.smallest = key.ToString();
            }
            current_output.largest = key.ToString();
            
            builder->Add(key, input->value());
            
            // 【关键修复】累加大小
            // 这里我们累加的是 Raw Key/Value 大小，这是最准确的估算
            current_file_bytes += key.size() + input->value().size();
		  // 打印：当前累计大小 vs 阈值
            // 请务必把这行加上，这是破案的关键
            //fprintf(stderr, "[DEBUG-CHECK] Current: %lu, Threshold: %lu\n", 
                    //current_file_bytes, c->MaxOutputFileSize());

            // 【关键判断】只使用手动计数器，不依赖 builder->FileSize()
            if (current_file_bytes >= limit) {
                // fprintf(stderr, "[Compaction] Splitting file. Bytes: %lu >= Limit: %lu\n", current_file_bytes, limit);
                status = FinishOutputFile();
                if (!status.ok()) break;
            }
        }
    }
    
    if (status.ok()) status = input->status();
    delete input;
    
    if (status.ok() && builder != nullptr) {
        status = FinishOutputFile();
    }
    
    if (builder != nullptr) delete builder;

    if (status.ok()) {
        VersionEdit edit;
        c->AddToEdit(&edit); 
        std::lock_guard<std::mutex> l(mutex_);
        status = versions_->LogAndApply(&edit, &mutex_);
    }

    // 【关键】LogAndApply 结束后（或者失败后），统一移除保护
    {
        std::lock_guard<std::mutex> l(mutex_);
        for (uint64_t num : produced_files) {
            pending_outputs_.erase(num);
        }
    }
    fprintf(stderr, "[Compaction] END Status: %s\n", status.ToString().c_str());
    return status;
}

// 后台循环
void DBImpl::BGWork() {
    while (true) {
        bool did_compaction = false;
        
        std::unique_lock<std::mutex> lock(mutex_);
        
        if (!bg_running_) break;

        // 1. Flush Check (简略)
        if (imm_ != nullptr) {
             // MakeRoomForWrite(false); 
        }

        // 2. Compaction Check
        versions_->current()->Finalize();
        Compaction* c = versions_->PickCompaction();

        if (c != nullptr) {
            Status s;
            
            if (c->IsTrivialMove()) {
                // --- Trivial Move ---
                std::shared_ptr<VersionEdit> edit = std::make_shared<VersionEdit>();
                FileMetaData* f = c->inputs(0)->at(0);
                
                edit->DeleteFile(c->level(), f->file_number);
                edit->AddFile(c->level() + 1, f->file_number, f->file_size, 
                              Slice(f->smallest), Slice(f->largest));
                
                // 保存变量用于打印
                uint64_t fnum = f->file_number;
                int lvl = c->level();

                s = versions_->LogAndApply(edit.get(), &mutex_);
                
                // 【修复】在日志中使用 fnum 和 lvl
                fprintf(stderr, "[Compaction] Trivial Move: File #%lu L%d -> L%d. Status: %s\n",
                        fnum, lvl, lvl+1, s.ToString().c_str());
                
            } else {
                // --- Normal Compaction ---
                fprintf(stderr, "[Compaction] Picked Level %d -> %d. Input0: %d, Input1: %d\n",
                        c->level(), c->level()+1, 
                        c->num_input_files(0), c->num_input_files(1));
                
                lock.unlock();
                s = DoCompactionWork(c);
                lock.lock();
                
                if (!s.ok()) {
                     fprintf(stderr, "[Compaction] Failed: %s\n", s.ToString().c_str());
                } else {
                     fprintf(stderr, "[Compaction] Success!\n");
                }
            }
            
            delete c;
            
            // 释放锁进行清理
            lock.unlock();
            DeleteObsoleteFiles();
            lock.lock();

            did_compaction = true;
            
            // 有任务就不睡了，继续下一轮
            continue;
            
        } else {
            // --- Idle ---
            lock.unlock();
            GarbageCollect();
            lock.lock();
            
            if (!bg_running_) break;
            
            // 【修复】使用 did_compaction 决定睡眠时间
            // 如果刚才忙（did_compaction=true），可能只是暂时没任务，短睡
            // 如果本来就闲，长睡
            // 但因为上面的 continue，走到这里肯定是 did_compaction=false
            
            bg_cv_.wait_for(lock, std::chrono::seconds(5));
        }
    }
}
void DBImpl::DeleteObsoleteFiles() {
    if (!bg_running_) return;

    std::set<uint64_t> live;
    {
        std::lock_guard<std::mutex> l(mutex_);
        versions_->AddLiveFiles(&live);
        
        // 【关键修复】将 pending_outputs 也视为 live
        for (auto num : pending_outputs_) {
            live.insert(num);
        }
    }

    

    std::vector<std::string> filenames;
    for (const auto& entry : std::filesystem::directory_iterator(dbname_)) {
        filenames.push_back(entry.path().filename().string());
    }

        fprintf(stderr, "[Cleaner] Live set size: %lu. Files on disk: %lu\n", 
            live.size(), filenames.size());


    // 避免在删除文件时持有 mutex_
    // (注意：这里没有 mutex_.unlock()，因为我们假设调用此函数前已经释放了锁)
    // 根据 BGWork 的逻辑，调用 DeleteObsoleteFiles 时并未持有锁。
    int deleted_count = 0;
    for (const auto& fname : filenames) {
        uint64_t number;
        bool keep = true;

        if (fname.length() > 4 && fname.substr(fname.length() - 4) == ".sst") {
            try {
                number = std::stoull(fname.substr(0, fname.length() - 4));
                if (live.find(number) == live.end()) {
                    keep = false;
                }
            } catch (...) { keep = true; }
        } else {
            keep = true;
        }

        if (!keep) {

            table_cache_->Evict(number);
            std::filesystem::remove(dbname_ + "/" + fname);
            deleted_count++;
        }
    }
}

Status DBImpl::Write(const WriteOptions& opt, WriteBatch* batch) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // 1. 检查空间
    Status s = MakeRoomForWrite(false);
    if (!s.ok()) return s;

    // 2. 遍历 Batch 中的 Entry
    // 注意：WriteBatch 的迭代器需要自己实现或暴露
    // 假设 WriteBatch 内部有一个 std::vector<Entry> entries_;
    for (const auto& entry : batch->entries()) {
        // 复用 WriteLocked 逻辑
        // 注意：WriteLocked 是原子的吗？在锁内是原子的。
        // 但如果我们要保证整个 Batch 原子性（要么全进 WAL，要么全不进），
        // 我们应该构造一个大的 WAL Record 包含所有 Entry。
        
        // Day 4 简化：在锁内循环调用 WriteLocked。
        // 虽然 WAL 是分条写的，但因为持有锁，外界看来是原子的。
        ValueType type = (entry.type == kTypeValue) ? kTypeValue : kTypeDeletion;
        s = WriteLocked(opt, type, entry.key, entry.value);
        if (!s.ok()) return s;
    }
    
    return Status::OK();
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
    std::lock_guard<std::mutex> lock(mutex_);
    Version* v = versions_->current();
    
    for (int i = 0; i < n; i++) {
        // Convert user key to internal key for comparison
        // Start: Sequence Max (查最新的)
        // Limit: Sequence Max
        // 注意：LSM 里的 Key 都是 InternalKey
        std::string k1 = EncodeInternalKey(range[i].start, kMaxSequenceNumber, kTypeValue);
        std::string k2 = EncodeInternalKey(range[i].limit, kMaxSequenceNumber, kTypeValue);
        
        uint64_t start_offset = versions_->ApproximateOffsetOf(v, k1);
        uint64_t limit_offset = versions_->ApproximateOffsetOf(v, k2);
        
        sizes[i] = (limit_offset >= start_offset) ? (limit_offset - start_offset) : 0;
    }
}

// 辅助函数：构造 InternalKey String (需在 db_impl.cc 或 util 中实现)
std::string DBImpl::EncodeInternalKey(const Slice& user_key, uint64_t seq, ValueType type) {
    std::string s;
    s.append(user_key.data(), user_key.size());
    PutFixed64(&s, PackSequenceAndType(seq, type));
    return s;
}

Status DBImpl::DumpSST(const Slice& start, const Slice& end, 
                       const std::string& fname, uint64_t seq) {
    // 1. 强制 Flush MemTable (确保数据落盘，简化 Iterator 逻辑)
    {
        std::lock_guard<std::mutex> l(mutex_);
        // 如果 mem 不为空，flush 它
        if (mem_->ApproximateMemoryUsage() > 0) {
             MakeRoomForWrite(true); 
        }
    }
    
    // 2. 获取快照
    SequenceNumber snapshot = (seq == 0) ? last_sequence_.load() : seq;

    // 3. 构建 Iterator (遍历所有 SSTable)
    // 我们需要一个能遍历整个 DB 的迭代器
    // 复用 VersionSet::MakeInputIterator 的逻辑，但这里是全量
    Version* v = versions_->current();
    v->Ref();

    std::vector<Iterator*> iters;
    // L0 Files
    const auto& l0 = v->GetFiles(0);
    for (auto* f : l0) {
        iters.push_back(table_cache_->NewIterator(ReadOptions(), f->file_number, f->file_size));
    }
    // L1+ (每层一个 LevelIterator)
    // Week 8 Day 4 实现了 NewLevelIterator
    for (int level = 1; level < kNumLevels; level++) {
        const auto& files = v->GetFiles(level);
        if (!files.empty()) {
            iters.push_back(NewLevelIterator(*versions_->icmp(), table_cache_, files, ReadOptions()));
        }
    }
    
    // 归并
    InternalKeyComparator icmp;
    Iterator* iter = NewMergingIterator(&icmp, iters.data(), iters.size());

    // 4. 创建 Builder
    std::unique_ptr<WritableFile> file;
    Status s = NewWritableFile(fname, &file);
    if (!s.ok()) {
        delete iter;
        v->Unref();
        return s;
    }
    TableBuilder builder(options_, file.get());

    // 5. 扫描并写入
    // 构造查找 Key (Start)
    LookupKey lkey(start, kMaxSequenceNumber); // 找最大的 Seq，保证包含所有版本
    iter->Seek(lkey.internal_key());
    
    for (; iter->Valid(); iter->Next()) {
        Slice key = iter->key();
        
        // 检查 Key 是否超出 End 范围
        if (end.size() > 0) {
            // 比较 User Key
            Slice user_key = ExtractUserKey(key);
            if (user_key.compare(end) >= 0) {
                break;
            }
        }
        
        // 过滤掉 Seq > snapshot 的数据
        // (省略，简化处理：全部导出，接收端再过滤)

        // 处理 BlobIndex
        Slice value = iter->value();
        std::string raw_val;
        BlobIndex b_idx;
        Slice input(value);
        if (b_idx.DecodeFrom(&input).ok() && input.empty()) {
             // 读 Blob
             Status bs = blob_store_->Get(b_idx, &raw_val);
             if (bs.ok()) {
                 builder.Add(key, raw_val); // 写入真实 Value
             } else {
                 // Blob 丢失？记录错误或跳过
             }
        } else {
             builder.Add(key, value);
        }
    }

    s = builder.Finish();
    if (s.ok()) s = file->Close();

    delete iter;
    v->Unref();
    return s;
}

Status DBImpl::DeleteRange(const WriteOptions& opt, const Slice& start, const Slice& end) {
    std::vector<std::string> keys_to_delete;

    // 1. 构建全量迭代器 (需要在锁内获取 Version 引用)
    Iterator* iter = nullptr;
    Version* current = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        current = versions_->current();
        current->Ref();

        // 构造迭代器列表
        std::vector<Iterator*> iters;
        // MemTable
        iters.push_back(mem_->NewIterator());
        // Immutable
        if (imm_) iters.push_back(imm_->NewIterator());
        // SSTables
        for (int level = 0; level <kNumLevels; level++) {
            const auto& files = current->GetFiles(level);
            if (!files.empty()) {
                if (level == 0) {
                     for (auto* f : files) {
                         iters.push_back(table_cache_->NewIterator(ReadOptions(), f->file_number, f->file_size));
                     }
                } else {
                     iters.push_back(NewLevelIterator(*versions_->icmp(), table_cache_, files, ReadOptions()));
                }
            }
        }
        InternalKeyComparator icmp; // 临时构造
        iter = NewMergingIterator(&icmp, iters.data(), iters.size());
    }

    // 2. 扫描范围 (耗时操作，建议在锁外进行？)
    // 注意：如果在扫描过程中数据变了怎么办？
    // LSM 的 Iterator 是 Snapshot Isolation 的。
    // 但是，我们在扫描过程中并没有持有锁，这意味着在此期间新写入的数据可能不会被扫描到。
    // 对于 DeleteRange 来说，通常我们要删除的是“过去的数据”，所以这没问题。
    // 真正的问题是：如果我们在扫描的同时，其他线程也在 Delete 这些 Key，会不会冲突？
    // WriteLocked 会处理并发写入，所以 Delete 操作本身是安全的。

    // 构造查找 Key (Start)
    // 这里的 start/end 是编码后的 DataKey (User Key)
    // 我们需要构造 Internal Key 用于 Seek
    LookupKey lkey(start, kMaxSequenceNumber);
    iter->Seek(lkey.internal_key());

    while (iter->Valid()) {
        Slice key = iter->key();
        Slice user_key = ExtractUserKey(key);

        // 检查是否超出 End
        if (user_key.compare(end) >= 0) {
            break;
        }

        // 收集 Key (深拷贝)
        keys_to_delete.push_back(user_key.ToString());
        
        iter->Next();
    }
    
    delete iter;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        current->Unref();
    }

    // 3. 执行批量删除
    // 为了防止一次 WriteBatch 太大，可以分批删
    const size_t kBatchSize = 1000;
    for (size_t i = 0; i < keys_to_delete.size(); i += kBatchSize) {
        WriteBatch batch;
        for (size_t j = i; j < i + kBatchSize && j < keys_to_delete.size(); ++j) {
            batch.Delete(keys_to_delete[j]);
        }
        // 调用 Write (会自动加锁)
        // 这里的 opt 可以复用传入的，或者强制 sync=false 提高性能
        Status s = Write(opt, &batch);
        if (!s.ok()) return s;
    }
    
    fprintf(stderr, "[DeleteRange] Deleted %lu keys in range.\n", keys_to_delete.size());
    return Status::OK();
}

Status DBImpl::IngestSST(const std::string& fname) {
    // 1. 打开 SST 文件
    std::unique_ptr<RandomAccessFile> file;
    Status s = NewRandomAccessFile(fname, &file);
    if (!s.ok()) return s;
    
    uint64_t fsize = std::filesystem::file_size(fname);
    Table* table;
    s = Table::Open(options_, file.release(), 0, fsize, &table);
    if (!s.ok()) return s;
    
    // 2. 遍历并写入
    Iterator* iter = table->NewIterator(ReadOptions());
    iter->SeekToFirst();
    
    // 使用 WriteBatch 优化
    while (iter->Valid()) {
        Slice key = iter->key(); // Internal Key
        Slice val = iter->value();
        
        // 解析 User Key 和 Value
        Slice user_key = ExtractUserKey(key);
        // 注意：SST 里存的是 Raw Value 还是 Blob Index？
        // DumpSST 存的是 Raw Value。
        // 所以我们直接 Put。
        
        Put(WriteOptions(), user_key, val);
        iter->Next();
    }
    
    delete iter;
    delete table;
    return Status::OK();
}

Status DBImpl::PutCF(CFType cf, const Slice& key, const Slice& value, uint64_t ts) {
    std::string encoded_key;
    if (cf == kCFLock) {
        encoded_key = EncodeLockKey(key);
    } else {
        encoded_key = EncodeMvccKey(static_cast<char>(cf), key, ts);
    }
    
    // 调用底层的 Put (它会加锁、写 WAL、写 MemTable)
    return Put(WriteOptions(), encoded_key, value);
}

Status DBImpl::DeleteCF(CFType cf, const Slice& key, uint64_t ts) {
    std::string encoded_key;
    if (cf == kCFLock) {
        encoded_key = EncodeLockKey(key);
    } else {
        encoded_key = EncodeMvccKey(static_cast<char>(cf), key, ts);
    }
    
    return Delete(WriteOptions(), encoded_key);
}

Status DBImpl::GetCF(CFType cf, const Slice& key, std::string* value, uint64_t ts) {
    std::string encoded_key;
    if (cf == kCFLock) {
        encoded_key = EncodeLockKey(key);
    } else {
        encoded_key = EncodeMvccKey(static_cast<char>(cf), key, ts);
    }
    
    return Get(ReadOptions(), encoded_key, value);
}

Iterator* DBImpl::NewIterator(const ReadOptions& options, CFType cf) {
    std::vector<Iterator*> list;
    
    // 1. 获取 Version (加引用)
    // 注意：迭代器需要持有 Version 的引用，直到迭代器销毁。
    // 标准做法是：Iterator 析构时 Unref。
    // 我们在 Week 7 实现了 RegisterCleanup。
    Version* current = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        current = versions_->current();
        current->Ref();
    }

    // 2. MemTable
    list.push_back(mem_->NewIterator());
    if (imm_ != nullptr) {
        list.push_back(imm_->NewIterator());
    }

    // 3. SSTables
    // L0
    const std::vector<FileMetaData*>& l0 = current->GetFiles(0);
    for (FileMetaData* f : l0) {
        list.push_back(table_cache_->NewIterator(options, f->file_number, f->file_size));
    }
    
    // L1+ (每层一个 LevelIterator)
    // 假设 kNumLevels = 7
    for (int level = 1; level < 7; level++) {
        const std::vector<FileMetaData*>& files = current->GetFiles(level);
        if (!files.empty()) {
            // NewLevelIterator 是 Week 8 实现的
            list.push_back(NewLevelIterator(*versions_->icmp(), table_cache_, files, options));
        }
    }
    
    // 4. 合并
    Iterator* internal_iter = NewMergingIterator(versions_->icmp(), list.data(), list.size());

    // 5. 注册清理函数 (释放 Version)
    internal_iter->RegisterCleanup([](void* arg1, void* arg2) {
        Version* v = reinterpret_cast<Version*>(arg1);
        v->Unref();
    }, current, nullptr);

    return internal_iter;
}

} // namespace titankv