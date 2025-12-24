#include "titankv/db_impl.h"
#include "wal/log_reader.h"
#include "util/coding.h"
#include "util/filename.h"
#include "lsm/table_builder.h"
#include "lsm/version_edit.h" 
#include <filesystem>
#include <iostream>
#include <algorithm>

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
  // 注意：Open 是单线程环境，不需要加锁
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
      table_cache_(nullptr) {
  
  // 初始化比较器
  InternalKeyComparator cmp;
  mem_ = new MemTable(cmp);
  mem_->Ref();

  table_cache_ = new TableCache(dbname_, options_);
  versions_ = new VersionSet(dbname_, options_);
  imm_ = nullptr;

  // 初始化 BlobStore (子目录)
  std::string blob_dir = dbname_ + "/blob";
  blob_store_ = new BlobStore(blob_dir);
}

DBImpl::~DBImpl() {
  if (mem_) mem_->Unref();
  if (imm_) imm_->Unref();
  delete blob_store_;
  delete table_cache_;
  delete versions_; 
}

// --- 核心恢复逻辑 (Crash Recovery) ---

Status DBImpl::Recover() {
  // 1. 扫描目录下的所有文件
  std::vector<std::string> filenames;
  // 假设 Env 有 GetChildren，或者直接用 filesystem
  for (const auto& entry : std::filesystem::directory_iterator(dbname_)) {
      filenames.push_back(entry.path().filename().string());
  }

  std::vector<uint64_t> sst_files;
  uint64_t max_log_number = 0;
  bool has_log = false;

  for (const auto& fname : filenames) {
      uint64_t number;
      // 解析文件名 (简单判断后缀)
      if (fname.length() > 4 && fname.substr(fname.length() - 4) == ".sst") {
          // 提取数字: 000001.sst -> 1
          number = std::stoull(fname.substr(0, fname.length() - 4));
          sst_files.push_back(number);
      } else if (fname.length() > 4 && fname.substr(fname.length() - 4) == ".log") {
          // 提取数字: 000003.log -> 3
          // 注意：我们要找最大的 Log 文件，那就是当前的活跃 WAL
          // (旧的 Log 应该在 Flush 后删除，Week 2 暂未实现删除，所以最大的就是最新的)
          try {
            number = std::stoull(fname.substr(0, fname.length() - 4));
            if (number > max_log_number) {
                max_log_number = number;
                has_log = true;
            }
          } catch (...) { 
              // 忽略解析错误 (比如 wal.log)
              if (fname == "wal.log") {
                  // 兼容旧逻辑：如果只有 wal.log，把它当作 log number 0
                  if (!has_log) max_log_number = 0; 
                  has_log = true; 
              }
          }
      }
  }

  // 2. 恢复 VersionSet (即告诉数据库有哪些 SSTable)
  // 我们需要获取文件大小和边界 Key (Smallest/Largest)
  // 简化版：只恢复文件名和大小，Smallest/Largest 暂时置空 (这会导致读放大，但逻辑正确)
  // 此时 Get 会查所有 SSTable，性能较差但结果正确。
  Version* v = versions_->current();
  for (uint64_t num : sst_files) {
      std::string fname = TableFileName(dbname_, num);
      uint64_t fsize = std::filesystem::file_size(fname);
      // Day 5 Hack: Smallest/Largest 留空，Get 时会跳过 Range Check 直接读
      v->AddFile(0, num, fsize, Slice(""), Slice(""));
      
      // 更新 next_file_number_，防止新生成的文件号冲突
      if (num >= versions_->NewFileNumber()) {
          // 这一步有点 hack，正确做法是 VersionSet::Recover 读取 MANIFEST
          // 这里我们简单地不断 NewFileNumber 直到超过现有的
          while (versions_->NewFileNumber() <= num); 
      }
  }

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
  if (!s.ok()) return s;

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
  // 恢复完成后，我们不再继续追加旧 Log，而是开启一个新的 Log
  // 这样旧 Log 就可以被安全归档/删除了
  
  // 这一步逻辑其实就是 MakeRoomForWrite 的一部分
  // 但我们不能直接调 MakeRoom，因为不需要 Flush
  
  uint64_t new_log_num = versions_->NewFileNumber();
  std::unique_ptr<WritableFile> lfile;
  s = NewWritableFile(LogFileName(dbname_, new_log_num), &lfile);
  if (!s.ok()) return s;
  
  logfile_ = std::move(lfile);
  log_ = std::make_unique<log::Writer>(logfile_.get());

  return Status::OK();
}

// --- 写入路径 ---

Status DBImpl::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  std::lock_guard<std::mutex> lock(mutex_);
    
  // 1. 检查 MemTable 空间
  Status s = MakeRoomForWrite(false);
  if (!s.ok()) return s;
  return WriteLocked(opt, kTypeValue, key, value);
}

Status DBImpl::Delete(const WriteOptions& opt, const Slice& key) {
  // 1. 获取全局锁 (与 Put 互斥)
  std::lock_guard<std::mutex> lock(mutex_);

  // 2. 检查 MemTable 空间 (如果满了需要 Flush)
  Status s = MakeRoomForWrite(false);
  if (!s.ok()) return s;

  // 3. 调用核心写入逻辑
  // kTypeDeletion: 标记为删除
  // Slice(): Value 为空
  return WriteLocked(opt, kTypeDeletion, key, Slice());
}

// 通用写入函数：处理 Blob 分离、WAL 和 MemTable
Status DBImpl::Write(const WriteOptions& opt, ValueType type, const Slice& key, const Slice& value) {
  // 加锁：保证 WAL 和 MemTable 的顺序一致性
  std::lock_guard<std::mutex> lock(mutex_);

  Status s;
  std::string value_to_store; // 最终存入 LSM 的值（可能是原始值，也可能是 BlobIndex）

  // 1. 键值分离逻辑 (WiscKey)
  // 只有 Put 操作且 Value 足够大时才分离，Delete 操作不需要分离
  if (type == kTypeValue && value.size() >= options_.min_blob_size) {
    BlobIndex b_index;
    // 写入 BlobStore (这是 IO 操作，但为了简单暂且放在锁内，Week 5 优化到锁外)
    s = blob_store_->Add(key, value, &b_index);
    if (!s.ok()) return s;

    // 序列化 Index
    b_index.EncodeTo(&value_to_store);
  } else {
    // 小 Value 或 Delete，直接存
    value_to_store.assign(value.data(), value.size());
  }

  // 2. 构造 WAL 条目并写入
  // Format: [Type] [KeyLen] [Key] [ValLen] [Val]
  std::string log_record = EncodeLogRecord(type, key, value_to_store);
  
  // 确保 Log Writer 已初始化 (针对非 Recover 启动的情况)
  if (!log_) {
    std::unique_ptr<WritableFile> lfile;
    s = NewWritableFile(dbname_ + "/wal.log", &lfile);
    if (!s.ok()) return s;
    logfile_ = std::move(lfile);
    log_ = std::make_unique<log::Writer>(logfile_.get());
  }

  s = log_->AddRecord(log_record);
  if (!s.ok()) return s;

  // 3. 根据选项决定是否 Sync 刷盘
  if (opt.sync) {
    s = logfile_->Sync();
    if (!s.ok()) return s;
  }

  // 4. 写入内存 MemTable
  // 分配 SeqNum
  last_sequence_++;
  mem_->Add(last_sequence_, type, key, value_to_store);

  return Status::OK();
}

// 这是一个 Private 函数，假设调用者已经持有了 mutex_
Status DBImpl::WriteLocked(const WriteOptions& opt, ValueType type, const Slice& key, const Slice& value) {
  Status s;
  std::string value_to_store;

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
  
  // 【关键修复】如果 log_ 为空（新库首次写入），必须初始化！
  if (!log_) {
    uint64_t new_log_num = versions_->NewFileNumber();
    std::unique_ptr<WritableFile> lfile;
    // 使用标准命名 00000X.log
    Status s = NewWritableFile(LogFileName(dbname_, new_log_num), &lfile);
    if (!s.ok()) return s;
    
    logfile_ = std::move(lfile);
    log_ = std::make_unique<log::Writer>(logfile_.get());
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

// --- 读取流程改造 ---
Status DBImpl::Get(const ReadOptions& options, const Slice& key, std::string* value) {
  // 1. 获取快照 (Snapshot)
  // 获取当前最新的 SequenceNumber，保证读取的一致性
  SequenceNumber snapshot = last_sequence_.load(std::memory_order_acquire);
  
  // 构造查找 Key (UserKey + Snapshot)
  LookupKey lkey(key, snapshot);
  Status s; // 用于接收 MemTable/Table 查找的状态结果

  // 2. 查活跃 MemTable
  if (mem_->Get(lkey, value, &s)) {
    // 在 MemTable 中找到了
    if (s.IsNotFound()) {
      return s; // 是删除标记 (Tombstone)，返回 NotFound
    }
    // 检查是否是 BlobIndex (键值分离)
    BlobIndex b_index;
    Slice input(*value);
    // 尝试解码：如果成功且没有剩余数据，说明存的是 BlobIndex
    if (b_index.DecodeFrom(&input).ok() && input.empty()) {
       // 是 BlobIndex，去 BlobStore 读实际数据
       return blob_store_->Get(b_index, value);
    }
    // 是普通 Value (内联的小 Value)
    return Status::OK();
  }

  // 3. 查 Immutable MemTable (正在刷盘的)
  // 如果 imm_ 不为空，说明正在进行 Flush，数据可能在这里
  if (imm_ != nullptr) {
    if (imm_->Get(lkey, value, &s)) {
      if (s.IsNotFound()) {
        return s;
      }
      BlobIndex b_index;
      Slice input(*value);
      if (b_index.DecodeFrom(&input).ok() && input.empty()) {
         return blob_store_->Get(b_index, value);
      }
      return Status::OK();
    }
  }

  // 4. 查 SSTables (磁盘 L0 层)
  // L0 文件可能有重叠，必须按时间倒序（新的在后）查找
  // current_version_->GetFiles() 返回的是按生成顺序排列的文件列表
  std::vector<FileMetaData*> files = versions_->current()->GetFiles();
  for (auto it = files.rbegin(); it != files.rend(); ++it) {
    FileMetaData* f = *it;
    
    // 构造回调上下文
    struct Context {
        std::string* val;
        bool found;
    } ctx {value, false};
    
    // 回调函数：当 TableReader 找到 Key 时调用
    auto callback = [](void* arg, const Slice& k, const Slice& v) {
        (void)k; // 忽略 Key，消除未使用参数警告
        Context* c = static_cast<Context*>(arg);
        c->found = true;
        *(c->val) = v.ToString(); // 暂存查到的 Raw Value
    };
    
    // 查 TableCache
    // lkey.internal_key() 包含了 UserKey + SeqNum，Table::InternalGet 会处理 MVCC 逻辑
    Status status = table_cache_->Get(options, f->file_number, f->file_size, lkey.internal_key(), &ctx, callback);
    
    if (!status.ok()) {
        // 如果读文件出错（比如 IO 错误），直接返回错误
        return status;
    }

    if (ctx.found) {
        // 在 SSTable 中找到了！
        // 同样检查是否是 BlobIndex
        BlobIndex b_index;
        Slice input(*value);
        if (b_index.DecodeFrom(&input).ok() && input.empty()) {
             return blob_store_->Get(b_index, value);
        }
        return Status::OK(); // 普通 Value
    }
  }

  return Status::NotFound("Key not found");
}
std::string DBImpl::EncodeLogRecord(ValueType type, const Slice& key, const Slice& value) {
  std::string dst;
  // 1. Type
  dst.push_back(static_cast<char>(type));
  // 2. Key
  PutVarint32(&dst, key.size());
  dst.append(key.data(), key.size());
  // 3. Value
  PutVarint32(&dst, value.size());
  dst.append(value.data(), value.size());
  return dst;
}

// --- 核心：Flush 逻辑 ---
Status DBImpl::WriteLevel0Table(MemTable* mem, Version* version) {
  // 1. 准备文件
  uint64_t file_number = versions_->NewFileNumber();
  std::string fname = TableFileName(dbname_, file_number);
  
  std::unique_ptr<WritableFile> file;
  Status s = NewWritableFile(fname, &file);
  if (!s.ok()) return s;

  TableBuilder builder(options_, file.get());

  // 2. 遍历 MemTable 写入 Builder
  // 需要 MemTable 提供 Iterator
  // 这里的 Iterator 返回的是 InternalKey (UserKey+Tag)
  Iterator* iter = mem->NewIterator();
  iter->SeekToFirst();
  
  std::string smallest, largest;
  bool first = true;
  
  // 假设 MemTable 不为空
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
  if (!s.ok()) return s;
  
  uint64_t file_size = builder.FileSize();
  file->Close(); // 确保落盘

  // 4. 更新版本元数据
  if (s.ok()) {
      version->AddFile(0, file_number, file_size, Slice(smallest), Slice(largest));
  }
  return s;
}

Status DBImpl::MakeRoomForWrite(bool force) {
    bool allow_switch = (force || mem_->ApproximateMemoryUsage() >= options_.write_buffer_size);
    if (!allow_switch) {
        return Status::OK();
    }

    if (imm_ != nullptr) {
        // 强制 Flush 旧的 Immutable
        Status s = WriteLevel0Table(imm_, versions_->current());
        if (!s.ok()) return s;
        imm_->Unref();
        imm_ = nullptr;
    }

    // 1. 切换 MemTable
    imm_ = mem_;
    mem_ = new MemTable(InternalKeyComparator());
    mem_->Ref();

    // 2. 切换 WAL
    // 关闭旧 Log
    logfile_.reset();
    log_.reset();

    // 获取新编号
    uint64_t new_log_number = versions_->NewFileNumber();
    
    // 创建新文件
    std::string log_fname = LogFileName(dbname_, new_log_number);
    Status s = NewWritableFile(log_fname, &logfile_);
    if (!s.ok()) return s;
    
    log_ = std::make_unique<log::Writer>(logfile_.get());

    // 3. 应用元数据变更 (MANIFEST)
    VersionEdit edit;
    edit.SetLogNumber(new_log_number);
    s = versions_->LogAndApply(&edit, &mutex_);
    if (!s.ok()) return s;

    // 4. Flush 刚刚切换出来的 imm_ (同步 Flush)
    s = WriteLevel0Table(imm_, versions_->current());
    if (!s.ok()) return s;
    
    imm_->Unref();
    imm_ = nullptr;

    return Status::OK();
}

} // namespace titankv