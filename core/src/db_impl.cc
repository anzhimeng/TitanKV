#include "titankv/db_impl.h"
#include "wal/log_reader.h"
#include "util/coding.h"
#include "lsm/table_builder.h"
#include "lsm/version_edit.h"
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
      versions_(nullptr) 
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


  InternalKeyComparator cmp;
  mem_ = new MemTable(cmp);
  mem_->Ref();


  // 初始化组件
  blob_store_ = new BlobStore(dbname_ + "/blob", options, uring_executor_.get());

  table_cache_ = new TableCache(dbname_, options_);
  versions_ = new VersionSet(dbname_, options_);
}

DBImpl::~DBImpl() {
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
  if (!s.ok()) return s;
  
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
  (void)opt;
  SequenceNumber snapshot = last_sequence_.load(std::memory_order_acquire);
  LookupKey lkey(key, snapshot);
  Status s;

  // 1. 查 MemTable
  if (mem_->Get(lkey, value, &s)) {
    if (s.IsNotFound()) return s;
    BlobIndex b_index;
    Slice input(*value);
    if (b_index.DecodeFrom(&input).ok() && input.empty()) {
       return blob_store_->Get(b_index, value);
    }
    return Status::OK();
  }

  // 2. 查 Immutable
  if (imm_ != nullptr) {
    if (imm_->Get(lkey, value, &s)) {
      if (s.IsNotFound()) return s;
      BlobIndex b_index;
      Slice input(*value);
      if (b_index.DecodeFrom(&input).ok() && input.empty()) {
         return blob_store_->Get(b_index, value);
      }
      return Status::OK();
    }
  }

  // 3. 查 SSTables (L0)
  // 获取当前版本并加引用
  Version* current;
  {
      std::lock_guard<std::mutex> lock(mutex_);
      current = versions_->current();
      current->Ref();
  }

  std::vector<FileMetaData*> files = current->GetFiles();
  fprintf(stderr, "[DBImpl::Get] Key: %s. Checking %lu SSTables.\n", 
          key.ToString().c_str(), files.size());
  Status result = Status::NotFound("Key not found");

  // L0 文件可能有重叠，必须按时间倒序（新的在后）查找
  for (auto it = files.rbegin(); it != files.rend(); ++it) {
    FileMetaData* f = *it;
    
    struct Context {
        std::string* val;
        bool found;
    } ctx {value, false};
    
    auto callback = [](void* arg, const Slice& k, const Slice& v) {
        (void)k;
        Context* c = static_cast<Context*>(arg);
        c->found = true;
        *(c->val) = v.ToString();
    };
    
    Status status = table_cache_->Get(opt, f->file_number, f->file_size, lkey.internal_key(), &ctx, callback);
    if (!status.ok()) {
    	   //fprintf(stderr, "[DBImpl::Get] Error reading SST #%lu: %s\n", f->file_number, status.ToString().c_str());
        result = status;
        break;
    }

    if (ctx.found) {
        BlobIndex b_index;
        Slice input(*value);
        if (b_index.DecodeFrom(&input).ok() && input.empty()) {
             result = blob_store_->Get(b_index, value);
        } else {
             result = Status::OK();
        }
        break; // 找到了，停止搜索
    }
  }

  // 释放版本引用
  current->Unref();
  return result;
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

} // namespace titankv