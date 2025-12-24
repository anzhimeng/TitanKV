#include "titankv/db_impl.h"
#include "wal/log_reader.h"
#include "util/coding.h"
#include <filesystem>
#include <iostream>

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
      last_sequence_(0) {
  
  // 初始化比较器
  InternalKeyComparator cmp;
  mem_ = new MemTable(cmp);
  mem_->Ref();

  // 初始化 BlobStore (子目录)
  std::string blob_dir = dbname_ + "/blob";
  blob_store_ = new BlobStore(blob_dir);
}

DBImpl::~DBImpl() {
  // 释放资源
  // 注意顺序：先释放 MemTable (Unref)，再释放其他
  if (mem_) mem_->Unref();
  delete blob_store_;
  // log_ 和 logfile_ 是 unique_ptr，自动释放
}

// --- 核心恢复逻辑 (Crash Recovery) ---

Status DBImpl::Recover() {
  // 1. 尝试打开存在的 WAL 文件
  std::string log_path = dbname_ + "/wal.log";
  if (!std::filesystem::exists(log_path)) {
    // 这是一个新 DB，无需恢复
    return Status::OK();
  }

  std::unique_ptr<SequentialFile> file;
  Status s = NewSequentialFile(log_path, &file);
  if (!s.ok()) return s;

  // 2. 读取并回放 Log
  // 这里我们使用 checksu=true，如果日志损坏则报错
  log::Reader reader(file.get(), nullptr, true, 0);
  
  Slice record;
  std::string scratch;
  
  while (reader.ReadRecord(&record, &scratch)) {
    // 解析 Log Record
    // Format: [Type(1B)] [KeyLen] [Key] [ValLen] [Value]
    Slice input = record;
    if (input.size() < 1) {
      return Status::Corruption("WAL record too short");
    }

    // 1. Type
    char type_char = input[0];
    input.remove_prefix(1);
    ValueType type = static_cast<ValueType>(type_char);

    // 2. Key
    uint32_t key_len;
    if (!GetVarint32(&input, &key_len)) return Status::Corruption("Bad WAL key len");
    if (input.size() < key_len) return Status::Corruption("Bad WAL key data");
    Slice key(input.data(), key_len);
    input.remove_prefix(key_len);

    // 3. Value (or BlobIndex)
    uint32_t val_len;
    if (!GetVarint32(&input, &val_len)) return Status::Corruption("Bad WAL val len");
    if (input.size() < val_len) return Status::Corruption("Bad WAL val data");
    Slice value(input.data(), val_len);
    
    // 4. 重建 MemTable
    // 恢复时 SequenceNumber 递增
    // 在生产级实现中，WAL 应该记录当时的 SeqNum。
    // 这里简化为重新分配 SeqNum，只要保持顺序即可。
    last_sequence_++;
    mem_->Add(last_sequence_, type, key, value);
  }

  // 3. 恢复完成后，我们需要重新打开 Log 为追加模式 (Writable)
  // 简单的做法是：关闭 Reader，重新以 Append 模式打开 Writer
  // 或者：Compaction 之后生成新 Log。Week 1 简化为继续追加。
  std::unique_ptr<WritableFile> lfile;
  // 注意：PosixWritableFile 默认是 O_TRUNC (清空) 还是 O_APPEND？
  // 我们的 Env 实现是 O_APPEND，所以是安全的。
  s = NewWritableFile(log_path, &lfile); 
  if (!s.ok()) return s;

  logfile_ = std::move(lfile);
  log_ = std::make_unique<log::Writer>(logfile_.get());

  return Status::OK();
}

// --- 写入路径 ---

Status DBImpl::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  return Write(opt, kTypeValue, key, value);
}

Status DBImpl::Delete(const WriteOptions& opt, const Slice& key) {
  // Delete 本质上是写入一个 value 为空的 Tombstone
  return Write(opt, kTypeDeletion, key, Slice());
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

// --- 读取路径 ---

Status DBImpl::Get(const ReadOptions& opt, const Slice& key, std::string* value) {
  // 读取路径通常不需要加互斥锁，因为 MemTable::Get 是无锁 SkipList
  // 但为了访问 mem_ 指针的安全，或者获取当前的 SequenceNumber (快照)，
  // 严格来说需要短暂加锁获取当前状态，或者使用原子指针。
  // Week 1 简化：由于没有后台 Flush 线程切换 mem_，所以直接读是安全的。
  
  // 快照：读取当前最新的 SeqNum
  // 任何 > snapshot 的数据对本次读取不可见
  (void)opt;
  
  SequenceNumber snapshot = last_sequence_.load(std::memory_order_acquire);
  
  LookupKey lkey(key, snapshot);
  std::string val_buf;
  Status s;

  // 1. 查 MemTable
  if (mem_->Get(lkey, &val_buf, &s)) {
    // Found in MemTable
    if (s.IsNotFound()) {
      return s; // 是 Tombstone (已删除)
    }

    // 2. 检查是否是 BlobIndex
    // 我们需要在 BlobIndex 定义里加个 helper，或者尝试解码
    // Day 1 简化逻辑：尝试解码，如果成功且合理，就去读 Blob；否则当做普通 Value
    // 更好的做法：在 EncodeLogRecord 时，ValueType 可以区分 kTypeBlobIndex
    // 这里复用 BlobIndex::DecodeFrom 的逻辑
    BlobIndex b_index;
    Slice input(val_buf);
    
    // 这种尝试解码的方式其实有风险（普通 Value 恰好符合 BlobIndex 格式）
    // 工业级做法：在 InternalKey 的 ValueType 中增加 kTypeBlobIndex
    // 或者在 Value 头部加 Magic Number。
    // 这里我们假设如果 min_blob_size 设置得当，小 Value 不会误判。
    if (b_index.DecodeFrom(&input).ok() && input.empty()) {
       // 是 BlobIndex，调用 BlobStore 真读取！
       return blob_store_->Get(b_index, value);
    } else {
       // 普通 Value
       *value = val_buf;
       return Status::OK();
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

} // namespace titankv