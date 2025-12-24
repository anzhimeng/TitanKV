#pragma once

#include <mutex>
#include <string>
#include <atomic>
#include <memory>
#include "titankv/db.h"
#include "lsm/memtable.h"
#include "lsm/version_set.h" 
#include "lsm/table_cache.h"
#include "blob/blob_store.h"
#include "wal/log_writer.h"
#include "util/env.h"
#include "lsm/version_set.h"


namespace titankv {

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);
  ~DBImpl() override;

  Status Put(const WriteOptions& options, const Slice& key, const Slice& value) override;
  Status Delete(const WriteOptions& options, const Slice& key) override;
  Status Get(const ReadOptions& options, const Slice& key, std::string* value) override;

  Status Recover();

 private:
  friend class DB;

  const std::string dbname_;
  const Options options_;

  std::mutex mutex_;
  MemTable* mem_;
  BlobStore* blob_store_;
  
  std::unique_ptr<WritableFile> logfile_; 
  std::unique_ptr<log::Writer> log_;     
  std::atomic<SequenceNumber> last_sequence_; 

  // Flush相关
  MemTable* imm_;                // 不可变内存表 (准备 Flush)
  TableCache* table_cache_;      // SSTable 缓存
  VersionSet* versions_; 

  // 核心函数：将 MemTable 写入 SSTable
  Status WriteLevel0Table(MemTable* mem,  Version* version);
  
  // 触发 Flush
  Status MaybeScheduleCompaction(); // Day 4 我们改为 MakeRoomForWrite 同步刷盘
  Status MakeRoomForWrite(bool force); // 检查 MemTable 是否满了

  // 私有辅助函数声明
  std::string EncodeLogRecord(ValueType type, const Slice& key, const Slice& value);
  Status Write(const WriteOptions& options, ValueType type, const Slice& key, const Slice& value);
  Status WriteLocked(const WriteOptions& options, ValueType type, const Slice& key, const Slice& value);
  
  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;
};

} // namespace titankv