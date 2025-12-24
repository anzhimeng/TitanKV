#pragma once

#include <mutex>
#include <string>
#include <atomic>
#include <memory>
#include "titankv/db.h"
#include "lsm/memtable.h"
#include "blob/blob_store.h"
#include "wal/log_writer.h"
#include "util/env.h"

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
  
  // --- 关键修复：确保这里的名字与 .cc 中一致 ---
  std::unique_ptr<WritableFile> logfile_; // .cc 中用的是 logfile_
  std::unique_ptr<log::Writer> log_;      // .cc 中用的是 log_
  std::atomic<SequenceNumber> last_sequence_; // .cc 中用到了这个

  // 私有辅助函数声明
  std::string EncodeLogRecord(ValueType type, const Slice& key, const Slice& value);
  Status Write(const WriteOptions& options, ValueType type, const Slice& key, const Slice& value);
  
  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;
};

} // namespace titankv