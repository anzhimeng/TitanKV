#pragma once

#include <mutex>
#include <string>
#include <atomic>
#include <memory>
#include <thread>
#include <condition_variable>
#include "titankv/db.h"
#include "lsm/memtable.h"
#include "lsm/version_set.h"
#include "lsm/table_cache.h"
#include "blob/blob_store.h"
#include "wal/log_writer.h"
#include "util/env.h"
#include "lsm/version_set.h"
#include "util/io_uring_executor.h" // 新增


namespace titankv {

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);
  ~DBImpl() override;

  Status Put(const WriteOptions& options, const Slice& key, const Slice& value) override;
  Status Delete(const WriteOptions& options, const Slice& key) override;
  Status Get(const ReadOptions& options, const Slice& key, std::string* value) override;

  Status Recover();

  // 【新增】手动触发 GC (Day 3 入口)
  Status GarbageCollect();

  const Options& GetOptions() const { return options_; } // 新增
  // 【新增】
  void SetGCThreshold(double threshold) {
  	blob_store_->SetGCThreshold(threshold);
  }

 private:
  friend class DB;

  const std::string dbname_;
  Options options_; 

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

  std::unique_ptr<IoUringExecutor> uring_executor_; // 新增成员
  // 【新增】受保护的文件集合 (正在 Flush 或 Compaction 生成的文件)

  // 【Day 3 新增】清理不再使用的文件
  void DeleteObsoleteFiles();

  // 【Day 3 新增】保护正在生成的文件不被误删
  // 必须在持有 mutex_ 时访问
  std::set<uint64_t> pending_outputs_; 

  // 后台任务控制
  std::thread bg_thread_;
  std::mutex bg_mutex_;
  std::condition_variable bg_cv_;
  bool bg_running_;

  // 后台线程主函数
  void BGWork();

  // 核心函数：将 MemTable 写入 SSTable
  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, uint64_t* file_number);
  
  // 触发 Flush
  Status MaybeScheduleCompaction(); // Day 4 我们改为 MakeRoomForWrite 同步刷盘
  Status MakeRoomForWrite(bool force); // 检查 MemTable 是否满了

  // 私有辅助函数声明
  std::string EncodeLogRecord(ValueType type, const Slice& key, const Slice& value);
  Status Write(const WriteOptions& options, ValueType type, const Slice& key, const Slice& value);
  // 增加 WriteBatch 支持
  Status Write(const WriteOptions& options, WriteBatch* batch) override;
  Status WriteLocked(const WriteOptions& options, ValueType type, const Slice& key, const Slice& value);

  Status DoCompactionWork(Compaction* c);

  // 【新增】辅助函数声明
  Status ResolveBlobIndex(std::string* value);
  Status GetLSMValue(const Slice& key, std::string* blob_index_str);
    
  // 【新增】辅助：完成 GC 回填
  Status FinishGC(const std::vector<GCRecord>& gc_records);
  
  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;
};

} // namespace titankv