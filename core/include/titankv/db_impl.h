#pragma once

#include <mutex>
#include <chrono>
#include <string>
#include <atomic>
#include <memory>
#include <thread>
#include <condition_variable>
#include "titankv/db.h"
#include "titankv/coprocessor.h"
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

  Status PutCF(CFType cf, const Slice& key, const Slice& value, uint64_t ts = 0) override;
  Status DeleteCF(CFType cf, const Slice& key, uint64_t ts = 0) override;
  Status GetCF(CFType cf, const Slice& key, std::string* value, uint64_t ts = 0) override;
  Status GetCFLocked(CFType cf, const Slice& key, std::string* value, uint64_t ts = 0);
  // MVCC Prewrite
  // mutations: 编码后的 mutation 列表 (key, value, type)
  // primary: primary key
  // start_ts: 事务开始时间
  // ttl: 锁超时时间
  Status MvccPrewrite(const std::vector<Mutation>& mutations, 
                              const std::string& primary,
                              uint64_t start_ts, 
                              uint64_t ttl,
                              uint64_t min_commit_ts,
                              bool is_pessimistic_lock,
                              const std::vector<std::string>& secondaries) override;

  Status AcquirePessimisticLock(const std::vector<std::string>& keys,
                                const std::string& primary,
                                uint64_t start_ts,
                                uint64_t ttl,
                                uint64_t for_update_ts,
                                bool return_values,
                                std::vector<std::string>* values,
                                std::vector<bool>* not_found) override;

  Status MvccPrewrite1PC(const std::vector<Mutation>& mutations, 
                         const std::string& primary,
                         uint64_t start_ts, 
                         uint64_t commit_ts,
                         uint64_t ttl);

  // MVCC Commit
  // keys: 需要提交的 Key 列表
  // start_ts: 事务开始时间
  // commit_ts: 事务提交时间
  Status MvccCommit(const std::vector<std::string>& keys, 
                            uint64_t start_ts, 
                            uint64_t commit_ts) override;
  Status MvccGet(const Slice& key, uint64_t start_ts, std::string* value) override;
  Status MvccGC(uint64_t safe_point) override;
  Status CheckTxnStatus(const Slice& primary, uint64_t lock_ts, uint64_t current_ts,
                                int* action, uint64_t* commit_ts) override;
  
  // 【新增】创建指定 CF 的迭代器
 Iterator* NewIterator(const ReadOptions& options, CFType cf) override;

  Status Recover();

  // 【新增】手动触发 GC (Day 3 入口)
  Status GarbageCollect();

  // 【新增】并发控制优化：Raft 前置冲突检测
  Status CheckConflict(const std::vector<std::string>& keys, uint64_t start_ts);

  // Coprocessor
  Status ExecuteCoprocessor(const CoprocessorRequest& req, CoprocessorResponse* resp) override;

  const Options& GetOptions() const { return options_; } // 新增
  // 【新增】
  void SetGCThreshold(double threshold) {
  	blob_store_->SetGCThreshold(threshold);
  }

  // 导入外部 SST 文件
  Status IngestSST(const std::string& file_path) override;
  Status DumpSST(const Slice& start, const Slice& end, const std::string& fname, uint64_t seq) override;

  Status DeleteRange(const WriteOptions& options, const Slice& start, const Slice& end) override;

  // Moved to public section

  void UpdateMaxTS(uint64_t ts) {
    uint64_t current_max = max_ts_.load(std::memory_order_relaxed);
    if (ts > current_max) {
        uint64_t expected = current_max;
        while (ts > expected && !max_ts_.compare_exchange_weak(expected, ts)) {
            // retry
        }
    }
   }
   uint64_t GetMaxTS() {
       return max_ts_.load(std::memory_order_relaxed);
   }
   void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
   
   // 【新增】辅助函数：公开给 Iterator 使用
   Status ResolveBlobIndex(std::string* value);

 private:
  void BGGC();
  std::thread bg_gc_thread_;
  std::mutex bg_gc_mutex_;
  std::condition_variable bg_gc_cv_;
  std::atomic<bool> bg_gc_running_{false};
  std::atomic<bool> bg_gc_scheduled_{false};

  friend class DB;

  const std::string dbname_;
  Options options_; 
  std::atomic<uint64_t> max_ts_{0}; 
  std::atomic<uint64_t> gc_safe_point_{0};

  // 【修改】核心业务锁：递归锁，防止死锁
  std::recursive_mutex mutex_;
  MemTable* mem_;
  BlobStore* blob_store_;
  
  std::unique_ptr<WritableFile> logfile_; 
  std::unique_ptr<log::Writer> log_;     
  std::atomic<SequenceNumber> last_sequence_; 
  size_t wal_pending_bytes_;
  std::chrono::steady_clock::time_point wal_last_sync_;

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
  // 增加私有方法
  Status GetLocked(const ReadOptions& opt, const Slice& key, std::string* value);
  // 私有辅助函数声明
  std::string EncodeLogRecord(ValueType type, const Slice& key, const Slice& value);
  Status Write(const WriteOptions& options, ValueType type, const Slice& key, const Slice& value);
  // 增加 WriteBatch 支持
  Status Write(const WriteOptions& options, WriteBatch* batch) override;
  Status WriteLocked(const WriteOptions& options, ValueType type, const Slice& key, const Slice& value);
  Status WriteLocked(const WriteOptions& options, WriteBatch* batch);
  Status MaybeSyncWAL(size_t bytes_written, bool force_sync);

  Status DoCompactionWork(Compaction* c);

  // 【新增】辅助函数声明
  Status GetLSMValue(const Slice& key, std::string* blob_index_str);
  bool DecodeBlobIndexFromValue(const std::string& value, BlobIndex* out);
    
  // 【新增】辅助：完成 GC 回填
  Status FinishGC(const std::vector<GCRecord>& gc_records);

  void StartBackgroundThread();

  std::string EncodeInternalKey(const Slice& user_key, uint64_t seq, ValueType type);
  
  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;
};

} // namespace titankv
