#pragma once

#include <string>
#include "titankv/status.h"
#include "titankv/slice.h"
#include "titankv/options.h"
#include "titankv/write_batch.h"
#include "util/coding.h"
#include "lsm/block.h"

namespace titankv {


struct Range {
  Slice start;
  Slice limit;
};

// ==========================================
// DB: 抽象基类接口
// ==========================================
class DB {
 public:
  // 打开数据库的静态工厂方法
  // name: 数据库目录路径
  // dbptr: 输出参数，返回打开的 DB 指针
  static Status Open(const Options& options, const std::string& name, DB** dbptr);

  DB() = default;
  virtual ~DB() = default;

  // 禁止拷贝和赋值
  DB(const DB&) = delete;
  DB& operator=(const DB&) = delete;

  // 写入 Key-Value
  virtual Status Put(const WriteOptions& options,
                     const Slice& key,
                     const Slice& value) = 0;

  // 删除 Key
  virtual Status Delete(const WriteOptions& options, const Slice& key) = 0;

  // 读取 Key
  // value: 输出参数，存储读取到的值
  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value) = 0;
  virtual Status Write(const WriteOptions& options, WriteBatch* batch) = 0;
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) = 0;
  // 【新增】将范围内的 KV 导出到一个 SST 文件
  // range_start, range_end: 导出的 Key 范围
  // file_path: 输出文件路径
  // snapshot_seq: 使用的快照版本 (0 表示最新)
  virtual Status DumpSST(const Slice& range_start, const Slice& range_end, 
                         const std::string& file_path, uint64_t snapshot_seq) = 0;
  // 删除范围内所有数据
  virtual Status DeleteRange(const WriteOptions& options, const Slice& start, const Slice& end) = 0;
  
  // 导入外部 SST 文件
  virtual Status IngestSST(const std::string& file_path) = 0;

  // 【新增】多列族接口
  // ts: 事务时间戳。如果是 Lock CF，该参数会被忽略（或传 0）
  virtual Status PutCF(CFType cf, const Slice& key, const Slice& value, uint64_t ts = 0) = 0;
  virtual Status DeleteCF(CFType cf, const Slice& key, uint64_t ts = 0) = 0;
  virtual Status GetCF(CFType cf, const Slice& key, std::string* value, uint64_t ts = 0) = 0;
  
  // 【新增】创建指定 CF 的迭代器
  virtual Iterator* NewIterator(const ReadOptions& options, CFType cf) = 0;

  // MVCC Prewrite
  // mutations: 编码后的 mutation 列表 (key, value, type)
  // primary: primary key
  // start_ts: 事务开始时间
  // ttl: 锁超时时间
  virtual Status MvccPrewrite(const std::vector<Mutation>& mutations, 
                              const std::string& primary,
                              uint64_t start_ts, 
                              uint64_t ttl) = 0;
                              // MVCC Commit
  // keys: 需要提交的 Key 列表
  // start_ts: 事务开始时间
  // commit_ts: 事务提交时间
  virtual Status MvccCommit(const std::vector<std::string>& keys, 
                            uint64_t start_ts, 
                            uint64_t commit_ts) = 0;
  // MVCC Get
  // key: 用户 Key
  // start_ts: 事务 StartTS
  // value: 输出参数
  virtual Status MvccGet(const Slice& key, uint64_t start_ts, std::string* value) = 0;
};

} // namespace titankv