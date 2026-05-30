#pragma once
#include "titankv/db.h"
#include "titankv/status.h"
#include <memory>

namespace titankv {

class MvccReader {
 public:
  // db: 数据库实例
  // snapshot: 读取的快照版本 (StartTS)
  MvccReader(DB* db, uint64_t snapshot);
  ~MvccReader();

  // 检查 Lock CF
  // 如果存在锁，返回 LockInfo (二进制)
  // 如果没有锁，返回 NotFound
  Status LoadLock(const Slice& key, std::string* lock_info);

  // 查找可见的 Write 记录
  // 返回: write_info (Type + StartTS)
  // commit_ts: 输出参数，返回找到记录的 CommitTS
  Status SeekWrite(const Slice& key, uint64_t* commit_ts, std::string* write_info);

  // 根据 Write 记录里的 StartTS，去 Default CF 读数据
  Status GetValue(const Slice& key, uint64_t start_ts, std::string* value);

 private:
  DB* db_;
  uint64_t snapshot_;
  std::unique_ptr<Iterator> write_iter_;
};

} // namespace titankv
