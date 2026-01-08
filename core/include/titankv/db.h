#pragma once

#include <string>
#include "titankv/status.h"
#include "titankv/slice.h"
#include "titankv/options.h"
#include "titankv/write_batch.h"

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
};

} // namespace titankv