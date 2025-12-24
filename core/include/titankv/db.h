#pragma once

#include <string>
#include "titankv/status.h"
#include "titankv/slice.h"

namespace titankv {

// ==========================================
// Options: 数据库配置参数
// ==========================================
struct Options {
    // 如果数据库不存在，是否创建
    bool create_if_missing = true;

    // 如果数据库已存在，是否报错
    bool error_if_exists = false;

    // MemTable 的大小阈值 (默认 4MB)
    // 超过此大小时，MemTable 会变成 Immutable 并 Flush 到磁盘
    size_t write_buffer_size = 4 * 1024 * 1024;

    // 【KV分离核心参数】Blob 分离阈值
    // Value 大小 >= 此值时，写入 BlobStore；否则直接内联到 LSM Tree
    size_t min_blob_size = 4096; // 4KB

    // Blob 文件的大小阈值 (默认 64MB)
    size_t max_blob_file_size = 64 * 1024 * 1024;
};

// ==========================================
// ReadOptions: 读操作配置
// ==========================================
struct ReadOptions {
    // 是否校验 checksum
    bool verify_checksums = false;

    // 是否填充 BlockCache (默认 true)
    bool fill_cache = true;

    // Snapshot* snapshot = nullptr; // TODO: 后续支持快照读
};

// ==========================================
// WriteOptions: 写操作配置
// ==========================================
struct WriteOptions {
    // 是否同步刷盘 (fsync)
    // true: 慢但安全，机器断电不丢数据
    // false: 快，但断电可能丢失最近写入的数据 (OS Cache)
    bool sync = false;
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
};

} // namespace titankv