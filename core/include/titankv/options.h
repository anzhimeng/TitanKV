#pragma once

#include <cstddef>
#include <memory>
#include <vector>
#include "titankv/statistics.h"
#include "titankv/slice.h"

namespace titankv {

class Cache; // 前置声明

class FilterPolicy {
 public:
  virtual ~FilterPolicy() = default;
  virtual const char* Name() const = 0;
  // 生成过滤器: keys 是输入，dst 是输出的 bitset
  virtual void CreateFilter(const std::vector<std::string>& keys, std::string* dst) const = 0;
  // 检查 key 是否匹配
  virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const = 0;
};

// 工厂函数
std::shared_ptr<FilterPolicy> NewBloomFilterPolicy(int bits_per_key);


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
    size_t write_buffer_size = 4 * 1024 * 1024;;

    // 【KV分离核心参数】Blob 分离阈值
    // Value 大小 >= 此值时，写入 BlobStore；否则直接内联到 LSM Tree
    size_t min_blob_size = 4096; // 4KB

    // Blob 文件的大小阈值 (默认 64MB)
    size_t max_blob_file_size = 64 * 1024 * 1024;

    // 【新增】LSM SSTable 文件大小上限 (默认 2MB)
    // Compaction 过程中，当 Builder 超过此大小时，会切分出新的 .sst 文件
    size_t max_file_size = 2 * 1024 * 1024;

    // SSTable Block 重启点间隔 (默认 16)
    // 每 16 个 Key 存一个完整的 Key，用于二分查找
    int block_restart_interval = 16;
    
    // Block 大小 (默认 4KB)
    size_t block_size = 4 * 1024;
    
    bool use_direct_io = false; // 【新增】开关

    int max_open_files = 500; 
    
    // 【新增】是否开启写前读来模拟垃圾 (默认关闭，仅用于测试)
    bool simulate_garbage_generation = false; 

    // 【新增】Block Cache
    // 使用 shared_ptr 方便管理生命周期
    std::shared_ptr<Cache> block_cache = nullptr;
    // 【新增】统计对象 (Shared Pointer)
    std::shared_ptr<Statistics> statistics = std::make_shared<Statistics>();
    std::shared_ptr<FilterPolicy> filter_policy = nullptr;
    size_t wal_sync_bytes = 1 * 1024 * 1024;
    uint64_t wal_sync_interval_ms = 1000;
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
} // namespace titankv
