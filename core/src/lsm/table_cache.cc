#include "lsm/table_cache.h"
#include "lsm/table.h"
#include "util/filename.h"
#include "util/env.h"
#include "util/coding.h"
#include <cstdio>

namespace titankv {

// 定义 deleter：当 Table 被从 LRU 驱逐时，关闭文件并释放内存
static void DeleteEntry(const Slice& key, void* value) {
  (void)key;
  Table* table = reinterpret_cast<Table*>(value);
  delete table; // 这会调用 Table::~Table -> delete file -> close fd
}

// 辅助：Cache 释放回调（用于 Iterator Cleanup）
static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options& options)
    : dbname_(dbname), options_(options) {
    // 创建 LRU Cache
    // 注意：NewLRUCache 的参数通常是字节容量。
    // 但我们可以把它当作“计数容量”来用，只要每次 Insert 的 charge 设为 1。
    // 这里设置容量为 max_open_files。
    cache_ = NewLRUCache(options_.max_open_files);
}

TableCache::~TableCache() {
    delete cache_;
}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle** handle) {
  // 1. 构造 Cache Key (8字节 file_number)
  char buf[8];
  EncodeFixed64(buf, file_number);
  Slice key(buf, 8);

  // 2. 查缓存
  *handle = cache_->Lookup(key);
  if (*handle != nullptr) {
    return Status::OK(); // 命中
  }

  // 3. 未命中，打开文件
  std::string fname = TableFileName(dbname_, file_number);
  std::unique_ptr<RandomAccessFile> file;
  
  Status s = NewRandomAccessFile(fname, &file);
  if (!s.ok()) {
     // 避免刷屏，只打印严重错误
     // fprintf(stderr, "[TableCache] Fatal: Cannot open SST file: %s. Error: %s\n", fname.c_str(), s.ToString().c_str());
     return s;
  }

  Table* table = nullptr;
  s = Table::Open(options_, file.release(), file_number, file_size, &table);
  if (!s.ok()) {
    delete table; // Table::Open 失败时 table 为 nullptr，delete 安全；如果部分成功需小心
    // 根据之前的实现，Table::Open 失败会处理好 file 的释放
    return s;
  }

  // 4. 插入缓存
  // charge = 1 (按文件个数计数)
  *handle = cache_->Insert(key, table, 1, &DeleteEntry);
  
  return Status::OK();
}

Iterator* TableCache::NewIterator(const ReadOptions& options, uint64_t file_number, uint64_t file_size) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    // 返回空迭代器或错误迭代器 (Day 4 简化：返回 nullptr)
    // 生产环境应返回 NewErrorIterator(s)
    fprintf(stderr, "[TableCache] NewIterator Error: %s\n", s.ToString().c_str());
    return nullptr; 
  }

  Table* table = reinterpret_cast<Table*>(cache_->Value(handle));
  Iterator* result = table->NewIterator(options);
  
  // 【关键】注册清理函数：当 Iterator 销毁时，释放 Cache Handle
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  
  return result;
}

Status TableCache::Get(const ReadOptions& options, uint64_t file_number, uint64_t file_size,
                       const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&, const Slice&)) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
      return s;
  }

  Table* table = reinterpret_cast<Table*>(cache_->Value(handle));
  s = table->InternalGet(options, k, arg, handle_result);

  // 【关键】用完立即释放引用计数
  cache_->Release(handle);
  
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[8];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, 8));
}

} // namespace titankv