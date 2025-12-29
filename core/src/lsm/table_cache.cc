#include "lsm/table_cache.h"
#include "util/filename.h"
#include "util/env.h"
#include <cstdio>

namespace titankv {

TableCache::TableCache(const std::string& dbname, const Options& options)
    : dbname_(dbname), options_(options) {}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size, std::shared_ptr<Table>* result) {
  // 1. 第一次查缓存 (快速路径)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cache_.find(file_number);
    if (it != cache_.end()) {
      *result = it->second;
      return Status::OK();
    }
  }

  // 2. 缓存未命中，执行 I/O (在锁外！允许并发打开文件)
  std::string fname = TableFileName(dbname_, file_number);
  std::unique_ptr<RandomAccessFile> file;
  
  Status s = NewRandomAccessFile(fname, &file);
  if (!s.ok()) {
    // 【关键调试】打印出到底是哪个路径打不开
    fprintf(stderr, "[TableCache] Fatal: Cannot open SST file: %s. Error: %s\n", 
            fname.c_str(), s.ToString().c_str());
    return s;
  }

  Table* table = nullptr;
  // 注意：Table::Open 如果失败，它负责 delete file (我们在 Table::Open 修复中约定了这一点)
  // 为了安全，我们这里 release()，如果 Open 失败，确保 Open 内部 delete 了 file
  s = Table::Open(options_, file.release(), file_number, file_size, &table);
  if (!s.ok()) {
    return s;
  }
  
  std::shared_ptr<Table> table_ptr(table);

  // 3. 第二次查缓存 (写入路径)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cache_.find(file_number);
    if (it != cache_.end()) {
      // 别的线程已经打开了，用它的
      *result = it->second;
    } else {
      // 我是第一个，放入缓存
      cache_[file_number] = table_ptr;
      *result = table_ptr;
    }
  }
  
  return Status::OK();
}

Status TableCache::Get(const ReadOptions& options, uint64_t file_number, uint64_t file_size,
                       const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&, const Slice&)) {
  std::shared_ptr<Table> table;
  Status s = FindTable(file_number, file_size, &table);
  if (!s.ok()) return s;

  return table->InternalGet(options, k, arg, handle_result);
}

void TableCache::Evict(uint64_t file_number) {
  std::lock_guard<std::mutex> lock(mutex_);
  cache_.erase(file_number);
}

} // namespace titankv