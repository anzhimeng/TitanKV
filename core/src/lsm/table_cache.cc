#include "lsm/table_cache.h"
#include "util/filename.h"
#include "util/env.h"
#include <cstdio>

namespace titankv {

TableCache::TableCache(const std::string& dbname, const Options& options)
    : dbname_(dbname), options_(options) {}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size, std::shared_ptr<Table>* result) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = cache_.find(file_number);
  if (it != cache_.end()) {
    *result = it->second;
    return Status::OK();
  }

  // 缓存未命中，打开文件
  std::string fname = TableFileName(dbname_, file_number);
  std::unique_ptr<RandomAccessFile> file;
  Status s = NewRandomAccessFile(fname, &file);
  if (!s.ok()) return s;

  Table* table = nullptr;
  s = Table::Open(options_, file.release(), file_size, &table);
  if (!s.ok()) {
    return s;
  }

  // 存入缓存
  std::shared_ptr<Table> table_ptr(table);
  cache_[file_number] = table_ptr;
  *result = table_ptr;
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