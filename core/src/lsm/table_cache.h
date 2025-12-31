#pragma once
#include <string>
#include <memory>
#include "titankv/status.h"
#include "titankv/options.h"
#include "util/cache.h" 
#include "lsm/block.h"

namespace titankv {

class Table;

class TableCache {
 public:
  TableCache(const std::string& dbname, const Options& options);
  ~TableCache();

  Iterator* NewIterator(const ReadOptions& options, uint64_t file_number, uint64_t file_size);
  Status Get(const ReadOptions& options, uint64_t file_number, uint64_t file_size,
             const Slice& k, void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));
  void Evict(uint64_t file_number);

 private:
  std::string dbname_;
  const Options& options_;
  
  Cache* cache_; 

  // 这里使用了 Cache::Handle，所以编译器必须看到 Cache 的完整定义
  Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle** handle);
};

} // namespace titankv