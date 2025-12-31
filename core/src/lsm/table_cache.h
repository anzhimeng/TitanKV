#pragma once
#include <string>
#include <memory>
#include <map>
#include <mutex>
#include "titankv/status.h"
#include "titankv/options.h"
#include "lsm/table.h"

namespace titankv {

class TableCache {
 public:
  TableCache(const std::string& dbname, const Options& options);
  ~TableCache() = default;

  // 获取指定 file_number 的 Table 对象
  // 如果缓存没有，则打开文件
  Status Get(uint64_t file_number, uint64_t file_size, Table** table);

  // 在指定文件中查找 Key
  Status Get(const ReadOptions& options, uint64_t file_number, uint64_t file_size,
             const Slice& k, void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));
  // 【新增】返回指定 SSTable 的迭代器
  Iterator* NewIterator(const ReadOptions& options, uint64_t file_number, uint64_t file_size);
  // 驱逐文件（例如删除文件时）
  void Evict(uint64_t file_number);

 private:
  std::string dbname_;
  const Options& options_;
  
  std::mutex mutex_;
  // 简单缓存：FileNumber -> Table 对象
  // 生产环境应用 LRU Cache，这里用 map 简化
  std::map<uint64_t, std::shared_ptr<Table>> cache_;

  Status FindTable(uint64_t file_number, uint64_t file_size, std::shared_ptr<Table>* table);
};

} // namespace titankv