#pragma once
#include <memory>
#include "titankv/status.h"
#include "titankv/options.h"
#include "util/env.h"
#include "lsm/dbformat.h"
#include "lsm/block.h"
#include "lsm/table_format.h"
#include "lsm/filter_block.h"

namespace titankv {

class Table {
 public:
  // 打开 SSTable 文件
  static Status Open(const Options& options, RandomAccessFile* file,
                     uint64_t file_number, uint64_t file_size, Table** table);

  ~Table();

  // 读取 Key
  // 1. 查 Index Block 找到 Data Block Handle
  // 2. 读 Data Block
  // 3. 在 Data Block 里查 Key
  Status InternalGet(const ReadOptions& options, const Slice& key, void* arg,
                     void (*handle_result)(void* arg, const Slice& k, const Slice& v));
  // 【新增】返回遍历整个 Table 的迭代器
  Iterator* NewIterator(const ReadOptions& options);

 private:
  struct Rep;
  Rep* rep_;

  explicit Table(Rep* rep) : rep_(rep) {}
  
  // 辅助：读取 Block
  static Status ReadBlock(RandomAccessFile* file, const ReadOptions& options, 
                          const BlockHandle& handle, BlockContents* contents);
  // 回调函数，符合 BlockFunction 签名
  static Iterator* BlockReader(void* arg, const ReadOptions& options, const Slice& handle_value);
};

} // namespace titankv