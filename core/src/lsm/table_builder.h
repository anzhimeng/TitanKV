#pragma once

#include <cstdint>
#include "titankv/options.h"
#include "titankv/status.h"
#include "util/env.h"
#include "lsm/filter_block.h"

namespace titankv {

class BlockBuilder;
class BlockHandle;

class TableBuilder {
 public:
  // file: 必须是已经打开的可写文件
  TableBuilder(const Options& options, WritableFile* file);

  // 禁止拷贝
  TableBuilder(const TableBuilder&) = delete;
  TableBuilder& operator=(const TableBuilder&) = delete;

  ~TableBuilder();

  // 添加 KV 对
  // 要求：传入的 Key 必须严格大于上一个 Key
  void Add(const Slice& key, const Slice& value);

  // 结束构建，写入 Index Block 和 Footer
  Status Finish();

  // 放弃构建 (例如出错时)
  void Abandon();

  // 获取当前生成的文件总大小
  uint64_t FileSize() const;

  // 获取添加的 KV 对数量
  uint64_t NumEntries() const;

  // 检查状态
  Status status() const { return status_; }

 private:
  // 将当前 Data Block 刷入磁盘
  void Flush();
  
  // 写入原始 Block 数据，并返回 Handle
  void WriteBlock(BlockBuilder* block, BlockHandle* handle);

  bool ok() const { return status().ok(); }

  const Options options_;
  WritableFile* file_;
  uint64_t offset_; // 当前文件写入偏移量
  Status status_;
  
  BlockBuilder* data_block_;  // 当前正在构建的数据块
  BlockBuilder* index_block_; // 索引块
  
  std::string last_key_;      // 记录上一个 Block 的最后一个 Key (用于索引)
  int64_t num_entries_;       // 总条目数
  bool closed_;               // 是否已 Finish/Abandon
  bool pending_index_entry_;  // 是否有一个索引项等待写入
  
  // 临时存储 BlockHandle 编码后的数据
  BlockHandle* pending_handle_; 

  FilterBlockBuilder* filter_block_; // 新增
};

} // namespace titankv