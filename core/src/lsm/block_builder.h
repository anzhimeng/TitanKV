#pragma once
#include <vector>
#include <string>
#include <cstdint>
#include "titankv/slice.h"
#include "titankv/options.h" // 需要 options.block_restart_interval

namespace titankv {

class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options);

  // 重置状态，准备构建新 Block
  void Reset();

  // 添加 KV。要求：Key 必须 > 上一个 Key (SSTable 是有序的)
  void Add(const Slice& key, const Slice& value);

  // 结束构建，返回打包好的 Slice (指向 buffer_)
  // 返回的 Slice 生命周期与 BlockBuilder 绑定
  Slice Finish();

  // 估算当前 Block 大小 (用于判断是否切分 Block)
  size_t CurrentSizeEstimate() const;

  // 判断是否为空
  bool empty() const { return buffer_.empty(); }

 private:
  const Options* options_;
  std::string buffer_;              // 存储压缩后的 KV 数据
  std::vector<uint32_t> restarts_;  // 重启点偏移量数组
  int counter_;                     // 当前重启点之后的 key 计数
  bool finished_;                   // 是否调用过 Finish
  std::string last_key_;            // 上一个插入的 Key (用于计算前缀)
};

} // namespace titankv