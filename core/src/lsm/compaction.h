#pragma once
#include "lsm/version_set.h"
#include <vector>

namespace titankv {

class Compaction {
 public:
  Compaction(int level) : level_(level) {}
  ~Compaction() = default;

  // level_: 输入层 (例如 L0)
  // inputs_[0]: level_ 层参与合并的文件
  // inputs_[1]: level_+1 层参与合并的文件
  int level() const { return level_; }

  // 获取输入文件列表
  // which: 0 或 1
  std::vector<FileMetaData*>* inputs(int which) { return &inputs_[which]; }

  // 获取输入文件数量
  int num_input_files(int which) const { return inputs_[which].size(); }

 private:
  int level_;
  std::vector<FileMetaData*> inputs_[2];
};

} // namespace titankv