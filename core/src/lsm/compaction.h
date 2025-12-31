#pragma once
#include "lsm/version_set.h"
#include "lsm/version_edit.h"
#include <vector>

namespace titankv {

class Compaction {
 public:
  Compaction(const Options* options, int level)
      : level_(level), 
      max_output_file_size_(options->max_file_size) {

  }
  ~Compaction() {
       // inputs 里的 FileMetaData* 是 Version 管理的，这里不需要 delete
  }

  // 【关键修复】定义 OutputFile 结构体
  struct OutputFile {
      uint64_t number;
      uint64_t file_size;
      std::string smallest; // 使用 string 简化
      std::string largest;
  };
  // level_: 输入层 (例如 L0)
  // inputs_[0]: level_ 层参与合并的文件
  // inputs_[1]: level_+1 层参与合并的文件
  int level() const { return level_; }
  // 【关键修复】添加 AddOutputFile 方法
  void AddOutputFile(const OutputFile& out) { outputs_.push_back(out); }
  // 获取输入文件列表
  // which: 0 或 1
  std::vector<FileMetaData*>* inputs(int which) { return &inputs_[which]; }
  const std::vector<OutputFile>& outputs() const { return outputs_; }
  // 【关键修复】添加 AddToEdit 方法
  void AddToEdit(VersionEdit* edit) {
      // 1. 删除输入文件
      for (int which = 0; which < 2; which++) {
          for (auto* f : inputs_[which]) {
              edit->DeleteFile(level_ + which, f->file_number);
          }
      }
      // 2. 添加输出文件
      for (const auto& out : outputs_) {
          // 这里将 string 转回 Slice
          edit->AddFile(level_ + 1, out.number, out.file_size, 
                        Slice(out.smallest), Slice(out.largest));
      }
  }
  // 判断是否可以直接把文件移动到下一层（如果下一层没有重叠）
  bool IsTrivialMove() const {
      const std::vector<FileMetaData*>& files = inputs_[0];
      // const std::vector<FileMetaData*>& inputs1 = inputs_[1];
      // 1. 只有1个输入文件
      // 2. 下一层没有重叠文件
      // 3. (高级) 且不与下下层 (Grandparent) 重叠太多 (Day 1 暂略)
      return (files.size() == 1 && inputs_[1].empty());
  }

  // 获取输入文件数量
  int num_input_files(int which) const { return inputs_[which].size(); }
  // 获取配置的文件大小上限 (供 DoCompactionWork 使用)
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

 private:
  int level_;
  uint64_t max_output_file_size_;
  std::vector<FileMetaData*> inputs_[2];
  // 【关键修复】存储输出文件列表
  std::vector<OutputFile> outputs_;
};

} // namespace titankv