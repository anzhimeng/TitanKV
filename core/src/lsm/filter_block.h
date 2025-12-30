#pragma once
#include <vector>
#include <string>
#include "titankv/options.h"
#include "titankv/slice.h"

namespace titankv {

class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy* policy);

  void AddKey(const Slice& key);
  Slice Finish();

 private:
  const FilterPolicy* policy_;
  std::vector<std::string> keys_; // 缓存所有 keys，Finish 时计算
  std::string result_; // 最终的 Filter 数据
};

class FilterBlockReader {
 public:
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(const Slice& key);

 private:
  const FilterPolicy* policy_;
  const char* data_;
  size_t size_;
};

} // namespace titankv