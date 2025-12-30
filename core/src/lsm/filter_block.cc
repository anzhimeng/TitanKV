#include "lsm/filter_block.h"

namespace titankv {

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

void FilterBlockBuilder::AddKey(const Slice& key) {
  keys_.push_back(key.ToString());
}

Slice FilterBlockBuilder::Finish() {
  if (!keys_.empty()) {
    policy_->CreateFilter(keys_, &result_);
  }
  return Slice(result_);
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy, const Slice& contents)
    : policy_(policy), data_(contents.data()), size_(contents.size()) {}

bool FilterBlockReader::KeyMayMatch(const Slice& key) {
  return policy_->KeyMayMatch(key, Slice(data_, size_));
}

} // namespace titankv