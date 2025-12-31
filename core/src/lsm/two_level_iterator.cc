#include "lsm/two_level_iterator.h"
#include "titankv/status.h"

namespace titankv {

class TwoLevelIterator : public Iterator {
 public:
  TwoLevelIterator(Iterator* index_iter, BlockFunction block_function,
                   void* arg, const ReadOptions& options)
      : block_function_(block_function),
        arg_(arg),
        options_(options),
        index_iter_(index_iter),
        data_iter_(nullptr) {}

  ~TwoLevelIterator() override {
    delete index_iter_;
    delete data_iter_;
  }

  void Seek(const Slice& target) override {
    // 1. 先查索引，找到目标 Key 可能在哪个 Block
    index_iter_->Seek(target);
    
    // 2. 根据索引加载 Data Block
    InitDataBlock();
    
    // 3. 在 Data Block 内部查找
    if (data_iter_ != nullptr) {
      data_iter_->Seek(target);
    }
    
    // 4. 处理边界情况 (例如 Block 是空的，或者 target > Block.LastKey)
    SkipEmptyDataBlocksForward();
  }

  void SeekToFirst() override {
    index_iter_->SeekToFirst();
    InitDataBlock();
    if (data_iter_ != nullptr) {
      data_iter_->SeekToFirst();
    }
    SkipEmptyDataBlocksForward();
  }

  void SeekToLast() override {
    index_iter_->SeekToLast();
    InitDataBlock();
    if (data_iter_ != nullptr) {
      data_iter_->SeekToLast();
    }
    // 反向跳过空块逻辑略复杂，Day 2 暂略，Compaction 主要用 Forward
    // SkipEmptyDataBlocksBackward(); 
  }

  void Next() override {
    assert(Valid());
    data_iter_->Next();
    SkipEmptyDataBlocksForward();
  }

  // Compaction 暂时不需要 Prev，留空
  void Prev() override {
    // data_iter_->Prev();
    // SkipEmptyDataBlocksBackward();
  }

  bool Valid() const override {
    return data_iter_ != nullptr && data_iter_->Valid();
  }

  Slice key() const override {
    assert(Valid());
    return data_iter_->key();
  }

  Slice value() const override {
    assert(Valid());
    return data_iter_->value();
  }

  Status status() const override {
    if (!index_iter_->status().ok()) {
      return index_iter_->status();
    }
    if (data_iter_ != nullptr && !data_iter_->status().ok()) {
      return data_iter_->status();
    }
    return status_;
  }

 private:
  BlockFunction block_function_;
  void* arg_;
  const ReadOptions options_;
  Iterator* index_iter_;
  Iterator* data_iter_; // 当前 Data Block 的迭代器
  std::string data_block_handle_; // 缓存当前加载的 Handle，避免重复加载
  Status status_;

  // 根据 index_iter_ 当前指向的 Handle，加载 Data Block
  void InitDataBlock() {
    if (!index_iter_->Valid()) {
      SetDataIterator(nullptr);
    } else {
      Slice handle = index_iter_->value();
      // 优化：如果 handle 没变，就不用重新加载 (Seek 经常会 Seek 到同一个 Block)
      if (data_iter_ != nullptr && handle.compare(Slice(data_block_handle_)) == 0) {
        // data_iter_ is already constructed for this handle
      } else {
        // 加载新 Block
        Iterator* iter = (*block_function_)(arg_, options_, handle);
        data_block_handle_.assign(handle.data(), handle.size());
        SetDataIterator(iter);
      }
    }
  }

  void SetDataIterator(Iterator* data_iter) {
    if (data_iter_) delete data_iter_;
    data_iter_ = data_iter;
  }

  // 如果 data_iter 跑完了，移动 index_iter 加载下一个 Block
  void SkipEmptyDataBlocksForward() {
    while (data_iter_ == nullptr || !data_iter_->Valid()) {
      // 当前 Data Block 完了
      if (!index_iter_->Valid()) {
        SetDataIterator(nullptr);
        return;
      }
      
      // 尝试下一个 Block
      if (data_iter_ != nullptr) {
          // 说明刚才 Valid() 为 false 是因为跑完了，现在该切 Block 了
          index_iter_->Next();
          InitDataBlock();
          if (data_iter_ != nullptr) {
              data_iter_->SeekToFirst();
          }
      } else {
          // data_iter_ 为 null，可能是 InitDataBlock 失败或 Index 刚开始
          // 这里的逻辑分支要小心死循环，简单起见：
          // 如果 InitDataBlock 后 data_iter 依然无效，说明 Block 是空的或者坏的，继续 Next
           if (!index_iter_->Valid()) return;
           // InitDataBlock 已经调用过了
      }
    }
  }
};

Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              BlockFunction block_function,
                              void* arg,
                              const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

} // namespace titankv