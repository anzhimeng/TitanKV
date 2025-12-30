#include "lsm/merging_iterator.h"
#include <vector>
#include <queue>

namespace titankv {

class MergingIterator : public Iterator {
 public:
  MergingIterator(const InternalKeyComparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(children, children + n), // 拷贝指针数组
        current_(nullptr),
        direction_(kForward) {
  }

  ~MergingIterator() override {
    for (auto* child : children_) {
      delete child;
    }
  }

  bool Valid() const override {
    return current_ != nullptr;
  }

  void SeekToFirst() override {
    // 1. 清空堆
    ClearHeap();
    
    // 2. 所有子迭代器 SeekToFirst
    for (auto* child : children_) {
      child->SeekToFirst();
      if (child->Valid()) {
          min_heap_.push(child);
      }
    }
    
    // 3. 取堆顶
    FindSmallest();
    direction_ = kForward;
  }

  void SeekToLast() override {
      // Compaction 主要用 Forward Scan，SeekToLast 实现较复杂
      // 简单实现：所有子迭代器 SeekToLast，然后用 Max-Heap (未实现)
      // Day 1 暂时留空或抛错，专注于 Compaction 需要的 Seek/Next
      current_ = nullptr; 
  }

  void Seek(const Slice& target) override {
    ClearHeap();
    for (auto* child : children_) {
      child->Seek(target);
      if (child->Valid()) {
        min_heap_.push(child);
      }
    }
    FindSmallest();
    direction_ = kForward;
  }

  void Next() override {
    assert(Valid());

    // 确保方向一致性 (如果之前是 Prev，现在转 Next，需要复杂处理，这里简化)
    if (direction_ != kForward) {
        // 生产级代码需要处理方向切换时的游标调整
        // Day 1 简化：假设只做前向扫描
        direction_ = kForward;
    }

    // 1. 当前最小的迭代器向前推一步
    // 注意：current_ 并不在堆里，它是在 FindSmallest 时被 pop 出来的
    current_->Next();
    
    // 2. 如果它还 Valid，放回堆里
    if (current_->Valid()) {
      min_heap_.push(current_);
    }
    
    // 3. 取出新的堆顶
    FindSmallest();
  }

  void Prev() override {
      // 暂时不支持，Compaction 不需要
      current_ = nullptr;
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

 private:
  // 定义堆的比较规则
  struct ComparatorWrapper {
      const InternalKeyComparator* comparator;
      
      // std::priority_queue 默认是最大堆，所以要反过来：
      // 如果 a > b，返回 true，这样 a 就会沉底，小的在上面
      bool operator()(Iterator* a, Iterator* b) const {
          return comparator->Compare(a->key(), b->key()) > 0;
      }
  };

  const InternalKeyComparator* comparator_;
  std::vector<Iterator*> children_;
  Iterator* current_; // 当前指向最小 Key 的那个迭代器
  
  // 最小堆
  // 使用 vector 作为底层容器，ComparatorWrapper 作为比较器
  std::priority_queue<Iterator*, std::vector<Iterator*>, ComparatorWrapper> min_heap_{ComparatorWrapper{nullptr}};
  
  // 方向控制
  enum Direction {
      kForward,
      kReverse
  };
  Direction direction_;

  void ClearHeap() {
      // 重新初始化堆，需要传入 comparator
      min_heap_ = std::priority_queue<Iterator*, std::vector<Iterator*>, ComparatorWrapper>(ComparatorWrapper{comparator_});
      current_ = nullptr;
  }

  void FindSmallest() {
      if (min_heap_.empty()) {
          current_ = nullptr;
      } else {
          current_ = min_heap_.top();
          min_heap_.pop(); // 从堆中移除，直到它 Next 后再放回来
      }
  }
};

Iterator* NewMergingIterator(const InternalKeyComparator* comparator, Iterator** children, int n) {
  if (n == 0) {
    return nullptr; // 或者返回一个 EmptyIterator
  } else if (n == 1) {
    // 优化：如果只有一个，直接返回它
    return children[0];
  } else {
    return new MergingIterator(comparator, children, n);
  }
}

} // namespace titankv