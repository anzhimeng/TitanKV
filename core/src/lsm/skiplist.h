#pragma once

#include <atomic>
#include <cassert>
#include <cstdlib>
#include "util/arena.h"
#include "util/random.h" // 稍微等下实现这个简单的随机数

namespace titankv {

template <typename Key, class Comparator>
class SkipList {
private:
    struct Node;

public:
    explicit SkipList(Comparator cmp, Arena* arena);

    // 禁止拷贝
    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;

    // 插入 Key。
    // 要求：外部必须加锁。
    void Insert(const Key& key);

    // 查找 Key 是否存在。
    // 要求：线程安全，不需要加锁。
    bool Contains(const Key& key) const;

    // 迭代器
    class Iterator {
    public:
        explicit Iterator(const SkipList* list);
        bool Valid() const;
        const Key& key() const;
        void Next();
        void Prev();
        void Seek(const Key& target);
        void SeekToFirst();
        void SeekToLast();

    private:
        const SkipList* list_;
        Node* node_;
    };

    Iterator* NewIterator();

private:
    // 最大层高
    enum { kMaxHeight = 12 };

    inline int GetMaxHeight() const {
        return max_height_.load(std::memory_order_relaxed);
    }

    Node* NewNode(const Key& key, int height);
    int RandomHeight();
    bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }
    bool KeyIsAfterNode(const Key& key, Node* n) const;
    Node* FindGreaterOrEqual(const Key& key, Node** prev) const;
    Node* FindLessThan(const Key& key) const;
    Node* FindLast() const;

    Comparator const compare_;
    Arena* const arena_;
    Node* const head_;
    std::atomic<int> max_height_;
    // 简单的随机数生成器
    Random rnd_;
};

// ===========================================
// Implementation Details (放在头文件里因为是模板类)
// ===========================================

template <typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
    explicit Node(const Key& k) : key(k) {}

    Key const key;

    // Accessors/Mutators for links.  Wrapped in methods so we can
    // add the appropriate memory barriers later.
    Node* Next(int n) {
        assert(n >= 0);
        // Use an 'acquire load' so that we observe a fully initialized
        // version of the returned Node.
        return next_[n].load(std::memory_order_acquire);
    }

    void SetNext(int n, Node* x) {
        assert(n >= 0);
        // Use a 'release store' so that anybody who reads through this
        // pointer observes a fully initialized *x.
        next_[n].store(x, std::memory_order_release);
    }
    
    // No-barrier variants for single thread initialization
    Node* NoBarrier_Next(int n) {
        return next_[n].load(std::memory_order_relaxed);
    }
    void NoBarrier_SetNext(int n, Node* x) {
        next_[n].store(x, std::memory_order_relaxed);
    }

private:
    // Array of length equal to the node height.  next_[0] is lowest level link.
    std::atomic<Node*> next_[1];
};

template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      max_height_(1),
      rnd_(0xdeadbeef) { // 初始化种子，可以是任意非零值，或者 time(NULL)
    for (int i = 0; i < kMaxHeight; i++) {
        head_->SetNext(i, nullptr);
    }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* 
SkipList<Key, Comparator>::NewNode(const Key& key, int height) {
    // 骚操作：在 Arena 里只分配一块内存
    // sizeof(Node) 包含了 next_[1]，所以只需额外分配 height - 1 个指针
    char* const node_memory = arena_->AllocateAligned(
        sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
    return new (node_memory) Node(key);
}

// 核心逻辑：找到 >= Key 的第一个节点
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* 
SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key, Node** prev) const {
    Node* x = head_;
    int level = GetMaxHeight() - 1;
    while (true) {
        Node* next = x->Next(level);
        if (KeyIsAfterNode(key, next)) {
            // Keep searching in this list
            x = next;
        } else {
            if (prev != nullptr) prev[level] = x;
            if (level == 0) {
                return next;
            } else {
                // Switch to next list
                level--;
            }
        }
    }
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
    // null n is considered infinite
    return (n != nullptr) && (compare_(n->key, key) < 0);
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key) {
    Node* prev[kMaxHeight];
    Node* x = FindGreaterOrEqual(key, prev);

    // 不允许插入重复键 (InternalKey 带 SeqNum 不会重复)
    assert(x == nullptr || !Equal(key, x->key));

    int height = RandomHeight();
    if (height > GetMaxHeight()) {
        for (int i = GetMaxHeight(); i < height; i++) {
            prev[i] = head_;
        }
        max_height_.store(height, std::memory_order_relaxed);
    }

    x = NewNode(key, height);
    for (int i = 0; i < height; i++) {
        x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
        prev[i]->SetNext(i, x);
    }
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) const {
    Node* x = FindGreaterOrEqual(key, nullptr);
    return x != nullptr && Equal(key, x->key);
}

// 简单的随机层高生成
template <typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight() {
    // Increase height with probability 1 in 4
    int height = 1;
    while (height < kMaxHeight && rnd_.OneIn(4)) {
        height++;
    }
    return height;
}

// --- Iterator Implementation ---

template <typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list) {
    list_ = list;
    node_ = nullptr;
}

template <typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::Valid() const {
    return node_ != nullptr;
}

template <typename Key, class Comparator>
inline const Key& SkipList<Key, Comparator>::Iterator::key() const {
    assert(Valid());
    return node_->key;
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next() {
    assert(Valid());
    node_ = node_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev() {
    // Prev is slower than Next
    assert(Valid());
    node_ = list_->FindLessThan(node_->key);
    if (node_ == list_->head_) {
        node_ = nullptr;
    }
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
    node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
    node_ = list_->head_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {
    node_ = list_->FindLast();
    if (node_ == list_->head_) {
        node_ = nullptr;
    }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* 
SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
    Node* x = head_;
    int level = GetMaxHeight() - 1;
    while (true) {
        assert(x == head_ || compare_(x->key, key) < 0);
        Node* next = x->Next(level);
        if (next == nullptr || compare_(next->key, key) >= 0) {
            if (level == 0) {
                return x;
            } else {
                level--;
            }
        } else {
            x = next;
        }
    }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* 
SkipList<Key, Comparator>::FindLast() const {
    Node* x = head_;
    int level = GetMaxHeight() - 1;
    while (true) {
        Node* next = x->Next(level);
        if (next == nullptr) {
            if (level == 0) {
                return x;
            } else {
                level--;
            }
        } else {
            x = next;
        }
    }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Iterator* 
SkipList<Key, Comparator>::NewIterator() {
    return new Iterator(this);
}

} // namespace titankv