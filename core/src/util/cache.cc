#include "util/cache.h"
#include "util/hash.h"
#include <vector>
#include <mutex>
#include <cstring>
#include <cassert>
#include <cstdlib>

namespace titankv {

struct LRUHandle {
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;
  size_t key_length;
  bool in_cache;
  uint32_t refs;
  uint32_t hash;
  char key_data[1];

  Slice key() const { return Slice(key_data, key_length); }
};

class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  void Resize();

  size_t capacity_;
  size_t usage_;
  std::mutex mutex_;
  LRUHandle** table_;
  uint32_t length_;
  uint32_t elems_;
  LRUHandle lru_;
};

LRUCache::LRUCache() : capacity_(0), usage_(0), length_(0), elems_(0), table_(nullptr) {
    lru_.next = &lru_;
    lru_.prev = &lru_;
    length_ = 16;
    table_ = new LRUHandle*[length_];
    memset(table_, 0, sizeof(LRUHandle*) * length_);
}

LRUCache::~LRUCache() {
    for (LRUHandle* e = lru_.next; e != &lru_; ) {
        LRUHandle* next = e->next;
        e->in_cache = false;
        Unref(e);
        e = next;
    }
    delete[] table_;
}

// 【关键修复】Ref: 如果有人要用，必须从 LRU 驱逐链表中移除
void LRUCache::Ref(LRUHandle* e) {
    if (e->refs == 1 && e->in_cache) {
        // 之前 refs=1，说明它在 LRU 链表里。
        // 现在 refs++ 变为 2，变成了“正在使用”，必须移出 LRU 链表，防止被驱逐。
        LRU_Remove(e);
    }
    e->refs++;
}

// 【关键修复】Unref: 如果用完了，且还在 Cache 中，放回 LRU 链表
void LRUCache::Unref(LRUHandle* e) {
    assert(e->refs > 0);
    e->refs--;
    if (e->refs == 0) {
        // 没人用，且不在 Cache 中（已经被 Erase 或 Evict）
        (*e->deleter)(e->key(), e->value);
        free(e);
    } else if (e->in_cache && e->refs == 1) {
        // 引用归 1，说明只有 Cache 持有它。
        // 它变成了“闲置”状态，放入 LRU 链表尾部，成为驱逐候选人。
        LRU_Append(e);
    }
}

void LRUCache::LRU_Remove(LRUHandle* e) {
    e->next->prev = e->prev;
    e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* e) {
    e->next = &lru_;
    e->prev = lru_.prev;
    e->prev->next = e;
    e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
    std::lock_guard<std::mutex> l(mutex_);
    LRUHandle* e = table_[hash & (length_ - 1)];
    while (e != nullptr && (e->hash != hash || key != e->key())) {
        e = e->next_hash;
    }
    if (e != nullptr) {
        Ref(e); // Ref 会把它从 LRU 列表移除
    }
    return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Release(Cache::Handle* handle) {
    std::lock_guard<std::mutex> l(mutex_);
    Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(const Slice& key, void* value)) {
    std::lock_guard<std::mutex> l(mutex_);

    LRUHandle* e = reinterpret_cast<LRUHandle*>(
        malloc(sizeof(LRUHandle) - 1 + key.size()));
    e->value = value;
    e->deleter = deleter;
    e->charge = charge;
    e->key_length = key.size();
    e->hash = hash;
    e->in_cache = false;
    e->refs = 1; 
    memcpy(e->key_data, key.data(), key.size());

    if (capacity_ > 0) {
        e->refs++; // Cache 持有引用
        e->in_cache = true;
        // 【关键修复】新插入的 Item 是要返回给用户的，所以 refs=2。
        // 它处于“使用中”状态，绝不能放入 LRU 链表！
        // LRU_Append(e); <--- 删掉这行
        usage_ += charge;

        LRUHandle** ptr = &table_[hash & (length_ - 1)];
        while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
            ptr = &(*ptr)->next_hash;
        }
        
        LRUHandle* old = *ptr;
        if (old != nullptr) {
            old->in_cache = false;
            *ptr = old->next_hash; 
            usage_ -= old->charge;
            
            // 旧值被替换了，如果它在 LRU 链表里（refs=1），需要移除
            // 如果它正在被别人用（refs>1），它本身就不在 LRU 链表里，Remove 是空操作吗？
            // 我们的 LRU_Remove 没有检查是否在链表里，这很危险。
            // 但根据逻辑，如果 refs=1, 它一定在。如果 refs>1，它一定不在。
            if (old->refs == 1) {
                LRU_Remove(old);
            }
            Unref(old);
        }
        
        e->next_hash = *ptr;
        *ptr = e;
        elems_++;
        if (elems_ > length_) Resize();
    } else {
        e->next = nullptr;
    }

    // 驱逐逻辑
    while (usage_ > capacity_ && lru_.next != &lru_) {
        LRUHandle* old = lru_.next;
        // 这里的断言现在应该永远为真，因为只有 refs=1 的才会在 lru_ 里
        assert(old->refs == 1);
        
        // 从哈希表移除
        LRU_Remove(old);
        
        uint32_t h = old->hash & (length_ - 1);
        LRUHandle** ptr = &table_[h];
        while (*ptr != old) {
            ptr = &(*ptr)->next_hash;
        }
        *ptr = old->next_hash;
        
        old->in_cache = false;
        usage_ -= old->charge;
        Unref(old); // 这会使 refs 变为 0 并 delete
        elems_--;
    }

    return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
    std::lock_guard<std::mutex> l(mutex_);
    uint32_t i = hash & (length_ - 1);
    LRUHandle** ptr = &table_[i];
    while (*ptr != nullptr) {
        LRUHandle* e = *ptr;
        if (e->hash == hash && key == e->key()) {
            *ptr = e->next_hash;
            e->in_cache = false;
            usage_ -= e->charge;
            // 如果在 LRU 链表中，移除
            if (e->refs == 1) {
                LRU_Remove(e);
            }
            Unref(e);
            elems_--;
            return;
        }
        ptr = &e->next_hash;
    }
}

// ... Resize 和 ShardedLRUCache 保持不变，直接复用之前的代码即可 ...
// (为了篇幅，这里假设 Resize 和 ShardedLRUCache 代码你已经有了且没动过)
// 如果需要，请从上一次 cache.cc 的回复中复制粘贴 ShardedLRUCache 部分
void LRUCache::Resize() {
    uint32_t new_len = 4;
    while (new_len < elems_) new_len *= 2;
    LRUHandle** new_table = new LRUHandle*[new_len];
    memset(new_table, 0, sizeof(LRUHandle*) * new_len);
    for (uint32_t i = 0; i < length_; i++) {
        LRUHandle* h = table_[i];
        while (h != nullptr) {
            LRUHandle* next = h->next_hash;
            uint32_t hash = h->hash;
            LRUHandle** ptr = &new_table[hash & (new_len - 1)];
            h->next_hash = *ptr;
            *ptr = h;
            h = next;
        }
    }
    delete[] table_;
    table_ = new_table;
    length_ = new_len;
}

class ShardedLRUCache : public Cache {
 private:
  static const int kNumShardBits = 4;
  static const int kNumShards = 1 << kNumShardBits;
  LRUCache shard_[kNumShards];
  std::mutex id_mutex_;
  uint64_t last_id_;

  static inline uint32_t Hash(const Slice& s) { 
      return titankv::Hash(s.data(), s.size(), 0); 
  }
  static inline uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  explicit ShardedLRUCache(size_t capacity) : last_id_(0) {
    size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int i = 0; i < kNumShards; i++) shard_[i].SetCapacity(per_shard);
  }
  Handle* Insert(const Slice& key, void* value, size_t charge, void (*deleter)(const Slice&, void*)) override {
    uint32_t hash = Hash(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  Handle* Lookup(const Slice& key) override {
    uint32_t hash = Hash(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  void Release(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  void Erase(const Slice& key) override {
    uint32_t hash = Hash(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  uint64_t NewId() override {
    std::lock_guard<std::mutex> l(id_mutex_);
    return ++(last_id_);
  }
};

Cache* NewLRUCache(size_t capacity) {
  return new ShardedLRUCache(capacity);
}

} // namespace titankv