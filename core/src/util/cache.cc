#include "util/cache.h"
#include "util/hash.h" // 【关键】引入 MurmurHash
#include <vector>
#include <mutex>
#include <cstring>
#include <cassert>
#include <cstdlib>

namespace titankv {

// 内部 LRU 节点
struct LRUHandle {
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;
  size_t key_length;
  bool in_cache;      // 是否还在哈希表中
  uint32_t refs;      // 引用计数
  uint32_t hash;      // 哈希值
  char key_data[1];   // 变长 Key 存储 (Flexible Array Member)

  Slice key() const { return Slice(key_data, key_length); }
};

// 单个分片的 LRU 实现
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
  
  // 哈希表
  LRUHandle** table_;
  uint32_t length_;
  uint32_t elems_;
  
  // LRU 链表头 (dummy node)
  LRUHandle lru_;
};

LRUCache::LRUCache() : capacity_(0), usage_(0), length_(0), elems_(0), table_(nullptr) {
    lru_.next = &lru_;
    lru_.prev = &lru_;
    // 初始哈希表大小
    length_ = 16;
    table_ = new LRUHandle*[length_];
    memset(table_, 0, sizeof(LRUHandle*) * length_);
}

LRUCache::~LRUCache() {
    for (LRUHandle* e = lru_.next; e != &lru_; ) {
        LRUHandle* next = e->next;
        e->in_cache = false; // 标记失效
        Unref(e); // 释放引用
        e = next;
    }
    delete[] table_;
}

void LRUCache::Ref(LRUHandle* e) {
    if (e->refs == 1 && e->in_cache) {
        // 如果之前引用计数为1（只有Cache持有），说明它之前在LRU列表里待回收
        // 现在有人要用它了，把它移出LRU列表（或者移到队尾，这里简单处理，Ref时不移动，Lookup时移动）
        LRU_Remove(e);
        LRU_Append(e);
    }
    e->refs++;
}

void LRUCache::Unref(LRUHandle* e) {
    assert(e->refs > 0);
    e->refs--;
    if (e->refs == 0) {
        // 引用归零，彻底删除
        (*e->deleter)(e->key(), e->value);
        free(e);
    } else if (e->in_cache && e->refs == 1) {
        // 只有 Cache 引用了，没有外部用户，放入 LRU 链表等待驱逐
        LRU_Remove(e);
        LRU_Append(e);
    }
}

void LRUCache::LRU_Remove(LRUHandle* e) {
    e->next->prev = e->prev;
    e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* e) {
    // 插入到 lru_ 之前 (即链表尾部，表示最近使用)
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
        Ref(e);
        // 移动到 LRU 尾部 (热数据)
        LRU_Remove(e);
        LRU_Append(e);
        return reinterpret_cast<Cache::Handle*>(e);
    }
    return nullptr;
}

void LRUCache::Release(Cache::Handle* handle) {
    std::lock_guard<std::mutex> l(mutex_);
    Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(const Slice& key, void* value)) {
    std::lock_guard<std::mutex> l(mutex_);

    // 1. 创建新节点
    LRUHandle* e = reinterpret_cast<LRUHandle*>(
        malloc(sizeof(LRUHandle) - 1 + key.size()));
    e->value = value;
    e->deleter = deleter;
    e->charge = charge;
    e->key_length = key.size();
    e->hash = hash;
    e->in_cache = false;
    e->refs = 1; // 返回给用户的引用
    memcpy(e->key_data, key.data(), key.size());

    if (capacity_ > 0) {
        e->refs++; // Cache 持有的引用
        e->in_cache = true;
        LRU_Append(e);
        usage_ += charge;

        // 插入哈希表
        LRUHandle** ptr = &table_[hash & (length_ - 1)];
        while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
            ptr = &(*ptr)->next_hash;
        }
        
        LRUHandle* old = *ptr;
        if (old != nullptr) {
            // 替换旧值
            old->in_cache = false;
            *ptr = old->next_hash; 
            LRU_Remove(old);
            usage_ -= old->charge;
            Unref(old);
        }
        
        e->next_hash = *ptr;
        *ptr = e;
        
        if (old == nullptr) {
            elems_++;
            if (elems_ > length_) Resize();
        }
    } else {
        // Cache 容量为 0，不缓存
        e->next = nullptr;
    }

    // 2. 驱逐 (如果容量超了)
    while (usage_ > capacity_ && lru_.next != &lru_) {
        LRUHandle* old = lru_.next; // LRU 头部是最老的
        assert(old->refs == 1);
        
        // 从哈希表移除
        uint32_t h = old->hash & (length_ - 1);
        LRUHandle** ptr = &table_[h];
        while (*ptr != old) {
            ptr = &(*ptr)->next_hash;
        }
        *ptr = old->next_hash;
        
        // 从 LRU 移除
        LRU_Remove(old);
        old->in_cache = false;
        usage_ -= old->charge;
        Unref(old); // 释放 Cache 的引用
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
            LRU_Remove(e);
            e->in_cache = false;
            usage_ -= e->charge;
            Unref(e);
            elems_--;
            return;
        }
        ptr = &e->next_hash;
    }
}

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

// ----------------------------------------
// ShardedLRUCache 实现 (Wrapper)
// ----------------------------------------

class ShardedLRUCache : public Cache {
 private:
  static const int kNumShardBits = 4;
  static const int kNumShards = 1 << kNumShardBits;
  LRUCache shard_[kNumShards];
  std::mutex id_mutex_;
  uint64_t last_id_;

  // 【关键修改】使用 MurmurHash
  static inline uint32_t Hash(const Slice& s) { 
      return titankv::Hash(s.data(), s.size(), 0); 
  }
  
  static inline uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  explicit ShardedLRUCache(size_t capacity) : last_id_(0) {
    size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int i = 0; i < kNumShards; i++) {
        shard_[i].SetCapacity(per_shard);
    }
  }

  Handle* Insert(const Slice& key, void* value, size_t charge,
                 void (*deleter)(const Slice& key, void* value)) override {
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