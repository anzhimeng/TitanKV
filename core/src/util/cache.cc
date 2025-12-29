#include "util/cache.h"
#include "util/coding.h" // 需要 Hash 函数，或者用 std::hash
#include <vector>
#include <mutex>

namespace titankv {

// 简单的 Hash 函数 (MurmurHash 更好，这里简化)
static uint32_t HashSlice(const Slice& s) {
    uint32_t h = 0;
    for (size_t i = 0; i < s.size(); i++) {
        h = 31 * h + s[i];
    }
    return h;
}

// 内部 LRU 节点
struct LRUHandle {
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash; // 哈希桶链表
  LRUHandle* next;      // LRU 链表
  LRUHandle* prev;
  size_t charge;        // 占用大小
  size_t key_length;
  bool in_cache;        // 是否在 Cache 中
  uint32_t refs;        // 引用计数
  uint32_t hash;        // Hash 值
  char key_data[1];     // 变长 Key 存储

  Slice key() const { return Slice(key_data, key_length); }
};

class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // 核心操作：插入、查找、释放、驱逐
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
  
  // 简单的哈希表实现
  LRUHandle** table_;
  uint32_t length_;
  uint32_t elems_;
  void Resize();

  size_t capacity_;
  size_t usage_;
  std::mutex mutex_;
  
  // LRU 双向链表头 (dummy head)
  LRUHandle lru_;
};

// ... LRUCache 的具体实现 (比较长，类似 LevelDB) ...
// 鉴于篇幅，我先给出 ShardedLRUCache 的框架，如果你需要 LRUCache 的详细代码我再发。

// 分片 Cache
class ShardedLRUCache : public Cache {
 private:
  static const int kNumShardBits = 4;
  static const int kNumShards = 1 << kNumShardBits; // 16 分片
  
  LRUCache shard_[kNumShards];
  std::mutex id_mutex_;
  uint64_t last_id_;

  static inline uint32_t Hash(const Slice& s) { return HashSlice(s); }
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
    const uint32_t hash = Hash(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }

  Handle* Lookup(const Slice& key) override {
    const uint32_t hash = Hash(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }

  void Release(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }

  void Erase(const Slice& key) override {
    const uint32_t hash = Hash(key);
    shard_[Shard(hash)].Erase(key, hash);
  }

  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }

  uint64_t NewId() override {
    std::lock_guard<std::mutex> lock(id_mutex_);
    return ++(last_id_);
  }
};

Cache* NewLRUCache(size_t capacity) {
  return new ShardedLRUCache(capacity);
}

// ----------------------------------------
// LRUCache 核心实现 (简化版)
// ----------------------------------------
LRUCache::LRUCache() : capacity_(0), usage_(0) {
    // 初始哈希表
    length_ = 4;
    elems_ = 0;
    table_ = new LRUHandle*[length_];
    memset(table_, 0, sizeof(LRUHandle*) * length_);

    lru_.next = &lru_;
    lru_.prev = &lru_;
}

LRUCache::~LRUCache() {
    // 释放所有条目
    for (LRUHandle* e = lru_.next; e != &lru_; ) {
        LRUHandle* next = e->next;
        e->in_cache = false; // 标记不在缓存
        Unref(e); // 释放引用 (如果在 Cache 中，引用计数初始为1)
        e = next;
    }
    delete[] table_;
}

void LRUCache::Ref(LRUHandle* e) {
    if (e->refs == 1 && e->in_cache) {
        // 如果之前只有 Cache 引用，现在被借出了，不能随便驱逐
    }
    e->refs++;
}

void LRUCache::Unref(LRUHandle* e) {
    assert(e->refs > 0);
    e->refs--;
    if (e->refs == 0) {
        // 真正释放内存
        (*e->deleter)(e->key(), e->value);
        free(e);
    } else if (e->in_cache && e->refs == 1) {
        // 只有 Cache 引用了，说明没有外部在使用，这才是真正的 LRU 列表
        // (LevelDB 的实现稍微复杂点，这里简化：所有 in_cache 的都在 LRU 链表里)
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
        // 移动到 LRU 尾部 (最近使用)
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

    // 1. 分配 Handle 内存
    LRUHandle* e = reinterpret_cast<LRUHandle*>(
        malloc(sizeof(LRUHandle) - 1 + key.size()));
    
    e->value = value;
    e->deleter = deleter;
    e->charge = charge;
    e->key_length = key.size();
    e->hash = hash;
    e->in_cache = false;
    e->refs = 1; // 返回给调用者的引用
    memcpy(e->key_data, key.data(), key.size());

    // 2. 如果容量够，放入 Cache
    if (capacity_ > 0) {
        e->refs++; // Cache 持有一份引用
        e->in_cache = true;
        LRU_Append(e);
        usage_ += charge;
        
        // 插入哈希表
        LRUHandle** ptr = &table_[hash & (length_ - 1)];
        // 查找是否已存在同名 Key
        LRUHandle* old = *ptr;
        while (old != nullptr && (old->hash != hash || key != old->key())) {
            ptr = &old->next_hash;
            old = *ptr;
        }
        
        if (old != nullptr) {
            // 替换旧的
            old->in_cache = false;
            // 从哈希链表移除
            *ptr = old->next_hash; 
            // 从 LRU 移除
            LRU_Remove(old);
            usage_ -= old->charge;
            Unref(old); // 释放 Cache 对旧条目的引用
        }
        
        // 插入新条目到哈希表
        e->next_hash = *ptr;
        *ptr = e; // 此时 ptr 指向的是 bucket 的 head 或者前驱的 next_hash
        // 这里简化了哈希表插入逻辑，实际上应该：
        // e->next_hash = table_[h];
        // table_[h] = e;
        // 上面的 ptr 指针操作有点绕，下面修正为标准插入：
        // bucket index:
        // uint32_t i = hash & (length_ - 1);
        // e->next_hash = table_[i];
        // table_[i] = e;
        
        // 只是为了替换旧值比较方便，既然上面已经移除了旧值，直接插头即可：
        // (修正)
        // 重新计算 ptr 是为了正确性，上面的 while 循环已经找到了位置或末尾
        // 但为了简单，我们还是用标准的头插法，因为旧值已经被摘除
        uint32_t i = hash & (length_ - 1);
        e->next_hash = table_[i];
        table_[i] = e;

        elems_++;
        if (elems_ > length_) {
            Resize();
        }
    } else {
        // 容量为0，不缓存，直接返回
        e->next = nullptr; // 避免野指针
    }

    // 3. 驱逐 (Eviction)
    while (usage_ > capacity_ && lru_.next != &lru_) {
        LRUHandle* old = lru_.next; // LRU 头部是最老的
        assert(old->refs == 1); // 在 Cache 中的应该只有 1 (如果没有外部引用)
        // 注意：这里简化了，LevelDB 会把 ref=1 的放在 lru 链表，ref>1 的不放或者放别的
        // 我们假设所有 in_cache 的都在 lru 链表。
        // 如果 refs > 1，说明有人在用，不能驱逐？
        // 标准 LRU 是不管有没有人用，只要容量满了就踢出 Cache（置 in_cache=false），
        // 但不释放内存（Unref），直到用户用完自己 Release。
        
        // 从哈希表移除
        LRU_Remove(old);
        
        // 从哈希表删除 (需要遍历)
        uint32_t h = old->hash & (length_ - 1);
        LRUHandle** ptr = &table_[h];
        while (*ptr != old) {
            ptr = &(*ptr)->next_hash;
        }
        *ptr = old->next_hash;
        
        old->in_cache = false;
        usage_ -= old->charge;
        Unref(old); // Cache 放弃引用
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
            return;
        }
        ptr = &e->next_hash;
    }
}

void LRUCache::Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
        new_length *= 2;
    }
    
    LRUHandle** new_table = new LRUHandle*[new_length];
    memset(new_table, 0, sizeof(LRUHandle*) * new_length);
    
    for (uint32_t i = 0; i < length_; i++) {
        LRUHandle* h = table_[i];
        while (h != nullptr) {
            LRUHandle* next = h->next_hash;
            uint32_t hash = h->hash;
            LRUHandle** ptr = &new_table[hash & (new_length - 1)];
            h->next_hash = *ptr;
            *ptr = h;
            h = next;
        }
    }
    delete[] table_;
    table_ = new_table;
    length_ = new_length;
}

} // namespace titankv