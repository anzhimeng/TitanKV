#pragma once

#include <cstdint>
#include <cstddef>
#include "titankv/slice.h"

namespace titankv {

class Cache;

// 创建一个具有固定容量的 LRU Cache
// capacity: 总字节数
Cache* NewLRUCache(size_t capacity);

class Cache {
 public:
  Cache() = default;
  virtual ~Cache() = default;

  // 缓存项句柄
  struct Handle {};

  // 插入
  // charge: 该条目占用的花费（通常是数据大小）
  // deleter: 当条目被彻底删除时的回调（用于释放 value）
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) = 0;

  // 查找
  // 如果找到，返回 Handle。调用者必须在使用完后调用 Release(handle)。
  virtual Handle* Lookup(const Slice& key) = 0;

  // 释放 Handle (引用计数 -1)
  virtual void Release(Handle* handle) = 0;

  // 获取 Value
  virtual void* Value(Handle* handle) = 0;

  // 显式从缓存中删除
  virtual void Erase(const Slice& key) = 0;

  // 生成唯一 ID
  virtual uint64_t NewId() = 0;

 private:
  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;
};

} // namespace titankv