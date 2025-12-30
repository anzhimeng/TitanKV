#pragma once

#include <cstdint>
#include <string>
#include "titankv/slice.h"

namespace titankv {

class Cache;

// 创建一个具有固定容量的 LRU Cache
// capacity: 总字节数
Cache* NewLRUCache(size_t capacity);

class Cache {
 public:
  Cache() = default;
  virtual ~Cache() = default;;

  // 缓存项的句柄 (Handle)，用于管理引用计数
  struct Handle {};

  // 插入
  // deleter: 当条目被驱逐时调用的回调函数 (用于释放 value 内存)
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) = 0;

  // 查找
  // 如果找到，返回 Handle，调用者必须在使用完后调用 Release
  virtual Handle* Lookup(const Slice& key) = 0;

  // 释放 Handle (引用计数 -1)
  virtual void Release(Handle* handle) = 0;

  // 获取 Value
  virtual void* Value(Handle* handle) = 0;

  // 驱逐某个 Key
  virtual void Erase(const Slice& key) = 0;

  // 生成唯一的 Cache ID (用于区分不同 DB 实例)
  virtual uint64_t NewId() = 0;

 private:
  // 禁止拷贝
  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;
};

} // namespace titankv