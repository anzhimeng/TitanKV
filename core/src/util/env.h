#pragma once

#include <string>
#include <memory>
#include "titankv/status.h"
#include "titankv/slice.h" // 假设 Slice 在这里

namespace titankv {

class WritableFile {
 public:
  WritableFile() = default;
  virtual ~WritableFile() = default;

  // Non-copyable
  WritableFile(const WritableFile&) = delete;
  WritableFile& operator=(const WritableFile&) = delete;

  virtual Status Append(const Slice& data) = 0;
  virtual Status Close() = 0;
  virtual Status Sync() = 0; 
  virtual Status Flush() { return Status::OK(); }
};

Status NewWritableFile(const std::string& filename, std::unique_ptr<WritableFile>* result);

class SequentialFile {
public:
    virtual ~SequentialFile() = default;
    // 读取最多 n 个字节到 scratch，result 返回实际读取的数据
    virtual Status Read(size_t n, Slice* result, char* scratch) = 0;
    virtual Status Skip(uint64_t n) = 0;
};

Status NewSequentialFile(const std::string& fname, std::unique_ptr<SequentialFile>* result);

class RandomAccessFile {
public:
    virtual ~RandomAccessFile() = default;
    // 从 offset 处读取 n 个字节
    // scratch: 调用者提供的缓冲区
    // result: 返回实际读取的 Slice (指向 scratch 或内部缓存)
    virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const = 0;
    // 【新增】获取原始文件描述符 (用于 io_uring)
    // 默认返回 -1 表示不支持
    virtual int UnsafeGetFD() const { return -1; }
};

// 增加 use_direct_io 参数，默认为 false
Status NewRandomAccessFile(const std::string& fname, 
                           std::unique_ptr<RandomAccessFile>* result,
                           bool use_direct_io = false);
Status WriteStringToFile(const std::string& fname, const Slice& data);

}
