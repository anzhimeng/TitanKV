#pragma once
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include "titankv/status.h"
#include "titankv/slice.h"
#include "blob/blob_format.h" // 包含 BlobIndex
#include "blob/blob_file.h"   // 包含 BlobWriter (之前可能漏了这个)
#include "util/env.h"         // 包含 RandomAccessFile

namespace titankv {

class BlobStore {
public:
    explicit BlobStore(std::string db_path);
    
    Status Add(const Slice& key, const Slice& value, BlobIndex* index);
    
    // 【新增】读取接口
    Status Get(const BlobIndex& index, std::string* value);

private:
    std::string db_path_;
    uint32_t next_file_id_;
    std::mutex mutex_;
    
    // 确保 BlobWriter 在这里是可见的
    std::unique_ptr<BlobWriter> active_writer_;
    
    // 确保 RandomAccessFile 在这里是可见的
    std::map<uint32_t, std::unique_ptr<RandomAccessFile>> open_files_;

    Status CreateNewBlobFile();
    Status GetFile(uint32_t file_id, RandomAccessFile** file);
};

} // namespace titankv