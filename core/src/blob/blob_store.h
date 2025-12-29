#pragma once
#include <map>
#include <memory>
#include <mutex>
#include <vector>
#include <string>
#include "titankv/status.h"
#include "titankv/slice.h"
#include "titankv/options.h"
#include "blob/blob_format.h"
#include "blob/blob_file.h"
#include "util/env.h"

namespace titankv {

// 【关键修复】前置声明：告诉编译器 IoUringExecutor 是一个类
class IoUringExecutor; 

class BlobStore {
public:
    // 现在编译器知道 IoUringExecutor 是个类了，所以 IoUringExecutor* 是合法的
    explicit BlobStore(std::string db_path, const Options& options, IoUringExecutor* executor = nullptr);
    
    Status Add(const Slice& key, const Slice& value, BlobIndex* index);
    Status Get(const BlobIndex& index, std::string* value);
    // 【Day 3 新增】批量获取接口
    // indices: 输入的索引列表
    // values: 输出的值列表 (预先 resize 好)
    // statuses: 每个请求的结果
    void MultiGet(const std::vector<BlobIndex>& indices, 
                  std::vector<std::string>* values, 
                  std::vector<Status>* statuses);

private:
    std::string db_path_;
    const Options options_;
    uint32_t next_file_id_;
    std::mutex mutex_;
    
    std::unique_ptr<BlobWriter> active_writer_;
    std::map<uint32_t, std::unique_ptr<RandomAccessFile>> open_files_;

    // 【新增】持有指针
    IoUringExecutor* executor_;

    Status CreateNewBlobFile();
    Status GetFile(uint32_t file_id, RandomAccessFile** file);
};

} // namespace titankv