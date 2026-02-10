#pragma once
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <vector>
#include <string>
#include <functional>
#include "titankv/status.h"
#include "titankv/slice.h"
#include "titankv/options.h"
#include "blob/blob_format.h"
#include "blob/blob_file.h"
#include "util/env.h"

namespace titankv {

// 统计单个 Blob 文件的状态
struct BlobFileMeta {
    uint32_t file_number;
    uint64_t file_size;     // 文件总大小 (不可变)
    uint64_t garbage_size;  // 垃圾大小 (累加)

    BlobFileMeta(uint32_t num, uint64_t size) 
        : file_number(num), file_size(size), garbage_size(0) {}

    // 计算有效率 (0.0 ~ 1.0)
    double GetValidRatio() const {
        if (file_size == 0) return 0.0;
        if (garbage_size >= file_size) return 0.0;
        return static_cast<double>(file_size - garbage_size) / file_size;
    }
};

// GC 产生的待更新索引
struct GCRecord {
    std::string key;
    BlobIndex old_index; // 【新增】用于 CAS 检查
    BlobIndex new_index; // 搬运后的新位置
};


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
    // 【Day 1 新增】通知某个 Blob 对象变成了垃圾
    void NotifyGarbage(uint32_t file_id, uint64_t size);
    
    // 【Day 1 新增】获取某个文件的有效率 (调试/GC用)
    double GetValidRatio(uint32_t file_id) const;
    // 【Day 2 新增】执行 GC
    // is_valid_cb: 回调函数，用于询问 LSM "这个 BlobIndex 是否还活着"
    // out_new_indexes: 输出参数，返回所有搬运后的新索引
    Status RunGC(std::function<bool(const Slice& key, const BlobIndex& old_index)> is_valid_cb,
                 std::vector<GCRecord>* out_new_indexes);
    // 【新增】动态设置 GC 阈值
    void SetGCThreshold(double threshold);

private:
    std::string db_path_;
    const Options options_;
    uint32_t next_file_id_;
    std::mutex mutex_;
    
    std::unique_ptr<BlobWriter> active_writer_;
    std::map<uint32_t, std::unique_ptr<RandomAccessFile>> open_files_;


    // 【新增】持有指针
    IoUringExecutor* executor_;

    // 【Day 1 新增】文件元数据缓存: FileID -> Meta
    // 使用 shared_ptr 方便后续 GC 线程访问
    std::map<uint32_t, std::shared_ptr<BlobFileMeta>> files_stats_;
    std::set<uint32_t> obsolete_files_;

    // 【新增】原子变量，默认 0.5
    std::atomic<double> gc_threshold_{0.5};
    
    // 辅助：注册新文件
    void RegisterNewFile(uint32_t file_id, uint64_t size);

    // 辅助：挑选一个 Victim 文件
    bool PickGCVictim(uint32_t* file_number);

    Status CreateNewBlobFile();
    Status GetFile(uint32_t file_id, RandomAccessFile** file);
};

} // namespace titankv
