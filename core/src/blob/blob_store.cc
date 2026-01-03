#include "blob/blob_store.h"
#include "util/io_uring_executor.h"
#include "util/aligned_buffer.h"
#include "util/crc32c.h"
#include "util/coding.h" // 【新增】用于 EncodeFixed32
#include <filesystem>
#include <cstdio>
#include <future>

namespace titankv {

// 构造函数
BlobStore::BlobStore(std::string db_path, const Options& options, IoUringExecutor* executor)
    : db_path_(std::move(db_path)), 
      options_(options), 
      next_file_id_(1),     
      executor_(executor) {
    
    // 1. 确保目录存在
    std::filesystem::create_directories(db_path_);

    // 2. 【关键修复】扫描目录，恢复 next_file_id_
    // 防止重启后覆盖旧的 Blob 文件
    if (std::filesystem::exists(db_path_)) {
        for (const auto& entry : std::filesystem::directory_iterator(db_path_)) {
            if (!entry.is_regular_file()) continue;
            
            std::string fname = entry.path().filename().string();
            // 文件名格式: 000001.blob
            if (fname.length() > 5 && fname.substr(fname.length() - 5) == ".blob") {
                try {
                    // 提取数字部分
                    uint32_t id = std::stoul(fname.substr(0, fname.length() - 5));
                    if (id >= next_file_id_) {
                        next_file_id_ = id + 1;
                    }
                } catch (...) {
                    // 忽略格式不对的文件
                }
            }
        }
    }
    
    fprintf(stderr, "[BlobStore] Recovered. NextFileID: %u\n", next_file_id_);
}

Status BlobStore::CreateNewBlobFile() {
  // 【新增】在创建新文件前，保存旧文件的 Meta
  if (active_writer_ != nullptr) {
      uint32_t old_id = next_file_id_ - 1;
      uint64_t old_size = active_writer_->FileSize();
      RegisterNewFile(old_id, old_size);
      
      // 记得 Sync，确保大小正确落盘
      // active_writer_->Sync(); 
  }

  char file_id_str[30]; 
  snprintf(file_id_str, sizeof(file_id_str), "%06u.blob", next_file_id_);
  
  std::filesystem::path p = std::filesystem::path(db_path_) / file_id_str;
  std::string filename = p.string();

  std::unique_ptr<WritableFile> file;
  Status s = NewWritableFile(filename, &file);
  if (!s.ok()) {
    return s;
  }

  active_writer_ = std::make_unique<BlobWriter>(std::move(file));
  next_file_id_++;
  return Status::OK();
}

Status BlobStore::Add(const Slice& key, const Slice& value, BlobIndex* index) {
  std::lock_guard<std::mutex> lock(mutex_); 

  if (active_writer_ == nullptr || active_writer_->FileSize() > options_.max_blob_file_size) {
    Status s = CreateNewBlobFile();
    if (!s.ok()) return s;
  }

  uint64_t offset = active_writer_->FileSize();
  Status s = active_writer_->AddRecord(key, value);
  if (!s.ok()) return s;

  index->file_id = next_file_id_ - 1; 
  index->offset = offset;
  index->size = BlobRecordHeader::kHeaderSize + key.size() + value.size();

  return Status::OK();
}

Status BlobStore::GetFile(uint32_t file_id, RandomAccessFile** file) {
    auto it = open_files_.find(file_id);
    if (it != open_files_.end()) {
        *file = it->second.get();
        return Status::OK();
    }

    char file_id_str[30];
    snprintf(file_id_str, sizeof(file_id_str), "%06u.blob", file_id);
    std::string filename = std::filesystem::path(db_path_) / file_id_str;

    std::unique_ptr<RandomAccessFile> new_file;
    // 使用 options_.use_direct_io
    Status s = NewRandomAccessFile(filename, &new_file, options_.use_direct_io);
    if (!s.ok()) return s;

    *file = new_file.get();
    open_files_[file_id] = std::move(new_file);
    return Status::OK();
}

Status BlobStore::Get(const BlobIndex& index, std::string* value) {
    std::lock_guard<std::mutex> lock(mutex_);

    RandomAccessFile* file;
    Status s = GetFile(index.file_id, &file);
    if (!s.ok()) return s;

    // --- Direct I/O 对齐逻辑 ---
    const size_t kAlign = 4096;
    uint64_t logic_offset = index.offset; 
    size_t logic_size = index.size; 

    uint64_t physical_offset = logic_offset & ~(kAlign - 1);
    size_t shift = logic_offset - physical_offset;
    size_t read_size = (shift + logic_size + kAlign - 1) & ~(kAlign - 1);
    
    AlignedBuffer aligned_buf(read_size, kAlign);

    int fd = file->UnsafeGetFD();
    
    if (executor_ && fd >= 0) {
        std::promise<int> promise;
        auto future = promise.get_future();

        executor_->SubmitRead(fd, physical_offset, read_size, aligned_buf.data, 
            [&promise](int res) { promise.set_value(res); }
        );

        int bytes_read = future.get();
        if (bytes_read < 0) return Status::IOError("uring read failed");
    } else {
        Slice s_dummy;
        s = file->Read(physical_offset, read_size, &s_dummy, aligned_buf.data);
        if (!s.ok()) return s;
    }

    const char* record_ptr = aligned_buf.data + shift;
    Slice input(record_ptr, logic_size);
    BlobRecordHeader header;
    s = header.DecodeFrom(&input);
    if (!s.ok()) return s;

    // =========================================================
    // 【新增】CRC 校验逻辑
    // =========================================================
    // 1. 重构 Header 的长度部分 (Size + KeySize) 用于 CRC 计算
    // BlobWriter 是对 buf+4 (即后8字节) 开始计算的 CRC
    char len_buf[8];
    EncodeFixed32(len_buf, header.size);
    EncodeFixed32(len_buf + 4, header.key_size);

    uint32_t calc_crc = crc32c::Value(len_buf, 8);

    // 2. 加上 Key 和 Value
    // input 目前指向 Key，长度是 key_size + size
    const char* payload_ptr = input.data(); 
    size_t payload_len = header.key_size + header.size;
    
    // 边界检查，防止 Buffer 越界读取
    if (payload_ptr + payload_len > aligned_buf.data + read_size) {
         return Status::Corruption("Blob record overflow");
    }

    calc_crc = crc32c::Extend(calc_crc, payload_ptr, payload_len);
    
    // 3. Mask 并比较 (或者 Unmask header.crc 进行比较)
    // 这里采用 Unmask stored CRC 的方式，因为 crc32c::Mask 可能会变
    if (crc32c::Unmask(header.crc) != calc_crc) {
        return Status::Corruption("Blob checksum mismatch");
    }
    // =========================================================

    const char* value_ptr = input.data() + header.key_size;
    value->assign(value_ptr, header.size);
    return Status::OK();
}


// MultiGet 暂时保持原样，或者你需要同样适配 Direct IO 逻辑
// Day 4 重点是单点 Get 的 Direct IO
void BlobStore::MultiGet(const std::vector<BlobIndex>& indices, 
                         std::vector<std::string>* values, 
                         std::vector<Status>* statuses) {
    // ... 
    // 注意：MultiGet 如果要支持 Direct IO，也需要对每个请求做 Alignment Adjustment
    // 逻辑会比较复杂。Day 3 的实现是基于 Header 同步读的。
    // 如果开启了 use_direct_io，Day 3 的 MultiGet 里的 Header Read 会报错 (因为没对齐)
    // 简单起见：如果 use_direct_io 为 true，MultiGet 可以循环调用 Get (退化)
    
    if (options_.use_direct_io) {
        size_t n = indices.size();
        values->resize(n);
        statuses->resize(n);
        for(size_t i=0; i<n; ++i) {
            (*statuses)[i] = Get(indices[i], &(*values)[i]);
        }
        return;
    }
    
    // ... 原有的 MultiGet 实现 ...
    size_t n = indices.size();
    values->resize(n);
    statuses->resize(n);
    for(size_t i=0; i<n; ++i) {
        (*statuses)[i] = Get(indices[i], &(*values)[i]);
    }
}

void BlobStore::RegisterNewFile(uint32_t file_id, uint64_t size) {
    auto meta = std::make_shared<BlobFileMeta>(file_id, size);
    files_stats_[file_id] = meta;
    fprintf(stderr, "[BlobStore] Registered File %u, Size %lu\n", file_id, size);
}

void BlobStore::NotifyGarbage(uint32_t file_id, uint64_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = files_stats_.find(file_id);
    if (it != files_stats_.end()) {
        it->second->garbage_size += size;
        
        // 简单的边界检查
        if (it->second->garbage_size > it->second->file_size) {
            it->second->garbage_size = it->second->file_size;
        }
        
        // 调试日志 (可选)
        // fprintf(stderr, "[GC-Stat] File %u: Garbage +%lu, Ratio now: %.2f\n", 
        //         file_id, size, it->second->GetValidRatio());
    } else {
        // 可能是重启后还没加载 Meta (Day 1 暂不处理持久化，只处理内存)
        // 或者文件已经被删除了
    }
}

double BlobStore::GetValidRatio(uint32_t file_id) const {
    // 注意：这里没加锁，生产环境建议加锁或使用原子变量
    auto it = files_stats_.find(file_id);
    if (it != files_stats_.end()) {
        return it->second->GetValidRatio();
    }
    return 0.0;
}

void BlobStore::SetGCThreshold(double threshold) {
    gc_threshold_.store(threshold);
    fprintf(stderr, "[BlobStore] GC Threshold updated to %.2f\n", threshold);
}

bool BlobStore::PickGCVictim(uint32_t* file_number) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    double min_ratio = 1.0;
    uint32_t victim = 0;
    bool found = false;

    // 【修改】读取原子变量
    double current_threshold = gc_threshold_.load();

    // 【新增】看看当前有多少个文件被监控
    // fprintf(stderr, "[PickGC] Stats map size: %lu. Active File ID: %u\n", 
    //        files_stats_.size(), next_file_id_ - 1);


    for (const auto& kv : files_stats_) {
        uint32_t fid = kv.first;
        // 跳过正在写的活跃文件！
        if (active_writer_ && next_file_id_ - 1 == fid) {
            continue; 
        }

        double ratio = kv.second->GetValidRatio();

        if (ratio <= current_threshold && ratio < min_ratio) {
            min_ratio = ratio;
            victim = fid;
            found = true;
        }
    }

    if (found) {
        *file_number = victim;
        return true;
    }
    return false;
}

Status BlobStore::RunGC(std::function<bool(const Slice&, const BlobIndex&)> is_valid_cb,
                        std::vector<GCRecord>* out_new_indexes) {
    uint32_t victim_file_id;
    if (!PickGCVictim(&victim_file_id)) {
        return Status::NotFound("No file needs GC");
    }

    fprintf(stderr, "[BlobGC] Picked file %u for GC.\n", victim_file_id);

    // 1. 打开 Victim 文件
    RandomAccessFile* file;
    Status s = GetFile(victim_file_id, &file);
    if (!s.ok()) return s;

    // 获取文件大小 (从 meta 获取，或者 file system)
    uint64_t file_size = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        file_size = files_stats_[victim_file_id]->file_size;
    }

    // 2. 遍历文件
    BlobFileIterator iter(file, victim_file_id, file_size);
    for (iter.Next(); iter.Valid(); iter.Next()) {
        Slice key = iter.key();
        BlobIndex old_index = iter.GetBlobIndex();

        // 3. 判活 (调用外部回调，查 LSM)
        if (is_valid_cb(key, old_index)) {
            // 4. 有效数据 -> 搬运
            BlobIndex new_index;
            // 直接调用 Add 写入当前活跃文件
            // 注意：Add 内部有锁，这里是安全的
            s = Add(key, iter.value(), &new_index);
            if (!s.ok()) return s;

            // 收集回填信息
            GCRecord record;
            record.key = key.ToString();
            record.old_index = old_index; // 【新增】赋值
            record.new_index = new_index;
            out_new_indexes->push_back(record);
        } else {
            // 垃圾数据 -> 丢弃，啥也不做
        }
    }

    // 5. 标记 Victim 文件待删除 (Day 2 先不物理删除，只是打印日志)
    fprintf(stderr, "[BlobGC] Rewrite done. %lu records moved.\n", out_new_indexes->size());
    // 【新增】清理元数据和物理文件
    {
        std::lock_guard<std::mutex> lock(mutex_);
        files_stats_.erase(victim_file_id); // 从统计中移除，防止下次再选
    }
    
    // 物理删除文件 (C++17)
    // 注意：在生产环境中，这里应该有一个 ObsoleteFile 机制，延迟删除，防止读请求并发访问。
    // 但在 Week 7 简化版中，只要我们确定没有 Iterator 正在读这个文件，就可以删。
    char file_id_str[30]; 
    snprintf(file_id_str, sizeof(file_id_str), "%06u.blob", victim_file_id);
    std::filesystem::path p = std::filesystem::path(db_path_) / file_id_str;
    
    std::error_code ec;
    std::filesystem::remove(p, ec); 
    if (ec) {
        fprintf(stderr, "[BlobGC] Failed to delete file %s: %s\n", p.c_str(), ec.message().c_str());
    } else {
        fprintf(stderr, "[BlobGC] Deleted file %u\n", victim_file_id);
    }
    
    return Status::OK();
}

} // namespace titankv