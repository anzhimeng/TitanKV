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
    std::filesystem::create_directories(db_path_);
}

Status BlobStore::CreateNewBlobFile() {
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

} // namespace titankv