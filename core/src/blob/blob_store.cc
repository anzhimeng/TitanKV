#include <filesystem> // C++17
#include "blob/blob_store.h"
#include <cstdio>

namespace titankv {

// 假设文件大小阈值为 64MB
const uint64_t kBlobFileSizeThreshold = 64 * 1024 * 1024; 

BlobStore::BlobStore(std::string db_path)
    : db_path_(std::move(db_path)), next_file_id_(1) {
  // 实际项目中，需要扫描 db_path_ 目录来恢复 next_file_id_
  // 但在 Week 1，我们可以从 1 开始
  std::filesystem::create_directories(db_path_);
}

// 辅助函数
Status BlobStore::CreateNewBlobFile() {
  char file_id_str[30];
  snprintf(file_id_str, sizeof(file_id_str), "%06u.blob", next_file_id_);
  std::string filename = std::filesystem::path(db_path_) / file_id_str;

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
  std::lock_guard<std::mutex> lock(mutex_); // 保护 active_writer_ 和 next_file_id_

  // 1. 检查是否需要滚动文件
  if (active_writer_ == nullptr || active_writer_->FileSize() > kBlobFileSizeThreshold) {
    Status s = CreateNewBlobFile();
    if (!s.ok()) {
      return s;
    }
  }

  // 2. 记录写入前的元数据
  uint64_t offset = active_writer_->FileSize();
  uint64_t record_size = kHeaderSize + key.size() + value.size();

  // 3. 调用 BlobWriter 写入数据
  Status s = active_writer_->AddRecord(key, value);
  if (!s.ok()) {
    return s;
  }

  // 4. 填充 BlobIndex 返回给调用者
  index->file_id = next_file_id_ - 1; // 因为 CreateNewBlobFile 里已经自增了
  index->offset = offset;
  index->size = record_size;

  return Status::OK();
}

Status BlobStore::GetFile(uint32_t file_id, RandomAccessFile** file) {
    // 1. 先查缓存
    auto it = open_files_.find(file_id);
    if (it != open_files_.end()) {
        *file = it->second.get();
        return Status::OK();
    }

    // 2. 缓存未命中，打开文件
    char file_id_str[30];
    snprintf(file_id_str, sizeof(file_id_str), "%06u.blob", file_id);
    std::string filename = std::filesystem::path(db_path_) / file_id_str;

    std::unique_ptr<RandomAccessFile> new_file;
    Status s = NewRandomAccessFile(filename, &new_file);
    if (!s.ok()) return s;

    *file = new_file.get();
    open_files_[file_id] = std::move(new_file);
    return Status::OK();
}

Status BlobStore::Get(const BlobIndex& index, std::string* value) {
    std::lock_guard<std::mutex> lock(mutex_); // 简单保护 open_files_ map

    RandomAccessFile* file;
    Status s = GetFile(index.file_id, &file);
    if (!s.ok()) return s;

    // Record 格式: [Header(12B)] [Key] [Value]
    // BlobIndex 指向 Record 的开头
    
    // 1. 读取 Header
    char header_buf[BlobRecordHeader::kHeaderSize];
    Slice header_slice;
    s = file->Read(index.offset, BlobRecordHeader::kHeaderSize, &header_slice, header_buf);
    if (!s.ok()) return s;

    BlobRecordHeader header;
    s = header.DecodeFrom(&header_slice);
    if (!s.ok()) return s;

    // 2. 计算 Value 的偏移量
    // Offset + HeaderSize + KeySize
    uint64_t value_offset = index.offset + BlobRecordHeader::kHeaderSize + header.key_size;
    
    // 3. 读取 Value
    // 这里的 buffer 分配是个优化点，现在直接 resize string
    value->resize(header.size);
    Slice value_slice;
    // string 的内存是连续的，可以直接写入 &(*value)[0]
    s = file->Read(value_offset, header.size, &value_slice, &(*value)[0]);
    
    if (!s.ok()) return s;
    
    fprintf(stderr, "[DEBUG] Get Blob: Header.size=%u, Header.key_size=%u, Actual KeyLen=%lu\n", 
        header.size, header.key_size, (unsigned long)index.size); // 注意 index.size 是总大小
    // TODO: 这里应该计算 CRC 并校验。
    // Week 1 暂时跳过 CRC 验证，只要读出来就行。

    return Status::OK();
}

}