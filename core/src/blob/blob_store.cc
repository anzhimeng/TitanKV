#include <filesystem> // C++17
#include "blob/blob_store.h"
#include "util/crc32c.h"
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
  std::lock_guard<std::mutex> lock(mutex_);

  RandomAccessFile* file;
  Status s = GetFile(index.file_id, &file);
  if (!s.ok()) return s;

  // 1. 读取 Header (12 字节)
  char header_buf[BlobRecordHeader::kHeaderSize];
  Slice header_slice;
  s = file->Read(index.offset, BlobRecordHeader::kHeaderSize, &header_slice, header_buf);
  if (!s.ok()) return s;

  BlobRecordHeader header;
  s = header.DecodeFrom(&header_slice);
  if (!s.ok()) return s;

  // 2. 读取 Key 和 Value
  // 为了校验 CRC，我们必须读取 Key，虽然用户只请求 Value。
  // 优化：我们可以一次性读取 Key + Value，减少一次系统调用 (pread)。
  
  size_t total_payload_size = header.key_size + header.size;
  std::string buffer; // 临时缓冲区，用于存放 Key + Value
  buffer.resize(total_payload_size);
  
  Slice payload_slice;
  uint64_t payload_offset = index.offset + BlobRecordHeader::kHeaderSize;
  
  s = file->Read(payload_offset, total_payload_size, &payload_slice, &buffer[0]);
  if (!s.ok()) return s;

  // 3. 校验 CRC
  // 校验范围：Header的长度字段(后8字节) + Key + Value
  // header_buf[0..3] 是 CRC，header_buf[4..11] 是 Size 和 KeySize
  
  uint32_t calc_crc = crc32c::Value(header_buf + 4, 8); // 计算 Header 后半部分
  calc_crc = crc32c::Extend(calc_crc, payload_slice.data(), payload_slice.size()); // 加上 Key + Value
  calc_crc = crc32c::Mask(calc_crc); // Mask

  if (calc_crc != header.crc) {
      return Status::Corruption("Blob checksum mismatch");
  }

  // 4. 提取 Value 返回给用户
  // payload_slice 包含 Key + Value
  // Value 位于 Key 之后
  if (payload_slice.size() != total_payload_size) {
      return Status::Corruption("Read incomplete blob data");
  }
  
  // 从 buffer 中截取 Value 部分
  // buffer: [Key...][Value...]
  value->assign(payload_slice.data() + header.key_size, header.size);

  return Status::OK();
}

}