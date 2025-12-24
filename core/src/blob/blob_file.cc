#include "blob/blob_file.h"
#include "blob/blob_format.h" // 必须包含这个
#include "util/coding.h"
#include "util/crc32c.h"
#include "titankv/status.h"

namespace titankv {

BlobWriter::BlobWriter(std::unique_ptr<WritableFile> file)
    : file_(std::move(file)), file_size_(0) {} // 注意成员变量名匹配 header

Status BlobWriter::AddRecord(const Slice& key, const Slice& value) {
  // 1. 使用 BlobRecordHeader 结构体，避免手动计算偏移量出错
  BlobRecordHeader header;
  header.key_size = static_cast<uint32_t>(key.size());
  header.size = static_cast<uint32_t>(value.size()); // Value Size
  header.crc = 0; // 暂时先填0，稍后计算

  // 2. 序列化 Header (EncodeTo 会自动处理 Offset 4/8 的正确顺序)
  char buf[BlobRecordHeader::kHeaderSize];
  header.EncodeTo(buf);

  // 3. 计算 CRC (Type + KeySize + ValSize + Key + Value)
  // 注意：BlobRecordHeader::EncodeTo 已经把长度写进 buf 了
  // 从 buf[4] 开始计算 CRC (跳过前4字节的CRC占位符)
  // 校验范围：Header的后8字节(长度信息) + Key + Value
  uint32_t crc = crc32c::Value(buf + 4, 8); 
  crc = crc32c::Extend(crc, key.data(), key.size());
  crc = crc32c::Extend(crc, value.data(), value.size());
  
  // Mask 并回填到 buf 的前 4 字节
  crc = crc32c::Mask(crc);
  EncodeFixed32(buf, crc);

  // 4. 写入 Header
  Status s = file_->Append(Slice(buf, BlobRecordHeader::kHeaderSize));
  if (!s.ok()) return s;

  // 5. 写入 Key
  s = file_->Append(key);
  if (!s.ok()) return s;

  // 6. 写入 Value
  s = file_->Append(value);
  if (!s.ok()) return s;

  // 7. 刷新到 OS Cache (重要)
  s = file_->Flush();
  if (!s.ok()) return s;

  // 8. 更新文件大小
  file_size_ += BlobRecordHeader::kHeaderSize + key.size() + value.size();

  return Status::OK();
}

} // namespace titankv