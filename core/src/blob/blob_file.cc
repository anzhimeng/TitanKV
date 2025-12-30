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

// --- BlobFileIterator 实现 ---

void BlobFileIterator::Next() {
  record_offset_ = current_offset_;
  valid_ = false;
  if (current_offset_ >= file_size_) {
    return; // EOF
  }

  // 1. 读取 Header
  char header_buf[BlobRecordHeader::kHeaderSize];
  Slice header_slice;
  status_ = file_->Read(current_offset_, BlobRecordHeader::kHeaderSize, &header_slice, header_buf);
  if (!status_.ok()) return;
  
  // 处理截断或损坏的文件
  if (header_slice.size() < BlobRecordHeader::kHeaderSize) {
      status_ = Status::Corruption("Truncated blob header");
      return;
  }

  BlobRecordHeader header;
  status_ = header.DecodeFrom(&header_slice);
  if (!status_.ok()) return;

  // 2. 读取 Key + Value
  size_t payload_size = header.key_size + header.size;
  buffer_.resize(payload_size);
  
  Slice payload_slice;
  status_ = file_->Read(current_offset_ + BlobRecordHeader::kHeaderSize, payload_size, &payload_slice, &buffer_[0]);
  if (!status_.ok()) return;
  
  if (payload_slice.size() != payload_size) {
      status_ = Status::Corruption("Truncated blob payload");
      return;
  }

  // 3. 【新增】校验 CRC
  // =========================================================
  // 3.1 重构长度部分 (必须与 Writer 顺序一致：ValueSize 然后 KeySize)
  char len_buf[8];
  EncodeFixed32(len_buf, header.size);      // Offset 0 (对应 Writer 的 buf+4)
  EncodeFixed32(len_buf + 4, header.key_size); // Offset 4 (对应 Writer 的 buf+8)

  // 3.2 计算 CRC
  uint32_t actual_crc = crc32c::Value(len_buf, 8);
  actual_crc = crc32c::Extend(actual_crc, buffer_.data(), buffer_.size());

  // 3.3 比较 (Header 里的 CRC 是 Mask 过的，需要 Unmask)
  if (crc32c::Unmask(header.crc) != actual_crc) {
      status_ = Status::Corruption("Blob CRC mismatch");
      // 遇到 CRC 错误，通常应该停止迭代，或者标记当前无效但继续尝试下一个（取决于策略）
      // 这里我们停止并报错
      return;
  }

  // 4. 填充 current_record_
  current_record_.header = header;
  current_record_.key = Slice(buffer_.data(), header.key_size);
  current_record_.value = Slice(buffer_.data() + header.key_size, header.size);


  current_offset_ += BlobRecordHeader::kHeaderSize + payload_size;
  valid_ = true;
  
  // 更新 offset 准备下一次读取
  // 注意：我们在这里不改变 current_offset_，而是记录下当前的，等下次 Next 再加？
  // 不，Next() 的语义是移动到下一个。所以我们需要记录当前 record 的起始位置供 GetBlobIndex 使用
  // 因此，我们需要一个 member 记录 record_start_offset
}

BlobIndex BlobFileIterator::GetBlobIndex() const {
    BlobIndex idx;
    idx.file_id = file_number_;
    // 注意：这里需要 careful。Next() 执行完后，current_offset_ 应该指向下一个 record
    // 所以当前的 offset 应该是 current_offset_ - total_record_size
    // 让我们重构一下 offset 管理：
    // current_offset_ 始终指向“下一个待读取的位置”
    // record_offset_ 指向“当前 Valid 的 record 的位置”
    
    idx.offset = record_offset_;
    idx.size = BlobRecordHeader::kHeaderSize + current_record_.header.key_size + current_record_.header.size;
    return idx;
}

} // namespace titankv