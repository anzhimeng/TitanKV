#include "wal/log_reader.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include <cstdio>

namespace titankv {
namespace log {

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {
}

Reader::~Reader() {
  delete[] backing_store_;
}

// 核心状态机：跳过初始块 -> 读取分片 -> 组装逻辑记录 -> 处理异常
bool Reader::ReadRecord(Slice* record, std::string* scratch) {
  // 如果是第一次读取且设置了 initial_offset，先跳过前面的垃圾数据
  if (last_record_offset_ < initial_offset_) {
    if (!SkipToInitialBlock()) {
      return false;
    }
  }

  scratch->clear();
  Slice fragment;
  bool in_fragmented_record = false;
  
  // 记录当前逻辑记录开始的物理偏移量
  uint64_t prospective_record_offset = 0;

  while (true) {
    // 读取下一个物理分片
    uint64_t physical_record_offset = end_of_buffer_offset_ - buffer_.size();
    unsigned int record_type = ReadPhysicalRecord(&fragment);

    // 计算当前记录在哪个位置，用于 resyncing 判断
    // uint64_t drop_size = 0;
    if (resyncing_) {
      if (record_type == kMiddleType) {
        // 如果我们正在 resync，且碰到了 Middle，说明这半截记录在 initial_offset 之前就开始了
        // 我们必须丢弃它，继续找 First/Full
        continue;
      } else if (record_type == kLastType) {
        // 同上，丢弃并结束 resync 状态，准备迎接新记录
        resyncing_ = false;
        continue;
      } else {
        // 遇到 Full 或 First，说明找到了一个新的合法起点
        resyncing_ = false;
      }
    }

    switch (record_type) {
      case kFullType:
        if (in_fragmented_record) {
          // 状态机错误：正在等 Middle/Last，却来了个 Full
          // 之前的 buffer 数据作废
          ReportCorruption(scratch->size(), "partial record without end(1)");
        }
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        *record = fragment; // 零拷贝：直接指向 backing_store_
        last_record_offset_ = prospective_record_offset;
        return true;

      case kFirstType:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "partial record without end(2)");
        }
        prospective_record_offset = physical_record_offset;
        // 必须拷贝到 scratch，因为后续可能还有 Middle/Last 需要拼接
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true;
        break;

      case kMiddleType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(), "missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(), "missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);
          last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;

      case kEof:
        if (in_fragmented_record) {
          // 文件在记录中间结束了
          scratch->clear();
          ReportCorruption(0, "partial record without end(3)");
        }
        return false;

      case kBadRecord:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        // 遇到坏记录，不返回 false，而是继续循环尝试读取下一个 Block
        break;

      default:
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            "unknown record type");
        in_fragmented_record = false;
        scratch->clear();
        break;
    }
  }
}

// 帮助函数：定位到 initial_offset 所在的 Block
bool Reader::SkipToInitialBlock() {
  // 计算 offset 在 Block 中的偏移
  size_t offset_in_block = initial_offset_ % kBlockSize;
  // 计算 Block 的起始位置
  uint64_t block_start_location = initial_offset_ - offset_in_block;

  // 如果起始位置 > 0，我们需要跳过前面的所有 Block
  if (block_start_location > 0) {
    Status s = file_->Skip(block_start_location);
    if (!s.ok()) {
      ReportDrop(block_start_location, s);
      return false;
    }
    end_of_buffer_offset_ = block_start_location;
  }
  return true;
}

// 核心 IO 逻辑
unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  while (true) {
    // 1. 如果 buffer 为空或不足以包含 Header，读取下一个 Block
    if (buffer_.size() < kHeaderSize) {
      if (!eof_) {
        // 上一个 Block 剩下的数据（Trailer），通常是 0 填充
        // 我们直接丢弃，开始读新 Block
        buffer_.clear();
        
        Status s = file_->Read(kBlockSize, &buffer_, backing_store_);
        end_of_buffer_offset_ += buffer_.size();
        
        if (!s.ok()) {
          buffer_.clear();
          ReportDrop(kBlockSize, s);
          eof_ = true;
          return kEof;
        } else if (buffer_.size() < kBlockSize) {
          eof_ = true; // 读到的不够 32KB，说明到文件尾了
        }
        continue; // 重新检查 buffer 大小
      } else {
        // 已经是 EOF 且 buffer 不够 Header，结束
        buffer_.clear();
        return kEof;
      }
    }

    // 2. 解析 Header
    const char* header = buffer_.data();
    // Little Endian 解析 Length (2 bytes)
    const uint32_t a = static_cast<uint32_t>(static_cast<unsigned char>(header[4]));
    const uint32_t b = static_cast<uint32_t>(static_cast<unsigned char>(header[5]));
    const uint32_t length = a | (b << 8);
    
    // Header[6] 是 Type
    const unsigned int type = header[6];

    // 3. 检查长度是否越界
    if (kHeaderSize + length > buffer_.size()) {
      size_t drop_size = buffer_.size();
      buffer_.clear();
      if (!eof_) {
        // 如果不是 EOF，说明 Block 里的数据格式坏了（Header 说长度很大，但 Block 没那么大）
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord; 
      }
      // 如果是 EOF，可能是写到一半断电，当作 EOF 处理
      return kEof;
    }

    // 4. 处理 Padding (kZeroType)
    if (type == kZeroType && length == 0) {
      // 这是一个填充 record，跳过整个 buffer 剩余部分
      buffer_.clear();
      return kBadRecord; // 返回 BadRecord 会让外层循环继续读下一个 Block
    }

    // 5. CRC 校验
    if (checksum_) {
      // 解析存储的 CRC
      uint32_t expected_crc = DecodeFixed32(header); // 存储的是 Mask 过的 CRC
      expected_crc = crc32c::Unmask(expected_crc);   // 还原

      // 计算实际 CRC: Cover Type(1B) + Payload(length)
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
      
      if (actual_crc != expected_crc) {
        // CRC 校验失败
        size_t drop_size = kHeaderSize + length;
        buffer_.remove_prefix(drop_size);
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }

    // 6. 成功解析
    // 移动 buffer 指针，跳过 Header
    buffer_.remove_prefix(kHeaderSize + length);
    
    // 跳过 initial_offset 之前的物理记录 (Strict Check)
    // 如果当前记录的结束位置在 initial_offset 之前，这仍然是旧数据
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length < initial_offset_) {
        result->clear();
        return kBadRecord; // 跳过此条，继续
    }

    // 返回 Payload
    *result = Slice(header + kHeaderSize, length);
    return type;
  }
}

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != nullptr && end_of_buffer_offset_ >= initial_offset_ + bytes) {
    reporter_->Corruption(bytes, reason);
  }
}

uint64_t Reader::LastRecordOffset() {
  return last_record_offset_;
}

} // namespace log
} // namespace titankv