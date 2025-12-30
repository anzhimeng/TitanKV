#include "lsm/table_builder.h"
#include "lsm/block_builder.h"
#include "lsm/table_format.h"
#include "util/coding.h"
#include "util/crc32c.h" // 需要计算 Block 的 CRC，Week 1 已有

namespace titankv {

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : options_(options),
      file_(file),
      offset_(0),
      data_block_(new BlockBuilder(&options)),
      index_block_(new BlockBuilder(&options)),
      num_entries_(0),
      closed_(false),
      pending_index_entry_(false),
      pending_handle_(new BlockHandle()) {
    if (options.filter_policy) {
        filter_block_ = new FilterBlockBuilder(options.filter_policy.get());
    } else {
        filter_block_ = nullptr;
    }
}

TableBuilder::~TableBuilder() {
  assert(closed_); // 必须先调用 Finish 或 Abandon
  delete data_block_;
  delete index_block_;
  delete pending_handle_;
  delete filter_block_;
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  assert(!closed_);
  if (!ok()) return;

  // 1. 如果之前 Flush 过一个 Block，现在需要往 Index Block 里记一笔
  // 索引格式: Key = LastKeyOfPrevBlock, Value = BlockHandle
  if (pending_index_entry_) {
    // 这里的 Key 应该是 "大于等于 PrevBlock.LastKey" 且 "小于 CurrentBlock.FirstKey" 的最短串
    // 也就是 "Separator"。
    // 但为了 Day 2 简单，直接用 PrevBlock.LastKey 也可以，是正确的。

        // key 是 InternalKey，需要提取 UserKey
    assert(last_key_.size() >= 8);
    Slice last_user_key(last_key_.data(), last_key_.size() - 8);
    
    std::string handle_encoding;
    pending_handle_->EncodeTo(&handle_encoding);
    
    index_block_->Add(last_user_key, Slice(handle_encoding));
    pending_index_entry_ = false;
  }

  // 【新增】添加 User Key 到过滤器
  if (filter_block_) {
      // key 是 InternalKey，需要提取 User Key
      assert(key.size() >= 8);
      Slice user_key(key.data(), key.size() - 8);
      filter_block_->AddKey(user_key);
  }

  // 2. 写入当前的 Data Block
  // 这里的 Key 实际上应该是 Internal Key
  last_key_.assign(key.data(), key.size());
  data_block_->Add(key, value);
  num_entries_++;

  // 3. 检查 Block 是否写满
  const size_t estimated_block_size = data_block_->CurrentSizeEstimate();
  if (estimated_block_size >= options_.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  assert(!closed_);
  if (!ok()) return;
  if (data_block_->empty()) return;

  // 1. 写入 Data Block
  WriteBlock(data_block_, pending_handle_);
  
  if (ok()) {
    // 2. 标记需要写索引
    // 我们不能现在立马写索引，因为这会导致 Index Block 的 Key 是 Data Block 的 Last Key
    // 虽然正确，但如果在 Add 的开头写，我们可以利用 "Separator" 优化（虽然我们这里简化了没做）
    // 主要是为了逻辑清晰：Block 落盘了，Handle 生成了，下一条 Add 负责把这个 Handle 记入索引
    pending_index_entry_ = true;
    
    // 3. 刷盘 (可选，OS Cache 足够，但在 Flush 时最好确保数据下去)
    status_ = file_->Flush();
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // 1. 完成构建
  Slice raw_block = block->Finish();

  // 2. 压缩 (Week 2 暂不实现压缩，直接存 Raw Data)
  // Slice block_content = raw_block;
  // CompressionType type = kNoCompression;

  // 3. 写入文件
  // 实际存储格式：[Data] [Type(1B)] [CRC(4B)]
  // 这样读取时可以校验完整性
  
  handle->set_offset(offset_);
  handle->set_size(raw_block.size()); // 这里仅记录 Data 大小，不含 Trailer

  Status s = file_->Append(raw_block);
  if (s.ok()) {
    // 写入 Trailer (Type + CRC)
    char trailer[5];
    trailer[0] = 0; // kNoCompression
    
    // 计算 CRC (数据 + Type)
    uint32_t crc = crc32c::Value(raw_block.data(), raw_block.size());
    crc = crc32c::Extend(crc, trailer, 1); // Extend Type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    
    s = file_->Append(Slice(trailer, 5));
    if (s.ok()) {
      // 偏移量增加：Data + Trailer
      offset_ += raw_block.size() + 5; 
    }
  }
  
  status_ = s;
  block->Reset(); // 重置 Builder 准备下一个 Block
}

Status TableBuilder::Finish() {
  // 1. 刷入最后一个 Data Block
  Flush();
  assert(!closed_);
  closed_ = true;

  BlockHandle index_block_handle;
  BlockHandle metaindex_block_handle; // 暂时为空

  // 【新增】写入 Filter Block (如果有)
  BlockHandle filter_block_handle;
  if (ok() && filter_block_) {
     Slice filter_content = filter_block_->Finish();
     // 作为一个 Raw Block 写入（不加 BlockBuilder 包装）
     // 复用 WriteBlock 的逻辑不太合适，因为 WriteBlock 封装了 BlockBuilder
     // 我们直接 Append
     filter_block_handle.set_offset(offset_);
     filter_block_handle.set_size(filter_content.size());
   
     status_ = file_->Append(filter_content);
     if (status_.ok()) {
         offset_ += filter_content.size();
     }
  }
    

  // 2. 写入 Index Block
  // 如果有 pending 的索引项（最后一个 Block），先写进去
  if (ok() && pending_index_entry_) {
    std::string handle_encoding;
    pending_handle_->EncodeTo(&handle_encoding);
    index_block_->Add(last_key_, Slice(handle_encoding));
    pending_index_entry_ = false;
  }
  
  if (ok()) {
    WriteBlock(index_block_, &index_block_handle);
  }

  // 3. 写入 Footer
  if (ok()) {
    Footer footer;
    footer.set_index_handle(index_block_handle);
    // footer.set_metaindex_handle(...) 

    // 复用 metaindex_handle 字段存储 filter handle
    if (filter_block_) {
        // footer.set_metaindex_handle(filter_block_handle);
        Slice content = filter_block_->Finish();
        // 手动构造 Trailer
        char trailer[5];
        trailer[0] = 0; // NoCompression
        uint32_t crc = crc32c::Value(content.data(), content.size());
        crc = crc32c::Extend(crc, trailer, 1);
        EncodeFixed32(trailer + 1, crc32c::Mask(crc));
        
        filter_block_handle.set_offset(offset_);
        filter_block_handle.set_size(content.size());
        
        file_->Append(content);
        file_->Append(Slice(trailer, 5));
        offset_ += content.size() + 5;
    }
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    status_ = file_->Append(footer_encoding);
    if (status_.ok()) {
      offset_ += footer_encoding.size();
    }
  }
  return status_;
}

void TableBuilder::Abandon() {
  assert(!closed_);
  closed_ = true;
}

uint64_t TableBuilder::NumEntries() const { return num_entries_; }

uint64_t TableBuilder::FileSize() const { return offset_; }

} // namespace titankv