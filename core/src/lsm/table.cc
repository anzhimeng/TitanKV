#include "lsm/table.h"
#include "lsm/table_format.h"
#include "lsm/block.h"
#include "util/coding.h"
#include "util/crc32c.h" // 确保包含 CRC 校验库
#include "util/cache.h" // 【新增】

namespace titankv {

// 当 Cache 驱逐一个 Block 时，自动调用此函数释放内存
static void DeleteCachedBlock(const Slice& key, void* value) {
  (void)key;
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

// Table::Rep 结构体
struct Table::Rep {
  Options options;
  Status status;
  // 【修复】RandomAccessFile 的所有权由 Rep 管理
  RandomAccessFile* file; 
  Block* index_block;

  // 【新增】记录文件号，用于生成 Cache Key
  uint64_t file_number; 

  Rep() : file(nullptr), index_block(nullptr) {} // 构造函数初始化指针

  ~Rep() {
    delete index_block;
    delete file; // 【修复】在 Rep 析构时释放 file
  }
};

Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t file_number, uint64_t file_size, Table** table) {
  *table = nullptr;
  
  // 使用 unique_ptr 接管 file，确保出错时自动 delete
  // 成功时 release() 给 Rep
  std::unique_ptr<RandomAccessFile> file_guard(file);

  if (file_size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(file_size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  BlockContents index_block_contents;
  // 这里的 file 还是 file_guard.get()
  s = ReadBlock(file, ReadOptions(), footer.index_handle(), &index_block_contents);
  if (!s.ok()) return s;

  Block* index_block = new Block(index_block_contents);
  
  Rep* rep = new Table::Rep;
  rep->options = options;
  rep->file = file_guard.release(); // 【关键】转移所有权给 Rep
  rep->index_block = index_block;
  rep->file_number = file_number; // 【新增】赋值
  
  *table = new Table(rep);
  return Status::OK();
}

Table::~Table() { delete rep_; }

// 读取 Block 的通用逻辑
Status Table::ReadBlock(RandomAccessFile* file, const ReadOptions& options,
                        const BlockHandle& handle, BlockContents* result) {
                        
  (void)options; 
  result->data = Slice();
  result->heap_allocated = false;

  size_t n = static_cast<size_t>(handle.size());
  // 【修复】为 Trailer (Type + CRC) 分配足够的空间，Trailer 是 5 字节
  // 类型 1B，CRC 4B
  char* buf = new char[n + 5]; 
  Slice contents; // 用于接收读取到的原始数据 (Data + Trailer)
  
  // 读 Data + Trailer
  Status s = file->Read(handle.offset(), n + 5, &contents, buf);
  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  // 【修复】如果实际读取的字节数小于期望，说明文件损坏或到末尾了
  if (contents.size() < n + 5) {
    delete[] buf;
    return Status::Corruption("Block data too short, incomplete trailer");
  }

  // 【修复】CRC 校验 (Week 2 必须完成)
  // Trailer 的最后 5 字节是 Type 和 CRC
  const char* trailer_ptr = contents.data() + n;
  char compression_type = trailer_ptr[0]; // Type (1 byte)
  uint32_t stored_crc = DecodeFixed32(trailer_ptr + 1); // CRC (4 bytes)
  stored_crc = crc32c::Unmask(stored_crc); // 解码 Masked CRC

  uint32_t actual_crc = crc32c::Value(contents.data(), n); // 计算 Data 部分 CRC
  actual_crc = crc32c::Extend(actual_crc, trailer_ptr, 1); // 扩展 Type 部分 CRC
  
  if (actual_crc != stored_crc) {
    delete[] buf;
    return Status::Corruption("Block checksum mismatch");
  }

  // 【修复】如果压缩了，需要解压 (Week 2 暂不实现压缩，假设 Type == 0)
  if (compression_type != 0) { // 0 代表无压缩 (kNoCompression)
    delete[] buf;
    return Status::Corruption("Block compression not supported yet");
  }
  
  result->data = Slice(buf, n); // 只返回 Data 部分
  result->heap_allocated = true; // 告知 Block 析构时释放 buf
  return Status::OK();
}

// 核心查找逻辑
Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&, const Slice&)) {
  UserKeyComparator user_cmp;
  Iterator* iiter = rep_->index_block->NewIterator(&user_cmp);
  
  assert(k.size() >= 8);
  Slice target_user_key(k.data(), k.size() - 8);

  iiter->Seek(target_user_key);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    BlockHandle handle;
    if (handle.DecodeFrom(&handle_value).ok()) {
      
      Block* block = nullptr;
      Cache::Handle* cache_handle = nullptr;

      // =========================================================
      // 【Block Cache 逻辑开始】
      // =========================================================
      Cache* block_cache = rep_->options.block_cache.get();
      
      if (block_cache != nullptr) {
          // 1. 构造 Cache Key (8字节 FileNum + 8字节 Offset)
          char cache_key_buffer[16];
          EncodeFixed64(cache_key_buffer, rep_->file_number);
          EncodeFixed64(cache_key_buffer + 8, handle.offset());
          Slice cache_key(cache_key_buffer, sizeof(cache_key_buffer));

          // 2. 查缓存
          cache_handle = block_cache->Lookup(cache_key);
          
          if (cache_handle != nullptr) {
              // 3. 命中！直接获取 Block 指针
              block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
          } else {
              // 4. 未命中，读磁盘
              BlockContents contents;
              if (ReadBlock(rep_->file, options, handle, &contents).ok()) {
                  block = new Block(contents);
                  // 5. 插入缓存
                  // charge 使用 contents.data.size() 近似
                  cache_handle = block_cache->Insert(cache_key, block, contents.data.size(), &DeleteCachedBlock);
              }
          }
      } else {
          // 无缓存模式 (旧逻辑)
          BlockContents contents;
          if (ReadBlock(rep_->file, options, handle, &contents).ok()) {
              block = new Block(contents);
          }
      }
      // =========================================================

      if (block != nullptr) {
        UserKeyComparator data_user_cmp;
        Iterator* diter = block->NewIterator(&data_user_cmp);
        
        diter->Seek(target_user_key);
        if (diter->Valid()) {
            Slice found_key = diter->key();
            if (found_key.size() >= 8) { 
                Slice found_user_key(found_key.data(), found_key.size() - 8);
                if (user_cmp.Compare(found_user_key, target_user_key) == 0) {
                     handle_result(arg, found_key, diter->value());
                }
            }
        }
        delete diter;
        
        // 【关键】清理资源
        if (cache_handle != nullptr) {
            // 如果是从 Cache 拿的，释放 Handle (引用计数 -1)
            // Block 本身不会被 delete，直到被 Cache 驱逐
            block_cache->Release(cache_handle);
        } else {
            // 如果没走 Cache，手动 delete Block
            delete block;
        }
      }
    }
  }
  delete iiter;
  return Status::OK();
}

} // namespace titankv