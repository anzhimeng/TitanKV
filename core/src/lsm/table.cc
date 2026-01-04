#include "lsm/table.h"
#include "lsm/table_format.h"
#include "lsm/block.h"
#include "lsm/two_level_iterator.h" // 确保包含这个
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/cache.h"

namespace titankv {

// 定义回调
static void DeleteCachedBlock(const Slice& key, void* value) {
  (void)key;
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

// 1. 定义清理回调函数 (放在匿名空间或 static)

static void DeleteBlock(void* arg, void* ignored) {
  (void)ignored;
  delete reinterpret_cast<Block*>(arg);
}

static void ReleaseCacheHandle(void* cache_arg, void* handle_arg) {
  Cache* cache = reinterpret_cast<Cache*>(cache_arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(handle_arg);
  cache->Release(handle);
}

struct Table::Rep {
  Options options;
  Status status;
  RandomAccessFile* file;
  Block* index_block;
  uint64_t file_number;
  
  // 【新增】持有 Comparator，供所有 Iterator 使用
  UserKeyComparator user_cmp;

  // Filter 相关
  Block* filter_data_block = nullptr;
  FilterBlockReader* filter = nullptr;

  Rep() : file(nullptr), index_block(nullptr), file_number(0) {}

  ~Rep() {
    delete index_block;
    delete filter_data_block;
    delete filter;
    delete file;
  }
};

Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t file_number, uint64_t file_size, Table** table) {
  *table = nullptr;
  // 【新增】空指针检查
  if (file == nullptr) {
      return Status::InvalidArgument("Table::Open called with null file pointer");
  }
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
  s = ReadBlock(file, ReadOptions(), footer.index_handle(), &index_block_contents);
  if (!s.ok()) return s;

  Block* index_block = new Block(index_block_contents);

  // 读取 Filter
  Block* filter_data_block = nullptr;
  FilterBlockReader* filter_reader = nullptr;
  if (options.filter_policy && footer.metaindex_handle().size() > 0) {
      BlockContents contents;
      if (ReadBlock(file, ReadOptions(), footer.metaindex_handle(), &contents).ok()) {
          filter_data_block = new Block(contents);
          Slice filter_content(filter_data_block->data(), filter_data_block->size());
          filter_reader = new FilterBlockReader(options.filter_policy.get(), filter_content);
      }
  }
  
  Rep* rep = new Table::Rep;
  rep->options = options;
  rep->file = file_guard.release();
  rep->index_block = index_block;
  rep->file_number = file_number;
  rep->filter_data_block = filter_data_block;
  rep->filter = filter_reader;
  
  *table = new Table(rep);
  return Status::OK();
}

Table::~Table() { delete rep_; }

Status Table::ReadBlock(RandomAccessFile* file, const ReadOptions& options,
                        const BlockHandle& handle, BlockContents* result) {
  (void)options; 
  result->data = Slice();
  result->heap_allocated = false;

  size_t n = static_cast<size_t>(handle.size());
  char* buf = new char[n + 5]; 
  Slice contents;
  
  Status s = file->Read(handle.offset(), n + 5, &contents, buf);
  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  
  if (contents.size() < n + 5) {
    delete[] buf;
    return Status::Corruption("Block data too short");
  }

  const char* trailer_ptr = contents.data() + n;
  uint32_t stored_crc = DecodeFixed32(trailer_ptr + 1);
  stored_crc = crc32c::Unmask(stored_crc);

  uint32_t actual_crc = crc32c::Value(contents.data(), n);
  actual_crc = crc32c::Extend(actual_crc, trailer_ptr, 1);
  
  if (actual_crc != stored_crc) {
    delete[] buf;
    return Status::Corruption("Block checksum mismatch");
  }
  
  result->data = Slice(buf, n);
  result->heap_allocated = true;
  return Status::OK();
}

// 【关键实现】静态回调函数
Iterator* Table::BlockReader(void* arg, const ReadOptions& options, const Slice& handle_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  
  // 1. 解析 Handle
  BlockHandle handle;
  // 【修复】创建本地 Slice 副本，因为 DecodeFrom 会修改指针
  Slice input = handle_value;
  if (!handle.DecodeFrom(&input).ok()) {
    return nullptr; 
  }

  // 2. 读取 Block (带缓存逻辑)
  Block* block = nullptr;
  
  // 这里的缓存逻辑 Week 7 已经写过，但为了 BlockReader 适配，
  // 我们简化一下：如果用 TwoLevelIterator，通常是在做 Compaction，
  // Compaction 时的 ReadOptions.fill_cache 通常为 false，避免污染缓存。
  // 但为了逻辑完整，我们还是加上 Cache 查找。
  
  Cache* block_cache = table->rep_->options.block_cache.get();
  Cache::Handle* cache_handle = nullptr;

  if (block_cache != nullptr && options.fill_cache) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->file_number);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice cache_key(cache_key_buffer, sizeof(cache_key_buffer));

      cache_handle = block_cache->Lookup(cache_key);
      
      if (cache_handle != nullptr) {
          block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
          BlockContents contents;
          if (ReadBlock(table->rep_->file, options, handle, &contents).ok()) {
              block = new Block(contents);
              // 插入缓存
              cache_handle = block_cache->Insert(cache_key, block, contents.data.size(), &DeleteCachedBlock);
          }
      }
  } else {
      // 无缓存模式
      BlockContents contents;
      if (ReadBlock(table->rep_->file, options, handle, &contents).ok()) {
          block = new Block(contents);
      }
  }

  if (block != nullptr) {
      // 【修复】使用 Rep 中的 user_cmp，而不是新建一个
      Iterator* iter = block->NewIterator(&table->rep_->user_cmp);
      
      // 注意：这里有个生命周期问题。
      // 如果 iter 是从 Cache 里的 Block 创建的，Block 不会被 delete。
      // 如果是从堆上 new 的 Block，需要有人负责 delete Block。
      // LevelDB 的做法是 Iterator 注册 Cleanup 回调。
      // Day 2 简化版：我们假设如果用了 Cache，Cache 负责释放。
      // 如果没用 Cache，这里的 block 指针实际上泄露给了 Iterator，
      // 而我们的 Iterator 目前还不支持 RegisterCleanup。
      // 这是一个已知缺陷，Compaction Week Day 2 暂且忽略内存泄漏，或者 BlockReader 返回的 Iterator 析构时负责 delete block。
      
      // 【关键修改】注册清理函数
      if (cache_handle != nullptr) {
          // 情况 A: Block 来自 Cache，迭代器销毁时释放 Cache Handle
          iter->RegisterCleanup(&ReleaseCacheHandle, block_cache, cache_handle);
      } else {
          // 情况 B: Block 是 new 出来的（未走 Cache），迭代器销毁时 delete block
          iter->RegisterCleanup(&DeleteBlock, block, nullptr);
      }
      return iter;
  }
  
  return nullptr;
}

// 【新增】
Iterator* Table::NewIterator(const ReadOptions& options) {
  // 使用 Rep 中的 user_cmp
  Iterator* index_iter = rep_->index_block->NewIterator(&rep_->user_cmp);
  
  return NewTwoLevelIterator(index_iter, &Table::BlockReader, const_cast<Table*>(this), options);
}

// 【关键修复】Table::InternalGet 函数
Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&, const Slice&)) {
  // k 是 InternalKey
  assert(k.size() >= 8);
  Slice target_user_key(k.data(), k.size() - 8);

  // 1. Bloom Filter 过滤
  // Bloom Filter 是基于 UserKey 构建的
  if (rep_->filter != nullptr && !rep_->filter->KeyMayMatch(target_user_key)) {
      // fprintf(stderr, "[InternalGet] Bloom Filter Miss for %s\n", target_user_key.ToString().c_str());
      return Status::OK(); // Key 不存在，直接返回 OK (NotFound)
  }
  // fprintf(stderr, "[InternalGet] Bloom Filter Hit for %s\n", target_user_key.ToString().c_str());

  // 2. Index Block 查找
  // Index Block 存的是 User Key，所以用 UserKeyComparator 查找
  Iterator* iiter = rep_->index_block->NewIterator(&rep_->user_cmp);
  if (iiter == nullptr) {
      return Status::Corruption("Index block is invalid");
  }
  iiter->Seek(target_user_key);

  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    BlockHandle handle;
    if (handle.DecodeFrom(&handle_value).ok()) {
      
      Block* block = nullptr;
      Cache::Handle* cache_handle = nullptr;
      Cache* block_cache = rep_->options.block_cache.get();
      
      // 3. Block Cache 查找 (Data Block)
      if (block_cache != nullptr && options.fill_cache) {
          char cache_key_buffer[16];
          EncodeFixed64(cache_key_buffer, rep_->file_number);
          EncodeFixed64(cache_key_buffer + 8, handle.offset());
          Slice cache_key(cache_key_buffer, sizeof(cache_key_buffer));

          cache_handle = block_cache->Lookup(cache_key);
          
          if (cache_handle != nullptr) {
              block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
              // fprintf(stderr, "[Cache] Hit block at offset %lu\n", handle.offset());
          } else {
              // fprintf(stderr, "[Cache] Miss block at offset %lu\n", handle.offset());
              BlockContents contents;
              if (ReadBlock(rep_->file, options, handle, &contents).ok()) {
                  block = new Block(contents);
                  cache_handle = block_cache->Insert(cache_key, block, contents.data.size(), &DeleteCachedBlock);
              }
          }
      } else {
          // 无缓存模式，直接读磁盘
          BlockContents contents;
          if (ReadBlock(rep_->file, options, handle, &contents).ok()) {
              block = new Block(contents);
          }
      }

      if (block != nullptr) {
        // 4. Data Block 内部查找
        // 【关键修复】Data Block 存的是 Internal Key，必须使用 InternalKeyComparator
        InternalKeyComparator icmp_local; // 临时构造一个，或者在 Table::Rep 里存一个
        Iterator* diter = block->NewIterator(&icmp_local);
        
        // Data Block 内部用 Internal Key 查找
        diter->Seek(k); 
        
        if (diter->Valid()) {
            Slice found_key = diter->key(); // 这是 Internal Key
            
            // 【关键修复】防御性检查：必须确保 Key 长度足够容纳 Tag (8字节)
            if (found_key.size() >= 8) { 
                // 安全提取 User Key (不会越界)
                Slice found_user_key(found_key.data(), found_key.size() - 8);
                
                // 比较 User Key 是否相等
                if (rep_->user_cmp.Compare(found_user_key, target_user_key) == 0) {
                     // 找到了！且 User Key 匹配。
                     // 回调函数接收到的是 Internal Key 和 Value
                     handle_result(arg, found_key, diter->value());
                }
            } else {
                // 如果读到了非法 Key (长度 < 8)，直接忽略，不做任何处理。
                // 这有效地防止了后续逻辑因为坏数据而 Crash。
                // 可选：打印一条错误日志
                // fprintf(stderr, "[InternalGet] Ignored corrupted key with len=%lu\n", found_key.size());
            }
        }
        delete diter; // 释放 Data Block 迭代器
        
        // 5. 释放 Block 资源
        if (cache_handle != nullptr) {
            block_cache->Release(cache_handle); // 释放 Cache 句柄
        } else {
            delete block; // 没走 Cache，手动删除 Block
        }
      }
    }
  }
  delete iiter; // 释放 Index Block 迭代器
  return Status::OK();
}

} // namespace titankv