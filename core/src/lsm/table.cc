#include "lsm/table.h"
#include "lsm/table_format.h"
#include "lsm/block.h"
#include "util/coding.h"

namespace titankv {

struct Table::Rep {
  ~Rep() {
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  Block* index_block;
};

Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t file_size, Table** table) {
  *table = nullptr;
  
  // 1. 读取 Footer (最后 48 字节)
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

  // 2. 读取 Index Block
  BlockContents index_block_contents;
  // ReadBlock 是个静态辅助函数，稍后实现
  s = ReadBlock(file, ReadOptions(), footer.index_handle(), &index_block_contents);
  if (!s.ok()) return s;

  Block* index_block = new Block(index_block_contents);
  
  Rep* rep = new Table::Rep;
  rep->options = options;
  rep->file = file;
  rep->index_block = index_block;
  
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
  char* buf = new char[n + 5]; // +5 for Trailer (Type + CRC)
  Slice contents;
  
  // 读 Data + Trailer
  Status s = file->Read(handle.offset(), n + 5, &contents, buf);
  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  
  // TODO: Check CRC (Week 2 暂略)
  // Check Type (contents[n])
  
  result->data = Slice(buf, n); // 只返回 Data 部分
  result->heap_allocated = true;
  return Status::OK();
}

// 核心查找逻辑

Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&, const Slice&)) {
  (void)options;
  UserKeyComparator user_cmp;
  Iterator* iiter = rep_->index_block->NewIterator(&user_cmp);
  
  assert(k.size() >= 8);
  Slice target_user_key(k.data(), k.size() - 8);

  // DEBUG LOG
  fprintf(stderr, "[InternalGet] Target UserKey: %s\n", target_user_key.ToString().c_str());

  iiter->Seek(target_user_key);
  if (iiter->Valid()) {
    // DEBUG LOG
    fprintf(stderr, "[InternalGet] Index Hit: %s\n", iiter->key().ToString().c_str());

    Slice handle_value = iiter->value();
    BlockHandle handle;
    if (handle.DecodeFrom(&handle_value).ok()) {
      BlockContents data_block_contents;
      if (ReadBlock(rep_->file, options, handle, &data_block_contents).ok()) {
        Block data_block(data_block_contents);
        
        UserKeyComparator data_user_cmp;
        Iterator* diter = data_block.NewIterator(&data_user_cmp);
        
        diter->Seek(target_user_key);
        if (diter->Valid()) {
            // DEBUG LOG
            fprintf(stderr, "[InternalGet] Data Block Hit. Key: %s (Len %lu)\n", 
                     diter->key().ToString().c_str(), diter->key().size());
          // 在 Data Block 里找到的 Key 也是 User Key
          // 但是，我们 InternalGet 返回的 Key 需要是 InternalKey。
          // 这是一个设计上的取舍。
          // 为了 Day 3 简单，这里直接返回 User Key 即可，后续再包装。
          // 或者，如果你想返回 InternalKey，你需要从外面传入完整的 InternalKey
            // 这里的 Key 是 InternalKey，我们需要比较 UserKey 部分
            Slice found_key = diter->key();
            if (found_key.size() >= 8) {
                Slice found_user_key(found_key.data(), found_key.size() - 8);
                if (user_cmp.Compare(found_user_key, target_user_key) == 0) {
                     handle_result(arg, found_key, diter->value());
                } else {
                    fprintf(stderr, "[InternalGet] Mismatch! Found UserKey: %s\n", found_user_key.ToString().c_str());
                }
            }
        } else {
            fprintf(stderr, "[InternalGet] Data Block Seek Invalid\n");
        }
        delete diter;
      }
    }
  } else {
      fprintf(stderr, "[InternalGet] Index Seek Invalid\n");
  }
  delete iiter;
  return Status::OK();
}

} // namespace titankv