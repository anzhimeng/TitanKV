#include "lsm/memtable.h"
#include "util/coding.h"
#include <cstdio>
#include <cstring>

namespace titankv {

MemTable::MemTable(const InternalKeyComparator& cmp)
    : comparator_(cmp), refs_(0), arena_(), table_(KeyComparator(cmp), &arena_) {}

MemTable::~MemTable() { assert(refs_ == 0); }

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr) const {
  Slice a, b;
  
  // 解析 a
  uint32_t a_len;
  Slice a_slice(aptr, 5); 
  GetVarint32(&a_slice, &a_len);
  a = Slice(a_slice.data(), a_len); 

  // 解析 b
  uint32_t b_len;
  Slice b_slice(bptr, 5); 
  GetVarint32(&b_slice, &b_len);
  b = Slice(b_slice.data(), b_len);

  // DEBUG LOG
  // fprintf(stderr, "Compare: KeyA_Len=%u, KeyB_Len=%u\n", a_len, b_len);

  return comparator.Compare(a, b);
}

void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key, const Slice& value) {
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  
  size_t encoded_len = VarintLength(internal_key_size) + internal_key_size +
                       VarintLength(val_size) + val_size;
  
  char* buf = arena_.Allocate(encoded_len);
  char* p = buf;
  
  // 1. Write Internal Key Size
  p = EncodeVarint32(p, internal_key_size);
  
  // 2. Write User Key
  std::memcpy(p, key.data(), key_size);
  p += key_size;
  
  // 3. Write Tag
  EncodeFixed64(p, PackSequenceAndType(s, type));
  p += 8; // 【重要】确保这里加了 8
  
  // 4. Write Value Size
  p = EncodeVarint32(p, val_size);
  
  // 5. Write Value
  std::memcpy(p, value.data(), val_size);
  p += val_size; // 【重要】确保这里加了 val_size
  
  assert(p == buf + encoded_len);
  
  // DEBUG LOG
  fprintf(stderr, "[MemTable::Add] Inserted Key: %s, Seq: %lu, InternalLen: %lu\n", 
          key.ToString().c_str(), s, internal_key_size);
  
  table_.Insert(buf);
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice mem_key = key.memtable_key();
  Table::Iterator iter(&table_);
  
  // DEBUG LOG
  fprintf(stderr, "[MemTable::Get] Seeking UserKey: %s\n", key.user_key().ToString().c_str());
  
  iter.Seek(mem_key.data()); 

  if (iter.Valid()) {
    const char* entry = iter.key();
    
    // 解析 entry 中的 Key 长度
    uint32_t key_length;
    Slice entry_slice(entry, 5); 
    const char* key_ptr = entry; 
    GetVarint32(&entry_slice, &key_length);
    key_ptr = entry_slice.data(); 
    
    // 解析 Entry Key
    Slice entry_key(key_ptr, key_length - 8); 
    
    // DEBUG LOG
    fprintf(stderr, "[MemTable::Get] Found Entry. UserKey: %s (Len: %lu)\n", 
            entry_key.ToString().c_str(), entry_key.size());

    if (comparator_.user_key_compare(entry_key, key.user_key()) == 0) {
      
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      ValueType type = static_cast<ValueType>(tag & 0xff);
      SequenceNumber seq = tag >> 8;

      fprintf(stderr, "[MemTable::Get] Key Match! Seq: %lu, Type: %d\n", seq, type);

      if (type == kTypeValue) {
        Slice val_slice(key_ptr + key_length, 5);
        uint32_t val_length;
        GetVarint32(&val_slice, &val_length);
        
        // 这里的 data 指针计算要注意：val_slice.data() 已经指向了 Varint 之后
        value->assign(val_slice.data(), val_length);
        *s = Status::OK();
        return true;
      } else {
        *s = Status::NotFound("Key deleted");
        return true;
      }
    } else {
        fprintf(stderr, "[MemTable::Get] Key Mismatch! Wanted: %s, Got: %s\n", 
                key.user_key().ToString().c_str(), entry_key.ToString().c_str());
    }
  } else {
      fprintf(stderr, "[MemTable::Get] Seek returned Invalid iterator!\n");
  }
  return false;
}

} // namespace titankv