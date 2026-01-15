#include "titankv/write_batch.h"
#include "lsm/dbformat.h" // ValueType

namespace titankv {

void WriteBatch::Put(const Slice& key, const Slice& value) {
    entries_.push_back({kTypeValue, key.ToString(), value.ToString()});
}

void WriteBatch::Delete(const Slice& key) {
    entries_.push_back({kTypeDeletion, key.ToString(), ""});
}

void WriteBatch::Clear() {
    entries_.clear();
}

void WriteBatch::PutCF(CFType cf, const Slice& key, const Slice& value, uint64_t ts) {
    std::string encoded_key;
    if (cf == kCFLock) {
        encoded_key = EncodeLockKey(key);
    } else {
        // Default 或 Write CF，需要 TS
        encoded_key = EncodeMvccKey(static_cast<char>(cf), key, ts);
    }
    // 复用底层的 Put (它会把 encoded_key 放入 entries_)
    Put(encoded_key, value);
}

void WriteBatch::DeleteCF(CFType cf, const Slice& key, uint64_t ts) {
    std::string encoded_key;
    if (cf == kCFLock) {
        encoded_key = EncodeLockKey(key);
    } else {
        encoded_key = EncodeMvccKey(static_cast<char>(cf), key, ts);
    }
    Delete(encoded_key);
}

std::string WriteBatch::Encode() const {
    std::string dst;
    // 1. 写入 Count
    PutFixed32(&dst, entries_.size());
    
    // 2. 写入 Entries
    for (const auto& entry : entries_) {
        dst.push_back(entry.type);
        PutVarint32(&dst, entry.key.size());
        dst.append(entry.key);
        PutVarint32(&dst, entry.value.size());
        dst.append(entry.value);
    }
    return dst;
}

} // namespace titankv