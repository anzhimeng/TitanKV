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

} // namespace titankv