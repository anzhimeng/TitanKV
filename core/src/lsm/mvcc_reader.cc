#include "lsm/mvcc_reader.h"
#include "util/coding.h"
#include "lsm/block.h"
#include <sstream>
#include <limits>


namespace titankv {

MvccReader::MvccReader(DB* db, uint64_t snapshot) : db_(db), snapshot_(snapshot) {}
MvccReader::~MvccReader() {}

Status MvccReader::LoadLock(const Slice& key, std::string* lock_info) {
    // Lock CF 不需要 TS，直接查
    return db_->GetCF(kCFLock, key, lock_info, 0);
}

Status MvccReader::SeekWrite(const Slice& key, uint64_t* commit_ts, std::string* write_info) {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions(), kCFWrite));
    std::string seek_key = EncodeMvccKey(kCFWrite, key, snapshot_);
    //fprintf(stderr, "[Seek] Target TS: %lu. Target Hex: %s\n", snapshot_, ToHex(seek_key.data(), seek_key.size()).c_str());
    //iter->Seek(seek_key);
    iter->SeekToFirst();
    while (iter->Valid()) {
        Slice found_key = iter->key();
        
        // 【修复】宽松的长度检查
        size_t min_len = 1 + key.size() + 8;
        if (found_key.size() < min_len) break; 
        
        // 比较 User Key
        Slice found_user_key(found_key.data() + 1, key.size());
        //fprintf(stderr, "[Seek] Found Hex: %s\n", ToHex(found_user_key.data(), found_user_key.size()).c_str());
        if (found_user_key != key) break;

        Slice val = iter->value();
        if (val.empty()) {
            iter->Next();
            continue;
        }

        // 提取 TS (Desc)
        uint64_t ts_desc = DecodeFixed64BigEndian(found_key.data() + 1 + key.size());
        *commit_ts = std::numeric_limits<uint64_t>::max() - ts_desc;
        
        *write_info = val.ToString();
        return Status::OK();
    }
    return Status::NotFound("No visible version");
}
Status MvccReader::GetValue(const Slice& key, uint64_t start_ts, std::string* value) {
    // 直接查 Default CF: d{Key}{Max-StartTS}
    // 这里可以用 GetCF，因为 StartTS 是确定的（Point Lookup）
    return db_->GetCF(kCFDefault, key, value, start_ts);
}

}