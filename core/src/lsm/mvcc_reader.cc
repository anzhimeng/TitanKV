#include "lsm/mvcc_reader.h"
#include "util/coding.h"
#include "lsm/block.h"

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
    
    // Seek 到最开始 (最小的 TS_Desc，即最大的 TS)
    // 也就是 TS=Max
    std::string start_key = EncodeMvccKey(kCFWrite, key, std::numeric_limits<uint64_t>::max());
    // 构造 Internal Key
    std::string internal_start;
    internal_start.append(start_key);
    PutFixed64(&internal_start, PackSequenceAndType(kMaxSequenceNumber, kTypeValue));
    
    iter->Seek(internal_start);

    // 线性扫描，找到第一个 TS <= snapshot_ 的
    for (; iter->Valid(); iter->Next()) {
        Slice internal_key = iter->key();
        Slice found_mvcc_key = ExtractUserKey(internal_key);
        
        // 检查 User Key (不含 TS) 是否匹配
        size_t user_key_len = key.size();
        if (found_mvcc_key.size() != 1 + user_key_len + 8) break;
        
        Slice found_user_key(found_mvcc_key.data() + 1, user_key_len);
        if (found_user_key != key) break;

        // 解析 TS
        uint64_t ts;
        DecodeMvccKey(found_mvcc_key, &ts);
        
        // 找到第一个 <= snapshot 的版本
        if (ts <= snapshot_) {
            *commit_ts = ts;
            *write_info = iter->value().ToString();
            return Status::OK();
        }
    }
    
    return Status::NotFound("No visible version");
}

Status MvccReader::GetValue(const Slice& key, uint64_t start_ts, std::string* value) {
    // 直接查 Default CF: d{Key}{Max-StartTS}
    // 这里可以用 GetCF，因为 StartTS 是确定的（Point Lookup）
    return db_->GetCF(kCFDefault, key, value, start_ts);
}

}