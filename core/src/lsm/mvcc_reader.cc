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
    iter->Seek(seek_key);
    while (iter->Valid()) {
        Slice found_key = iter->key();
        if (found_key.size() < 9) break;

        Slice mvcc_key = ExtractUserKey(found_key);
        if (mvcc_key.size() < 9 || mvcc_key[0] != kCFWrite) break;

        uint64_t found_commit_ts = 0;
        Slice found_user_key = DecodeMvccKey(mvcc_key, &found_commit_ts);
        if (found_user_key != key) break;
        if (found_commit_ts > snapshot_) {
            iter->Next();
            continue;
        }

        Slice val = iter->value();
        if (val.empty()) {
            iter->Next();
            continue;
        }
        *commit_ts = found_commit_ts;
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
