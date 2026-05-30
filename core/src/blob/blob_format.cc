#include "blob/blob_format.h"
#include "util/coding.h"
#include <cstdio>

namespace titankv {

const size_t BlobRecordHeader::kHeaderSize;

// --- BlobIndex ---

void BlobIndex::EncodeTo(std::string* dst) const {
    PutVarint32(dst, file_id);
    PutVarint64(dst, offset);
    PutVarint64(dst, size);
}

Status BlobIndex::DecodeFrom(Slice* input) {
    if (GetVarint32(input, &file_id) &&
        GetVarint64(input, &offset) &&
        GetVarint64(input, &size)) {
        return Status::OK();
    }
    return Status::Corruption("BlobIndex decode failed");
}

std::string BlobIndex::ToString() const {
    char buf[100];
    snprintf(buf, sizeof(buf), "BlobIndex(file=%u, off=%lu, size=%lu)",
             file_id, offset, size);
    return std::string(buf);
}

// --- BlobRecordHeader ---

void BlobRecordHeader::EncodeTo(char* dst) const {
    EncodeFixed32(dst, crc);
    EncodeFixed32(dst + 4, size);
    EncodeFixed32(dst + 8, key_size);
}

Status BlobRecordHeader::DecodeFrom(Slice* input) {
    if (input->size() < kHeaderSize) {
        return Status::Corruption("BlobRecordHeader too short");
    }
    const char* ptr = input->data();
    crc = DecodeFixed32(ptr);
    size = DecodeFixed32(ptr + 4);
    key_size = DecodeFixed32(ptr + 8);

    // Sanity check
    if (size > 1024 * 1024 * 100 || key_size > 1024 * 1024) {
         return Status::Corruption("BlobRecordHeader has invalid sizes");
    }

    input->remove_prefix(kHeaderSize);
    return Status::OK();
}

// --- Helper Functions ---

Status ParseBlobRecord(Slice* input, ParsedBlobRecord* result) {
    // 1. 记录原始 Slice 的开始指针 (如果以后要算 CRC，可能需要用到)
    // const char* start_ptr = input->data();

    // 2. 解析 Header (DecodeFrom 会自动 remove_prefix HeaderSize)
    Status s = result->header.DecodeFrom(input);
    if (!s.ok()) {
        return s;
    }

    // 3. 检查剩余数据量
    size_t needed_size = result->header.key_size + result->header.size;
    if (input->size() < needed_size) {
        return Status::Corruption("BlobRecord data too short");
    }

    // 4. 提取 Key 和 Value (零拷贝)
    result->key = Slice(input->data(), result->header.key_size);
    result->value = Slice(input->data() + result->header.key_size, result->header.size);

    // 5. 移动指针
    input->remove_prefix(needed_size);

    return Status::OK();
}

} // namespace titankv
