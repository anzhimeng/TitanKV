#include "util/coding.h"

namespace titankv {

// --- Fixed Implementation ---

void EncodeFixed32(char* dst, uint32_t value) {
    uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);
    // 强制小端序
    buffer[0] = static_cast<uint8_t>(value);
    buffer[1] = static_cast<uint8_t>(value >> 8);
    buffer[2] = static_cast<uint8_t>(value >> 16);
    buffer[3] = static_cast<uint8_t>(value >> 24);
}

void EncodeFixed64(char* dst, uint64_t value) {
    uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);
    buffer[0] = static_cast<uint8_t>(value);
    buffer[1] = static_cast<uint8_t>(value >> 8);
    buffer[2] = static_cast<uint8_t>(value >> 16);
    buffer[3] = static_cast<uint8_t>(value >> 24);
    buffer[4] = static_cast<uint8_t>(value >> 32);
    buffer[5] = static_cast<uint8_t>(value >> 40);
    buffer[6] = static_cast<uint8_t>(value >> 48);
    buffer[7] = static_cast<uint8_t>(value >> 56);
}

void PutFixed32(std::string* dst, uint32_t value) {
    char buf[sizeof(value)];
    EncodeFixed32(buf, value);
    dst->append(buf, sizeof(buf));
}

void PutFixed64(std::string* dst, uint64_t value) {
    char buf[sizeof(value)];
    EncodeFixed64(buf, value);
    dst->append(buf, sizeof(buf));
}

uint32_t DecodeFixed32(const char* ptr) {
    const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);
    return (static_cast<uint32_t>(buffer[0])) |
           (static_cast<uint32_t>(buffer[1]) << 8) |
           (static_cast<uint32_t>(buffer[2]) << 16) |
           (static_cast<uint32_t>(buffer[3]) << 24);
}

uint64_t DecodeFixed64(const char* ptr) {
    const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);
    return (static_cast<uint64_t>(buffer[0])) |
           (static_cast<uint64_t>(buffer[1]) << 8) |
           (static_cast<uint64_t>(buffer[2]) << 16) |
           (static_cast<uint64_t>(buffer[3]) << 24) |
           (static_cast<uint64_t>(buffer[4]) << 32) |
           (static_cast<uint64_t>(buffer[5]) << 40) |
           (static_cast<uint64_t>(buffer[6]) << 48) |
           (static_cast<uint64_t>(buffer[7]) << 56);
}

// 原理：每个字节存 7 位数据。
// 最高位 (MSB, bit 7) 为 1 表示后面还有字节，为 0 表示这是最后一个字节。
// 128 的二进制是 1000 0000，用作标志位 (B)。

char* EncodeVarint32(char* dst, uint32_t v) {
    // 使用 uint8_t 指针操作，避免 char 有符号带来的位运算困扰
    uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
    static const int B = 128;

    if (v < (1 << 7)) {
        *(ptr++) = v;
    } else if (v < (1 << 14)) {
        *(ptr++) = v | B;
        *(ptr++) = v >> 7;
    } else if (v < (1 << 21)) {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = v >> 14;
    } else if (v < (1 << 28)) {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = (v >> 14) | B;
        *(ptr++) = v >> 21;
    } else {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = (v >> 14) | B;
        *(ptr++) = (v >> 21) | B;
        *(ptr++) = v >> 28;
    }
    
    return reinterpret_cast<char*>(ptr);
}
char* EncodeVarint64(char* dst, uint64_t v) {
    static const int B = 128;
    uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
    while (v >= B) {
        *(ptr++) = (v & (B - 1)) | B;
        v >>= 7;
    }
    *(ptr++) = static_cast<uint8_t>(v);
    return reinterpret_cast<char*>(ptr);
}

void PutVarint32(std::string* dst, uint32_t v) {
    char buf[5];
    char* ptr = EncodeVarint32(buf, v);
    dst->append(buf, ptr - buf);
}

void PutVarint64(std::string* dst, uint64_t v) {
    char buf[10];
    char* ptr = EncodeVarint64(buf, v);
    dst->append(buf, ptr - buf);
}

int VarintLength(uint64_t v) {
    int len = 1;
    while (v >= 128) {
        v >>= 7;
        len++;
    }
    return len;
}

const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
    uint64_t result = 0;
    for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
        uint64_t byte = *(reinterpret_cast<const uint8_t*>(p));
        p++;
        if (byte & 128) {
            // More bytes follow
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return p;
        }
    }
    return nullptr;
}

bool GetVarint32(Slice* input, uint32_t* value) {
    uint64_t v;
    const char* p = input->data();
    const char* limit = p + input->size();
    const char* q = GetVarint64Ptr(p, limit, &v);
    if (q == nullptr) {
        return false;
    } else {
        *value = static_cast<uint32_t>(v);
        input->remove_prefix(q - p);
        return true;
    }
}

bool GetVarint64(Slice* input, uint64_t* value) {
    const char* p = input->data();
    const char* limit = p + input->size();
    const char* q = GetVarint64Ptr(p, limit, value);
    if (q == nullptr) {
        return false;
    } else {
        input->remove_prefix(q - p);
        return true;
    }
}

} // namespace titankv
