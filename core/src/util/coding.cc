#include "util/coding.h"
#include <limits>
#include <cassert>
#include <cstring>


namespace titankv {

std::string ToHex(const std::string& s) {
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for (unsigned char c : s) {
        ss << std::setw(2) << (int)c << " ";
    }
    return ss.str();
}

std::string ToHex(const char* data, size_t len) {
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for (size_t i = 0; i < len; ++i) {
        ss << std::setw(2) << static_cast<unsigned int>(static_cast<unsigned char>(data[i])) << " ";
    }
    return ss.str();
}

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
    const char* p = input->data();
    const char* limit = p + input->size();
    const char* q = GetVarint32Ptr(p, limit, value); // 复用刚写的函数
    if (q == nullptr) {
        return false;
    } else {
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

const char* GetVarint32Ptr(const char* p, const char* limit, uint32_t* value) {
    if (p < limit) {
        uint32_t result = *(reinterpret_cast<const uint8_t*>(p));
        if ((result & 128) == 0) {
            // 快速路径：只有 1 个字节，直接返回
            *value = result;
            return p + 1;
        }
    }

    // 慢速路径：多字节解析
    uint32_t result = 0;
    for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
        uint32_t byte = *(reinterpret_cast<const uint8_t*>(p));
        p++;
        if (byte & 128) {
            // 最高位是 1，说明后面还有数据
            // 取低 7 位，左移后累加到 result
            result |= ((byte & 127) << shift);
        } else {
            // 最高位是 0，这是最后一个字节
            result |= (byte << shift);
            *value = result;
            return p;
        }
    }
    
    // 解析失败：读过了 limit 或者数字太大超过了 32 位 (5字节)
    return nullptr;
}

bool GetLengthPrefixedSlice(Slice* input, Slice* result) {
    uint32_t len;
    // 1. 读取长度
    if (GetVarint32(input, &len)) {
        // 2. 检查剩余数据是否足够
        if (input->size() >= len) {
            *result = Slice(input->data(), len);
            input->remove_prefix(len);
            return true;
        }
    }
    return false;
}

// 【新增】Big Endian 编码
void PutFixed64BigEndian(std::string* dst, uint64_t value) {
    char buf[8];
    buf[0] = (value >> 56) & 0xff;
    buf[1] = (value >> 48) & 0xff;
    buf[2] = (value >> 40) & 0xff;
    buf[3] = (value >> 32) & 0xff;
    buf[4] = (value >> 24) & 0xff;
    buf[5] = (value >> 16) & 0xff;
    buf[6] = (value >> 8) & 0xff;
    buf[7] = value & 0xff;
    dst->append(buf, 8);
}

// 【新增】Big Endian 解码
uint64_t DecodeFixed64BigEndian(const char* ptr) {
    const uint8_t* buffer = reinterpret_cast<const uint8_t*>(ptr);
    return (static_cast<uint64_t>(buffer[0]) << 56) |
           (static_cast<uint64_t>(buffer[1]) << 48) |
           (static_cast<uint64_t>(buffer[2]) << 40) |
           (static_cast<uint64_t>(buffer[3]) << 32) |
           (static_cast<uint64_t>(buffer[4]) << 24) |
           (static_cast<uint64_t>(buffer[5]) << 16) |
           (static_cast<uint64_t>(buffer[6]) << 8) |
           (static_cast<uint64_t>(buffer[7]));
}

// 【修改】EncodeMvccKey 使用 Big Endian
std::string EncodeMvccKey(char cf, const Slice& key, uint64_t ts) {
    std::string dst;
    dst.push_back(cf);
    dst.append(key.data(), key.size());
    uint64_t ts_desc = std::numeric_limits<uint64_t>::max() - ts;
    // 使用 Big Endian !
    PutFixed64BigEndian(&dst, ts_desc); 
    return dst;
}

std::string EncodeLockKey(const Slice& key) {
    std::string dst;
    // 假设 kCFLock 在 coding.h 中定义为 'l'
    // 如果没有 enum，这里直接用 'l' 也行
    dst.push_back('l'); 
    dst.append(key.data(), key.size());
    return dst;
}

Slice DecodeMvccKey(const Slice& internal_key, uint64_t* ts) {
    assert(internal_key.size() >= 9); // 1B Prefix + 8B TS
    
    const char* data = internal_key.data();
    size_t size = internal_key.size();
    
    // 提取 TS
    uint64_t ts_desc = DecodeFixed64BigEndian(data + size - 8);
    *ts = std::numeric_limits<uint64_t>::max() - ts_desc;
    return Slice(data + 1, size - 9);
}

} // namespace titankv
