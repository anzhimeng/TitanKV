#pragma once
#include <cstdint>
#include <string>
#include "titankv/slice.h"

namespace titankv {

    // --- Fixed-length encoding (定长编码) ---
    // // 用于磁盘 Header，速度快，易于解析


    // 追加 4字节 / 8字节 到 dst
    void PutFixed32(std::string* dst, uint32_t value);
    void PutFixed64(std::string* dst, uint64_t value);

    //往固定 buffer写入
    void EncodeFixed32(char* dst, uint32_t value);
    void EncodeFixed64(char* dst, uint64_t value);

    //从 buffer读取
    uint32_t DecodeFixed32(const char* ptr);
    uint64_t DecodeFixed64(const char* ptr);

    // --- Varint encoding (变长编码) ---
    // // 用于内存索引，节省 RAM
    char* EncodeVarint32(char* dst, uint32_t v);
    char* EncodeVarint64(char* dst, uint64_t v);

    // 追加变长整数，返回写入的字节数
    void PutVarint32(std::string* dst, uint32_t value);
    void PutVarint64(std::string* dst, uint64_t value);

    // 辅助函数：计算变长编码后的长度
    int VarintLength(uint64_t v);

    // 从 Slice 中解析变长整数，解析成功后会自动推进 Slice 的指针
    // // 失败返回 false
    bool GetVarint32(Slice* input, uint32_t* value);
    bool GetVarint64(Slice* input, uint64_t* value);
}   // namespace titankv
