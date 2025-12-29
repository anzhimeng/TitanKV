#pragma once
#include <cstdlib>
#include <memory>
#include <cstring>
#include <stdexcept>

namespace titankv {

// 简单的对齐内存管理 (RAII)
struct AlignedBuffer {
    char* data;
    size_t size;
    size_t alignment;

    AlignedBuffer(size_t size, size_t alignment = 4096) 
        : size(size), alignment(alignment) {
        // posix_memalign 要求 alignment 是 2 的幂且是 void* 的倍数
        if (posix_memalign((void**)&data, alignment, size) != 0) {
            throw std::runtime_error("Aligned alloc failed");
        }
    }

    ~AlignedBuffer() {
        free(data);
    }
    
    // 禁止拷贝，只能移动 (类似 unique_ptr)
    AlignedBuffer(const AlignedBuffer&) = delete;
    AlignedBuffer& operator=(const AlignedBuffer&) = delete;
    
    AlignedBuffer(AlignedBuffer&& other) noexcept : data(other.data), size(other.size), alignment(other.alignment) {
        other.data = nullptr;
        other.size = 0;
    }
};

} // namespace titankv