#include "util/arena.h"

namespace titankv {

static const int kBlockSize = 4096; // 默认 Block 大小 4KB

Arena::Arena() : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0) {}

Arena::~Arena() {
    for (char* b : blocks_) {
        delete[] b;
    }
}

char* Arena::AllocateFallback(size_t bytes) {
    if (bytes > kBlockSize / 4) {
        // 如果申请的对象很大（比如超过 1KB），单独分配一个 Block 给它，
        // 避免浪费当前 Block 的剩余空间
        char* result = AllocateNewBlock(bytes);
        return result;
    }

    // 否则，申请一个新的标准 Block
    alloc_ptr_ = AllocateNewBlock(kBlockSize);
    alloc_bytes_remaining_ = kBlockSize;

    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes) {
    char* result = new char[block_bytes];
    blocks_.push_back(result);
    memory_usage_.fetch_add(block_bytes + sizeof(char*), std::memory_order_relaxed);
    return result;
}

char* Arena::Allocate(size_t bytes) {
    // 快速路径：当前 Block 够用
    if (bytes <= alloc_bytes_remaining_) {
        char* result = alloc_ptr_;
        alloc_ptr_ += bytes;
        alloc_bytes_remaining_ -= bytes;
        return result;
    }
    // 慢速路径
    return AllocateFallback(bytes);
}

char* Arena::AllocateAligned(size_t bytes) {
    const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
    // 这是一个位操作技巧，确保 alloc_ptr_ 是 align 的倍数
    // current_mod 是当前指针地址模 align 的余数
    size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
    size_t slop = (current_mod == 0 ? 0 : align - current_mod);
    size_t needed = bytes + slop;

    char* result;
    if (needed <= alloc_bytes_remaining_) {
        result = alloc_ptr_ + slop;
        alloc_ptr_ += needed;
        alloc_bytes_remaining_ -= needed;
    } else {
        // AllocateFallback 出来的新 Block 开头肯定是对齐的
        result = AllocateFallback(bytes);
    }
    assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
    return result;
}

} // namespace titankv