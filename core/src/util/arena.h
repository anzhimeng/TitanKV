#pragma once
#include <vector>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <atomic>

namespace titankv {

class Arena {
public:
    Arena();
    ~Arena();

    // 禁止拷贝
    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;

    // 基本分配接口
    char* Allocate(size_t bytes);

    //带对齐的分配 (Node 节点通常需要 8 字节对齐)
    char* AllocateAligned(size_t bytes);

    // 统计内存使用量
    size_t MemoryUsage() const {
        return memory_usage_.load(std::memory_order_relaxed);
    }

private:
    char* AllocateFallback(size_t bytes);
    char* AllocateNewBlock(size_t block_bytes);

    // 当前 Block 的分配状态
    char* alloc_ptr_;
    size_t alloc_bytes_remaining_;

    // 所有的 Block
    std::vector<char*> blocks_;

    // 统计
    std::atomic<size_t> memory_usage_;
};

} // namespace titankv