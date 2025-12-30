#pragma once
#include <atomic>
#include <cstdint>

namespace titankv {

struct Statistics {
    // 基础指标
    std::atomic<uint64_t> blob_bytes_written{0};
    std::atomic<uint64_t> blob_bytes_read{0};
    
    // GC 指标
    std::atomic<uint64_t> gc_run_count{0};          // GC 运行次数
    std::atomic<uint64_t> gc_bytes_reclaimed{0};    // 回收的垃圾大小
    std::atomic<uint64_t> gc_keys_moved{0};         // 搬运的有效 Key 数量

    void Reset() {
        blob_bytes_written = 0;
        blob_bytes_read = 0;
        gc_run_count = 0;
        gc_bytes_reclaimed = 0;
        gc_keys_moved = 0;
    }
};

} // namespace titankv