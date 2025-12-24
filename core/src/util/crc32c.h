#pragma once

#include <cstddef>
#include <cstdint>

namespace titankv {
namespace crc32c {

// 计算数据的 CRC32C 校验和。
// 底层将调用 google/crc32c 的硬件加速实现。
uint32_t Value(const char* data, size_t n);

// 在现有的 crc 基础上继续计算 data 的校验和。
// 用于分段写入场景（例如先算 Header，再算 Key，再算 Value）。
// init_crc: 上一次计算的结果。
uint32_t Extend(uint32_t init_crc, const char* data, size_t n);

// 返回一个“掩码”后的 CRC。
// 动机：如果不做 Mask，原本包含校验和的数据可能会被错误地识别为校验和本身。
// 此外，Mask 还能检测到全是 0 的数据块中的错误（因为 CRC(0)=0，但 Mask(0)!=0）。
// LevelDB/RocksDB 都在写入磁盘前对 CRC 进行 Mask。
uint32_t Mask(uint32_t crc);

// 解除掩码，恢复原始 CRC。
// 读取数据进行校验时使用。
uint32_t Unmask(uint32_t masked_crc);

}  // namespace crc32c
}  // namespace titankv