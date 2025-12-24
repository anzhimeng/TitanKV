#include "util/crc32c.h"
#include <crc32c/crc32c.h> // 引入 Google 库的头文件

namespace titankv {
namespace crc32c {

// 直接透传调用 Google 的硬件加速实现
uint32_t Value(const char* data, size_t n) {
  return ::crc32c::Crc32c(data, n);
}

uint32_t Extend(uint32_t init_crc, const char* data, size_t n) {
  return ::crc32c::Extend(init_crc, reinterpret_cast<const uint8_t*>(data), n);
}

// Mask 逻辑必须保留，Google 库不提供这个业务逻辑
static const uint32_t kMaskDelta = 0xa282ead8;

uint32_t Mask(uint32_t crc) {
  // Rotate right by 15 bits and add a constant.
  return ((crc >> 15) | (crc << 17)) + kMaskDelta;
}

uint32_t Unmask(uint32_t masked_crc) {
  uint32_t rot = masked_crc - kMaskDelta;
  return ((rot >> 17) | (rot << 15));
}

}  // namespace crc32c
}  // namespace titankv