#include "titankv/options.h"
#include "titankv/slice.h"
#include "util/hash.h"
#include <vector>

namespace titankv {

class BloomFilterPolicy : public FilterPolicy {
 public:
  explicit BloomFilterPolicy(int bits_per_key) : bits_per_key_(bits_per_key) {
    // k = ln(2) * (m/n).  (m/n) is bits_per_key.
    k_ = static_cast<size_t>(bits_per_key * 0.69);
    if (k_ < 1) k_ = 1;
    if (k_ > 30) k_ = 30;
  }

  const char* Name() const override { return "titankv.BuiltinBloomFilter"; }

  void CreateFilter(const std::vector<std::string>& keys, std::string* dst) const override {
    size_t n = keys.size();
    // 计算需要的 bit 数
    size_t bits = n * bits_per_key_;
    if (bits < 64) bits = 64;
    
    // 对齐到字节
    size_t bytes = (bits + 7) / 8;
    bits = bytes * 8;

    const size_t init_size = dst->size();
    dst->resize(init_size + bytes, 0);
    
    // 在最后追加 k_ (哈希函数个数)，方便读取时知道用了几个哈希
    dst->push_back(static_cast<char>(k_)); 
    
    char* array = &(*dst)[init_size];

    for (size_t i = 0; i < n; i++) {
      // Double Hashing 模拟多哈希函数: h(i) = h1 + i * h2
      uint32_t h = Hash(keys[i].data(), keys[i].size(), 0xbc9f1d34);
      const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
      
      for (size_t j = 0; j < k_; j++) {
        const uint32_t bitpos = h % bits;
        array[bitpos / 8] |= (1 << (bitpos % 8));
        h += delta;
      }
    }
  }

  bool KeyMayMatch(const Slice& key, const Slice& bloom_filter) const override {
    const size_t len = bloom_filter.size();
    if (len < 2) return false;

    const char* array = bloom_filter.data();
    const size_t bits = (len - 1) * 8;

    // 最后一个字节存的是 k_
    const size_t k = array[len - 1];
    if (k > 30) return true; // 保守处理

    uint32_t h = Hash(key.data(), key.size(), 0xbc9f1d34);
    const uint32_t delta = (h >> 17) | (h << 15);

    for (size_t j = 0; j < k; j++) {
      const uint32_t bitpos = h % bits;
      if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0) {
        return false;
      }
      h += delta;
    }
    return true;
  }

 private:
  size_t bits_per_key_;
  size_t k_;
};

std::shared_ptr<FilterPolicy> NewBloomFilterPolicy(int bits_per_key) {
  return std::make_shared<BloomFilterPolicy>(bits_per_key);
}

} // namespace titankv