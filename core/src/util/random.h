#pragma once

#include <cstdint>
#include <cassert>

namespace titankv {

// 一个基于线性同余生成器 (LCG) 的高性能伪随机数生成器。
// 算法：seed = (seed * A) % M
// 其中 A = 16807, M = 2^31 - 1
// 这个实现参考了 LevelDB/RocksDB，速度极快且不需要锁（线程不安全，每个线程应持有自己的实例）。
class Random {
 private:
  uint32_t seed_;

 public:
  // 初始化种子
  // seed 不能为 0，因为 0 * A % M 永远是 0。
  // 我们在构造函数里处理这种情况。
  explicit Random(uint32_t s) : seed_(s & 0x7fffffffu) {
    // 避免 seed 为 0 或 M
    if (seed_ == 0 || seed_ == 2147483647L) {
      seed_ = 1;
    }
  }

  // 生成下一个 32 位随机数
  // 返回值范围: [1, 2^31 - 2]
  uint32_t Next() {
    static const uint32_t M = 2147483647L; // 2^31 - 1
    static const uint64_t A = 16807;       // 7^5

    // 我们实际上要计算： seed_ = (seed_ * A) % M
    // 因为 2^31 - 1 是梅森素数，我们可以利用位运算优化取模过程。
    // product = seed_ * A
    // seed_ = (product >> 31) + (product & M)
    // 这一步原理基于：x = q * M + r => x % M = r (近似)
    
    uint64_t product = seed_ * A;

    // 混合高位和低位
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));

    // 如果结果溢出了 M，我们需要减去 M
    // 这种情况只会发生一次，因为 (A-1) + (M-1) 略大于 M
    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }

  // 生成一个 [0, n-1] 之间的均匀分布随机数
  // 要求 n > 0
  uint32_t Uniform(int n) { 
    assert(n > 0);
    return Next() % n; 
  }

  // 以 1/n 的概率返回 true
  // 常用于 SkipList 决定是否增加层高
  bool OneIn(int n) { 
    return (Next() % n) == 0; 
  }

  // 生成一个“倾斜”的随机数 (Skewed Distribution)
  // 返回 [0, max_log] 范围内的数，但是较小的数出现概率远大于较大的数。
  // 原理：先随机选一个范围 [0, max_log]，假设选了 k，
  // 然后再在 [0, 2^k - 1] 里随机选个数。
  // 这常用于测试中模拟“热点数据”或者不同大小的 Value。
  uint32_t Skewed(int max_log) {
    return Uniform(1 << Uniform(max_log + 1));
  }
};

} // namespace titankv