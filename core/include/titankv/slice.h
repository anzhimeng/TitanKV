#pragma once

#include <cassert>
#include <cstddef>
#include <cstring>
#include <string>

namespace titankv {

class Slice {
public:
    // 构造一个空的 Slice
    Slice() : data_(""), size_(0) {}

    // 从 C 字符串构造
    Slice(const char* d, size_t n) : data_(d), size_(n) {}

    // 从 std::string 构造
    Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}

    // 从字符串字面量构造
    Slice(const char* s) : data_(s), size_(strlen(s)) {}

    // 获取数据指针
    const char* data() const { return data_; }

    // 获取长度
    size_t size() const { return size_; }

    // 判断是否为空
    bool empty() const { return size_ == 0; }

    // 获取第 n 个字节
    char operator[](size_t n) const {
        assert(n < size_);
        return data_[n];
    }

    // 清除前缀 n 个字节 (常用于解析数据时移动指针)
    void remove_prefix(size_t n) {
        assert(n <= size_);
        data_ += n;
        size_ -= n;
    }

    // 转为 std::string (会发生拷贝，仅用于打印日志或调试)
    std::string ToString() const { return std::string(data_, size_); }

    // 比较逻辑 (类似 strcmp)
    // r < 0  : this < b
    // r == 0 : this == b
    // r > 0  : this > b
    int compare(const Slice& b) const;

    // 判断前缀是否匹配
    bool starts_with(const Slice& x) const {
        return ((size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0));
    }

    void clear() {
        data_ = "";
        size_ = 0;
    }

private:
    const char* data_;
    size_t size_;
};

// 比较函数的实现
inline int Slice::compare(const Slice& b) const {
    const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
    int r = memcmp(data_, b.data_, min_len);
    if (r == 0) {
        if (size_ < b.size_) r = -1;
        else if (size_ > b.size_) r = +1;
    }
    return r;
}

// 重载 == 操作符
inline bool operator==(const Slice& x, const Slice& y) {
    return ((x.size() == y.size()) &&
            (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice& x, const Slice& y) { return !(x == y); }

} // namespace titankv