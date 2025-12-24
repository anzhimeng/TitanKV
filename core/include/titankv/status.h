#pragma once

#include <string>
#include "titankv/slice.h"

namespace titankv {

class Status {
public:
    // 默认构造为 OK
    Status() : state_(nullptr) {}
    
    // 析构函数
    ~Status() { delete[] state_; }

    // 拷贝构造与赋值 (深拷贝)
    Status(const Status& rhs);
    Status& operator=(const Status& rhs);

    // 移动构造与赋值 (C++11, 性能更高)
    Status(Status&& rhs) noexcept : state_(rhs.state_) { rhs.state_ = nullptr; }
    Status& operator=(Status&& rhs) noexcept;

    // 静态工厂方法：创建各种状态
    static Status OK() { return Status(); }
    static Status NotFound(const Slice& msg, const Slice& msg2 = Slice()) {
        return Status(kNotFound, msg, msg2);
    }
    static Status Corruption(const Slice& msg, const Slice& msg2 = Slice()) {
        return Status(kCorruption, msg, msg2);
    }
    static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice()) {
        return Status(kNotSupported, msg, msg2);
    }
    static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice()) {
        return Status(kInvalidArgument, msg, msg2);
    }
    static Status IOError(const Slice& msg, const Slice& msg2 = Slice()) {
        return Status(kIOError, msg, msg2);
    }

    // 状态检查
    bool ok() const { return state_ == nullptr; }
    bool IsNotFound() const { return code() == kNotFound; }
    bool IsCorruption() const { return code() == kCorruption; }
    bool IsIOError() const { return code() == kIOError; }
    bool IsNotSupported() const { return code() == kNotSupported; }
    bool IsInvalidArgument() const { return code() == kInvalidArgument; }

    // 转为字符串 (用于打印日志)
    std::string ToString() const;

private:
    // 内部错误码定义
    enum Code {
        kOk = 0,
        kNotFound = 1,
        kCorruption = 2,
        kNotSupported = 3,
        kInvalidArgument = 4,
        kIOError = 5
    };

    // 构造错误状态的私有函数
    Status(Code code, const Slice& msg, const Slice& msg2);
    
    // 获取当前错误码
    Code code() const {
        return (state_ == nullptr) ? kOk : static_cast<Code>(state_[4]);
    }

    // state_ 的内存布局:
    //    state_[0..3] == length of message (4 bytes)
    //    state_[4]    == code (1 byte)
    //    state_[5..]  == message
    // 如果 state_ == nullptr，表示 OK
    const char* state_;
};

} // namespace titankv