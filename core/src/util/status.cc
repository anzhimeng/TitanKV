#include "titankv/status.h"
#include <cstdio>
#include <cstdint>
#include <cstring>

namespace titankv {

// 辅助函数：拷贝 state 内存
const char* CopyState(const char* state) {
    uint32_t size;
    // 读取前4个字节作为长度
    memcpy(&size, state, sizeof(size));
    // size + 4字节长度 + 1字节Code
    char* result = new char[size + 5];
    memcpy(result, state, size + 5);
    return result;
}

// 构造函数：拼装错误信息
// layout: [len (4B)] [code (1B)] [msg]
Status::Status(Code code, const Slice& msg, const Slice& msg2) {
    assert(code != kOk);
    const uint32_t len1 = static_cast<uint32_t>(msg.size());
    const uint32_t len2 = static_cast<uint32_t>(msg2.size());
    const uint32_t size = len1 + (len2 ? (2 + len2) : 0);

    char* result = new char[size + 5];

    // 1. 写入总长度
    memcpy(result, &size, sizeof(size));
    // 2. 写入 Code
    result[4] = static_cast<char>(code);
    // 3. 写入 msg
    memcpy(result + 5, msg.data(), len1);
    // 4. 如果有 msg2，写入 ": " 和 msg2
    if (len2) {
        result[5 + len1] = ':';
        result[5 + len1 + 1] = ' ';
        memcpy(result + 5 + len1 + 2, msg2.data(), len2);
    }
    state_ = result;
}

// 拷贝构造
Status::Status(const Status& rhs) {
    state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
}

// 赋值操作符
Status& Status::operator=(const Status& rhs) {
    if (this != &rhs) {
        delete[] state_;
        state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
    }
    return *this;
}

// 移动赋值
Status& Status::operator=(Status&& rhs) noexcept {
    if (this != &rhs) {
        delete[] state_;
        state_ = rhs.state_;
        rhs.state_ = nullptr;
    }
    return *this;
}

std::string Status::ToString() const {
    if (state_ == nullptr) {
        return "OK";
    }

    char tmp[30];
    const char* type;
    switch (code()) {
        case kOk: type = "OK"; break;
        case kNotFound: type = "NotFound: "; break;
        case kCorruption: type = "Corruption: "; break;
        case kNotSupported: type = "Not implemented: "; break;
        case kInvalidArgument: type = "Invalid argument: "; break;
        case kIOError: type = "IO error: "; break;
        case kAborted: type = "Aborted: "; break;
        default:
            snprintf(tmp, sizeof(tmp), "Unknown code(%d): ", static_cast<int>(code()));
            type = tmp;
            break;
    }

    std::string result(type);
    uint32_t length;
    memcpy(&length, state_, sizeof(length));
    result.append(state_ + 5, length);
    return result;
}

} // namespace titankv
