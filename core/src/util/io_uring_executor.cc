#include "util/io_uring_executor.h"
#include <iostream>
#include <cassert>
#include <unistd.h> // for pread

namespace titankv {

struct RequestContext {
    IoCallback callback;
};

IoUringExecutor::IoUringExecutor(unsigned queue_depth) : running_(true) {
    // 0 is flags
    int ret = io_uring_queue_init(queue_depth, &ring_, 0);
    if (ret < 0) {
        // 在构造函数中抛出异常是标准做法，或者使用 Init 函数返回 Status
        std::cerr << "Failed to init io_uring: " << ret << std::endl;
        // 这里为了简单不抛异常，但实际不可用
    }
    poller_thread_ = std::thread(&IoUringExecutor::PollLoop, this);
}

IoUringExecutor::~IoUringExecutor() {
    running_ = false;
    
    // 发送 NOP 唤醒 Poller
    {
        std::lock_guard<std::mutex> lock(sq_mutex_);
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (sqe) {
            io_uring_prep_nop(sqe);
            io_uring_submit(&ring_);
        }
    }

    if (poller_thread_.joinable()) {
        poller_thread_.join(); // 等待线程结束
    }
    
    io_uring_queue_exit(&ring_);
}

// 辅助函数：尝试获取 SQE，如果满了就提交并重试
static struct io_uring_sqe* GetSqeOrFlush(struct io_uring* ring) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        // 队列满了，提交已有的请求
        io_uring_submit(ring);
        // 再次尝试
        sqe = io_uring_get_sqe(ring);
    }
    return sqe;
}

void IoUringExecutor::SubmitRead(int fd, uint64_t offset, size_t length, char* buf, IoCallback callback) {
    std::lock_guard<std::mutex> lock(sq_mutex_);

    // 【修改】使用更稳健的获取逻辑
    struct io_uring_sqe* sqe = GetSqeOrFlush(&ring_);
    
    if (!sqe) {
        // 如果依然满（极其罕见，说明内核处理不过来了），只能降级同步
        // 或者在这里忙等待 (Busy Wait)
        ssize_t r = pread(fd, buf, length, offset);
        callback(r);
        return;
    }

    io_uring_prep_read(sqe, fd, buf, length, offset);
    RequestContext* ctx = new RequestContext{callback};
    io_uring_sqe_set_data(sqe, ctx);
    io_uring_submit(&ring_);
}

void IoUringExecutor::SubmitReadBatch(std::vector<ReadRequest>& requests) {
    if (requests.empty()) return;

    std::lock_guard<std::mutex> lock(sq_mutex_);

    int count = 0;
    for (auto& req : requests) {
        // 【修改】使用更稳健的获取逻辑
        struct io_uring_sqe* sqe = GetSqeOrFlush(&ring_);
        
        if (!sqe) {
            // 实在不行，降级同步
            ssize_t r = pread(req.fd, req.buf, req.length, req.offset);
            req.callback(r);
            continue;
        }

        io_uring_prep_read(sqe, req.fd, req.buf, req.length, req.offset);
        RequestContext* ctx = new RequestContext{req.callback};
        io_uring_sqe_set_data(sqe, ctx);
        count++;
    }

    if (count > 0) {
        io_uring_submit(&ring_);
    }
}
void IoUringExecutor::PollLoop() {
    struct io_uring_cqe* cqe;
    
    while (running_) {
        // 阻塞等待
        int ret = io_uring_wait_cqe(&ring_, &cqe);
        
        if (ret < 0) {
             // 错误处理，通常退出
             if (!running_) break;
             continue;
        }
        
        if (io_uring_cqe_get_data(cqe) == nullptr) {
            // 可能是 NOP 用于唤醒
            io_uring_cqe_seen(&ring_, cqe);
            continue;
        }

        RequestContext* ctx = reinterpret_cast<RequestContext*>(io_uring_cqe_get_data(cqe));
        if (ctx) {
            ctx->callback(cqe->res);
            delete ctx;
        }

        io_uring_cqe_seen(&ring_, cqe);
    }
}

} // namespace titankv