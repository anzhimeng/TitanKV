#pragma once

#include <liburing.h>
#include <functional>
#include <thread>
#include <atomic>
#include <mutex>
#include <vector> 
#include "titankv/status.h"

namespace titankv {

using IoCallback = std::function<void(int)>;

// 【新增】批量读取请求结构体
struct ReadRequest {
    int fd;
    uint64_t offset;
    size_t length;
    char* buf;
    IoCallback callback;
};

class IoUringExecutor {
 public:
  explicit IoUringExecutor(unsigned queue_depth = 128);
  ~IoUringExecutor();

  IoUringExecutor(const IoUringExecutor&) = delete;
  IoUringExecutor& operator=(const IoUringExecutor&) = delete;

  void SubmitRead(int fd, uint64_t offset, size_t length, char* buf, IoCallback callback);

  // 【新增】批量提交接口声明
  void SubmitReadBatch(std::vector<ReadRequest>& requests);

 private:
  struct io_uring ring_;
  std::thread poller_thread_;
  std::atomic<bool> running_;
  std::mutex sq_mutex_;

  void PollLoop();
};

} // namespace titankv