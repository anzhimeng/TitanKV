#include "gtest/gtest.h"
#include "util/io_uring_executor.h"
#include <fcntl.h>
#include <unistd.h>
#include <future>
#include <vector>
#include <filesystem>

using namespace titankv;

TEST(IoUringTest, SimpleRead) {
    // 1. 准备测试文件
    std::string fname = "/tmp/titankv_uring_test.txt";
    std::string content = "Hello, io_uring World!";
    {
        int fd = open(fname.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        ASSERT_GT(fd, 0);
        write(fd, content.data(), content.size());
        close(fd);
    }

    // 2. 启动 Executor
    IoUringExecutor executor;

    // 3. 打开文件用于读取
    int fd = open(fname.c_str(), O_RDONLY);
    ASSERT_GT(fd, 0);

    // 4. 准备异步读取
    std::vector<char> buffer(content.size());
    std::promise<int> promise;
    auto future = promise.get_future();

    // 5. 提交请求
    executor.SubmitRead(fd, 0, content.size(), buffer.data(), 
        [&](int res) {
            // 回调函数在 Poller 线程中执行
            promise.set_value(res);
        }
    );

    // 6. 等待结果 (Future)
    // 这一步证明了我们可以等待异步操作完成
    int bytes_read = future.get();

    // 7. 验证
    ASSERT_EQ(bytes_read, content.size());
    std::string read_str(buffer.begin(), buffer.end());
    ASSERT_EQ(read_str, content);

    close(fd);
    std::filesystem::remove(fname);
}