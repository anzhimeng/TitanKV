#include "gtest/gtest.h"
#include "blob/blob_store.h"
#include "util/io_uring_executor.h" // 【关键修复】必须包含，否则 IoUringExecutor 类型不完整
#include "titankv/options.h"        // 【关键修复】必须包含，用于构造 BlobStore
#include <filesystem>
#include <vector>
#include <string>

using namespace titankv;

TEST(BlobStoreTest, BasicWrite) {
  // 1. 准备测试目录
  std::string test_dir = "/tmp/titankv_blob_test";
  std::filesystem::remove_all(test_dir); 
  std::filesystem::create_directory(test_dir);

  // 2. 初始化组件
  IoUringExecutor executor(1024); 
  Options options; // 【新增】

  // 3. 初始化 BlobStore
  // 【修改】传入 options 和 executor
  BlobStore store(test_dir, options, &executor);

  // 4. 准备数据
  std::string key = "test_key";
  std::string value(1024, 'a'); // 1KB

  // 5. 写入
  BlobIndex index;
  Status s = store.Add(key, value, &index);
  
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(index.file_id, 1);
  ASSERT_GT(index.size, 1024);

  // 6. 验证文件生成
  // 注意：文件名格式是 %06u.blob，1 -> 000001.blob
  bool file_exists = std::filesystem::exists(test_dir + "/000001.blob");
  ASSERT_TRUE(file_exists);

  // 7. 读取验证 (顺便测试下 Get)
  std::string res;
  s = store.Get(index, &res);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(res, value);

  std::filesystem::remove_all(test_dir);
}

TEST(BlobStoreTest, MultipleWrites) {
  std::string test_dir = "/tmp/titankv_blob_test_multi";
  std::filesystem::remove_all(test_dir);
  std::filesystem::create_directory(test_dir);

  Options options;
  // 【修改】传入 options (这里不传 executor，测试同步降级路径)
  BlobStore store(test_dir, options);
  
  // 写入 100 次
  for (int i = 0; i < 100; ++i) {
    BlobIndex index;
    Status s = store.Add("key" + std::to_string(i), "value", &index);
    ASSERT_TRUE(s.ok());
  }

  // 验证目录下有 .blob 文件
  int file_count = 0;
  for (const auto & entry : std::filesystem::directory_iterator(test_dir)) {
    if (entry.path().extension() == ".blob") {
      file_count++;
    }
  }
  ASSERT_GT(file_count, 0);

  std::filesystem::remove_all(test_dir);
}