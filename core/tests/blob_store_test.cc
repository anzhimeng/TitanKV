#include "gtest/gtest.h"
#include "blob/blob_store.h"
#include <filesystem>
#include <vector>
#include <string>

// 【关键修复】引入命名空间，否则找不到 BlobStore/Status
using namespace titankv;

TEST(BlobStoreTest, BasicWrite) {
  // 1. 准备测试目录
  std::string test_dir = "/tmp/titankv_blob_test";
  std::filesystem::remove_all(test_dir); // 清理旧数据
  std::filesystem::create_directory(test_dir);

  // 2. 初始化 BlobStore
  BlobStore store(test_dir);

  // 3. 准备数据
  std::string key = "test_key";
  std::string value(1024, 'a'); // 1KB 的 Value

  // 4. 写入数据
  BlobIndex index;
  Status s = store.Add(key, value, &index);
  
  // 5. 验证结果
  ASSERT_TRUE(s.ok()) << s.ToString(); // 如果失败，打印错误信息
  ASSERT_EQ(index.file_id, 1);         // 第一个文件 ID 应该是 1
  ASSERT_GT(index.size, 1024);         // Size = Header + Key + Value > 1024

  // 6. 验证文件是否生成
  bool file_exists = std::filesystem::exists(test_dir + "/000001.blob");
  ASSERT_TRUE(file_exists);

  // 清理
  std::filesystem::remove_all(test_dir);
}

TEST(BlobStoreTest, MultipleWrites) {
  std::string test_dir = "/tmp/titankv_blob_test_multi";
  std::filesystem::remove_all(test_dir);
  std::filesystem::create_directory(test_dir);

  BlobStore store(test_dir);
  
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