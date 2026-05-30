#include "gtest/gtest.h"
#include "blob/blob_store.h"
#include "util/io_uring_executor.h"
#include "titankv/db.h"       
#include "titankv/db_impl.h"
#include <filesystem>
#include <vector>

using namespace titankv;

TEST(BlobGCTest, ValidRatioStats) {
    std::string dbname = "/tmp/titankv_gc_test";
    std::filesystem::remove_all(dbname);
    
    Options options;
    options.max_blob_file_size = 1000; // 设定小一点，方便切文件
    
    IoUringExecutor executor;
    BlobStore store(dbname, options, &executor);

    // 1. 写入数据，触发文件切换
    // File 1: [Key1][Key2] ...
    BlobIndex idx1, idx2, idx3;
    store.Add("Key1", std::string(600, 'a'), &idx1); // ~400+ bytes
    store.Add("Key2", std::string(600, 'b'), &idx2); // ~400+ bytes
    // 此时 File 1 大概 800+ bytes
    
    store.Add("Key3", std::string(600, 'c'), &idx3); // Trigger switch to File 2
    // 此时 File 1 应该被 Register 了
    
    // File 1 ID 应该是 1 (如果从1开始)
    uint32_t file_1_id = idx1.file_id;
    
    // 2. 检查初始状态 (应该是 100% valid)
    // 注意：RegisterNewFile 是在切文件时调用的，所以 File 1 应该有了
    double ratio = store.GetValidRatio(file_1_id);
    ASSERT_NEAR(ratio, 1.0, 0.01);

    // 3. 模拟 Key1 被删除/覆盖 (NotifyGarbage)
    // 告诉 BlobStore: idx1 对应的数据是垃圾了
    store.NotifyGarbage(idx1.file_id, idx1.size);

    // 4. 检查 Ratio
    // File 1 总共约 800+，垃圾 400+，有效率应该在 0.5 左右
    ratio = store.GetValidRatio(file_1_id);
    fprintf(stderr, "File 1 Valid Ratio: %.2f\n", ratio);
    ASSERT_LT(ratio, 1.0);
    ASSERT_GT(ratio, 0.0);

    // 5. 模拟 Key2 也被删除
    store.NotifyGarbage(idx2.file_id, idx2.size);
    
    // 6. 检查 Ratio (应该是 0.0)
    ratio = store.GetValidRatio(file_1_id);
    ASSERT_NEAR(ratio, 0.0, 0.01);
    
    std::filesystem::remove_all(dbname);
}

TEST(BlobGCTest, RewriteLogic) {
    // 1. Setup
    std::string dbname = "/tmp/titankv_gc_rewrite";
    std::filesystem::remove_all(dbname);

    Options options; 
    options.max_blob_file_size = 1000;
    IoUringExecutor executor;
    BlobStore store(dbname, options, &executor);

    // 1. 写入数据，生成 File 1
    BlobIndex idx1, idx2;
    store.Add("Key1", std::string(600, 'a'), &idx1);
    store.Add("Key2", std::string(600, 'b'), &idx2);
    // 此时 active writer 是 File 1 (还没切，因为 Add 内部是先判断再写)
    // 再写一个触发切换
    BlobIndex idx3;
    store.Add("Key3", std::string(600, 'c'), &idx3); 
    // 现在 File 1 是 Immutable，File 2 是 Active

    // 2. 标记 Key1 为垃圾
    store.NotifyGarbage(idx1.file_id, idx1.size);

    // 3. 执行 GC
    // 我们模拟一个 LSM 回调：
    auto mock_is_valid = [&](const Slice& key, const BlobIndex& old_idx) {
        // 只有 Key2 是活的
        if (key == "Key2") return true;
        return false; // Key1 已死
    };

    std::vector<GCRecord> new_indexes;
    Status s = store.RunGC(mock_is_valid, &new_indexes);
    
    // 4. 验证
    ASSERT_TRUE(s.ok());
    // 应该只有 Key2 被搬运
    ASSERT_EQ(new_indexes.size(), 1);
    ASSERT_EQ(new_indexes[0].key, "Key2");
    
    // 新的索引应该指向 File 2 (Active File)
    ASSERT_EQ(new_indexes[0].new_index.file_id, 2); 
    // 且数据内容应该正确
    std::string val;
    s = store.Get(new_indexes[0].new_index, &val);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(val, std::string(600, 'b'));

    // 清理
    // ...
}

TEST(BlobGCTest, IntegratedGC) {
    // 1. Setup
    std::string dbname = "/tmp/titankv_gc_integrated";
    std::filesystem::remove_all(dbname);
    Options options;
    options.min_blob_size = 0; // 强制所有 KV 分离
    options.max_blob_file_size = 1024; // 小文件
    
    DB* db;
    ASSERT_TRUE(DB::Open(options, dbname, &db).ok());
    DBImpl* db_impl = dynamic_cast<DBImpl*>(db); // Hack access

    // 2. 构造数据
    // File 1: KeyA, KeyB
    db->Put(WriteOptions(), "KeyA", std::string(600, 'a'));
    db->Put(WriteOptions(), "KeyB", std::string(600, 'b'));
    
    // File 2: KeyC (Trigger switch)
    db->Put(WriteOptions(), "KeyC", std::string(600, 'c'));

    // 3. 用户更新 KeyA -> File 2/3 (Invalidate File 1 `s` KeyA)
    // 此时 File 1 中 KeyA 是垃圾，KeyB 是有效
    db->Put(WriteOptions(), "KeyA", std::string(600, 'A')); 

    // 4. 触发 GC (File 1 valid ratio = 0.5)
    // 预期：KeyB 被搬运到新文件，KeyA 因为被更新过，不会被回滚
    db_impl->GarbageCollect();

    // 5. 验证
    std::string val;
    // KeyA 应该是新的 'A'
    ASSERT_TRUE(db->Get(ReadOptions(), "KeyA", &val).ok());
    ASSERT_EQ(val, std::string(600, 'A'));
    
    // KeyB 应该还在，且内容是 'b'
    ASSERT_TRUE(db->Get(ReadOptions(), "KeyB", &val).ok());
    ASSERT_EQ(val, std::string(600, 'b'));

    delete db;
    std::filesystem::remove_all(dbname);
}