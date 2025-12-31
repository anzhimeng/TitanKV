#include "gtest/gtest.h"
#include "lsm/block.h"
#include "lsm/dbformat.h"
#include "lsm/table_cache.h"
#include "lsm/table_builder.h"
#include "util/filename.h"
#include "util/coding.h"
#include <vector>
#include <thread>
#include <atomic>
#include <filesystem>

using namespace titankv;

// =========================================================
// 1. 验证 Iterator Cleanup 机制
//    目标：确保 Iterator 析构时，注册的资源（如 Cache Handle）能被释放，否则 OOM。
// =========================================================

// 模拟的 Cleanup 回调
static void MockCleanup(void* arg1, void* arg2) {
    int* counter = reinterpret_cast<int*>(arg1);
    (*counter)++;
}

// 一个简单的 Mock Iterator
class MockIterator : public Iterator {
public:
    bool Valid() const override { return false; }
    void SeekToFirst() override {}
    void SeekToLast() override {}
    void Seek(const Slice& target) override {}
    void Next() override {}
    void Prev() override {}
    Slice key() const override { return Slice(); }
    Slice value() const override { return Slice(); }
    Status status() const override { return Status::OK(); }
};

TEST(PreCompactionCheck, IteratorCleanup) {
    int cleanup_count = 0;
    
    {
        // 作用域开始
        Iterator* iter = new MockIterator();
        
        // 注册两个清理函数
        iter->RegisterCleanup(MockCleanup, &cleanup_count, nullptr);
        iter->RegisterCleanup(MockCleanup, &cleanup_count, nullptr);
        
        // 此时不应执行
        ASSERT_EQ(cleanup_count, 0);
        
        delete iter; 
        // 作用域结束，触发析构
    }
    
    // 验证：析构后，cleanup_count 应该增加 2 次
    ASSERT_EQ(cleanup_count, 2) << "Iterator cleanup not triggered correctly!";
}

// =========================================================
// 2. 验证 InternalKeyComparator 逻辑
//    目标：确保 UserKey 升序，SeqNum 降序 (新数据在前)。
// =========================================================

class ComparatorCheckTest : public testing::Test {
protected:
    InternalKeyComparator cmp;

    std::string MakeKey(const std::string& user_key, uint64_t seq, ValueType type) {
        std::string key = user_key;
        PutFixed64(&key, (seq << 8) | type);
        return key;
    }
};

TEST_F(ComparatorCheckTest, OrderCheck) {
    // Case 1: 不同的 User Key (升序)
    // "a" < "b"
    ASSERT_LT(cmp.Compare(MakeKey("a", 100, kTypeValue), MakeKey("b", 100, kTypeValue)), 0);
    
    // Case 2: 相同的 User Key, 不同的 SeqNum (降序)
    // Seq 200 应该排在 Seq 100 前面 (即 < )
    ASSERT_LT(cmp.Compare(MakeKey("a", 200, kTypeValue), MakeKey("a", 100, kTypeValue)), 0);
    
    // Case 3: 相同的 User Key, 相同的 SeqNum (相等)
    ASSERT_EQ(cmp.Compare(MakeKey("a", 100, kTypeValue), MakeKey("a", 100, kTypeValue)), 0);
    
    // Case 4: 混合测试
    // a@200 < a@100 < b@200
    std::string k1 = MakeKey("a", 200, kTypeValue);
    std::string k2 = MakeKey("a", 100, kTypeValue);
    std::string k3 = MakeKey("b", 200, kTypeValue);
    
    ASSERT_LT(cmp.Compare(k1, k2), 0);
    ASSERT_LT(cmp.Compare(k2, k3), 0);
}

// =========================================================
// 3. 验证 TableCache 并发安全性
//    目标：多线程同时请求同一个文件，不能 Crash，且都能拿到结果。
// =========================================================

TEST(PreCompactionCheck, TableCacheConcurrency) {
    std::string dbname = "/tmp/titankv_precheck";
    std::filesystem::remove_all(dbname);
    std::filesystem::create_directories(dbname);
    
    Options options;
    options.block_restart_interval = 16;
    
    // 1. 先造一个真实的 SSTable 文件 (FileNum = 1)
    std::string fname = TableFileName(dbname, 1);
    {
        std::unique_ptr<WritableFile> file;
        ASSERT_TRUE(NewWritableFile(fname, &file).ok());
        TableBuilder builder(options, file.get());
        
        // 写入一些数据
        std::string key = "key"; 
        PutFixed64(&key, (100 << 8) | kTypeValue);
        builder.Add(key, "val");
        
        ASSERT_TRUE(builder.Finish().ok());
        file->Close();
    }
    uint64_t fsize = std::filesystem::file_size(fname);

    // 2. 初始化 TableCache
    TableCache cache(dbname, options);

    // 3. 启动 20 个线程并发 Get
    std::atomic<int> success_count{0};
    std::vector<std::thread> threads;
    
    for (int i = 0; i < 20; ++i) {
        threads.emplace_back([&]() {
            // 构造一个简单的 Callback
            auto callback = [](void* arg, const Slice& k, const Slice& v) {
                (void)k; (void)v;
            };
            
            // 构造 Key
            std::string key = "key"; 
            PutFixed64(&key, (100 << 8) | kTypeValue);
            
            // 并发调用 Get
            // 注意：Get 内部会调用 FindTable，这里会测试锁的正确性
            Status s = cache.Get(ReadOptions(), 1, fsize, key, nullptr, callback);
            if (s.ok()) {
                success_count++;
            }
        });
    }

    // 等待所有线程结束
    for (auto& t : threads) {
        t.join();
    }

    ASSERT_EQ(success_count, 20) << "Some threads failed to get table!";
    
    std::filesystem::remove_all(dbname);
}