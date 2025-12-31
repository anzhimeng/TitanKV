#include "gtest/gtest.h"
#include "lsm/version_set.h"
#include "lsm/table_builder.h"
#include "lsm/table_cache.h"
#include "util/filename.h"
#include "util/env.h"
#include "util/coding.h"
#include <filesystem>

using namespace titankv;

class LevelIteratorTest : public testing::Test {
protected:
    std::string dbname_ = "/tmp/titankv_level_iter_test";
    Options options_;
    TableCache* cache_;
    InternalKeyComparator icmp_;

    void SetUp() override {
        std::filesystem::remove_all(dbname_);
        std::filesystem::create_directories(dbname_);
        options_.block_restart_interval = 16;
        cache_ = new TableCache(dbname_, options_);
    }

    void TearDown() override {
        delete cache_;
        std::filesystem::remove_all(dbname_);
    }

    void CreateSSTable(uint64_t file_num, const std::string& start_key, const std::string& end_key) {
        std::string fname = TableFileName(dbname_, file_num);
        std::unique_ptr<WritableFile> file;
        NewWritableFile(fname, &file);
        TableBuilder builder(options_, file.get());

        // 写入两个 KV：start_key 和 end_key
        // 注意：Internal Key 格式
        std::string k1 = start_key; PutFixed64(&k1, (100 << 8) | kTypeValue);
        std::string k2 = end_key;   PutFixed64(&k2, (100 << 8) | kTypeValue);

        builder.Add(k1, "val1");
        builder.Add(k2, "val2");
        builder.Finish();
        file->Close();
    }
    
    FileMetaData* MakeMeta(uint64_t file_num, const std::string& smallest, const std::string& largest) {
        FileMetaData* f = new FileMetaData();
        f->file_number = file_num;
        f->file_size = std::filesystem::file_size(TableFileName(dbname_, file_num));
        
        std::string k1 = smallest; PutFixed64(&k1, (100 << 8) | kTypeValue);
        std::string k2 = largest;  PutFixed64(&k2, (100 << 8) | kTypeValue);
        f->smallest = k1;
        f->largest = k2;
        return f;
    }
};

TEST_F(LevelIteratorTest, IterateOverFiles) {
    // 构造 3 个不重叠的文件 (模拟 L1)
    // File 1: a -> b
    // File 2: d -> e
    // File 3: g -> h
    CreateSSTable(1, "a", "b");
    CreateSSTable(2, "d", "e");
    CreateSSTable(3, "g", "h");

    std::vector<FileMetaData*> files;
    files.push_back(MakeMeta(1, "a", "b"));
    files.push_back(MakeMeta(2, "d", "e"));
    files.push_back(MakeMeta(3, "g", "h"));

    Iterator* iter = NewLevelIterator(icmp_, cache_, files, ReadOptions());

    iter->SeekToFirst();
    
    // File 1
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(ExtractUserKey(iter->key()).ToString(), "a");
    iter->Next();
    ASSERT_EQ(ExtractUserKey(iter->key()).ToString(), "b");
    iter->Next();

    // File 2 (跨文件自动跳转)
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(ExtractUserKey(iter->key()).ToString(), "d");
    iter->Next();
    ASSERT_EQ(ExtractUserKey(iter->key()).ToString(), "e");
    iter->Next();
    
    // File 3
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(ExtractUserKey(iter->key()).ToString(), "g");
    
    // Seek 中间
    iter->Seek(Slice(MakeMeta(2, "d", "d")->smallest)); // Seek "d"
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(ExtractUserKey(iter->key()).ToString(), "d");

    delete iter;
    for(auto f : files) delete f;
}