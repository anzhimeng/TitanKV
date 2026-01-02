#include "gtest/gtest.h"
#include "lsm/version_set.h"
#include "lsm/table_builder.h"
#include "lsm/table_cache.h"
#include "lsm/compaction.h"
#include "util/filename.h"
#include "util/env.h"
#include "util/coding.h"
#include <filesystem>

using namespace titankv;

class CompactionIterTest : public testing::Test {
protected:
    std::string dbname_ = "/tmp/titankv_compact_iter_test";
    Options options_;
    TableCache* cache_;
    VersionSet* versions_;

    void SetUp() override {
        std::filesystem::remove_all(dbname_);
        std::filesystem::create_directories(dbname_);
        options_.block_restart_interval = 16;
        cache_ = new TableCache(dbname_, options_);
        versions_ = new VersionSet(dbname_, options_);
    }

    void TearDown() override {
        delete versions_;
        delete cache_;
        std::filesystem::remove_all(dbname_);
    }

    // 辅助：生成 SSTable 并返回 Meta
    FileMetaData* CreateSSTable(uint64_t file_num, const std::string& start_k, const std::string& end_k) {
        std::string fname = TableFileName(dbname_, file_num);
        std::unique_ptr<WritableFile> file;
        NewWritableFile(fname, &file);
        TableBuilder builder(options_, file.get());

        // 写入 Key (InternalKey)
        std::string k1 = start_k; PutFixed64(&k1, (100 << 8) | kTypeValue);
        std::string k2 = end_k;   PutFixed64(&k2, (100 << 8) | kTypeValue);

        builder.Add(k1, "val");
        builder.Add(k2, "val");
        builder.Finish();
        file->Close();

        FileMetaData* f = new FileMetaData();
        f->file_number = file_num;
        f->file_size = std::filesystem::file_size(fname);
        f->smallest = k1;
        f->largest = k2;
        return f;
    }
};

TEST_F(CompactionIterTest, MergeL0AndL1) {
    // 构造 Compaction 对象
    // Level 0 -> Level 1
    Options *opt;
    Compaction c(opt, 0, versions_->current());

    // L0: 两个重叠文件
    // File 1: "a" ... "c"
    // File 2: "b" ... "d"
    c.inputs(0)->push_back(CreateSSTable(1, "a", "c"));
    c.inputs(0)->push_back(CreateSSTable(2, "b", "d"));

    // L1: 一个不重叠文件
    // File 3: "e" ... "g"
    c.inputs(1)->push_back(CreateSSTable(3, "e", "g"));

    // 创建 Input Iterator
    Iterator* iter = versions_->MakeInputIterator(&c, cache_, ReadOptions());
    
    // 预期顺序: a, b, c, d, e, g
    std::vector<std::string> expected = {"a", "b", "c", "d", "e", "g"};
    
    iter->SeekToFirst();
    int i = 0;
    while(iter->Valid()) {
        Slice key = iter->key();
        // 提取 User Key
        std::string user_key(key.data(), key.size() - 8);
        
        ASSERT_LT(i, expected.size());
        ASSERT_EQ(user_key, expected[i]);
        
        // fprintf(stderr, "Got: %s\n", user_key.c_str());
        
        iter->Next();
        i++;
    }
    ASSERT_EQ(i, expected.size());

    delete iter;
    // 清理 FileMetaData (实际由 Version 管理，这里手动清理)
    for (auto* f : *c.inputs(0)) delete f;
    for (auto* f : *c.inputs(1)) delete f;
}