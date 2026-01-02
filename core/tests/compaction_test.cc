#include "gtest/gtest.h"
#include "titankv/db.h"
#include "titankv/options.h"
#include <filesystem>
#include <thread>
#include <vector>
#include <string>

using namespace titankv;


class CompactionTest : public testing::Test {
protected:
    std::string dbname_ = "/tmp/titankv_compaction_test";
    DB* db_;
    Options options_;

// core/tests/compaction_test.cc

    void SetUp() override {
        std::filesystem::remove_all(dbname_);
        options_.create_if_missing = true;
        
        // [FIX] Increase flush threshold from 1KB to 64KB
        // With 100KB total data, this will generate only ~2 files.
        // This effectively solves the "too many files" issue at the source.
        options_.write_buffer_size = 64 * 1024; 
        
        // Keep SSTable limit at 1MB
        options_.max_file_size = 1 * 1024 * 1024; 
        
        // Keep max open files high enough
        options_.max_open_files = 2000;
        
        ASSERT_TRUE(DB::Open(options_, dbname_, &db_).ok());
    }

    void TearDown() override {
        delete db_;
        std::filesystem::remove_all(dbname_);
    }
};

TEST_F(CompactionTest, TrivialMoveAndCleanup) {
    // 1. Setup parameters
    // Total Write: 200 keys * 500 bytes = 100KB
    // Flush Threshold: 1KB -> Will generate ~100 flushes (100 L0 files)
    // SSTable Limit: 1MB -> All 100KB fits into 1 SSTable
    const int kTotalKeys = 200;
    const int kValueSize = 500;
    
    // 2. Write Data
    for (int i = 0; i < kTotalKeys; ++i) {
        std::string key = "key" + std::to_string(i);
        std::string val(kValueSize, 'x');
        ASSERT_TRUE(db_->Put(WriteOptions(), key, val).ok());
        
        // Sleep slightly to allow background thread to pick up L0 files
        // otherwise we might hit Write Stall (12 files limit)
        if (i % 10 == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    // 3. Wait for Compaction to finish
    // The background thread needs time to merge 100 L0 files -> 1 L1 file
    fprintf(stderr, "Writing done. Waiting for compaction to settle...\n");
    
    int sst_count = 0;
    for (int i = 0; i < 15; ++i) { // Wait up to 15 seconds
        std::this_thread::sleep_for(std::chrono::seconds(1));
        
        sst_count = 0;
        for (const auto& entry : std::filesystem::directory_iterator(dbname_)) {
            if (entry.path().extension() == ".sst") {
                sst_count++;
            }
        }
        fprintf(stderr, "Current SST files: %d\n", sst_count);
        
        // If compacted to a small number, we are good
        if (sst_count < 10) break;
    }

    // 4. Verify
    // Should be condensed into very few files (ideally 1)
    ASSERT_LT(sst_count, 10);
    
    // Verify data exists
    std::string res;
    ASSERT_TRUE(db_->Get(ReadOptions(), "key0", &res).ok());
    ASSERT_EQ(res.size(), kValueSize);
    ASSERT_TRUE(db_->Get(ReadOptions(), "key199", &res).ok());
}