#include "gtest/gtest.h"
#include "titankv/db.h"
#include "titankv/db_impl.h"
#include "util/coding.h"
#include <filesystem>
#include <vector>
#include <string>

using namespace titankv;

class ParallelCommitTest : public testing::Test {
protected:
    std::string dbname_ = "/tmp/titankv_pc_test";
    DB* db_;
    DBImpl* db_impl_;
    Options options_;

    void SetUp() override {
        std::filesystem::remove_all(dbname_);
        options_.create_if_missing = true;
        ASSERT_TRUE(DB::Open(options_, dbname_, &db_).ok());
        db_impl_ = reinterpret_cast<DBImpl*>(db_);
    }

    void TearDown() override {
        delete db_;
        std::filesystem::remove_all(dbname_);
    }
    
    // Helper to decode lock
    void DecodeLock(const std::string& val, std::string* primary, uint64_t* start_ts, 
                    uint64_t* min_commit_ts, std::vector<std::string>* secondaries) {
        Slice input = val;
        
        // Primary Key (Fixed32 Length + Data)
        ASSERT_TRUE(input.size() >= 4);
        uint32_t primary_len = DecodeFixed32(input.data());
        input.remove_prefix(4);
        
        ASSERT_TRUE(input.size() >= primary_len);
        *primary = std::string(input.data(), primary_len);
        input.remove_prefix(primary_len);
        
        ASSERT_TRUE(input.size() >= 8);
        *start_ts = DecodeFixed64(input.data());
        input.remove_prefix(8);
        
        uint64_t ttl;
        ASSERT_TRUE(input.size() >= 8);
        ttl = DecodeFixed64(input.data());
        input.remove_prefix(8);
        
        ASSERT_TRUE(input.size() >= 1);
        char op = input[0];
        input.remove_prefix(1);
        
        // Optional fields logic from db_impl.cc
        // If remaining >= 8, it's min_commit_ts
        if (input.size() >= 8) {
            *min_commit_ts = DecodeFixed64(input.data());
            input.remove_prefix(8);
        } else {
            *min_commit_ts = 0;
        }
        
        // If remaining >= 8, it's for_update_ts
        if (input.size() >= 8) {
            uint64_t for_update_ts;
            for_update_ts = DecodeFixed64(input.data());
            input.remove_prefix(8);
        }
        
        // Secondaries
        if (input.size() > 0) {
            uint32_t count;
            ASSERT_TRUE(GetVarint32(&input, &count));
            for (uint32_t i = 0; i < count; ++i) {
                uint32_t len;
                ASSERT_TRUE(GetVarint32(&input, &len));
                std::string sec;
                if (input.size() >= len) {
                    sec = std::string(input.data(), len);
                    input.remove_prefix(len);
                }
                secondaries->push_back(sec);
            }
        }
    }
};

TEST_F(ParallelCommitTest, LockInfoWithSecondaries) {
    std::vector<Mutation> mutations;
    Mutation m;
    m.op = Mutation::Put;
    m.key = "key1";
    m.value = "val1";
    mutations.push_back(m);

    std::vector<std::string> secondaries = {"sec1", "sec2"};
    
    // MvccPrewrite with secondaries
    // min_commit_ts = 101
    Status s = db_impl_->MvccPrewrite(mutations, "key1", 100, 1000, 101, false, secondaries);
    ASSERT_TRUE(s.ok());

    // Verify Lock CF
    std::string lock_val;
    s = db_->GetCF(kCFLock, "key1", &lock_val, 0);
    ASSERT_TRUE(s.ok());

    // Decode Lock
    std::string primary;
    uint64_t start_ts, min_commit_ts = 0;
    std::vector<std::string> decoded_secs;
    
    DecodeLock(lock_val, &primary, &start_ts, &min_commit_ts, &decoded_secs);
    
    ASSERT_EQ(primary, "key1");
    ASSERT_EQ(start_ts, 100);
    // MinCommitTS should be >= 101. 
    // Logic in MvccPrewrite: if min_commit_ts <= max_ts, set to max_ts + 1.
    // max_ts starts at 0. So it should be 101.
    ASSERT_EQ(min_commit_ts, 101);
    
    ASSERT_EQ(decoded_secs.size(), 2);
    ASSERT_EQ(decoded_secs[0], "sec1");
    ASSERT_EQ(decoded_secs[1], "sec2");
}

TEST_F(ParallelCommitTest, MaxTSUpdate) {
    db_impl_->UpdateMaxTS(200);
    ASSERT_EQ(db_impl_->GetMaxTS(), 200);
    
    std::string val;
    db_impl_->MvccGet("key", 300, &val); // Should update MaxTS to 300
    ASSERT_EQ(db_impl_->GetMaxTS(), 300);
}

TEST_F(ParallelCommitTest, MinCommitTSConstraint) {
    // Set MaxTS to 200
    db_impl_->UpdateMaxTS(200);
    
    std::vector<Mutation> mutations;
    Mutation m;
    m.op = Mutation::Put;
    m.key = "key2";
    m.value = "val2";
    mutations.push_back(m);
    
    // Try to Prewrite with min_commit_ts = 150 ( < MaxTS)
    Status s = db_impl_->MvccPrewrite(mutations, "key2", 100, 1000, 150, false, {});
    ASSERT_TRUE(s.ok());
    
    // Verify Lock's MinCommitTS
    std::string lock_val;
    s = db_->GetCF(kCFLock, "key2", &lock_val, 0);
    ASSERT_TRUE(s.ok());
    
    std::string primary;
    uint64_t start_ts, min_commit_ts = 0;
    std::vector<std::string> decoded_secs;
    
    DecodeLock(lock_val, &primary, &start_ts, &min_commit_ts, &decoded_secs);
    
    // Should be pushed to MaxTS + 1 = 201
    ASSERT_EQ(min_commit_ts, 201);
}
