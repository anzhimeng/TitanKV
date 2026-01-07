#include "gtest/gtest.h"
#include "titankv/db.h"
#include "titankv/options.h"
#include <iostream>

using namespace titankv;

TEST(Debug, ScanAll) {
    std::string dbname = "/tmp/node1"; // 指向你的数据目录
    DB* db;
    Options options;
    Status s = DB::Open(options, dbname, &db);
    ASSERT_TRUE(s.ok());

    // 这是一个 hack：我们需要一个 Iterator 来遍历全库
    // 假设 DBImpl 暴露了 NewIterator，或者我们用 internal 的 TableCache
    // 简单点：直接用 DBImpl 的 Debug 接口 (如果有的话)
    
    // 或者，我们在 DB 接口加一个 ScanAll
    // 这里我们直接用 DBImpl 的 Recover 逻辑改写一个遍历器
    // ...
    
    // 既然我们没有 Scan 接口，最简单的方法是：
    // 在 DBImpl::Put 中打印日志！
    // "Writing Key: [7a 00 00 00 00 00 00 00 01 68 65 6c 6c 6f]"
}