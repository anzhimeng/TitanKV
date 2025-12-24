#include <iostream>
#include <cassert>
#include "core/include/titan_db.h" // 用户引用总入口

using namespace titankv;

int main() {
    DB* db;
    Options options;
    options.create_if_missing = true;
    
    // 1. 打开数据库
    Status s = DB::Open(options, "/tmp/titankv_example", &db);
    assert(s.ok());

    // 2. 写入数据
    std::string key = "name";
    std::string value = "TitanKV Engine";
    s = db->Put(WriteOptions(), key, value);
    assert(s.ok());
    std::cout << "Put key: " << key << ", value: " << value << std::endl;

    // 3. 读取数据
    std::string res;
    s = db->Get(ReadOptions(), key, &res);
    assert(s.ok());
    std::cout << "Get value: " << res << std::endl;

    // 4. 清理
    delete db;
    return 0;
}