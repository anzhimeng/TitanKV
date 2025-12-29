// core/tests/debug_test.cc

#include "gtest/gtest.h"
#include "lsm/table.h"
#include "util/filename.h"
#include "util/env.h"
#include <filesystem>
#include <iostream>
#include <vector>
#include <algorithm>

using namespace titankv;

// 这个测试不依赖 DBImpl，直接尝试打开磁盘上的 SST 文件
TEST(Debug, InspectSSTables) {
    // 【注意】这里必须是你压测生成的真实数据目录
    // 如果你之前的启动命令是 --db_path=/tmp/node1，就填这个
    std::string dbname = "/tmp/node1"; 
    
    if (!std::filesystem::exists(dbname)) {
        std::cout << "Directory not found: " << dbname << std::endl;
        return;
    }

    std::vector<std::string> sst_files;
    for (const auto& entry : std::filesystem::directory_iterator(dbname)) {
        if (entry.path().extension() == ".sst") {
            sst_files.push_back(entry.path().string());
        }
    }
    
    // 排序，方便看
    std::sort(sst_files.begin(), sst_files.end());

    std::cout << "Found " << sst_files.size() << " SSTable files." << std::endl;

    Options options;
    for (const auto& fname : sst_files) {
        uint64_t fsize = std::filesystem::file_size(fname);
        std::cout << "Checking " << fname << " (Size: " << fsize << ")... ";

        std::unique_ptr<RandomAccessFile> file;
        Status s = NewRandomAccessFile(fname, &file);
        if (!s.ok()) {
            std::cout << "FAILED to open file: " << s.ToString() << std::endl;
            continue;
        }

        Table* table = nullptr;
        // 尝试解析 Footer 和 Index Block
        s = Table::Open(options, file.release(), fsize, &table);
        
        if (s.ok()) {
            std::cout << "OK (Valid SSTable)" << std::endl;
            delete table;
        } else {
            std::cout << "CORRUPTED: " << s.ToString() << std::endl;
            // 如果这里报错 Bad Magic Number，说明文件没写完或者被截断了
        }
    }
}