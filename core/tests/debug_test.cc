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

	Options options;
	for (const auto& entry : std::filesystem::directory_iterator(dbname)) {
	        if (entry.path().extension() == ".sst") {
	            std::string fname = entry.path().string();
	            uint64_t fsize = std::filesystem::file_size(fname);
	            
	            // 【新增】解析文件编号 (例如 000005.sst -> 5)
	            std::string filename_only = entry.path().filename().string();
	            uint64_t file_number = 0;
	            try {
	                file_number = std::stoull(filename_only.substr(0, filename_only.length() - 4));
	            } catch (...) {
	                std::cout << "Warning: could not parse file number from " << filename_only << std::endl;
	            }
	
	            std::cout << "Checking " << fname << " (Size: " << fsize << ")... ";
	
	            std::unique_ptr<RandomAccessFile> file;
	            Status s = NewRandomAccessFile(fname, &file);
	            if (!s.ok()) {
	                std::cout << "FAILED to open file: " << s.ToString() << std::endl;
	                continue;
	            }
	
	            Table* table = nullptr;
	            // 【关键修复】传入 file_number (第3个参数)
	            s = Table::Open(options, file.release(), file_number, fsize, &table);
	            
	            if (s.ok()) {
	                std::cout << "OK (Valid SSTable)" << std::endl;
	                delete table;
	            } else {
	                std::cout << "CORRUPTED: " << s.ToString() << std::endl;
	            }
	        }
	    }
}