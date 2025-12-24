#pragma once
#include <string>
#include <cstdint>

namespace titankv {

// 生成 SSTable 文件名: dbname/000001.sst
std::string TableFileName(const std::string& dbname, uint64_t number);

// 生成 WAL 文件名: dbname/000001.log
std::string LogFileName(const std::string& dbname, uint64_t number);

// 生成 Manifest 文件名: dbname/MANIFEST-000001
std::string ManifestFileName(const std::string& dbname, uint64_t number);

// 生成 Current 文件名 (指向当前的 Manifest): dbname/CURRENT
std::string CurrentFileName(const std::string& dbname);


} // namespace titankv