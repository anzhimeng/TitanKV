#include "util/filename.h"
#include <cstdio>

namespace titankv {

static std::string MakeFileName(const std::string& dbname, uint64_t number, const char* suffix) {
  char buf[100];
  std::snprintf(buf, sizeof(buf), "/%06lu.%s", number, suffix);
  return dbname + buf;
}

std::string TableFileName(const std::string& dbname, uint64_t number) {
  return MakeFileName(dbname, number, "sst");
}

std::string LogFileName(const std::string& dbname, uint64_t number) {
  return MakeFileName(dbname, number, "log");
}

std::string ManifestFileName(const std::string& dbname, uint64_t number) {
  char buf[100];
  std::snprintf(buf, sizeof(buf), "/MANIFEST-%06lu", number);
  return dbname + buf;
}

std::string CurrentFileName(const std::string& dbname) {
  return dbname + "/CURRENT";
}

std::string TempFileName(const std::string& dbname, uint64_t number) {
  return MakeFileName(dbname, number, "dbtmp");
}

} // namespace titankv