#include "titan_c.h"
#include "titan_db.h" // Week 2 整理的总入口
#include "titankv/db_impl.h"
#include <cstring>
#include <cstdlib>
#include <thread>

using namespace titankv;

// 内部辅助：将 Status 转换为 char* 错误信息
static void set_error(char** err, const Status& s) {
    if (s.ok()) {
        *err = nullptr;
    } else {
        std::string msg = s.ToString();
        *err = static_cast<char*>(malloc(msg.size() + 1));
        std::strcpy(*err, msg.c_str());
    }
}

extern "C" {

struct titan_db_t {
    DB* rep;
};

titan_db_t* titan_open(const char* name, const titan_options_t* c_opt, char** err) {
    Options options;
    // 转换配置
    if (c_opt) {
        options.create_if_missing = c_opt->create_if_missing;
        options.use_direct_io = c_opt->use_direct_io;
    } else {
        options.create_if_missing = true;
        options.use_direct_io = false;
    }

    // 保留测试能力，可以在 titan_options_t 里加个字段传进来
    // 这里简单处理：默认 false，高性能模式
    
    options.simulate_garbage_generation = false; 
    
    DB* db;
    Status s = DB::Open(options, std::string(name), &db);
    
    if (!s.ok()) {
        set_error(err, s);
        return nullptr;
    }
    
    titan_db_t* wrapper = new titan_db_t;
    wrapper->rep = db;
    *err = nullptr;
    return wrapper;
}

void titan_close(titan_db_t* db) {
    if (db) {
        delete db->rep;
        delete db;
    }
}

void titan_put(titan_db_t* db, const char* key, size_t klen, 
               const char* val, size_t vlen, char** err) {
    Status s = db->rep->Put(WriteOptions(), Slice(key, klen), Slice(val, vlen));
    set_error(err, s);
}

void titan_get(titan_db_t* db, const char* key, size_t klen, 
               char** val, size_t* vlen, char** err) {
    std::string result;
    Status s = db->rep->Get(ReadOptions(), Slice(key, klen), &result);
    
    if (s.ok()) {
        *vlen = result.size();
        // 必须 malloc 内存传给 Go，因为 std::string 出栈就析构了
        *val = static_cast<char*>(malloc(result.size()));
        memcpy(*val, result.data(), result.size());
        *err = nullptr;
    } else {
        *val = nullptr;
        *vlen = 0;
        set_error(err, s);
    }
}

void titan_delete(titan_db_t* db, const char* key, size_t klen, char** err) {
    Status s = db->rep->Delete(WriteOptions(), Slice(key, klen));
    set_error(err, s);
}

void titan_free(void* ptr) {
    if (ptr) free(ptr);
}

void titan_get_statistics(titan_db_t* db, titan_stats_t* stats) {
    if (!db || !db->rep) return;
    
    auto impl = reinterpret_cast<titankv::DBImpl*>(db->rep);
    auto cpp_stats = impl->GetOptions().statistics;

    stats->blob_bytes_read = cpp_stats->blob_bytes_read.load();
    stats->blob_bytes_written = cpp_stats->blob_bytes_written.load();
    stats->gc_run_count = cpp_stats->gc_run_count.load();
    stats->gc_bytes_reclaimed = cpp_stats->gc_bytes_reclaimed.load();
    stats->gc_keys_moved = cpp_stats->gc_keys_moved.load();
}

void titan_set_gc_threshold(titan_db_t* db, double threshold) {
    if (db && db->rep) {
        auto impl = reinterpret_cast<titankv::DBImpl*>(db->rep);
        impl->SetGCThreshold(threshold);
    }
}

} // extern "C"