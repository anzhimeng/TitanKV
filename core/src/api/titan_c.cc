#include "titan_c.h"
#include "titan_db.h" // Week 2 整理的总入口
#include "titankv/db_impl.h"
#include "titankv/write_batch.h"
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

void titan_batch_write(titan_db_t* db, 
                       const char** keys, size_t* klen, 
                       const char** vals, size_t* vlen, 
                       int count, char** err) {
    if (!db || !db->rep) {
        // 简单的错误处理
        *err = strdup("db is closed or invalid");
        return;
    }

    WriteBatch batch;
    for (int i = 0; i < count; ++i) {
        // 构造 Slice (零拷贝，直接引用 C 传进来的内存)
        Slice key(keys[i], klen[i]);
        Slice value(vals[i], vlen[i]);
        
        // 区分 Put 和 Delete
        // 这里为了简化 C 接口，假设如果是 Delete，vals[i] 传 nullptr 或者 vlen[i] == 0 且有一个标记数组？
        // 既然 titankv.proto 里 Put 和 Delete 是分开的 RPC，
        // 而 PeerStorage::Append 产生的都是 Put (Raft Log 持久化)。
        // Raft 状态机的 Apply 才是真正的 Put/Delete。
        
        // 【注意】目前的 handleReady 中，我们是把 Raft Log Entry 写入 DB。
        // Raft Log 的 Key 是 `r{RegionID}_{Index}`，Value 是 Entry 的序列化数据。
        // 这是一个 Put 操作。
        
        // 如果你需要支持 Delete（比如 Apply 阶段的 Delete），C 接口需要一个 op_type 数组。
        // 但 handleReady 只做 Log Append (Put)。
        // 所以我们默认全部是 Put。
        
        batch.Put(key, value);
    }

    Status s = db->rep->Write(WriteOptions(), &batch);
    set_error(err, s);
}

void titan_get_approximate_sizes(titan_db_t* db, 
                                 const char** start_keys, size_t* start_lens,
                                 const char** end_keys, size_t* end_lens,
                                 int n, uint64_t* sizes) {
    std::vector<Range> ranges(n);
    for (int i=0; i<n; ++i) {
        ranges[i].start = Slice(start_keys[i], start_lens[i]);
        ranges[i].limit = Slice(end_keys[i], end_lens[i]);
    }
    
    db->rep->GetApproximateSizes(ranges.data(), n, sizes);
}

void titan_ingest_sst(titan_db_t* db, const char* path, char** err) {
    if (!db || !db->rep) {
        // Handle null db error if needed
        return;
    }
   
    auto impl = reinterpret_cast<titankv::DBImpl*>(db->rep);
    
    titankv::Status s = impl->IngestSST(std::string(path));
    set_error(err, s);
}

void titan_dump_sst(titan_db_t* db, const char* start, size_t slen,
                    const char* end, size_t elen,
                    const char* path, char** err) {
    if (!db || !db->rep) {
        // set error
        return;
    }
    auto impl = reinterpret_cast<titankv::DBImpl*>(db->rep);
    
    titankv::Status s = impl->DumpSST(
        titankv::Slice(start, slen), 
        titankv::Slice(end, elen), 
        std::string(path), 
        0
    );
    
    set_error(err, s);
}
} // extern "C"