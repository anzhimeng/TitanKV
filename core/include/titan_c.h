#pragma once

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// 定义一个不透明指针 (Opaque Pointer)，Go 只需要持有它，不需要知道内部结构
typedef struct titan_db_t titan_db_t;

typedef struct {
    bool create_if_missing;
    bool use_direct_io; // 【新增】
} titan_options_t;

// 修改 open 接口，接收 options
titan_db_t* titan_open(const char* name, const titan_options_t* options, char** err);

// 关闭数据库
void titan_close(titan_db_t* db);

// 写入
void titan_put(titan_db_t* db, const char* key, size_t klen, 
               const char* val, size_t vlen, char** err);

// 读取
// val: 输出参数，指向数据(malloc分配)，调用者需释放
// vlen: 输出参数，数据长度
void titan_get(titan_db_t* db, const char* key, size_t klen, 
               char** val, size_t* vlen, char** err);

// 删除
void titan_delete(titan_db_t* db, const char* key, size_t klen, char** err);

// 释放由 titan_get 或 错误信息 返回的字符串内存
void titan_free(void* ptr);

typedef struct {
    uint64_t blob_bytes_written;
    uint64_t blob_bytes_read;
    uint64_t gc_run_count;
    uint64_t gc_bytes_reclaimed;
    uint64_t gc_keys_moved;
} titan_stats_t;

// 获取统计信息
void titan_get_statistics(titan_db_t* db, titan_stats_t* stats);

// 【新增】
void titan_set_gc_threshold(titan_db_t* db, double threshold);

// 批量写入接口
// keys: 指针数组，每个指针指向一个 key 字符串
// klen: 数组，存储每个 key 的长度
// vals: 指针数组，每个指针指向一个 value 字符串
// vlen: 数组，存储每个 value 的长度
// count: 批次中 KV 对的数量
void titan_batch_write(titan_db_t* db, 
                       const char** keys, size_t* klen, 
                       const char** vals, size_t* vlen, 
                       int count, char** err);
void titan_get_approximate_sizes(titan_db_t* db, 
                       const char** start_keys, size_t* start_lens,
                       const char** end_keys, size_t* end_lens,
                        int n, uint64_t* sizes);
void titan_ingest_sst(titan_db_t* db, const char* path, char** err);
void titan_delete_range(titan_db_t* db, const char* start, size_t slen, 
                        const char* end, size_t elen, char** err);
void titan_dump_sst(titan_db_t* db, const char* start, size_t slen,
                    const char* end, size_t elen,
                    const char* path, char** err);                        

#ifdef __cplusplus
}
#endif