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
    bool verify_checksums;
    bool fill_cache;
} titan_read_options_t;

typedef struct titan_iterator_t titan_iterator_t;

typedef struct {
    bool create_if_missing;
    bool use_direct_io; // 【新增】
    
    // 【新增】调优参数
    size_t write_buffer_size;       // MemTable 大小
    size_t max_file_size;           // SSTable 文件大小
    size_t max_blob_file_size;      // Blob 文件大小
    size_t min_blob_size;           // Blob 分离阈值
    size_t block_size;              // Block 大小
    size_t block_cache_size;        // Block Cache 大小 (0 表示不使用)
    int bloom_filter_bits;          // Bloom Filter bits per key (0 表示不使用)
    
    size_t wal_sync_bytes;          // WAL 刷盘字节数
    uint64_t wal_sync_interval_ms;  // WAL 刷盘间隔
} titan_options_t;

typedef enum {
    CF_DEFAULT = 0, // 'd'
    CF_LOCK    = 1, // 'l'
    CF_WRITE   = 2  // 'w'
} titan_cf_t;

typedef struct {
    int op; // 0: Put, 1: Delete
    const char* key;
    size_t klen;
    const char* value;
    size_t vlen;
} titan_mutation_t;

// Coprocessor
typedef struct titan_coprocessor_request {
    uint8_t coprocessor_type; // 0=Count, 1=Sum
    const char* start_key;
    size_t start_key_len;
    const char* end_key;
    size_t end_key_len;
    uint64_t start_ts;
    const char* filter_value;
    size_t filter_value_len;
    uint8_t filter_operator; // 0=EQ, 1=NEQ, 2=GT, 3=LT, 4=GE, 5=LE
} titan_coprocessor_request_t;

typedef struct {
    uint64_t count;
    int64_t sum;
    char* error_msg;
} titan_coprocessor_response_t;

extern void titan_coprocessor_execute(titan_db_t* db, 
                                      const titan_coprocessor_request_t* req, 
                                      titan_coprocessor_response_t* resp, 
                                      char** err);
// End Coprocessor

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

// 删除范围 [start, end)
void titan_delete_range(titan_db_t* db, const char* start_key, size_t start_len, 
                        const char* end_key, size_t end_len, char** err);

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
void titan_batch_write_ops(titan_db_t* db, 
                       const char** keys, size_t* klen, 
                       const char** vals, size_t* vlen, 
                       const int* ops, int count, char** err);
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
                    
void titan_put_cf(titan_db_t* db, titan_cf_t cf, const char* key, size_t klen, 
                  const char* val, size_t vlen, uint64_t ts, char** err);

void titan_delete_cf(titan_db_t* db, titan_cf_t cf, const char* key, size_t klen, uint64_t ts, char** err);

void titan_get_cf(titan_db_t* db, titan_cf_t cf, const char* key, size_t klen, uint64_t ts,
                  char** val, size_t* vlen, char** err);    

void* titan_mvcc_reader_create(titan_db_t* db, uint64_t snapshot);
void titan_mvcc_reader_destroy(void* reader);
int titan_mvcc_reader_seek_write(void* reader, const char* key, size_t klen,
                                 uint64_t* commit_ts, char** val, size_t* vlen);
                                 
void titan_mvcc_prewrite(titan_db_t* db, const titan_mutation_t* mutations, int count,
                         const char* primary, size_t plen, uint64_t start_ts, uint64_t ttl, char** err);
void titan_mvcc_prewrite_1pc(titan_db_t* db, const titan_mutation_t* mutations, int count,
                             const char* primary, size_t plen, uint64_t start_ts, 
                             uint64_t commit_ts, uint64_t ttl, char** err);

void titan_mvcc_prewrite_async(titan_db_t* db, const titan_mutation_t* mutations, int count,
                         const char* primary, size_t plen, uint64_t start_ts, uint64_t ttl, 
                         uint64_t min_commit_ts, bool is_pessimistic_lock, 
                         const char** secondaries, const size_t* secondary_lens, int secondary_count,
                         char** err);

void titan_acquire_pessimistic_lock(titan_db_t* db, 
                                    const char** keys, size_t* klen, int count,
                                    const char* primary, size_t plen, 
                                    uint64_t start_ts, uint64_t ttl, 
                                    uint64_t for_update_ts,
                                    bool return_values,
                                    char*** values, size_t** vlens, bool** not_founds,
                                    char** err);

void titan_mvcc_commit(titan_db_t* db, const char** keys, size_t* klens, int count,
                       uint64_t start_ts, uint64_t commit_ts, char** err);
                       
void titan_mvcc_get(titan_db_t* db, const char* key, size_t klen, uint64_t start_ts,
                    char** val, size_t* vlen, char** err);
void titan_check_txn_status(titan_db_t* db, const char* pkey, size_t plen, 
                            uint64_t lock_ts, uint64_t current_ts,
                            int* action, uint64_t* commit_ts, char** err);
void titan_mvcc_gc(titan_db_t* db, uint64_t safe_point, char** err);

// Check for transaction conflicts
// keys: array of keys to check
// klen: array of key lengths
// count: number of keys
// start_ts: transaction start timestamp
// err: output error message (if conflict or other error)
void titan_check_conflict(titan_db_t* db, 
                          const char** keys, size_t* klen, 
                          int count, 
                          uint64_t start_ts, 
                          char** err);

// Iterator
titan_iterator_t* titan_create_iterator(titan_db_t* db, const titan_read_options_t* options, titan_cf_t cf);
void titan_iterator_destroy(titan_iterator_t* iter);
bool titan_iterator_valid(titan_iterator_t* iter);
void titan_iterator_seek_to_first(titan_iterator_t* iter);
void titan_iterator_seek_to_last(titan_iterator_t* iter);
void titan_iterator_seek(titan_iterator_t* iter, const char* key, size_t klen);
void titan_iterator_next(titan_iterator_t* iter);
void titan_iterator_prev(titan_iterator_t* iter);
void titan_iterator_key(titan_iterator_t* iter, const char** key, size_t* klen);
void titan_iterator_value(titan_iterator_t* iter, const char** val, size_t* vlen);
void titan_iterator_status(titan_iterator_t* iter, char** err);

#ifdef __cplusplus
}
#endif
