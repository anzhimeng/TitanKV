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

#ifdef __cplusplus
}
#endif