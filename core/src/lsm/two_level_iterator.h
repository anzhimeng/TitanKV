#pragma once
#include "lsm/block.h" // 借用 Iterator 基类
#include "titankv/options.h"

namespace titankv {

// 回调函数类型定义
// arg: 用户上下文 (通常是 Table* 或 File*)
// options: 读选项
// handle_value: Index Block 中存储的 BlockHandle 序列化数据
// 返回: Data Block 的迭代器 (调用者拥有所有权)
typedef Iterator* (*BlockFunction)(void* arg, const ReadOptions& options, const Slice& handle_value);

// 工厂方法
Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              BlockFunction block_function,
                              void* arg,
                              const ReadOptions& options);

} // namespace titankv