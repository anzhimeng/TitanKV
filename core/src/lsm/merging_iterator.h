#pragma once

#include "lsm/block.h" // 借用基类 Iterator
#include "lsm/dbformat.h"

namespace titankv {

// 工厂方法：创建一个多路归并迭代器
// comparator: 用于比较 Key 的大小
// children: 子迭代器数组
// n: 子迭代器数量
// 注意：返回的 Iterator 析构时，会负责 delete 所有的 children
Iterator* NewMergingIterator(const InternalKeyComparator* comparator, Iterator** children, int n);

} // namespace titankv