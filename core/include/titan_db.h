#pragma once

// 这是 TitanKV 的对外公共接口聚合文件
// 用户只需 #include "titan_db.h" 即可使用核心功能

#include "titankv/db.h"
#include "titankv/status.h"
#include "titankv/slice.h"
#include "titankv/options.h"

// 注意：不要包含 internal 的头文件 (如 memtable.h, blob_store.h)
// 保持对外接口的纯净性