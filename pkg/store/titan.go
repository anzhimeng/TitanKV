package store

/*
#cgo CFLAGS: -I../../core/include
#cgo LDFLAGS: -L../../build/core -ltitankv -L../../build/third_party/liburing -luring -L../../build/third_party/crc32c -lcrc32c -lstdc++
#include <stdlib.h>
#include "titan_c.h"
*/
import "C"
import (
	"errors"
	"unsafe"
	"path/filepath"
	"strings"

	"titankv/api/titankvpb"
)

var ErrKeyNotFound = errors.New("key not found")

type CFType int

const (
	CFDefault CFType = 0
	CFLock    CFType = 1
	CFWrite   CFType = 2
)

type Statistics struct {
	BlobBytesWritten uint64
	BlobBytesRead    uint64
	GCRunCount       uint64
	GCBytesReclaimed uint64
	GCKeysMoved      uint64
}

func (s *TitanStore) GetStatistics() *Statistics {
	var cStats C.titan_stats_t
	C.titan_get_statistics(s.db, &cStats)

	return &Statistics{
		BlobBytesWritten: uint64(cStats.blob_bytes_written),
		BlobBytesRead:    uint64(cStats.blob_bytes_read),
		GCRunCount:       uint64(cStats.gc_run_count),
		GCBytesReclaimed: uint64(cStats.gc_bytes_reclaimed),
		GCKeysMoved:      uint64(cStats.gc_keys_moved),
	}
}

type TitanStore struct {
	db *C.titan_db_t
	path string
}

// 修改 Open 函数，增加 directIO 参数
func Open(path string, useDirectIO bool) (*TitanStore, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

    // 构造 C 结构体
	cOpts := C.titan_options_t{
		create_if_missing: C.bool(true),
		use_direct_io:     C.bool(useDirectIO),
	}

	var cErr *C.char
    // 传入 &cOpts
	db := C.titan_open(cPath, &cOpts, &cErr)

	if cErr != nil {
		defer C.titan_free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}

	return &TitanStore{
		db:   db,
		path: path, // 【修复】赋值
	}, nil
}

func (s *TitanStore) Close() {
	if s.db != nil {
		C.titan_close(s.db)
		s.db = nil
	}
}

func (s *TitanStore) Put(key, value []byte) error {
	var cErr *C.char
	
	// 注意：如果 key/value 为空，&key[0] 会 panic，需要处理
	var kPtr, vPtr *C.char
	if len(key) > 0 { kPtr = (*C.char)(unsafe.Pointer(&key[0])) }
	if len(value) > 0 { vPtr = (*C.char)(unsafe.Pointer(&value[0])) }

	C.titan_put(s.db, kPtr, C.size_t(len(key)), vPtr, C.size_t(len(value)), &cErr)

	if cErr != nil {
		defer C.titan_free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

func (s *TitanStore) Get(key []byte) ([]byte, error) {
	var cErr *C.char
	var cVal *C.char
	var cLen C.size_t

	var kPtr *C.char
	if len(key) > 0 { kPtr = (*C.char)(unsafe.Pointer(&key[0])) }

	C.titan_get(s.db, kPtr, C.size_t(len(key)), &cVal, &cLen, &cErr)

	if cErr != nil {
		defer C.titan_free(unsafe.Pointer(cErr))
		errStr := C.GoString(cErr)
        	// C++ Status::NotFound() 通常返回 "NotFound: ..."
        	if strings.Contains(errStr, "NotFound") {
            return nil, ErrKeyNotFound
        	}
        	return nil, errors.New(errStr)
	}

	// 如果找到了数据
	if cVal != nil {
		// GoBytes 会拷贝数据，这很安全
		val := C.GoBytes(unsafe.Pointer(cVal), C.int(cLen))
		// 必须释放 C++ malloc 的内存
		C.titan_free(unsafe.Pointer(cVal))
		return val, nil
	}
    
    // cVal == nil 且 cErr == nil ? 这种情况对应我们的 C++ 实现其实是不会发生的
    // 因为 C++ 中 NotFound 会返回 error
    return nil, errors.New("key not found") 
}

func (s *TitanStore) Delete(key []byte) error {
    var cErr *C.char
    var kPtr *C.char
	if len(key) > 0 { kPtr = (*C.char)(unsafe.Pointer(&key[0])) }

    C.titan_delete(s.db, kPtr, C.size_t(len(key)), &cErr)
    if cErr != nil {
		defer C.titan_free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
    return nil
}

// BatchPut 批量写入
// 修复点：将指针数组分配在 C 堆上，避免 "Go pointer to unpinned Go pointer"
func (s *TitanStore) BatchPut(keys [][]byte, values [][]byte) error {
	count := len(keys)
	if count == 0 {
		return nil
	}
	if len(values) != count {
		return errors.New("keys and values length mismatch")
	}

	// ==========================================
	// 1. 在 C 堆上分配 指针数组 和 长度数组
	// ==========================================
	// 我们需要 char** keys, size_t* kLens, char** vals, size_t* vLens

	// 计算指针大小 (通常是 8 字节) 和 size_t 大小
	ptrSize := unsafe.Sizeof(uintptr(0))
	sizeTSize := unsafe.Sizeof(C.size_t(0))

	// 分配 C 内存 (这就完全脱离了 Go GC 的管控范围，安全了)
	cKeysArr := C.malloc(C.size_t(count) * C.size_t(ptrSize))
	defer C.free(cKeysArr)

	cKLensArr := C.malloc(C.size_t(count) * C.size_t(sizeTSize))
	defer C.free(cKLensArr)

	cValsArr := C.malloc(C.size_t(count) * C.size_t(ptrSize))
	defer C.free(cValsArr)

	cVLensArr := C.malloc(C.size_t(count) * C.size_t(sizeTSize))
	defer C.free(cVLensArr)

	// ==========================================
	// 2. 将 C 数组转为 Go 切片方便赋值
	// ==========================================
	// 这是一个常用的 CGO 技巧：把 C 指针强转为大数组指针，然后切片
	// 注意：这里没有分配新内存，只是为了能用 slice[i] = ... 语法
	// maxLen := 1 << 30 // 2GB limit for safety check
	
	cKeysSlice := (*[1 << 30]*C.char)(cKeysArr)[:count:count]
	cKLensSlice := (*[1 << 30]C.size_t)(cKLensArr)[:count:count]
	
	cValsSlice := (*[1 << 30]*C.char)(cValsArr)[:count:count]
	cVLensSlice := (*[1 << 30]C.size_t)(cVLensArr)[:count:count]

	// 用于收集需要释放的数据块 (CBytes 分配的内存需要手动 free)
	toFree := make([]unsafe.Pointer, 0, count*2)
	defer func() {
		for _, ptr := range toFree {
			C.free(ptr)
		}
	}()

	// ==========================================
	// 3. 填充数据 (使用 C.CBytes 复制数据)
	// ==========================================
	for i := 0; i < count; i++ {
		// 处理 Key
		if len(keys[i]) > 0 {
			// C.CBytes 会在 C 堆上申请内存并拷贝数据
			kPtr := C.CBytes(keys[i])
			toFree = append(toFree, kPtr) // 记下来，最后释放
			cKeysSlice[i] = (*C.char)(kPtr)
		} else {
			cKeysSlice[i] = nil
		}
		cKLensSlice[i] = C.size_t(len(keys[i]))

		// 处理 Value
		if len(values[i]) > 0 {
			vPtr := C.CBytes(values[i])
			toFree = append(toFree, vPtr)
			cValsSlice[i] = (*C.char)(vPtr)
		} else {
			cValsSlice[i] = nil
		}
		cVLensSlice[i] = C.size_t(len(values[i]))
	}

	var cErr *C.char

	// ==========================================
	// 4. 调用 C 接口
	// ==========================================
	// 注意：这里传入的是 C.malloc 分配的地址，绝对安全
	C.titan_batch_write(s.db,
		(**C.char)(cKeysArr), (*C.size_t)(cKLensArr),
		(**C.char)(cValsArr), (*C.size_t)(cVLensArr),
		C.int(count), &cErr)

	if cErr != nil {
		defer C.titan_free(unsafe.Pointer(cErr)) // 假设你有这个释放函数
		return errors.New(C.GoString(cErr))
	}

	return nil
}

func (s *TitanStore) SetGCThreshold(threshold float64) {
    C.titan_set_gc_threshold(s.db, C.double(threshold))
}

// 批量估算大小
func (s *TitanStore) GetApproximateSizes(startKeys, endKeys [][]byte) []uint64 {
	n := len(startKeys)
	if n != len(endKeys) || n == 0 {
		return nil
	}

	// 准备 C 数组
	// 注意：这是 CGO 中处理字符串数组的标准做法
	cStartKeys := make([]*C.char, n)
	cStartLens := make([]C.size_t, n)
	cEndKeys := make([]*C.char, n)
	cEndLens := make([]C.size_t, n)
	cSizes := make([]C.uint64_t, n)

	// 分配内存并填充
	for i := 0; i < n; i++ {
		// CBytes 分配 C 内存，需要 free
		cStartKeys[i] = (*C.char)(C.CBytes(startKeys[i]))
		cStartLens[i] = C.size_t(len(startKeys[i]))
		
		cEndKeys[i] = (*C.char)(C.CBytes(endKeys[i]))
		cEndLens[i] = C.size_t(len(endKeys[i]))
	}

	// 确保释放内存
	defer func() {
		for i := 0; i < n; i++ {
			C.free(unsafe.Pointer(cStartKeys[i]))
			C.free(unsafe.Pointer(cEndKeys[i]))
		}
	}()

	// 调用 C 函数
	// 注意：&cStartKeys[0] 获取的是指向第一个 char* 的指针，即 char**
	C.titan_get_approximate_sizes(s.db, 
		&cStartKeys[0], &cStartLens[0],
		&cEndKeys[0], &cEndLens[0],
		C.int(n), &cSizes[0])

	// 转换结果
	sizes := make([]uint64, n)
	for i := 0; i < n; i++ {
		sizes[i] = uint64(cSizes[i])
	}
	return sizes
}

func (s *TitanStore) DumpSST(start, end []byte, path string) error {
    cPath := C.CString(path)
    defer C.free(unsafe.Pointer(cPath))
    
    var cErr *C.char
    // 调用 C 接口
    var kStart, kEnd *C.char
    if len(start) > 0 { kStart = (*C.char)(unsafe.Pointer(&start[0])) }
    if len(end) > 0 { kEnd = (*C.char)(unsafe.Pointer(&end[0])) }

    C.titan_dump_sst(s.db, kStart, C.size_t(len(start)), 
                     kEnd, C.size_t(len(end)), cPath, &cErr)
                     
    if cErr != nil {
        defer C.titan_free(unsafe.Pointer(cErr))
        return errors.New(C.GoString(cErr))
    }
    return nil
}

func (s *TitanStore) IngestSST(path string) error {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var cErr *C.char
    
    // 【修改】调用 C 接口
	C.titan_ingest_sst(s.db, cPath, &cErr)

	if cErr != nil {
		defer C.titan_free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// 【修复】获取快照目录
func (s *TitanStore) GetSnapDir() string {
	return filepath.Join(s.path, "snap")
}

func (s *TitanStore) DeleteRange(start, end []byte) error {
    var cErr *C.char
    
    var kStart, kEnd *C.char
    if len(start) > 0 { kStart = (*C.char)(unsafe.Pointer(&start[0])) }
    if len(end) > 0 { kEnd = (*C.char)(unsafe.Pointer(&end[0])) }

    C.titan_delete_range(s.db, kStart, C.size_t(len(start)), 
                         kEnd, C.size_t(len(end)), &cErr)

    if cErr != nil {
        defer C.titan_free(unsafe.Pointer(cErr))
        return errors.New(C.GoString(cErr))
    }
    return nil
}

// PutCF 带列族和时间戳的写入
func (s *TitanStore) PutCF(cf CFType, key, value []byte, ts uint64) error {
	var cErr *C.char
	
	var kPtr, vPtr *C.char
	if len(key) > 0 { kPtr = (*C.char)(unsafe.Pointer(&key[0])) }
	if len(value) > 0 { vPtr = (*C.char)(unsafe.Pointer(&value[0])) }

	C.titan_put_cf(s.db, C.titan_cf_t(cf), kPtr, C.size_t(len(key)), 
                   vPtr, C.size_t(len(value)), C.uint64_t(ts), &cErr)

	if cErr != nil {
		defer C.titan_free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// GetCF 带列族和时间戳的读取 (精确匹配)
func (s *TitanStore) GetCF(cf CFType, key []byte, ts uint64) ([]byte, error) {
	var cErr *C.char
	var cVal *C.char
	var cLen C.size_t

	var kPtr *C.char
	if len(key) > 0 { kPtr = (*C.char)(unsafe.Pointer(&key[0])) }

	C.titan_get_cf(s.db, C.titan_cf_t(cf), kPtr, C.size_t(len(key)), C.uint64_t(ts),
                   &cVal, &cLen, &cErr)

	if cErr != nil {
		defer C.titan_free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}

	if cVal != nil {
		val := C.GoBytes(unsafe.Pointer(cVal), C.int(cLen))
		C.titan_free(unsafe.Pointer(cVal))
		return val, nil
	}
    
    return nil, errors.New("key not found") 
}

// DeleteCF 带列族和时间戳的删除
func (s *TitanStore) DeleteCF(cf CFType, key []byte, ts uint64) error {
    var cErr *C.char
    var kPtr *C.char
	if len(key) > 0 { kPtr = (*C.char)(unsafe.Pointer(&key[0])) }

    C.titan_delete_cf(s.db, C.titan_cf_t(cf), kPtr, C.size_t(len(key)), C.uint64_t(ts), &cErr)
    if cErr != nil {
		defer C.titan_free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
    return nil
}

func (s *TitanStore) Prewrite(mutations []*titankvpb.Mutation, primary []byte, startTS uint64, ttl uint64) error {
    // log.Printf("[Store] Invoking CGO titan_mvcc_prewrite...")
    count := len(mutations)
    if count == 0 { return nil }

    // 用于收集所有分配的 C 内存指针，最后统一释放
    // 这一点非常重要，否则会导致严重的内存泄漏
    var ptrsToFree []unsafe.Pointer
    defer func() {
        for _, ptr := range ptrsToFree {
            C.free(ptr)
        }
    }()

    // 1. 分配 C 数组
    // sizeof(titan_mutation_t) 可以在 C 中获取，但在 Go 中我们通常通过 C.titan_mutation_t 访问
    // 我们需要一个 slice 来持有这些 C 结构体，防止 GC
    cMutations := make([]C.titan_mutation_t, count)
    
    // 2. 填充数据
    // 我们需要保持 Go 内存不被回收，直到 C 调用结束。
    // 在这个函数作用域内，Go 指针是安全的（因为传给了 C 函数，Go 运行时会钉住它们？不一定，最好用 C.CString 或者 unsafe）
    // 为了绝对安全和简单，我们这里不做深拷贝，而是直接传指针，因为 C++ 端会拷贝数据。
    // 只要 C++ 端不持有这些指针超过函数返回，就是安全的。
    
    //for i, m := range mutations {
        //cMutations[i].op = C.int(m.Op)
        //if len(m.Key) > 0 {
            //cMutations[i].key = (*C.char)(unsafe.Pointer(&m.Key[0]))
            //cMutations[i].klen = C.size_t(len(m.Key))
        //}
        //if len(m.Value) > 0 {
           // cMutations[i].value = (*C.char)(unsafe.Pointer(&m.Value[0]))
            //cMutations[i].vlen = C.size_t(len(m.Value))
       // }
   // }
    
    // Primary Key
   // var pKey *C.char
    //if len(primary) > 0 {
        // = (*C.char)(unsafe.Pointer(&primary[0]))
    //}

    // 2. 填充数据 (把数据拷贝到 C 内存)
    for i, m := range mutations {
        cMutations[i].op = C.int(m.Op)

        // 处理 Key
        if len(m.Key) > 0 {
            // C.CBytes 会在 C 堆上 malloc 内存并拷贝数据
            cKeyPtr := C.CBytes(m.Key)
            cMutations[i].key = (*C.char)(cKeyPtr)
            cMutations[i].klen = C.size_t(len(m.Key))
            // 加入释放列表
            ptrsToFree = append(ptrsToFree, cKeyPtr)
        } else {
            cMutations[i].key = nil
            cMutations[i].klen = 0
        }

        // 处理 Value
        if len(m.Value) > 0 {
            cValPtr := C.CBytes(m.Value)
            cMutations[i].value = (*C.char)(cValPtr)
            cMutations[i].vlen = C.size_t(len(m.Value))
            // 加入释放列表
            ptrsToFree = append(ptrsToFree, cValPtr)
        } else {
            cMutations[i].value = nil
            cMutations[i].vlen = 0
        }
    }

    // 3. 处理 Primary Key
    var pKey *C.char
    var pKeyLen C.size_t
    if len(primary) > 0 {
        pKeyPtr := C.CBytes(primary)
        pKey = (*C.char)(pKeyPtr)
        pKeyLen = C.size_t(len(primary))
        // 加入释放列表
        ptrsToFree = append(ptrsToFree, pKeyPtr)
    }

    // 3. 调用 C 接口
    var cErr *C.char
	C.titan_mvcc_prewrite(
        s.db, 
        &cMutations[0], 
        C.int(count),
        pKey, 
        pKeyLen,
        C.uint64_t(startTS), 
        C.uint64_t(ttl), 
        &cErr,
    	)

    if cErr != nil {
        defer C.titan_free(unsafe.Pointer(cErr))
        return errors.New(C.GoString(cErr))
    }
    return nil
}

func (s *TitanStore) Commit(keys [][]byte, startTS, commitTS uint64) error {
    count := len(keys)
    if count == 0 { return nil }

    // 1. 分配 C 端的指针数组 (char**) 和长度数组 (size_t*)
    // 这块内存由 C 管理（手动 free），Go GC 不会动它
    cKeysPtr := C.malloc(C.size_t(count) * C.size_t(unsafe.Sizeof(uintptr(0))))
    cLensPtr := C.malloc(C.size_t(count) * C.size_t(unsafe.Sizeof(C.size_t(0))))
    
    defer C.free(cKeysPtr)
    defer C.free(cLensPtr)

    // 将 void* 转换为切片以便操作 (Go trick)
    // 注意：这里需要非常小心，或者我们可以不用切片，直接指针运算
    // 简单起见，我们用 SliceHeader 或者简单的指针偏移
    // 但为了安全，最推荐的做法是：在循环里分别调用 Commit？不，那样失去了 Batch。
    
    // 让我们用更安全的转换方式：
    // 创建一个 Go slice 映射到 C 内存 (为了赋值)
    // 这是一个 unsafe 操作，但只要我们不让 GC 移动它就行。
    // 其实，我们可以用一个临时的 Go slice，然后传给 C？不，报错就是因为 Go slice 里的指针。
    
    // --- 最佳修复：深拷贝 Key 到 C 内存 ---
    // 虽然有拷贝开销，但这绝对安全，且解决了 "Go pointer to unpinned Go pointer"
    
    // 我们需要构建两个 C 数组：
    // char** keys_array
    // size_t* lens_array
    
    // 使用 Slice 辅助构造，最后再复制到 C 内存太麻烦。
    // 我们直接在 C++ 侧改接口？不。
    
    // 我们使用一个临时的 Go slice 存储 C 指针
    tempKeys := make([]*C.char, count)
    tempLens := make([]C.size_t, count)
    
    for i, k := range keys {
        // 深拷贝 Key 到 C 内存
        // C.CBytes 返回的是 unsafe.Pointer，需要转为 *C.char
        // 注意：CBytes 会 malloc，我们需要在函数结束时 free
        p := (*C.char)(C.CBytes(k))
        tempKeys[i] = p
        tempLens[i] = C.size_t(len(k))
    }
    
    // 注册 cleanup
    defer func() {
        for _, p := range tempKeys {
            C.free(unsafe.Pointer(p))
        }
    }()

    // 现在 tempKeys 是一个 Go slice，里面存的是 C 的指针 (非 Go pointer)。
    // 我们可以安全地把 &tempKeys[0] 传给 C 吗？
    // 是的！因为 slice 本身在 Go 栈/堆上，传它的地址给 C 是允许的（只要它里面的元素不是指向 Go 内存的指针）。
    // 现在里面的元素指向的是 C 内存，所以是安全的。

    var cErr *C.char
    C.titan_mvcc_commit(s.db, 
                        &tempKeys[0], // char**
                        &tempLens[0], // size_t*
                        C.int(count),
                        C.uint64_t(startTS), C.uint64_t(commitTS), &cErr)

    if cErr != nil {
        defer C.titan_free(unsafe.Pointer(cErr))
        return errors.New(C.GoString(cErr))
    }
    return nil
}

func (s *TitanStore) MvccGet(key []byte, startTS uint64) ([]byte, error) {
    var cVal *C.char
    var cLen C.size_t
    var cErr *C.char
    
    kPtr := (*C.char)(unsafe.Pointer(&key[0]))
    
    C.titan_mvcc_get(s.db, kPtr, C.size_t(len(key)), C.uint64_t(startTS), &cVal, &cLen, &cErr)
    
    if cErr != nil {
        defer C.titan_free(unsafe.Pointer(cErr))
        return nil, errors.New(C.GoString(cErr))
    }
    
    if cVal != nil {
        val := C.GoBytes(unsafe.Pointer(cVal), C.int(cLen))
        C.titan_free(unsafe.Pointer(cVal))
        return val, nil
    }
    return nil, nil // Should not happen if err is nil
}

// 1. GetLockCF: 获取 Lock 列族的值 (Raw Bytes)
func (s *TitanStore) GetLockCF(key []byte) ([]byte, error) {
    var cVal *C.char
    var cLen C.size_t
    var cErr *C.char
    
    kPtr := (*C.char)(unsafe.Pointer(&key[0]))
    
    // 我们需要 C 接口暴露 titan_get_cf (Week 13 Day 2 应该已经有了)
    // CF_LOCK = 1
    C.titan_get_cf(s.db, 1, kPtr, C.size_t(len(key)), 0, &cVal, &cLen, &cErr)
    
    if cErr != nil {
        defer C.titan_free(unsafe.Pointer(cErr))
        return nil, errors.New(C.GoString(cErr))
    }
    
    if cVal != nil {
        val := C.GoBytes(unsafe.Pointer(cVal), C.int(cLen))
        C.titan_free(unsafe.Pointer(cVal))
        return val, nil
    }
    return nil, errors.New("key not found")
}

// 2. CheckTxnStatus: 检查事务状态
func (s *TitanStore) CheckTxnStatus(primaryKey []byte, lockTS, currentTS uint64) (int, uint64, error) {
    var action C.int
    var commitTS C.uint64_t
    var cErr *C.char
    
    kPtr := (*C.char)(unsafe.Pointer(&primaryKey[0]))
    
    C.titan_check_txn_status(s.db, kPtr, C.size_t(len(primaryKey)), 
                             C.uint64_t(lockTS), C.uint64_t(currentTS),
                             &action, &commitTS, &cErr)
                             
    if cErr != nil {
        defer C.titan_free(unsafe.Pointer(cErr))
        return 0, 0, errors.New(C.GoString(cErr))
    }
    
    return int(action), uint64(commitTS), nil
}