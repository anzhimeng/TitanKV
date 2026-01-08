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
	"strings"
)

var ErrKeyNotFound = errors.New("key not found")

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

	return &TitanStore{db: db}, nil
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

func (s *TitanStore) GetApproximateSizes(startKeys, endKeys [][]byte) []uint64 {
    n := len(startKeys)
    if n != len(endKeys) || n == 0 { return nil }
    
    // 构造 C 数组 (此处略繁琐，为了演示逻辑)
    // 实际上需要分配 C 内存并拷贝
    // 简单实现：循环调用单次 Get (C++ 锁开销较大，生产环境应 Batch)
    // 但为了 Day 1 跑通，我们先循环调用 CGO 接口，或者就在 CGO 层做循环
    
    // 假设你已经实现了 titan_get_approximate_sizes 的 CGO 绑定
    // ...
    
    // 返回 mock 值用于测试 (Week 11 Day 1 重点是调度逻辑，C++ 估算可以先 Mock)
    // return []uint64{100 * 1024 * 1024} // 100MB
}