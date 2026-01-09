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