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
		return nil, errors.New(C.GoString(cErr))
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

func (s *TitanStore) SetGCThreshold(threshold float64) {
    C.titan_set_gc_threshold(s.db, C.double(threshold))
}