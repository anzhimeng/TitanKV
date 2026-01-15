package store

/*
#include <stdlib.h>
#include "titan_c.h"
*/
import "C"
import (
	"errors"
	"unsafe"
)

type MvccReader struct {
	ptr unsafe.Pointer
}

func (s *TitanStore) NewMvccReader(ts uint64) *MvccReader {
	ptr := C.titan_mvcc_reader_create(s.db, C.uint64_t(ts))
	return &MvccReader{ptr: ptr}
}

func (r *MvccReader) Close() {
	C.titan_mvcc_reader_destroy(r.ptr)
}

func (r *MvccReader) SeekWrite(key []byte) (uint64, []byte, error) {
	var commitTs C.uint64_t
	var valPtr *C.char
	var valLen C.size_t

    // 【修复】定义并初始化 kPtr, kLen
	kPtr := (*C.char)(unsafe.Pointer(&key[0]))
    kLen := C.size_t(len(key))

	ret := C.titan_mvcc_reader_seek_write(r.ptr, kPtr, kLen, &commitTs, &valPtr, &valLen)
	
    // 注意：Day 3 定义的 C 接口返回值：0 是 OK, -1 是 NotFound
	if ret == 0 {
		val := C.GoBytes(unsafe.Pointer(valPtr), C.int(valLen))
		C.titan_free(unsafe.Pointer(valPtr)) // 使用 titan_free 而不是 free
		return uint64(commitTs), val, nil
	}
	return 0, nil, errors.New("not found")
}