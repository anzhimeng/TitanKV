package store

/*
#cgo CFLAGS: -I../../core/include
#cgo LDFLAGS: -L../../build/core -ltitankv -L../../build/third_party/crc32c -lcrc32c -lstdc++
#include <stdlib.h>
#include "titan_c.h"
*/
import "C"
import (
	"errors"
	"unsafe"
)

type TitanStore struct {
	db *C.titan_db_t
}

func Open(path string) (*TitanStore, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var cErr *C.char
	db := C.titan_open(cPath, &cErr)

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