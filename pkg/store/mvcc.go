package store

/*
#include "titan_c.h"
*/
import "C"

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
    
    ret := C.titan_mvcc_reader_seek_write(r.ptr, kPtr, kLen, &commitTs, &valPtr, &valLen)
    if ret == 0 {
        val := C.GoBytes(unsafe.Pointer(valPtr), C.int(valLen))
        C.free(unsafe.Pointer(valPtr))
        return uint64(commitTs), val, nil
    }
    return 0, nil, errors.New("not found")
}