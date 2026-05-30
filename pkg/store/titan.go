package store

/*
#cgo CFLAGS: -I../../core/include
#cgo LDFLAGS: -L../../core/build -ltitankv -L../../core/build/liburing_build -luring -L../../core/build/crc32c_build -lcrc32c -lstdc++
#include <stdlib.h>
#include "titan_c.h"
*/
import "C"
import (
	"errors"
	"path/filepath"
	"strings"
	"unsafe"

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

func (s *TitanStore) CheckConflict(keys [][]byte, startTS uint64) error {
	count := len(keys)
	if count == 0 {
		return nil
	}

	// 1. Prepare Keys (C Heap)
	ptrSize := unsafe.Sizeof(uintptr(0))
	sizeTSize := unsafe.Sizeof(C.size_t(0))

	cKeysArr := C.malloc(C.size_t(count) * C.size_t(ptrSize))
	defer C.free(cKeysArr)
	cKLensArr := C.malloc(C.size_t(count) * C.size_t(sizeTSize))
	defer C.free(cKLensArr)

	cKeysSlice := (*[1 << 30]*C.char)(cKeysArr)[:count:count]
	cKLensSlice := (*[1 << 30]C.size_t)(cKLensArr)[:count:count]

	// Flatten keys
	totalKeySize := 0
	for _, k := range keys {
		totalKeySize += len(k)
	}

	var keysBlock unsafe.Pointer
	if totalKeySize > 0 {
		keysBlock = C.malloc(C.size_t(totalKeySize))
		defer C.free(keysBlock)
	}

	offset := uintptr(0)
	for i, k := range keys {
		l := len(k)
		if l > 0 {
			dest := unsafe.Pointer(uintptr(keysBlock) + offset)
			// Use unsafe.Slice for copy
			destSlice := unsafe.Slice((*byte)(dest), l)
			copy(destSlice, k)

			cKeysSlice[i] = (*C.char)(dest)
			offset += uintptr(l)
		} else {
			cKeysSlice[i] = nil
		}
		cKLensSlice[i] = C.size_t(l)
	}

	var cErr *C.char
	C.titan_check_conflict(s.db, (**C.char)(cKeysArr), (*C.size_t)(cKLensArr), C.int(count), C.uint64_t(startTS), &cErr)

	if cErr != nil {
		defer C.titan_free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

func (s *TitanStore) AcquirePessimisticLock(keys [][]byte, primary []byte, startTS, ttl, forUpdateTS uint64, returnValues bool) ([][]byte, []bool, error) {
	count := len(keys)
	if count == 0 {
		return nil, nil, nil
	}

	// 1. Prepare Keys (C Heap)
	ptrSize := unsafe.Sizeof(uintptr(0))
	sizeTSize := unsafe.Sizeof(C.size_t(0))

	cKeysArr := C.malloc(C.size_t(count) * C.size_t(ptrSize))
	defer C.free(cKeysArr)
	cKLensArr := C.malloc(C.size_t(count) * C.size_t(sizeTSize))
	defer C.free(cKLensArr)

	cKeysSlice := (*[1 << 30]*C.char)(cKeysArr)[:count:count]
	cKLensSlice := (*[1 << 30]C.size_t)(cKLensArr)[:count:count]

	// Flatten keys
	totalKeySize := 0
	for _, k := range keys {
		totalKeySize += len(k)
	}

	var keysBlock unsafe.Pointer
	if totalKeySize > 0 {
		keysBlock = C.malloc(C.size_t(totalKeySize))
		defer C.free(keysBlock)
	}

	offset := uintptr(0)
	for i, k := range keys {
		l := len(k)
		if l > 0 {
			dest := unsafe.Pointer(uintptr(keysBlock) + offset)
			// Use unsafe.Slice for copy
			destSlice := unsafe.Slice((*byte)(dest), l)
			copy(destSlice, k)

			cKeysSlice[i] = (*C.char)(dest)
			offset += uintptr(l)
		} else {
			cKeysSlice[i] = nil
		}
		cKLensSlice[i] = C.size_t(l)
	}

	// 2. Prepare Primary Key
	var cPrimary *C.char
	if len(primary) > 0 {
		cPrimary = (*C.char)(C.CBytes(primary))
		defer C.free(unsafe.Pointer(cPrimary))
	}
	cPLen := C.size_t(len(primary))

	// 3. Prepare Output Pointers
	var cOutValues **C.char
	var cOutVLens *C.size_t
	var cOutNotFounds *C.bool
	var cErr *C.char

	// 4. Call C Function
	C.titan_acquire_pessimistic_lock(
		s.db,
		(**C.char)(cKeysArr),
		(*C.size_t)(cKLensArr),
		C.int(count),
		cPrimary,
		cPLen,
		C.uint64_t(startTS),
		C.uint64_t(ttl),
		C.uint64_t(forUpdateTS),
		C.bool(returnValues),
		&cOutValues,
		&cOutVLens,
		&cOutNotFounds,
		&cErr,
	)

	if cErr != nil {
		defer C.titan_free(unsafe.Pointer(cErr))
		return nil, nil, errors.New(C.GoString(cErr))
	}

	// 5. Process Results
	var values [][]byte
	var notFounds []bool

	if returnValues {
		// Only if we asked for values and call succeeded
		// The arrays were malloc-ed by C++, so we must free them.
		defer C.free(unsafe.Pointer(cOutValues))
		defer C.free(unsafe.Pointer(cOutVLens))
		defer C.free(unsafe.Pointer(cOutNotFounds))

		outValuesSlice := (*[1 << 30]*C.char)(unsafe.Pointer(cOutValues))[:count:count]
		outVLensSlice := (*[1 << 30]C.size_t)(unsafe.Pointer(cOutVLens))[:count:count]
		outNotFoundsSlice := (*[1 << 30]C.bool)(unsafe.Pointer(cOutNotFounds))[:count:count]

		values = make([][]byte, count)
		notFounds = make([]bool, count)

		for i := 0; i < count; i++ {
			notFounds[i] = bool(outNotFoundsSlice[i])
			if !notFounds[i] {
				vLen := int(outVLensSlice[i])
				vPtr := outValuesSlice[i]
				if vLen > 0 && vPtr != nil {
					values[i] = C.GoBytes(unsafe.Pointer(vPtr), C.int(vLen))
					// Free individual value buffer allocated by C++
					C.free(unsafe.Pointer(vPtr))
				}
			}
		}
	} else {
		// If not returning values, we might still want to initialize return slices to correct length
		// or just return nil. The signature implies we return them.
		// If returnValues is false, C++ sets output pointers to null (or doesn't touch them if I recall correctly).
		// titan_c.cc: if (s.ok() && return_values) { ... } else { *out_values = nullptr; ... }
		// So we are good.
	}

	return values, notFounds, nil
}



type TitanStore struct {
	db   *C.titan_db_t
	path string
}

type TitanOptions struct {
	CreateIfMissing bool
	UseDirectIO     bool

	// Tuning parameters
	WriteBufferSize   int
	MaxFileSize       int
	MaxBlobFileSize   int
	MinBlobSize       int
	BlockSize         int
	BlockCacheSize    int
	BloomFilterBits   int
	WALSyncBytes      int
	WALSyncIntervalMS uint64
}

func DefaultOptions() TitanOptions {
	return TitanOptions{
		CreateIfMissing: true,
		UseDirectIO:     false,
		// Defaults matching C++ implementation
		WriteBufferSize: 128 * 1024 * 1024,
		MaxFileSize:     64 * 1024 * 1024,
		MaxBlobFileSize: 128 * 1024 * 1024,
		BlockSize:       16 * 1024,
		MinBlobSize:     4096,
		BloomFilterBits: 10,
	}
}

// 修改 Open 函数，支持 TitanOptions
func Open(path string, opts TitanOptions) (*TitanStore, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	// 构造 C 结构体
	cOpts := C.titan_options_t{
		create_if_missing:    C.bool(opts.CreateIfMissing),
		use_direct_io:        C.bool(opts.UseDirectIO),
		write_buffer_size:    C.size_t(opts.WriteBufferSize),
		max_file_size:        C.size_t(opts.MaxFileSize),
		max_blob_file_size:   C.size_t(opts.MaxBlobFileSize),
		min_blob_size:        C.size_t(opts.MinBlobSize),
		block_size:           C.size_t(opts.BlockSize),
		block_cache_size:     C.size_t(opts.BlockCacheSize),
		bloom_filter_bits:    C.int(opts.BloomFilterBits),
		wal_sync_bytes:       C.size_t(opts.WALSyncBytes),
		wal_sync_interval_ms: C.uint64_t(opts.WALSyncIntervalMS),
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
	if len(key) > 0 {
		kPtr = (*C.char)(unsafe.Pointer(&key[0]))
	}
	if len(value) > 0 {
		vPtr = (*C.char)(unsafe.Pointer(&value[0]))
	}

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
	if len(key) > 0 {
		kPtr = (*C.char)(unsafe.Pointer(&key[0]))
	}

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
	if len(key) > 0 {
		kPtr = (*C.char)(unsafe.Pointer(&key[0]))
	}

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
	// 3. 填充数据 (使用 单次分配 + 切片复用)
	// ==========================================
	totalKeySize := 0
	totalValSize := 0
	for i := 0; i < count; i++ {
		totalKeySize += len(keys[i])
		totalValSize += len(values[i])
	}

	var keysBlock, valsBlock unsafe.Pointer
	if totalKeySize > 0 {
		keysBlock = C.malloc(C.size_t(totalKeySize))
		defer C.free(keysBlock)
	}
	if totalValSize > 0 {
		valsBlock = C.malloc(C.size_t(totalValSize))
		defer C.free(valsBlock)
	}

	// 填充 keys
	offset := uintptr(0)
	for i := 0; i < count; i++ {
		l := len(keys[i])
		if l > 0 {
			dest := unsafe.Pointer(uintptr(keysBlock) + offset)
			destSlice := unsafe.Slice((*byte)(dest), l)
			copy(destSlice, keys[i])
			cKeysSlice[i] = (*C.char)(dest)
			offset += uintptr(l)
		} else {
			cKeysSlice[i] = nil
		}
		cKLensSlice[i] = C.size_t(l)
	}

	// 填充 values
	offset = uintptr(0)
	for i := 0; i < count; i++ {
		l := len(values[i])
		if l > 0 {
			dest := unsafe.Pointer(uintptr(valsBlock) + offset)
			destSlice := unsafe.Slice((*byte)(dest), l)
			copy(destSlice, values[i])
			cValsSlice[i] = (*C.char)(dest)
			offset += uintptr(l)
		} else {
			cValsSlice[i] = nil
		}
		cVLensSlice[i] = C.size_t(l)
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

func (s *TitanStore) BatchWriteOps(keys [][]byte, values [][]byte, ops []int) error {
	count := len(keys)
	if count == 0 {
		return nil
	}
	if len(values) != count {
		return errors.New("keys and values length mismatch")
	}
	if len(ops) != count {
		return errors.New("keys and ops length mismatch")
	}

	ptrSize := unsafe.Sizeof(uintptr(0))
	sizeTSize := unsafe.Sizeof(C.size_t(0))
	intSize := unsafe.Sizeof(C.int(0))

	cKeysArr := C.malloc(C.size_t(count) * C.size_t(ptrSize))
	defer C.free(cKeysArr)

	cKLensArr := C.malloc(C.size_t(count) * C.size_t(sizeTSize))
	defer C.free(cKLensArr)

	cValsArr := C.malloc(C.size_t(count) * C.size_t(ptrSize))
	defer C.free(cValsArr)

	cVLensArr := C.malloc(C.size_t(count) * C.size_t(sizeTSize))
	defer C.free(cVLensArr)

	cOpsArr := C.malloc(C.size_t(count) * C.size_t(intSize))
	defer C.free(cOpsArr)

	cKeysSlice := (*[1 << 30]*C.char)(cKeysArr)[:count:count]
	cKLensSlice := (*[1 << 30]C.size_t)(cKLensArr)[:count:count]
	cValsSlice := (*[1 << 30]*C.char)(cValsArr)[:count:count]
	cVLensSlice := (*[1 << 30]C.size_t)(cVLensArr)[:count:count]
	cOpsSlice := (*[1 << 30]C.int)(cOpsArr)[:count:count]

	toFree := make([]unsafe.Pointer, 0, count*2)
	defer func() {
		for _, ptr := range toFree {
			C.free(ptr)
		}
	}()

	totalKeySize := 0
	totalValSize := 0
	for i := 0; i < count; i++ {
		totalKeySize += len(keys[i])
		totalValSize += len(values[i])
	}

	var keysBlock, valsBlock unsafe.Pointer
	if totalKeySize > 0 {
		keysBlock = C.malloc(C.size_t(totalKeySize))
		toFree = append(toFree, keysBlock)
	}
	if totalValSize > 0 {
		valsBlock = C.malloc(C.size_t(totalValSize))
		toFree = append(toFree, valsBlock)
	}

	keyOffset := uintptr(0)
	valOffset := uintptr(0)
	for i := 0; i < count; i++ {
		l := len(keys[i])
		if l > 0 {
			dest := unsafe.Pointer(uintptr(keysBlock) + keyOffset)
			destSlice := unsafe.Slice((*byte)(dest), l)
			copy(destSlice, keys[i])
			cKeysSlice[i] = (*C.char)(dest)
			keyOffset += uintptr(l)
		} else {
			cKeysSlice[i] = nil
		}
		cKLensSlice[i] = C.size_t(l)

		l = len(values[i])
		if l > 0 {
			dest := unsafe.Pointer(uintptr(valsBlock) + valOffset)
			destSlice := unsafe.Slice((*byte)(dest), l)
			copy(destSlice, values[i])
			cValsSlice[i] = (*C.char)(dest)
			valOffset += uintptr(l)
		} else {
			cValsSlice[i] = nil
		}
		cVLensSlice[i] = C.size_t(l)
		cOpsSlice[i] = C.int(ops[i])
	}

	var cErr *C.char
	C.titan_batch_write_ops(s.db,
		(**C.char)(cKeysArr), (*C.size_t)(cKLensArr),
		(**C.char)(cValsArr), (*C.size_t)(cVLensArr),
		(*C.int)(cOpsArr), C.int(count), &cErr)

	if cErr != nil {
		defer C.titan_free(unsafe.Pointer(cErr))
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
	if len(start) > 0 {
		kStart = (*C.char)(unsafe.Pointer(&start[0]))
	}
	if len(end) > 0 {
		kEnd = (*C.char)(unsafe.Pointer(&end[0]))
	}

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
	if len(start) > 0 {
		kStart = (*C.char)(unsafe.Pointer(&start[0]))
	}
	if len(end) > 0 {
		kEnd = (*C.char)(unsafe.Pointer(&end[0]))
	}

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
	if len(key) > 0 {
		kPtr = (*C.char)(unsafe.Pointer(&key[0]))
	}
	if len(value) > 0 {
		vPtr = (*C.char)(unsafe.Pointer(&value[0]))
	}

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
	if len(key) > 0 {
		kPtr = (*C.char)(unsafe.Pointer(&key[0]))
	}

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
	if len(key) > 0 {
		kPtr = (*C.char)(unsafe.Pointer(&key[0]))
	}

	C.titan_delete_cf(s.db, C.titan_cf_t(cf), kPtr, C.size_t(len(key)), C.uint64_t(ts), &cErr)
	if cErr != nil {
		defer C.titan_free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

func (s *TitanStore) Prewrite(mutations []*titankvpb.Mutation, primary []byte, startTS uint64, ttl uint64) error {
	count := len(mutations)
	if count == 0 {
		return nil
	}

	totalSize := 0
	for _, m := range mutations {
		totalSize += len(m.Key) + len(m.Value)
	}
	totalSize += len(primary)

	var bufPtr unsafe.Pointer
	if totalSize > 0 {
		bufPtr = C.malloc(C.size_t(totalSize))
		defer C.free(bufPtr)
	}

	buf := unsafe.Slice((*byte)(bufPtr), totalSize)
	offset := 0

	cMutations := make([]C.titan_mutation_t, count)
	for i, m := range mutations {
		cMutations[i].op = C.int(m.Op)

		if len(m.Key) > 0 {
			copy(buf[offset:offset+len(m.Key)], m.Key)
			cMutations[i].key = (*C.char)(unsafe.Pointer(&buf[offset]))
			cMutations[i].klen = C.size_t(len(m.Key))
			offset += len(m.Key)
		} else {
			cMutations[i].key = nil
			cMutations[i].klen = 0
		}

		if len(m.Value) > 0 {
			copy(buf[offset:offset+len(m.Value)], m.Value)
			cMutations[i].value = (*C.char)(unsafe.Pointer(&buf[offset]))
			cMutations[i].vlen = C.size_t(len(m.Value))
			offset += len(m.Value)
		} else {
			cMutations[i].value = nil
			cMutations[i].vlen = 0
		}
	}

	var pKey *C.char
	var pKeyLen C.size_t
	if len(primary) > 0 {
		copy(buf[offset:offset+len(primary)], primary)
		pKey = (*C.char)(unsafe.Pointer(&buf[offset]))
		pKeyLen = C.size_t(len(primary))
		offset += len(primary)
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

func (s *TitanStore) PrewriteAsync(mutations []*titankvpb.Mutation, primary []byte, startTS uint64, ttl uint64, minCommitTS uint64, isPessimisticLock bool, secondaries [][]byte) error {
	count := len(mutations)
	if count == 0 {
		return nil
	}

	totalSize := 0
	for _, m := range mutations {
		totalSize += len(m.Key) + len(m.Value)
	}
	totalSize += len(primary)
	for _, sec := range secondaries {
		totalSize += len(sec)
	}

	var bufPtr unsafe.Pointer
	if totalSize > 0 {
		bufPtr = C.malloc(C.size_t(totalSize))
		defer C.free(bufPtr)
	}

	buf := unsafe.Slice((*byte)(bufPtr), totalSize)
	offset := 0

	cMutations := make([]C.titan_mutation_t, count)
	for i, m := range mutations {
		cMutations[i].op = C.int(m.Op)

		if len(m.Key) > 0 {
			copy(buf[offset:offset+len(m.Key)], m.Key)
			cMutations[i].key = (*C.char)(unsafe.Pointer(&buf[offset]))
			cMutations[i].klen = C.size_t(len(m.Key))
			offset += len(m.Key)
		} else {
			cMutations[i].key = nil
			cMutations[i].klen = 0
		}

		if len(m.Value) > 0 {
			copy(buf[offset:offset+len(m.Value)], m.Value)
			cMutations[i].value = (*C.char)(unsafe.Pointer(&buf[offset]))
			cMutations[i].vlen = C.size_t(len(m.Value))
			offset += len(m.Value)
		} else {
			cMutations[i].value = nil
			cMutations[i].vlen = 0
		}
	}

	var pKey *C.char
	var pKeyLen C.size_t
	if len(primary) > 0 {
		copy(buf[offset:offset+len(primary)], primary)
		pKey = (*C.char)(unsafe.Pointer(&buf[offset]))
		pKeyLen = C.size_t(len(primary))
		offset += len(primary)
	}

	// Prepare Secondaries
	secCount := len(secondaries)
	var cSecs **C.char
	var cSecLens *C.size_t

	if secCount > 0 {
		ptrSize := unsafe.Sizeof(uintptr(0))
		sizeTSize := unsafe.Sizeof(C.size_t(0))
		
		cSecsArr := C.malloc(C.size_t(secCount) * C.size_t(ptrSize))
		defer C.free(cSecsArr)
		cSecLensArr := C.malloc(C.size_t(secCount) * C.size_t(sizeTSize))
		defer C.free(cSecLensArr)
		
		cSecsSlice := (*[1 << 30]*C.char)(cSecsArr)[:secCount:secCount]
		cSecLensSlice := (*[1 << 30]C.size_t)(cSecLensArr)[:secCount:secCount]

		for i, sec := range secondaries {
			if len(sec) > 0 {
				copy(buf[offset:offset+len(sec)], sec)
				cSecsSlice[i] = (*C.char)(unsafe.Pointer(&buf[offset]))
				cSecLensSlice[i] = C.size_t(len(sec))
				offset += len(sec)
			} else {
				cSecsSlice[i] = nil
				cSecLensSlice[i] = 0
			}
		}
		cSecs = (**C.char)(cSecsArr)
		cSecLens = (*C.size_t)(cSecLensArr)
	}

	var cErr *C.char
	C.titan_mvcc_prewrite_async(
		s.db,
		&cMutations[0],
		C.int(count),
		pKey,
		pKeyLen,
		C.uint64_t(startTS),
		C.uint64_t(ttl),
		C.uint64_t(minCommitTS),
		C.bool(isPessimisticLock),
		cSecs,
		cSecLens,
		C.int(secCount),
		&cErr,
	)

	if cErr != nil {
		defer C.titan_free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}


func (s *TitanStore) Prewrite1PC(mutations []*titankvpb.Mutation, primary []byte, startTS, commitTS uint64, ttl uint64) error {
	count := len(mutations)
	if count == 0 {
		return nil
	}

	totalSize := 0
	for _, m := range mutations {
		totalSize += len(m.Key) + len(m.Value)
	}
	totalSize += len(primary)

	var bufPtr unsafe.Pointer
	if totalSize > 0 {
		bufPtr = C.malloc(C.size_t(totalSize))
		defer C.free(bufPtr)
	}

	buf := unsafe.Slice((*byte)(bufPtr), totalSize)
	offset := 0

	cMutations := make([]C.titan_mutation_t, count)
	for i, m := range mutations {
		cMutations[i].op = C.int(m.Op)

		if len(m.Key) > 0 {
			copy(buf[offset:offset+len(m.Key)], m.Key)
			cMutations[i].key = (*C.char)(unsafe.Pointer(&buf[offset]))
			cMutations[i].klen = C.size_t(len(m.Key))
			offset += len(m.Key)
		} else {
			cMutations[i].key = nil
			cMutations[i].klen = 0
		}

		if len(m.Value) > 0 {
			copy(buf[offset:offset+len(m.Value)], m.Value)
			cMutations[i].value = (*C.char)(unsafe.Pointer(&buf[offset]))
			cMutations[i].vlen = C.size_t(len(m.Value))
			offset += len(m.Value)
		} else {
			cMutations[i].value = nil
			cMutations[i].vlen = 0
		}
	}

	var pKey *C.char
	var pKeyLen C.size_t
	if len(primary) > 0 {
		copy(buf[offset:offset+len(primary)], primary)
		pKey = (*C.char)(unsafe.Pointer(&buf[offset]))
		pKeyLen = C.size_t(len(primary))
		offset += len(primary)
	}

	var cErr *C.char
	C.titan_mvcc_prewrite_1pc(
		s.db,
		&cMutations[0],
		C.int(count),
		pKey,
		pKeyLen,
		C.uint64_t(startTS),
		C.uint64_t(commitTS),
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
	if count == 0 {
		return nil
	}

	totalSize := 0
	for _, k := range keys {
		totalSize += len(k)
	}

	var bufPtr unsafe.Pointer
	if totalSize > 0 {
		bufPtr = C.malloc(C.size_t(totalSize))
		defer C.free(bufPtr)
	}

	buf := unsafe.Slice((*byte)(bufPtr), totalSize)
	offset := 0

	tempKeys := make([]*C.char, count)
	tempLens := make([]C.size_t, count)

	for i, k := range keys {
		if len(k) > 0 {
			copy(buf[offset:offset+len(k)], k)
			tempKeys[i] = (*C.char)(unsafe.Pointer(&buf[offset]))
			tempLens[i] = C.size_t(len(k))
			offset += len(k)
			continue
		}
		tempKeys[i] = nil
		tempLens[i] = C.size_t(len(k))
	}

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

func (s *TitanStore) GC(safePoint uint64) error {
	var cErr *C.char
	C.titan_mvcc_gc(s.db, C.uint64_t(safePoint), &cErr)
	if cErr != nil {
		defer C.titan_free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Coprocessor Support
type CoprocessorType int

const (
    CoprocessorTypeCount CoprocessorType = 0
    CoprocessorTypeSum   CoprocessorType = 1
)

type FilterOperator int

const (
    FilterOperatorEqual          FilterOperator = 0
    FilterOperatorNotEqual       FilterOperator = 1
    FilterOperatorGreater        FilterOperator = 2
    FilterOperatorLess           FilterOperator = 3
    FilterOperatorGreaterOrEqual FilterOperator = 4
    FilterOperatorLessOrEqual    FilterOperator = 5
)

type CoprocessorRequest struct {
    Type           CoprocessorType
    StartKey       []byte
    EndKey         []byte
    StartTS        uint64
    FilterValue    []byte
    FilterOperator FilterOperator
}

type CoprocessorResponse struct {
    Count uint64
    Sum   int64
}

func (s *TitanStore) ExecuteCoprocessor(req *CoprocessorRequest) (*CoprocessorResponse, error) {
    if s.db == nil {
        return nil, errors.New("db is closed")
    }

    var cReq C.titan_coprocessor_request_t
    cReq.coprocessor_type = C.uint8_t(req.Type)
    
    if len(req.StartKey) > 0 {
        cStartKey := C.CBytes(req.StartKey)
        defer C.free(cStartKey)
        cReq.start_key = (*C.char)(cStartKey)
        cReq.start_key_len = C.size_t(len(req.StartKey))
    }
    
    if len(req.EndKey) > 0 {
        cEndKey := C.CBytes(req.EndKey)
        defer C.free(cEndKey)
        cReq.end_key = (*C.char)(cEndKey)
        cReq.end_key_len = C.size_t(len(req.EndKey))
    }
    
    cReq.start_ts = C.uint64_t(req.StartTS)
    
    if len(req.FilterValue) > 0 {
        cFilterValue := C.CBytes(req.FilterValue)
        defer C.free(cFilterValue)
        cReq.filter_value = (*C.char)(cFilterValue)
        cReq.filter_value_len = C.size_t(len(req.FilterValue))
    }
    cReq.filter_operator = C.uint8_t(req.FilterOperator)

    var cResp C.titan_coprocessor_response_t
    var cErr *C.char
    
    C.titan_coprocessor_execute(s.db, &cReq, &cResp, &cErr)
    
    if cErr != nil {
        defer C.titan_free(unsafe.Pointer(cErr))
        return nil, errors.New(C.GoString(cErr))
    }
    
    return &CoprocessorResponse{
        Count: uint64(cResp.count),
        Sum:   int64(cResp.sum),
    }, nil
}

// Iterator support
type Iterator struct {
	iter *C.titan_iterator_t
}

func (s *TitanStore) NewIterator(start, end []byte) *Iterator {
	var cOpts C.titan_read_options_t
	cOpts.verify_checksums = C.bool(false)
	cOpts.fill_cache = C.bool(true)

	// Default to Default CF
	cIter := C.titan_create_iterator(s.db, &cOpts, C.titan_cf_t(CFDefault))
	if cIter == nil {
		return nil
	}
	return &Iterator{iter: cIter}
}

func (i *Iterator) Valid() bool {
	return bool(C.titan_iterator_valid(i.iter))
}

func (i *Iterator) Seek(key []byte) {
	var cKey *C.char
	var cLen C.size_t
	if len(key) > 0 {
		cKey = (*C.char)(unsafe.Pointer(&key[0]))
		cLen = C.size_t(len(key))
	}
	C.titan_iterator_seek(i.iter, cKey, cLen)
}

func (i *Iterator) Next() {
	C.titan_iterator_next(i.iter)
}

func (i *Iterator) Close() {
	if i.iter != nil {
		C.titan_iterator_destroy(i.iter)
		i.iter = nil
	}
}

func (i *Iterator) Key() []byte {
	var cKey *C.char
	var cLen C.size_t
	C.titan_iterator_key(i.iter, &cKey, &cLen)
	if cKey == nil || cLen == 0 {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(cKey), C.int(cLen))
}

func (i *Iterator) Value() []byte {
	var cVal *C.char
	var cLen C.size_t
	C.titan_iterator_value(i.iter, &cVal, &cLen)
	if cVal == nil || cLen == 0 {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(cVal), C.int(cLen))
}
