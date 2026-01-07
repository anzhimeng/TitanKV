package raftstore

import (
	"encoding/binary"
)

const (
	// Local Min Region ID
	LocalMinRegionID = 0
	// Local Max Region ID
	LocalMaxRegionID = 0xFFFFFFFFFFFFFFFF
)

var (
	// 物理隔离前缀
	LocalPrefix = []byte{0x01} // 用于存储本地元数据 (StoreIdent 等)
	RegionPrefix = []byte{'z'} // 数据前缀
	RaftPrefix   = []byte{'r'} // Raft Log 前缀
	
	// Suffix for Raft State
	RaftLogSuffix    = byte(0x01)
	RaftStateSuffix  = byte(0x02)
	ApplyStateSuffix = byte(0x03)
	RegionStateSuffix= byte(0x04)
)

// --- 工具函数 ---

func encodeRegionID(regionID uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, regionID)
	return b
}

func decodeRegionID(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// --- Data Key Encoding: z{RegionID}_{UserKey} ---

func DataKey(regionID uint64, userKey []byte) []byte {
	k := make([]byte, 1+8+len(userKey))
	k[0] = 'z'
	binary.BigEndian.PutUint64(k[1:], regionID)
	copy(k[9:], userKey)
	return k
}

// --- Raft Log Encoding: r{RegionID}{LogSuffix}{Index} ---

func RaftLogKey(regionID uint64, index uint64) []byte {
	k := make([]byte, 1+8+1+8)
	k[0] = 'r'
	binary.BigEndian.PutUint64(k[1:], regionID)
	k[9] = RaftLogSuffix
	binary.BigEndian.PutUint64(k[10:], index)
	return k
}

// --- Raft State Encoding: r{RegionID}{StateSuffix} ---

func RaftStateKey(regionID uint64) []byte {
	k := make([]byte, 1+8+1)
	k[0] = 'r'
	binary.BigEndian.PutUint64(k[1:], regionID)
	k[9] = RaftStateSuffix
	return k
}

func ApplyStateKey(regionID uint64) []byte {
	k := make([]byte, 1+8+1)
	k[0] = 'r'
	binary.BigEndian.PutUint64(k[1:], regionID)
	k[9] = ApplyStateSuffix
	return k
}

func RegionStateKey(regionID uint64) []byte {
	k := make([]byte, 1+8+1)
	k[0] = 'r'
	binary.BigEndian.PutUint64(k[1:], regionID)
	k[9] = RegionStateSuffix
	return k
}