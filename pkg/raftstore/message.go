package raftstore

import (
	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb"
)

type MsgType int

const (
	MsgTypeNull       MsgType = 0
	MsgTypeRaftMessage MsgType = 1 // 网络层发来的 Raft 消息 (Vote, AppendEntries)
	MsgTypeRaftCmd     MsgType = 2 // Client 发来的读写请求 (Put, Get, Scan)
	MsgTypeTick        MsgType = 3 // 全局定时器触发的 Tick
	MsgTypeSplit       MsgType = 4 // (后续) 分裂请求
	MsgTypeRegionApproximateSize MsgType = 5 // (后续) 统计大小
)

type Msg struct {
	Type     MsgType
	RegionID uint64
	
	// 载荷 (Union)
	RaftMessage *titankvpb.RaftMessage
	RaftCmd     *titankvpb.RaftCommand // 实际上这应该是一个 Request 包装，包含 Callback

	Callback func(error)
}

// 简单的工厂函数
func NewMsgRaftCmd(regionID uint64, cmd *titankvpb.RaftCommand, cb func(error)) Msg {
    return Msg{
        Type:     MsgTypeRaftCmd,
        RegionID: regionID,
        RaftCmd:  cmd,
        Callback: cb,
    }
}

func NewMsgRaftCmd(regionID uint64, cmd *titankvpb.RaftCommand) Msg {
	return Msg{Type: MsgTypeRaftCmd, RegionID: regionID, RaftCmd: cmd}
}

func NewMsgTick() Msg {
	return Msg{Type: MsgTypeTick}
}