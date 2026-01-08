package raftstore

import (
	"titankv/api/titankvpb"
)

type MsgType int

const (
	MsgTypeNull       MsgType = 0
	MsgTypeRaftMessage MsgType = 1 // 网络层发来的 Raft 消息 (Vote, AppendEntries)
	MsgTypeRaftCmd     MsgType = 2 // Client 发来的读写请求 (Put, Get, Scan)
	MsgTypeTick        MsgType = 3 // 全局定时器触发的 Tick
	MsgTypeSplit       MsgType = 4 // (后续) 分裂请求
	MsgTypeRegionApproximateSize MsgType = 5 // (后续) 统计大小
	MsgTypeSplitCheck MsgType = 6 
)

type Msg struct {
	Type     MsgType
	RegionID uint64
	
	// 载荷 (Union)
	RaftMessage *titankvpb.RaftMessage
	RaftCmd     *titankvpb.RaftCommand // 实际上这应该是一个 Request 包装，包含 Callback

	Callback func(error)
}

func NewMsgRaftMessage(msg *titankvpb.RaftMessage) Msg {
	return Msg{
        Type: MsgTypeRaftMessage, 
        RaftMessage: msg, 
        RegionID: msg.RegionId,
    }
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


func NewMsgTick() Msg {
	return Msg{Type: MsgTypeTick}
}

func NewMsgSplitCheck(regionID uint64) Msg {
    return Msg{Type: MsgTypeSplitCheck, RegionID: regionID}
}