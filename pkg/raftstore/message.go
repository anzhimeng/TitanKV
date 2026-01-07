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
}

// 简单的工厂函数
func NewMsgRaftMessage(msg *titankvpb.RaftMessage) Msg {
	// 解析出 RegionID (假设 RaftMessage 协议里有 RegionID，如果没有，需要修改 Proto)
	// Week 10 Day 1 的 Proto 没加，我们假设 msg 包含 RegionID
	// 实际工程中，RaftMessage 应该包含 RegionID
	return Msg{Type: MsgTypeRaftMessage, RaftMessage: msg, RegionID: msg.RegionId}
}

func NewMsgRaftCmd(regionID uint64, cmd *titankvpb.RaftCommand) Msg {
	return Msg{Type: MsgTypeRaftCmd, RegionID: regionID, RaftCmd: cmd}
}

func NewMsgTick() Msg {
	return Msg{Type: MsgTypeTick}
}