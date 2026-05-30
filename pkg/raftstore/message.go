package raftstore

import (
	"sync"
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
	MsgTypeReadIndex MsgType = 7
	MsgTypeRaftCmdBatch MsgType = 8
	MsgTypeAddPeer      MsgType = 9
	MsgTypeRaftMessageBatch MsgType = 10
	MsgTypeMergeCheck       MsgType = 11
	MsgTypePeerStopped      MsgType = 12
)

type Msg struct {
	Type     MsgType
	RegionID uint64
	
	// 载荷 (Union)
	RaftMessage *titankvpb.RaftMessage
	RaftMessages []*titankvpb.RaftMessage // 批量 Raft 消息
	RaftCmd     *titankvpb.RaftCommand // Client 发来的读写请求 (Put, Get, Scan)
    // 【新增】ReadIndex 专用字段
    ReadIndexRet chan uint64 // 成功时返回 index
	RaftCmds []*titankvpb.RaftCommand
	Callbacks []func(error)


	Callback func(error)
	
	// 【新增】AddPeer 专用
	Peer *Peer
}

var raftMsgPool = sync.Pool{
	New: func() interface{} {
		return &titankvpb.RaftMessage{}
	},
}

func AcquireRaftMessage() *titankvpb.RaftMessage {
	return raftMsgPool.Get().(*titankvpb.RaftMessage)
}

func ReleaseRaftMessage(msg *titankvpb.RaftMessage) {
	msg.Reset()
	raftMsgPool.Put(msg)
}

func NewMsgRaftMessage(msg *titankvpb.RaftMessage) Msg {
	return Msg{
        Type: MsgTypeRaftMessage, 
        RaftMessage: msg, 
        RegionID: msg.RegionId,
    }
}

func NewMsgRaftMessageBatch(msgs []*titankvpb.RaftMessage) Msg {
	if len(msgs) == 0 {
		return Msg{Type: MsgTypeNull}
	}
	return Msg{
		Type:         MsgTypeRaftMessageBatch,
		RaftMessages: msgs,
		RegionID:     msgs[0].RegionId,
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

func NewMsgRaftCmdBatch(regionID uint64, cmds []*titankvpb.RaftCommand, cbs []func(error)) Msg {
	return Msg{
		Type:      MsgTypeRaftCmdBatch,
		RegionID:  regionID,
		RaftCmds:  cmds,
		Callbacks: cbs,
	}
}


func NewMsgTick() Msg {
	return Msg{Type: MsgTypeTick}
}

func NewMsgSplitCheck(regionID uint64) Msg {
    return Msg{Type: MsgTypeSplitCheck, RegionID: regionID}
}

func NewMsgMergeCheck(regionID uint64) Msg {
	return Msg{Type: MsgTypeMergeCheck, RegionID: regionID}
}

func NewMsgPeerStopped(peer *Peer) Msg {
	return Msg{Type: MsgTypePeerStopped, RegionID: peer.regionID, Peer: peer}
}

func NewMsgReadIndex(regionID uint64, retCh chan uint64) Msg {
    return Msg{
        Type:         MsgTypeReadIndex,
        RegionID:     regionID,
        ReadIndexRet: retCh,
    }
}
