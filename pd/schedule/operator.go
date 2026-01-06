package schedule

import (
	"fmt"
	"titankv/pd/api/pdpb"
)

// OpStep 是调度的最小原子步骤
type OpStep interface {
	String() string
	IsFinish(region *pdpb.Region, leader *pdpb.Peer) bool
}

// --- 具体步骤定义 ---

// 1. 转移 Leader
type TransferLeader struct {
	FromStore uint64
	ToStore   uint64
}

func (t *TransferLeader) String() string {
	return fmt.Sprintf("transfer leader from store %d to %d", t.FromStore, t.ToStore)
}

func (t *TransferLeader) IsFinish(region *pdpb.Region, leader *pdpb.Peer) bool {
	return leader != nil && leader.StoreId == t.ToStore
}

// 2. 增加副本 (Add Peer)
type AddPeer struct {
	ToStore uint64
	PeerID  uint64 // 预分配的 PeerID
}

func (a *AddPeer) String() string {
	return fmt.Sprintf("add peer %d on store %d", a.PeerID, a.ToStore)
}

func (a *AddPeer) IsFinish(region *pdpb.Region, leader *pdpb.Peer) bool {
	for _, p := range region.Peers {
		if p.Id == a.PeerID && p.StoreId == a.ToStore {
			return true
		}
	}
	return false
}

// 3. 移除副本 (Remove Peer)
type RemovePeer struct {
	FromStore uint64
}

func (r *RemovePeer) String() string {
	return fmt.Sprintf("remove peer on store %d", r.FromStore)
}

func (r *RemovePeer) IsFinish(region *pdpb.Region, leader *pdpb.Peer) bool {
	for _, p := range region.Peers {
		if p.StoreId == r.FromStore {
			return false // 还在，没完成
		}
	}
	return true
}

// --- Operator 定义 ---

// Operator 包含一个 Region 的 ID 和一系列需要顺序执行的步骤
type Operator struct {
	RegionID uint64
	Kind     string // e.g., "balance-leader", "balance-region"
	Steps    []OpStep
	Current  int // 当前执行到第几步
}

func NewOperator(regionID uint64, kind string, steps ...OpStep) *Operator {
	return &Operator{
		RegionID: regionID,
		Kind:     kind,
		Steps:    steps,
		Current:  0,
	}
}

func (o *Operator) String() string {
	return fmt.Sprintf("Operator[%s | Region %d]: %v", o.Kind, o.RegionID, o.Steps)
}

// 检查当前步骤是否完成，推进进度
// 返回: true 表示整个 Operator 完成
func (o *Operator) Check(region *pdpb.Region, leader *pdpb.Peer) bool {
	if o.Current >= len(o.Steps) {
		return true
	}
	if o.Steps[o.Current].IsFinish(region, leader) {
		o.Current++
	}
	return o.Current >= len(o.Steps)
}