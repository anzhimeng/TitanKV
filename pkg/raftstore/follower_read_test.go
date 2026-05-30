package raftstore

import (
	"os"
	"testing"
	"time"

	"titankv/api/titankvpb"
	"titankv/pkg/store"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestFollowerReadIndex(t *testing.T) {
	// 1. Setup Store
	dir, err := os.MkdirTemp("", "raftstore-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := store.Open(dir, store.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 2. Setup Region & State
	region := &titankvpb.Region{
		Id:       1,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Peers:    []*titankvpb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}},
	}
	// Use helper functions from peer.go (unexported but accessible in same package)
	initRaftState(s, region)
	writeRegionState(s, region)

	// 3. Create Peer (Follower)
	peer, err := NewPeer(1, region, s)
	if err != nil {
		t.Fatal(err)
	}

	// 4. Inject Leader Heartbeat to let Follower know the Leader (Peer 2)
	// Term must be > 5 (initial term is 5)
	err = peer.raftGroup.Step(raftpb.Message{
		Type: raftpb.MsgHeartbeat,
		From: 2, // Peer 2 is leader
		To:   1,
		Term: 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 5. Trigger ReadIndex
	readCh := make(chan uint64, 1)
	msg := Msg{
		Type:         MsgTypeReadIndex,
		RegionID:     1,
		ReadIndexRet: readCh,
	}
	// Verify handleReadIndexBatch handles it
	peer.handleReadIndexBatch([]Msg{msg})

	// 6. Check Ready for MsgReadIndex
	// Follower should send MsgReadIndex to Leader
	if !peer.raftGroup.HasReady() {
		t.Fatal("Expected Ready after ReadIndex")
	}
	rd := peer.raftGroup.Ready()

	foundMsg := false
	var reqCtx []byte
	for _, m := range rd.Messages {
		if m.Type == raftpb.MsgReadIndex {
			foundMsg = true
			if len(m.Entries) > 0 {
				reqCtx = m.Entries[0].Data
			}
			break
		}
	}
	if !foundMsg {
		t.Fatal("Expected MsgReadIndex in Ready messages")
	}

	// 7. Simulate receiving MsgReadIndexResp from Leader
	err = peer.raftGroup.Step(raftpb.Message{
		Type:    raftpb.MsgReadIndexResp,
		From:    2,
		To:      1,
		Term:    10,
		Entries: []raftpb.Entry{{Data: reqCtx}}, // Response carries the same ctx
		Index:   100,                             // Committed Index
	})
	if err != nil {
		t.Fatal(err)
	}
	
	// 8. Consume Ready to get ReadStates
	if !peer.raftGroup.HasReady() {
		t.Fatal("Expected Ready after ReadIndexResp")
	}
	rd = peer.raftGroup.Ready()
	if len(rd.ReadStates) == 0 {
		t.Fatal("Expected ReadStates in Ready")
	}
	
	// 9. Handle ReadStates (Notify callbacks)
	peer.handleReadStates(rd.ReadStates)

	// 10. Verify Result
	select {
	case idx := <-readCh:
		if idx != 100 {
			t.Fatalf("Expected read index 100, got %d", idx)
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for read index result")
	}
}
