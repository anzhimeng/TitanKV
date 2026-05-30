package raftstore

import (
	"bytes"
	"os"
	"testing"

	"titankv/api/titankvpb"
	"titankv/pkg/store"
)

func TestCollectDataAndIngest(t *testing.T) {
	// 1. Setup Store
	dir, err := os.MkdirTemp("", "titan-merge-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Open Store with default options
	opts := store.DefaultOptions()
	opts.CreateIfMissing = true
	s, err := store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 2. Prepare Source Region (Region 1, Key "a"-"m")
	sourceID := uint64(1)
	targetID := uint64(2)

	// Write some data to Source
	// DataKey(1, "key1") -> "value1"
	// "key1" is between "a" and "m" (lexicographically "k" > "a" and "k" < "m")
	key1 := []byte("key1")
	val1 := []byte("value1")
	encodedKey1 := DataKey(sourceID, key1)
	t.Logf("EncodedKey1: %x (len: %d)", encodedKey1, len(encodedKey1))

	if err := s.Put(encodedKey1, val1); err != nil {
		t.Fatal(err)
	}

	// Verify Get
	gotVal, err := s.Get(encodedKey1)
	if err != nil {
		t.Fatalf("Get failed immediately after Put: %v", err)
	}
	if !bytes.Equal(gotVal, val1) {
		t.Errorf("Get returned wrong value: %x", gotVal)
	}

	// 3. Create StoreWorker (Partial mock)
	w := &StoreWorker{store: s}

	// 4. Create Source Peer (Mock)
	sourcePeer := &Peer{
		regionID: sourceID,
		region: &titankvpb.Region{
			Id:       sourceID,
			StartKey: []byte("a"),
			EndKey:   []byte("m"),
		},
	}

	// 5. Test collectData
	data, err := w.collectData(sourcePeer)
	if err != nil {
		t.Fatalf("collectData failed: %v", err)
	}

	if len(data) != 1 {
		t.Errorf("Expected 1 key, got %d", len(data))
		for i, k := range data {
			t.Logf("Data[%d]: Key=%x (%s) Value=%x (%s)", i, k.Key, string(k.Key), k.Value, string(k.Value))
		}
	} else {
		if !bytes.Equal(data[0].Key, key1) {
			t.Errorf("Expected key %s (hex: %x), got %s (hex: %x)", key1, key1, data[0].Key, data[0].Key)
		}
		if !bytes.Equal(data[0].Value, val1) {
			t.Errorf("Expected value %s (hex: %x), got %s (hex: %x)", val1, val1, data[0].Value, data[0].Value)
		}
	}

	// 6. Test Ingest (execMerge Target logic)
	// Prepare Target Peer (Mock)
	targetPeer := &Peer{
		regionID: targetID,
		region: &titankvpb.Region{
			Id:       targetID,
			StartKey: []byte("m"),
			EndKey:   []byte("z"),
		},
		storage: &PeerStorage{
			engine:    s, // Same store
			raftState: titankvpb.RaftLocalState{LastIndex: 10, Commit: 10, Term: 5},
		},
	}


	// Construct MergeRequest
	req := &titankvpb.MergeRequest{
		SourceRegion:   sourcePeer.region,
		TargetRegionId: targetID,
		Data:           data,
	}

	// Execute Merge (Target side)
	targetPeer.execMerge(req, s)

	// 7. Verify Data in Target
	// Should find DataKey(2, "key1")
	targetEncodedKey := DataKey(targetID, key1)
	val, err := s.Get(targetEncodedKey)
	if err != nil {
		t.Fatalf("Target data not found: %v", err)
	}
	if !bytes.Equal(val, val1) {
		t.Errorf("Target value mismatch. Expected %s, got %s", val1, val)
	}

	// Verify Range Update
	// Target StartKey should be "a" (from Source StartKey)
	if !bytes.Equal(targetPeer.region.StartKey, []byte("a")) {
		t.Errorf("Target StartKey mismatch. Expected 'a', got %s", targetPeer.region.StartKey)
	}
	// Target EndKey should remain "z"
	if !bytes.Equal(targetPeer.region.EndKey, []byte("z")) {
		t.Errorf("Target EndKey mismatch. Expected 'z', got %s", targetPeer.region.EndKey)
	}
}

func TestCollectDataSizeLimit(t *testing.T) {
	// 1. Setup Store
	dir, err := os.MkdirTemp("", "titan-merge-limit-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := store.DefaultOptions()
	opts.CreateIfMissing = true
	s, err := store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 2. Prepare Source Region
	sourceID := uint64(1)
    
    // Write 3MB of data (limit is 2MB)
    // 3 * 1024 * 1024 bytes
    // Use large values
    largeVal := make([]byte, 1024*1024) // 1MB
    
    // Key 1
    if err := s.Put(DataKey(sourceID, []byte("k1")), largeVal); err != nil {
        t.Fatal(err)
    }
    // Key 2
    if err := s.Put(DataKey(sourceID, []byte("k2")), largeVal); err != nil {
        t.Fatal(err)
    }
    // Key 3
    if err := s.Put(DataKey(sourceID, []byte("k3")), largeVal); err != nil {
        t.Fatal(err)
    }

	// 3. Create StoreWorker (Partial mock)
	w := &StoreWorker{store: s}

	// 4. Create Source Peer (Mock)
	sourcePeer := &Peer{
		regionID: sourceID,
		region: &titankvpb.Region{
			Id:       sourceID,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		},
	}

	// 5. Test collectData - Should Fail
	_, err = w.collectData(sourcePeer)
	if err == nil {
		t.Fatal("collectData should fail due to size limit, but succeeded")
	}
    t.Logf("collectData failed as expected: %v", err)
}
