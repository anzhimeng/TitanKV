package service

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"titankv/api/titankvpb"
	"titankv/pkg/raftstore"
	"titankv/pkg/store"

	"github.com/stretchr/testify/assert"
)

func TestCoprocessorConcurrency(t *testing.T) {
	// 1. Setup Store
	dir, err := os.MkdirTemp("", "titan-coprocessor-concurrency")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := store.DefaultOptions()
	opts.CreateIfMissing = true
	opts.BlockCacheSize = 8 * 1024 * 1024
	opts.BloomFilterBits = 10
	
	s, err := store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	server := &Server{
		store: s,
	}

	// 2. Prepare Initial Data (1000 keys)
	regionID := uint64(1)
	initialCount := 1000
	initialTS := uint64(100)
	initialCommitTS := uint64(110)
	
	t.Log("Writing initial data...")
	for i := 0; i < initialCount; i++ {
		keyStr := fmt.Sprintf("%05d", i)
		valStr := "100" // value for Sum test
		
		encodedKey := raftstore.DataKey(regionID, []byte(keyStr))
		
		mut := &titankvpb.Mutation{
			Op:    titankvpb.Mutation_Put,
			Key:   encodedKey,
			Value: []byte(valStr),
		}
		s.PrewriteAsync([]*titankvpb.Mutation{mut}, encodedKey, initialTS, 0, 0, false, nil)
		s.Commit([][]byte{encodedKey}, initialTS, initialCommitTS)
	}

	// 3. Concurrent Test: Reader (Snapshot Isolation) vs Writer (Updates)
	var wg sync.WaitGroup
	
	// Create channels for signaling
	stopCh := make(chan struct{})

	// Writer: Update keys 0-499 with new values
	// These updates happen at TS=200, commit=210
	updateStartTS := uint64(200)
	updateCommitTS := uint64(210)
	updateCount := 500
	
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < updateCount; i++ {
			select {
			case <-stopCh:
				return
			default:
				keyStr := fmt.Sprintf("%05d", i)
				valStr := "200" // New value
				
				encodedKey := raftstore.DataKey(regionID, []byte(keyStr))
				
				mut := &titankvpb.Mutation{
					Op:    titankvpb.Mutation_Put,
					Key:   encodedKey,
					Value: []byte(valStr),
				}
				
				s.PrewriteAsync([]*titankvpb.Mutation{mut}, encodedKey, updateStartTS, 0, 0, false, nil)
				s.Commit([][]byte{encodedKey}, updateStartTS, updateCommitTS)
				
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	// Reader 1: Snapshot Isolation (Read at TS=150)
	// Should see initial state (all values "100", sum = 1000 * 100 = 100000)
	// Should NOT see updates from TS=200
	snapshotTS := uint64(150)
	var successReads int64
	var failedReads int64
	
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
				req := &titankvpb.CoprocessorRequest{
					Context: &titankvpb.RegionContext{
						RegionId: regionID,
					},
					Type: titankvpb.CoprocessorRequest_SUM,
					StartTs: snapshotTS,
				}
				resp, err := server.ExecuteCoprocessor(context.Background(), req)
				if err != nil {
					fmt.Printf("Reader error: %v\n", err)
					return
				}
				
				// Verify Snapshot Isolation
				// Sum should be 1000 * 100 = 100000
				if resp.Sum != 100000 {
					fmt.Printf("Snapshot Isolation failed! Expected 100000, got %d\n", resp.Sum)
					atomic.AddInt64(&failedReads, 1)
					return
				}
				atomic.AddInt64(&successReads, 1)
				
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Let them run for 2 seconds
	time.Sleep(2 * time.Second)
	close(stopCh)
	wg.Wait()
	
	t.Logf("Snapshot Reader executed %d successful checks", successReads)
	assert.Greater(t, successReads, int64(10))
	assert.Equal(t, int64(0), failedReads)

	// 4. Verify Updates Visibility (Read at TS=300)
	// Should see updated values for first 500 keys ("200") and old values for rest ("100")
	// Sum = 500 * 200 + 500 * 100 = 100000 + 50000 = 150000
	t.Log("Verifying Updates Visibility...")
	req := &titankvpb.CoprocessorRequest{
		Context: &titankvpb.RegionContext{
			RegionId: regionID,
		},
		Type: titankvpb.CoprocessorRequest_SUM,
		StartTs: 300,
	}
	resp, err := server.ExecuteCoprocessor(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, int64(150000), resp.Sum)
}
