package service

import (
	"context"
	"fmt"
	"os"
	"testing"

	"titankv/api/titankvpb"
	"titankv/pkg/raftstore"
	"titankv/pkg/store"
)

func BenchmarkCoprocessor(b *testing.B) {
	// 1. Setup Store
	dir, err := os.MkdirTemp("", "titan-coprocessor-benchmark")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := store.DefaultOptions()
	opts.CreateIfMissing = true
	opts.BlockCacheSize = 64 * 1024 * 1024 // 64MB Cache
	opts.BloomFilterBits = 10
	
	s, err := store.Open(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	server := &Server{
		store: s,
	}

	// 2. Prepare Data (10k keys)
	regionID := uint64(1)
	count := 10000
	startTS := uint64(100)
	commitTS := uint64(110)
	
	for i := 0; i < count; i++ {
		keyStr := fmt.Sprintf("%05d", i) // "00000" - "09999"
		valStr := keyStr
		
		encodedKey := raftstore.DataKey(regionID, []byte(keyStr))
		
		mut := &titankvpb.Mutation{
			Op:    titankvpb.Mutation_Put,
			Key:   encodedKey,
			Value: []byte(valStr),
		}
		s.PrewriteAsync([]*titankvpb.Mutation{mut}, encodedKey, startTS, 0, 0, false, nil)
		s.Commit([][]byte{encodedKey}, startTS, commitTS)
	}
	
	ctx := context.Background()
	req := &titankvpb.CoprocessorRequest{
		Context: &titankvpb.RegionContext{
			RegionId: regionID,
		},
		Type: titankvpb.CoprocessorRequest_COUNT,
		StartTs: commitTS + 1,
	}

	b.ResetTimer()
	
	// 3. Run Benchmark
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := server.ExecuteCoprocessor(ctx, req)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkCoprocessorWithFilter(b *testing.B) {
	// 1. Setup Store
	dir, err := os.MkdirTemp("", "titan-coprocessor-benchmark-filter")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := store.DefaultOptions()
	opts.CreateIfMissing = true
	opts.BlockCacheSize = 64 * 1024 * 1024
	opts.BloomFilterBits = 10
	
	s, err := store.Open(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	server := &Server{
		store: s,
	}

	// 2. Prepare Data (10k keys)
	regionID := uint64(1)
	count := 10000
	startTS := uint64(100)
	commitTS := uint64(110)
	
	for i := 0; i < count; i++ {
		keyStr := fmt.Sprintf("%05d", i)
		valStr := keyStr
		encodedKey := raftstore.DataKey(regionID, []byte(keyStr))
		mut := &titankvpb.Mutation{
			Op:    titankvpb.Mutation_Put,
			Key:   encodedKey,
			Value: []byte(valStr),
		}
		s.PrewriteAsync([]*titankvpb.Mutation{mut}, encodedKey, startTS, 0, 0, false, nil)
		s.Commit([][]byte{encodedKey}, startTS, commitTS)
	}
	
	ctx := context.Background()
	// Filter: Greater "05000" -> Scan half
	req := &titankvpb.CoprocessorRequest{
		Context: &titankvpb.RegionContext{
			RegionId: regionID,
		},
		Type: titankvpb.CoprocessorRequest_COUNT,
		StartTs: commitTS + 1,
		FilterValue: []byte("05000"),
		FilterOperator: titankvpb.CoprocessorRequest_GREATER,
	}

	b.ResetTimer()
	
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := server.ExecuteCoprocessor(ctx, req)
			if err != nil {
				b.Fatal(err)
			}
			if resp.Count != 4999 {
				// b.Fatal("Count mismatch")
			}
		}
	})
}
