package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"titankv/api/raft_serverpb"
	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb"
	"titankv/pkg/client"
	"titankv/pkg/raftstore"
	"titankv/pkg/store"
	"titankv/pkg/txn"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

var (
	scenario    = flag.String("scenario", "all", "all|confchange|snapshot|perf|bank")
	logDir      = flag.String("log-dir", "logs_scenario", "log directory")
	pdAddr      = flag.String("pd-addr", "127.0.0.1:9000", "pd grpc address")
	cluster     = flag.String("cluster", "1=127.0.0.1:9091,2=127.0.0.1:9092,3=127.0.0.1:9093,4=127.0.0.1:9094", "cluster config")
	basePort    = flag.Int("base-port", 9091, "base port")
	nodes       = flag.Int("nodes", 4, "node count")
	directIO    = flag.Bool("direct-io", true, "use direct io")
	duration    = flag.Duration("duration", 2*time.Minute, "perf duration")
	concurrency = flag.Int("concurrency", 16, "perf concurrency")
	keySpace    = flag.Int("key-space", 20000, "perf key space")
	valueSize   = flag.Int("value-size", 1024, "perf value size")
	prefill     = flag.Int("prefill", 5000, "prefill keys for splits")
)

var (
	processes = make(map[int]*exec.Cmd)
	logFiles  = make(map[int]*os.File)
	pdLogFile *os.File
)

func main() {
	flag.Parse()
	setupLogging()
	printNotes()
	cleanup()
	startPD()
	waitForPDReady(8 * time.Second)

	if strings.ToLower(*scenario) == "bank" {
		*directIO = false
	}

	for i := 1; i <= 3; i++ {
		startNode(i)
	}
	time.Sleep(6 * time.Second)

	switch strings.ToLower(*scenario) {
	case "confchange":
		runConfChangeScenario()
	case "snapshot":
		runSnapshotScenario()
	case "perf":
		runPerfScenario()
	case "bank":
		runBankScenario()
	default:
		runConfChangeScenario()
		runSnapshotScenario()
		runPerfScenario()
	}

	cleanup()
}

func setupLogging() {
	if err := os.MkdirAll(*logDir, 0755); err != nil {
		panic(err)
	}
}

func cleanup() {
	exec.Command("pkill", "titankv-server").Run()
	exec.Command("pkill", "pd-server").Run()
	os.RemoveAll("/tmp/pd1")
	for i := 1; i <= *nodes; i++ {
		os.RemoveAll(fmt.Sprintf("/tmp/node%d", i))
		if f := logFiles[i]; f != nil {
			f.Close()
		}
	}
	if pdLogFile != nil {
		pdLogFile.Close()
	}
}

func startPD() {
	logFile, err := os.Create(filepath.Join(*logDir, "pd.log"))
	if err != nil {
		log.Fatal(err)
	}
	pdLogFile = logFile
	cmd := exec.Command("./pd-server",
		"--name=pd1",
		"--data-dir=/tmp/pd1",
		"--client-urls=http://127.0.0.1:2379",
		"--peer-urls=http://127.0.0.1:2380",
		"--initial-cluster=pd1=http://127.0.0.1:2380",
		"--addr=:9000",
	)
	cmd.Stdout = io.MultiWriter(logFile)
	cmd.Stderr = io.MultiWriter(logFile)
	if err := cmd.Start(); err != nil {
		log.Fatalf("start pd failed: %v", err)
	}
}

func waitForPDReady(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := grpc.Dial(*pdAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			client := pdpb.NewPDClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err = client.AllocID(ctx, &pdpb.AllocIDRequest{})
			cancel()
			conn.Close()
			if err == nil {
				log.Printf("pd ready")
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	log.Fatalf("pd not ready within %v", timeout)
}

func waitForRegionForKey(key []byte, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	c, err := client.NewClient(*pdAddr)
	if err != nil {
		return err
	}
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := c.LocateLeader(ctx, key)
		cancel()
		if err == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("wait region for key failed")
}

func startNode(id int) {
	if processes[id] != nil && processes[id].ProcessState == nil {
		return
	}
	logFile, err := os.OpenFile(filepath.Join(*logDir, fmt.Sprintf("node%d.log", id)), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	logFiles[id] = logFile
	cmd := exec.Command("./titankv-server",
		fmt.Sprintf("--id=%d", id),
		fmt.Sprintf("--port=%d", *basePort+id-1),
		fmt.Sprintf("--db_path=/tmp/node%d", id),
		fmt.Sprintf("--cluster=%s", *cluster),
		fmt.Sprintf("--direct_io=%v", *directIO),
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		log.Fatalf("start node %d failed: %v", id, err)
	}
	processes[id] = cmd
}

func killNode(id int) {
	if cmd := processes[id]; cmd != nil && cmd.Process != nil {
		cmd.Process.Signal(syscall.SIGKILL)
		cmd.Wait()
		processes[id] = nil
	}
}

func runConfChangeScenario() {
	log.Println("scenario confchange start")
	startNode(4)
	time.Sleep(8 * time.Second)

	killNode(2)
	waitForPeerStore(1, 4, 140*time.Second)
	startNode(2)
	time.Sleep(6 * time.Second)

	killNode(1)
	time.Sleep(2 * time.Second)
	startNode(1)
	time.Sleep(12 * time.Second)

	checkConfChangeConsistency([]int{1, 2, 3, 4}, 1)
	log.Println("scenario confchange end")
}

func loadRegionState(dbPath string, regionID uint64) (*raft_serverpb.RegionLocalState, *titankvpb.RaftLocalState, error) {
	s, err := store.Open(dbPath, false)
	if err != nil {
		return nil, nil, err
	}
	defer s.Close()

	regionVal, regionErr := s.Get(raftstore.RegionStateKey(regionID))
	raftVal, raftErr := s.Get(raftstore.RaftStateKey(regionID))

	if regionErr != nil && !errors.Is(regionErr, store.ErrKeyNotFound) {
		return nil, nil, regionErr
	}
	if raftErr != nil && !errors.Is(raftErr, store.ErrKeyNotFound) {
		return nil, nil, raftErr
	}

	regionMissing := regionErr != nil && errors.Is(regionErr, store.ErrKeyNotFound)
	raftMissing := raftErr != nil && errors.Is(raftErr, store.ErrKeyNotFound)

	if regionMissing && raftMissing {
		return nil, nil, nil
	}
	if regionMissing != raftMissing {
		return nil, nil, fmt.Errorf("region missing=%v raft missing=%v", regionMissing, raftMissing)
	}

	var regionState raft_serverpb.RegionLocalState
	if err := proto.Unmarshal(regionVal, &regionState); err != nil {
		return nil, nil, err
	}
	var raftState titankvpb.RaftLocalState
	if err := proto.Unmarshal(raftVal, &raftState); err != nil {
		return nil, nil, err
	}
	return &regionState, &raftState, nil
}

func checkRegionState(dbPath string, regionID uint64) (bool, string, error) {
	regionState, raftState, err := loadRegionState(dbPath, regionID)
	if err != nil {
		return false, "", err
	}
	if regionState == nil || raftState == nil {
		return false, "region and raft state missing", nil
	}
	if regionState.Region == nil {
		return false, "region state has nil region", nil
	}
	peerIDs := make([]uint64, 0, len(regionState.Region.Peers))
	seen := map[uint64]struct{}{}
	storeIDs := make([]uint64, 0, len(regionState.Region.Peers))
	for _, p := range regionState.Region.Peers {
		peerIDs = append(peerIDs, p.Id)
		seen[p.Id] = struct{}{}
		storeIDs = append(storeIDs, p.StoreId)
	}
	sorted := append([]uint64(nil), peerIDs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	unique := len(seen) == len(peerIDs)
	sort.Slice(storeIDs, func(i, j int) bool { return storeIDs[i] < storeIDs[j] })
	confVer := uint64(0)
	if regionState.Region.RegionEpoch != nil {
		confVer = regionState.Region.RegionEpoch.ConfVer
	}
	return true, fmt.Sprintf("peerIDs=%v storeIDs=%v unique=%v confVer=%d commit=%d lastIndex=%d", sorted, storeIDs, unique, confVer, raftState.Commit, raftState.LastIndex), nil
}

func runSnapshotScenario() {
	log.Println("scenario snapshot start")
	addr := fmt.Sprintf("127.0.0.1:%d", *basePort)
	data := bytes.Repeat([]byte("a"), 128*1024)
	wrongSize := uint64(len(data) + 1024)

	if err := sendSnapshot(addr, 1, data, wrongSize, 0, nil); err == nil {
		log.Printf("snapshot size mismatch expected error but got nil")
	} else {
		log.Printf("snapshot size mismatch error: %v", err)
	}

	checksum := crc32.ChecksumIEEE(data)
	if err := sendSnapshot(addr, 1, data, uint64(len(data)), checksum+1, nil); err == nil {
		log.Printf("snapshot checksum mismatch expected error but got nil")
	} else {
		log.Printf("snapshot checksum mismatch error: %v", err)
	}

	raftSnap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: 1,
			Term:  1,
			ConfState: raftpb.ConfState{
				Voters: []uint64{1},
			},
		},
	}
	meta, _ := raftSnap.Marshal()

	if err := sendSnapshot(addr, 1, data, uint64(len(data)), checksum, meta); err != nil {
		log.Printf("snapshot valid send failed: %v", err)
	} else {
		log.Printf("snapshot valid send ok")
	}
	log.Println("scenario snapshot end")
}

func sendSnapshot(addr string, regionID uint64, data []byte, fileSize uint64, checksum uint32, raftMeta []byte) error {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := titankvpb.NewTitanKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := client.StreamSnapshot(ctx)
	if err != nil {
		return err
	}

	chunkSize := 64 * 1024
	var offset uint64
	for offset < uint64(len(data)) {
		end := offset + uint64(chunkSize)
		if end > uint64(len(data)) {
			end = uint64(len(data))
		}
		chunk := &titankvpb.SnapshotChunk{
			RegionId: regionID,
			FileSize: fileSize,
			Offset:   offset,
			Data:     data[offset:end],
			IsLast:   false,
		}
		if err := stream.Send(chunk); err != nil {
			return err
		}
		offset = end
	}

	last := &titankvpb.SnapshotChunk{
		RegionId:         regionID,
		FileSize:         fileSize,
		Offset:           offset,
		Checksum:         uint64(checksum),
		IsLast:           true,
		RaftSnapshotData: raftMeta,
	}
	if err := stream.Send(last); err != nil {
		return err
	}
	_, err = stream.CloseAndRecv()
	return err
}

func runPerfScenario() {
	log.Println("scenario perf start")
	c, err := client.NewClient(*pdAddr)
	if err != nil {
		log.Printf("perf create client failed: %v", err)
		return
	}

	monitor := startApplyMonitor("/tmp/node1", 1, 200*time.Millisecond)

	value := make([]byte, *valueSize)
	rand.Read(value)

	start := time.Now()
	for i := 0; i < *prefill; i++ {
		key := []byte(fmt.Sprintf("prefill-%08d", i))
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := c.Put(ctx, key, value)
		cancel()
		if err != nil && i < 10 {
			log.Printf("prefill err: %v", err)
		}
	}
	log.Printf("prefill done in %v", time.Since(start))

	var ops uint64
	var errs uint64
	var mu sync.Mutex
	latencies := make([]time.Duration, 0, 100000)
	deadline := time.Now().Add(*duration)

	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(worker)))
			for time.Now().Before(deadline) {
				id := r.Intn(*keySpace)
				key := []byte(fmt.Sprintf("k-%08d", id))
				val := make([]byte, *valueSize)
				r.Read(val)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				begin := time.Now()
				err := c.Put(ctx, key, val)
				cancel()
				elapsed := time.Since(begin)
				mu.Lock()
				latencies = append(latencies, elapsed)
				mu.Unlock()
				if err != nil {
					atomic.AddUint64(&errs, 1)
				}
				atomic.AddUint64(&ops, 1)
			}
		}(i)
	}
	wg.Wait()

	mu.Lock()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := percentile(latencies, 0.50)
	p95 := percentile(latencies, 0.95)
	p99 := percentile(latencies, 0.99)
	mu.Unlock()

	if monitor != nil {
		monitor.stop()
	}

	throughput := float64(ops) / duration.Seconds()
	if monitor != nil {
		log.Printf("perf ops=%d errs=%d throughput=%.2f/s p50=%v p95=%v p99=%v applyGapMax=%v applyGapAvg=%v", ops, errs, throughput, p50, p95, p99, monitor.maxGap(), monitor.avgGap())
	} else {
		log.Printf("perf ops=%d errs=%d throughput=%.2f/s p50=%v p95=%v p99=%v", ops, errs, throughput, p50, p95, p99)
	}

	log.Println("scenario perf end")
}

type applyMonitor struct {
	stopCh   chan struct{}
	doneCh   chan struct{}
	maxDelay time.Duration
	totalGap time.Duration
	gapCount uint64
	mu       sync.Mutex
}

func startApplyMonitor(dbPath string, regionID uint64, interval time.Duration) *applyMonitor {
	s, err := store.Open(dbPath, false)
	if err != nil {
		log.Printf("apply monitor open store failed: %v", err)
		return nil
	}

	m := &applyMonitor{
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	go func() {
		defer close(m.doneCh)
		defer s.Close()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		var lastIndex uint64
		lastChange := time.Now()

		for {
			select {
			case <-m.stopCh:
				return
			case <-ticker.C:
				val, err := s.Get(raftstore.ApplyStateKey(regionID))
				if err != nil {
					continue
				}
				var applyState titankvpb.RaftApplyState
				if err := proto.Unmarshal(val, &applyState); err != nil {
					continue
				}
				if applyState.AppliedIndex != lastIndex {
					now := time.Now()
					gap := now.Sub(lastChange)
					m.mu.Lock()
					if gap > m.maxDelay {
						m.maxDelay = gap
					}
					m.totalGap += gap
					m.gapCount++
					m.mu.Unlock()
					lastIndex = applyState.AppliedIndex
					lastChange = now
				}
			}
		}
	}()
	return m
}

func (m *applyMonitor) stop() {
	close(m.stopCh)
	<-m.doneCh
}

func (m *applyMonitor) maxGap() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.maxDelay
}

func (m *applyMonitor) avgGap() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.gapCount == 0 {
		return 0
	}
	return time.Duration(int64(m.totalGap) / int64(m.gapCount))
}

func waitForPeerStore(regionID uint64, storeID uint64, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := grpc.Dial(*pdAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			client := pdpb.NewPDClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			resp, err := client.GetRegion(ctx, &pdpb.GetRegionRequest{Key: []byte{}})
			cancel()
			conn.Close()
			if err == nil && resp != nil && resp.Region != nil {
				for _, p := range resp.Region.Peers {
					if p.StoreId == storeID {
						log.Printf("confchange detected peer store %d in region %d", storeID, regionID)
						return
					}
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
	log.Printf("confchange wait timeout for store %d in region %d", storeID, regionID)
}

func checkConfChangeConsistency(nodeIDs []int, regionID uint64) {
	type nodeState struct {
		region *raft_serverpb.RegionLocalState
		raft   *titankvpb.RaftLocalState
		err    error
	}

	for _, id := range nodeIDs {
		killNode(id)
	}
	time.Sleep(2 * time.Second)

	states := make(map[int]nodeState)
	for _, id := range nodeIDs {
		path := fmt.Sprintf("/tmp/node%d", id)
		region, raftState, err := loadRegionState(path, regionID)
		states[id] = nodeState{region: region, raft: raftState, err: err}
	}

	var basePeers string
	for _, id := range nodeIDs {
		st := states[id]
		if st.err != nil {
			log.Printf("confchange check node %d error: %v", id, st.err)
			continue
		}
		if st.region == nil || st.raft == nil {
			log.Printf("confchange check node %d missing state", id)
			continue
		}
		ok, detail, err := checkRegionState(fmt.Sprintf("/tmp/node%d", id), regionID)
		if err != nil {
			log.Printf("confchange check node %d error: %v", id, err)
			continue
		}
		if !ok {
			log.Printf("confchange check node %d failed: %s", id, detail)
		} else {
			log.Printf("confchange check node %d ok: %s", id, detail)
		}
		peersSig := regionPeersSignature(st.region.Region)
		if basePeers == "" {
			basePeers = peersSig
		} else if peersSig != basePeers {
			log.Printf("confchange peers mismatch node %d peers=%s base=%s", id, peersSig, basePeers)
		}
	}

	for _, id := range nodeIDs {
		startNode(id)
	}
	time.Sleep(6 * time.Second)
}

func regionPeersSignature(region *titankvpb.Region) string {
	if region == nil {
		return ""
	}
	ids := make([]string, 0, len(region.Peers))
	for _, p := range region.Peers {
		ids = append(ids, fmt.Sprintf("%d-%d", p.StoreId, p.Id))
	}
	sort.Strings(ids)
	confVer := uint64(0)
	if region.RegionEpoch != nil {
		confVer = region.RegionEpoch.ConfVer
	}
	return fmt.Sprintf("confver=%d peers=%s", confVer, strings.Join(ids, ","))
}

func printNotes() {
	log.Printf("scenario options: -scenario=all|confchange|snapshot|perf|bank")
	log.Printf("confchange uses node4 and waits for peer add via PD scheduler")
	log.Printf("snapshot sends size mismatch, checksum mismatch, then valid snapshot")
	log.Printf("perf reports throughput, latency percentiles, apply gap stats when available")
}

func runBankScenario() {
	log.Println("scenario bank start")
	c, err := client.NewClient(*pdAddr)
	if err != nil {
		log.Printf("bank create client failed: %v", err)
		return
	}
	{
		conn, err := grpc.Dial(*pdAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			pdcli := pdpb.NewPDClient(conn)
			ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
			region := &pdpb.Region{
				Id:          1,
				StartKey:    []byte{},
				EndKey:      []byte{},
				RegionEpoch: &pdpb.RegionEpoch{ConfVer: 1, Version: 1},
				Peers: []*pdpb.Peer{
					{Id: 1, StoreId: 1},
					{Id: 2, StoreId: 2},
					{Id: 3, StoreId: 3},
				},
			}
			leader := &pdpb.Peer{Id: 1, StoreId: 1}
			_, _ = pdcli.RegionHeartbeat(ctx2, &pdpb.RegionHeartbeatRequest{
				Region: region,
				Leader: leader,
			})
			cancel2()
			conn.Close()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	keyA := []byte("bank:A")
	keyB := []byte("bank:B")

	if err := waitForRegionForKey(keyA, 60*time.Second); err != nil {
		log.Printf("bank wait region A failed: %v", err)
		return
	}
	if err := waitForRegionForKey(keyB, 60*time.Second); err != nil {
		log.Printf("bank wait region B failed: %v", err)
		return
	}

	{
		initTxn, err := txn.NewTransaction(ctx, c)
		if err != nil {
			log.Printf("bank init txn create failed: %v", err)
			return
		}
		initTxn.Set(keyA, []byte("1000"))
		initTxn.Set(keyB, []byte("0"))
		if err := initTxn.Commit(ctx); err != nil {
			log.Printf("bank init commit failed: %v", err)
			return
		}
	}

	parseBalance := func(b []byte) int {
		n := 0
		for _, c := range b {
			if c < '0' || c > '9' {
				return 0
			}
			n = n*10 + int(c-'0')
		}
		return n
	}
	toBalanceBytes := func(n int) []byte {
		return []byte(fmt.Sprintf("%d", n))
	}

	var wg sync.WaitGroup
	workers := 50
	opsPerWorker := 50
	errs := uint64(0)
	violations := int32(0)
	done := make(chan struct{})
	progressDone := make(chan struct{})
	doneOps := uint64(0)
	totalOps := uint64(workers * opsPerWorker)

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				vTxn, err := txn.NewTransaction(ctx, c)
				if err != nil {
					continue
				}
				aVal, errA := vTxn.Get(ctx, keyA)
				bVal, errB := vTxn.Get(ctx, keyB)
				if errA != nil || errB != nil || aVal == nil || bVal == nil {
					continue
				}
				sum := parseBalance(aVal) + parseBalance(bVal)
				if sum != 1000 {
					if atomic.CompareAndSwapInt32(&violations, 0, 1) {
						cancel()
					}
					return
				}
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-progressDone:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				log.Printf("bank progress ops=%d/%d errs=%d", atomic.LoadUint64(&doneOps), totalOps, atomic.LoadUint64(&errs))
			}
		}
	}()

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			for j := 0; j < opsPerWorker; j++ {
				func() {
					defer atomic.AddUint64(&doneOps, 1)
					if atomic.LoadInt32(&violations) == 1 {
						return
					}
					select {
					case <-ctx.Done():
						return
					default:
					}
					amt := r.Intn(20) + 1
					dir := r.Intn(2)

					t, err := txn.NewTransaction(ctx, c)
					if err != nil {
						atomic.AddUint64(&errs, 1)
						time.Sleep(10 * time.Millisecond)
						return
					}

					av, errA := t.Get(ctx, keyA)
					bv, errB := t.Get(ctx, keyB)
					if errA != nil || errB != nil || av == nil || bv == nil {
						atomic.AddUint64(&errs, 1)
						time.Sleep(10 * time.Millisecond)
						return
					}
					aBal := parseBalance(av)
					bBal := parseBalance(bv)

					if dir == 0 {
						if aBal >= amt {
							aBal -= amt
							bBal += amt
						} else {
							return
						}
					} else {
						if bBal >= amt {
							bBal -= amt
							aBal += amt
						} else {
							return
						}
					}

					t.Set(keyA, toBalanceBytes(aBal))
					t.Set(keyB, toBalanceBytes(bBal))

					if err := t.Commit(ctx); err != nil {
						atomic.AddUint64(&errs, 1)
						time.Sleep(5 * time.Millisecond)
						return
					}
				}()
			}
		}(i)
	}
	wg.Wait()
	close(done)
	close(progressDone)

	if ctx.Err() != nil && atomic.LoadInt32(&violations) == 0 {
		log.Printf("bank aborted: %v", ctx.Err())
		return
	}

	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer verifyCancel()
	verTxn, err := txn.NewTransaction(verifyCtx, c)
	if err != nil {
		log.Printf("bank verify txn create failed: %v", err)
		return
	}
	aVal, _ := verTxn.Get(verifyCtx, keyA)
	bVal, _ := verTxn.Get(verifyCtx, keyB)
	sum := parseBalance(aVal) + parseBalance(bVal)

	if sum != 1000 || atomic.LoadInt32(&violations) == 1 {
		log.Printf("❌ Bank Test FAILED: A+B=%d (errs=%d)", sum, errs)
	} else {
		log.Printf("✅ Bank Test PASSED: A+B=%d (errs=%d)", sum, errs)
	}
	log.Println("scenario bank end")
}
func percentile(data []time.Duration, p float64) time.Duration {
	if len(data) == 0 {
		return 0
	}
	idx := int(float64(len(data)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(data) {
		idx = len(data) - 1
	}
	return data[idx]
}
