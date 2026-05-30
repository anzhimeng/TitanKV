package service

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb"
	"titankv/pkg/raftstore"
	"titankv/pkg/store"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	titankvpb.UnimplementedTitanKVServer
	// 【修改】只依赖 Router 和 Store
	router        *raftstore.Router
	store         *store.TitanStore
	latches       *raftstore.Latches
	batchMu       sync.Mutex
	batchers      map[uint64]*regionBatcher
	batchMax      int
	batchWait     time.Duration
	inflightLimit int
	dynamicMin    int
	dynamicMax    int
	dynamicTarget time.Duration
	backoff       time.Duration
	adjustEvery   time.Duration
}

var encodeBufPool = sync.Pool{
	New: func() any {
		return make([]byte, 0, 4096)
	},
}

func getEncodeBuf(size int) []byte {
	buf := encodeBufPool.Get().([]byte)
	if cap(buf) < size {
		return make([]byte, size)
	}
	return buf[:size]
}

func putEncodeBuf(buf []byte) {
	if buf == nil {
		return
	}
	encodeBufPool.Put(buf[:0])
}

func encodeDataKeys(regionID uint64, keys [][]byte) ([][]byte, []byte) {
	if len(keys) == 0 {
		return make([][]byte, 0), nil
	}
	total := 0
	needEncode := false
	for _, k := range keys {
		if rid, _, ok := raftstore.DecodeDataKey(k); ok && rid == regionID {
			continue
		}
		needEncode = true
		total += 1 + 8 + len(k)
	}
	if !needEncode {
		return keys, nil
	}
	buf := getEncodeBuf(total)
	encoded := make([][]byte, len(keys))
	offset := 0
	for i, k := range keys {
		if rid, _, ok := raftstore.DecodeDataKey(k); ok && rid == regionID {
			encoded[i] = k
			continue
		}
		n := 1 + 8 + len(k)
		s := buf[offset : offset+n]
		s[0] = 'z'
		binary.BigEndian.PutUint64(s[1:], regionID)
		copy(s[9:], k)
		encoded[i] = s
		offset += n
	}
	return encoded, buf
}

func encodeDataKeysWithPrimary(regionID uint64, keys [][]byte, primary []byte) ([][]byte, []byte, []byte) {
	if len(keys) == 0 {
		return make([][]byte, 0), primary, nil
	}
	total := 0
	needEncode := false
	for _, k := range keys {
		if rid, _, ok := raftstore.DecodeDataKey(k); ok && rid == regionID {
			continue
		}
		needEncode = true
		total += 1 + 8 + len(k)
	}
	primaryEncoded := false
	if rid, _, ok := raftstore.DecodeDataKey(primary); ok && rid == regionID {
		primaryEncoded = true
	} else {
		needEncode = true
		total += 1 + 8 + len(primary)
	}
	if !needEncode {
		return keys, primary, nil
	}
	buf := getEncodeBuf(total)
	encoded := make([][]byte, len(keys))
	offset := 0
	for i, k := range keys {
		if rid, _, ok := raftstore.DecodeDataKey(k); ok && rid == regionID {
			encoded[i] = k
			continue
		}
		n := 1 + 8 + len(k)
		s := buf[offset : offset+n]
		s[0] = 'z'
		binary.BigEndian.PutUint64(s[1:], regionID)
		copy(s[9:], k)
		encoded[i] = s
		offset += n
	}
	if primaryEncoded {
		return encoded, primary, buf
	}
	pn := 1 + 8 + len(primary)
	ps := buf[offset : offset+pn]
	ps[0] = 'z'
	binary.BigEndian.PutUint64(ps[1:], regionID)
	copy(ps[9:], primary)
	return encoded, ps, buf
}

func NewServer(router *raftstore.Router, s *store.TitanStore) *Server {
	return &Server{
		router:        router,
		store:         s,
		latches:       raftstore.NewLatches(),
		batchers:      make(map[uint64]*regionBatcher),
		batchMax:      2048,
		batchWait:     500 * time.Microsecond,
		inflightLimit: 10000,
		dynamicMin:    500,
		dynamicMax:    10000,
		dynamicTarget: 1000 * time.Microsecond,
		backoff:       2 * time.Microsecond,
		adjustEvery:   1 * time.Millisecond,
	}
}

type pendingReq struct {
	cmd  *titankvpb.RaftCommand
	ctx  context.Context
	resp chan error
}

type regionBatcher struct {
	regionID uint64
	router   *raftstore.Router
	reqCh    chan *pendingReq
	inflight chan struct{}
	maxBatch int
	wait     time.Duration
	dynamic  *dynamicLimiter
	backoff  time.Duration
}

type dynamicLimiter struct {
	mu          sync.Mutex
	tokens      chan struct{}
	currentMax  int
	min         int
	max         int
	targetDelay time.Duration
	adjustEvery time.Duration
	lastAdjust  time.Time
}

func newLimiter(init, min, max int, targetDelay, adjustEvery time.Duration) *dynamicLimiter {
	if init < min {
		init = min
	}
	if init > max {
		init = max
	}
	l := &dynamicLimiter{
		tokens:      make(chan struct{}, max),
		currentMax:  init,
		min:         min,
		max:         max,
		targetDelay: targetDelay,
		adjustEvery: adjustEvery,
		lastAdjust:  time.Now(),
	}
	for i := 0; i < init; i++ {
		l.tokens <- struct{}{}
	}
	return l
}

func (l *dynamicLimiter) acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.tokens:
		return nil
	}
}

func (l *dynamicLimiter) release() {
	l.mu.Lock()
	limit := l.currentMax
	l.mu.Unlock()
	if len(l.tokens) < limit {
		l.tokens <- struct{}{}
	}
}

func (l *dynamicLimiter) feedback(batchLen int, wait time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if now.Sub(l.lastAdjust) < l.adjustEvery {
		return
	}
	if wait > l.targetDelay && batchLen < 4 {
		// 太慢且批量很小，说明压力不大但延迟偏高 -> 降低并发
		if l.currentMax > l.min {
			l.currentMax--
		}
	} else if wait < l.targetDelay && batchLen >= 4 {
		// 速度快且批次足够大 -> 提升并发
		if l.currentMax < l.max {
			l.currentMax++
		}
	}
	if l.currentMax < l.min {
		l.currentMax = l.min
	}
	if l.currentMax > l.max {
		l.currentMax = l.max
	}
	for len(l.tokens) > l.currentMax {
		<-l.tokens
	}
	for len(l.tokens) < l.currentMax {
		l.tokens <- struct{}{}
	}
	l.lastAdjust = now
}

func (s *Server) getBatcher(regionID uint64) *regionBatcher {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()
	if b, ok := s.batchers[regionID]; ok {
		return b
	}
	max := s.dynamicMax
	if max <= 0 {
		max = s.inflightLimit
	}
	min := s.dynamicMin
	if min <= 0 {
		min = 1
	}
	if min > max {
		min = max
	}
	init := s.inflightLimit
	if init > max {
		init = max
	}
	b := &regionBatcher{
		regionID: regionID,
		router:   s.router,
		reqCh:    make(chan *pendingReq, s.inflightLimit*2),
		inflight: make(chan struct{}, s.inflightLimit),
		maxBatch: s.batchMax,
		wait:     s.batchWait,
		dynamic:  newLimiter(init, min, max, s.dynamicTarget, s.adjustEvery),
		backoff:  s.backoff,
	}
	s.batchers[regionID] = b
	go b.run()
	return b
}

func (b *regionBatcher) enqueue(ctx context.Context, cmd *titankvpb.RaftCommand) error {
	if err := b.dynamic.acquire(ctx); err != nil {
		return err
	}
	req := &pendingReq{
		cmd:  cmd,
		ctx:  ctx,
		resp: make(chan error, 1),
	}

	select {
	case b.reqCh <- req:
	case <-ctx.Done():
		b.dynamic.release()
		return ctx.Err()
	}

	select {
	case err := <-req.resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *regionBatcher) run() {
	var batch []*pendingReq
	var timer *time.Timer
	var timerC <-chan time.Time
	var firstArrival time.Time

	flush := func() {
		if len(batch) == 0 {
			return
		}
		var cmds []*titankvpb.RaftCommand
		var callbacks []func(error)
		for _, req := range batch {
			if err := req.ctx.Err(); err != nil {
				b.dynamic.release()
				req.resp <- err
				close(req.resp)
				continue
			}
			cb := func(err error) {
				b.dynamic.release()
				req.resp <- err
				close(req.resp)
			}
			cmds = append(cmds, req.cmd)
			callbacks = append(callbacks, cb)
		}
		if len(cmds) > 0 {
			msg := raftstore.NewMsgRaftCmdBatch(b.regionID, cmds, callbacks)
			if !b.router.Send(b.regionID, msg) {
				err := status.Error(codes.NotFound, "region not found on this store")
				for _, cb := range callbacks {
					cb(err)
				}
			}
		}
		if !firstArrival.IsZero() {
			wait := time.Since(firstArrival)
			b.dynamic.feedback(len(batch), wait)
			firstArrival = time.Time{}
		}
		batch = batch[:0]
		if timer != nil {
			timer.Stop()
			timer = nil
			timerC = nil
		}
	}

	for {
		select {
		case req := <-b.reqCh:
			batch = append(batch, req)
			if len(batch) == 1 {
				firstArrival = time.Now()
				timer = time.NewTimer(b.wait)
				timerC = timer.C
			}
			if len(batch) >= b.maxBatch {
				flush()
			}
		case <-timerC:
			flush()
		}
	}
}

// 辅助：获取 Peer 以进行 Epoch 检查
func (s *Server) getPeer(regionID uint64) (*raftstore.Peer, error) {
	peer := s.router.GetLocalPeer(regionID).(*raftstore.Peer)
	if peer == nil {
		return nil, status.Error(codes.NotFound, "region not found on this store")
	}
	return peer, nil
}

// 辅助：Epoch 检查
func (s *Server) checkEpoch(ctx context.Context, regionID uint64, reqEpoch *titankvpb.RegionEpoch) error {
	peer, err := s.getPeer(regionID)
	if err != nil {
		return err
	}
	if err := peer.CheckEpoch(toPdpbEpoch(reqEpoch)); err != nil {
		return status.Error(codes.Aborted, "EpochNotMatch")
	}
	return nil
}

func (s *Server) Put(ctx context.Context, req *titankvpb.PutRequest) (*titankvpb.PutResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	if req.Context == nil {
		return nil, status.Error(codes.InvalidArgument, "missing region context")
	}

	regionID := req.Context.RegionId

	// Concurrency Control: Acquire Latches to prevent WriteConflict in Raft Apply
	keys := [][]byte{req.Key}
	release := s.latches.Acquire(keys)
	defer release()

	cmd := &titankvpb.RaftCommand{
		Header: &titankvpb.RaftRequestHeader{
			RegionId:    regionID,
			RegionEpoch: req.Context.RegionEpoch,
			Peer:        req.Context.Peer,
		},
		Type:  titankvpb.RaftCommand_NORMAL,
		Op:    titankvpb.RaftCommand_PUT,
		Key:   req.Key,
		Value: req.Value,
	}
	if err := s.getBatcher(regionID).enqueue(ctx, cmd); err != nil {
		return nil, err
	}
	return &titankvpb.PutResponse{}, nil
}

func (s *Server) Get(ctx context.Context, req *titankvpb.GetRequest) (*titankvpb.GetResponse, error) {
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}
	if req.Context == nil {
		return nil, status.Error(codes.InvalidArgument, "missing region context")
	}
	if req.StartTs == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing start_ts")
	}

	regionID := req.Context.RegionId

	if err := s.checkEpoch(ctx, regionID, req.Context.RegionEpoch); err != nil {
		return nil, err
	}

	// =========================================================
	// Phase 1: Linearizability Check (ReadIndex)
	// 确保我们读到的是最新的数据状态 (防止脑裂读旧数据)
	// =========================================================

	readCh := make(chan uint64, 1)
	msg := raftstore.Msg{
		Type:         raftstore.MsgTypeReadIndex,
		RegionID:     regionID,
		ReadIndexRet: readCh,
	}
	if !s.router.Send(regionID, msg) {
		return nil, status.Error(codes.NotFound, "region not found on this store")
	}

	var readIndex uint64
	select {
	case index := <-readCh:
		readIndex = index
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// 等待 Apply Index >= Read Index
	peer := s.router.GetLocalPeer(regionID).(*raftstore.Peer)
	if peer == nil {
		return nil, status.Error(codes.NotFound, "region lost during read")
	}
	if err := peer.WaitApplied(ctx, readIndex); err != nil {
		return nil, err
	}

	// =========================================================
	// Phase 2: MVCC Read (Snapshot Read)
	// 在本地引擎中，根据 StartTS 读取可见版本
	// =========================================================

	// 注意：Store 里的 Key 不需要再加 z{RegionID} 前缀了！
	// 为什么？因为我们在 C++ 层实现的 PutCF/GetCF 会自动处理 MVCC 编码。
	// 但是！C++ 层的 MVCC Key 是基于 User Key 的。
	// 如果我们想支持 Multi-Raft，底层的 Key 应该是 z{RegionID}_{MvccKey}。
	// 这涉及到 C++ 层的改造。
	//
	// 【关键回顾】：Week 13 Day 1 我们实现的 EncodeMvccKey 是： Prefix(1) + UserKey + TS(8)。
	// 它并没有包含 RegionID！
	// 这意味着目前的 MVCC 实现是单机版的，不支持 Multi-Raft 数据隔离。
	//
	// 为了 Week 14 能跑通，我们需要做一个适配：
	// 将 z{RegionID}_{UserKey} 作为一个整体，当作 MVCC 的 "User Key" 传给 C++。
	// 这样 C++ 编码后就是：Prefix(1) + z{RegionID}_{UserKey} + TS(8)。
	// 虽然多了一层前缀，但逻辑是完全正确的，且实现了隔离。

	encodedKey := raftstore.DataKey(regionID, req.Key)

	// 调用 MvccGet
	val, err := s.store.MvccGet(encodedKey, req.StartTs)
	if err != nil {
		if strings.Contains(err.Error(), "Key is locked") {
			lockInfo, _ := s.getLockInfo(req.Context.RegionId, req.Key)
			st := status.New(codes.Aborted, "KeyLocked")
			ds, _ := st.WithDetails(&titankvpb.KeyError{LockInfo: lockInfo})
			return nil, ds.Err()
		}
		if strings.Contains(err.Error(), "NotFound") ||
			strings.Contains(err.Error(), "key not found") ||
			strings.Contains(err.Error(), "Key deleted") ||
			strings.Contains(err.Error(), "Failed to open file") {
			return nil, status.Error(codes.NotFound, "key not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &titankvpb.GetResponse{Value: val}, nil
}

func (s *Server) Delete(ctx context.Context, req *titankvpb.DeleteRequest) (*titankvpb.DeleteResponse, error) {
	if req.Context == nil {
		return nil, status.Error(codes.InvalidArgument, "missing region context")
	}
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty key")
	}
	regionID := req.Context.RegionId

	cmd := &titankvpb.RaftCommand{
		Header: &titankvpb.RaftRequestHeader{
			RegionId:    req.Context.RegionId,
			RegionEpoch: req.Context.RegionEpoch,
			Peer:        req.Context.Peer,
		},
		Type: titankvpb.RaftCommand_NORMAL,
		Op:   titankvpb.RaftCommand_DELETE,
		Key:  req.Key,
	}
	if err := s.getBatcher(regionID).enqueue(ctx, cmd); err != nil {
        return nil, err
    }
    return &titankvpb.DeleteResponse{}, nil
}

func (s *Server) Write(stream titankvpb.TitanKV_WriteServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if req.Context == nil {
			_ = stream.Send(&titankvpb.WriteResponse{Error: "missing region context"})
			continue
		}
		regionID := req.Context.RegionId
		if err := s.checkEpoch(stream.Context(), regionID, req.Context.RegionEpoch); err != nil {
			if st, ok := status.FromError(err); ok {
				_ = stream.Send(&titankvpb.WriteResponse{Error: st.Message()})
			} else {
				_ = stream.Send(&titankvpb.WriteResponse{Error: err.Error()})
			}
			continue
		}
		if len(req.Mutations) == 0 {
			_ = stream.Send(&titankvpb.WriteResponse{})
			continue
		}

		var firstErr error
		for _, m := range req.Mutations {
			if m == nil || len(m.Key) == 0 {
				firstErr = status.Error(codes.InvalidArgument, "empty key")
				break
			}
			cmd := &titankvpb.RaftCommand{
				Header: &titankvpb.RaftRequestHeader{
					RegionId:    regionID,
					RegionEpoch: req.Context.RegionEpoch,
					Peer:        req.Context.Peer,
				},
				Type: titankvpb.RaftCommand_NORMAL,
			}
			if m.Op == titankvpb.Mutation_Delete {
				cmd.Op = titankvpb.RaftCommand_DELETE
				cmd.Key = m.Key
			} else if m.Op == titankvpb.Mutation_Put {
				cmd.Op = titankvpb.RaftCommand_PUT
				cmd.Key = m.Key
				cmd.Value = m.Value
			} else {
				firstErr = status.Error(codes.InvalidArgument, "unsupported mutation")
				break
			}
			if err := s.getBatcher(regionID).enqueue(stream.Context(), cmd); err != nil {
				firstErr = err
				break
			}
		}

		if firstErr != nil {
			if st, ok := status.FromError(firstErr); ok {
				_ = stream.Send(&titankvpb.WriteResponse{Error: st.Message()})
			} else {
				_ = stream.Send(&titankvpb.WriteResponse{Error: firstErr.Error()})
			}
			continue
		}

		_ = stream.Send(&titankvpb.WriteResponse{})
	}
}

// 处理 Raft 消息 (节点间通信)
func (s *Server) Raft(ctx context.Context, req *titankvpb.RaftMessage) (*titankvpb.RaftResponse, error) {
	// 使用 Router 分发
	msg := raftstore.NewMsgRaftMessage(req)
	if !s.router.Send(req.RegionId, msg) {
		// Region 可能正在创建中或者还没 Ready，甚至不存在
		// 生产环境可能需要重试或者返回错误
		// return nil, status.Error(codes.NotFound, "region not found")
	}
	return &titankvpb.RaftResponse{}, nil
}

// --- PD 交互接口 ---
// Week 10 Day 5 联调时我们主要测 Put/Get，不需要 PD 介入 Store 管理

// UpdateConfig 接口实现 (Week 7 遗留)
func (s *Server) UpdateConfig(ctx context.Context, req *titankvpb.UpdateConfigRequest) (*titankvpb.UpdateConfigResponse, error) {
	if req.GcThreshold > 0 {
		s.store.SetGCThreshold(req.GcThreshold)
	}
	if req.GcSafePoint > 0 {
		if err := s.store.GC(req.GcSafePoint); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return &titankvpb.UpdateConfigResponse{}, nil
}

// 辅助转换
func toPdpbEpoch(e *titankvpb.RegionEpoch) *pdpb.RegionEpoch {
	if e == nil {
		return nil
	}
	return &pdpb.RegionEpoch{ConfVer: e.ConfVer, Version: e.Version}
}

func (s *Server) StreamSnapshot(stream titankvpb.TitanKV_StreamSnapshotServer) error {
	var file *os.File
	var regionID uint64
	var raftSnapshot raftpb.Snapshot // 【新增】暂存元数据
	var hasher hash.Hash32
	var expectedSize uint64
	var expectedChecksum uint64
	var received uint64
	var nextOffset uint64

	// 1. 接收 Loop
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			if file == nil {
				return stream.SendAndClose(&titankvpb.RaftResponse{})
			}
			if received != expectedSize {
				file.Close()
				os.Remove(file.Name())
				return status.Error(codes.InvalidArgument, "snapshot size mismatch")
			}
			if expectedChecksum != 0 && hasher.Sum32() != uint32(expectedChecksum) {
				file.Close()
				os.Remove(file.Name())
				return status.Error(codes.InvalidArgument, "snapshot checksum mismatch")
			}
			file.Close()
			// 传输完成
			// 把文件路径塞回 Snapshot.Data
			// 尝试反序列化 SnapshotData 以保留 Region 信息
			var snapData raftstore.SnapshotData
			if err := json.Unmarshal(raftSnapshot.Data, &snapData); err == nil {
				// 如果成功，更新 FilePath 并重新序列化
				snapData.FilePath = file.Name()
				if newData, err := json.Marshal(snapData); err == nil {
					raftSnapshot.Data = newData
				} else {
					// 序列化失败，回退
					raftSnapshot.Data = []byte(file.Name())
				}
			} else {
				// 如果不是 JSON（旧格式），直接存路径
				raftSnapshot.Data = []byte(file.Name())
			}

			s.finishSnapshot(regionID, &raftSnapshot)
			return stream.SendAndClose(&titankvpb.RaftResponse{})
		}
		if err != nil {
			return err
		}

		if file == nil {
			regionID = chunk.RegionId
			file, err = os.CreateTemp("", "snap-*.sst")
			if err != nil {
				return err
			}
			hasher = crc32.NewIEEE()
			expectedSize = chunk.FileSize
		}
		if chunk.FileSize != expectedSize {
			file.Close()
			os.Remove(file.Name())
			return status.Error(codes.InvalidArgument, "snapshot file size inconsistent")
		}
		if chunk.Offset != nextOffset {
			file.Close()
			os.Remove(file.Name())
			return status.Error(codes.InvalidArgument, "snapshot offset mismatch")
		}
		if len(chunk.Data) > 0 {
			if _, err := file.Write(chunk.Data); err != nil {
				file.Close()
				os.Remove(file.Name())
				return err
			}
			hasher.Write(chunk.Data)
			received += uint64(len(chunk.Data))
			nextOffset += uint64(len(chunk.Data))
		}
		if chunk.IsLast {
			expectedChecksum = chunk.Checksum
		}

		// 【新增】如果包含元数据，保存下来
		if len(chunk.RaftSnapshotData) > 0 {
			raftSnapshot.Unmarshal(chunk.RaftSnapshotData)
		}
	}
}

func (s *Server) finishSnapshot(regionID uint64, snap *raftpb.Snapshot) {
	// 构造 MsgSnap 消息
	// 注意：我们需要把 raftpb.Snapshot 包装进 raftpb.Message
	rMsg := raftpb.Message{
		Type:     raftpb.MsgSnap,
		Snapshot: *snap,
	}
	data, _ := rMsg.Marshal()

	msg := raftstore.Msg{
		Type:     raftstore.MsgTypeRaftMessage, // 当作普通 Raft 消息处理
		RegionID: regionID,
		RaftMessage: &titankvpb.RaftMessage{
			RegionId: regionID,
			Data:     data,
		},
	}
	s.router.Send(regionID, msg)
}

func (s *Server) BatchRaft(stream titankvpb.TitanKV_BatchRaftServer) error {
	for {
		batch, err := stream.Recv()
		if err != nil {
			return err
		}

		if len(batch.Msgs) == 0 {
			continue
		}

		// Group by RegionID to reduce Router lock contention
		grouped := make(map[uint64][]*titankvpb.RaftMessage)
		for _, msg := range batch.Msgs {
			grouped[msg.RegionId] = append(grouped[msg.RegionId], msg)
		}

		for regionID, msgs := range grouped {
			if len(msgs) == 1 {
				raftMsg := raftstore.NewMsgRaftMessage(msgs[0])
				s.router.Send(regionID, raftMsg)
			} else {
				raftMsg := raftstore.NewMsgRaftMessageBatch(msgs)
				s.router.Send(regionID, raftMsg)
			}
		}
	}
}

func (s *Server) Prewrite(ctx context.Context, req *titankvpb.PrewriteRequest) (*titankvpb.PrewriteResponse, error) {
	// log.Println("!!! SERVER RECEIVED PREWRITE !!!")
	if req.Context == nil {
		return nil, status.Error(codes.InvalidArgument, "missing context")
	}

	// 1. 检查 Epoch
	if err := s.checkEpoch(ctx, req.Context.RegionId, req.Context.RegionEpoch); err != nil {
		return nil, err
	}

	regionID := req.Context.RegionId

	// 2. 编码 Keys (Multi-Raft 隔离)
	keys := make([][]byte, len(req.Mutations))
	for i, m := range req.Mutations {
		keys[i] = m.Key
	}
	encodedKeys, encPrimary, buf := encodeDataKeysWithPrimary(regionID, keys, req.PrimaryKey)
	if buf != nil || !bytes.Equal(encPrimary, req.PrimaryKey) {
		for i, m := range req.Mutations {
			m.Key = encodedKeys[i]
		}
		req.PrimaryKey = encPrimary
	}

	// 3. 调用 Store (Through Raft)
	cmd := &titankvpb.RaftCommand{
		Header: &titankvpb.RaftRequestHeader{
			RegionId:    req.Context.RegionId,
			RegionEpoch: req.Context.RegionEpoch,
			Peer:        req.Context.Peer,
		},
		Type:            titankvpb.RaftCommand_TXN,
		PrewriteRequest: req,
	}

	// Concurrency Control: Acquire Latches to prevent WriteConflict in Raft Apply
	release := s.latches.Acquire(keys)
	defer release()

	// Optimization: Check Conflict in Local Store BEFORE Raft Proposal
	// This prevents wasting Raft Log and network bandwidth for obvious conflicts.
	if err := s.store.CheckConflict(encodedKeys, req.StartTs); err != nil {
		// Parse simple error from CheckConflict
		errStr := err.Error()
		if strings.Contains(errStr, "Write conflict") {
			return &titankvpb.PrewriteResponse{
				Error: "WriteConflict",
				// We don't have detailed ConflictTs here, but this is enough to abort the transaction.
			}, nil
		}
		if strings.Contains(errStr, "Key is locked") {
			// For Locked keys, we ideally need LockInfo.
			// Since CheckConflict is a lightweight check, we can just return KeyLocked error.
			// The client might treat this as a backoff signal.
			// Or we can try to get LockInfo here?
			// But getLockInfo reads from Store... which is fine.
			// Let's try to populate LockInfo if possible.
			// But we don't know WHICH key is locked from the error message.
			// So just return generic error.
			return &titankvpb.PrewriteResponse{
				Error: "KeyLocked",
			}, nil
		}
		return &titankvpb.PrewriteResponse{Error: errStr}, nil
	}

	if req.Use_1Pc {
		// log.Printf("[1PC] Prewrite received for key %s, StartTS=%d, CommitTS=%d", req.Mutations[0].Key, req.StartTs, req.CommitTs)
	}

	// Async Commit: 确保 Secondaries 也在 PrewriteRequest 中被传递到 Raft 层
	// PrewriteRequest 已经包含了 Async Commit 字段，直接传递即可。
	// 下游 Apply 的时候，Store 会解析这些字段。

	err := s.getBatcher(regionID).enqueue(ctx, cmd)
	putEncodeBuf(buf)

	if req.GetUse_1Pc() && err == nil {
		return &titankvpb.PrewriteResponse{
			OnePcCommitted: true,
			OnePcCommitTs:  req.GetCommitTs(),
		}, nil
	}

	if err != nil {
		// 解析错误
		if strings.Contains(err.Error(), "Write conflict") {
			// log.Printf("[Server] Store.Prewrite WriteConflict: %v", err) // Reduce log spam
			var startTS uint64
			var conflictTS uint64
			for _, part := range strings.Fields(err.Error()) {
				part = strings.Trim(part, ",")
				if strings.HasPrefix(part, "start_ts=") {
					if v, parseErr := strconv.ParseUint(strings.TrimPrefix(part, "start_ts="), 10, 64); parseErr == nil {
						startTS = v
					}
				}
				if strings.HasPrefix(part, "conflict_ts=") {
					if v, parseErr := strconv.ParseUint(strings.TrimPrefix(part, "conflict_ts="), 10, 64); parseErr == nil {
						conflictTS = v
					}
				}
			}
			resp := &titankvpb.PrewriteResponse{Error: "WriteConflict"}
			if startTS != 0 || conflictTS != 0 {
				resp.Conflict = &titankvpb.WriteConflict{
					StartTs:    startTS,
					ConflictTs: conflictTS,
					Key:        req.Mutations[0].Key,
					Primary:    req.PrimaryKey,
				}
			}
			return resp, nil
		}
		if strings.Contains(err.Error(), "Key is locked") {
			lockInfo, _ := s.getLockInfo(req.Context.RegionId, req.Mutations[0].Key)

			return &titankvpb.PrewriteResponse{
				Error:    "KeyLocked",
				KeyError: &titankvpb.KeyError{LockInfo: lockInfo},
			}, nil
		}
		// 还可以处理 WriteConflict
		return &titankvpb.PrewriteResponse{Error: err.Error()}, nil
	}

	resp := &titankvpb.PrewriteResponse{}
	if req.Use_1Pc {
		resp.OnePcCommitted = true
	}
	// Support Parallel Commit
	if req.UseAsyncCommit {
		resp.MinCommitTs = req.MinCommitTs
	}
	return resp, nil
}

func (s *Server) AcquirePessimisticLock(ctx context.Context, req *titankvpb.AcquirePessimisticLockRequest) (*titankvpb.AcquirePessimisticLockResponse, error) {
	if req.Context == nil {
		return nil, status.Error(codes.InvalidArgument, "missing region context")
	}
	if len(req.Mutations) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty mutations")
	}
	// log.Printf("[Server] AcquirePessimisticLock: %v", req.Mutations[0].Key)
	regionID := req.Context.RegionId

	// Encode keys with region prefix
	keys := make([][]byte, len(req.Mutations))
	for i, m := range req.Mutations {
		keys[i] = m.Key
	}
	encodedKeys, encPrimary, buf := encodeDataKeysWithPrimary(regionID, keys, req.PrimaryKey)
	if buf != nil || !bytes.Equal(encPrimary, req.PrimaryKey) {
		for i, m := range req.Mutations {
			m.Key = encodedKeys[i]
		}
		req.PrimaryKey = encPrimary
	}

	cmd := &titankvpb.RaftCommand{
		Header: &titankvpb.RaftRequestHeader{
			RegionId:    regionID,
			RegionEpoch: req.Context.RegionEpoch,
			Peer:        req.Context.Peer,
		},
		Type:                          titankvpb.RaftCommand_TXN,
		AcquirePessimisticLockRequest: req,
	}

	err := s.getBatcher(regionID).enqueue(ctx, cmd)
	putEncodeBuf(buf)
	if err != nil {
		return nil, err
	}
	return &titankvpb.AcquirePessimisticLockResponse{}, nil
}

func (s *Server) Commit(ctx context.Context, req *titankvpb.CommitRequest) (*titankvpb.CommitResponse, error) {
	if req.Context == nil {
		return nil, status.Error(codes.InvalidArgument, "missing context")
	}

	// 1. 检查 Epoch
	if err := s.checkEpoch(ctx, req.Context.RegionId, req.Context.RegionEpoch); err != nil {
		return nil, err
	}

	regionID := req.Context.RegionId

	// 2. 编码 Keys
	encodedKeys, buf := encodeDataKeys(regionID, req.Keys)
	req.Keys = encodedKeys

	// 3. 调用 Store (Through Raft)
	cmd := &titankvpb.RaftCommand{
		Header: &titankvpb.RaftRequestHeader{
			RegionId:    req.Context.RegionId,
			RegionEpoch: req.Context.RegionEpoch,
			Peer:        req.Context.Peer,
		},
		Type:          titankvpb.RaftCommand_TXN,
		CommitRequest: req,
	}

	err := s.getBatcher(regionID).enqueue(ctx, cmd)
	putEncodeBuf(buf)
	if err != nil {
		return &titankvpb.CommitResponse{Error: err.Error()}, nil
	}

	return &titankvpb.CommitResponse{}, nil
}

func (s *Server) CheckTxnStatus(ctx context.Context, req *titankvpb.CheckTxnStatusRequest) (*titankvpb.CheckTxnStatusResponse, error) {
	if req.Context == nil {
		return nil, status.Error(codes.InvalidArgument, "missing context")
	}

	if err := s.checkEpoch(ctx, req.Context.RegionId, req.Context.RegionEpoch); err != nil {
		return nil, err
	}

	regionID := req.Context.RegionId
	encPrimary := raftstore.DataKey(regionID, req.PrimaryKey)

	action, commitTS, err := s.store.CheckTxnStatus(encPrimary, req.LockTs, req.CurrentTs)
	if err != nil {
		return nil, err
	}

	return &titankvpb.CheckTxnStatusResponse{
		Action:   titankvpb.CheckTxnStatusResponse_Action(action),
		CommitTs: commitTS,
	}, nil
}

func (s *Server) ResolveLock(ctx context.Context, req *titankvpb.ResolveLockRequest) (*titankvpb.ResolveLockResponse, error) {
	log.Printf("[DEBUG-Server] ResolveLock RPC received. Keys: %d", len(req.Keys))
	// 1. 路由与 Epoch 检查 (Multi-Raft 适配)
	if req.Context != nil {
		regionID := req.Context.RegionId

		// 【关键修复】通过 Router 获取 Peer
		peer := s.router.GetLocalPeer(regionID)
		if peer == nil {
			return nil, status.Error(codes.NotFound, "region not found")
		}

		// 调用 Peer 的 CheckEpoch
		if err := peer.CheckEpoch(toPdpbEpoch(req.Context.RegionEpoch)); err != nil {
			return nil, status.Error(codes.Aborted, "EpochNotMatch")
		}
	}

	// 2. 编码 Key (Multi-Raft 适配)
	// 所有的 Key 都要加上 z{RegionID} 前缀
	regionID := req.Context.RegionId
	var encodedKeys [][]byte
	for _, k := range req.Keys {
		// 【关键修复】检查 Key 是否已经编码
		// 假设 DataKey 的第一个字节是 'z' (Week 10 定义)
		// 并且后续是 RegionID。
		// 简单判断：如果以 'z' 开头，且长度足够，且包含 RegionID，就不再编码。
		// 或者更简单：Client 传来的 Keys 约定必须是 User Key。

		// 既然 LockInfo 返回的是 Encoded Key，Client 为了方便直接传回来了。
		// 我们在这里做一个 Hack：如果 Key 以 'z' 开头，假设它已经是 Encoded Key。

		isEncoded := false
		if len(k) >= 9 && k[0] == 'z' {
			// 进一步检查 RegionID 是否匹配 (可选，但更安全)
			// rid := binary.BigEndian.Uint64(k[1:9])
			// if rid == regionID { isEncoded = true }
			isEncoded = true
		}

		if isEncoded {
			encodedKeys = append(encodedKeys, k)
		} else {
			encodedKeys = append(encodedKeys, raftstore.DataKey(regionID, k))
		}
	}

	// 3. 调用 Store 执行 (Through Raft)
	req.Keys = encodedKeys
	cmd := &titankvpb.RaftCommand{
		Header: &titankvpb.RaftRequestHeader{
			RegionId:    req.Context.RegionId,
			RegionEpoch: req.Context.RegionEpoch,
			Peer:        req.Context.Peer,
		},
		Type:               titankvpb.RaftCommand_TXN,
		ResolveLockRequest: req,
	}

	err := s.getBatcher(regionID).enqueue(ctx, cmd)
	log.Printf("[DEBUG-Server] ResolveLock Store.Commit result: %v", err)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &titankvpb.ResolveLockResponse{}, nil
}

// 辅助：读取锁信息
func (s *Server) getLockInfo(regionID uint64, key []byte) (*titankvpb.LockInfo, error) {
	encKey := raftstore.DataKey(regionID, key)
	valBytes, err := s.store.GetLockCF(encKey) // 需要实现
	if err != nil {
		return nil, err
	}

	if len(valBytes) < 4+8+8+1 {
		return nil, fmt.Errorf("bad lock val")
	}
	primaryLen := binary.LittleEndian.Uint32(valBytes[:4])
	if int(4+primaryLen+8+8+1) > len(valBytes) {
		return nil, fmt.Errorf("bad lock val")
	}
	offset := 4
	primary := valBytes[offset : offset+int(primaryLen)]
	offset += int(primaryLen)
	startTS := binary.LittleEndian.Uint64(valBytes[offset : offset+8])
	offset += 8
	ttl := binary.LittleEndian.Uint64(valBytes[offset : offset+8])
	offset += 8
	// op_type (1 byte)
	if offset+1 > len(valBytes) {
		// Compatible with old version or just ignore
	} else {
		offset += 1
	}

	var minCommitTs uint64
	if offset+8 <= len(valBytes) {
		minCommitTs = binary.LittleEndian.Uint64(valBytes[offset : offset+8])
		offset += 8
	}

	var forUpdateTs uint64
	if offset+8 <= len(valBytes) {
		forUpdateTs = binary.LittleEndian.Uint64(valBytes[offset : offset+8])
		offset += 8
	}

	var secondaries [][]byte
	if offset < len(valBytes) {
		secCount, n := binary.Uvarint(valBytes[offset:])
		if n > 0 {
			offset += n
			for i := 0; i < int(secCount); i++ {
				secLen, n := binary.Uvarint(valBytes[offset:])
				if n <= 0 {
					break
				}
				offset += n
				if offset+int(secLen) > len(valBytes) {
					break
				}
				sec := valBytes[offset : offset+int(secLen)]
				secondaries = append(secondaries, sec)
				offset += int(secLen)
			}
		}
	}

	if rid, userKey, ok := raftstore.DecodeDataKey(primary); ok && rid == regionID {
		primary = userKey
	}

	return &titankvpb.LockInfo{
		PrimaryKey:     primary,
		LockVersion:    startTS,
		Ttl:            ttl,
		Key:            key,
		MinCommitTs:    minCommitTs,
		ForUpdateTs:    forUpdateTs,
		UseAsyncCommit: minCommitTs > 0,
		Secondaries:    secondaries,
	}, nil
}

func (s *Server) ExecuteCoprocessor(ctx context.Context, req *titankvpb.CoprocessorRequest) (*titankvpb.CoprocessorResponse, error) {
	if req.Context == nil {
		return nil, status.Error(codes.InvalidArgument, "missing region context")
	}

	// 1. Convert Proto Enum to Internal Enum
	var copType store.CoprocessorType
	switch req.Type {
	case titankvpb.CoprocessorRequest_COUNT:
		copType = store.CoprocessorTypeCount
	case titankvpb.CoprocessorRequest_SUM:
		copType = store.CoprocessorTypeSum
	default:
		return nil, status.Error(codes.InvalidArgument, "unknown coprocessor type")
	}

	var filterOp store.FilterOperator
	switch req.FilterOperator {
	case titankvpb.CoprocessorRequest_EQUAL:
		filterOp = store.FilterOperatorEqual
	case titankvpb.CoprocessorRequest_NOT_EQUAL:
		filterOp = store.FilterOperatorNotEqual
	case titankvpb.CoprocessorRequest_GREATER:
		filterOp = store.FilterOperatorGreater
	case titankvpb.CoprocessorRequest_LESS:
		filterOp = store.FilterOperatorLess
	case titankvpb.CoprocessorRequest_GREATER_OR_EQUAL:
		filterOp = store.FilterOperatorGreaterOrEqual
	case titankvpb.CoprocessorRequest_LESS_OR_EQUAL:
		filterOp = store.FilterOperatorLessOrEqual
	default:
		filterOp = store.FilterOperatorEqual
	}

	// 2. Encode Keys with Region ID
	regionID := req.Context.RegionId

	var startKey []byte
	if len(req.StartKey) > 0 {
		startKey = raftstore.DataKey(regionID, req.StartKey)
	} else {
		startKey = raftstore.DataKey(regionID, nil)
	}

	var endKey []byte
	if len(req.EndKey) > 0 {
		endKey = raftstore.DataKey(regionID, req.EndKey)
	} else {
		endKey = raftstore.DataKey(regionID+1, nil)
	}

	// 3. Construct Store Request
	storeReq := &store.CoprocessorRequest{
		Type:           copType,
		StartKey:       startKey,
		EndKey:         endKey,
		StartTS:        req.StartTs,
		FilterValue:    req.FilterValue,
		FilterOperator: filterOp,
	}

	// 4. Call Store
	resp, err := s.store.ExecuteCoprocessor(storeReq)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// 5. Construct Response
	return &titankvpb.CoprocessorResponse{
		Count: resp.Count,
		Sum:   resp.Sum,
	}, nil
}
