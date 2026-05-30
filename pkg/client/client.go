package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	tsBatchSize               = 262144
	grpcReadBufferSize        = 2 * 1024 * 1024
	grpcWriteBufferSize       = 2 * 1024 * 1024
	grpcInitialWindowSize     = 16 << 20
	grpcInitialConnWindowSize = 16 << 20
)

func dialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithReadBufferSize(grpcReadBufferSize),
		grpc.WithWriteBufferSize(grpcWriteBufferSize),
		grpc.WithInitialWindowSize(grpcInitialWindowSize),
		grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
	}
}

type Client struct {
	pdClient   pdpb.PDClient
	cache      *RegionCache
	connMu     sync.Mutex
	conns      map[string]*grpc.ClientConn
	kvClients  map[string]titankvpb.TitanKVClient
	tsMu       sync.Mutex
	tsPhysical int64
	tsLogical  int64
	tsRemain   uint32
	tsFetching int32
	tsCond     *sync.Cond
	stats      ConflictStats

	// Async TSO pre-fetching
	tsBatchCh chan tsResult
	tsReqCh   chan struct{}
}

type tsResult struct {
	resp *pdpb.GetTSResponse
	err  error
}

type ConflictStats struct {
	PrewriteKeyLocked     uint64
	PrewriteKeyLockedPri  uint64
	PrewriteKeyLockedSec  uint64
	PrewriteWriteConflict uint64
	PrewriteWritePri      uint64
	PrewriteWriteSec      uint64
	PrewriteEpochNotMatch uint64
	CommitKeyLocked       uint64
	CommitLockMismatch    uint64
	CommitEpochNotMatch   uint64
	GetKeyLocked          uint64
	GetKeyLockedPri       uint64
	GetKeyLockedSec       uint64
	GetEpochNotMatch      uint64
	ResolveNoAction       uint64
	ResolveRollback       uint64
	ResolveCommit         uint64
	ResolveLockNotExist   uint64
	ResolveTtlExpire      uint64
	ResolveError          uint64
}

func (c *Client) GetConflictStats() ConflictStats {
	return ConflictStats{
		PrewriteKeyLocked:     atomic.LoadUint64(&c.stats.PrewriteKeyLocked),
		PrewriteKeyLockedPri:  atomic.LoadUint64(&c.stats.PrewriteKeyLockedPri),
		PrewriteKeyLockedSec:  atomic.LoadUint64(&c.stats.PrewriteKeyLockedSec),
		PrewriteWriteConflict: atomic.LoadUint64(&c.stats.PrewriteWriteConflict),
		PrewriteWritePri:      atomic.LoadUint64(&c.stats.PrewriteWritePri),
		PrewriteWriteSec:      atomic.LoadUint64(&c.stats.PrewriteWriteSec),
		PrewriteEpochNotMatch: atomic.LoadUint64(&c.stats.PrewriteEpochNotMatch),
		CommitKeyLocked:       atomic.LoadUint64(&c.stats.CommitKeyLocked),
		CommitLockMismatch:    atomic.LoadUint64(&c.stats.CommitLockMismatch),
		CommitEpochNotMatch:   atomic.LoadUint64(&c.stats.CommitEpochNotMatch),
		GetKeyLocked:          atomic.LoadUint64(&c.stats.GetKeyLocked),
		GetKeyLockedPri:       atomic.LoadUint64(&c.stats.GetKeyLockedPri),
		GetKeyLockedSec:       atomic.LoadUint64(&c.stats.GetKeyLockedSec),
		GetEpochNotMatch:      atomic.LoadUint64(&c.stats.GetEpochNotMatch),
		ResolveNoAction:       atomic.LoadUint64(&c.stats.ResolveNoAction),
		ResolveRollback:       atomic.LoadUint64(&c.stats.ResolveRollback),
		ResolveCommit:         atomic.LoadUint64(&c.stats.ResolveCommit),
		ResolveLockNotExist:   atomic.LoadUint64(&c.stats.ResolveLockNotExist),
		ResolveTtlExpire:      atomic.LoadUint64(&c.stats.ResolveTtlExpire),
		ResolveError:          atomic.LoadUint64(&c.stats.ResolveError),
	}
}
type putTask struct {
	ctx   context.Context
	key   []byte
	value []byte
	resp  chan error
}

type Pipeline struct {
	client *Client
	tasks  chan putTask
	wg     sync.WaitGroup
}

type WriteStream struct {
	client   *Client
	stream   titankvpb.TitanKV_WriteClient
	regionID uint64
}

func NewPipeline(c *Client, workers int, queueSize int) *Pipeline {
	if workers <= 0 {
		workers = 4
	}
	if queueSize <= 0 {
		queueSize = 1024
	}
	p := &Pipeline{
		client: c,
		tasks:  make(chan putTask, queueSize),
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for task := range p.tasks {
				err := p.client.Put(task.ctx, task.key, task.value)
				task.resp <- err
				close(task.resp)
			}
		}()
	}
	return p
}

func (p *Pipeline) PutAsync(ctx context.Context, key, value []byte) <-chan error {
	ch := make(chan error, 1)
	p.tasks <- putTask{ctx: ctx, key: key, value: value, resp: ch}
	return ch
}

func (p *Pipeline) Close() {
	close(p.tasks)
	p.wg.Wait()
}

func (c *Client) OpenWriteStream(ctx context.Context, regionID uint64, key []byte) (*WriteStream, error) {
	addr, err := c.getAddrForReq(ctx, regionID, key)
	if err != nil {
		return nil, err
	}
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	stream, err := titankvpb.NewTitanKVClient(conn).Write(ctx)
	if err != nil {
		return nil, err
	}
	return &WriteStream{client: c, stream: stream, regionID: regionID}, nil
}

func (ws *WriteStream) Send(ctx context.Context, mutations []*titankvpb.Mutation) error {
	if len(mutations) == 0 {
		return nil
	}
	key := mutations[0].Key
	if ws.regionID == 0 {
		if len(key) > 0 {
			_, _ = ws.client.LocateLeader(ctx, key)
		}
		if region, _ := ws.client.cache.Search(key); region != nil {
			ws.regionID = region.Id
		} else {
			ws.regionID = 1
		}
	}
	var epoch *titankvpb.RegionEpoch
	if region, _ := ws.client.cache.Search(key); region != nil && region.RegionEpoch != nil {
		epoch = &titankvpb.RegionEpoch{
			ConfVer: region.RegionEpoch.ConfVer,
			Version: region.RegionEpoch.Version,
		}
	}
	req := &titankvpb.WriteRequest{
		Context: &titankvpb.RegionContext{
			RegionId:    ws.regionID,
			RegionEpoch: epoch,
		},
		Mutations: mutations,
	}
	if err := ws.stream.Send(req); err != nil {
		return err
	}
	resp, err := ws.stream.Recv()
	if err != nil {
		return err
	}
	if resp.Error != "" {
		if strings.Contains(resp.Error, "epoch") {
			ws.client.cache.Invalidate(key)
		}
		return errors.New(resp.Error)
	}
	return nil
}

func (ws *WriteStream) CloseSend() error {
	return ws.stream.CloseSend()
}

func NewClient(pdAddr string) (*Client, error) {
	conn, err := grpc.Dial(pdAddr, dialOptions()...)
	if err != nil {
		return nil, err
	}

	c := &Client{
		pdClient:  pdpb.NewPDClient(conn),
		cache:     NewRegionCache(),
		conns:     make(map[string]*grpc.ClientConn),
		kvClients: make(map[string]titankvpb.TitanKVClient),
		tsBatchCh: make(chan tsResult, 1),
		tsReqCh:   make(chan struct{}, 1),
	}
	c.tsCond = sync.NewCond(&c.tsMu)
	
	// Start TSO pre-fetcher
	go c.tsLoop()
	
	return c, nil
}

func (c *Client) tsLoop() {
	for range c.tsReqCh {
		req := &pdpb.GetTSRequest{Count: tsBatchSize}
		resp, err := c.pdClient.GetTS(context.Background(), req)
		c.tsBatchCh <- tsResult{resp: resp, err: err}
	}
}

// 核心逻辑：定位 Key 所在的 Leader 地址
func (c *Client) LocateLeader(ctx context.Context, key []byte) (string, error) {
	// 1. 查本地缓存
	region, leader := c.cache.Search(key)
	if region != nil && leader != nil {
		addr := c.cache.GetStoreAddr(leader.StoreId)
		if addr != "" {
			return addr, nil
		}
	}

	// 2. 缓存未命中，查 PD (RPC)
	req := &pdpb.GetRegionRequest{Key: key}
	var lastErr error
	// Retry up to 20 times (approx 10 seconds) to handle cluster bootstrap or leader election delays
	for i := 0; i < 20; i++ {
		resp, err := c.pdClient.GetRegion(ctx, req)
		if err != nil {
			lastErr = fmt.Errorf("PD GetRegion failed: %v", err)
			// Don't break immediately on network error, retry might help
		} else if resp == nil || resp.Region == nil {
			c.cache.Invalidate(key)
			lastErr = fmt.Errorf("PD returned nil region")
		} else if resp.Leader == nil {
			c.cache.Invalidate(key)
			lastErr = fmt.Errorf("no leader for region %d", resp.Region.Id)
		} else {
			c.cache.UpdateRegion(resp.Region, resp.Leader)

			addr := c.cache.GetStoreAddr(resp.Leader.StoreId)
			if addr != "" {
				return addr, nil
			}

			storeReq := &pdpb.GetStoreRequest{StoreId: resp.Leader.StoreId}
			storeResp, err := c.pdClient.GetStore(ctx, storeReq)
			if err != nil {
				// If GetStore fails, we can retry loop
				lastErr = fmt.Errorf("PD GetStore failed: %v", err)
			} else if storeResp == nil || storeResp.Store == nil {
				lastErr = fmt.Errorf("PD returned nil store for %d", resp.Leader.StoreId)
			} else {
				addr = storeResp.Store.Address
				c.cache.UpdateStore(resp.Leader.StoreId, addr)
				return addr, nil
			}
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(time.Duration(i+1) * 100 * time.Millisecond):
		}
	}
	if lastErr != nil {
		return "", lastErr
	}

	return "", fmt.Errorf("region not found")
}

// 智能 Put
func (c *Client) Put(ctx context.Context, key, value []byte) error {
	bo := NewBackoffer(ctx)
	for i := 0; i < 8; i++ {
		addr, err := c.LocateLeader(ctx, key)
		if err != nil {
			c.cache.Invalidate(key)
			if bo.Sleep() != nil {
				return err
			}
			addr = "127.0.0.1:9091"
		}

		kvClient, err := c.getKVClient(addr)
		if err != nil {
			if bo.Sleep() != nil {
				return err
			}
			continue
		}

		region, _ := c.cache.Search(key)
		if region == nil {
			c.cache.Invalidate(key)
			if bo.Sleep() != nil {
				return fmt.Errorf("region not found")
			}
			continue
		}

		// 【新增】RegionEpoch 类型转换
		var epoch *titankvpb.RegionEpoch
		if region != nil && region.RegionEpoch != nil {
			epoch = &titankvpb.RegionEpoch{
				ConfVer: region.RegionEpoch.ConfVer,
				Version: region.RegionEpoch.Version,
			}
		}

		// 构造请求
		req := &titankvpb.PutRequest{
			Context: &titankvpb.RegionContext{
				RegionId:    region.Id,
				RegionEpoch: epoch, // 使用转换后的 epoch
			},
			Key:   key,
			Value: value,
		}

		// 【关键】使用 kvClient 调用
		resp, err := kvClient.Put(ctx, req)

		if err != nil {
			c.cache.Invalidate(key)
			if bo.Sleep() != nil {
				return err
			}
			continue
		}

		if resp.ErrCode == 1 {
			c.cache.Invalidate(key)
			if bo.Sleep() != nil {
				return fmt.Errorf("epoch mismatch")
			}
			continue
		}

		return nil
	}
	return fmt.Errorf("max retries exceeded")
}

// 获取 TSO
func (c *Client) GetTS(ctx context.Context) (uint64, error) {
	for {
		c.tsMu.Lock()
		
		// 1. Try to trigger pre-fetch if cache is low
		if c.tsRemain < tsBatchSize/4 {
			select {
			case c.tsReqCh <- struct{}{}:
			default:
			}
		}

		if c.tsRemain > 0 {
			ts := uint64(c.tsPhysical)<<18 | uint64(c.tsLogical)
			c.tsLogical++
			c.tsRemain--
			c.tsMu.Unlock()
			return ts, nil
		}
		
		// 2. Cache empty. Need to fetch.
		// Ensure a request is inflight
		select {
		case c.tsReqCh <- struct{}{}:
		default:
		}

		// 3. Elect a leader to wait for the batch
		if atomic.CompareAndSwapInt32(&c.tsFetching, 0, 1) {
			c.tsMu.Unlock()
			
			// Wait for result from pre-fetcher
			res := <-c.tsBatchCh
			
			c.tsMu.Lock()
			atomic.StoreInt32(&c.tsFetching, 0)
			
			if res.err != nil {
				c.tsCond.Broadcast() // Wake up others (they will retry or fail)
				c.tsMu.Unlock()
				return 0, res.err
			}
			
			if res.resp == nil || res.resp.Timestamp == nil || res.resp.Count == 0 {
				c.tsCond.Broadcast()
				c.tsMu.Unlock()
				return 0, fmt.Errorf("invalid pd tso response")
			}

			c.tsPhysical = res.resp.Timestamp.Physical
			c.tsLogical = res.resp.Timestamp.Logical - int64(res.resp.Count) + 1
			c.tsRemain = res.resp.Count
			
			// Allocate one for self
			ts := uint64(c.tsPhysical)<<18 | uint64(c.tsLogical)
			c.tsLogical++
			c.tsRemain--
			
			c.tsCond.Broadcast()
			c.tsMu.Unlock()
			return ts, nil
			
		} else {
			// Follower: wait for leader to fill cache
			c.tsCond.Wait()
			c.tsMu.Unlock()
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			continue
		}
	}
}

func (c *Client) SendPrewrite(ctx context.Context, req *titankvpb.PrewriteRequest) (*titankvpb.PrewriteResponse, error) {
	if len(req.Mutations) == 0 {
		return &titankvpb.PrewriteResponse{}, nil
	}
	key := req.Mutations[0].Key

	// 2. 获取 RegionID (如果有的话)
	var regionID uint64
	if req.Context != nil {
		regionID = req.Context.RegionId
	}

	// 填充默认 Context (如果 Transaction 层没填)
	if req.Context == nil {
		req.Context = &titankvpb.RegionContext{RegionId: 1, RegionEpoch: &titankvpb.RegionEpoch{ConfVer: 1, Version: 1}}
	}

	bo := NewBackoffer(ctx)
	var lastErr error
	for i := 0; i < 5; i++ {
		// 【修改】使用 getAddrForReq
		addr, err := c.getAddrForReq(ctx, regionID, key)
		if err != nil {
			if bo.Sleep() != nil {
				return nil, err
			}
			addr = "127.0.0.1:9091"
		}

		kvClient, err := c.getKVClient(addr)
		if err != nil {
			lastErr = err
			if bo.Sleep() != nil {
				return nil, lastErr
			}
			continue
		}
		resp, err := kvClient.Prewrite(ctx, req)

		if err != nil {
			// 网络错误，重试
			c.cache.Invalidate(key) // 可能是切主了
			lastErr = err
			if bo.Sleep() != nil {
				return nil, lastErr
			}
			continue
		}

		if resp != nil {
			if resp.KeyError != nil || resp.Error == "KeyLocked" {
				atomic.AddUint64(&c.stats.PrewriteKeyLocked, 1)
				if resp.KeyError != nil && resp.KeyError.LockInfo != nil {
					if bytes.Equal(resp.KeyError.LockInfo.PrimaryKey, resp.KeyError.LockInfo.Key) {
						atomic.AddUint64(&c.stats.PrewriteKeyLockedPri, 1)
					} else {
						atomic.AddUint64(&c.stats.PrewriteKeyLockedSec, 1)
					}
				}
			}
			if resp.Conflict != nil || resp.Error == "WriteConflict" {
				atomic.AddUint64(&c.stats.PrewriteWriteConflict, 1)
				if resp.Conflict != nil {
					if bytes.Equal(resp.Conflict.Key, resp.Conflict.Primary) {
						atomic.AddUint64(&c.stats.PrewriteWritePri, 1)
					} else {
						atomic.AddUint64(&c.stats.PrewriteWriteSec, 1)
					}
				}
			}
			if resp.Error == "EpochNotMatch" {
				atomic.AddUint64(&c.stats.PrewriteEpochNotMatch, 1)
			}
		}
		return resp, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("send prewrite max retries exceeded")
}

func (c *Client) AcquirePessimisticLock(ctx context.Context, req *titankvpb.AcquirePessimisticLockRequest) (*titankvpb.AcquirePessimisticLockResponse, error) {
	if len(req.Mutations) == 0 {
		return &titankvpb.AcquirePessimisticLockResponse{}, nil
	}
	key := req.Mutations[0].Key

	var regionID uint64
	if req.Context != nil {
		regionID = req.Context.RegionId
	}

	if req.Context == nil {
		req.Context = &titankvpb.RegionContext{RegionId: 1, RegionEpoch: &titankvpb.RegionEpoch{ConfVer: 1, Version: 1}}
	}

	bo := NewBackoffer(ctx)
	var lastErr error
	for i := 0; i < 5; i++ {
		addr, err := c.getAddrForReq(ctx, regionID, key)
		if err != nil {
			if bo.Sleep() != nil {
				return nil, err
			}
			addr = "127.0.0.1:9091"
		}

		kvClient, err := c.getKVClient(addr)
		if err != nil {
			lastErr = err
			if bo.Sleep() != nil {
				return nil, lastErr
			}
			continue
		}
		resp, err := kvClient.AcquirePessimisticLock(ctx, req)

		if err != nil {
			c.cache.Invalidate(key)
			lastErr = err
			if bo.Sleep() != nil {
				return nil, lastErr
			}
			continue
		}

		return resp, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("acquire pessimistic lock max retries exceeded")
}

// 定向发送 Commit
func (c *Client) SendCommit(ctx context.Context, req *titankvpb.CommitRequest) (*titankvpb.CommitResponse, error) {
	key := req.Keys[0]
	var regionID uint64
	if req.Context != nil {
		regionID = req.Context.RegionId
	}

	if req.Context == nil {
		req.Context = &titankvpb.RegionContext{RegionId: 1}
	}

	addr, err := c.getAddrForReq(ctx, regionID, key)
	if addr == "" {
		addr = "127.0.0.1:9091"
	}

	kvClient, err := c.getKVClient(addr)
	if err != nil {
		return nil, err
	}

	resp, err := kvClient.Commit(ctx, req)
	if err != nil {
		st, _ := status.FromError(err)
		if st.Code() == codes.Aborted && st.Message() == "EpochNotMatch" {
			atomic.AddUint64(&c.stats.CommitEpochNotMatch, 1)
		}
		return nil, err
	}
	if resp != nil && resp.Error != "" {
		if strings.Contains(resp.Error, "Lock mismatch") {
			atomic.AddUint64(&c.stats.CommitLockMismatch, 1)
		}
		if strings.Contains(resp.Error, "Key is locked") {
			atomic.AddUint64(&c.stats.CommitKeyLocked, 1)
		}
		if resp.Error == "EpochNotMatch" {
			atomic.AddUint64(&c.stats.CommitEpochNotMatch, 1)
		}
	}
	return resp, nil
}

func (c *Client) SendCommitBatch(ctx context.Context, reqs []*titankvpb.CommitRequest, workers int) error {
	if len(reqs) == 0 {
		return nil
	}
	if workers <= 0 {
		workers = 4
	}
	if workers > len(reqs) {
		workers = len(reqs)
	}

	g, gctx := errgroup.WithContext(ctx)
	bufferSize := workers * 2
	if bufferSize > len(reqs) {
		bufferSize = len(reqs)
	}
	if bufferSize < 1 {
		bufferSize = 1
	}
	reqCh := make(chan *titankvpb.CommitRequest, bufferSize)

	for i := 0; i < workers; i++ {
		g.Go(func() error {
			for {
				select {
				case <-gctx.Done():
					return gctx.Err()
				case req, ok := <-reqCh:
					if !ok {
						return nil
					}
					if _, err := c.SendCommit(gctx, req); err != nil {
						return err
					}
				}
			}
		})
	}

	for _, req := range reqs {
		select {
		case <-gctx.Done():
			close(reqCh)
			return gctx.Err()
		case reqCh <- req:
		}
	}
	close(reqCh)
	return g.Wait()
}

func (c *Client) SnapshotGet(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	bo := NewBackoffer(ctx)
	for i := 0; i < 3; i++ { // 重试 3 次
		// 1. 定位路由
		var regionID uint64
		region, _ := c.cache.Search(key)
		if region != nil {
			regionID = region.Id
		}
		addr, err := c.getAddrForReq(ctx, regionID, key)
		if err != nil {
			addr = "127.0.0.1:9091"
		}
		if region == nil {
			region, _ = c.cache.Search(key)
		}

		kvClient, err := c.getKVClient(addr)
		if err != nil {
			return nil, err
		}

		// 2. 构造请求
		// 填充 RegionContext
		var context *titankvpb.RegionContext
		if region != nil {
			context = &titankvpb.RegionContext{
				RegionId:    region.Id,
				RegionEpoch: toTitanEpoch(region.RegionEpoch),
			}
		} else {
			context = &titankvpb.RegionContext{RegionId: 1}
		}

		req := &titankvpb.GetRequest{
			Context: context,
			Key:     key,
			StartTs: ts, // 【关键】传入事务开始时间
		}

		// 3. 发送请求
		resp, err := kvClient.Get(ctx, req)

		if err != nil {
			// 处理错误
			st, _ := status.FromError(err)
			if st.Code() == codes.NotFound {
				return nil, nil
			}
			if strings.Contains(st.Message(), "Failed to open file") {
				return nil, nil
			}
			if st.Code() == codes.Aborted && st.Message() == "KeyLocked" {
				atomic.AddUint64(&c.stats.GetKeyLocked, 1)
				// 解析 Details
				for _, detail := range st.Details() {
					if keyErr, ok := detail.(*titankvpb.KeyError); ok {
						if keyErr.LockInfo != nil {
							if bytes.Equal(keyErr.LockInfo.PrimaryKey, keyErr.LockInfo.Key) {
								atomic.AddUint64(&c.stats.GetKeyLockedPri, 1)
							} else {
								atomic.AddUint64(&c.stats.GetKeyLockedSec, 1)
							}
						}
						// 捕获 ResolveLocks 的返回值
						resolved, resolveErr := c.ResolveLocks(ctx, keyErr.LockInfo)
						if resolveErr != nil {
							return nil, resolveErr
						}
						if resolved {
							continue // 重试
						}
					}
				}
				if boErr := bo.Sleep(); boErr != nil {
					return nil, boErr // 超时放弃
				}
				continue
			}
			if st.Code() == codes.Aborted && st.Message() == "EpochNotMatch" {
				atomic.AddUint64(&c.stats.GetEpochNotMatch, 1)
				c.cache.Invalidate(key)
				time.Sleep(50 * time.Millisecond)
				continue
			}

			// 网络错误等
			time.Sleep(50 * time.Millisecond)
			continue
		}

		return resp.Value, nil
	}
	return nil, fmt.Errorf("snapshot get max retries exceeded")
}

// 遇到锁时的处理逻辑
// lockInfo: 从 Prewrite/Get 错误中解析出的锁信息
func (c *Client) ResolveLocks(ctx context.Context, lockInfo *titankvpb.LockInfo) (bool, error) {
	if lockInfo == nil {
		return false, fmt.Errorf("nil lock info")
	}
	// 1. 检查 Primary 状态
	// 需要定位 Primary Key 所在的 Leader
	// lockInfo.PrimaryKey

	// 构造 CheckTxnStatusRequest
	// 当前时间：从 PD 获取或者用本地时间（不准）
	// 正确做法：PD GetTS。为了快，用 lockInfo.LockVersion 估算? 不行。
	// 必须去 PD 拿个 TSO 作为 current_ts。
	currentTS, err := c.GetTS(ctx)
	if err != nil {
		return false, err
	}
	checkReq := &titankvpb.CheckTxnStatusRequest{
		PrimaryKey: lockInfo.PrimaryKey,
		LockTs:     lockInfo.LockVersion,
		CurrentTs:  currentTS,
	}

	// 发送 CheckTxnStatus (定向发送给 Primary)
	checkResp, err := c.SendCheckTxnStatus(ctx, checkReq) // 需实现
	if err != nil {
		atomic.AddUint64(&c.stats.ResolveError, 1)
		return false, err
	}
	// 2. 根据 Action 决定
	if checkResp.Action == titankvpb.CheckTxnStatusResponse_NoAction {
		atomic.AddUint64(&c.stats.ResolveNoAction, 1)
		return false, nil // 等待，不重试（或者由上层 Backoff）
	}

	// 3. 执行 Resolve
	// 如果是 TTL Expire，Server 已经帮我们回滚了 Primary。
	// 我们需要清理当前的 Secondary Key (lockInfo.Key)。

	commitTS := checkResp.CommitTs // 如果是 Rollback，这里是 0
	if checkResp.Action == titankvpb.CheckTxnStatusResponse_Rollback ||
		checkResp.Action == titankvpb.CheckTxnStatusResponse_LockNotExist ||
		checkResp.Action == titankvpb.CheckTxnStatusResponse_TtlExpire {
		commitTS = 0
	}
	if checkResp.Action == titankvpb.CheckTxnStatusResponse_Rollback {
		atomic.AddUint64(&c.stats.ResolveRollback, 1)
	} else if checkResp.Action == titankvpb.CheckTxnStatusResponse_LockNotExist {
		atomic.AddUint64(&c.stats.ResolveLockNotExist, 1)
	} else if checkResp.Action == titankvpb.CheckTxnStatusResponse_TtlExpire {
		atomic.AddUint64(&c.stats.ResolveTtlExpire, 1)
	} else if checkResp.Action == titankvpb.CheckTxnStatusResponse_Commit {
		atomic.AddUint64(&c.stats.ResolveCommit, 1)
	}

	resolveReq := &titankvpb.ResolveLockRequest{
		StartTs:  lockInfo.LockVersion,
		CommitTs: commitTS,
		Keys:     [][]byte{lockInfo.Key}, // 清理阻挡我们的这把锁
	}

	// 发送 ResolveLock (定向发送给当前 Key)
	// 注意：lockInfo.Key 不一定是 Primary，所以要重新路由
	_, err = c.SendResolveLock(ctx, resolveReq) // 需实现
	if err != nil {
		atomic.AddUint64(&c.stats.ResolveError, 1)
		return false, err
	}

	return true, nil // 成功清理，上层可以立即重试
}

// 发送 CheckTxnStatus (发给 Primary Key 所在的 Leader)
func (c *Client) SendCheckTxnStatus(ctx context.Context, req *titankvpb.CheckTxnStatusRequest) (*titankvpb.CheckTxnStatusResponse, error) {
	key := req.PrimaryKey
	if req.Context == nil || req.Context.RegionId == 0 {
		if len(key) > 0 {
			_, _ = c.LocateLeader(ctx, key)
		}
		if region, _ := c.cache.Search(key); region != nil {
			req.Context = &titankvpb.RegionContext{
				RegionId:    region.Id,
				RegionEpoch: toTitanEpoch(region.RegionEpoch),
			}
		} else {
			req.Context = &titankvpb.RegionContext{RegionId: 1}
		}
	}

	regionID := req.Context.RegionId
	addr, _ := c.getAddrForReq(ctx, regionID, key)

	kvClient, err := c.getKVClient(addr)
	if err != nil {
		return nil, err
	}

	return kvClient.CheckTxnStatus(ctx, req)
}

// 注意：SendResolveLock 已经是 GroupByRegion 的产物了 (Client.ResolveLocks 调用它)
// ResolveLocks 内部会分组调用 SendResolveLock
func (c *Client) SendResolveLock(ctx context.Context, req *titankvpb.ResolveLockRequest) (*titankvpb.ResolveLockResponse, error) {
	if len(req.Keys) == 0 {
		return &titankvpb.ResolveLockResponse{}, nil
	}
	key := req.Keys[0]
	if req.Context == nil {
		region, _ := c.cache.Search(key)
		if region != nil {
			req.Context = &titankvpb.RegionContext{
				RegionId:    region.Id,
				RegionEpoch: toTitanEpoch(region.RegionEpoch),
			}
		} else {
			req.Context = &titankvpb.RegionContext{RegionId: 1}
		}
	}
	regionID := req.Context.RegionId

	addr, _ := c.getAddrForReq(ctx, regionID, key)

	kvClient, err := c.getKVClient(addr)
	if err != nil {
		return nil, err
	}

	return kvClient.ResolveLock(ctx, req)
}

func (c *Client) GetRegionCache() *RegionCache {
	return c.cache
}

// Follower Read Context Key
type followerReadKey struct{}

// WithFollowerRead returns a context that enables Follower Read
func WithFollowerRead(ctx context.Context) context.Context {
	return context.WithValue(ctx, followerReadKey{}, true)
}

func isFollowerRead(ctx context.Context) bool {
	val := ctx.Value(followerReadKey{})
	return val != nil && val.(bool)
}

// 统一的辅助发送逻辑
func (c *Client) getAddrForReq(ctx context.Context, regionID uint64, key []byte) (string, error) {
	// 0. Follower Read Logic
	if isFollowerRead(ctx) && len(key) > 0 {
		region, leader := c.cache.Search(key)
		if region != nil {
			// Select a candidate peer (prefer follower)
			var candidates []*pdpb.Peer
			for _, p := range region.Peers {
				if leader != nil && p.Id == leader.Id {
					continue
				}
				candidates = append(candidates, p)
			}

			var target *pdpb.Peer
			if len(candidates) > 0 {
				// Randomly select a follower
				target = candidates[rand.Intn(len(candidates))]
			} else if leader != nil {
				// Fallback to leader if no followers available
				target = leader
			} else if len(region.Peers) > 0 {
				// Fallback to any peer
				target = region.Peers[0]
			}

			if target != nil {
				addr := c.cache.GetStoreAddr(target.StoreId)
				if addr != "" {
					return addr, nil
				}
				// If address not in cache, try to fetch from PD
				storeResp, err := c.pdClient.GetStore(ctx, &pdpb.GetStoreRequest{StoreId: target.StoreId})
				if err == nil && storeResp != nil && storeResp.Store != nil {
					c.cache.UpdateStore(target.StoreId, storeResp.Store.Address)
					return storeResp.Store.Address, nil
				}
			}
		}
	}

	// 1. 优先尝试通过 RegionID 直接获取
	if regionID > 0 {
		addr := c.cache.GetLeaderAddr(regionID)
		if addr != "" {
			return addr, nil
		}
		leader := c.cache.GetLeader(regionID)
		if leader != nil {
			storeResp, err := c.pdClient.GetStore(ctx, &pdpb.GetStoreRequest{StoreId: leader.StoreId})
			if err != nil {
				return "", err
			}
			if storeResp != nil && storeResp.Store != nil && storeResp.Store.Address != "" {
				c.cache.UpdateStore(leader.StoreId, storeResp.Store.Address)
				return storeResp.Store.Address, nil
			}
		}
	}

	// 2. 缓存未命中，回退到 LocateKey (这会去 PD 查并更新缓存)
	if len(key) > 0 {
		addr, err := c.LocateLeader(ctx, key)
		if err == nil && addr != "" {
			return addr, nil
		}
	}

	// 3. 都没有，只能 Blind Guess (单机测试用)
	return "127.0.0.1:9091", nil
}

func (c *Client) getKVClient(addr string) (titankvpb.TitanKVClient, error) {
	if addr == "" {
		addr = "127.0.0.1:9091"
	}
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if client, ok := c.kvClients[addr]; ok {
		return client, nil
	}
	conn, ok := c.conns[addr]
	if !ok {
		var err error
		conn, err = grpc.Dial(addr, dialOptions()...)
		if err != nil {
			return nil, err
		}
		c.conns[addr] = conn
	}
	client := titankvpb.NewTitanKVClient(conn)
	c.kvClients[addr] = client
	return client, nil
}

func (c *Client) getConn(addr string) (*grpc.ClientConn, error) {
	if addr == "" {
		addr = "127.0.0.1:9091"
	}
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}
	conn, err := grpc.Dial(addr, dialOptions()...)
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	return conn, nil
}

func toPdpbEpoch(e *titankvpb.RegionEpoch) *pdpb.RegionEpoch {
	if e == nil {
		return &pdpb.RegionEpoch{}
	}
	return &pdpb.RegionEpoch{ConfVer: e.ConfVer, Version: e.Version}
}

func toTitanEpoch(e *pdpb.RegionEpoch) *titankvpb.RegionEpoch {
	if e == nil {
		return nil
	}
	return &titankvpb.RegionEpoch{
		ConfVer: e.ConfVer,
		Version: e.Version,
	}
}
