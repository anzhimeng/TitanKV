package client

import (
	"context"
	"fmt"
	"log" 
	"time"
	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
     "google.golang.org/grpc/codes"
)

type Client struct {
	pdClient pdpb.PDClient
	cache    *RegionCache
}

func NewClient(pdAddr string) (*Client, error) {
	conn, err := grpc.Dial(pdAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	
	return &Client{
		pdClient: pdpb.NewPDClient(conn),
		cache:    NewRegionCache(),
	}, nil
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
    resp, err := c.pdClient.GetRegion(ctx, req)
    if err != nil {
        return "", fmt.Errorf("PD GetRegion failed: %v", err)
    }
    
    // 3. 更新缓存
    if resp.Region != nil && resp.Leader != nil {
        c.cache.UpdateRegion(resp.Region, resp.Leader)
        
        // 还需要获取 Store 地址
        // 这一步也需要 RPC (GetStore)，或者我们假设 PD 返回的 Leader 包含地址信息？
        // Proto 中 Peer 只有 ID 和 StoreID。
        // 我们需要再调一次 GetStore。
        
        storeReq := &pdpb.GetStoreRequest{StoreId: resp.Leader.StoreId}
        storeResp, err := c.pdClient.GetStore(ctx, storeReq)
        if err != nil {
             return "", fmt.Errorf("PD GetStore failed: %v", err)
        }
        
        addr := storeResp.Store.Address
        c.cache.UpdateStore(resp.Leader.StoreId, addr)
        return addr, nil
    }

	return "", fmt.Errorf("region not found")
}

// 智能 Put
func (c *Client) Put(ctx context.Context, key, value []byte) error {
	for i := 0; i < 3; i++ {
		addr, err := c.LocateLeader(ctx, key)
		if err != nil {
			addr = "127.0.0.1:9091" 
		}

		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil { return err }
		
		// 【关键】重命名为 kvClient，避免与结构体字段 c.pdClient 混淆，或者与之前的 client 变量冲突
		kvClient := titankvpb.NewTitanKVClient(conn)
		
		region, _ := c.cache.Search(key)
        
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
		conn.Close()

		if err != nil {
			c.cache.Invalidate(key)
			continue
		}

		if resp.ErrCode == 1 { 
			log.Printf("Epoch mismatch for key %s, invalidating cache...", key)
			c.cache.Invalidate(key)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		return nil
	}
	return fmt.Errorf("max retries exceeded")
}


// 获取 TSO
func (c *Client) GetTS(ctx context.Context) (uint64, error) {
    req := &pdpb.GetTSRequest{Count: 1}
    resp, err := c.pdClient.GetTS(ctx, req)
    if err != nil {
        return 0, err
    }
    // 组合 Physical + Logical
    ts := uint64(resp.Timestamp.Physical) << 18 | uint64(resp.Timestamp.Logical)
    return ts, nil
}

func (c *Client) SendPrewrite(ctx context.Context, req *titankvpb.PrewriteRequest) (*titankvpb.PrewriteResponse, error) {
    key := req.Mutations[0].Key
    regionID := req.Context.RegionId // Transaction 层已经填好了

    // 填充默认 Context (如果 Transaction 层没填)
    if req.Context == nil {
        req.Context = &titankvpb.RegionContext{RegionId: 1, RegionEpoch: &titankvpb.RegionEpoch{ConfVer: 1, Version: 1}}
    }
    
    bo := NewBackoffer(ctx)
    for i := 0; i < 5; i++ {
        // 【修改】使用 getAddrForReq
        addr, err := c.getAddrForReq(ctx, regionID, key)
        if err != nil {
             if bo.Sleep() != nil { return nil, err }
             addr = "127.0.0.1:9091"
        }

        conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
        if err != nil {
            if bo.Sleep() != nil { return nil, err }
            continue
        }

        client := titankvpb.NewTitanKVClient(conn)
        resp, err := client.Prewrite(ctx, req)
        conn.Close()

        if err != nil {
            // 网络错误，重试
            c.cache.Invalidate(key) // 可能是切主了
            if bo.Sleep() != nil { return nil, err }
            continue
        }
        
        // 成功响应 (哪怕包含业务 Error)
        return resp, nil
    }
    return nil, fmt.Errorf("send prewrite max retries exceeded")
}

// 定向发送 Commit
func (c *Client) SendCommit(ctx context.Context, req *titankvpb.CommitRequest) (*titankvpb.CommitResponse, error) {
    key := req.Keys[0]
    regionID := req.Context.RegionId

    if req.Context == nil {
        req.Context = &titankvpb.RegionContext{RegionId: 1}
    }

    addr, _ := c.getAddrForReq(ctx, regionID, key)
    
    conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil { return nil, err }
    defer conn.Close()

    return titankvpb.NewTitanKVClient(conn).Commit(ctx, req)
}

func (c *Client) SnapshotGet(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
    bo := NewBackoffer(ctx)
    for i := 0; i < 3; i++ { // 重试 3 次
        // 1. 定位路由
        addr, err := c.LocateLeader(ctx, key)
        if err != nil {
            addr = "127.0.0.1:9091" // Fallback for test
        }

        conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
        if err != nil { return nil, err }
        kvClient := titankvpb.NewTitanKVClient(conn)

        // 2. 构造请求
        // 填充 RegionContext
        region, _ := c.cache.Search(key)
        var context *titankvpb.RegionContext
        if region != nil {
            context = &titankvpb.RegionContext{
                RegionId: region.Id,
                // 【修复】手动转换 Epoch 类型
                RegionEpoch: &titankvpb.RegionEpoch{
                    ConfVer: region.RegionEpoch.ConfVer,
                    Version: region.RegionEpoch.Version,
                },
            }
        } else {
             context = &titankvpb.RegionContext{RegionId: 1} // Fallback
        }
        
        req := &titankvpb.GetRequest{
            Context: context,
            Key:     key,
            StartTs: ts, // 【关键】传入事务开始时间
        }

        // 3. 发送请求
        resp, err := kvClient.Get(ctx, req)
        conn.Close()

        if err != nil {
            // 处理错误
            st, _ := status.FromError(err)
            log.Printf("[DEBUG] SnapshotGet Error: Code=%v, Msg=%s", st.Code(), st.Message())
            if st.Code() == codes.NotFound {
                return nil, nil // Key 不存在，返回 nil, nil (符合 Go 习惯)
            }
            if st.Code() == codes.Aborted && st.Message() == "KeyLocked" {
            	log.Printf("[Client] SnapshotGet hit lock. Details len: %d", len(st.Details()))
		      // 解析 Details
		      for _, detail := range st.Details() {
		          if keyErr, ok := detail.(*titankvpb.KeyError); ok {
		                // 捕获 ResolveLocks 的返回值
		                resolved, resolveErr := c.ResolveLocks(ctx, keyErr.LockInfo)
		                
		                // 打印 Resolve 的结果，而不是 Get 的错误
		                log.Printf("[Client] Resolve result: %v, Err: %v", resolved, resolveErr)
		                
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
    // 1. 检查 Primary 状态
    // 需要定位 Primary Key 所在的 Leader
    // lockInfo.PrimaryKey
    
    // 构造 CheckTxnStatusRequest
    // 当前时间：从 PD 获取或者用本地时间（不准）
    // 正确做法：PD GetTS。为了快，用 lockInfo.LockVersion 估算? 不行。
    // 必须去 PD 拿个 TSO 作为 current_ts。
    currentTS, err := c.GetTS(ctx)
    if err != nil { return false, err }
    log.Printf("[Client] Checking Txn Status: Primary=%s, LockTS=%d, CallerTS=%d", 
        string(lockInfo.PrimaryKey), lockInfo.LockVersion, currentTS)
    checkReq := &titankvpb.CheckTxnStatusRequest{
        PrimaryKey: lockInfo.PrimaryKey,
        LockTs:     lockInfo.LockVersion,
        CurrentTs:  currentTS,
    }
    
    // 发送 CheckTxnStatus (定向发送给 Primary)
    checkResp, err := c.SendCheckTxnStatus(ctx, checkReq) // 需实现
    if err != nil { return false, err }
    log.Printf("[Client] Check Result: Action=%v", checkResp.Action)
    // 2. 根据 Action 决定
    if checkResp.Action == titankvpb.CheckTxnStatusResponse_NoAction {
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

    resolveReq := &titankvpb.ResolveLockRequest{
        StartTs:  lockInfo.LockVersion,
        CommitTs: commitTS,
        Keys:     [][]byte{lockInfo.Key}, // 清理阻挡我们的这把锁
    }
    
    // 发送 ResolveLock (定向发送给当前 Key)
    // 注意：lockInfo.Key 不一定是 Primary，所以要重新路由
    log.Printf("[Client] Sending ResolveLock: CommitTS=%d (0=Rollback)", commitTS)
    _, err = c.SendResolveLock(ctx, resolveReq) // 需实现
    if err != nil { return false, err }

    return true, nil // 成功清理，上层可以立即重试
}

// 发送 CheckTxnStatus (发给 Primary Key 所在的 Leader)
func (c *Client) SendCheckTxnStatus(ctx context.Context, req *titankvpb.CheckTxnStatusRequest) (*titankvpb.CheckTxnStatusResponse, error) {
    key := req.PrimaryKey
    // CheckTxnStatus 可能没传 RegionID (Resolve 逻辑里)，所以主要靠 Key
    regionID := uint64(0)
    if req.Context != nil { regionID = req.Context.RegionId }

    if req.Context == nil {
        req.Context = &titankvpb.RegionContext{RegionId: 1}
    }

    addr, _ := c.getAddrForReq(ctx, regionID, key)
    
    conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil { return nil, err }
    defer conn.Close()

    return titankvpb.NewTitanKVClient(conn).CheckTxnStatus(ctx, req)
}

// 注意：SendResolveLock 已经是 GroupByRegion 的产物了 (Client.ResolveLocks 调用它)
// ResolveLocks 内部会分组调用 SendResolveLock
func (c *Client) SendResolveLock(ctx context.Context, req *titankvpb.ResolveLockRequest) (*titankvpb.ResolveLockResponse, error) {
    if len(req.Keys) == 0 { return &titankvpb.ResolveLockResponse{}, nil }
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

    conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil { return nil, err }
    defer conn.Close()

    return titankvpb.NewTitanKVClient(conn).ResolveLock(ctx, req)
}

func (c *Client) GetRegionCache() *RegionCache {
    return c.cache
}

// 统一的辅助发送逻辑
func (c *Client) getAddrForReq(ctx context.Context, regionID uint64, key []byte) (string, error) {
    // 1. 优先尝试通过 RegionID 直接获取
    if regionID > 0 {
        addr := c.cache.GetLeaderAddr(regionID)
        if addr != "" {
            return addr, nil
        }
    }
    
    // 2. 缓存未命中，回退到 LocateKey (这会去 PD 查并更新缓存)
    if len(key) > 0 {
        return c.LocateLeader(ctx, key)
    }
    
    // 3. 都没有，只能 Blind Guess (单机测试用)
    return "127.0.0.1:9091", nil
}

func toPdpbEpoch(e *titankvpb.RegionEpoch) *pdpb.RegionEpoch {
    if e == nil { return &pdpb.RegionEpoch{} }
    return &pdpb.RegionEpoch{ConfVer: e.ConfVer, Version: e.Version}
}

func toTitanEpoch(e *pdpb.RegionEpoch) *titankvpb.RegionEpoch {
    if e == nil { return nil }
    return &titankvpb.RegionEpoch{
        ConfVer: e.ConfVer,
        Version: e.Version,
    }
}