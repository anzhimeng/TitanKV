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
	// 1. 查缓存
	region, leader := c.cache.Search(key)
	if region != nil && leader != nil {
		addr := c.cache.GetStoreAddr(leader.StoreId)
		if addr != "" {
			return addr, nil
		}
	}

	// 2. 缓存未命中，查 PD
	// (注意：这部分 Server 端逻辑还没写，我们在 Week 9 实现，这里先写 Client 端逻辑)
	// resp, err := c.pdClient.GetRegion(ctx, &pdpb.GetRegionRequest{Key: key})
	// if err != nil { return "", err }
	
	// 模拟 PD 返回 (Day 5 Mock)
	// 假设 PD 告诉我们 Key 属于 Region 1, Leader 在 Node 1
	// 实际上这里应该调用 RPC
	
	// mockRegion := &pdpb.Region{Id: 1, StartKey: []byte(""), EndKey: []byte("")}
	// mockLeader := &pdpb.Peer{Id: 1, StoreId: 1}
	// c.cache.UpdateRegion(mockRegion, mockLeader)
	// c.cache.UpdateStore(1, "127.0.0.1:9091")

	// return "127.0.0.1:9091", nil
	return "", fmt.Errorf("PD GetRegion not implemented yet (Week 9)")
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

// 快照读 (Week 14 Server 端实现后才能真正跑通)
func (c *Client) SnapshotGet(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
    // 构造请求，带上 TS
    // ...
    // 这里先 Mock 或者留空，等待 Week 14
    return nil, fmt.Errorf("SnapshotGet not implemented yet")
}

func (c *Client) SendPrewrite(ctx context.Context, req *titankvpb.PrewriteRequest) (*titankvpb.PrewriteResponse, error) {
	// 使用第一个 Key 来定位 Leader
    // (因为经过 GroupByRegion，这个 Batch 里的所有 Key 都属于同一个 Region)
	key := req.Mutations[0].Key
    
	// 1. 定位
	addr, err := c.LocateLeader(ctx, key)
	if err != nil {
        // 简单的重试策略或 fallback
		addr = "127.0.0.1:9091" 
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil { return nil, err }
	defer conn.Close()
    
	// 2. 发送
	return titankvpb.NewTitanKVClient(conn).Prewrite(ctx, req)
}

// 定向发送 Commit
func (c *Client) SendCommit(ctx context.Context, req *titankvpb.CommitRequest) (*titankvpb.CommitResponse, error) {
	key := req.Keys[0]

    // 如果 Request 里还没填 Context (针对 Primary Commit 的情况)，尝试填充
    if req.Context == nil {
        region, _ := c.cache.Search(key)
        if region != nil {
            req.Context = &titankvpb.RegionContext{
                RegionId:    region.Id,
                RegionEpoch: region.RegionEpoch,
            }
        } else {
             // Fallback
             req.Context = &titankvpb.RegionContext{RegionId: 1}
        }
    }

	addr, err := c.LocateLeader(ctx, key)
	if err != nil {
		addr = "127.0.0.1:9091"
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil { return nil, err }
	defer conn.Close()

	return titankvpb.NewTitanKVClient(conn).Commit(ctx, req)
}

func (c *Client) GetRegionCache() *RegionCache {
    return c.cache
}