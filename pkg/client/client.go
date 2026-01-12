package client

import (
	"context"
	"fmt"
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
	for i := 0; i < 3; i++ { // 重试 3 次
		// 1. 定位
		addr, err := c.LocateLeader(ctx, key)
		if err != nil {
		    // 暂时 fallback 到硬编码地址用于测试 Week 8
            // 实际代码这里应该 return err 或者 backoff
			addr = "127.0.0.1:9091" 
		}

		// 2. 发送请求
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil { return err }
		kvClient := titankvpb.NewTitanKVClient(conn)
		
		resp, err := kvClient.Put(ctx, &titankvpb.PutRequest{Key: key, Value: value})
		conn.Close()

		if err != nil {
            // 网络错误，清除缓存，重试
			c.cache.Invalidate(key)
			continue
		}
		// 构造请求时带上 Context
        	region, _ := c.cache.Search(key)
       	req := &titankvpb.PutRequest{
          Context: &titankvpb.RegionContext{
                RegionId:    region.Id,
                RegionEpoch: region.RegionEpoch, // 带上 Client 认为的版本
            },
          Key: key, 
          Value: value,
          }
        
        resp, err := client.Put(ctx, req)
        
        // 检查错误
        if status.Code(err) == codes.Aborted && status.Convert(err).Message() == "EpochNotMatch" {
            // 【关键】Epoch 不匹配，说明路由过时了
            log.Printf("Epoch mismatch for key %s, invalidating cache...", key)
            c.cache.Invalidate(key)
            // 下一次循环会去 PD 重新拉取
            continue
        }
		
		// 3. 处理业务层错误 (NotLeader)
		if resp.ErrCode == 1 { // Not Leader
			// Server 告诉我们新 Leader 是谁，更新缓存
			// (需要 Server 返回 Leader 所在的 Store 地址，目前 Proto 里只有 LeaderId)
			// 这里简单处理：清除缓存，下一轮循环去问 PD
			c.cache.Invalidate(key)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		return nil
	}
	return fmt.Errorf("max retries exceeded")
}

// pkg/client/client.go

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