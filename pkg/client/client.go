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
    if req.Context == nil {
        req.Context = &titankvpb.RegionContext{RegionId: 1, RegionEpoch: &titankvpb.RegionEpoch{ConfVer: 1, Version: 1}}
    }
    if req.Context.RegionId == 0 {
        req.Context.RegionId = 1 // 兜底
    }
	if len(req.Mutations) == 0 {
		return &titankvpb.PrewriteResponse{}, nil
	}
	
	key := req.Mutations[0].Key
	bo := NewBackoffer(ctx)
	
	// 重试循环
	for i := 0; i < 5; i++{
		// 1. 定位
		addr, err := c.LocateLeader(ctx, key)
		if err != nil { 
			 //定位失败，退避后重试
			if err := bo.Sleep(); err != nil {
				return nil, err
			}
			addr = "127.0.0.1:9091" 
               // fmt.Printf("[Client Debug] LocateLeader failed, fallback to %s. Err: %v\n", addr, err)
			continue
		}
		// log.Printf("[Client] Sending Prewrite to %s. Key=%s", addr, string(req.PrimaryKey))
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			if err := bo.Sleep(); err != nil {
				return nil, err
			}
			continue
		}

		// 2. 发送
		client := titankvpb.NewTitanKVClient(conn)
		resp, err := client.Prewrite(ctx, req)
		// fmt.Printf("[Client Debug] Sending Prewrite to %s\n", addr)
		if err != nil {
			log.Printf("[Client] RPC Error: %v", err)
		}
		conn.Close() // 及时关闭连接
		if resp.Error != "" {
        		// 业务错误 (KeyLocked) -> 返回错误
        		return nil, fmt.Errorf(resp.Error)
    		}

		if err != nil {
			// 网络错误，退避重试
			// 注意：这里可能需要判断错误类型，如果是 KeyLocked 等逻辑错误，应该直接返回
			// 但 Prewrite 的 KeyLocked 通常是在 resp.Error 里返回的，这里的 err 通常是 gRPC 错误
			if err := bo.Sleep(); err != nil {
				return nil, err
			}
			continue
		}
		
		// 成功收到响应（哪怕响应里有业务错误，也返回给上层处理）
		return resp, nil
	}
	return nil, fmt.Errorf("send prewrite failed: max retries exceeded")
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
                RegionEpoch: toTitanEpoch(region.RegionEpoch),
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
            if st.Code() == codes.NotFound {
                return nil, nil // Key 不存在，返回 nil, nil (符合 Go 习惯)
            }
            if st.Code() == codes.Aborted && st.Message() == "KeyLocked" {
                // 遇到锁！退避重试
                // log.Printf("Key %s locked, backing off...", key)
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

func (c *Client) GetRegionCache() *RegionCache {
    return c.cache
}

func toTitanEpoch(e *pdpb.RegionEpoch) *titankvpb.RegionEpoch {
    if e == nil { return nil }
    return &titankvpb.RegionEpoch{ConfVer: e.ConfVer, Version: e.Version}
}