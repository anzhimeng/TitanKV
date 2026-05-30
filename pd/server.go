package pd

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"titankv/api/titankvpb"
	"titankv/pd/api/pdpb"
	"titankv/pd/cluster"
	"titankv/pd/id"
	"titankv/pd/schedule"
	"titankv/pd/schedule/schedulers"
	"titankv/pd/tso"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/embed"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	etcdTimeout = 3 * time.Second
)

type Server struct {
	pdpb.UnimplementedPDServer

	cfg    *Config
	etcd   *embed.Etcd
	client *clientv3.Client

	// 原子变量：1 表示我是 Leader，0 表示 Follower
	isLeader int64

	// 全局 Context，用于 Server 停止
	ctx    context.Context
	cancel context.CancelFunc

	// 核心组件
	tso         *tso.Allocator
	idAllocator *id.Allocator
	cluster     *cluster.RaftCluster
	coordinator *schedule.Coordinator
}

func NewServer(cfg *Config) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (s *Server) Run() error {
	// 1. 转换配置
	etcdCfg, err := s.cfg.GenEmbedEtcdConfig()
	if err != nil {
		return err
	}

	// Register PD Server to Etcd's gRPC server
	etcdCfg.ServiceRegister = func(gs *grpc.Server) {
		pdpb.RegisterPDServer(gs, s)
	}

	// 2. 启动嵌入式 Etcd
	log.Printf("Starting embedded etcd...")
	e, err := embed.StartEtcd(etcdCfg)
	if err != nil {
		return err
	}
	s.etcd = e

	// 3. 等待 Etcd 就绪
	select {
	case <-e.Server.ReadyNotify():
		log.Printf("Etcd is ready!")
	case <-time.After(60 * time.Second):
		e.Close()
		return fmt.Errorf("server took too long to start")
	}

	// 4. 创建 Client
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   s.cfg.ClientUrls,
		DialTimeout: etcdTimeout,
	})
	if err != nil {
		return err
	}
	s.client = client

	// 5. 初始化核心组件
	s.tso = tso.NewAllocator(s.client)
	s.idAllocator = id.NewAllocator(s.client)
	s.cluster = cluster.NewRaftCluster(s.client)

	if err := s.cluster.Load(s.ctx); err != nil {
		log.Printf("Load stores warning: %v", err)
	}
	// 注意：LoadRegions 是在 Cluster 中定义的，确保那里有这个方法
	// 如果没有，可以暂且忽略，因为 Heartbeat 会自动补全
	// if err := s.cluster.LoadRegions(s.ctx); err != nil { ... }

	// 初始化调度器
	s.coordinator = schedule.NewCoordinator(s.cluster)

	// 【关键修复 1】使用 schedulers 包名
	s.coordinator.AddScheduler(schedulers.NewBalanceLeaderScheduler())

	// 【关键修复 2】传入 s.idAllocator
	s.coordinator.AddScheduler(schedulers.NewBalanceRegionScheduler(s.idAllocator))

	s.coordinator.AddScheduler(schedulers.NewReplicaScheduler(s.idAllocator))

	// 6. 启动竞选
	go s.campaignLoop()

	return nil
}

func (s *Server) Close() {
	s.cancel()
	if s.client != nil {
		s.client.Close()
	}
	if s.etcd != nil {
		s.etcd.Close()
	}
}

// 检查自己是否是 Leader
func (s *Server) IsLeader() bool {
	return atomic.LoadInt64(&s.isLeader) == 1
}

func (s *Server) campaignLoop() {
	for {
		if s.ctx.Err() != nil {
			return
		}

		// 1. 创建 Session
		session, err := concurrency.NewSession(s.client, concurrency.WithTTL(5))
		if err != nil {
			log.Printf("Failed to create session: %v", err)
			time.Sleep(time.Second)
			continue
		}

		// 2. 创建 Election 对象
		election := concurrency.NewElection(session, "/pd/leader")

		// 3. 开始竞选
		log.Println("Campaigning for leader...")
		if err := election.Campaign(s.ctx, s.cfg.Name); err != nil {
			log.Printf("Campaign failed: %v", err)
			session.Close()
			continue
		}

		// 4. 当选成功！
		log.Println("I am the Leader!")
		atomic.StoreInt64(&s.isLeader, 1)

		// 5. 初始化 Leader 独占服务
		// 初始化 TSO
		if err := s.tso.Initialize(s.ctx); err != nil {
			log.Printf("Failed to initialize TSO: %v", err)
			atomic.StoreInt64(&s.isLeader, 0)
			session.Close()
			continue
		}

		// 创建 Leader 专用的 Context，退位时统一取消
		leaderCtx, cancel := context.WithCancel(s.ctx)

		// 启动 TSO 同步循环
		go s.tso.SyncLoop(leaderCtx)

		// 启动 调度器循环
		go s.coordinator.Run(leaderCtx)

		go s.startGCLoop(leaderCtx)

		// 启动 集群监控 (Week 8 Day 3)
		// 假设你已经在 Cluster 中实现了 StartMonitor
		// go s.cluster.StartMonitor(leaderCtx)

		// 6. 阻塞直到 Session 过期
		select {
		case <-session.Done():
			log.Println("Session expired, stepping down")
		case <-s.ctx.Done():
			log.Println("Server stopping, stepping down")
		}

		// 7. 退位清理
		cancel() // 停止 TSO, Coordinator, Monitor
		atomic.StoreInt64(&s.isLeader, 0)
		session.Close()
	}
}

func (s *Server) startGCLoop(ctx context.Context) {
	interval := s.cfg.GCInterval
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.IsLeader() {
				continue
			}
			safePoint, err := s.calcGCSafePoint()
			if err != nil || safePoint == 0 {
				if err != nil {
					log.Printf("GC safe point unavailable: %v", err)
				}
				continue
			}
			stores := s.cluster.GetStores()
			for _, st := range stores {
				if st.GetStatus() != cluster.StoreStatusUp {
					continue
				}
				s.triggerStoreGC(ctx, st.Meta.Address, safePoint)
			}
		}
	}
}

func (s *Server) calcGCSafePoint() (uint64, error) {
	ts, err := s.tso.Generate(1)
	if err != nil {
		return 0, err
	}
	lagMs := s.cfg.GCSafePointLag.Milliseconds()
	safePhysical := ts.Physical - lagMs
	if safePhysical < 0 {
		safePhysical = 0
	}
	return uint64(safePhysical) << 18, nil
}

func (s *Server) triggerStoreGC(ctx context.Context, addr string, safePoint uint64) {
	if addr == "" {
		return
	}
	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(reqCtx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Printf("GC dial failed (%s): %v", addr, err)
		return
	}
	defer conn.Close()

	client := titankvpb.NewTitanKVClient(conn)
	_, err = client.UpdateConfig(reqCtx, &titankvpb.UpdateConfigRequest{
		GcSafePoint: safePoint,
	})
	if err != nil {
		log.Printf("GC trigger failed (%s): %v", addr, err)
	}
}

// --- 实现 gRPC 接口 ---

func (s *Server) GetTS(ctx context.Context, req *pdpb.GetTSRequest) (*pdpb.GetTSResponse, error) {
	if !s.IsLeader() {
		return nil, status.Error(codes.Unavailable, "not pd leader")
	}

	count := req.Count
	if count == 0 {
		count = 1
	}

	ts, err := s.tso.Generate(count)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pdpb.GetTSResponse{
		Timestamp: &ts,
		Count:     count,
	}, nil
}

func (s *Server) AllocID(ctx context.Context, req *pdpb.AllocIDRequest) (*pdpb.AllocIDResponse, error) {
	if !s.IsLeader() {
		return nil, status.Error(codes.Unavailable, "not pd leader")
	}

	id, err := s.idAllocator.Alloc(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pdpb.AllocIDResponse{Id: id}, nil
}

func (s *Server) GetRegion(ctx context.Context, req *pdpb.GetRegionRequest) (*pdpb.GetRegionResponse, error) {
	// 允许 Follower 提供读服务？为了简单，暂时只允许 Leader
	// 生产环境可以允许 Follower 读，只要元数据是新的
	if !s.IsLeader() {
		return nil, status.Error(codes.Unavailable, "not pd leader")
	}

	region, leader := s.cluster.GetRegion(req.Key)

	return &pdpb.GetRegionResponse{
		Region: region,
		Leader: leader,
	}, nil
}

// 【修复】PutStore 只是单纯的注册 Store，不涉及 Region 和 Operator
func (s *Server) PutStore(ctx context.Context, req *pdpb.PutStoreRequest) (*pdpb.PutStoreResponse, error) {
	if !s.IsLeader() {
		return nil, status.Error(codes.Unavailable, "not pd leader")
	}
	if err := s.cluster.PutStore(ctx, req.Store); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pdpb.PutStoreResponse{}, nil
}

func (s *Server) StoreHeartbeat(ctx context.Context, req *pdpb.StoreHeartbeatRequest) (*pdpb.StoreHeartbeatResponse, error) {
	if !s.IsLeader() {
		return nil, status.Error(codes.Unavailable, "not pd leader")
	}
	if err := s.cluster.HandleStoreHeartbeat(req); err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &pdpb.StoreHeartbeatResponse{}, nil
}

// 【修复】RegionHeartbeat 才是处理调度指令的地方
func (s *Server) RegionHeartbeat(ctx context.Context, req *pdpb.RegionHeartbeatRequest) (*pdpb.RegionHeartbeatResponse, error) {
	if !s.IsLeader() {
		return nil, status.Error(codes.Unavailable, "not pd leader")
	}
	if req.Region == nil {
		return nil, status.Error(codes.InvalidArgument, "missing region")
	}

	if err := s.cluster.HandleRegionHeartbeat(ctx, req); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := &pdpb.RegionHeartbeatResponse{}

	// 检查是否有调度指令 (Operator)
	op := s.coordinator.GetOperator(req.Region.Id)
	if op != nil {
		if op.Current < len(op.Steps) {
			step := op.Steps[op.Current]
			// 检查是否完成
			if step.IsFinish(req.Region, req.Leader) {
				op.Current++
				if op.Current >= len(op.Steps) {
					s.coordinator.RemoveOperator(req.Region.Id)
					log.Printf("[PD] Operator finished: %s", op.String())
				}
			} else {
				// 未完成，下发指令
				switch step := step.(type) {
				case *schedule.TransferLeader:
					for _, p := range req.Region.Peers {
						if p.StoreId == step.ToStore {
							resp.TransferLeader = &pdpb.TransferLeader{
								PeerId:  p.Id,
								StoreId: step.ToStore,
							}
							break
						}
					}
				case *schedule.AddPeer:
					log.Printf("[PD] Operator addpeer: %s", op.String())
					resp.ChangePeer = &pdpb.ChangePeer{
						ChangeType: pdpb.ChangePeer_ADD_NODE,
						Peer:       &pdpb.Peer{Id: step.PeerID, StoreId: step.ToStore},
					}
				case *schedule.RemovePeer:
					for _, p := range req.Region.Peers {
						if p.StoreId == step.FromStore {
							resp.ChangePeer = &pdpb.ChangePeer{
								ChangeType: pdpb.ChangePeer_REMOVE_NODE,
								Peer:       p,
							}
							break
						}
					}
				}
			}
		}
	}

	return resp, nil
}

func (s *Server) GetStore(ctx context.Context, req *pdpb.GetStoreRequest) (*pdpb.GetStoreResponse, error) {
	if !s.IsLeader() {
		return nil, status.Error(codes.Unavailable, "not leader")
	}

	// 【修改】调用 GetStoreInfoByID
	storeInfo := s.cluster.GetStoreInfoByID(req.StoreId)
	if storeInfo == nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("store %d not found", req.StoreId))
	}

	return &pdpb.GetStoreResponse{
		Store: storeInfo.Meta, // storeInfo 已经是 Clone 过的副本，可以直接取 Meta
	}, nil
}
