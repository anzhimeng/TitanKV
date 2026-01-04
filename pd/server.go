package pd

import (
	"context"
	"fmt" // 【新增】修复 undefined: fmt
	"log"
	"sync/atomic"
	"time"

	"titankv/pd/api/pdpb"
     "titankv/pd/tso"
     "titankv/pd/id"
     "titankv/pd/cluster"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency" // 确保包含这个
	"go.etcd.io/etcd/server/v3/embed"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)
const (
	etcdTimeout = 3 * time.Second
)

type Server struct {
     pdpb.UnimplementedPDServer // 实现 gRPC 接口
	cfg      *Config
	etcd     *embed.Etcd
	client   *clientv3.Client
	
	// 原子变量：1 表示我是 Leader，0 表示 Follower
	isLeader int64 
	
	// 用于通知退出的 Context
	ctx      context.Context
	cancel   context.CancelFunc
	tso *tso.Allocator
	cluster *cluster.RaftCluster // 【新增】
     idAllocator *id.Allocator    // 还需要一个 ID 分配器 (见下文)
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


	// 4. 创建连接自己的 Client (用于后续选主和元数据存取)
	// 因为是连接本地，Endpoints 填 ClientUrls 即可
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   s.cfg.ClientUrls,
		DialTimeout: etcdTimeout,
	})
	if err != nil {
		return err
	}
	s.client = client
	// 初始化 TSO
     s.tso = tso.NewAllocator(s.client)
     s.cluster = cluster.NewRaftCluster(s.client)
    	s.idAllocator = id.NewAllocator(s.client)
	// 5. 启动竞选 Loop (异步)
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

		// 1. 创建 Session (带 TTL 的会话)
		// 如果 PD 挂了，TTL 过期，Lease 自动释放，其他节点可以抢
		session, err := concurrency.NewSession(s.client, concurrency.WithTTL(5)) // 5秒 TTL
		if err != nil {
			log.Printf("Failed to create session: %v", err)
			time.Sleep(time.Second)
			continue
		}

		// 2. 创建 Election 对象
		election := concurrency.NewElection(session, "/pd/leader")

		// 3. 开始竞选 (阻塞调用，直到当选)
		log.Println("Campaigning for leader...")
		if err := election.Campaign(s.ctx, s.cfg.Name); err != nil {
			log.Printf("Campaign failed: %v", err)
			session.Close()
			continue
		}

		// 4. 当选成功！
		log.Println("I am the Leader!")
		atomic.StoreInt64(&s.isLeader, 1)

		// 初始化 TSO
		if err := s.tso.Initialize(s.ctx); err != nil {
			log.Printf("Failed to initialize TSO: %v", err)
			atomic.StoreInt64(&s.isLeader, 0)
			session.Close()
			continue
		}

		// 创建一个属于 Leader 任期的 Context
		// 当退位时，调用 cancel()，TSO 和 Monitor 都会自动停止
		leaderCtx, cancel := context.WithCancel(s.ctx)

		// 1. 启动 TSO 同步
		go s.tso.SyncLoop(leaderCtx)

		// 2. 【关键修复】启动集群监控 (检查心跳超时)
		go s.cluster.StartMonitor(leaderCtx)

		// 阻塞直到 Session 过期
		select {
		case <-session.Done():
			log.Println("Session expired, stepping down")
		case <-s.ctx.Done():
			log.Println("Server stopping, stepping down")
		}

		// 退位清理
		cancel() // 通知 TSO 和 Monitor 停止
		atomic.StoreInt64(&s.isLeader, 0)
		session.Close()
	}
}


func (s *Server) GetTS(ctx context.Context, req *pdpb.GetTSRequest) (*pdpb.GetTSResponse, error) {
    // 1. 检查是否是 Leader
    if !s.IsLeader() {
        // 生产环境应该返回 Leader 的地址让 Client 重定向
        return nil, status.Error(codes.Unavailable, "not leader")
    }

    // 2. 分配时间戳
    count := req.Count
    if count == 0 { count = 1 }
    
    ts, err := s.tso.Generate(count)
    if err != nil {
        return nil, status.Error(codes.Internal, err.Error())
    }

    return &pdpb.GetTSResponse{
        Timestamp: &ts,
        Count:     count,
    }, nil
}

func (s *Server) PutStore(ctx context.Context, req *pdpb.PutStoreRequest) (*pdpb.PutStoreResponse, error) {
    if !s.IsLeader() { return nil, status.Error(codes.Unavailable, "not leader") }
    
    if err := s.cluster.PutStore(ctx, req.Store); err != nil {
        return nil, err
    }
    return &pdpb.PutStoreResponse{}, nil
}

func (s *Server) StoreHeartbeat(ctx context.Context, req *pdpb.StoreHeartbeatRequest) (*pdpb.StoreHeartbeatResponse, error) {
    if !s.IsLeader() { return nil, status.Error(codes.Unavailable, "not leader") }

    if err := s.cluster.HandleStoreHeartbeat(req); err != nil {
        // 如果 Store 不存在，可能需要通知 TitanKV 重新注册
        return nil, status.Error(codes.NotFound, err.Error())
    }
    return &pdpb.StoreHeartbeatResponse{}, nil
}

func (s *Server) AllocID(ctx context.Context, req *pdpb.AllocIDRequest) (*pdpb.AllocIDResponse, error) {
    if !s.IsLeader() {
        return nil, status.Error(codes.Unavailable, "not leader")
    }

    id, err := s.idAllocator.Alloc(ctx)
    if err != nil {
        return nil, status.Error(codes.Internal, err.Error())
    }

    return &pdpb.AllocIDResponse{Id: id}, nil
}