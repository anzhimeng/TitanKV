# TitanKV 系统架构详解 (System Architecture)

TitanKV 采用典型的存储计算分离架构，整体分为三层：PD (Placement Driver) 调度层、TiKV (Storage) 存储层和 Coprocessor (Compute) 计算层。

## 1. 整体架构图 (High-Level Architecture)

```mermaid
graph TD
    Client[Client (Go SDK)] -->|gRPC| PD[PD Cluster (TSO & Schedule)]
    Client -->|gRPC (Read/Write)| Node1[TitanKV Node 1]
    Client -->|gRPC (Read/Write)| Node2[TitanKV Node 2]
    Client -->|gRPC (Read/Write)| Node3[TitanKV Node 3]

    subgraph "TitanKV Cluster (Storage Layer)"
        Node1 <-->|Raft Log Replication| Node2
        Node2 <-->|Raft Log Replication| Node3
        Node1 <-->|Raft Log Replication| Node3
        
        subgraph "Node Internal Structure"
            API[gRPC API Server]
            RaftStore[Raft Consensus Module]
            Txn[Transaction Coordinator]
            Engine[Storage Engine (C++)]
            
            API --> Txn
            Txn --> RaftStore
            RaftStore --> Engine
            
            subgraph "Storage Engine (C++)"
                LSM[LSM-Tree (Keys)]
                Blob[BlobStore (Values)]
                GC[Blob GC Worker]
                Coprocessor[Coprocessor Executor]
            end
            
            Engine --> LSM
            Engine --> Blob
        end
    end
```

---

## 2. 核心模块详解 (Core Modules)

### A. Client SDK (Go)
- **职责:** 提供 KV 读写接口 (Get, Put, Delete, Scan)。
- **路由 (Routing):** 缓存 Region 元数据，根据 Key Range 自动定位目标 Region Leader。
- **重试机制:** 处理网络抖动、Region Split、Leader Transfer 等异常。
- **Backoff:** 指数退避策略，避免雪崩效应。

### B. Placement Driver (PD)
- **TSO (Timestamp Oracle):** 
    - 提供全局唯一且单调递增的时间戳 (uint64)，作为事务的 StartTS 和 CommitTS。
    - **逻辑时钟 + 物理时钟:** 保证分布式一致性。
- **调度 (Scheduling):** 
    - **Region Balance:** 根据各节点的存储容量和负载，自动迁移 Region 副本。
    - **Leader Balance:** 均衡各节点的 Leader 数量，避免热点。
- **元数据管理:** 存储集群拓扑信息 (Store, Region, Peer)。

### C. RaftStore (Consensus Layer)
- **Multi-Raft:** 
    - 每个 Region 对应一个 Raft Group。
    - **Region Split/Merge:** 数据量超过阈值 (如 96MB) 时自动分裂，过小时自动合并。
- **Raft Log:** 
    - 使用 RocksDB 存储 Raft Log，保证 crash-safe。
    - **Pipeline:** 实现了并行的 Log Append 和 Network Send。
- **ReadIndex:** 
    - 处理读请求时，Leader 仅需确认自身状态，无需写入 Log，大幅提升读性能。

### D. Transaction Coordinator (Percolator)
- **2PC (Two-Phase Commit):** 
    - **Prewrite:** 写入 Lock CF，包含 Primary Key 信息。
    - **Commit:** 检查 Lock，写入 Write CF，提交事务。
- **Concurrency Control:** 
    - **Optimistic Concurrency Control (OCC):** 乐观锁，冲突时重试。
    - **Pessimistic Locking (可选):** 支持悲观锁 `AcquirePessimisticLock`。
- **Resolve Lock:** 
    - 遇到故障节点的锁时，根据 Primary Key 状态决定 Rollback 或 Commit。

### E. Storage Engine (C++ Core)
- **LSM-Tree (Keys):** 
    - 使用 RocksDB/LevelDB 存储 Key 和 BlobIndex。
    - **Compaction:** 后台合并 SSTable，回收旧版本 Key。
- **BlobStore (Values):** 
    - **WiscKey 实现:** Value 写入追加日志文件 (BlobFile)。
    - **BlobIndex:** `<BlobFileID, Offset, Size>` 存储在 LSM-Tree 中。
- **Garbage Collection (GC):** 
    - **Mark & Sweep:** 扫描 LSM-Tree 获取有效 BlobIndex，重写旧 BlobFile，回收无效空间。

### F. Coprocessor (Compute Layer)
- **算子下推:** 
    - 支持 `Sum`, `Count`, `Avg`, `Min`, `Max` 等聚合函数。
    - 支持 `Equal`, `Greater`, `Less`, `In` 等过滤条件。
- **执行模型:** 
    - **Vectorized:** 批量处理 Key-Value，利用 CPU 缓存。
    - **MVCC Scan:** 在存储层直接扫描 Write CF，过滤不可见版本。

---

## 3. 关键数据流程 (Key Data Flows)

### A. 写流程 (Write Path)
1.  **Client** 向 PD 获取 TSO (StartTS)。
2.  **Client** 本地缓存 Region 路由，向对应 Leader 发送 `Prewrite` 请求。
3.  **Leader (RaftStore)** 接收请求，将其封装为 Raft Log Entry，广播给 Followers。
4.  **Followers** 收到 Log，写入本地 RocksDB (Raft Log CF)，回复 Leader。
5.  **Leader** 收到多数派响应，Apply Log 到状态机 (Storage Engine)。
6.  **Storage Engine** 写入 Lock CF (Prewrite 成功)。
7.  **Client** 收到 Prewrite 成功响应，向 PD 获取 TSO (CommitTS)。
8.  **Client** 发送 `Commit` 请求。
9.  **Leader** 再次走 Raft 流程，Apply Commit Log。
10. **Storage Engine** 写入 Write CF，清理 Lock CF。
11. **Client** 收到 Commit 成功响应。

### B. 读流程 (Read Path - ReadIndex)
1.  **Client** 向 PD 获取 TSO (StartTS)。
2.  **Client** 发送 `Get` 请求给 Leader。
3.  **Leader** 记录当前 Commit Index (ReadIndex)。
4.  **Leader** 向 Followers 发送 Heartbeat，确认自己仍是 Leader (Lease 机制)。
5.  **Leader** 等待 Apply Index >= ReadIndex (确保线性一致性)。
6.  **Storage Engine** 根据 StartTS 在 Write CF 查找可见版本 (Snapshot Read)。
7.  **Leader** 返回 Value 给 Client。

### C. Coprocessor 执行流程
1.  **Client** 构造 `CoprocessorRequest` (包含 Range, StartTS, Aggregation Type, Filter)。
2.  **Leader** 接收请求，通过 ReadIndex 确认一致性。
3.  **Storage Engine** 创建 Iterator，定位到 Range 起始位置。
4.  **Iterator** 扫描 Write CF，跳过 `CommitTS > StartTS` 的版本 (Snapshot Isolation)。
5.  **Iterator** 对可见数据执行 Filter 和 Aggregation。
6.  **Storage Engine** 返回 `CoprocessorResponse` (聚合结果) 给 Client。
