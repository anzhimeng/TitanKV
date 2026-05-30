# TitanKV 技术亮点详解 (Technical Highlights)

TitanKV 是一个受 TiKV 和 Badger 启发的高性能分布式 Key-Value 存储系统。该项目从底层的 LSM-Tree 存储引擎到上层的分布式事务协议，实现了完整的存储栈。

**核心价值与含金量 (Core Value):**
- **混合存储架构:** 实现了基于 WiscKey 思想的键值分离 (Key-Value Separation)，大幅降低写放大。
- **分布式共识:** 定制化 Multi-Raft 实现，包含流水线 (Pipeline) 优化、批处理 (Batching) 和线性一致性读 (ReadIndex)。
- **ACID 事务:** 完整实现基于 Percolator 模型的分布式事务 (2PC)，支持快照隔离 (SI)、无锁读 (Lock-Free Read) 和崩溃恢复。
- **存算分离:** Coprocessor 框架将计算 (Filter/Aggregation) 下推至存储节点，显著减少网络开销。
- **生产级并发控制:** 解决了快照隔离异常 (Write Skew)、写冲突 (Write Conflict) 和线性一致性读等复杂并发问题。

---

## 1. 存储引擎优化 (Storage Engine Optimization)

### A. 键值分离 (Key-Value Separation)
传统 LSM-Tree (如 RocksDB) 在 Compaction 过程中会反复读写 Value，导致严重的写放大。TitanKV 采用 **WiscKey** 架构：
- **LSM-Tree (vLog):** 只存储 Key 和 Value 的索引 (BlobIndex)，大幅减少 LSM-Tree 的体积和 Compaction 开销。
- **BlobStore:** 大 Value 直接追加写入 BlobFile，只在 GC 时进行重写。
- **收益:** 写放大从 10x+ 降低至接近 1.1x (仅 Log 追加)，大幅提升写入吞吐。

### B. 异步 Blob GC (Asynchronous Garbage Collection)
为了解决 BlobFile 的空间回收问题，实现了异步 GC 机制：
- **后台线程:** 独立的 `BGGC` 线程定期扫描 BlobFile。
- **无锁设计:** 通过 MVCC 版本检查，GC 线程只清理不再被引用的 Blob 数据，不阻塞前台读写操作。
- **Direct I/O:** 支持 Direct I/O 绕过系统页缓存，进一步提升大文件读写性能。

---

## 2. 分布式共识与 Raft 优化 (Distributed Consensus)

### A. Multi-Raft 架构
- **Region 分片:** 将数据水平切分为多个 Region，每个 Region 由一个独立的 Raft Group 管理，实现无限水平扩展。
- **Leader 均衡:** PD (Placement Driver) 动态调度 Region Leader，确保存储和计算负载均衡。

### B. 高性能 Raft 实现
- **流水线 (Pipeline):** 实现了 gRPC 流水线传输，Raft 消息发送无需等待前一次 RPC 返回，显著降低网络延迟。
- **批处理 (Batching):** 将多个 Raft Log Entry 打包成一个 Batch 发送，减少 syscall 和网络包开销。
- **预投票 (Pre-Vote):** 引入 Pre-Vote 阶段，防止网络分区的节点在恢复后通过高 Term 扰乱集群 Leader。

### C. 线性一致性读 (Linearizable Read / ReadIndex)
- **ReadIndex:** 实现了 Raft ReadIndex 协议。Leader 在处理读请求时，只需确认自己拥有最新的 Commit Index 且仍是 Leader (通过 Heartbeat)，无需将读请求写入 Raft Log 复制。
- **性能数据:** 读性能从 12.5k TPS (Log Read) 提升至 **49.4k TPS** (ReadIndex)，提升约 **4倍**。

---

## 3. 分布式事务 (Distributed Transactions)

### A. Percolator 模型 (2PC)
实现了去中心化的两阶段提交 (2PC) 协议：
- **Prewrite:** 写入数据并加锁 (Lock CF)，记录 Primary Key。
- **Commit:** 清理锁，写入提交版本 (Write CF)，异步清理 Secondary Keys。
- **优势:** 无单点事务管理器，支持跨行、跨表、跨 Region 的 ACID 事务。

### B. 快照隔离 (Snapshot Isolation)
- **MVCC:** 基于多版本并发控制 (MVCC) 实现 SI 隔离级别。
- **TSO:** 使用 PD 分配的全局单调递增时间戳 (Timestamp Oracle) 作为版本号，确保事务的全序关系。
- **并发控制:** 实现了 `CheckTxnStatus` 和 `ResolveLock` 机制，解决读写冲突。

### C. 崩溃恢复 (Crash Recovery)
- **TTL 机制:** 锁 (Lock) 带有 TTL (Time-To-Live)。
- **Lazy Cleanup:** 读操作遇到过期锁时，主动查询 Primary Key 状态，若 Primary 已提交则推进 Commit，若已回滚则清理锁，确保系统活性。

---

## 4. 计算下推 (Coprocessor)

### A. 算子下推 (Operator Pushdown)
将计算逻辑下推到存储节点执行，避免将大量原始数据拉取到计算层：
- **支持算子:** Sum, Count, Filter (Equal, Greater, Less 等)。
- **收益:** 在聚合查询场景下，网络传输量减少 99% 以上。

### B. 向量化与迭代器优化
- **Iterator Scan:** 摒弃了基于 `GetCF` 的随机读，改用 RocksDB Iterator 进行顺序扫描。
- **性能对比:** 10k Keys 的 Count 操作耗时从 76ms 降低至 **8.4ms**，性能提升 **9倍**。

### C. 强一致性保证
- **Snapshot Isolation:** Coprocessor 扫描时严格遵循 MVCC 可见性规则，只处理 `CommitTS <= StartTS` 的数据。
- **Bug 修复:** 修复了 `Seek` 操作在 `Max-TS` 编码下可能定位到新版本数据的 Bug，通过向后扫描确保快照一致性。

---

## 5. 性能基准测试 (Benchmarks)

| 测试场景 | 基准 (Baseline) | 优化后 (Optimized) | 提升幅度 |
| :--- | :--- | :--- | :--- |
| **只读吞吐 (Read TPS)** | 12.5k (Log Read) | **49.4k** (ReadIndex) | **~395%** |
| **事务写入 (Txn TPS)** | 6k (ACID) | 33k (Pipeline/Batch) | **~550%** |
| **聚合查询 (Coprocessor)** | 76ms (GetCF) | **8.4ms** (Iterator) | **~900%** |
| **写放大 (Write Amp)** | ~10x (LSM) | **~1.1x** (BlobStore) | **降低 90%** |

---

## 6. 代码规模与工程质量
- **代码量:** 核心代码约 21,000 行 (Go + C++)。
- **语言:** 
  - Control Plane (Raft, Network, TSO): **Go**
  - Data Plane (Storage Engine): **C++** (通过 CGO 调用)
- **测试覆盖:** 包含单元测试、集成测试 (Bank Transfer)、混沌测试 (Chaos) 和并发测试 (Race Detector)。
