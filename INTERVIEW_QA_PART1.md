# TitanKV 核心面试题库与高分解答 (Interview Q&A)

这份文档针对 TitanKV 项目的底层存储引擎、分布式共识 (Raft) 进行深度剖析，提取了面试官最爱问的高频、高难度问题，并提供了结构化的满分回答。

---

## 🌟 第一部分：底层存储引擎与键值分离 (WiscKey / LSM-Tree)

### Q1: 为什么要在 LSM-Tree 的基础上引入 WiscKey 架构（键值分离）？它解决了什么痛点？
**考察点：** 存储引擎原理、写放大 (Write Amplification) 的理解。

**高分解答：**
- **痛点 (LSM-Tree 的局限)：** 传统的 LSM-Tree (如 LevelDB/RocksDB) 将 Key 和 Value 一起存放在 SSTable 中。在后台进行 Compaction（压实）时，不可避免地要将相同的 Value 数据反复读取和写入磁盘。对于大 Value 场景，这种**写放大**极其严重（通常可达 10 倍以上），不仅消耗了大量的磁盘 I/O 带宽，还大大缩短了 SSD 的寿命。
- **解决方案 (WiscKey / TitanKV)：** 我在项目中引入了键值分离的架构。LSM-Tree 中只存储 Key 和 Value 的位置索引（即 `BlobIndex: <FileID, Offset, Size>`），而真正的 Value 被直接追加写入到单独的日志文件（BlobFile）中。
- **收益：** 由于 Value 不再参与 LSM-Tree 的 Compaction 过程，写放大从原本的 `10x+` 直接断崖式下降到接近 `1.1x`（仅包含一次追加写和极少量的垃圾回收开销），系统整体的写入吞吐量得到了质的飞跃。

### Q2: 键值分离后，BlobFile 会不断膨胀，你是如何做垃圾回收 (GC) 的？会阻塞前台读写吗？
**考察点：** 异步处理、空间放大、MVCC 结合。

**高分解答：**
- **GC 触发时机：** 当系统检测到 BlobFile 中有较多数据被覆盖或删除（通过采样或者统计阈值，比如有效数据比例低于 50%）时，会触发 GC。
- **异步与无锁设计：** 我专门设计了一个后台 `BGGC` (Background GC) 线程。它会在后台异步读取需要 GC 的 BlobFile。由于系统基于 MVCC，GC 线程会去 LSM-Tree 中查询当前的最新版本索引，如果发现 BlobFile 中的数据已经被新版本覆盖或被删除，就直接丢弃；如果是仍然有效的记录，则将其重写到新的 BlobFile 中，并更新 LSM-Tree 中的索引。
- **不阻塞前台：** 这个过程是完全异步的，不会阻塞客户端的前台 Put/Get 请求。同时，为了应对大文件的读写，我还加入了 Direct I/O 支持，绕过了操作系统的 Page Cache，防止后台 GC 污染前台查询的缓存命中率。

---

## 🌟 第二部分：分布式共识与 Raft 优化 (Distributed Consensus)

### Q3: 你的 Raft 实现了 ReadIndex 优化，能详细讲讲它和普通的 Raft Read 有什么区别吗？它是如何保证线性一致性 (Linearizability) 的？
**考察点：** 强一致性读、Raft 协议细节、网络与磁盘 I/O 优化。

**高分解答：**
- **普通的 Raft Read：** 在标准 Raft 中，即使是只读请求，也要作为一个 Log Entry 追加到 Raft Log 中，并通过网络复制给多数派，等 Commit 后才能应用并返回结果。这涉及了磁盘 I/O 和一整个复制流程，延迟极高。
- **ReadIndex 优化：** 
  1. 当 Leader 收到读请求时，它不需要写 Log，而是先记录下当前的 Commit Index，称之为 `ReadIndex`。
  2. 为了防止当前节点实际上已经因为网络隔离变成了“假 Leader”（发生了脑裂），它必须向集群中的多数派节点发送一次 Heartbeat，确认自己仍然是合法的 Leader（Leader Lease 机制）。
  3. 确认身份后，Leader 等待本地的状态机应用进度 (Apply Index) 追上刚才记录的 `ReadIndex`。
  4. 一旦追上，就可以安全地从本地状态机读取数据并返回给客户端。
- **收益：** 这个优化完全去除了只读请求的磁盘 I/O（不写 Log），同时省去了 Log 复制的带宽，让读 TPS 从 **12.5k 暴增到 49.4k**（约 4 倍提升），几乎逼近了单机引擎的读取极限。

### Q4: 你提到了 Raft Pipeline 和 Batching 优化，具体是怎么做的？解决了什么问题？
**考察点：** RPC 优化、吞吐量与延迟的权衡、高并发处理。

**高分解答：**
- **痛点：** 原始的 gRPC RPC 调用通常是 Ping-Pong 模式（发一个请求，等一个响应）。在 Raft 复制中，如果每次 AppendEntries 都要等待对方 Ack 后再发下一批，网络 RTT（往返延迟）将成为极大的瓶颈。
- **Pipeline (流水线)：** 我在底层的网络传输层引入了 Pipeline 机制。Leader 发送 Raft 日志时，不再等待 Follower 的响应，而是像流水线一样源源不断地发送数据。Follower 乱序回复 Ack，Leader 在本地维护一个滑动窗口或队列来推进匹配进度。这彻底隐藏了网络 RTT 的影响。
- **Batching (批处理)：** 在高并发场景下，如果为每一个小的 KV 请求都发起一次 Raft Append，Syscall 和协议头开销会拖垮 CPU。我实现了一个队列，将极短时间窗口内（如 1ms）的多个 Raft Message 打包成一个 Batch 统一发送。
- **收益：** 结合这两项优化，集群在 ACID 事务场景下的吞吐量达到了 **33k+ TPS**，是基准线（6k TPS）的 5.5 倍。

### Q5: 集群在发生网络抖动时，如何防止断网节点恢复后扰乱集群（引发无意义的选举）？
**考察点：** Raft 异常处理、Pre-Vote 机制。

**高分解答：**
- **痛点：** 如果一个 Follower 发生网络分区，它收不到 Heartbeat 就会不断增加自己的 Term（任期）并尝试发起选举。当网络恢复时，它携带着超高的 Term 重新连入集群，会导致当前的稳定 Leader 被迫下台，引发整个集群的重新选举和可用性抖动。
- **Pre-Vote (预投票) 机制：** 为了解决这个问题，我引入了 Pre-Vote 阶段。当节点超时准备发起选举前，它不能增加自己的 Term，而是先发送一轮 `PreVote` 请求给其他节点。只有当它收到了多数派的 `PreVoteResponse`（证明它确实能连通多数派，且它的日志足够新），它才被允许真正地增加 Term 并发起正式的 `RequestVote`。
- **效果：** 断网节点在 Pre-Vote 阶段就会被拒绝（因为它的日志落后或者其他节点觉得 Leader 依然健康），从而被静默拦截，彻底杜绝了网络恢复带来的选举风暴。