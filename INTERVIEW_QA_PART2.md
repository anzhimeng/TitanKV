# TitanKV 核心面试题库与高分解答 (Interview Q&A) - 第二部分

这份文档聚焦于 TitanKV 项目中最具挑战性的分布式事务 (Percolator) 和计算下推 (Coprocessor) 模块，解析了快照隔离、崩溃恢复、读写冲突以及棘手的并发 Bug 修复。

---

## 🌟 第三部分：分布式事务与 MVCC (Distributed Transactions)

### Q1: 讲讲 Percolator 事务模型在你的项目中是怎么实现的？为什么要选它而不是传统的 2PC？
**考察点：** 2PC (Two-Phase Commit)、分布式事务模型、去中心化架构。

**高分解答：**
- **痛点 (传统 2PC)：** 传统的两阶段提交需要一个中心化的 Transaction Coordinator (TM)。这会导致单点故障，一旦 TM 挂掉，参与者持有的锁将陷入死锁状态。同时，跨节点的网络交互非常复杂。
- **解决方案 (Percolator)：** 我在 TitanKV 中完整实现了基于 Google Percolator 模型的去中心化 2PC：
  1. **Prewrite 阶段：** 客户端从 PD 获取一个全局唯一时间戳 `StartTS`。客户端在所有要写入的 Key 中选出一个作为 `Primary Key`，其余作为 `Secondary Keys`。所有的写操作都带上指向 Primary 的指针，并写入到 `Lock CF` 中。
  2. **Commit 阶段：** 客户端再次从 PD 获取 `CommitTS`。先向 Primary Key 发送 Commit 请求，如果在 `Lock CF` 检查发现锁还在（没被别人抢占或清理），就将数据真正写入 `Write CF`，并删除锁。
  3. **异步 Secondary 提交：** Primary 成功后，事务即告成功。客户端可以异步去 Commit 剩下的 Secondary Keys。
- **优势：** 将事务的状态与数据本身绑定（锁信息存在底层 KV 中），彻底去除了单点 TM，极大地提升了系统的横向扩展能力和高可用性。

### Q2: 你的事务如何实现快照隔离 (Snapshot Isolation, SI)？如果在读取时遇到了正在执行的事务（Pending Lock），你会怎么处理？
**考察点：** 隔离级别、MVCC 多版本并发控制、读写冲突。

**高分解答：**
- **快照隔离 (SI)：** 每次事务开始时，获取一个 `StartTS`。读操作时，只会在 `Write CF` 中扫描那些 `CommitTS <= StartTS` 的数据版本，这保证了事务只能看到它开始前已经提交的数据，实现了 SI 级别，杜绝了脏读和不可重复读。
- **处理 Pending Lock (读写冲突)：**
  - 如果读取时在 `Lock CF` 中发现了一个锁，且它的 `LockTS <= 当前事务的 StartTS`，说明有另外一个事务正在修改这条数据，而且它可能在这个快照之前就已经开始了。
  - 这时，读取操作不能简单地忽略这个锁去读老版本（因为那个事务可能已经 Commit 了，只是还没来得及清理 Secondary 的锁），也不能直接读（因为它还没 Commit）。
  - **策略 (Backoff 与 ResolveLock)：** 客户端会先进行指数退避（Backoff）重试，等待锁被释放。如果超时，客户端会主动根据锁里的指针去检查 `Primary Key` 的状态。如果 Primary 已经 Commit，客户端就帮忙提交这个 Secondary 锁；如果 Primary 被 Rollback，客户端就帮忙清理这个锁，然后再进行读取。这种设计被称为 **Lazy Cleanup (懒清理)**。

### Q3: 如果在 2PC 过程中 Coordinator (即发起事务的客户端) 宕机了，系统会发生什么？怎么做 Crash Recovery？
**考察点：** 异常处理、事务活性 (Liveness)、TTL 机制。

**高分解答：**
- **痛点：** 客户端在 `Prewrite` 成功后、还没发 `Commit` 就宕机了，底层的 KV 节点会一直留着这些锁，导致后续所有想读写这些 Key 的事务被无限期阻塞。
- **解决方案 (TTL 与 CheckTxnStatus)：**
  - 我在 `Lock` 结构中加入了一个 `TTL` (Time-To-Live) 字段。
  - 当其他事务被这些“僵尸锁”阻塞时，它们会去调用 `CheckTxnStatus` 检查这些锁的 Primary Key。
  - 如果 Primary Key 的锁的 `当前物理时间 > 锁的物理时间 + TTL`，说明发起事务的客户端大概率已经挂了（锁过期）。
  - 这时，系统会主动触发 Rollback，在 `Write CF` 中写入一条回滚记录（标明该 `StartTS` 已作废），并清理掉 `Lock CF` 中的锁。
- **容错保证：** 这样一来，即使发生大规模的客户端宕机，底层存储节点依然能通过这种分布式的、基于 TTL 的 Lazy Cleanup 机制实现自愈，保证了数据一致性和系统可用性。

---

## 🌟 第四部分：计算下推与并发 Bug (Coprocessor)

### Q4: 什么是 Coprocessor？它如何提升系统性能？
**考察点：** 存算分离、网络开销优化、迭代器扫描。

**高分解答：**
- **痛点：** 对于 `SELECT SUM(salary) FROM users WHERE age > 30` 这种聚合和过滤查询，如果按传统做法把所有数据拉到客户端或计算节点，网络 I/O 和序列化开销将是灾难性的。
- **解决方案：** 我在底层的 C++ 存储引擎中实现了 Coprocessor 框架。客户端将聚合算子 (`Sum`, `Count`) 和过滤条件 (`age > 30`) 下推给存储节点。
- **优化迭代：** 
  - 第一版：在存储节点扫描时，对每个 Key 调用一次 `GetCF` 去拉取 Value。性能较差。
  - 第二版：我将其优化为使用 RocksDB 的 `Iterator` 进行顺序扫描 (`Seek` 然后不断 `Next`)，并结合 MVCC 版本过滤。
- **收益：** 这一优化使得扫描 10k 个 Key 的耗时从 76ms 断崖式下降到了 **8.4ms**，实现了 **9 倍** 的性能提升，同时为网络层节省了 99% 的带宽。

### Q5: 你在简历里提到修复了一个 Coprocessor 快照隔离的棘手 Bug，能详细讲讲吗？
**考察点：** 深度调试能力、MVCC 编码细节、并发与数据结构结合。

**高分解答：**
- **Bug 现象：** 在执行 `TestCoprocessorConcurrency` (并发测试) 时，我启动了一个写线程疯狂更新数据，同时启动一个读线程不断执行 Coprocessor 的 Sum 聚合请求。按理说，读请求指定了 `StartTS=150`，它应该只能看到老数据，但测试却报错说读到了最新的数据，破坏了快照隔离 (SI)。
- **根因分析 (Max-TS 编码的陷阱)：** 
  - 在 MVCC 引擎中，为了让同一个 Key 的最新版本排在前面（便于快速 `Seek` 到最新值），我使用了 `Big Endian` 对 `MaxUint64 - CommitTS` 进行编码作为 Key 的后缀。
  - 当 Coprocessor 尝试查找 `TargetTS=150` 的版本时，它会构造一个 `SeekKey = UserKey + (Max - 150)` 并调用 `Seek`。
  - 然而，如果后台刚刚提交了一个新版本（比如 `CommitTS=210`），那么它在底层的实际字节序是 `UserKey + (Max - 210)`。
  - 因为 `Max - 210` 在数值（和字典序）上 **小于** `Max - 150`，所以 `Seek(TargetTS)` 实际上会首先碰上 `CommitTS=210` 这个比快照更新的版本！
  - 原来的代码认为 `Seek` 会直接停在第一个可见的旧版本上，这是完全错误的假设。
- **解决方案 (向前扫描算法)：**
  - 我重构了扫描逻辑：在 `Seek` 之后，提取出当前迭代器指向的 `CommitTS`。
  - 如果发现 `CommitTS > TargetTS`（即碰到了新版本），程序进入一个 `while` 循环，不断调用 `Next()` 向前扫描，直到越过所有新版本，精确停在第一个 `CommitTS <= TargetTS` 的合法版本上。
  - **结果：** 这一改动一举消灭了高并发下的 Phantom Read (幻读) 问题，并发测试稳定通过。这让我深刻体会到了底层字节序编码与上层隔离级别语义之间的微妙关联。
