# TitanKV Technical Highlights

## 1. Project Overview & "Gold Content" (项目含金量)

TitanKV is a high-performance distributed Key-Value storage system inspired by TiKV and Badger. It implements a complete storage stack from the underlying LSM-Tree engine to the upper-layer distributed transaction protocol.

**Core Technical Value ("Gold Content"):**
- **Hybrid Storage Architecture:** Implements WiscKey-based Key-Value separation (LSM-Tree for keys, BlobFile for values) to reduce Write Amplification.
- **Distributed Consensus:** Custom Multi-Raft implementation with pipeline optimization, batching, and leadership transfer.
- **ACID Transactions:** Full implementation of Percolator-based distributed transactions (2PC), supporting Snapshot Isolation (SI), Lock-Free Read, and Crash Recovery.
- **Compute-Storage Separation:** Coprocessor framework pushes down computation (Filter/Aggregation) to the storage nodes, significantly reducing network overhead.
- **Production-Grade Concurrency:** Solved complex concurrency issues like Snapshot Isolation anomalies (Write Skew), Write Conflicts, and linearizable reads (ReadIndex).

**Job Suitability:**
This project demonstrates deep understanding of distributed systems, storage engines, and concurrency control. It is suitable for interviewing for:
- **Distributed Systems Engineer** (P6/P7 equivalent)
- **Storage Infrastructure Engineer** (Database Kernel, Cloud Storage)
- **Senior Backend Engineer** (High concurrency, Strong consistency requirements)

---

## 2. System Architecture

```mermaid
graph TD
    Client[Client (Go SDK)] -->|gRPC| Gateway[Gateway / Load Balancer]
    Gateway -->|Route| Node1[TitanKV Node 1]
    Gateway -->|Route| Node2[TitanKV Node 2]
    Gateway -->|Route| Node3[TitanKV Node 3]

    subgraph "TitanKV Node"
        API[gRPC API Layer]
        Txn[Transaction Manager (Percolator)]
        Raft[Raft Consensus Module]
        Store[Titan Storage Engine (C++)]
        
        API --> Txn
        Txn --> Raft
        Raft --> Store
        
        subgraph "Storage Engine (C++)"
            LSM[LSM Tree (Keys)]
            Blob[Blob Store (Values)]
            GC[Blob GC (Background)]
            Coprocessor[Coprocessor Executor]
        end
        
        Store --> LSM
        Store --> Blob
    end

    PD[Placement Driver (TSO & Scheduling)] -->|Timestamp| Client
    Node1 <-->|Raft Msg| Node2
    Node1 <-->|Raft Msg| Node3
```

**Key Components:**
1.  **Placement Driver (PD):** Provides strictly monotonic timestamps (TSO) for transactions and handles region scheduling.
2.  **Raft Consensus:** Ensures data consistency across replicas. Optimizations include ReadIndex (Linearizable Read) and Pre-Vote.
3.  **Storage Engine:** C++ based engine handling LSM-Tree compaction, BlobFile management, and MVCC version control.
4.  **Transaction Layer:** Handles 2PC (Prewrite/Commit), conflict detection, and lock cleanup.

---

## 3. Detailed Modules & Features

### A. Storage Engine (C++ Core)
- **LSM-Tree:** LevelDB/RocksDB-style tiered storage for keys.
- **BlobStore:** WiscKey implementation. Large values are stored in BlobFiles, LSM tree only stores references (BlobIndex).
- **Garbage Collection (GC):** Asynchronous BlobGC running in background threads to reclaim space from deleted/overwritten blobs without blocking reads.
- **Direct I/O:** Support for Direct I/O to bypass OS page cache for large blob writes.

### B. Distributed Consensus (Raft)
- **Multi-Raft:** Sharding data into Regions, each maintained by a Raft group.
- **Pipeline & Batching:** Optimized Raft transport layer (gRPC pipeline) and message batching to improve throughput (33k+ TPS).
- **ReadIndex:** Linearizable read support (ReadIndex) bypassing Log replication for read-only requests (~49.4k TPS).
- **Pre-Vote:** Prevents disruptive nodes from triggering unnecessary elections.

### C. Distributed Transactions
- **Percolator Protocol:** Decentralized 2-Phase Commit (2PC).
- **Snapshot Isolation (SI):** MVCC-based isolation. Readers see a consistent snapshot based on StartTS.
- **Crash Recovery:** Lazy cleanup of locks left by crashed clients (TTL-based CheckTxnStatus).
- **Parallel Commit:** Optimization to reduce commit latency by pipelining Prewrite and Commit phases.

### D. Coprocessor (Compute Pushdown)
- **Framework:** Allows executing logic (Sum, Count) directly on storage nodes.
- **Filter Pushdown:** Supports pushing down predicates (Equal, Greater, Less, etc.) to filter data close to source.
- **Vectorized Execution:** (Partial) Batch processing of keys.
- **MVCC Awareness:** Scans Write CF to ensure visibility correctness (Snapshot Isolation).

---

## 4. Performance Benchmarks

### Environment
- **CPU:** 8 vCPU
- **Memory:** 16 GB
- **Disk:** NVMe SSD
- **Network:** Local Loopback (Simulated)

### Results

| Metric | Baseline (Raw) | Optimized | Improvement |
| :--- | :--- | :--- | :--- |
| **Read Throughput** | ~12.5k TPS (Log Read) | **~49.4k TPS** (ReadIndex) | **~4x** |
| **Transaction TPS** | ~45k (Raw Put) | **~6k** (ACID 2PC) | Protocol Overhead (Expected) |
| **Coprocessor (Count)** | ~76ms (GetCF loop) | **~8.4ms** (Iterator) | **~9x** |

**Analysis:**
- **ReadIndex** creates a massive performance jump by avoiding Raft Log disk I/O for read operations.
- **Coprocessor** optimization (switching from random `GetCF` to sequential `Iterator` scan) reduced latency by 90%, proving the value of compute pushdown.

---

## 5. Code Scale
- **Total Lines of Code:** ~21,000 lines (Core Logic).
- **Languages:** Go (Control Plane, Raft, Networking) + C++ (Data Plane, Storage Engine).
- **Complexity:** High. Involves CGO memory management, lock-free programming, and distributed state machine implementation.

---

## 6. Recent Critical Fixes
- **Coprocessor Snapshot Isolation Bug:** Fixed a critical concurrency bug where the Coprocessor `Seek` operation could incorrectly land on a newer MVCC version (due to `Max-TS` encoding nuances), causing phantom reads. Implemented a robust forward-scan mechanism to ensure strictly consistent snapshots.
