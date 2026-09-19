# MiniKV

一个基于 **C++17** 实现的轻量级分布式 Key-Value 存储系统。

MiniKV 底层采用 **LSM-Tree + WAL** 实现本地存储，上层通过 **Raft** 实现多节点日志复制和多数派提交，并使用 **Kubernetes StatefulSet + PVC** 部署 3 节点集群。

## 核心特性

* C++17 / Linux Socket / epoll
* TCP KV Server，支持 PUT / GET / DEL / PING
* TCP 4-byte length-prefix framing，处理粘包/半包
* MemTable + SkipList + Arena
* WAL 崩溃恢复
* SSTable + Bloom Filter + Index Block
* VersionSet / Manifest / CURRENT
* Background Compaction
* Raft Leader Election / AppendEntries / Log Replication
* Majority Commit
* Leader 故障恢复与 Follower 日志追赶
* Docker / Kubernetes 部署
* StatefulSet + Headless Service + PVC

---

## Architecture

```text
                    Client
                      |
                      | TCP
                      v
              +---------------+
              |   KV Server   |
              | epoll + worker|
              +-------+-------+
                      |
              +-------+-------+
              |               |
             GET         PUT / DELETE
              |               |
              v               v
           DBImpl       RaftNode::Propose
              |               |
              |          Raft Log Replication
              |               |
              |          Majority Commit
              |               |
              +-------+-------+
                      |
                      v
                Apply to DB
                      |
                      v
             MemTable + WAL
                      |
                      v
                   SSTable
                      |
                      v
                 Compaction
```

3 节点：

```text
minikv-0 <---- Raft UDP ----> minikv-1
     ^                              |
     |                              |
     +----------- minikv-2 ---------+
```

---

## Write Path

PUT / DELETE：

```text
Client
  ↓
KV Server
  ↓
Leader
  ↓
RaftNode::Propose
  ↓
Append Log
  ↓
Majority Replication
  ↓
commit_index
  ↓
ApplyCommittedEntries
  ↓
DBImpl::Put / Delete
  ↓
WAL + MemTable
  ↓
SSTable / Compaction
```

GET 当前采用 **Leader-only** 策略，直接读取本地 DB，不经过 Raft Propose。

---

## Storage Engine

```text
Write
  ↓
WAL
  ↓
MemTable
  ↓
Immutable MemTable
  ↓
Flush
  ↓
L0 SSTable
  ↓
Compaction
  ↓
Higher Levels
```

主要组件：

```text
MemTable
  └─ SkipList + Arena

SSTable
  ├─ Data Block
  ├─ Index Block
  ├─ Bloom Filter
  └─ Footer

VersionSet
  ├─ Version
  ├─ VersionEdit
  ├─ Manifest
  └─ CURRENT
```

---

## Raft

当前集群：

```text
3 nodes
majority = 2
```

实现了：

* Leader Election
* RequestVote
* AppendEntries
* Log Replication
* Majority Commit
* Leader Failure Recovery
* Follower Catch-up

Raft 控制面当前使用 UDP，客户端数据面使用 TCP。

---

## Kubernetes

使用：

```text
StatefulSet
+
Headless Service
+
PVC
```

节点：

```text
minikv-0
minikv-1
minikv-2
```

Raft Peer 使用稳定 Kubernetes DNS，不依赖固定 Pod IP。

---

## Build

```bash
git clone git@github.com:Albert1207-wyman/MiniKV.git
cd MiniKV

cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

生成：

```text
build/kv_server
build/kv_client
build/kv_bench
```

---

## Test

### Local 3-node

```bash
./minikv_cluster.sh test
```

```text
PASS: 30
FAIL: 0
```

### Docker

```bash
./docker_test.sh
```

```text
PASS: 12
FAIL: 0
```

### Kubernetes

```bash
./k8s/k8s_test.sh
```

```text
PASS: 15
FAIL: 0
```

覆盖：

```text
Leader Election
PUT / GET / DELETE
Replication
Leader Failure
Pod Rebuild
PVC Persistence
Data Recovery
Log Convergence
```

---

## Benchmark

当前开发环境下，本地存储测试结果：

```text
Sequential Write   ≈ 272K ops/s
Random Read        ≈ 380K ops/s
```

Kubernetes GET Benchmark：

```text
4 threads × 10,000 ops
Total: 40,000
QPS:   1635.7
Error: 0%

avg: 2.440 ms
p50: 2.376 ms
p95: 3.565 ms
p99: 4.849 ms
```

> Benchmark 结果与硬件、虚拟机、磁盘、编译参数和测试负载有关，仅用于当前实现的工程测试。

---

## Project Focus

MiniKV 主要实践：

```text
C++17
Linux Network Programming
epoll / TCP / UDP
LSM-Tree
WAL
SSTable
Compaction
Raft
Distributed KV
Kubernetes
Failure Recovery
```

> MiniKV 是一个面向学习和工程实践的轻量级分布式 KV 存储系统，并非生产级数据库。
