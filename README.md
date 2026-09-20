# MiniKV

一个基于 **C++17** 实现的轻量级分布式 KV 存储系统。

核心技术：

```text
C++17 / Linux / epoll
        ↓
     KV Server
        ↓
LSM-Tree + WAL
        ↓
  3-node Raft
        ↓
Docker / Kubernetes
```

## Architecture

```text
                         Client
                           |
                           | TCP
                           v
                  +------------------+
                  |    KV Server     |
                  |  epoll + worker  |
                  +--------+---------+
                           |
                +----------+----------+
                |                     |
               GET                PUT / DELETE
                |                     |
                v                     v
             DBImpl           RaftNode::Propose
                |                     |
                |                Raft Log
                |                     |
                |             Majority Commit
                |                     |
                +----------+----------+
                           |
                           v
                    Apply to DB
                           |
                    +------+------+
                    |             |
                 MemTable        WAL
                    |
                    v
                 SSTable
                    |
                    v
                Compaction
```

---

## Storage

MiniKV 的本地存储采用 LSM-Tree：

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
├── SkipList
└── Arena

SSTable
├── Data Block
├── Index Block
├── Bloom Filter
└── Footer

VersionSet
├── VersionEdit
├── Manifest
└── CURRENT
```

WAL 用于本地崩溃恢复，Compaction 用于合并 SSTable、减少 L0 文件数量和读取压力。

---

## Raft

3 节点集群：

```text
             Raft UDP
       +--------+--------+
       |        |        |
       v        v        v
    node0    node1    node2
       \        |        /
        +--- Majority ---+
                |
              Commit
                |
               Apply
```

实现：

```text
Leader Election
RequestVote
AppendEntries
Log Replication
Majority Commit
Leader Failure Recovery
Follower Catch-up
```

PUT / DELETE 经多数派提交后再应用到本地状态机。

GET 当前采用 **Leader-only** 策略。

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

部署 3 个有状态节点：

```text
Kubernetes
│
├── StatefulSet
│   ├── minikv-0 ─── PVC
│   ├── minikv-1 ─── PVC
│   └── minikv-2 ─── PVC
│
├── Headless Service
│       │
│       └── Stable Pod DNS
│
└── Client Services
        │
        └── NodePort
```

Raft 节点通过稳定 Pod DNS 通信，不依赖固定 Pod IP。

故障恢复流程：

```text
Leader Pod Failure
        ↓
New Leader Election
        ↓
Old Data Recovery
        ↓
StatefulSet Rebuild
        ↓
PVC Remount
        ↓
Follower Log Catch-up
        ↓
Cluster Convergence
```

---

# Performance

所有数据来自当前开发环境和对应测试负载，仅用于当前实现的工程测试。

## Compaction A/B

在 **`sync=1`、相同测试负载**下：

| 配置             | SSTable 数量 |    Random Read |
| -------------- | ---------: | -------------: |
| Compaction OFF |        784 | 50,945.2 ops/s |
| Compaction ON  |          5 |  342,297 ops/s |

```text
SSTable:
784 → 5

Random Read:
50,945.2 → 342,297 ops/s
```

结果表明，Compaction 会增加后台合并开销，但能显著减少 SSTable 数量，并改善后续读取效率。

---

## WAL Sync A/B

对比 WAL 同步持久化开关：

| 配置     |          单线程写入 |
| ------ | -------------: |
| sync=1 | 1,112.88 ops/s |
| sync=0 | 80,089.6 ops/s |

`sync=0` 减少了同步持久化带来的写入开销，该测试主要用于观察持久化可靠性与写入性能之间的权衡。

---

## Basic Storage Benchmark

在当前测试负载下：

| Benchmark        |          Result |
| ---------------- | --------------: |
| Sequential Write | 272,906.8 ops/s |
| Random Read      | 380,002.2 ops/s |
| Random Write     | 230,074.1 ops/s |

Random Read：

```text
found = 100000 / 100000
```

---

## Kubernetes GET Benchmark

通过 Kubernetes **Pod `port-forward`** 访问当前 Leader：

```text
4 threads × 10,000 ops
Total = 40,000
```

结果：

```text
success     = 40000
failed      = 0
error rate  = 0%

QPS         = 1635.7

avg         = 2.440567 ms
p50         = 2.376344 ms
p95         = 3.564650 ms
p99         = 4.849034 ms
```

测试环境：

```text
VMware
  ↓
Ubuntu
  ↓
kind
  ↓
Kubernetes
  ↓
MiniKV 3-node cluster
```

---

# Validation

当前已经完成：

| Environment            |      Result |
| ---------------------- | ----------: |
| Local 3-node Raft      | **30 / 30** |
| Docker Integration     | **12 / 12** |
| Kubernetes Integration | **15 / 15** |
| **Total**              | **57 / 57** |

关键验证场景：

```text
✓ Leader Election
✓ PUT / GET / DELETE
✓ Raft Log Replication
✓ Majority Commit
✓ Leader Failure
✓ New Leader Election
✓ Old Data Recovery
✓ Follower Log Catch-up
✓ Pod Rebuild
✓ PVC Persistence
✓ WAL Recovery
✓ Cluster Convergence
```

测试入口：

```bash
./minikv_cluster.sh test
./docker_test.sh
./k8s/k8s_test.sh
```

---

# Build

```bash
git clone git@github.com:Albert1207-wyman/MiniKV.git
cd MiniKV

cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

主要程序：

```text
build/kv_server
build/kv_client
build/kv_bench
```

---

# Project Structure

```text
MiniKV/
├── CMakeLists.txt
├── Dockerfile
├── docker-compose.yml
├── docker_test.sh
├── minikv_cluster.sh
│
├── include/
│   ├── db.h
│   ├── db_impl.h
│   ├── memtable.h
│   ├── skiplist.h
│   ├── table.h
│   ├── version_set.h
│   ├── raft.h
│   └── ...
│
├── src/
│   ├── db_impl.cpp
│   ├── memtable.cpp
│   ├── table.cpp
│   ├── version_set.cpp
│   ├── raft.cpp
│   ├── kv_server.cpp
│   └── ...
│
├── tools/
│   ├── kv_server_main.cpp
│   ├── kv_client.cpp
│   └── kv_bench.cpp
│
└── k8s/
    ├── client-services.yaml
    ├── headless-service.yaml
    ├── statefulset.yaml
    ├── start-minikv.sh
    └── k8s_test.sh
```

---

# Current Limitations

* GET 当前采用 Leader-only
* 未实现 Raft ReadIndex
* 未实现 Leader Lease Read
* 当前 Raft 控制面使用 UDP
* 未完整实现 bottom-level tombstone elimination
* 项目定位为学习与工程实践系统，不是生产级数据库

---

# Project Focus

```text
C++17
Linux / epoll
TCP / UDP
LSM-Tree
WAL
SSTable
Compaction
Raft
Distributed KV
Docker
Kubernetes
Failure Recovery
```

> MiniKV：使用 C++17 构建的轻量级分布式 KV 存储系统，采用 LSM-Tree + WAL 完成本地存储与恢复，通过 3 节点 Raft 实现日志复制与多数派提交，并使用 Kubernetes 验证有状态部署与故障恢复。
