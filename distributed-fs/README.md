# Distributed FS

一个基于 Go 实现的分布式文件存储项目。项目从 P2P 文件传输原型演进而来，当前重点是构建一个“元数据准确、数据副本最终一致、多副本容错”的小型分布式文件系统。

## Features

- 中心化 metadata control plane，记录文件版本、副本状态、checksum、primary replica 和 tombstone。
- Manager 使用 bbolt 持久化 metadata，重启后可以恢复文件版本、副本状态和 tombstone。
- 多副本最终一致：写入 primary 成功后发布 metadata，secondary replica 由后台 worker 异步复制。
- 副本状态机：`pending` / `healthy` / `stale` / `missing` / `deleted`。
- 节点状态机：`healthy` / `down`，storage node 通过 heartbeat 刷新状态。
- 读取时优先 primary，primary 不可读时降级读取其他 healthy replica。
- 复制 worker 支持 pending task、失败标记 missing、repair task 重新入队。
- HTTP API + CLI + Docker Compose demo。
- 保留原 P2P demo，作为早期传输层原型和后续演进基础。

## Architecture

```text
CLI / HTTP Client
        |
        v
Manager
  |-- MetadataCoordinator
  |-- MetadataStore
  |-- ReplicationPlanner
  |-- ReplicationTaskQueue
  |-- ReplicationWorker
        |
        v
Storage Nodes
  |-- HTTP object API
  |-- LocalObjectStore
  |-- Store (CAS layout)
```

## Consistency Model

本项目不做强一致和 quorum。当前语义是：

- Metadata 是权威控制面。
- 数据副本允许短暂不一致。
- `Put` 先写 primary object，primary 成功后再发布 metadata。
- secondary replicas 初始为 `pending`。
- 后台 replication loop 将 pending/missing/stale 副本修复为 `healthy`。
- `Get` 只读取 metadata 中标记为 `healthy` 的副本，并校验 checksum。

一句话总结：

```text
metadata accurate, replicas eventually consistent
```

## Run With Docker Compose

```bash
make compose-up
```

另开一个终端：

```bash
make build
./bin/fs nodes
printf "hello dfs\n" > input.txt
./bin/fs put demo.txt input.txt
sleep 2
./bin/fs stat demo.txt
./bin/fs get demo.txt output.txt
cat output.txt
./bin/fs delete demo.txt
```

或者直接运行：

```bash
make demo
```

停止并清理：

```bash
make compose-down
```

## CLI

```bash
fs put [-manager http://127.0.0.1:9000] <key> <path>
fs get [-manager http://127.0.0.1:9000] <key> <out>
fs delete [-manager http://127.0.0.1:9000] <key>
fs stat [-manager http://127.0.0.1:9000] <key>
fs nodes [-manager http://127.0.0.1:9000]
```

## HTTP API

Manager:

- `PUT /files/{key}`
- `GET /files/{key}`
- `DELETE /files/{key}`
- `GET /files/{key}/metadata`
- `GET /nodes`
- `GET /metrics`
- `POST /replication/run`

Storage node:

- `PUT /objects/{key}?version={version}`
- `GET /objects/{key}?version={version}`
- `HEAD /objects/{key}?version={version}`
- `DELETE /objects/{key}?version={version}`

## Local Development

```bash
make test
make build
```

如果本机 Go build cache 权限受限：

```bash
GOCACHE=/private/tmp/dfs-go-build go test ./...
GOCACHE=/private/tmp/dfs-go-build go build ./...
```

## Manual Run

启动 storage nodes：

```bash
./bin/fs storage -id node1 -addr :9101 -root data/node1 -manager http://127.0.0.1:9000
./bin/fs storage -id node2 -addr :9102 -root data/node2 -manager http://127.0.0.1:9000
./bin/fs storage -id node3 -addr :9103 -root data/node3 -manager http://127.0.0.1:9000
```

启动 manager：

```bash
./bin/fs manager \
  -metadata-db data/manager/metadata.db \
  -node node1=http://127.0.0.1:9101 \
  -node node2=http://127.0.0.1:9102 \
  -node node3=http://127.0.0.1:9103
```

原始 P2P demo：

```bash
./bin/fs p2p-demo
```

## Interview Notes

可以重点讲：

- 为什么不直接做强一致：项目目标是多副本容错和最终一致，复杂度更适合秋招项目落地。
- 为什么 metadata 要准确：读取、修复、删除都依赖 metadata 作为权威控制面。
- 为什么 `Put` 要先写 primary 再发布 metadata：避免 metadata 指向不存在的数据。
- 异步复制如何工作：pending task -> worker -> remote object copy -> replica healthy。
- 故障时如何处理：读失败标记 missing/stale，repair loop 后续重新复制。
- 监控和报错机制：
  - manager/storage 都提供 `/healthz`。
  - manager 提供 `/metrics`，返回文件数、删除文件数、pending task 数、节点状态和副本状态分布。
  - 后台 heartbeat、repair、replication loop 使用 `log.Printf` 输出错误。
- 后续演进：
  - metadata manager 用 Raft 做高可用。
  - 整文件副本升级为 chunk-based storage。
  - P2P transport 替换当前 HTTP object copy。
  - repair worker 增加限速、重试退避和任务持久化。
