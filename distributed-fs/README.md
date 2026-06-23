# Distributed FS

一个基于 Go 实现的分布式文件存储项目。项目从 P2P 文件传输原型演进而来，当前重点是构建一个“元数据准确、数据副本最终一致、多副本容错”的小型分布式文件系统。

## Features

- 中心化 metadata control plane，记录文件版本、chunk manifest、副本状态、checksum、primary replica 和 tombstone。
- Manager 使用 bbolt 持久化 metadata 和 replication task，重启后可以恢复文件版本、副本状态、tombstone 和未完成复制任务。
- 多副本最终一致：写入 primary 成功后发布 metadata，secondary replica 由后台 worker 异步复制。
- 副本状态机：`pending` / `healthy` / `stale` / `missing` / `deleted`。
- 节点状态机：`healthy` / `down`，storage node 通过 heartbeat 刷新状态。
- 读取时优先 primary，primary 不可读时降级读取其他 healthy replica。
- 复制 worker 支持持久化 task、运行租约、失败退避重试、最大重试次数和 dead task，避免任务永久卡在 running 或忙等重试。
- 上传、下载和 object copy 主链路基于 `io.Reader` / `io.Writer` 流式传输；Manager 通过临时文件 staging 计算 size/checksum，避免大文件常驻内存。
- 文件内部按固定大小 chunk 存储，metadata 保存每个 chunk 的 offset、size 和 checksum；读取时按 chunk manifest 重组并校验。
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
- `Put` 先写 primary chunks，primary 成功后再发布 metadata。
- secondary replicas 初始为 `pending`。
- 后台 replication loop 将 pending/missing/stale 副本修复为 `healthy`。
- `Get` 只读取 metadata 中标记为 `healthy` 的副本，并校验 checksum。
- 每个文件版本包含 chunk manifest；复制任务按目标副本复制该版本的 chunks。
- replication task 持久化在 Manager 的 bbolt 数据库中，`pending` 任务重启后继续执行，`running` / `failed` 任务重启后恢复为可重试状态。
- worker 执行任务前会设置 lease，失败后按 attempts 设置 `run_after` 退避重试，超过最大重试次数后进入 `dead`。

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

## Design Notes

- Metadata 是系统的权威控制面，读取、修复、删除都依赖 metadata 中的版本、副本状态和 tombstone。
- `Put` 先写 primary chunks，primary 成功后再发布 metadata，避免 metadata 指向不存在的数据。
- 异步复制流程为：pending task -> worker lease -> remote chunk copy -> replica healthy。
- 当前对外仍是整文件 PUT/GET，内部使用固定大小 chunk；尚未提供 multipart upload 或 range read API。
- 复制失败会记录最近错误并按 backoff 重新调度；running task 的 lease 过期后会被恢复为 pending 或标记为 dead。
- 读失败会将副本标记为 `missing` 或 `stale`，后续 repair loop 会重新规划复制任务。
- manager 和 storage 都提供 `/healthz`；manager 提供 `/metrics`，返回文件数、删除文件数、replication task 状态分布、节点状态和副本状态分布。
- 后台 heartbeat、repair、replication loop 使用 `log.Printf` 输出运行错误。

## Future Work

- 使用 Raft 将 metadata manager 扩展为高可用集群。
- 将 P2P transport 接入当前 HTTP object copy 主链路。
- 增加 multipart upload、range read 和 chunk 级 repair。
- 为 repair worker 增加限速和任务过期清理。
