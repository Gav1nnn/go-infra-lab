# Distributed FS

一个基于 Go 实现的 P2P 分布式文件存储系统原型。项目重点在于实现分布式存储的核心链路：节点连接、消息分发、文件流传输、本地 CAS 存储和加密传输。

## Features

- 基于 TCP 的 P2P 网络层，使用 `Transport` / `Peer` 接口隔离上层逻辑和底层传输实现。
- 自定义消息协议，通过首字节区分控制消息和文件流。
- 使用 gob 编码控制消息，文件内容直接通过 TCP stream 传输。
- 基于 `io.Reader` / `io.Writer` 实现流式读写，避免一次性加载完整文件。
- 使用 `io.TeeReader` 同时写入本地磁盘和缓存网络发送数据。
- 使用 `io.MultiWriter` 向多个 peer 广播文件流。
- 使用 `io.LimitReader` 限制流读取长度，避免 TCP 连接阻塞。
- 基于 SHA-1 的 CAS 风格目录结构，降低单目录文件数量。
- 使用 AES-CTR 对文件流进行加密传输。

## Architecture

```text
main.go
  |
  v
FileServer
  |-- Store: local CAS storage
  |-- Transport: P2P network abstraction
  |-- Crypto: stream encryption/decryption
  |
  v
p2p.TCPTransport
  |-- listen / dial
  |-- decode control messages
  |-- coordinate file stream reads
```

## Data Flow

### Store

```text
client reader
  |
  v
io.TeeReader
  |-----------------> local Store.Write()
  |
  v
file buffer -> AES-CTR encrypt -> peer stream
```

### Get

```text
check local store
  |
  |-- hit  -> return local file reader
  |
  |-- miss -> broadcast MessageGetFile
              receive encrypted stream
              decrypt and cache locally
              return local file reader
```

## Run

```bash
make run
```

The demo starts three nodes:

```text
:3000
:7000
:5000
```

Node `:5000` connects to `:3000` and `:7000`, stores files, removes its local copy, and fetches the files back from the network.

## Test

```bash
make test
```

or:

```bash
go test ./...
```

## Project Notes

This project is intended as a learning-oriented distributed storage prototype rather than a production-ready storage system. The current implementation is useful for demonstrating:

- Go interface-oriented design.
- TCP network programming.
- Stream-oriented file processing.
- Basic distributed node communication.
- Local content-addressed storage layout.
- Encrypted file transfer.

Potential improvements:

- Add length-prefixed messages to make the TCP protocol more robust.
- Add request timeout and error response messages.
- Add a CLI or HTTP API for `put` / `get` / `delete`.
- Add replication metadata and multi-node integration tests.
- Add Docker Compose for launching multiple local nodes.
