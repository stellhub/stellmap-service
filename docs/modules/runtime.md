# internal/runtime

## 这个包是做什么的

`internal/runtime` 承载节点运行时的基础设施，负责把本地节点与其他节点连接起来。

它解决的是“节点之间如何找到彼此、如何复制注册表变更、如何安装快照”这类运行时问题，而不是注册中心的业务语义问题。

## 核心逻辑

这个包的核心逻辑分成三部分：

1. 地址簿管理
   `AddressBook` 维护节点 ID 到 HTTP、gRPC、admin 地址的映射，并提供线程安全的读写、快照和删除能力。

2. Ready 消息转发
   `PeerTransport` 会消费本地 Raft 节点产生的 `Ready`，把其中的普通 Raft 消息按目标节点分组后，通过 gRPC 批量转发给远端节点。

3. 快照分片安装与下载
   `InternalTransportService` 是节点内部 gRPC 服务实现，负责：
   - 接收远端节点发送来的 Raft 消息
   - 接收快照分片并在本地拼装后安装
   - 从本地快照存储读取最新快照并切成 chunk 对外提供下载

## 提供了哪些能力

- 维护集群成员地址簿
- 复用连接缓存，向其他节点转发内部 Raft 消息
- 处理 `MsgSnap` 快照消息并按 chunk 发送
- 接收快照 chunk、校验 offset 并在 EOF 时安装注册表快照
- 从本地快照存储导出快照供其他节点拉取

## 关键文件

- `address_book.go`
  地址簿的线程安全实现
- `peer_transport.go`
  Ready 消息转发、snapshot chunk 拆分、peer client 生命周期管理
- `transport_service.go`
  内部 gRPC 服务实现，负责消息接收和快照安装/下载

## 典型协作关系

- 上层 `internal/app` 会持有 `AddressBook` 和 `PeerTransport`
- `PeerTransport` 依赖 `internal/transport/grpc` 的客户端能力
- `InternalTransportService` 依赖：
  - `raftnode` 进行 `Step` 和 `InstallSnapshot`
  - `snapshot.Store` 读取本地最新快照

## 包边界

这个包不负责：

- 注册中心查询与过滤
- 注册表数据存储
- WAL 持久化
- 外部 HTTP API

它专注于“集群内部运行时通信与快照传输”。
