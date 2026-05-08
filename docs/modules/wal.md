# internal/wal

## 这个包是做什么的

`internal/wal` 提供 Raft 日志的预写日志能力，负责把 `HardState` 和 `Entry` 持久化下来，并在节点重启后恢复出来。

它解决的是“承载注册变更的 Raft 日志怎么安全落盘、怎么恢复、怎么在 snapshot 之后回收旧日志”。

## 核心逻辑

### 1. WAL 抽象

`wal.go` 定义了最小 WAL 接口：

- `Open`
- `Append`
- `Load`
- `TruncatePrefix`
- `Sync`
- `Close`

这个接口刻意保持很小，目的是让上层只依赖 Raft 真正需要的持久化语义，而不把注册中心产品语义耦合进日志层。

### 2. FileWAL

`FileWAL` 是生产使用的真实实现，采用：

- `hardstate.bin`
  保存最近一次持久化的 HardState
- `*.wal`
  保存日志条目 segment

核心流程如下：

1. `Open`
   - 创建目录
   - 读取 `hardstate.bin`
   - 顺序扫描全部 `*.wal`
   - 恢复内存中的 `state`、`entries`、`segments`

2. `Append`
   分三种情况处理：
   - 只有 HardState 更新：只写状态文件
   - 新日志与当前 tail 连续：增量追加到最后一个 segment
   - 新日志与旧 tail 重叠：触发 tail rewrite，按 Raft 语义覆盖旧日志

3. `TruncatePrefix`
   - 删除完全落在截断点之前的 segment
   - 重写跨越截断点的边界 segment
   - 保留未受影响的 tail segment

4. `Load`
   返回当前 HardState 和全部日志条目的副本

### 3. segment 管理

`segment.go` 里的 `Segment` 维护单个 `.wal` 文件所覆盖的日志索引区间。

通过 segment 切分，有几个直接收益：

- 正常写路径只会追加最后一个文件
- 日志压缩时可以按段回收
- 覆盖冲突时只重写受影响的 tail

### 4. 编解码抽象

`codec.go` 抽象了 HardState 和 Entry 的编解码，目前默认使用 `ProtobufCodec`。

这让 FileWAL 不需要直接关心 protobuf 细节，也为后续替换磁盘格式预留了空间。

### 5. 内存版 WAL

`memory.go` 提供 `MemoryWAL`，用于测试和联调，语义尽量贴近 `FileWAL`，但不触碰磁盘。

## 提供了哪些能力

- 持久化 Raft HardState
- 持久化和恢复日志条目
- 在日志冲突时重写 tail
- 在注册表快照生成之后回收旧日志
- 维护 segment 元信息
- 提供可替换的编解码层
- 提供内存版 WAL 便于测试

## 关键文件

- `wal.go`
  WAL 接口、FileWAL 主流程、segment 追加/重写/截断逻辑
- `codec.go`
  HardState / Entry 编解码抽象与 protobuf 实现
- `segment.go`
  segment 元信息结构
- `memory.go`
  内存版 WAL
- `repair.go`
  WAL 修复能力抽象
- `wal_test.go`
  segment 增量追加、tail rewrite、prefix truncate 回归测试

## 包边界

这个包不负责：

- 状态机 apply
- 快照文件存储
- 节点间 transport
- HTTP 或注册中心业务

它专注于“Raft 日志持久化与恢复”。
