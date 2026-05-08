# internal/storage

## 这个包是做什么的

`internal/storage` 提供状态机抽象和具体存储实现，负责承接“Raft 已提交的注册变更最终如何落到本地注册表状态”。

它既包含最小状态机接口，也包含真实的 Pebble 存储实现和测试用内存实现。

## 核心逻辑

### 1. 状态机抽象

`state_machine.go` 定义了：

- `Command`
  状态机命令，支持：
  - `put`
  - `delete`
  - `delete_prefix`
- `Result`
  一次 apply 的结果
- `KV`
  一条键值记录
- `MemberAddress`
  成员地址元数据
- `StateMachine`
  最小状态机接口：
  - `Apply`
  - `Get`
  - `Scan`
  - `Snapshot`
  - `Restore`
  - `AppliedIndex`

### 2. PebbleStore

`PebbleStore` 是生产使用的真实实现，核心职责有四类：

1. 保存实例注册表数据
2. 保存状态机元信息
   - `applied index`
   - `applied term`
3. 保存成员地址元数据
   - `__meta__/members/`
4. 提供 snapshot 导出与恢复

它的关键逻辑包括：

- `Apply`
  使用 Pebble batch 原子执行单次状态机命令
- `Get` / `Scan`
  读取注册表数据，并在扫描时跳过 `__meta__/` 内部键
- `Snapshot`
  把实例注册数据和成员地址一起编码导出
- `Restore`
  清空现有注册表数据和成员地址后，按快照内容重建
- `SetAppliedIndex` / `SetAppliedTerm`
  持久化 Raft 已应用进度
- `SetMemberAddress` / `ListMemberAddresses`
  管理成员地址元数据

### 3. MemoryStateMachine

`MemoryStateMachine` 是测试用最小实现，适合不关心真实磁盘存储时的快速验证。

## 提供了哪些能力

- 定义统一的状态机接口
- 执行实例记录写入、删除和批量前缀清理
- 支持按键查询和范围扫描
- 导出和恢复注册表快照
- 记录已应用日志索引与任期
- 持久化并恢复动态成员地址
- 提供 Pebble 版和内存版两种实现

## 关键文件

- `state_machine.go`
  状态机接口、命令模型、内存版状态机
- `pebble.go`
  Pebble 版状态机、元数据存储、snapshot 导出恢复
- `metadata.go`
  状态机本地持久化元信息结构
- `batch.go`
  批处理命令结构
- `keyspace.go`
  逻辑 key 辅助方法

## 包边界

这个包不负责：

- Raft 日志本身的持久化
- 节点间消息转发
- 注册中心领域校验
- HTTP 协议处理

它关注的是“注册表最终数据如何持久化和恢复”。
