# internal/snapshot

## 这个包是做什么的

`internal/snapshot` 提供快照抽象和快照存储实现，负责把注册中心当前的实例注册表视图导出成独立快照文件，并支持后续恢复和安装。

它是 Raft snapshot 能力与底层文件/内存存储之间的桥接层。

## 核心逻辑

这个包的核心在 `Store` 抽象和 `FileStore` 实现。

### 1. 快照抽象

`snapshot.go` 定义了：

- `Metadata`
  快照的 term、index、conf state、checksum、file size
- `Exporter`
  上层把注册表数据写入快照的导出函数
- `Store`
  快照的统一生命周期接口：
  - `Create`
  - `OpenLatest`
  - `Install`
  - `Cleanup`

### 2. 文件版快照存储

`FileStore` 使用如下布局：

- `*.snap` 保存快照内容
- `*.meta` 保存快照元信息

核心流程如下：

1. `Create`
   - 让上层 exporter 把注册表数据写入临时 `.snap.tmp`
   - 写入过程中计算 SHA-256 checksum
   - 获取文件大小，补全 `Metadata`
   - 原子重命名为正式 `.snap`
   - 单独写入 `.meta`
   - 触发旧快照清理

2. `OpenLatest`
   - 扫描目录中的 `.meta`
   - 选出最新快照
   - 打开对应 `.snap` 文件返回给上层

3. `Install`
   - 复用 `Create` 逻辑，把远端传来的快照流落地为本地快照

4. `Cleanup`
   - 只保留最近 N 份快照
   - 成对删除历史 `.snap` 和 `.meta`

### 3. 内存版快照存储

`MemoryStore` 是测试/联调用的轻量实现，只保留最新一份快照，不触碰磁盘。

## 提供了哪些能力

- 定义统一的快照元数据和存储接口
- 把实例注册表数据导出成独立快照文件
- 打开本地最新快照用于恢复
- 安装远端快照到本地存储
- 自动清理旧快照
- 提供内存版快照存储用于测试

## 关键文件

- `snapshot.go`
  快照核心抽象、`Store` 接口、文件版快照存储实现
- `metadata.go`
  快照元信息结构
- `memory.go`
  内存版快照存储
- `installer.go`
  安装能力抽象
- `reader.go`
  最小快照 reader 封装
- `writer.go`
  最小快照 writer 封装

## 包边界

这个包只负责“注册表快照文件生命周期”，不负责：

- 如何从 Raft Ready 决定何时触发 snapshot
- 如何把 snapshot 安装进具体存储实现
- WAL 截断

这些动作由 `raftnode`、`runtime` 和 `storage` 协同完成。
