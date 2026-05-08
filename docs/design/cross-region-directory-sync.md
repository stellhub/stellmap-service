# StellMap 跨 Region 目录同步方案设计

## 1. 背景

当前 `StellMap` 已具备以下能力：

- 单 `Raft Group` 内的强一致实例注册、注销、心跳与查询
- 基于 `SSE` 的实例变化 watch：`GET /api/v1/registry/watch`
- 以已提交日志索引作为 `revision` 的增量事件流

当前 `StellMap` 还不具备以下能力：

- 跨共识域、跨 `region` 的目录同步
- 远端目录投影与本地原生目录的分层存储
- 同步回环防护
- 每个远端源的同步水位持久化与恢复
- 面向跨 `region` 服务发现的聚合查询语义

因此，如果部署形态是“每个 `region` 一套独立 StellMap 集群”，那么仅靠现有能力，客户端无法只查询本地 StellMap 就同时得到远端 `region` 的服务目录。

本设计的目标，就是在不打破“每个 `region` 独立共识域”这一前提下，为 StellMap 增加一套 **跨 Region 目录同步** 能力。

## 2. 设计目标

### 2.1 目标

- 为跨 `region` 目录同步提供独立于客户端 watch 的内部同步接口
- 让每个 `region` 保持独立共识域，不做跨 `region` 单一 `Raft` 集群
- 将远端目录以“复制视图”的方式落到本地，而不是伪装成本地原生注册
- 支持 `snapshot + revision` 的增量追平模式
- 支持每个远端源独立的同步水位管理
- 支持防回环，避免目录在多个 `region` 之间无限反复复制
- 支持按服务维度控制哪些目录允许被导出到哪些 `region`

### 2.2 非目标

- 不在第一阶段实现双向冲突合并
- 不在第一阶段实现跨 `region` 的强一致读写
- 不在第一阶段让远端复制目录参与本地心跳续约
- 不在第一阶段做“所有远端实例原样透传给所有客户端”的全开放模型

## 3. 总体思路

整体思路分成四层：

1. **源集群**
   每个 `region` 的 StellMap 都是本地实例注册的权威源。

2. **同步器**
   本地部署一个 `replicator`，去订阅远端 StellMap 的 `SSE watch`。

3. **复制视图**
   同步到本地的数据不写入原生注册前缀，而是写入单独的“复制目录前缀”。

4. **查询聚合**
   本地查询接口按策略把“本地原生目录”和“远端复制目录”合并后返回给客户端。

5. **内部同步通道**
   跨 `region` 同步器通过独立内部 watch 接口订阅目录变化，不复用客户端公开 watch。

换句话说，跨 `region` 目录同步不是把多个 `region` 拼成一个更大的共识域，而是：

- 本地数据仍然由本地集群强一致维护
- 远端数据以异步复制视图的方式进入本地
- 查询阶段再做本地优先、远端回退、网关入口替换等策略处理

## 4. 核心原则

### 4.1 本地注册是权威，远端复制是投影

必须明确区分两类数据：

- **本地原生目录**
  由当前 `region` 的业务实例直接注册，受本地心跳、TTL、清理任务管理

- **远端复制目录**
  由本地同步器从其他 `region` 拉取并落地，只能由同步器更新，不能被当作本地实例续约

### 4.2 不能直接复用原生注册前缀

如果把远端同步目录直接写入 `/registry/...`：

- 本地过期清理会误删远端目录
- 本地 watch 会把远端目录再当成本地目录继续广播
- 很难区分本地实例和远端实例
- 容易形成同步回环

因此必须使用单独前缀承载远端复制视图。

### 4.3 以事件流为同步源，以水位做恢复

同步链路要沿用现有“`snapshot + revision` 事件流”的思路，但通过独立内部接口输出：

- 初次同步使用 `snapshot`
- 之后使用 `revision` 增量追平
- 本地持久化每个远端源的最后已应用水位

### 4.4 客户端 watch 与同步 watch 必须分离

客户端 watch 和跨 `region` 同步 watch 的目标完全不同：

- 客户端 watch 面向业务调用方、SDK、sidecar
- 同步 watch 面向受信任的内部复制器

两者在以下方面天然不同：

- 暴露面不同
- 返回字段不同
- 权限要求不同
- 回放语义不同
- 流控与限流策略不同

因此本方案从第一阶段开始就要求：

- 公共客户端 watch 继续保留在 `/api/v1/registry/watch`
- 跨 `region` 目录同步单独使用内部接口

## 5. 数据模型设计

## 5.1 服务级跨 Region 策略

为服务定义以下跨区策略字段：

- `crossRegionMode=forbid|gateway|direct`
- `homeRegion`
- `exportToRegions`

含义如下：

- `forbid`
  不允许跨 `region` 导出目录
- `gateway`
  允许跨 `region` 访问，但远端返回的是“服务入口”而不是实例列表
- `direct`
  允许跨 `region` 直接同步实例目录
- `homeRegion`
  服务的主属地
- `exportToRegions`
  允许导出到哪些远端 `region`

说明：

- 这些字段描述的是“服务如何被导出”，不是“实例如何注册”
- 它们应该作为服务级元数据，不建议只挂在单实例上

### 5.1.1 gateway 模式的建模原则

当 `crossRegionMode=gateway` 时，不建议把“网关入口”伪装成普通实例来注册。

原因：

- 网关不是业务实例本身
- 网关入口通常是服务级、区域级暴露，不是单个实例客户端能准确感知的属性
- 如果让实例客户端自己上报网关地址，容易出现：
  - 实例并不知道最终网关域名
  - 网关切换后实例侧数据滞后
  - 同一个服务多个实例重复上报同一份网关信息

因此建议区分两类对象：

- **实例注册**
  仍由业务实例客户端上报自己的本地直连地址，例如 Pod IP、Node IP、内网域名
- **跨 Region 导出入口**
  由网关控制器、控制面或运维面单独维护，描述“外部 region 应该通过哪个入口访问这个服务”

### 5.1.2 gateway 模式的导出对象

建议新增服务导出对象 `ServiceExportPolicy`：

```go
type ServiceExportPolicy struct {
    Namespace         string            `json:"namespace"`
    Service           string            `json:"service"`
    CrossRegionMode   string            `json:"crossRegionMode"`
    HomeRegion        string            `json:"homeRegion"`
    ExportToRegions   []string          `json:"exportToRegions,omitempty"`
    GatewayEndpoints  []Endpoint        `json:"gatewayEndpoints,omitempty"`
    Metadata          map[string]string `json:"metadata,omitempty"`
    UpdatedAtUnix     int64             `json:"updatedAtUnix"`
}
```

其中：

- `direct` 模式下，`GatewayEndpoints` 可以为空
- `gateway` 模式下，远端应返回 `GatewayEndpoints`

谁来维护它：

- 推荐由网关控制器、控制面服务或运维工具维护
- 不推荐由普通业务实例注册接口直接维护

这样可以避免让实例客户端承担“理解全局网关拓扑”的职责。

## 5.2 复制实例模型

建议新增复制视图模型 `ReplicatedValue`：

```go
type ReplicatedValue struct {
    Namespace         string            `json:"namespace"`
    Service           string            `json:"service"`
    InstanceID        string            `json:"instanceId"`
    Zone              string            `json:"zone,omitempty"`
    Labels            map[string]string `json:"labels,omitempty"`
    Metadata          map[string]string `json:"metadata,omitempty"`
    Endpoints         []Endpoint        `json:"endpoints"`
    LeaseTTLSeconds   int64             `json:"leaseTtlSeconds"`
    RegisteredAtUnix  int64             `json:"registeredAtUnix"`
    LastHeartbeatUnix int64             `json:"lastHeartbeatUnix"`

    SourceRegion      string            `json:"sourceRegion"`
    SourceClusterID   string            `json:"sourceClusterId"`
    SourceRevision    uint64            `json:"sourceRevision"`
    ExportedAtUnix    int64             `json:"exportedAtUnix"`
    ReplicatedAtUnix  int64             `json:"replicatedAtUnix"`
    Origin            string            `json:"origin"` // 固定为 "replicated"
}
```

设计意图：

- 保留原始实例信息，便于查询阶段复用现有过滤逻辑
- 显式记录来源 `region`、来源集群、来源修订号
- 用 `Origin=replicated` 把远端目录与本地目录分层

## 5.3 复制水位模型

为每个远端同步源保存一份 `ReplicationCheckpoint`：

```go
type ReplicationCheckpoint struct {
    SourceRegion          string `json:"sourceRegion"`
    SourceClusterID       string `json:"sourceClusterId"`
    LastAppliedRevision   uint64 `json:"lastAppliedRevision"`
    LastSnapshotRevision  uint64 `json:"lastSnapshotRevision"`
    LastEventType         string `json:"lastEventType,omitempty"`
    UpdatedAtUnix         int64  `json:"updatedAtUnix"`
}
```

用途：

- 节点重启后从上次水位继续同步
- 避免同一事件重复落地
- 便于观测和告警

## 6. 存储前缀设计

建议把本地目录和远端复制目录彻底分开。

### 6.1 本地原生目录

保持现状：

```text
/registry/{namespace}/{service}/{instanceId}
```

### 6.2 远端复制目录

新增复制前缀：

```text
/replication/regions/{sourceRegion}/clusters/{sourceClusterId}/registry/{namespace}/{service}/{instanceId}
```

示例：

```text
/replication/regions/cn-bj/clusters/bj-prod-01/registry/prod/order-service/order-10.0.1.23
```

### 6.3 同步水位前缀

新增检查点前缀：

```text
/replication/checkpoints/{sourceRegion}/{sourceClusterId}
```

### 6.4 导出策略前缀

如果服务级策略单独存储，可使用：

```text
/registry-policies/{namespace}/{service}
```

这个前缀下保存的不是实例，而是服务导出策略，例如：

- `crossRegionMode`
- `homeRegion`
- `exportToRegions`
- `gatewayEndpoints`

## 7. 内部同步协议设计

## 7.1 同步接口与客户端接口分离

客户端接口保持：

```text
GET /api/v1/registry/watch?namespace=prod&service=order-service
```

内部同步接口新增为：

```text
GET /internal/v1/replication/watch?namespace=prod&service=order-service&targetRegion=cn-bj
```

设计约束：

- 只允许内网访问
- 必须携带同步鉴权信息
- 只输出允许导出到 `targetRegion` 的目录
- 只输出本地原生目录，不输出复制目录

## 7.2 为什么不能继续复用客户端 watch

客户端 watch 当前语义是：

- 建连先返回 `snapshot`
- 后续返回 `upsert` / `delete`
- 每条事件都带 `revision`

但它仍然不适合直接作为跨 `region` 同步接口，原因有：

- 它是公共数据面接口，不适合作为集群间复制入口
- 它当前按客户端查询语义过滤，而不是按导出策略过滤
- 它没有携带源集群身份字段
- 它没有面向复制器的鉴权、回放、流控约束
- 后续一旦要加 `sinceRevision`、批量快照、导出策略上下文，会让客户端接口变重

因此必须拆分。

## 7.3 同步 watch 返回模型

内部同步 watch 建议返回以下事件模型：

```json
{
  "revision": 12345,
  "type": "upsert",
  "namespace": "prod",
  "service": "order-service",
  "instanceId": "order-10.0.1.23",
  "sourceRegion": "cn-sh",
  "sourceClusterId": "sh-prod-01",
  "exportedAtUnix": 1710000000,
  "instance": {}
}
```

其中：

- `sourceRegion`
- `sourceClusterId`
- `exportedAtUnix`

属于同步专用字段，不建议放入公共客户端 watch。

### snapshot 事件

```json
{
  "revision": 12345,
  "type": "snapshot",
  "namespace": "prod",
  "service": "order-service",
  "sourceRegion": "cn-sh",
  "sourceClusterId": "sh-prod-01",
  "instances": []
}
```

## 7.4 同步接口路径建议

建议专门放在内部前缀下：

```text
GET /internal/v1/replication/watch
```

查询参数建议：

- `namespace`
- `service`
- `targetRegion`
- `sinceRevision`（第二阶段）
- `mode=snapshot_then_stream`

请求头建议：

- `X-StellMap-Replication-Region`
- `X-StellMap-Replication-Cluster`
- `Authorization: Bearer <replication-token>`

## 7.5 snapshot + 增量同步流程

### 初次同步

1. 同步器向远端发起 `/internal/v1/replication/watch`
2. 收到 `snapshot` 事件
3. 用快照内容全量覆盖本地对应的复制视图
4. 记录 `LastSnapshotRevision`
5. 继续接收后续事件

### 增量同步

1. 收到 `upsert` 或 `delete`
2. 判断 `event.revision > LastAppliedRevision`
3. 若成立，则应用到本地复制前缀
4. 更新 `LastAppliedRevision`

### 重连恢复

第一阶段恢复策略为：

1. 连接断开
2. 重新建连
3. 再收一份 `snapshot`
4. 用新的快照覆盖本地复制视图
5. 继续接收新事件

说明：

- 这会带来额外的全量成本
- 但实现简单、正确性高
- 第二阶段再补 `sinceRevision` 做事件回放优化

## 7.6 传输协议选择

内部同步 watch 第一阶段建议继续使用：

- `HTTP/2 + SSE`

而不是：

- `HTTP/2 server push`

原因：

1. `SSE` 的语义就是“一个长连接上的增量事件流”，和同步 watch 完全匹配
2. `HTTP/2` 可以提供多路复用、头压缩、连接复用，对大量长连接更友好
3. `server push` 的语义是“服务端替客户端预推某个请求对应的响应资源”，并不是一个通用的持续事件流机制
4. `server push` 是逐跳能力，和代理、反向代理、中间层的兼容性更差
5. 当前主流客户端生态对 `server push` 支持并不理想，长期演进价值偏低

因此建议：

- 公共客户端 watch：`HTTP/1.1` 和 `HTTP/2` 都兼容，继续使用 `SSE`
- 内部同步 watch：优先运行在 `HTTP/2` 上，但消息模型仍然是 `SSE`
- 若后续需要更强的双向流控和内部协议约束，再演进到 `gRPC stream`

## 8. 同步器设计

建议新增独立组件：`replicator`

职责：

- 维护到多个远端 `region` 的 SSE 连接
- 通过内部同步 watch 订阅远端目录变化
- 管理每个远端源的同步水位
- 将远端目录事件转换为本地复制命令
- 负责失败重连、退避、快照重建

建议配置：

```text
replication.targets[0].region=cn-bj
replication.targets[0].clusterId=bj-prod-01
replication.targets[0].watchURL=http://bj-stellmap:8080
replication.targets[0].services=prod/order-service,prod/payment-service
```

说明：

- 第一阶段建议按服务白名单同步，不做全注册表全量联邦
- 后续再扩展成按命名空间或策略自动发现

## 9. 防回环设计

## 9.1 问题

如果：

- 上海同步北京
- 北京同步上海

而且双方都把复制目录继续当成“可导出原生目录”，就会形成回环。

## 9.2 基本规则

必须落实以下规则：

1. **只有本地原生目录可以对外导出**
2. **复制目录永远不能再次进入导出流**
3. **所有复制记录必须带来源标识**

## 9.3 事件层防回环

内部同步 watch 事件只能来自：

- `/registry/...`

不能来自：

- `/replication/...`

也就是说，内部同步接口只能导出原生目录事件，不对复制目录生成同步事件。

## 9.4 存储层防回环

即便误操作写入复制前缀，也不能触发“同步再导出”。

因此查询和导出逻辑都要明确区分：

- `origin=local`
- `origin=replicated`

## 10. 水位管理设计

## 10.1 水位粒度

建议按“远端源”维度保存水位：

- `sourceRegion`
- `sourceClusterId`

必要时可扩展到：

- `sourceRegion + sourceClusterId + namespace + service`

第一阶段建议先按“源集群”粒度管理。

## 10.2 水位写入时机

每次成功应用远端事件后：

1. 先写复制目录
2. 再写本地检查点

这样即使崩溃重启，也只会：

- 重复应用少量事件
- 不会跳过尚未落盘的事件

## 10.3 幂等要求

复制应用必须具备幂等性：

- `upsert` 多次写入同一实例应得到同样结果
- `delete` 重复执行应安全无害
- `revision <= checkpoint` 的事件直接跳过

## 11. 查询语义设计

本地查询时，建议支持三种模式：

### 11.1 local_only

只查本地原生目录。

适合：

- 默认服务发现
- 高实时性要求
- 禁止跨 `region` 的服务

### 11.2 local_prefer

先查本地原生目录，本地为空时回退到远端复制目录。

适合：

- 同 `region` 优先
- 跨 `region` 灾备回退
- `gateway` 模式服务的远端入口回退

### 11.3 merged

把本地原生目录与远端复制目录合并返回，但结果中必须保留：

- `sourceRegion`
- `sourceClusterId`
- `origin`

适合：

- 控制台
- 调试接口
- 特殊白名单客户端

## 11.4 gateway 模式下的返回语义

当服务策略为：

```text
crossRegionMode=gateway
```

则跨 `region` 查询不应返回远端实例明细，而应返回远端服务入口。

建议返回模型中增加：

- `origin=replicated_gateway`
- `sourceRegion`
- `gatewayEndpoints`

说明：

- 本地 `region` 查询仍然可以返回本地实例
- 远端 `region` 查询只返回服务网关入口
- 这样客户端不需要知道远端实例拓扑，也不需要知道真正的网关是如何绑定到后端实例的

## 12. TTL 与过期清理设计

远端复制目录不能直接复用本地过期清理逻辑。

### 本地原生目录

继续保持现状：

- 本地心跳驱动 `LastHeartbeatUnix`
- Leader 后台扫描 TTL 过期
- 通过本地 `Raft delete` 真正清理

### 远端复制目录

以源集群事件为准：

- 源集群发 `delete`，本地才删除复制目录
- 若连接中断，则保留最后同步视图
- 可额外增加“复制陈旧度告警”，但不应直接按本地 TTL 删除

原因：

- 复制目录的心跳时钟在源集群，不在本地
- 本地自行 TTL 清理容易误删远端仍然健康的目录

## 13. 故障恢复与一致性

## 13.1 远端短暂不可达

处理方式：

- 重连并指数退避
- 保留本地已同步的复制视图
- 暴露同步滞后指标

## 13.2 本地重启

处理方式：

- 先恢复本地检查点
- 重新连接远端 watch
- 当前第一阶段直接重新拿 `snapshot`
- 覆盖本地复制视图后继续增量同步

## 13.3 源集群重建

如果源集群替换为新的 `clusterId`：

- 应视为新的同步源
- 原有检查点不复用
- 由运维显式决定是否清理旧复制目录

## 14. 观测与运维

建议新增以下指标：

- `stellmap_replication_connected{source_region,source_cluster}`
- `stellmap_replication_last_applied_revision{source_region,source_cluster}`
- `stellmap_replication_lag_seconds{source_region,source_cluster}`
- `stellmap_replication_snapshot_total{source_region,source_cluster}`
- `stellmap_replication_event_total{source_region,source_cluster,type}`
- `stellmap_replication_error_total{source_region,source_cluster}`

建议新增以下控制面查询：

- 当前有哪些远端同步目标
- 每个目标当前连接状态
- 每个目标最后已应用 revision
- 最后一次快照时间
- 最近错误信息

## 15. 分阶段落地建议

### Phase 1：最小可用

- 新增独立内部同步 watch 接口
- 单向同步
- 服务级白名单配置
- 仅支持 `crossRegionMode=direct`
- 独立复制前缀
- 独立检查点前缀
- 断线后重新拉 `snapshot`
- 内部同步连接优先跑在 `HTTP/2`
- 查询阶段先仅提供 `local_prefer`

### Phase 2：能力增强

- 支持 `sinceRevision`
- 增加复制状态控制面
- 支持更多服务级导出策略
- 支持 `crossRegionMode=gateway`
- 评估是否演进为 `gRPC stream`

### Phase 3：联邦化增强

- 支持自动发现可导出服务
- 支持 gateway/direct 混合返回
- 支持更细粒度的回退策略

## 16. 对当前代码的落点建议

建议新增或修改这些模块：

- `internal/registry`
  - 新增复制目录模型
  - 新增复制前缀与检查点前缀工具
  - 新增复制目录与本地目录的查询合并逻辑

- `internal/transport/http`
  - 保留公共 `WatchInstances`
  - 新增内部复制专用 watch

- `internal/app`
  - 增加复制同步器生命周期管理

- `internal/storage`
  - 增加复制目录和复制检查点的持久化能力

- `cmd/stellmapd`
  - 增加 replication target 配置

## 17. 第一阶段实现清单

第一阶段目标限定为：

- 独立内部同步 watch
- 单向跨 `region` 目录同步
- 仅支持 `direct` 模式
- 仅同步服务白名单
- 断线后全量 `snapshot` 重建

## 17.1 新增数据结构

### `internal/registry`

建议新增文件：

- `internal/registry/replication.go`

建议新增结构：

```go
type ReplicatedValue struct {
    Namespace         string
    Service           string
    InstanceID        string
    Zone              string
    Labels            map[string]string
    Metadata          map[string]string
    Endpoints         []Endpoint
    LeaseTTLSeconds   int64
    RegisteredAtUnix  int64
    LastHeartbeatUnix int64

    SourceRegion      string
    SourceClusterID   string
    SourceRevision    uint64
    ReplicatedAtUnix  int64
    Origin            string
}

type ReplicationCheckpoint struct {
    SourceRegion         string
    SourceClusterID      string
    LastAppliedRevision  uint64
    LastSnapshotRevision uint64
    UpdatedAtUnix        int64
}
```

建议新增辅助函数：

- `ReplicationPrefix(sourceRegion, sourceClusterID string) string`
- `ReplicatedKey(sourceRegion, sourceClusterID, namespace, service, instanceID string) []byte`
- `ReplicationCheckpointKey(sourceRegion, sourceClusterID string) []byte`
- `ParseReplicatedKey(key []byte) (...)`

### `internal/transport/http`

建议在：

- `internal/transport/http/dto.go`

新增 DTO：

```go
type ReplicationWatchEventDTO struct {
    Revision        uint64                `json:"revision"`
    Type            string                `json:"type"`
    Namespace       string                `json:"namespace"`
    Service         string                `json:"service"`
    InstanceID      string                `json:"instanceId,omitempty"`
    SourceRegion    string                `json:"sourceRegion"`
    SourceClusterID string                `json:"sourceClusterId"`
    ExportedAtUnix  int64                 `json:"exportedAtUnix,omitempty"`
    Instance        *RegistryInstanceDTO  `json:"instance,omitempty"`
    Instances       []RegistryInstanceDTO `json:"instances,omitempty"`
}
```

## 17.2 新增路由与 handler

### 公共路由保持不变

- `GET /api/v1/registry/watch`

### 新增内部同步路由

建议新增：

- `GET /internal/v1/replication/watch`

建议涉及文件：

- `internal/transport/http/server.go`
- `internal/transport/http/handler_registry.go`
- `internal/transport/http/handlers_impl.go`

建议新增接口：

```go
type ReplicationHandler interface {
    WatchReplication(w http.ResponseWriter, r *http.Request)
}
```

建议新增实现：

- `ReplicationAPI`

职责：

- 校验内部同步请求来源和 token
- 解析 `namespace/service/targetRegion`
- 过滤不可导出的服务
- 输出 `snapshot + upsert + delete`
- 事件只来自原生目录，不来自复制目录

## 17.3 新增存储前缀

第一阶段需要正式落地两个前缀：

### 复制目录前缀

```text
/replication/regions/{sourceRegion}/clusters/{sourceClusterId}/registry/{namespace}/{service}/{instanceId}
```

用途：

- 存储远端同步过来的实例目录

### 同步检查点前缀

```text
/replication/checkpoints/{sourceRegion}/{sourceClusterId}
```

用途：

- 存储最后已应用 `revision`
- 存储最后一次快照水位

第一阶段暂不要求落地：

- `/registry-policies/...`

因为第一阶段只支持白名单 + `direct` 模式，可以先通过启动配置完成。

## 17.4 新增后台组件

### `internal/app`

建议新增：

- `internal/app/replication.go`

新增组件：

```go
type ReplicationTarget struct {
    SourceRegion    string
    SourceClusterID string
    WatchURL        string
    Token           string
    Services        []string
}

type Replicator struct {
    ...
}
```

职责：

- 为每个 target 建立独立同步 goroutine
- 发起 `/internal/v1/replication/watch`
- 处理 snapshot 覆盖和增量事件
- 更新 checkpoint
- 指数退避重连

同时建议：

- 在 `internal/app/app.go` 中把 replicator 纳入生命周期管理

## 17.5 新增配置项

### `cmd/stellmapd`

建议新增配置：

- `--region`
- `--cluster-name`
- `--replication-targets-file`
- `--replication-token`

其中：

- `region` 用于标识当前节点所属地域
- `cluster-name` 用于同步来源标识，建议后续替代当前纯数字 `cluster-id` 的展示角色
- `replication-targets-file` 指向结构化 JSON 配置文件，描述远端 watch 地址与服务白名单
- `replication-token` 用于内部同步鉴权

第一阶段可以先使用一个简单的字符串格式，例如：

```text
--replication-targets-file=/etc/stellmapd/replication-targets.json
```

后续再改成更清晰的多参数或配置文件格式。

## 17.6 查询侧实现调整

第一阶段查询实现只需要支持：

- 本地目录查询
- 本地为空时回退远端复制目录

建议新增：

- `internal/registry/query_merge.go`

建议新增能力：

- `MergeLocalPrefer(local, replicated []ValueLike) []ValueLike`

或者直接在查询 handler 中做第一版合并，后续再下沉到 `registry` 包。

## 17.7 测试清单

### `internal/registry`

新增测试建议：

- 复制目录 key 生成与解析测试
- checkpoint key 生成与解析测试
- replicated value 编解码测试

### `internal/transport/http`

新增测试建议：

- `GET /internal/v1/replication/watch` 首次返回 `snapshot`
- 仅导出原生目录，不导出复制目录
- 带 `targetRegion` 时正确过滤不可导出服务
- 鉴权失败返回 `401/403`

### `internal/storage`

新增测试建议：

- 复制目录写入与扫描测试
- checkpoint 持久化与恢复测试
- snapshot/restore 后复制目录与 checkpoint 一并恢复

### `internal/app`

新增测试建议：

- replicator 首次同步会用 snapshot 覆盖本地复制目录
- `revision` 小于等于 checkpoint 时被跳过
- 同步中断后会退避重连
- checkpoint 在事件成功写入后更新

### `cmd/stellmapd`

新增集成测试建议：

- 两套独立 StellMap，A 作为源，B 作为目标
- A 注册实例，B 通过同步看到复制目录
- A 删除实例，B 最终删除复制目录
- B 的公共客户端 watch 不会收到复制目录事件

## 17.8 第一阶段明确不做的内容

- `gateway` 模式的服务导出对象
- `sinceRevision` 增量回放
- 多源冲突处理
- 自动服务导出策略
- 双向复制
- gRPC 流式同步

## 18. 结论

StellMap 的跨 `region` 目录同步，不应该通过“跨 `region` 组成一个更大的 Raft 共识域”来实现，而应该通过：

- 每个 `region` 独立共识
- 复用 `SSE watch` 做异步目录同步
- 本地目录与远端复制目录分层存储
- 通过来源标识、防回环规则和同步水位保证正确性

这样既保留了本地注册中心的强一致和自治性，也为跨 `region` 服务发现提供了可以渐进演进的基础。
