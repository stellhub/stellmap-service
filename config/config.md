# StellMap 配置说明 / StellMap Configuration Guide

本文档说明 [stellmapd.toml](E:/PersonalCode/GoProject/stellmap/config/stellmapd.toml) 中每个配置项的作用、用法和建议，方便在开发、测试和生产环境中快速查阅。

This document explains every configuration item in [stellmapd.toml](E:/PersonalCode/GoProject/stellmap/config/stellmapd.toml), including purpose, usage, and recommendations for development, testing, and production environments.

## 1. 配置加载规则 / Configuration Loading Rules

- 中文：`stellmapd` 优先从命令行参数读取配置；如果命令行指定了同名参数，会覆盖 `TOML` 文件中的值。
- English: `stellmapd` gives higher priority to command-line flags; when the same option is provided on the CLI, it overrides the value from the `TOML` file.

- 中文：当前优先级为：`命令行参数 > --config 指定的 TOML 文件 > 未提供时报错`。
- English: The effective priority is: `CLI flags > TOML file passed by --config > startup validation error if required fields are missing`.

- 中文：配置文件在 [internal/app/config_file.go](E:/PersonalCode/GoProject/stellmap/internal/app/config_file.go:47) 中解析，在 [internal/app/app.go](E:/PersonalCode/GoProject/stellmap/internal/app/app.go:82) 中校验。
- English: The configuration file is parsed in [internal/app/config_file.go](E:/PersonalCode/GoProject/stellmap/internal/app/config_file.go:47) and validated in [internal/app/app.go](E:/PersonalCode/GoProject/stellmap/internal/app/app.go:82).

## 2. 当前配置文件内容 / Current Configuration File

```toml
[node]
id = 1
cluster_id = 100
region = "default"
data_dir = "/data/stellmap/node"

[server]
http_addr = "0.0.0.0:8080"
admin_addr = "127.0.0.1:18080"
grpc_addr = "127.0.0.1:19090"

[auth]
admin_token = "stellmap"
replication_token = "stellmap"
prometheus_sd_token = "stellmap"

[cluster]
peer_ids = "1"
peer_grpc_addrs = "1=127.0.0.1:19090"
peer_http_addrs = "1=127.0.0.1:8080"
peer_admin_addrs = "1=127.0.0.1:18080"

[runtime]
request_timeout = "5s"
shutdown_timeout = "10s"

[registry]
cleanup_interval = "1s"
cleanup_delete_limit = 128

[replication]
targets_file = ""
```

## 3. 配置项详解 / Detailed Item Reference

### 3.1 `[node]`

#### `id = 1`

- 中文作用：当前节点的唯一 ID，用于 Raft 节点身份识别。
- English purpose: Unique node identifier used as the Raft member identity.

- 中文用法：必须是大于 `0` 的整数；同一个集群中的所有节点都不能重复。
- English usage: Must be an integer greater than `0`; every node in the same cluster must use a distinct value.

- 中文建议：节点一旦基于某个 `data_dir` 跑起来，就不要随意修改 `id`，否则历史持久化数据和节点身份可能不一致。
- English recommendation: Once a node has started with a specific `data_dir`, do not casually change `id`, or the persisted state may no longer match the node identity.

#### `cluster_id = 100`

- 中文作用：标识当前节点属于哪一个集群。
- English purpose: Identifies which cluster this node belongs to.

- 中文用法：必须是大于 `0` 的整数；同一套集群中的所有节点应该保持完全一致。
- English usage: Must be a positive integer; all members in the same cluster should use the exact same value.

- 中文建议：如果你有多套环境，例如 `dev`、`staging`、`prod`，建议为每套集群分配不同的 `cluster_id`，避免运维混淆。
- English recommendation: If you run multiple environments such as `dev`, `staging`, and `prod`, assign different `cluster_id` values to each cluster to avoid operational confusion.

#### `region = "default"`

- 中文作用：标识当前节点所属区域，主要用于跨 region 复制时标注来源信息。
- English purpose: Marks the region of the current node, mainly used as source metadata during cross-region replication.

- 中文用法：这是必填字符串，不能为空。
- English usage: This is a required non-empty string.

- 中文建议：单机开发可以保留 `default`；正式环境建议使用更明确的名字，例如 `cn-sh`、`cn-bj`、`us-west-2`。
- English recommendation: `default` is fine for local development, but production should use a meaningful region name such as `cn-sh`, `cn-bj`, or `us-west-2`.

#### `data_dir = "/data/stellmap/node"`

- 中文作用：本地持久化数据目录，通常保存 Raft、WAL、snapshot 等运行状态。
- English purpose: Local persistent data directory for raft state, WAL files, snapshots, and related runtime data.

- 中文用法：启动时如果目录不存在，程序会自动创建。
- English usage: The directory is created automatically on startup if it does not exist.

- 中文建议：每个节点都必须使用独立的数据目录，不能多个节点共用同一个路径。
- English recommendation: Every node must have its own dedicated data directory; never share one path across multiple nodes.

### 3.2 `[server]`

#### `http_addr = "0.0.0.0:8080"`

- 中文作用：公共 HTTP 服务监听地址，对外注册、查询、心跳、健康检查、指标和 Prometheus SD 都走这里。
- English purpose: Public HTTP listen address for registration, query, heartbeat, health, metrics, and Prometheus SD endpoints.

- 中文用法：格式为 `host:port`。
- English usage: The format is `host:port`.

- 中文建议：开发环境用 `0.0.0.0` 比较方便；生产环境建议前面放网关或负载均衡，只暴露公共 HTTP 面。
- English recommendation: `0.0.0.0` is convenient in development; in production, expose only the public HTTP plane behind a gateway or load balancer.

#### `admin_addr = "127.0.0.1:18080"`

- 中文作用：独立的管理面 HTTP 地址，用于状态查询、成员变更、Leader 转移等管理操作。
- English purpose: Dedicated admin HTTP endpoint used for status queries, membership changes, and leader transfer operations.

- 中文用法：除了需要 `Authorization: Bearer <admin_token>`，当前实现还强制要求请求来源是 `127.0.0.1`，见 [cmd/stellmapd/main.go](E:/PersonalCode/GoProject/stellmap/cmd/stellmapd/main.go:184)。
- English usage: In addition to `Authorization: Bearer <admin_token>`, the current implementation also enforces that the request source must be `127.0.0.1`, as shown in [cmd/stellmapd/main.go](E:/PersonalCode/GoProject/stellmap/cmd/stellmapd/main.go:184).

- 中文建议：保持监听在 `127.0.0.1` 最安全，远程管理时通过 SSH 登录到节点本机执行。
- English recommendation: Keeping this endpoint bound to `127.0.0.1` is the safest option; perform remote administration by SSH-ing into the node itself.

#### `grpc_addr = "127.0.0.1:19090"`

- 中文作用：内部 gRPC 监听地址，用于节点间 Raft 消息传输和快照复制。
- English purpose: Internal gRPC listen address for inter-node raft transport and snapshot replication.

- 中文用法：这是内部复制面，不是业务方直接调用的公共接口。
- English usage: This is an internal replication plane rather than a public business-facing API.

- 中文建议：当前值适合单机测试；如果是多机部署，通常应改成 `0.0.0.0:<port>`，并在 `peer_grpc_addrs` 中填写其他节点可实际访问的地址。
- English recommendation: The current value is suitable for single-host testing; in multi-host deployments, it is usually better to bind to `0.0.0.0:<port>` and provide real reachable addresses in `peer_grpc_addrs`.

### 3.3 `[auth]`

#### `admin_token = "stellmap"`

- 中文作用：管理面 HTTP Bearer Token。
- English purpose: Bearer token for the admin HTTP plane.

- 中文用法：请求 admin API 时必须携带 `Authorization: Bearer <admin_token>`。
- English usage: Requests to admin APIs must include `Authorization: Bearer <admin_token>`.

- 中文建议：默认值 `stellmap` 只是占位符，生产环境务必替换成强随机字符串。
- English recommendation: The default value `stellmap` is only a placeholder; replace it with a strong random string in production.

#### `replication_token = "stellmap"`

- 中文作用：内部复制 watch 的固定鉴权 Token。
- English purpose: Shared bearer token used for internal replication watch authorization.

- 中文用法：当本节点作为复制客户端拉取远端 `/internal/v1/replication/watch` 时，会使用这个 token，见 [internal/app/replication.go](E:/PersonalCode/GoProject/stellmap/internal/app/replication.go:131)。
- English usage: When this node acts as a replication client and pulls from the remote `/internal/v1/replication/watch` endpoint, it uses this token, as shown in [internal/app/replication.go](E:/PersonalCode/GoProject/stellmap/internal/app/replication.go:131).

- 中文建议：如果启用跨 region 复制，请确保两端复制通道使用兼容的 token，并且不要与其他用途混用。
- English recommendation: If cross-region replication is enabled, make sure both sides use compatible replication tokens and avoid reusing the token for unrelated purposes.

#### `prometheus_sd_token = "stellmap"`

- 中文作用：保护 `/internal/v1/prometheus/sd` 的 Bearer Token。
- English purpose: Bearer token protecting the `/internal/v1/prometheus/sd` endpoint.

- 中文用法：Prometheus 访问 HTTP SD 时需要带这个 token，接口校验逻辑见 [internal/transport/http/handlers_impl.go](E:/PersonalCode/GoProject/stellmap/internal/transport/http/handlers_impl.go:1204)。
- English usage: Prometheus must send this token when calling the HTTP SD endpoint; the validation logic is implemented in [internal/transport/http/handlers_impl.go](E:/PersonalCode/GoProject/stellmap/internal/transport/http/handlers_impl.go:1204).

- 中文建议：最好不要和 `admin_token`、`replication_token` 共用同一个值，避免权限边界模糊。
- English recommendation: It is better not to reuse the same value as `admin_token` or `replication_token`, so that permission boundaries remain clearer.

### 3.4 `[cluster]`

#### `peer_ids = "1"`

- 中文作用：集群节点 ID 列表，启动时用于构造集群 peer 集合。
- English purpose: Comma-separated node ID list used to construct the cluster peer set at startup.

- 中文用法：格式如 `"1"` 或 `"1,2,3"`。
- English usage: Typical formats are `"1"` for a single node or `"1,2,3"` for a three-node cluster.

- 中文建议：虽然代码会自动把当前节点 ID 补进列表，但配置中仍建议显式写全，便于排查问题。
- English recommendation: Even though the code auto-includes the current node ID when missing, explicitly listing all peers in the config makes troubleshooting easier.

#### `peer_grpc_addrs = "1=127.0.0.1:19090"`

- 中文作用：节点 ID 到 gRPC 地址的映射，用于内部节点通信。
- English purpose: Mapping from node ID to gRPC address for internal peer communication.

- 中文用法：格式必须是 `1=host:port,2=host:port`。
- English usage: The required format is `1=host:port,2=host:port`.

- 中文建议：这里应填写其他节点“真正可访问”的地址，不要在多机部署时继续使用 `127.0.0.1`。
- English recommendation: This field should contain addresses that other nodes can actually reach; do not keep using `127.0.0.1` in multi-host deployments.

#### `peer_http_addrs = "1=127.0.0.1:8080"`

- 中文作用：节点 ID 到 HTTP 地址的映射，用于地址簿、状态返回、Leader 地址展示等。
- English purpose: Mapping from node ID to HTTP address used by the address book, status output, and leader address reporting.

- 中文用法：格式同样是 `1=host:port,2=host:port`。
- English usage: The format is also `1=host:port,2=host:port`.

- 中文建议：如果监听地址是 `0.0.0.0:8080`，这里更应该写业务侧或其他节点能够访问到的真实地址，例如 `10.0.0.11:8080` 或域名。
- English recommendation: If the listen address is `0.0.0.0:8080`, this mapping should usually contain the real reachable address such as `10.0.0.11:8080` or a hostname.

#### `peer_admin_addrs = "1=127.0.0.1:18080"`

- 中文作用：节点 ID 到 admin 地址的映射，主要用于状态展示和成员地址记录。
- English purpose: Mapping from node ID to admin address, mainly used for metadata, status display, and persisted member address records.

- 中文用法：格式为 `1=host:port,2=host:port`。
- English usage: The format is `1=host:port,2=host:port`.

- 中文建议：即使这里配置成远程地址，也不代表远端可以直接访问管理接口，因为 admin 面仍然受本机来源限制。
- English recommendation: Even if this field contains remote-looking addresses, it does not mean the admin API is remotely accessible, because the admin plane is still restricted to localhost-origin requests.

### 3.5 `[runtime]`

#### `request_timeout = "5s"`

- 中文作用：单次请求处理超时，很多 HTTP 控制面逻辑、查询逻辑和后台清理逻辑都会复用它。
- English purpose: Per-request processing timeout reused across many HTTP control-path, query, and cleanup operations.

- 中文用法：使用 Go `time.ParseDuration` 格式，例如 `500ms`、`5s`、`1m`。
- English usage: Uses Go `time.ParseDuration` syntax such as `500ms`, `5s`, or `1m`.

- 中文建议：`5s` 适合大多数默认场景；如果网络较慢或磁盘压力较大，可以适当调高，但不建议无限制放大。
- English recommendation: `5s` is reasonable for most default scenarios; increase it if the network is slow or disk pressure is high, but avoid making it excessively large.

#### `shutdown_timeout = "10s"`

- 中文作用：服务优雅退出的等待超时。
- English purpose: Timeout used for graceful service shutdown.

- 中文用法：服务收到退出信号后，HTTP、admin、gRPC、日志和节点停止流程都会参考这个值。
- English usage: After a shutdown signal is received, HTTP, admin, gRPC, logging, and node stop logic all use this value as part of graceful termination.

- 中文建议：开发环境用 `10s` 基本够用；生产环境如果存在较重的 IO 或较多连接，可考虑增大到 `15s` 到 `30s`。
- English recommendation: `10s` is usually enough in development; production environments with heavier IO or more connections may prefer `15s` to `30s`.

### 3.6 `[registry]`

#### `cleanup_interval = "1s"`

- 中文作用：Leader 后台扫描并清理过期注册实例的周期。
- English purpose: Leader-only background interval for scanning and deleting expired registry instances.

- 中文用法：查询接口本身会跳过已过期实例，而这个配置控制的是“多久真正把过期数据从存储中删掉”。
- English usage: Query APIs already hide expired instances, while this setting controls how often the expired data is physically removed from storage.

- 中文建议：开发环境 `1s` 没问题；生产环境如果数据规模较大，可以考虑 `3s`、`5s` 或更长，降低后台扫描频率。
- English recommendation: `1s` is fine for development; larger production deployments may prefer `3s`, `5s`, or more to reduce background scan frequency.

#### `cleanup_delete_limit = 128`

- 中文作用：每轮清理最多删除多少个过期实例键。
- English purpose: Maximum number of expired registry keys deleted in one cleanup round.

- 中文用法：这是一个节流参数，避免在单轮清理时制造过大的写入或 Raft 压力。
- English usage: This acts as a throttling parameter so one cleanup round does not create too much write or raft pressure.

- 中文建议：如果过期实例堆积较多，可以适当调大到 `256` 或 `512`；如果集群规模较小，默认值通常已经够用。
- English recommendation: If expired instances accumulate significantly, you may raise it to `256` or `512`; for small clusters, the default is usually sufficient.

### 3.7 `[replication]`

#### `targets_file = ""`

- 中文作用：跨 region 目录同步目标配置文件路径。
- English purpose: Path to the cross-region directory replication target configuration file.

- 中文用法：空字符串表示未启用跨 region 复制；非空时应指向一个 JSON 文件。
- English usage: An empty string means cross-region replication is disabled; otherwise it should point to a JSON file.

- 中文建议：如果暂时不需要跨 region 同步，保持为空即可；如果启用，建议使用绝对路径并确保部署时文件一并下发。
- English recommendation: Leave it empty if cross-region synchronization is not needed; if enabled, prefer an absolute path and make sure the file is deployed together with the service.

#### `targets_file` 对应 JSON 结构 / JSON Structure for `targets_file`

- 中文：复制目标文件会在 [internal/app/replication.go](E:/PersonalCode/GoProject/stellmap/internal/app/replication.go:52) 中解析。
- English: The replication target file is parsed in [internal/app/replication.go](E:/PersonalCode/GoProject/stellmap/internal/app/replication.go:52).

- 中文：每个 target 至少需要以下字段：
- English: Each replication target must include at least the following fields:

```json
[
  {
    "sourceRegion": "cn-bj",
    "sourceClusterId": "200",
    "baseURL": "http://10.0.0.21:8080",
    "services": [
      {
        "namespace": "prod",
        "service": "order-service"
      }
    ]
  }
]
```

- 中文：`sourceRegion` 表示远端源 region，`sourceClusterId` 表示远端集群标识，`baseURL` 表示远端 HTTP 基地址，`services` 表示需要复制的服务列表。
- English: `sourceRegion` identifies the remote source region, `sourceClusterId` identifies the remote cluster, `baseURL` is the remote HTTP base address, and `services` lists the namespace/service pairs to replicate.

## 4. 单机与多机建议 / Single-Node and Multi-Node Recommendations

### 单机开发建议 / Single-Node Development

- 中文：当前这份 `stellmapd.toml` 基本就是单机开发模板。
- English: The current `stellmapd.toml` is essentially a single-node development template.

- 中文：`peer_ids = "1"`、`peer_*_addrs` 全部指向本机，适合本地调试。
- English: `peer_ids = "1"` and all `peer_*_addrs` point to the local machine, which is ideal for local debugging.

- 中文：如果你只是本机运行，当前配置可以直接作为起点，但请尽早修改默认 token。
- English: If you only run locally, this config is a good starting point, but you should still change the default tokens early.

### 多节点集群建议 / Multi-Node Cluster

- 中文：多节点时至少需要修改 `node.id`、`server.grpc_addr`、`cluster.peer_ids`、`cluster.peer_grpc_addrs`、`cluster.peer_http_addrs`。
- English: In a multi-node cluster, you should at least adjust `node.id`, `server.grpc_addr`, `cluster.peer_ids`, `cluster.peer_grpc_addrs`, and `cluster.peer_http_addrs`.

- 中文：不要把多机环境里的 peer 地址继续写成 `127.0.0.1`，否则节点之间互相无法连接。
- English: Do not keep peer addresses as `127.0.0.1` in a multi-host environment, or the nodes will not be able to reach each other.

- 中文：`server.http_addr` 和 `server.grpc_addr` 常见做法是监听 `0.0.0.0:<port>`，而 `peer_*_addrs` 写真实可达地址。
- English: A common pattern is to bind `server.http_addr` and `server.grpc_addr` to `0.0.0.0:<port>` while using real reachable addresses in `peer_*_addrs`.

## 5. 安全建议 / Security Recommendations

- 中文：不要在测试、预发、生产环境继续使用 `stellmap` 作为任何 token。
- English: Do not continue using `stellmap` as any token in testing, staging, or production.

- 中文：只对业务侧暴露公共 HTTP 面，不要把内部 gRPC 面和 admin 面直接暴露给公网。
- English: Expose only the public HTTP plane to business users; do not directly expose the internal gRPC plane or admin plane to the public network.

- 中文：`admin_addr` 尽量保持在本机回环地址，通过 SSH 或容器内命令执行管理动作。
- English: Keep `admin_addr` on the localhost loopback whenever possible, and perform admin actions via SSH or from inside the container.

- 中文：为每个节点准备独立持久化目录和足够可靠的磁盘。
- English: Provide each node with its own persistent directory and sufficiently reliable storage.

## 6. 常见启动方式 / Common Startup Command

```bash
./stellmapd --config=./config/stellmapd.toml
```

- 中文：如果需要临时覆盖某个配置项，可以直接在命令行追加参数。
- English: If you need to temporarily override a configuration item, append the corresponding CLI flag.

```bash
./stellmapd --config=./config/stellmapd.toml --http-addr=:28080 --admin-token=new-token
```

## 7. 总结 / Summary

- 中文：这份 `stellmapd.toml` 当前是一个可启动的单机模板，重点是方便本地开发和基础验证。
- English: The current `stellmapd.toml` is a runnable single-node template focused on local development and basic verification.

- 中文：如果要进入测试或生产环境，最优先要处理的是 token 安全、peer 地址真实性、数据目录独立性，以及多节点场景下的 gRPC/HTTP 连通性。
- English: Before moving into testing or production, the top priorities are token security, real reachable peer addresses, isolated data directories, and proper gRPC/HTTP connectivity in multi-node deployments.
