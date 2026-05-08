## Prometheus 与 Grafana 最小接入示例

### 1 Prometheus 通过 StellMap 做 HTTP SD

`StellMap` 已经提供：

- `GET /internal/v1/prometheus/sd`

Prometheus 只需要改配置，不需要改源码。

最小 Prometheus 配置示例：

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: stellmap-services
    http_sd_configs:
      - url: http://10.0.0.11:8080/internal/v1/prometheus/sd?endpoint=metrics
        refresh_interval: 30s
        authorization:
          type: Bearer
          credentials: stellmap
```

如果你希望同时抓取 StellMap 自身节点指标，可以使用：

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: stellmap-all
    http_sd_configs:
      - url: http://10.0.0.11:8080/internal/v1/prometheus/sd?endpoint=metrics&includeSelf=true
        refresh_interval: 30s
        authorization:
          type: Bearer
          credentials: stellmap
```

### 2 docker-compose 最小示例

如果你想快速在测试机上拉一套 `Prometheus + Grafana`，可以先用下面这个最小 `docker-compose.yml`：

```yaml
version: "3.8"

services:
  prometheus:
    image: prom/prometheus:v2.54.1
    container_name: prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro

  grafana:
    image: grafana/grafana:11.1.0
    container_name: grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=admin
```

对应的 `prometheus.yml` 最小示例：

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: stellmap-all
    http_sd_configs:
      - url: http://10.0.0.11:8080/internal/v1/prometheus/sd?endpoint=metrics&includeSelf=true
        refresh_interval: 30s
        authorization:
          type: Bearer
          credentials: stellmap
```

Grafana 接入步骤：

1. 打开 `http://<grafana-ip>:3000`
2. 使用 `admin / admin` 登录
3. 添加数据源，类型选 `Prometheus`
4. URL 填写 `http://prometheus:9090`
5. 保存后即可按 `service`、`namespace`、`target_kind` 等标签建图

### 3 Grafana 面板与新增指标说明

当前 `StellMap` 暴露的指标可以分成 4 组：

- 基础运行时指标：`go_*`、`process_*`
- 传输层指标：`stellmap_http_server_*`、`stellmap_grpc_server_*`
- 复制状态指标：`stellmap_replication_*`
- 注册中心画像与治理指标：`stellmap_registry_*`

#### 3.1 新增的注册中心画像与治理指标

本次新增了以下指标：

- `stellmap_registry_active_instances`
  - 当前仍处于有效租约内的实例数
  - 标签：
    - `namespace`
    - `service`
    - `organization`
    - `business_domain`
    - `capability_domain`
    - `application`
    - `role`
    - `zone`
- `stellmap_registry_register_requests_total`
  - 注册请求总量
  - 标签：
    - `namespace`
    - `service`
    - `organization`
    - `business_domain`
    - `capability_domain`
    - `application`
    - `role`
    - `zone`
    - `code`
- `stellmap_registry_heartbeat_requests_total`
  - 心跳请求总量
  - 标签同上
- `stellmap_registry_deregister_requests_total`
  - 注销请求总量
  - 标签同上
- `stellmap_registry_watch_sessions`
  - 当前处于活跃状态的 watch 会话数
  - 标签：
    - `watch_kind`
      - `instances`：业务客户端对注册中心实例目录的 watch
      - `replication`：内部跨 region 复制 watch
    - `caller_namespace`
    - `caller_service`
    - `caller_organization`
    - `caller_business_domain`
    - `caller_capability_domain`
    - `caller_application`
    - `caller_role`
    - `target_namespace`
    - `target_service`
    - `target_scope`
      - `service`
      - `service_set`
      - `service_prefix`
      - `namespace`

其中：

- `active_instances` 不是走请求热路径，而是在 `Prometheus scrape` 时扫描本地注册中心目录并聚合，因此不会放大注册/心跳写路径开销
- `watch_sessions` 是会话级 `gauge`，只在连接建立和断开时增减，不会在每条 watch event 上打点

#### 3.2 调用方身份透传约定

如果你希望做“谁 watch 了最多的下游服务”这类治理面板，业务客户端在调用：

- `GET /api/v1/registry/watch`

时应尽量透传调用方身份。

支持两种方式：

1. URL Query 参数

```text
callerNamespace
callerService
callerOrganization
callerBusinessDomain
callerCapabilityDomain
callerApplication
callerRole
```

2. HTTP Header

```text
X-StellMap-Caller-Namespace
X-StellMap-Caller-Service
X-StellMap-Caller-Organization
X-StellMap-Caller-Business-Domain
X-StellMap-Caller-Capability-Domain
X-StellMap-Caller-Application
X-StellMap-Caller-Role
```

兼容的简化 header 也支持：

```text
X-Caller-Namespace
X-Caller-Service
X-Caller-Organization
X-Caller-Business-Domain
X-Caller-Capability-Domain
X-Caller-Application
X-Caller-Role
```

如果调用方没有透传这些字段，对应 watch 指标会落到 `unknown`。

#### 3.3 推荐 Grafana 面板

下面给出一套可以直接使用的面板 PromQL。

##### A. 总览面板

+ 实例存活

```shell
max(up{job="prometheus"})
```

+ CPU使用率

```shell
rate(process_cpu_seconds_total[5m]) * 100
```

+ GC次数

```shell
rate(go_gc_duration_seconds_count[5m])
```

+ TopN

```shell
topk(
  10,
  sum by (service) (
    increase(stellmap_registry_register_requests_total[1h])
  )
)
```

##### B. Http

+ P99

```shell
histogram_quantile(
  0.99,
  sum by (le) (
    rate(stellmap_http_server_request_duration_seconds_bucket{route!="/api/v1/registry/watch"}[5m])
  )
)
```

+ QPS【route】

```shell
sum by (route) (
  rate(stellmap_http_server_requests_total[5m])
)
```

+ QPS【method】

```shell
sum by (route, method, code) (
  rate(stellmap_http_server_requests_total[5m])
)
```

##### Regisger

+ Register【service】

```shell
sum by (service) (
  rate(stellmap_registry_register_requests_total[5m])
)
```

+ Register【QPS】

```shell
sum(rate(stellmap_registry_register_requests_total[5m]))
```

##### Deregister

+ Deregister【QPS】

```shell
sum(rate(stellmap_registry_deregister_requests_total[5m]))
```

+ Deregister【service】

```shell
sum by (service) (
  rate(stellmap_registry_deregister_requests_total[5m])
)
```