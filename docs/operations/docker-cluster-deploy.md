# StellMap Docker 集群部署指南

本文档面向当前仓库里的 `stellmapd` 实现，说明如何基于 Docker 方式启动一个 3 节点 `StellMap` 集群。

## 1. 当前项目对 Docker 部署的适配结论

结合当前代码实现，容器化部署时需要注意下面几个事实：

1. `stellmapd` 同时监听公共 `HTTP`、独立 `admin HTTP`、内部 `gRPC` 三个端口。
2. `server.http_addr` 和 `server.grpc_addr` 在容器里需要监听 `0.0.0.0`，否则宿主机映射端口后无法访问。
3. `admin` listener 除了要有 `Authorization: Bearer <token>`，还强制要求请求来源必须是 `127.0.0.1`。
4. 这意味着 Docker 场景下的集群管理动作不能直接从宿主机访问容器的 `admin` 端口，而应该通过 `docker exec` 进入容器内部执行 `stellmapctl`。
5. 当前实现把“监听地址”和“节点对外公布地址”分开配置的能力还没有拆出来，所以 Docker 示例里通过：
   - `server.*_addr` 负责本容器监听
   - `cluster.peer_*_addrs` 负责节点之间互相发现

基于上面的约束，当前仓库已经补充了：

- 根目录 `Dockerfile`
- `deploy/docker-compose.cluster.yml`
- `deploy/docker/stellmapd-node-1.toml`
- `deploy/docker/stellmapd-node-2.toml`
- `deploy/docker/stellmapd-node-3.toml`

## 2. 目录说明

相关文件如下：

- [Dockerfile](../../Dockerfile)
- [deploy/docker-compose.cluster.yml](../../deploy/docker-compose.cluster.yml)
- [deploy/docker/stellmapd-node-1.toml](../../deploy/docker/stellmapd-node-1.toml)
- [deploy/docker/stellmapd-node-2.toml](../../deploy/docker/stellmapd-node-2.toml)
- [deploy/docker/stellmapd-node-3.toml](../../deploy/docker/stellmapd-node-3.toml)

## 3. 启动前准备

要求：

1. 已安装 Docker
2. 已安装 Docker Compose Plugin
3. 当前工作目录位于仓库根目录

先确认 Docker 可用：

```bash
docker version
docker compose version
```

## 4. 构建并启动 3 节点集群

在仓库根目录执行：

```bash
docker compose -f deploy/docker-compose.cluster.yml up -d --build
```

启动后会创建三个容器：

- `stellmap-node1`
- `stellmap-node2`
- `stellmap-node3`

其中宿主机暴露的公共 HTTP 端口为：

- `127.0.0.1:8081 -> stellmap-node1:8080`
- `127.0.0.1:8082 -> stellmap-node2:8080`
- `127.0.0.1:8083 -> stellmap-node3:8080`

数据目录使用 Docker Volume 持久化：

- `node1-data`
- `node2-data`
- `node3-data`

## 5. 健康检查与集群状态确认

### 5.1 检查容器状态

```bash
docker compose -f deploy/docker-compose.cluster.yml ps
```

### 5.2 检查对外 HTTP readiness

```bash
curl http://127.0.0.1:8081/readyz
curl http://127.0.0.1:8082/readyz
curl http://127.0.0.1:8083/readyz
```

### 5.3 在容器内部查询集群状态

由于 `admin` listener 只接受来自 `127.0.0.1` 的请求，因此需要进入容器内部执行：

```bash
docker exec -e STELLMAP_ADMIN_TOKEN=stellmap stellmap-node1 \
  stellmapctl status --server=http://127.0.0.1:18080
```

如果想查看另外两个节点的视角：

```bash
docker exec -e STELLMAP_ADMIN_TOKEN=stellmap stellmap-node2 \
  stellmapctl status --server=http://127.0.0.1:18080

docker exec -e STELLMAP_ADMIN_TOKEN=stellmap stellmap-node3 \
  stellmapctl status --server=http://127.0.0.1:18080
```

## 6. 基本读写验证

### 6.1 注册实例

```bash
curl -X POST http://127.0.0.1:8081/api/v1/register \
  -H "Content-Type: application/json" \
  -d '{
    "namespace":"default",
    "service":"demo.order.api",
    "instanceId":"order-1",
    "organization":"demo",
    "businessDomain":"trade",
    "capabilityDomain":"order",
    "application":"order",
    "role":"api",
    "zone":"zone-a",
    "leaseTtlSeconds":15,
    "endpoints":[
      {
        "name":"http",
        "protocol":"http",
        "host":"order-service",
        "port":8088,
        "path":"/"
      }
    ],
    "labels":{
      "env":"dev"
    },
    "metadata":{
      "source":"docker-compose"
    }
  }'
```

### 6.2 查询实例

```bash
curl "http://127.0.0.1:8081/api/v1/instances?namespace=default&service=demo.order.api"
```

### 6.3 续约

```bash
curl -X POST http://127.0.0.1:8081/api/v1/heartbeat \
  -H "Content-Type: application/json" \
  -d '{
    "namespace":"default",
    "service":"demo.order.api",
    "instanceId":"order-1",
    "organization":"demo",
    "businessDomain":"trade",
    "capabilityDomain":"order",
    "application":"order",
    "role":"api",
    "leaseTtlSeconds":15
  }'
```

## 7. 查看日志

```bash
docker logs -f stellmap-node1
docker logs -f stellmap-node2
docker logs -f stellmap-node3
```

## 8. 停止与清理

停止集群但保留数据：

```bash
docker compose -f deploy/docker-compose.cluster.yml down
```

停止并删除数据卷：

```bash
docker compose -f deploy/docker-compose.cluster.yml down -v
```

## 9. 生产部署建议

如果你要把这套方案从本地 `docker compose` 扩展到正式环境，建议至少补齐下面几点：

1. 把默认 token 改成强随机值，不要继续使用 `stellmap`。
2. 使用独立镜像仓库，例如 Harbor、Docker Hub 私有仓库或云厂商镜像仓库。
3. 为每个节点使用独立持久卷，避免数据目录被多个节点共享。
4. 节点之间至少放通：
   - 公共 `HTTP` 端口
   - 内部 `gRPC` 端口
5. `admin` 端口建议继续只允许容器内本机访问，不要直接暴露公网。
6. 在反向代理或接入层前面只暴露公共 `HTTP` 入口，不要把内部 `gRPC` 直接开放给业务侧。
7. 为容器增加资源限制、日志采集和重启策略。
8. 结合宿主机磁盘性能评估 `WAL`、`Pebble` 和 `snapshot` 的存储容量。

## 10. 多主机 Docker 集群的配置原则

如果不是单机 `docker compose`，而是多台宿主机分别运行容器，配置原则如下：

1. `server.http_addr` 仍然监听 `0.0.0.0:<port>`
2. `server.grpc_addr` 仍然监听 `0.0.0.0:<port>`
3. `server.admin_addr` 建议仍然监听 `127.0.0.1:<port>`
4. `cluster.peer_http_addrs` 填每个节点真实可达的容器地址或宿主机地址
5. `cluster.peer_grpc_addrs` 填每个节点真实可达的容器地址或宿主机地址
6. `cluster.peer_admin_addrs` 主要用于状态展示和错误返回提示，不能代替“本机 admin 运维”这条约束

如果未来要进一步优化 Docker / Kubernetes 体验，建议后续演进出“监听地址”和“广播地址”分离配置，这样对外返回的 `leaderAddr` 会更准确。
