# StellMap 部署说明

当前仓库已经切换为“GitHub Actions 直推 CVM + 本地 `/data/start.sh` 启动”的部署方式。

如果你希望使用 Docker / Docker Compose 部署，请参考：

- [Docker 集群部署指南](./docker-cluster-deploy.md)

## 1. 目标机最终文件

每次发布后，目标 CVM 的 `/data` 目录下会有 4 个文件：

- `/data/stellmapd.toml`
- `/data/stellmapd`
- `/data/stellmapctl`
- `/data/start.sh`

其中：

- `stellmapd.toml` 是运行时读取的配置文件
- `stellmapd` 是服务端二进制
- `stellmapctl` 是控制面 CLI
- `start.sh` 是本地安装并启动 systemd 服务的脚本

## 2. 最简单的启动方式

在目标机器上执行：

```bash
sudo bash /data/start.sh
```

默认行为：

1. 从 `/data` 读取 `stellmapd`、`stellmapctl`、`stellmapd.toml`
2. 安装到 `/opt/stellmap/bin`
3. 安装配置到 `/etc/stellmapd/stellmapd.toml`
4. 生成 `systemd` 服务 `stellmapd`
5. 启动并做健康检查

## 3. 本地构建 + start.sh 启动

如果你希望先在当前源码目录本地构建发布包，再把产物目录直接交给 `start.sh` 安装启动，可以执行：

```bash
bash deploy/build.sh
```

默认会输出到：

- `dist/release/stellmapd`
- `dist/release/stellmapctl`
- `dist/release/stellmapd.toml`
- `dist/release/start.sh`

这 4 个文件的目录结构与目标机 `/data` 目录约定一致，因此可以直接进入输出目录执行：

```bash
cd dist/release
sudo bash ./start.sh
```

如果你需要覆盖构建参数，例如指定输出目录或目标平台，可以执行：

```bash
bash deploy/build.sh \
  --output-dir ./dist/release \
  --goos linux \
  --goarch amd64
```

## 4. 默认配置文件

默认配置文件是：

- [config/stellmapd.toml](../../config/stellmapd.toml)

当前仓库里的这份默认配置已经改成“可直接启动的单机配置”，默认值包括：

- `node.id = 1`
- `cluster_id = 100`
- `http_addr = "0.0.0.0:8080"`
- `admin_addr = "127.0.0.1:18080"`
- `grpc_addr = "0.0.0.0:19090"`
- 单节点 `peer_*` 地址簿

如果你要部署多节点集群，需要在发布前或发布后按节点修改 `/data/stellmapd.toml` 中的：

- `node.id`
- `server.http_addr`
- `server.grpc_addr`
- `cluster.peer_ids`
- `cluster.peer_http_addrs`
- `cluster.peer_grpc_addrs`
- `cluster.peer_admin_addrs`

## 5. GitHub Actions 直推方式

工作流文件：

- [release-stellmapd.yml](../release/release-stellmapd.yml)

工作流会：

1. 构建 `stellmapd`
2. 构建 `stellmapctl`
3. 复制 `config/stellmapd.toml`
4. 复制 `deploy/start.sh`
5. 通过 SSH 上传到一台或多台 CVM 的 `/data`
6. 可选执行远程命令，例如 `sudo bash /data/start.sh`

## 6. 需要配置的 Actions Secrets / Variables

- `VM_HOSTS`
- `VM_USER`
- `VM_PORT`
- `VM_SSH_KEY` 或 `VM_SSH_PASSWORD`

多台机器使用分号分隔，例如：

```text
10.0.0.11;10.0.0.12;10.0.0.13
```

可选：

- `REMOTE_POST_UPLOAD_CMD`

推荐值：

```text
sudo bash /data/start.sh
```

## 7. 常用检查命令

查看服务状态：

```bash
sudo systemctl status stellmapd
```

查看日志：

```bash
sudo journalctl -u stellmapd -f
```

查看健康检查：

```bash
curl http://127.0.0.1:8080/readyz
```

查看控制面状态：

```bash
curl \
  -H "Authorization: Bearer stellmap" \
  http://127.0.0.1:18080/admin/v1/status
```
