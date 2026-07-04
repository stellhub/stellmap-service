# GitHub Actions 说明

当前仓库的发布工作流是：

- [release-stellmapd.yml](../release/release-stellmapd.yml)

## 1. 工作流会做什么

工作流会在 `ubuntu-latest` 上执行：

1. 运行 `go test ./...`
2. 构建 `stellmapd`
3. 构建 `stellmapctl`
4. 复制 [config/stellmapd.toml](../../config/stellmapd.toml)
5. 复制 [deploy/start.sh](../../deploy/start.sh)
6. 通过 SSH 把这 4 个文件上传到一台或多台 CVM 的 `/data` 目录

上传后的目录结构如下：

- `/data/stellmapd.toml`
- `/data/stellmapd`
- `/data/stellmapctl`
- `/data/start.sh`

## 2. 需要配置什么

在仓库的 `Settings -> Secrets and variables -> Actions` 中配置：

- `VM_HOSTS`
- `VM_USER`
- `VM_PORT`
- `VM_SSH_KEY` 或 `VM_SSH_PASSWORD`

说明：

- `VM_HOSTS` 支持多台机器，使用分号分隔，例如 `10.0.0.11;10.0.0.12`
- `VM_SSH_KEY` 和 `VM_SSH_PASSWORD` 二选一即可，工作流优先使用私钥
- `VM_PORT` 默认可设为 `22`

可选配置：

- `REMOTE_POST_UPLOAD_CMD`

它会在每台机器上传完成后继续执行，例如：

```text
sudo bash /data/start.sh
```

## 3. 推荐触发方式

当前支持：

- 手工触发 `workflow_dispatch`
- 推送 `v*` tag 自动触发

如果你想先验证部署链路，建议直接手工触发一次，并在输入框里传入：

- `vm_hosts`
- `remote_post_upload_cmd`

例如：

```text
vm_hosts=10.0.0.11;10.0.0.12
remote_post_upload_cmd=sudo bash /data/start.sh
```
