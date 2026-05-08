#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  bash deploy/build.sh

Or override build options:
  bash deploy/build.sh \
    --output-dir ./dist/release \
    --goos linux \
    --goarch amd64

Options:
  --output-dir PATH   构建输出目录，默认 {repo-root}/dist/release
  --goos NAME         目标操作系统，默认 linux
  --goarch NAME       目标架构，默认 amd64
  --go-bin PATH       go 命令路径，默认 go
  --config-file PATH  打包使用的配置文件，默认 {repo-root}/config/stellmapd.toml
  -h, --help          查看帮助

Behavior:
  1. 基于当前源码构建 stellmapd 和 stellmapctl。
  2. 复制 start.sh 与 stellmapd.toml 到输出目录。
  3. 输出目录可直接作为 start.sh 的 source-dir 使用。
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

log_step() {
  echo "==> $*"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${REPO_ROOT}/release"
GOOS="linux"
GOARCH="amd64"
GO_BIN="go"
CONFIG_FILE="${REPO_ROOT}/config/stellmapd.toml"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --goos)
      GOOS="$2"
      shift 2
      ;;
    --goarch)
      GOARCH="$2"
      shift 2
      ;;
    --go-bin)
      GO_BIN="$2"
      shift 2
      ;;
    --config-file)
      CONFIG_FILE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

require_cmd "${GO_BIN}"
require_cmd cp
require_cmd chmod
require_cmd mkdir

[[ -f "${REPO_ROOT}/go.mod" ]] || die "go.mod not found under repo root: ${REPO_ROOT}"
[[ -f "${CONFIG_FILE}" ]] || die "config file not found: ${CONFIG_FILE}"
[[ -f "${SCRIPT_DIR}/start.sh" ]] || die "start.sh not found: ${SCRIPT_DIR}/start.sh"

mkdir -p "${OUTPUT_DIR}"

log_step "building stellmapd for ${GOOS}/${GOARCH}"
(
  cd "${REPO_ROOT}"
  CGO_ENABLED=0 GOOS="${GOOS}" GOARCH="${GOARCH}" \
    "${GO_BIN}" build -trimpath -ldflags="-s -w" \
    -o "${OUTPUT_DIR}/stellmapd" \
    ./cmd/stellmapd
)

log_step "building stellmapctl for ${GOOS}/${GOARCH}"
(
  cd "${REPO_ROOT}"
  CGO_ENABLED=0 GOOS="${GOOS}" GOARCH="${GOARCH}" \
    "${GO_BIN}" build -trimpath -ldflags="-s -w" \
    -o "${OUTPUT_DIR}/stellmapctl" \
    ./cmd/stellmapctl
)

log_step "copying runtime files"
cp "${CONFIG_FILE}" "${OUTPUT_DIR}/stellmapd.toml"
cp "${SCRIPT_DIR}/start.sh" "${OUTPUT_DIR}/start.sh"
chmod +x "${OUTPUT_DIR}/stellmapd" "${OUTPUT_DIR}/stellmapctl" "${OUTPUT_DIR}/start.sh"

echo
echo "Build output : ${OUTPUT_DIR}"
echo "Binary       : ${OUTPUT_DIR}/stellmapd"
echo "Ctl          : ${OUTPUT_DIR}/stellmapctl"
echo "Config       : ${OUTPUT_DIR}/stellmapd.toml"
echo "Start script : ${OUTPUT_DIR}/start.sh"
echo
echo "Run example:"
echo "  cd ${OUTPUT_DIR}"
echo "  sudo bash ./start.sh"
