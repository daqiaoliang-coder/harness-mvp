#!/usr/bin/env bash
#
# 以「真实 GitHub 仓库 + 本地 trae-cli agent」启动 harness（gateway + loop-node）。
#
# 密钥解析优先级：shell 环境变量 > macOS 钥匙串 > .env 文件
#
# 推荐把密钥存入 macOS 钥匙串（只需存一次；-w 不带值会隐藏输入，
# 不进 shell 历史、不落盘到项目目录）：
#   security add-generic-password -a "$USER" -s harness-github-token -w
#   security add-generic-password -a "$USER" -s harness-doubao-api-key -w
# 换 provider 时把 doubao 换成对应名字：anthropic / openai / google / openrouter 等。
# 首次读取时 macOS 会弹一次授权框，点「始终允许」后不再提示。
#
# 临时覆盖单次运行，仍可直接 export：
#   export GITHUB_TOKEN=ghp_xxx
#   export DOUBAO_API_KEY=...
#
# 可选模型配置：
#   export TRAE_PROVIDER=doubao        # 默认 doubao
#   export TRAE_MODEL=doubao-seed-1.6  # 默认 doubao-seed-1.6
#   export TRAE_BASE_URL=https://...   # 自定义 endpoint（如火山方舟）
#
# 然后：./scripts/dev-real.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# 原生模块（better-sqlite3）按 Node 20 ABI 编译，统一使用 nvm 的 Node 20
if [[ -s "${NVM_DIR:-$HOME/.nvm}/nvm.sh" ]]; then
  # shellcheck disable=SC1091
  . "${NVM_DIR:-$HOME/.nvm}/nvm.sh"
  nvm use --silent 20
fi

# 读取 .env 中某个 key 的值（密钥不直接从 .env 导出，统一走 resolve_secret）
env_file_value() {
  local want="$1" line
  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" == "$want="* ]]; then
      printf '%s' "${line#*=}"
      return 0
    fi
  done <"$REPO_ROOT/.env"
}

# 非密钥配置：shell 环境变量优先，其次 .env
while IFS= read -r line || [[ -n "$line" ]]; do
  [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
  key="${line%%=*}"
  case "$key" in
    GITHUB_TOKEN | *_API_KEY) continue ;;
  esac
  if [[ -z "${!key+x}" ]]; then
    export "${line?}"
  fi
done <"$REPO_ROOT/.env"

# 从 macOS 钥匙串读取一个 generic password（不存在则静默返回空）
keychain_read() {
  security find-generic-password -a "$USER" -s "$1" -w 2>/dev/null || true
}

# 密钥三级解析：shell 环境变量 > 钥匙串 > .env
# 用法: resolve_secret <变量名> <钥匙串 service 名>
resolve_secret() {
  local key="$1" service="$2" v
  if [[ -n "${!key:-}" ]]; then
    return 0
  fi
  v="$(keychain_read "$service")"
  if [[ -z "$v" ]]; then
    v="$(env_file_value "$key")"
  fi
  if [[ -n "$v" ]]; then
    export "$key=$v"
  fi
  return 0
}

provider_slug() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9' '-'
}

resolve_secret GITHUB_TOKEN harness-github-token

TRAE_PROVIDER_RESOLVED="${TRAE_PROVIDER:-$(env_file_value TRAE_PROVIDER)}"
TRAE_PROVIDER_RESOLVED="${TRAE_PROVIDER_RESOLVED:-doubao}"
MODEL_KEY_VAR="$(printf '%s' "$TRAE_PROVIDER_RESOLVED" | tr '[:lower:]' '[:upper:]' | tr -c 'A-Z0-9_' '_')_API_KEY"
resolve_secret "$MODEL_KEY_VAR" "harness-$(provider_slug "$TRAE_PROVIDER_RESOLVED")-api-key"

if [[ -z "${GITHUB_TOKEN:-}" ]]; then
  cat >&2 <<'EOF'
[dev-real] 缺少 GITHUB_TOKEN（需 daqiaoliang-coder/agent-runtime 的 Issues 读写权限）。
  推荐存入钥匙串（只需一次，输入内容不回显）：
    security add-generic-password -a "$USER" -s harness-github-token -w
  或本次临时注入：
    export GITHUB_TOKEN=ghp_xxx
EOF
  exit 2
fi

if [[ -z "${!MODEL_KEY_VAR:-}" ]]; then
  cat >&2 <<EOF
[dev-real] 缺少 ${MODEL_KEY_VAR}（TRAE_PROVIDER=${TRAE_PROVIDER_RESOLVED}）。
  推荐存入钥匙串（只需一次，输入内容不回显）：
    security add-generic-password -a "\$USER" -s harness-$(provider_slug "$TRAE_PROVIDER_RESOLVED")-api-key -w
  或本次临时注入：
    export ${MODEL_KEY_VAR}=...
EOF
  exit 2
fi

# uv tool 安装的 trae-cli 位于 ~/.local/bin
export PATH="$HOME/.local/bin:$PATH"
if ! command -v trae-cli >/dev/null; then
  echo "[dev-real] 未找到 trae-cli，请先安装：" >&2
  echo "  uv tool install --python 3.12 --with docker --with pexpect git+https://github.com/bytedance/trae-agent.git" >&2
  exit 2
fi

# 某些环境下 npm 解包会丢失 node-pty spawn-helper 的可执行位（表现为 posix_spawnp failed）
case "$(uname -m)" in
  x86_64) pty_arch=x64 ;;
  arm64) pty_arch=arm64 ;;
esac
pty_helper="$REPO_ROOT/node_modules/node-pty/prebuilds/$(uname -s | tr '[:upper:]' '[:lower:]')-${pty_arch}/spawn-helper"
[[ -f "$pty_helper" && ! -x "$pty_helper" ]] && chmod +x "$pty_helper"

# 启动前清理残留的本项目 dev 栈（concurrently → gateway/loop-node 整棵树）。
# 仅自动清理命令行属于本仓库的监听者；若端口被无关程序占用则拒绝启动，避免误杀。
HTTP_PORT_RESOLVED="${HTTP_PORT:-8787}"
listener_pids="$(lsof -nP -tiTCP:"$HTTP_PORT_RESOLVED" -sTCP:LISTEN 2>/dev/null || true)"
if [[ -n "$listener_pids" ]]; then
  # 仅认定「命令行包含本仓库路径」且属于 node/npm/tsx/concurrently 工具链的进程，
  # 避免把恰好引用过仓库路径的交互式 shell 误判为栈成员。
  is_stack_process() {
    case "$1" in
      *"$REPO_ROOT"*)
        case "$1" in
          *node* | *npm* | *concurrently* | *tsx*) return 0 ;;
          *) return 1 ;;
        esac ;;
      *) return 1 ;;
    esac
  }

  stack_roots=()
  blocked=""
  for lpid in $listener_pids; do
    lcmd="$(ps -o command= -p "$lpid" 2>/dev/null || true)"
    if ! is_stack_process "$lcmd"; then
      blocked="$lpid"
      break
    fi
    # 沿父进程上溯到仍属于本项目工具链的最高祖先（通常是 concurrently/npm）
    root="$lpid"
    while :; do
      ppid="$(ps -o ppid= -p "$root" 2>/dev/null | tr -d ' ' || true)"
      [[ -z "$ppid" || "$ppid" == "0" || "$ppid" == "1" ]] && break
      pcmd="$(ps -o command= -p "$ppid" 2>/dev/null || true)"
      is_stack_process "$pcmd" || break
      root="$ppid"
    done
    stack_roots+=("$root")
  done

  if [[ -n "$blocked" ]]; then
    echo "[dev-real] 端口 ${HTTP_PORT_RESOLVED} 被非本项目进程占用（PID ${blocked}），拒绝自动清理：" >&2
    ps -o pid,command -p "$blocked" >&2 || true
    echo "  请先结束该进程，或用 HTTP_PORT=<其他端口> $0 启动。" >&2
    exit 2
  fi

  # 收集每棵子树的全部 PID（去重、先杀子进程）
  collect_pids() { echo "$1"; local c; for c in $(pgrep -P "$1" 2>/dev/null || true); do collect_pids "$c"; done; }
  all_pids=""
  for r in "${stack_roots[@]}"; do all_pids="$all_pids $(collect_pids "$r")"; done
  all_pids="$(printf '%s\n' $all_pids | sort -ru | tr '\n' ' ')"
  echo "[dev-real] 发现旧的 harness dev 栈仍在运行（PID:${all_pids% }），先发送 TERM 清理…"
  # shellcheck disable=SC2086
  kill -TERM $all_pids 2>/dev/null || true
  sleep 2
  left=""
  for p in $all_pids; do kill -0 "$p" 2>/dev/null && left="$left $p"; done
  if [[ -n "$left" ]]; then
    echo "[dev-real] 部分进程未退出，发送 KILL:${left}"
    # shellcheck disable=SC2086
    kill -KILL $left 2>/dev/null || true
    sleep 1
  fi
fi

# harness 会把 prompt 文件路径作为最后一个参数追加给 AGENT_CMD
export AGENT_CMD="$REPO_ROOT/scripts/trae-prompt-runner.sh"

echo "[dev-real] github=${GITHUB_OWNER}/${GITHUB_REPO} (${GITHUB_MODE})  agent=trae-cli (${TRAE_PROVIDER_RESOLVED}/${TRAE_MODEL:-doubao-seed-1.6})"
exec npm run dev
