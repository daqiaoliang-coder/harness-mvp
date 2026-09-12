#!/usr/bin/env bash
#
# 以「真实 GitHub 仓库 + 本地 trae-cli agent」启动 harness（gateway + loop-node）。
#
# 启动前请在当前 shell 中 export 两个密钥（都不会落盘）：
#   export GITHUB_TOKEN=ghp_xxx        # daqiaoliang-coder/agent-runtime 的 Issues 读写权限
#   export DOUBAO_API_KEY=...          # 或 ANTHROPIC_API_KEY / OPENAI_API_KEY（与 TRAE_PROVIDER 对应）
#
# 可选模型覆盖：
#   export TRAE_PROVIDER=doubao        # 默认 doubao
#   export TRAE_MODEL=doubao-seed-1.6  # 默认 doubao-seed-1.6
#   export TRAE_BASE_URL=https://...   # 自定义 endpoint（如火山方舟）
#
# 然后：./scripts/dev-real.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# 加载非密钥配置（GITHUB_OWNER/REPO、AGENTS 等）
set -a
# shellcheck disable=SC1091
. "$REPO_ROOT/.env"
set +a

if [[ -z "${GITHUB_TOKEN:-}" ]]; then
  echo "[dev-real] 请先 export GITHUB_TOKEN=<PAT，需 agent-runtime 仓库 Issues 读写权限>" >&2
  exit 2
fi

# uv tool 安装的 trae-cli 位于 ~/.local/bin
export PATH="$HOME/.local/bin:$PATH"
if ! command -v trae-cli >/dev/null; then
  echo "[dev-real] 未找到 trae-cli，请先安装：" >&2
  echo "  uv tool install --python 3.12 --with docker --with pexpect git+https://github.com/bytedance/trae-agent.git" >&2
  exit 2
fi

# harness 会把 prompt 文件路径作为最后一个参数追加给 AGENT_CMD
export AGENT_CMD="$REPO_ROOT/scripts/trae-prompt-runner.sh"

echo "[dev-real] github=${GITHUB_OWNER}/${GITHUB_REPO} (${GITHUB_MODE})  agent=trae-cli (${TRAE_PROVIDER:-doubao}/${TRAE_MODEL:-doubao-seed-1.6})"
exec npm run dev
