#!/usr/bin/env bash
#
# trae-prompt-runner.sh
#
# 让 bytedance/trae-agent 的 `trae-cli` 满足 harness 的 agent 约定：
#   接收 prompt 文件路径（最后一个参数）→ 输出打到 stdout → 退出码 0 表示成功
#
# trae-cli 本身用 `run -f <file>` 读取任务文件，但必须提供 YAML 配置，
# 所以本脚本在每次运行时生成一份临时配置，model provider/model 由环境变量决定，
# API key 只从环境变量读取（不落盘）。
#
# 可配置环境变量：
#   TRAE_PROVIDER   LLM provider，默认 doubao（可选 anthropic / openai / google / openrouter / ollama 等）
#   TRAE_MODEL      模型名，默认 doubao-seed-1.6
#   TRAE_BASE_URL   自定义 endpoint（如火山方舟 Ark 地址），可选
#   TRAE_MAX_STEPS  最大执行步数，默认 200
#   <PROVIDER>_API_KEY  对应 provider 的 key，例如 DOUBAO_API_KEY / ANTHROPIC_API_KEY
set -euo pipefail

PROMPT_FILE="${1:?用法: trae-prompt-runner.sh <prompt-file-path>}"

# uv tool 安装的 trae-cli 默认位于 ~/.local/bin
export PATH="$HOME/.local/bin:$PATH"
TRAE_CLI="$(command -v trae-cli || true)"
if [[ -z "$TRAE_CLI" ]]; then
  echo "[trae-runner] 找不到 trae-cli，请先安装：" >&2
  echo "  uv tool install --python 3.12 --with docker --with pexpect git+https://github.com/bytedance/trae-agent.git" >&2
  exit 127
fi

PROVIDER="${TRAE_PROVIDER:-doubao}"
MODEL="${TRAE_MODEL:-doubao-seed-1.6}"
MAX_STEPS="${TRAE_MAX_STEPS:-200}"

KEY_VAR="$(printf '%s' "$PROVIDER" | tr '[:lower:]' '[:upper:]' | tr -c 'A-Z0-9_' '_')_API_KEY"
if [[ -z "${!KEY_VAR:-}" ]]; then
  echo "[trae-runner] 缺少 ${KEY_VAR}（TRAE_PROVIDER=${PROVIDER}）。请先 export ${KEY_VAR}=... 再启动 harness。" >&2
  exit 2
fi

TMP_CFG="$(mktemp -t trae-harness-config.XXXXXX).yaml"
trap 'rm -f "$TMP_CFG"' EXIT

# trae-cli 要求 YAML 至少包含 model_providers / models / agents；
# api_key 故意留空 —— trae-cli 会按 CLI > 环境变量 > 配置文件 的优先级从 $KEY_VAR 读取。
cat >"$TMP_CFG" <<YAML
model_providers:
  ${PROVIDER}:
    api_key: ""
    provider: ${PROVIDER}
models:
  harness_model:
    model_provider: ${PROVIDER}
    model: "${MODEL}"
    max_tokens: 4096
    temperature: 0.5
    top_p: 1
    top_k: 0
    max_retries: 10
    parallel_tool_calls: true
agents:
  trae_agent:
    enable_lakeview: false
    model: harness_model
    max_steps: ${MAX_STEPS}
    tools:
      - bash
      - str_replace_based_edit_tool
      - sequentialthinking
      - task_done
YAML

ARGS=(run --config-file "$TMP_CFG" -f "$PROMPT_FILE" -w "$PWD" -ct simple)
if [[ -n "${TRAE_BASE_URL:-}" ]]; then
  ARGS+=(--model-base-url "$TRAE_BASE_URL")
fi

echo "[trae-runner] provider=${PROVIDER} model=${MODEL} cwd=${PWD} prompt=${PROMPT_FILE}" >&2
exec "$TRAE_CLI" "${ARGS[@]}"
