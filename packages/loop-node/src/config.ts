import path from 'node:path';
import { fileURLToPath } from 'node:url';

export interface LoopNodeConfig {
  gatewayUrl: string;
  token: string;
  nodeId: string;
  agents: string[];
  templatesDir: string;
  workspaceDir: string;
  agentCmd: string;
  agentArgs: string[];
  /** 单个 run 的总时长上限（ms），0 表示禁用 */
  runTimeoutMs: number;
  /** agent 连续无输出的判定时长（ms），用于识别卡死，0 表示禁用 */
  idleTimeoutMs: number;
}

const here = path.dirname(fileURLToPath(import.meta.url));
const pkgRoot = path.resolve(here, '..');
const repoRoot = path.resolve(here, '../../..');

function numberEnv(name: string, defaultValue: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return defaultValue;
  const n = Number(raw);
  return Number.isFinite(n) && n >= 0 ? n : defaultValue;
}

export function loadConfig(): LoopNodeConfig {
  const defaultMockAgent = path.resolve(repoRoot, 'scripts/mock-agent.mjs');
  const agentCmd = process.env.AGENT_CMD ?? process.execPath;
  const agentArgsRaw = process.env.AGENTS ?? defaultMockAgent;

  return {
    gatewayUrl: process.env.GATEWAY_URL ?? 'ws://localhost:8787/ws',
    token: process.env.WORKER_TOKEN ?? 'dev-token',
    nodeId: process.env.NODE_ID ?? `node-${Math.random().toString(36).slice(2, 8)}`,
    agents: (process.env.AGENTS ?? 'mock').split(',').map((s) => s.trim()),
    templatesDir: process.env.TEMPLATES_DIR ?? path.resolve(repoRoot, 'templates'),
    workspaceDir: process.env.WORKSPACE_DIR ?? path.resolve(pkgRoot, 'workspace'),
    agentCmd,
    agentArgs: agentArgsRaw.split(/\s+/).filter(Boolean),
    // 默认总时长 60min 兜底失控循环；连续 5min 无输出判定 agent 卡死
    runTimeoutMs: numberEnv('RUN_TIMEOUT_MS', 60 * 60 * 1000),
    idleTimeoutMs: numberEnv('RUN_IDLE_TIMEOUT_MS', 5 * 60 * 1000),
  };
}
