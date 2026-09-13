/**
 * loop-node 配置解析：全部取自环境变量，缺省值面向本地 mock 模式开箱即用
 * （dev-token、本机 gateway、scripts/mock-agent.mjs）。
 * 进程启动时解析一次并在各模块间共享；非法数值配置静默回退默认值，不让节点起不来。
 */
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

// 空串视为未设置；非有限数字或负数静默回退默认值（超时项约定 0 = 禁用）
function numberEnv(name: string, defaultValue: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return defaultValue;
  const n = Number(raw);
  return Number.isFinite(n) && n >= 0 ? n : defaultValue;
}

export function loadConfig(): LoopNodeConfig {
  const defaultMockAgent = path.resolve(repoRoot, 'scripts/mock-agent.mjs');
  const agentCmd = process.env.AGENT_CMD ?? process.execPath;
  /**
   * AGENT_ARGS 专用于「传给 agent 命令的参数」，缺省回退到内置 mock agent。
   *
   * 为什么不能复用 AGENTS：这两个变量语义完全不同 ——
   *   AGENTS      = 向 Gateway 声明本节点可用的 agent 列表（hello 消息用）
   *   AGENT_ARGS  = 启动 agent 进程时附加的命令行参数
   * 此前实现用 AGENTS 兼作 agentArgs，导致 mock 模式（AGENTS=mock）下
   * 实际执行的是 `node mock <promptFile>`，posix_spawnp 直接失败 ——
   * 每个 run 都以「agent 启动失败」告终，重试耗尽后熔断转 HITL，
   * 所谓「mock 模式零配置开箱即用」根本跑不通。
   *
   * 回退值必须是绝对路径：loop-node 的 cwd 是 packages/loop-node，
   * 相对路径 scripts/mock-agent.mjs 在该 cwd 下解析不到。
   */
  const agentArgsRaw = process.env.AGENT_ARGS ?? defaultMockAgent;

  return {
    gatewayUrl: process.env.GATEWAY_URL ?? 'ws://localhost:8787/ws',
    token: process.env.WORKER_TOKEN ?? 'dev-token',
    nodeId: process.env.NODE_ID ?? `node-${Math.random().toString(36).slice(2, 8)}`,
    // 上报给 gateway 的可执行节点类型，gateway 按节点名匹配派发；与 agentArgs 共用 AGENTS
    agents: (process.env.AGENTS ?? 'mock').split(',').map((s) => s.trim()),
    templatesDir: process.env.TEMPLATES_DIR ?? path.resolve(repoRoot, 'templates'),
    workspaceDir: process.env.WORKSPACE_DIR ?? path.resolve(pkgRoot, 'workspace'),
    agentCmd,
    agentArgs: agentArgsRaw.split(/\s+/).filter(Boolean),
    // 总时长 60min 兜底失控循环；连续 5min 无输出判定 agent 卡死。
    // 由 index.ts 执行层消费：触发后先 SIGTERM 再 SIGKILL 强杀（0 = 禁用）
    runTimeoutMs: numberEnv('RUN_TIMEOUT_MS', 60 * 60 * 1000),
    idleTimeoutMs: numberEnv('RUN_IDLE_TIMEOUT_MS', 5 * 60 * 1000),
  };
}
