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
}

const here = path.dirname(fileURLToPath(import.meta.url));
const pkgRoot = path.resolve(here, '..');
const repoRoot = path.resolve(here, '../../..');

export function loadConfig(): LoopNodeConfig {
  const defaultMockAgent = path.resolve(repoRoot, 'scripts/mock-agent.mjs');
  const agentCmd = process.env.AGENT_CMD ?? process.execPath;
  const agentArgsRaw = process.env.AGENT_ARGS ?? defaultMockAgent;

  return {
    gatewayUrl: process.env.GATEWAY_URL ?? 'ws://localhost:8787/ws',
    token: process.env.WORKER_TOKEN ?? 'dev-token',
    nodeId: process.env.NODE_ID ?? `node-${Math.random().toString(36).slice(2, 8)}`,
    agents: (process.env.AGENTS ?? 'mock').split(',').map((s) => s.trim()),
    templatesDir: process.env.TEMPLATES_DIR ?? path.resolve(repoRoot, 'templates'),
    workspaceDir: process.env.WORKSPACE_DIR ?? path.resolve(pkgRoot, 'workspace'),
    agentCmd,
    agentArgs: agentArgsRaw.split(/\s+/).filter(Boolean),
  };
}
