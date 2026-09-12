import path from 'node:path';
import { fileURLToPath } from 'node:url';

export interface GatewayConfig {
  httpPort: number;
  dbPath: string;
  projectId: string;
  workerToken: string;
  pipeline: string[];
  github: {
    mode: 'real' | 'mock';
    token?: string;
    owner?: string;
    repo?: string;
    pollIntervalMs: number;
  };
}

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, '../../..');

export function loadConfig(): GatewayConfig {
  const hasCreds =
    !!process.env.GITHUB_TOKEN && !!process.env.GITHUB_OWNER && !!process.env.GITHUB_REPO;
  const mode = (process.env.GITHUB_MODE as 'real' | 'mock') ?? (hasCreds ? 'real' : 'mock');

  return {
    httpPort: Number(process.env.HTTP_PORT ?? 8787),
    dbPath: process.env.DB_PATH
      ? path.resolve(process.cwd(), process.env.DB_PATH)
      : path.resolve(repoRoot, 'data/events.db'),
    projectId: process.env.PROJECT_ID ?? 'demo',
    workerToken: process.env.WORKER_TOKEN ?? 'dev-token',
    pipeline: (process.env.PIPELINE ?? 'plan,code,test')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean),
    github: {
      mode,
      token: process.env.GITHUB_TOKEN,
      owner: process.env.GITHUB_OWNER,
      repo: process.env.GITHUB_REPO,
      pollIntervalMs: Number(process.env.GITHUB_POLL_MS ?? 5000),
    },
  };
}
