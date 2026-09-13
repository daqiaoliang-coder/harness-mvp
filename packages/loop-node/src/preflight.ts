import { exec } from 'node:child_process';
import { promisify } from 'node:util';

const execAsync = promisify(exec);

export type CheckLevel = 'BLOCKER' | 'WARNING' | 'INFO';

export interface CheckResult {
  name: string;
  level: CheckLevel;
  ok: boolean;
  message: string;
}

export interface PreflightConfig {
  checks: {
    name: string;
    level: CheckLevel;
    command?: string;
    envVar?: string;
  }[];
}

export const DEFAULT_PREFLIGHT: PreflightConfig = {
  checks: [
    { name: 'git', level: 'BLOCKER', command: 'git --version' },
    {
      name: 'network',
      level: 'BLOCKER',
      command: 'curl -sf --max-time 5 https://api.github.com > /dev/null',
    },
    { name: 'node', level: 'BLOCKER', command: 'node --version' },
  ],
};

export async function runPreflight(config: PreflightConfig): Promise<CheckResult[]> {
  const results: CheckResult[] = [];

  for (const check of config.checks) {
    if (check.envVar) {
      const val = process.env[check.envVar];
      results.push({
        name: check.name,
        level: check.level,
        ok: !!val,
        message: val ? `${check.envVar} 已设置` : `${check.envVar} 未设置`,
      });
      continue;
    }

    if (check.command) {
      try {
        await execAsync(check.command, { timeout: 10_000 });
        results.push({ name: check.name, level: check.level, ok: true, message: '通过' });
      } catch (err) {
        results.push({
          name: check.name,
          level: check.level,
          ok: false,
          message: (err as Error).message.slice(0, 200),
        });
      }
    }
  }

  return results;
}
