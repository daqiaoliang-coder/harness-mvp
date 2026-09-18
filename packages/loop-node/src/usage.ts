/**
 * agent 真实用量采集：解析 run 工作目录下 trae-cli 产出的 trajectories/*.json，
 * 累加每次 LLM 交互的 usage。只在 agent 退出后同步读一次，不参与执行路径。
 */
import fs from 'node:fs';
import path from 'node:path';

export interface TokenUsage {
  input: number;
  output: number;
}

/**
 * 汇总 workdir/trajectories/*.json 的 llm_interactions[].response.usage。
 *
 * 口径说明：input_tokens / output_tokens 原样累加，不二次加工 ——
 * cache_read/creation 与 reasoning_tokens 是否已含在 input/output 因 provider 实现而异，
 * 自行拆算反而引入错误口径；需要更精细的口径时再扩展字段。
 *
 * 容错原则：目录不存在（mock agent、启动前失败）、文件损坏、usage 缺失都静默跳过，
 * 返回 undefined 让调用方回落到控制面的注入 token 估算口径 ——
 * 用量采集失败不该影响 run.result 的送达。
 */
export function collectUsage(workdir: string): TokenUsage | undefined {
  const dir = path.join(workdir, 'trajectories');
  let files: string[];
  try {
    files = fs.readdirSync(dir).filter((f) => f.endsWith('.json'));
  } catch {
    return undefined;
  }
  let input = 0;
  let output = 0;
  let seen = false;
  for (const f of files) {
    let data: { llm_interactions?: Array<{ response?: { usage?: Record<string, number> } }> };
    try {
      data = JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8'));
    } catch {
      continue;
    }
    for (const it of data?.llm_interactions ?? []) {
      const u = it?.response?.usage;
      if (!u) continue;
      seen = true;
      input += Number(u.input_tokens) || 0;
      output += Number(u.output_tokens) || 0;
    }
  }
  return seen ? { input, output } : undefined;
}
