import fs from 'node:fs/promises';
import path from 'node:path';
import YAML from 'yaml';
import {
  DEFAULT_BUDGET,
  DEFAULT_RETRY,
  type BudgetConfig,
  type RetryPolicy,
} from '@harness/shared';

export interface Template {
  node: string;
  next?: string;
  inputs: string[];
  budget: BudgetConfig;
  /**
   * 节点级重试与熔断策略，由模板 frontmatter 声明。
   * 未声明时取 DEFAULT_RETRY；解析结果随 run.result 回报控制面，
   * 控制面不读模板，因此这是策略唯一的传递通道。
   */
  retry: RetryPolicy;
  body: string;
}

export async function loadTemplate(dir: string, nodeKey: string): Promise<Template> {
  const file = path.join(dir, `${nodeKey}.md`);
  const raw = await fs.readFile(file, 'utf8');
  const { meta, body } = parseFrontmatter(raw);
  return {
    node: typeof meta.node === 'string' ? meta.node : nodeKey,
    next: typeof meta.next === 'string' ? meta.next : undefined,
    inputs: Array.isArray(meta.inputs) ? meta.inputs.map(String) : [],
    budget: parseBudget(meta.budget),
    retry: parseRetry(meta.retry),
    body,
  };
}

function parseBudget(raw: unknown): BudgetConfig {
  if (!raw || typeof raw !== 'object') return DEFAULT_BUDGET;
  const b = raw as Record<string, unknown>;
  return {
    maxInputTokens:
      typeof b.max_input_tokens === 'number' ? b.max_input_tokens : DEFAULT_BUDGET.maxInputTokens,
    maxOutputTokens:
      typeof b.max_output_tokens === 'number' ? b.max_output_tokens : DEFAULT_BUDGET.maxOutputTokens,
    maxToolCalls:
      typeof b.max_tool_calls === 'number' ? b.max_tool_calls : DEFAULT_BUDGET.maxToolCalls,
  };
}

/**
 * 解析模板声明的重试策略，逐字段校验并回退默认值。
 *
 * 为什么必须逐字段防御：模板由业务方维护，不在编译期检查范围内。
 * 若直接把 raw 透传，一个拼错的字段（如 max_attempt 少个 s）会让
 * policy.maxAttempts 变成 undefined，控制面 `rec.attempt >= undefined`
 * 恒为 false —— 熔断静默失效，节点无限重跑烧 token，且不报错。
 * 这比解析失败更难排查，所以宁可回退默认值。
 *
 * on_exhausted 只接受枚举内的值：控制面据此决定熔断后是转人工、
 * 直接失败还是跳过，非法值会让流程走进未定义分支。
 */
const EXHAUSTED_ACTIONS = ['hitl', 'fail', 'skip'] as const;

/** 导出供单测：逐字段校验是防「拼错字段名导致熔断静默失效」的关键，必须有回归覆盖。 */
export function parseRetry(raw: unknown): RetryPolicy {
  if (!raw || typeof raw !== 'object') return { ...DEFAULT_RETRY };
  const r = raw as Record<string, unknown>;

  const positiveInt = (v: unknown, fallback: number): number =>
    typeof v === 'number' && Number.isFinite(v) && v > 0 ? Math.floor(v) : fallback;

  const onExhausted = r.on_exhausted;
  return {
    maxAttempts: positiveInt(r.max_attempts, DEFAULT_RETRY.maxAttempts),
    maxTokens: positiveInt(r.max_tokens, DEFAULT_RETRY.maxTokens),
    maxDurationMs: positiveInt(r.max_duration_ms, DEFAULT_RETRY.maxDurationMs),
    onExhausted: EXHAUSTED_ACTIONS.includes(onExhausted as never)
      ? (onExhausted as RetryPolicy['onExhausted'])
      : DEFAULT_RETRY.onExhausted,
  };
}

function parseFrontmatter(raw: string): { meta: Record<string, unknown>; body: string } {
  if (!raw.startsWith('---')) return { meta: {}, body: raw };
  const end = raw.indexOf('\n---', 3);
  if (end === -1) return { meta: {}, body: raw };
  const fmText = raw.slice(3, end);
  const body = raw.slice(end + 4).replace(/^\r?\n/, '');
  try {
    const parsed = YAML.parse(fmText);
    return {
      meta: parsed && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : {},
      body,
    };
  } catch {
    return { meta: {}, body: raw };
  }
}

export function renderPrompt(template: Template, context: Record<string, string>): string {
  let out = template.body;
  for (const [k, v] of Object.entries(context)) {
    out = out.split(`{{${k}}}`).join(v);
  }
  return out;
}
