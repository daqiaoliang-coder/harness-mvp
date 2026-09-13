export interface BudgetConfig {
  maxInputTokens: number;
  maxOutputTokens: number;
  maxToolCalls: number;
}

export const DEFAULT_BUDGET: BudgetConfig = {
  maxInputTokens: 200_000,
  maxOutputTokens: 50_000,
  maxToolCalls: 200,
};

/** 粗略估算 token 数：中文约 1.5 字符/token，英文约 4 字符/token */
export function estimateTokens(text: string): number {
  let cjk = 0;
  let other = 0;
  for (const ch of text) {
    const code = ch.codePointAt(0)!;
    if (code >= 0x4e00 && code <= 0x9fff) cjk++;
    else other++;
  }
  return Math.ceil(cjk / 1.5 + other / 4);
}

export interface BudgetUsage {
  inputTokens: number;
  outputTokens: number;
  toolCalls: number;
  exceeded: boolean;
  /** 被截断或被省略的字段名 */
  exceededFields: string[];
  /** 预算耗尽后被整体省略（只留占位符）的字段名 */
  droppedFields: string[];
}

/**
 * 按预算裁剪 context，**硬保证** usage.inputTokens <= budget.maxInputTokens。
 *
 * 三段式策略：
 *  1. 预算内 → 原样透传；
 *  2. 预算不足但仍有剩余余量 → 二分求出「头部 + 截断标记」不超过余量的最大前缀；
 *  3. 余量耗尽 → 只留一行占位符，且**不计入 total**（占位符是有界常量，
 *     否则字段数量一多，累计的标记文本本身就能把预算撑爆）。
 *
 * 为什么必须这样：早期实现是「超预算字段一律截到前 200 字符并累加」，
 * 在字段数较多时（例如 20 个字段 × 每个 1000 token、预算 200）
 * 裁剪后总量反而达到 1200，预算完全不封顶。
 */
export function pruneContext(
  context: Record<string, string>,
  budget: BudgetConfig,
): { pruned: Record<string, string>; usage: BudgetUsage } {
  const limit = budget.maxInputTokens;
  const pruned: Record<string, string> = {};
  const exceededFields: string[] = [];
  const droppedFields: string[] = [];
  let total = 0;

  for (const [key, value] of Object.entries(context)) {
    const tokens = estimateTokens(value);

    // 1) 预算内，原样透传
    if (total + tokens <= limit) {
      pruned[key] = value;
      total += tokens;
      continue;
    }

    const remaining = limit - total;
    exceededFields.push(key);

    // 3) 余量耗尽，只留占位符且不计入 total
    if (remaining <= 0) {
      pruned[key] = `[已省略：上下文预算耗尽，原字段约 ${tokens} tokens]`;
      droppedFields.push(key);
      continue;
    }

    // 2) 二分求最大可保留前缀，使「前缀 + 截断标记」整体不超过余量。
    //    estimateTokens 对前缀长度单调不减，故二分成立。
    const marker = (head: string) => `${head}\n\n[...已截断，原字段约 ${tokens} tokens]`;
    let lo = 0;
    let hi = value.length;
    while (lo < hi) {
      const mid = (lo + hi + 1) >> 1;
      if (estimateTokens(marker(value.slice(0, mid))) <= remaining) lo = mid;
      else hi = mid - 1;
    }
    const head = value.slice(0, lo);
    pruned[key] = marker(head);
    total += estimateTokens(marker(head));
  }

  return {
    pruned,
    usage: {
      inputTokens: total,
      outputTokens: 0,
      toolCalls: 0,
      exceeded: exceededFields.length > 0,
      exceededFields,
      droppedFields,
    },
  };
}
