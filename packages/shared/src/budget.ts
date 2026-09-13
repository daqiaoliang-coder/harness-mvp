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
  exceededFields: string[];
}

/**
 * 按预算裁剪 context：
 * - 总输入不超过 maxInputTokens
 * - 超预算的字段只保留前 200 字符 + 截断标记
 */
export function pruneContext(
  context: Record<string, string>,
  budget: BudgetConfig,
): { pruned: Record<string, string>; usage: BudgetUsage } {
  const entries = Object.entries(context);
  const pruned: Record<string, string> = {};
  let total = 0;
  const exceededFields: string[] = [];

  for (const [key, value] of entries) {
    const tokens = estimateTokens(value);
    if (total + tokens <= budget.maxInputTokens) {
      pruned[key] = value;
      total += tokens;
    } else {
      const head = value.slice(0, 200);
      pruned[key] = head + `\n\n[...已截断，原字段约 ${tokens} tokens]`;
      total += estimateTokens(pruned[key]);
      exceededFields.push(key);
    }
  }

  return {
    pruned,
    usage: {
      inputTokens: total,
      outputTokens: 0,
      toolCalls: 0,
      exceeded: exceededFields.length > 0,
      exceededFields,
    },
  };
}
