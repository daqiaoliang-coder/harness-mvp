/**
 * 熔断器（纯内存记账，runKey = `${issueNumber}:${nodeKey}`）：拦截「同一节点反复失败」
 * 造成的 token 空耗。失败次数 / 累计 token / 总时长任一触顶即熔断，由 Scheduler 打
 * hitl:waiting 标签把 issue 挂起转人工；节点成功一次即 reset。记账不跨重启，
 * 持久的流程状态仍以 GitHub 标签为准。
 */
import { DEFAULT_RETRY, type RetryPolicy } from '@harness/shared';

// 默认策略与策略类型均以 shared 为唯一事实源，此处仅做转发，
// 使既有 import { DEFAULT_RETRY } from './circuit-breaker.js' 的调用方无需改动。
export { DEFAULT_RETRY };
export type { RetryPolicy };

interface RunAttempt {
  attempt: number;
  tokens: number;
  startedAt: number;
}

export interface CheckResult {
  allowed: boolean;
  /** 本次是第几次尝试（1-based）；allowed 为 true 时有意义 */
  attempt: number;
  reason?: string;
}

export class CircuitBreaker {
  private attempts = new Map<string, RunAttempt>();

  /**
   * 记录一次失败并判定是否还允许重试。
   *
   * 语义：tokensConsumed 先累加，再做阈值判定——记账与判定在同一次调用内完成，
   * 调用方无需关心顺序（此前 recordTokens 必须在 check 之后调用，
   * 而首次 check 才建立条目，导致首次失败的用量被静默丢弃）。
   *
   * attempt 表示「已累计的失败次数」，首次失败即为 1，
   * 因此 maxAttempts: N 表示第 N 次失败时熔断。
   */
  check(runKey: string, policy: RetryPolicy, tokensConsumed = 0): CheckResult {
    const prev = this.attempts.get(runKey);
    const rec: RunAttempt = prev ?? { attempt: 0, tokens: 0, startedAt: Date.now() };
    rec.attempt += 1;
    if (tokensConsumed > 0) rec.tokens += tokensConsumed;
    this.attempts.set(runKey, rec);

    if (rec.attempt >= policy.maxAttempts) {
      return {
        allowed: false,
        attempt: rec.attempt,
        reason: `已达到最大重试次数 ${policy.maxAttempts}（累计 ${rec.tokens} tokens）`,
      };
    }

    if (rec.tokens >= policy.maxTokens) {
      return {
        allowed: false,
        attempt: rec.attempt,
        reason: `已超过 token 预算 ${policy.maxTokens}（实际 ${rec.tokens}）`,
      };
    }

    if (Date.now() - rec.startedAt >= policy.maxDurationMs) {
      return {
        allowed: false,
        attempt: rec.attempt,
        reason: `已超过最长执行时间 ${policy.maxDurationMs}ms`,
      };
    }

    return { allowed: true, attempt: rec.attempt };
  }

  /**
   * 单独补记 token（例如事后从事件流回填用量）。
   * runKey 不存在时自动建立条目，不再静默忽略。
   */
  recordTokens(runKey: string, tokens: number) {
    if (tokens <= 0) return;
    const prev = this.attempts.get(runKey);
    if (prev) {
      prev.tokens += tokens;
      return;
    }
    this.attempts.set(runKey, { attempt: 0, tokens, startedAt: Date.now() });
  }

  /** 读取累计 token（用于事件记录与看板）。 */
  tokens(runKey: string): number {
    return this.attempts.get(runKey)?.tokens ?? 0;
  }

  reset(runKey: string) {
    this.attempts.delete(runKey);
  }
}
