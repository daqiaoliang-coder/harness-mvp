export interface RetryPolicy {
  maxAttempts: number;
  maxTokens: number;
  maxDurationMs: number;
  onExhausted: 'hitl' | 'fail' | 'skip';
}

export const DEFAULT_RETRY: RetryPolicy = {
  maxAttempts: 3,
  maxTokens: 5_000_000,
  maxDurationMs: 3_600_000,
  onExhausted: 'hitl',
};

interface RunAttempt {
  attempt: number;
  tokens: number;
  startedAt: number;
}

export class CircuitBreaker {
  private attempts = new Map<string, RunAttempt>();

  check(runKey: string, policy: RetryPolicy): { allowed: boolean; reason?: string } {
    const prev = this.attempts.get(runKey);
    if (!prev) {
      this.attempts.set(runKey, { attempt: 1, tokens: 0, startedAt: Date.now() });
      return { allowed: true };
    }

    if (prev.attempt >= policy.maxAttempts) {
      return {
        allowed: false,
        reason: `已达到最大重试次数 ${policy.maxAttempts}（累计 ${prev.tokens} tokens）`,
      };
    }

    if (prev.tokens >= policy.maxTokens) {
      return {
        allowed: false,
        reason: `已超过 token 预算 ${policy.maxTokens}（实际 ${prev.tokens}）`,
      };
    }

    if (Date.now() - prev.startedAt >= policy.maxDurationMs) {
      return {
        allowed: false,
        reason: `已超过最长执行时间 ${policy.maxDurationMs}ms`,
      };
    }

    prev.attempt++;
    return { allowed: true };
  }

  recordTokens(runKey: string, tokens: number) {
    const prev = this.attempts.get(runKey);
    if (prev) prev.tokens += tokens;
  }

  reset(runKey: string) {
    this.attempts.delete(runKey);
  }
}
