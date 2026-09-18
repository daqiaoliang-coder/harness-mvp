/**
 * 交付效率评分（Meego 四维中的第一维，MVP 先闭环一维）。
 *
 * 纯函数：只吃事件流（证据包同源），不访问任何外部状态，便于离线复算与单元测试。
 * 评分口径与锚点全部导出为常量（SCORE_ANCHORS_V0），目前是 v0 初始值 ——
 * 真实数据攒够后应按分布校准，改常量即可，不动计算逻辑。
 *
 * 刻意不做的事：
 *  - 流程未完成不打分（score: null），避免「证据不足被误判 0 分」（Meego 的真实踩坑）；
 *  - transcript 缺失不扣分，只标记 tokensEstimated，估算口径不应惩罚执行面；
 *  - 熔断不单独扣分：它的代价已由 retries 与 HITL 体现，重复计罚会双重归因。
 */
import type { HarnessEvent } from '@harness/shared';

export const SCORE_ANCHORS_V0 = {
  version: 'delivery-v0' as const,
  /** 每次重试（attempt-1）扣 0.5，封顶 2 分 */
  retryPenaltyEach: 0.5,
  retryPenaltyCap: 2,
  /** 每次非预期人工介入扣 1，封顶 3 分 */
  hitlPenaltyEach: 1,
  hitlPenaltyCap: 3,
  /** 人工等待累计超过该时长额外扣 1 分 */
  hitlWaitExtraMs: 30 * 60 * 1000,
  hitlWaitExtraPenalty: 1,
  /** 节点执行总耗时（控制面口径，逐 attempt 求和）阈值与扣分 */
  durationMsThresholds: [
    { ms: 120 * 60 * 1000, penalty: 2 },
    { ms: 60 * 60 * 1000, penalty: 1 },
  ] as { ms: number; penalty: number }[],
} as const;

export interface NodeMetrics {
  nodeKey: string;
  /** 最终观察到的尝试次数（失败累计 / 成功总尝试） */
  attempts: number;
  /** 该节点逐 attempt 的控制面耗时合计 */
  durationMs: number;
  /** 该节点逐 attempt 的 token 合计（真实 transcript 优先，否则注入估算） */
  tokens: number;
  /** 是否至少一个 attempt 只有估算口径 */
  tokensEstimated: boolean;
}

export interface IssueMetrics {
  workItemId: string;
  completed: boolean;
  nodeCount: number;
  nodes: NodeMetrics[];
  /** Σ(max(attempt-1, 0))，即所有节点的返工/重试总次数 */
  retries: number;
  totalDurationMs: number;
  totalTokens: number;
  /** 任一节点用到估算口径即为 true，看板/评分展示必须标注 */
  tokensEstimated: boolean;
  hitlCount: number;
  inferredHitlCount: number;
  hitlWaitMs: number;
  breakerTrips: number;
  failures: number;
}

export interface DeliveryScore {
  /** 0–5（0.5 粒度）；流程未完成时为 null，理由见 reasons */
  score: number | null;
  grade: 'A' | 'B' | 'C' | 'D' | 'E' | null;
  reasons: string[];
  metrics: IssueMetrics;
  anchorVersion: string;
}

function num(v: unknown): number | undefined {
  return typeof v === 'number' && Number.isFinite(v) ? v : undefined;
}

/** 单次终态事件的 token 口径：transcript 真实用量优先，缺省回退注入估算 */
function attemptTokens(d: Record<string, unknown>): { tokens: number; estimated: boolean } {
  const t = d.tokens as { input?: number; output?: number } | undefined;
  if (t && (num(t.input) || num(t.output))) {
    return { tokens: (num(t.input) ?? 0) + (num(t.output) ?? 0), estimated: false };
  }
  // failed 事件用 tokensConsumed（记账口径，本身可能已是真实值合计）
  return { tokens: num(d.tokensConsumed) ?? num(d.inputTokens) ?? 0, estimated: true };
}

/** 从事件流汇总单个 issue 的执行指标（证据包与评分共用的唯一派生口径）。 */
export function summarize(workItemId: string, events: HarnessEvent[]): IssueMetrics {
  const nodes = new Map<string, NodeMetrics>();
  let completed = false;
  let hitlCount = 0;
  let inferredHitlCount = 0;
  let hitlWaitMs = 0;
  let breakerTrips = 0;
  let failures = 0;

  const nodeOf = (key: unknown): NodeMetrics => {
    const k = String(key ?? 'unknown');
    let n = nodes.get(k);
    if (!n) {
      n = { nodeKey: k, attempts: 0, durationMs: 0, tokens: 0, tokensEstimated: false };
      nodes.set(k, n);
    }
    return n;
  };

  for (const ev of events) {
    const d = (ev.details ?? {}) as Record<string, unknown>;
    switch (ev.event) {
      case 'issue.node.failed': {
        failures += 1;
        const n = nodeOf(d.nodeKey);
        n.attempts = Math.max(n.attempts, num(d.attempt) ?? 0);
        n.durationMs += num(d.durationMs) ?? 0;
        const t = attemptTokens(d);
        n.tokens += t.tokens;
        n.tokensEstimated ||= t.estimated;
        break;
      }
      case 'issue.node.advanced':
      case 'issue.completed': {
        if (ev.event === 'issue.completed') completed = true;
        // 指标归属「刚执行完的节点」：advanced 的 from；completed 自带 nodeKey（末节点无 to）
        const key = ev.event === 'issue.completed' ? d.nodeKey : d.from;
        const n = nodeOf(key);
        n.attempts = Math.max(n.attempts, num(d.attempt) ?? 0);
        n.durationMs += num(d.durationMs) ?? 0;
        const t = attemptTokens(d);
        n.tokens += t.tokens;
        n.tokensEstimated ||= t.estimated;
        break;
      }
      case 'hitl.entered':
        hitlCount += 1;
        if (d.inferred === true) inferredHitlCount += 1;
        break;
      case 'hitl.resolved':
        hitlWaitMs += num(d.waitMs) ?? 0;
        break;
      case 'circuit_breaker.tripped':
        breakerTrips += 1;
        break;
    }
  }

  const list = [...nodes.values()];
  return {
    workItemId,
    completed,
    nodeCount: list.length,
    nodes: list,
    retries: list.reduce((s, n) => s + Math.max(n.attempts - 1, 0), 0),
    totalDurationMs: list.reduce((s, n) => s + n.durationMs, 0),
    totalTokens: list.reduce((s, n) => s + n.tokens, 0),
    tokensEstimated: list.some((n) => n.tokensEstimated),
    hitlCount,
    inferredHitlCount,
    hitlWaitMs,
    breakerTrips,
    failures,
  };
}

function roundHalf(v: number): number {
  return Math.round(v * 2) / 2;
}

function gradeOf(score: number): DeliveryScore['grade'] {
  if (score >= 4.5) return 'A';
  if (score >= 3.5) return 'B';
  if (score >= 2.5) return 'C';
  if (score >= 1.5) return 'D';
  return 'E';
}

/** 计算交付效率评分。events 应为该 issue 的完整证据集（store.findByWorkItem 的结果）。 */
export function scoreDelivery(workItemId: string, events: HarnessEvent[]): DeliveryScore {
  const a = SCORE_ANCHORS_V0;
  const metrics = summarize(workItemId, events);
  const reasons: string[] = [];

  if (!metrics.completed) {
    reasons.push('流程尚未完成（无 issue.completed），不产出效率分，避免证据不足被误判');
    return { score: null, grade: null, reasons, metrics, anchorVersion: a.version };
  }

  let penalty = 0;

  const retryPenalty = Math.min(metrics.retries * a.retryPenaltyEach, a.retryPenaltyCap);
  if (retryPenalty > 0) {
    penalty += retryPenalty;
    reasons.push(`返工 ${metrics.retries} 次（-${retryPenalty}）`);
  }

  const hitlWaitExtra = metrics.hitlWaitMs > a.hitlWaitExtraMs ? a.hitlWaitExtraPenalty : 0;
  const hitlPenalty = Math.min(
    metrics.hitlCount * a.hitlPenaltyEach + hitlWaitExtra,
    a.hitlPenaltyCap,
  );
  if (hitlPenalty > 0) {
    penalty += hitlPenalty;
    reasons.push(
      `人工介入 ${metrics.hitlCount} 次/等待 ${(metrics.hitlWaitMs / 60000).toFixed(1)} 分钟（-${hitlPenalty}）` +
        (metrics.inferredHitlCount ? `，其中 ${metrics.inferredHitlCount} 次进入时刻为近似值` : ''),
    );
  }

  const durationRule = a.durationMsThresholds.find((t) => metrics.totalDurationMs >= t.ms);
  if (durationRule) {
    penalty += durationRule.penalty;
    reasons.push(
      `节点执行总耗时 ${(metrics.totalDurationMs / 60000).toFixed(1)} 分钟 ≥ ${durationRule.ms / 60000} 分钟（-${durationRule.penalty}）`,
    );
  }

  if (metrics.tokensEstimated) reasons.push('token 含字符折算估算口径（未取得 agent transcript）');
  if (reasons.length === 0) reasons.push('一次通过、无人工介入、耗时正常');

  const score = roundHalf(Math.min(5, Math.max(0, 5 - penalty)));
  return { score, grade: gradeOf(score), reasons, metrics, anchorVersion: a.version };
}
