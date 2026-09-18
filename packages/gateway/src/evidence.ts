/**
 * 评测证据包冻结：按 workItemId 把散落的事件流聚合成一个不可变快照，
 * 对应 Meego 的 evidence.json + structured-events。MVP 为单文件 JSON，
 * 后续若接对象存储/评测实例，只需在导出处替换落地方式，派生口径不变。
 */
import type { HarnessEvent } from '@harness/shared';
import type { Issue } from './github.js';
import { scoreDelivery, type DeliveryScore, type IssueMetrics } from './scoring.js';

export const EVIDENCE_SCHEMA_VERSION = 1 as const;

export interface EvidenceBundle {
  schemaVersion: typeof EVIDENCE_SCHEMA_VERSION;
  /** 证据类型，便于消费方区分后续扩展（如 L2 完整实例包） */
  kind: 'issue.evidence';
  workItemId: string;
  exportedAt: string;
  /** 导出时刻的 GitHub 侧快照（标签/状态）；取不到时省略 */
  issueSnapshot?: {
    number: number;
    title: string;
    labels: string[];
    state: Issue['state'];
  };
  eventCount: number;
  /** 该 issue 的完整事件集（含按 runId 关联的 worker 原始事件），按 seq 升序 */
  events: HarnessEvent[];
  metrics: IssueMetrics;
  score: DeliveryScore;
}

export function buildEvidence(
  workItemId: string,
  events: HarnessEvent[],
  issue?: Issue,
  now: () => string = () => new Date().toISOString(),
): EvidenceBundle {
  const score = scoreDelivery(workItemId, events);
  return {
    schemaVersion: EVIDENCE_SCHEMA_VERSION,
    kind: 'issue.evidence',
    workItemId,
    exportedAt: now(),
    issueSnapshot: issue
      ? { number: issue.number, title: issue.title, labels: issue.labels, state: issue.state }
      : undefined,
    eventCount: events.length,
    /** 该 issue 的完整事件集（含按 runId 关联的 worker 原始事件）；冻结时强制按 seq 排序 */
    events: [...events].sort((a, b) => a.seq - b.seq),
    score,
    // metrics 与 score 同源，重复字段是为了让不关心评分的消费方少一层嵌套
    metrics: score.metrics,
  };
}
