import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { HarnessEvent } from '@harness/shared';
import { scoreDelivery, summarize } from './scoring.js';
import { buildEvidence } from './evidence.js';

let seq = 0;
function ev(
  event: string,
  details: Record<string, unknown> = {},
  extra: Partial<HarnessEvent> = {},
): HarnessEvent {
  return {
    id: `ev_${++seq}`,
    seq,
    timestamp: new Date(seq * 1000).toISOString(),
    source: event.startsWith('run.') ? 'worker' : 'github',
    event,
    projectId: 'test',
    workItemId: '1',
    details,
    ...extra,
  };
}

/** 三节点一次通过的完整成功流：plan→code 各一条 advanced，test 走 completed */
function cleanFlow(): HarnessEvent[] {
  return [
    ev('issue.node.advanced', { from: 'plan', to: 'code', attempt: 1, durationMs: 10_000, tokens: { input: 100, output: 20 } }),
    ev('issue.node.advanced', { from: 'code', to: 'test', attempt: 1, durationMs: 20_000, tokens: { input: 200, output: 40 } }),
    ev('issue.completed', { nodeKey: 'test', attempt: 1, durationMs: 30_000, tokens: { input: 300, output: 60 } }),
  ];
}

test('一次通过：5 分 / A，指标逐节点正确归属（advanced 归 from，completed 归末节点）', () => {
  const events = cleanFlow();
  const r = scoreDelivery('1', events);
  assert.equal(r.score, 5);
  assert.equal(r.grade, 'A');
  const m = r.metrics;
  assert.equal(m.nodeCount, 3);
  assert.equal(m.retries, 0);
  assert.equal(m.totalDurationMs, 60_000);
  assert.equal(m.totalTokens, 720);
  assert.equal(m.completed, true);
  assert.equal(m.tokensEstimated, false);
});

test('返工 2 次 + 1 次短时人工介入：3 分 / C，扣分原因可追溯', () => {
  const events = [
    // plan 失败两次（attempt 1、2），第三次成功（attempt 3）
    ev('issue.node.failed', { nodeKey: 'plan', attempt: 1, durationMs: 5_000, tokensConsumed: 100 }),
    ev('issue.node.failed', { nodeKey: 'plan', attempt: 2, durationMs: 5_000, tokensConsumed: 100 }),
    ev('hitl.entered', { nodeKey: 'plan', inferred: false }),
    ev('hitl.resolved', { nodeKey: 'plan', waitMs: 60_000, inferredEntry: false }),
    ev('issue.node.advanced', { from: 'plan', to: 'code', attempt: 3, durationMs: 5_000, tokensConsumed: 100 }),
    ev('issue.node.advanced', { from: 'code', to: 'test', attempt: 1, durationMs: 1_000, tokensConsumed: 0 }),
    ev('issue.completed', { nodeKey: 'test', attempt: 1, durationMs: 1_000, tokensConsumed: 0 }),
  ];
  const r = scoreDelivery('1', events);
  assert.equal(r.score, 3, `reasons: ${r.reasons.join('; ')}`);
  assert.equal(r.grade, 'C');
  assert.equal(r.metrics.retries, 2);
  assert.equal(r.metrics.hitlCount, 1);
  assert.equal(r.metrics.hitlWaitMs, 60_000);
  assert.match(r.reasons.join(), /返工 2 次/);
  assert.match(r.reasons.join(), /人工介入 1 次/);
});

test('流程未完成：不打分（null），避免证据不足被误判 0 分', () => {
  const r = scoreDelivery('1', [ev('issue.node.failed', { nodeKey: 'plan', attempt: 1, durationMs: 1000 })]);
  assert.equal(r.score, null);
  assert.equal(r.grade, null);
  assert.equal(r.metrics.completed, false);
});

test('无 transcript 仅估算口径：标记 tokensEstimated 但不扣分', () => {
  const events = [
    ev('issue.node.advanced', { from: 'plan', to: 'code', attempt: 1, durationMs: 1_000, inputTokens: 888 }),
    ev('issue.node.advanced', { from: 'code', to: 'test', attempt: 1, durationMs: 1_000, inputTokens: 888 }),
    ev('issue.completed', { nodeKey: 'test', attempt: 1, durationMs: 1_000, inputTokens: 888 }),
  ];
  const r = scoreDelivery('1', events);
  assert.equal(r.score, 5);
  assert.equal(r.metrics.totalTokens, 888 * 3);
  assert.equal(r.metrics.tokensEstimated, true);
  assert.match(r.reasons.join(), /估算口径/);
});

test('人工等待超 30 分钟额外扣分；总耗时超 60 分钟扣分', () => {
  const events = [
    ev('issue.node.advanced', { from: 'plan', to: 'code', attempt: 1, durationMs: 40 * 60 * 1000, tokensConsumed: 1 }),
    ev('hitl.entered', { nodeKey: 'code', inferred: false }),
    ev('hitl.resolved', { nodeKey: 'code', waitMs: 31 * 60 * 1000, inferredEntry: false }),
    ev('issue.node.advanced', { from: 'code', to: 'test', attempt: 1, durationMs: 25 * 60 * 1000, tokensConsumed: 1 }),
    ev('issue.completed', { nodeKey: 'test', attempt: 1, durationMs: 1_000, tokensConsumed: 1 }),
  ];
  const r = scoreDelivery('1', events);
  // HITL 1 次 1 分 + 等待超时 1 分 + 总耗时 65min 落在 [60,120) 扣 1 分 = 2 分
  assert.equal(r.score, 2, `reasons: ${r.reasons.join('; ')}`);
});

test('证据包：事件按 seq 排序、带评分与冻结元数据，score 与 metrics 同源', () => {
  const shuffled = [...cleanFlow()].reverse();
  const bundle = buildEvidence('1', shuffled, undefined, () => '2026-09-14T00:00:00.000Z');
  assert.equal(bundle.schemaVersion, 1);
  assert.equal(bundle.exportedAt, '2026-09-14T00:00:00.000Z');
  const seqs = bundle.events.map((e) => e.seq);
  assert.deepEqual(seqs, [...seqs].sort((a, b) => a - b), '证据包事件必须按 seq 升序');
  assert.equal(bundle.score.score, 5);
  assert.equal(bundle.metrics, bundle.score.metrics);
  assert.equal(bundle.issueSnapshot, undefined);

  const m = summarize('1', shuffled);
  assert.equal(m.nodeCount, 3);
});
