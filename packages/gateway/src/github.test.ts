import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createGithubClient, detectNode, type ApiCallRecord } from './github.js';
import type { GatewayConfig } from './config.js';

const pipeline = ['plan', 'code', 'test'];

const issue = (labels: string[]) => ({
  number: 1,
  title: 't',
  body: 'b',
  labels,
  state: 'open' as const,
});

test('detectNode: 无 node 标签 → 首个节点且 isNew', () => {
  const r = detectNode(issue([]), pipeline);
  assert.deepEqual(r, { current: 'plan', index: 0, isNew: true, suspended: false });
});

test('detectNode: 人工标签不干扰推导', () => {
  const r = detectNode(issue(['bug', 'p0', 'node:code']), pipeline);
  assert.deepEqual(r, { current: 'code', index: 1, isNew: false, suspended: false });
});

/**
 * 回归：熔断打上 hitl:waiting 后，调度必须把该 issue 识别为「已挂起」。
 *
 * 此前 detectNode 只认 node: 前缀，挂起标签被无视 → current 仍是失败节点
 * → 下一轮 tick 重新派发 → 失败 → 再熔断，无限循环持续烧 token，
 * 恰好抵消熔断本身的价值。
 */
test('detectNode: hitl:waiting → suspended 为真且不推导出节点', () => {
  const r = detectNode(issue(['hitl:waiting', 'node:test']), pipeline);
  assert.equal(r.suspended, true);
  assert.equal(r.current, null, '挂起态仍推导出节点，会被重复派发');
  assert.equal(r.isNew, false, '挂起态不应被当成新接入 issue 重新打标签');
});

test('detectNode: 任意 hitl: 前缀标签都算挂起（放行前的中间态）', () => {
  assert.equal(detectNode(issue(['hitl:review']), pipeline).suspended, true);
});

test('detectNode: 无标签的新 issue 不算挂起', () => {
  assert.equal(detectNode(issue([]), pipeline).suspended, false);
});

test('detectNode: done 标签 → current 为 null 且 index 越过末尾', () => {
  const r = detectNode(issue(['node:done']), pipeline);
  assert.equal(r.current, null);
  assert.equal(r.index, pipeline.length);
  assert.equal(r.isNew, false);
  assert.equal(r.suspended, false);
});

test('detectNode: done 与 node 标签共存时以 done 优先', () => {
  const r = detectNode(issue(['node:plan', 'node:done']), pipeline);
  assert.equal(r.current, null);
});

test('detectNode: 未知节点标签 → current 归 null、index -1，调度应跳过', () => {
  const r = detectNode(issue(['node:deploy']), pipeline);
  // 不在 pipeline 中的标签：current 归 null（不派发未知节点），index 保留 -1
  assert.equal(r.current, null);
  assert.equal(r.index, -1);
  assert.equal(r.isNew, false);
});

test('detectNode: 空 pipeline 且无标签 → current 为 null', () => {
  const r = detectNode(issue([]), []);
  assert.deepEqual(r, { current: null, index: 0, isNew: true, suspended: false });
});

const realConfig = {
  github: { mode: 'real' as const, token: 't', owner: 'o', repo: 'r', pollIntervalMs: 1 },
} as unknown as GatewayConfig;

test('openapi 埋点：成功调用记录 method/status/attempts/latency', async () => {
  const orig = globalThis.fetch;
  const records: ApiCallRecord[] = [];
  globalThis.fetch = (async () => new Response('[]', { status: 200 })) as typeof fetch;
  try {
    const client = createGithubClient(realConfig, (r) => records.push(r));
    const issues = await client.listIssues();
    assert.deepEqual(issues, []);
    assert.equal(records.length, 1);
    assert.equal(records[0].ok, true);
    assert.equal(records[0].status, 200);
    assert.equal(records[0].attempts, 1);
    assert.equal(records[0].method, 'GET');
    assert.match(records[0].path, /issues/);
    assert.equal(records[0].error, undefined);
  } finally {
    globalThis.fetch = orig;
  }
});

test('openapi 埋点：POST 404 不重试，失败记录带状态码与错误摘要', async () => {
  const orig = globalThis.fetch;
  const records: ApiCallRecord[] = [];
  globalThis.fetch = (async () => new Response('not found', { status: 404 })) as typeof fetch;
  try {
    const client = createGithubClient(realConfig, (r) => records.push(r));
    await assert.rejects(() => client.addComment(1, 'x'), /404/);
    assert.equal(records.length, 1);
    assert.equal(records[0].ok, false);
    assert.equal(records[0].status, 404);
    assert.equal(records[0].attempts, 1, '非幂等请求不应重试');
    assert.match(records[0].error ?? '', /404/);
  } finally {
    globalThis.fetch = orig;
  }
});
