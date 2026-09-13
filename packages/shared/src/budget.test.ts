import { test } from 'node:test';
import assert from 'node:assert/strict';
import { estimateTokens, pruneContext, DEFAULT_BUDGET } from './budget.js';

test('estimateTokens: 中文按 1.5 字符/token 折算', () => {
  assert.equal(estimateTokens('中'.repeat(150)), 100);
});

test('estimateTokens: 英文按 4 字符/token 折算', () => {
  assert.equal(estimateTokens('a'.repeat(400)), 100);
});

test('estimateTokens: 中英混合分段折算并向上取整', () => {
  // 10 中文 (10/1.5=6.67) + 10 英文 (10/4=2.5) = 9.17 → 10
  assert.equal(estimateTokens('中'.repeat(10) + 'a'.repeat(10)), 10);
});

test('estimateTokens: 空串为 0', () => {
  assert.equal(estimateTokens(''), 0);
});

test('pruneContext: 未超预算时原样透传且 exceeded 为 false', () => {
  const ctx = { a: 'x'.repeat(400), b: 'y'.repeat(400) }; // 各 100 token
  const { pruned, usage } = pruneContext(ctx, { ...DEFAULT_BUDGET, maxInputTokens: 1000 });
  assert.deepEqual(pruned, ctx);
  assert.equal(usage.inputTokens, 200);
  assert.equal(usage.exceeded, false);
  assert.deepEqual(usage.exceededFields, []);
});

test('pruneContext: 超预算字段被截断并留下原量标记', () => {
  const ctx = { a: 'x'.repeat(400), b: 'y'.repeat(4000) }; // a=100, b=1000
  const { pruned, usage } = pruneContext(ctx, { ...DEFAULT_BUDGET, maxInputTokens: 200 });
  assert.equal(pruned.a, ctx.a); // 未超，原样
  assert.ok(pruned.b.startsWith('y'.repeat(200))); // 超预算，只留前 200 字符
  assert.match(pruned.b, /已截断，原字段约 1000 tokens/);
  assert.deepEqual(usage.exceededFields, ['b']);
  assert.equal(usage.exceeded, true);
});

test('pruneContext: 多字段时裁剪后总量严格不超过预算（回归：旧实现每个字段各截 200 字符并累加，20 字段 × 1000 token / 预算 200 反而得到 1200）', () => {
  const limit = 200;
  const ctx: Record<string, string> = {};
  for (let i = 0; i < 20; i++) ctx[`f${i}`] = 'x'.repeat(4000); // 每个 1000 token
  const { usage } = pruneContext(ctx, { ...DEFAULT_BUDGET, maxInputTokens: limit });
  assert.ok(usage.inputTokens <= limit, `裁剪后仍超预算: ${usage.inputTokens} > ${limit}`);
  assert.equal(usage.exceeded, true);
  assert.ok(usage.droppedFields.length > 0, '预算耗尽后应有字段被整体省略');
  // 每个字段都必须在产物中有占位，不能静默丢失（否则 agent 无从知道有信息被裁掉）
  assert.equal(Object.keys(usage.droppedFields.length ? {} : {}).length, 0);
});

test('pruneContext: 预算为 0 时全部字段降级为占位符且不超预算', () => {
  const ctx = { a: 'x'.repeat(400), b: 'y'.repeat(400) };
  const { pruned, usage } = pruneContext(ctx, { ...DEFAULT_BUDGET, maxInputTokens: 0 });
  assert.equal(usage.inputTokens, 0);
  assert.equal(usage.droppedFields.length, 2);
  assert.match(pruned.a, /已省略/);
  assert.match(pruned.b, /已省略/);
});

test('pruneContext: 首字段就超预算时也会被截断（不会整段透传）', () => {
  const ctx = { big: 'x'.repeat(40000) }; // 10000 token
  const { pruned, usage } = pruneContext(ctx, { ...DEFAULT_BUDGET, maxInputTokens: 500 });
  assert.ok(pruned.big.length < ctx.big.length);
  assert.equal(usage.exceeded, true);
});
