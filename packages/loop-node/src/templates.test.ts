import { test } from 'node:test';
import assert from 'node:assert/strict';
import { parseRetry } from './templates.js';
import { DEFAULT_RETRY } from '@harness/shared';

/**
 * 这组测试守的是「模板声明驱动重试策略」这条链路。
 *
 * 核心风险不是解析不出来，而是**解析出 undefined 却不报错**：
 * 模板由业务方手写、不在编译期检查范围内，一个拼错的字段名
 * （max_attempt 少个 s）会让 policy.maxAttempts 变成 undefined，
 * 控制面 `attempt >= undefined` 恒为 false —— 熔断静默失效，
 * 节点无限重跑烧 token，且全程无报错。因此必须逐字段回退默认值。
 */

test('parseRetry: 未声明 retry 段时取全局默认值', () => {
  assert.deepEqual(parseRetry(undefined), DEFAULT_RETRY);
  assert.deepEqual(parseRetry(null), DEFAULT_RETRY);
  assert.deepEqual(parseRetry('not-an-object'), DEFAULT_RETRY);
});

test('parseRetry: 完整声明时逐项生效', () => {
  const r = parseRetry({
    max_attempts: 5,
    max_tokens: 1_000_000,
    max_duration_ms: 60_000,
    on_exhausted: 'skip',
  });
  assert.deepEqual(r, {
    maxAttempts: 5,
    maxTokens: 1_000_000,
    maxDurationMs: 60_000,
    onExhausted: 'skip',
  });
});

test('parseRetry: 部分声明时缺失项回退默认，不产生 undefined', () => {
  const r = parseRetry({ max_attempts: 7 });
  assert.equal(r.maxAttempts, 7);
  assert.equal(r.maxTokens, DEFAULT_RETRY.maxTokens);
  assert.equal(r.maxDurationMs, DEFAULT_RETRY.maxDurationMs);
  assert.equal(r.onExhausted, DEFAULT_RETRY.onExhausted);
  // 关键：任何字段都不允许是 undefined，否则熔断比较会静默失效
  for (const [k, v] of Object.entries(r)) {
    assert.notEqual(v, undefined, `${k} 为 undefined 会让熔断判定静默失效`);
  }
});

test('parseRetry: 字段名拼错时回退默认值（回归：静默失效风险）', () => {
  // 少个 s / 用驼峰而非下划线 —— 都应回退而不是产出 undefined
  const r = parseRetry({ max_attempt: 9, maxAttempts: 9, onExhausted: 'fail' });
  assert.equal(r.maxAttempts, DEFAULT_RETRY.maxAttempts, '拼错的字段名不应被采纳');
  assert.equal(r.onExhausted, DEFAULT_RETRY.onExhausted, '驼峰写法不应被采纳');
});

test('parseRetry: 非法数值（0 / 负数 / 小数 / NaN / 非数字）回退默认', () => {
  for (const bad of [0, -1, NaN, 'abc', {}, []]) {
    const r = parseRetry({ max_attempts: bad, max_tokens: bad });
    assert.equal(r.maxAttempts, DEFAULT_RETRY.maxAttempts, `max_attempts=${String(bad)} 应回退`);
    assert.equal(r.maxTokens, DEFAULT_RETRY.maxTokens, `max_tokens=${String(bad)} 应回退`);
  }
  // 小数向下取整：次数 / 字节数必须是整数
  assert.equal(parseRetry({ max_attempts: 3.9 }).maxAttempts, 3);
});

test('parseRetry: on_exhausted 只接受枚举值，非法值回退 hitl', () => {
  for (const v of ['hitl', 'fail', 'skip']) {
    assert.equal(parseRetry({ on_exhausted: v }).onExhausted, v);
  }
  for (const bad of ['HITL', 'abort', '', null, 0]) {
    assert.equal(
      parseRetry({ on_exhausted: bad }).onExhausted,
      DEFAULT_RETRY.onExhausted,
      `on_exhausted=${String(bad)} 应回退 hitl`,
    );
  }
});

test('parseRetry: 每次返回新对象，不共享全局默认值的引用', () => {
  const a = parseRetry(undefined);
  const b = parseRetry(undefined);
  assert.notEqual(a, b, '默认值被共享引用时，一处修改会污染全局默认');
  a.maxAttempts = 99;
  assert.equal(b.maxAttempts, DEFAULT_RETRY.maxAttempts);
  assert.equal(DEFAULT_RETRY.maxAttempts, 3, '全局默认值被意外修改');
});
