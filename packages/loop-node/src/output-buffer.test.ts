import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createOutputBuffer } from './output-buffer.js';

/**
 * 有界缓冲测试。
 *
 * 守的是「worker 内存恒定有界」这条性质：agent 输出可能达数百 MB，
 * 无上限累积会撑爆执行节点，且终态 run.result 要把整个缓冲塞进一个 WS 帧。
 * 此前这段逻辑内联在 handleLaunch 里，无法单测；抽成独立模块后才能锁定行为。
 */

test('未超上限时原样累积，truncated 为 false', () => {
  const b = createOutputBuffer({ max: 100, keep: 20 });
  b.append('hello ');
  b.append('world');
  assert.equal(b.value, 'hello world');
  assert.equal(b.truncated, false);
});

test('恰好等于上限时不裁剪（边界）', () => {
  const b = createOutputBuffer({ max: 10, keep: 4 });
  b.append('0123456789');
  assert.equal(b.value.length, 10);
  assert.equal(b.truncated, false, '等于上限不该触发裁剪');
});

test('超过上限时裁剪为尾部 keep，并标记 truncated', () => {
  const b = createOutputBuffer({ max: 10, keep: 4 });
  b.append('0123456789ABCD');
  assert.equal(b.value, 'ABCD', '应保留尾部而非头部');
  assert.equal(b.value.length, 4);
  assert.equal(b.truncated, true);
});

test('裁剪后长度恒定有界：持续追加不会无限增长', () => {
  const b = createOutputBuffer({ max: 1000, keep: 100 });
  // 模拟长会话：600 块 × 1000 字符 = 600KB 原始输出
  for (let i = 0; i < 600; i++) {
    b.append(String(i).padStart(1000, 'x'));
  }
  // 关键断言：缓冲长度不得超过 max（而非 600KB）
  assert.ok(
    b.value.length <= 1000,
    `缓冲长度 ${b.value.length} 超过上限 1000 —— 有界性失效`,
  );
  assert.equal(b.truncated, true);
});

test('保留的是尾部内容（「尾部即结论」前提）', () => {
  const b = createOutputBuffer({ max: 20, keep: 10 });
  b.append('HEAD-xxxxxx'); // 11 字符
  b.append('TAIL-END!!'); // 10 字符 → 合计 21，超 max → 裁到尾部 10 = 'TAIL-END!!'
  // 注意：value 是 getter，不能用 assert.equal(b.value, '字面量') 连续断言 ——
  // TS 的控制流分析会把两次期望取交集收窄成 never，后续 .endsWith 就报
  // 「Property does not exist on type 'never'」。先取到局部变量再断言。
  const afterSecond: string = b.value;
  assert.equal(afterSecond, 'TAIL-END!!');
  assert.equal(b.truncated, true);

  b.append('Z'); // 11 字符，未超 max，不再裁剪
  const afterThird: string = b.value;
  assert.equal(afterThird, 'TAIL-END!!Z');
  assert.ok(afterThird.endsWith('Z'), '最后一块输出必须保留');
  assert.ok(!afterThird.includes('HEAD'), '头部内容应已被丢弃');
  // 即使当前缓冲看起来「完整」，中段确实丢过内容，标记不应回落
  assert.equal(b.truncated, true);
});

test('truncated 一旦置真不再回落', () => {
  const b = createOutputBuffer({ max: 10, keep: 4 });
  b.append('0123456789ABCDE'); // 触发截断
  assert.equal(b.truncated, true);
  b.append('xy'); // 后续小块追加
  assert.equal(b.truncated, true, '截断状态应持续到 run 结束，供 error 提示使用');
});

test('空串追加是 no-op，不影响缓冲与标记', () => {
  const b = createOutputBuffer({ max: 10, keep: 4 });
  b.append('abc');
  b.append('');
  assert.equal(b.value, 'abc');
  assert.equal(b.truncated, false);
});

test('keep >= max 时不裁剪（配置不合理时退化为无界累积而非产生负长度）', () => {
  const b = createOutputBuffer({ max: 4, keep: 10 });
  b.append('0123456789');
  // keep > max 时 slice(-10) 对 10 字符串是原样返回，裁剪无意义，故跳过
  assert.equal(b.value, '0123456789');
  assert.equal(b.truncated, false);
});

test('单块即超上限：只保留该块尾部', () => {
  const b = createOutputBuffer({ max: 10, keep: 3 });
  b.append('x'.repeat(5000));
  assert.equal(b.value.length, 3);
  assert.equal(b.truncated, true);
});

test('多次小块累积跨过上限：跨块内容被正确保留', () => {
  const b = createOutputBuffer({ max: 6, keep: 4 });
  b.append('ab');
  b.append('cd');
  b.append('ef'); // 此时 6 字符，未超
  assert.equal(b.value, 'abcdef');
  b.append('gh'); // 8 字符，超上限 → 裁到尾部 4
  assert.equal(b.value, 'efgh', '应跨块保留尾部 4 字符');
});
