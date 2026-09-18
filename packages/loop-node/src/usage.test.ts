import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { collectUsage } from './usage.js';

/** 临时目录沙箱：每个用例独立的假 run 工作目录；corrupted 追加一个损坏文件验证容错 */
function makeWorkdir(trajectories: object[], opts: { corrupted?: boolean } = {}): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'usage-test-'));
  const tdir = path.join(dir, 'trajectories');
  fs.mkdirSync(tdir);
  trajectories.forEach((t, i) =>
    fs.writeFileSync(path.join(tdir, `t${i}.json`), JSON.stringify(t)),
  );
  if (opts.corrupted) fs.writeFileSync(path.join(tdir, 'broken.json'), '{corrupted');
  return dir;
}

test('collectUsage: 多个 trajectory 文件的 usage 全部累加', () => {
  const dir = makeWorkdir([
    { llm_interactions: [{ response: { usage: { input_tokens: 100, output_tokens: 10 } } }] },
    { llm_interactions: [{ response: { usage: { input_tokens: 200, output_tokens: 20 } } }, { response: { usage: { input_tokens: 50, output_tokens: 5 } } }] },
  ]);
  assert.deepEqual(collectUsage(dir), { input: 350, output: 35 });
});

test('collectUsage: 损坏文件被跳过，不阻塞其余文件采集', () => {
  const dir = makeWorkdir(
    [{ llm_interactions: [{ response: { usage: { input_tokens: 100, output_tokens: 10 } } }] }],
    { corrupted: true },
  );
  assert.deepEqual(collectUsage(dir), { input: 100, output: 10 });
});

test('collectUsage: 无 trajectories 目录返回 undefined（mock agent / 启动前失败）', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'usage-test-'));
  assert.equal(collectUsage(dir), undefined);
});

test('collectUsage: 有目录但全无 usage（空交互）返回 undefined', () => {
  const dir = makeWorkdir([{ llm_interactions: [{ response: {} }] }]);
  assert.equal(collectUsage(dir), undefined);
});
