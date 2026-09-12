#!/usr/bin/env node
/**
 * 无需真实 Agent CLI 的模拟器。
 * 读入 prompt 文件，模拟流式输出，最后输出一段 ARTIFACT JSON。
 *
 * 换成真实 Agent 时，只要保证：
 *   - 接收 prompt 文件路径作为最后一个参数
 *   - 把过程输出打到 stdout
 *   - 退出码 0 表示成功
 */
import fs from 'node:fs/promises';

const promptFile = process.argv[2];
if (!promptFile) {
  console.error('usage: mock-agent <prompt-file>');
  process.exit(2);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const prompt = await fs.readFile(promptFile, 'utf8');

process.stdout.write('\x1b[36m[mock-agent]\x1b[0m 启动，prompt 长度 = ' + prompt.length + '\n');
await sleep(300);

const lines = prompt.split('\n').slice(0, 24);
for (const line of lines) {
  process.stdout.write('\x1b[90m>\x1b[0m ' + line + '\n');
  await sleep(40);
}

await sleep(400);
process.stdout.write('\x1b[32m[mock-agent]\x1b[0m 执行完成\n');
process.stdout.write('\n=== ARTIFACT ===\n');
process.stdout.write(
  JSON.stringify(
    {
      status: 'ok',
      summary: `mock agent 处理了 ${prompt.length} 字符的 prompt`,
      generatedAt: new Date().toISOString(),
    },
    null,
    2,
  ),
);
process.stdout.write('\n');
process.exit(0);
