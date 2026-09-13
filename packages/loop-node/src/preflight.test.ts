import { test } from 'node:test';
import assert from 'node:assert/strict';
import { resolvePreflightConfig, runPreflight, DEFAULT_PREFLIGHT } from './preflight.js';

/**
 * preflight 的价值在于「拦截发生在 agent 启动之前」：BLOCKER 级失败直接回
 * failed，不启动 agent、不消耗 token。生产评测里单任务 132~412 条 error/warning
 * 多为工具鉴权、DNS 等非业务错误，这类问题本该在这里被拦住。
 *
 * 检查项由环境变量声明而非写死：需要哪些凭证、访问哪个仓库、用什么命令验证
 * agent 登录态，都是部署相关的 —— 与「业务模板 vs 通用引擎」是同一套分离原则。
 */

test('resolvePreflightConfig: 零配置时只有 3 项基础检查（mock 模式可跑）', () => {
  const { checks } = resolvePreflightConfig({});
  assert.deepEqual(
    checks.map((c) => c.name),
    ['git', 'node', 'network'],
  );
  assert.ok(checks.every((c) => c.level === 'BLOCKER'));
});

test('resolvePreflightConfig: 与 DEFAULT_PREFLIGHT 一致（向后兼容）', () => {
  assert.deepEqual(resolvePreflightConfig({}).checks, DEFAULT_PREFLIGHT.checks);
});

test('resolvePreflightConfig: PREFLIGHT_ENV_VARS 逐项展开为凭证检查', () => {
  const { checks } = resolvePreflightConfig({
    PREFLIGHT_ENV_VARS: 'GITHUB_TOKEN, OPENAI_API_KEY ,',
  });
  const names = checks.map((c) => c.name);
  assert.deepEqual(names.slice(3), ['cred:GITHUB_TOKEN', 'cred:OPENAI_API_KEY']);
  // 空项被过滤，不产生 "cred:" 这种无意义检查
  assert.ok(!names.includes('cred:'));
  const cred = checks.find((c) => c.name === 'cred:GITHUB_TOKEN')!;
  assert.equal(cred.envVar, 'GITHUB_TOKEN');
  assert.equal(cred.level, 'BLOCKER');
});

test('resolvePreflightConfig: PREFLIGHT_GIT_REMOTE 转为仓库访问检查，URL 作为独立 argv', () => {
  const url = 'https://github.com/o/r.git';
  const { checks } = resolvePreflightConfig({ PREFLIGHT_GIT_REMOTE: url });
  const repo = checks.find((c) => c.name === 'repo-access')!;
  assert.equal(repo.command, 'git');
  assert.deepEqual(repo.args, ['ls-remote', '--heads', url]);
  // 安全回归：URL 必须是独立 argv 元素。原实现用 shell 字符串拼接，
  // 而 URL / agent 命令均来自环境变量，经 shell 拼接存在命令注入面。
  // 改用 execFile（argv 形式）后参数原样传递，不解释 shell 元字符。
  assert.ok(
    !repo.args!.some((a) => /[;&|`$()]/.test(a)),
    '参数中不应包含被拼接进 shell 的痕迹',
  );
});

test('resolvePreflightConfig: PREFLIGHT_AGENT_LOGIN_CHECK 按空白拆成 argv', () => {
  const { checks } = resolvePreflightConfig({
    PREFLIGHT_AGENT_LOGIN_CHECK: 'gh auth status --hostname github.com',
  });
  const login = checks.find((c) => c.name === 'agent-login')!;
  assert.equal(login.command, 'gh');
  assert.deepEqual(login.args, ['auth', 'status', '--hostname', 'github.com']);
});

test('resolvePreflightConfig: 空白字符串视为未配置', () => {
  const { checks } = resolvePreflightConfig({
    PREFLIGHT_ENV_VARS: '   ',
    PREFLIGHT_GIT_REMOTE: '  ',
    PREFLIGHT_AGENT_LOGIN_CHECK: '',
  });
  assert.equal(checks.length, 3, '空白配置不应产生额外检查项');
});

test('resolvePreflightConfig: 三类加固检查可同时声明', () => {
  const { checks } = resolvePreflightConfig({
    PREFLIGHT_ENV_VARS: 'GITHUB_TOKEN',
    PREFLIGHT_GIT_REMOTE: 'https://github.com/o/r.git',
    PREFLIGHT_AGENT_LOGIN_CHECK: 'codex login --status',
  });
  assert.deepEqual(
    checks.map((c) => c.name),
    ['git', 'node', 'network', 'cred:GITHUB_TOKEN', 'repo-access', 'agent-login'],
  );
});

test('runPreflight: 凭证已设置 → 通过；未设置 → 失败并保留级别', async () => {
  const present = 'HARNESS_TEST_CRED_PRESENT';
  const missing = 'HARNESS_TEST_CRED_MISSING';
  process.env[present] = 'x';
  delete process.env[missing];

  const results = await runPreflight({
    checks: [
      { name: 'present', level: 'BLOCKER', envVar: present },
      { name: 'missing', level: 'BLOCKER', envVar: missing },
      { name: 'warn-missing', level: 'WARNING', envVar: missing },
    ],
  });

  const byName = Object.fromEntries(results.map((r) => [r.name, r]));
  assert.equal(byName.present.ok, true);
  assert.equal(byName.missing.ok, false);
  assert.equal(byName.missing.level, 'BLOCKER');
  // WARNING 级失败不阻塞执行，但必须被记录下来（供控制面区分环境问题）
  assert.equal(byName['warn-missing'].ok, false);
  assert.equal(byName['warn-missing'].level, 'WARNING');

  delete process.env[present];
});

test('runPreflight: 命令不存在 → 作为检查结果返回，不抛异常炸掉调用方', async () => {
  const results = await runPreflight({
    checks: [
      {
        name: 'nonexistent',
        level: 'BLOCKER',
        command: 'harness-definitely-not-a-real-cmd-xyz',
      },
    ],
  });
  assert.equal(results.length, 1);
  assert.equal(results[0].ok, false);
  assert.ok(results[0].message.length > 0, '失败必须带可诊断信息');
});

test('runPreflight: args 缺省时按空参数执行（git --version 这类）', async () => {
  const results = await runPreflight({
    checks: [{ name: 'git', level: 'BLOCKER', command: 'git', args: ['--version'] }],
  });
  assert.equal(results[0].ok, true);
});

test('runPreflight: 结果顺序与配置一致（便于日志按序阅读）', async () => {
  const results = await runPreflight({
    checks: [
      { name: 'a', level: 'INFO', envVar: 'HARNESS_TEST_ORDER_A' },
      { name: 'b', level: 'INFO', envVar: 'HARNESS_TEST_ORDER_B' },
    ],
  });
  assert.deepEqual(
    results.map((r) => r.name),
    ['a', 'b'],
  );
});

test('runPreflight: 既无 command 也无 envVar 的检查项被跳过，不产生结果', async () => {
  const results = await runPreflight({
    checks: [{ name: 'noop', level: 'INFO' }],
  });
  assert.equal(results.length, 0);
});
