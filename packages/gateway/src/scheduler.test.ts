import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Scheduler, type Run } from './scheduler.js';
import type { HarnessEvent, RetryPolicy } from '@harness/shared';
import type { GithubClient, Issue } from './github.js';
import { CircuitBreaker, DEFAULT_RETRY } from './circuit-breaker.js';

/**
 * 这组测试的核心目的是守住一个曾经真实存在的 bug：
 * issue 去重锁在 onRunResult 开头就被释放，早于 GitHub 回写。
 * 由于调度是「轮询标签」驱动的，锁提前消失 + 标签尚未推进
 * = 下一轮 tick 把同一节点重复派发。
 * 现在锁必须在回写结束后才释放，以下测试锁定该行为。
 */

const pipeline = ['plan', 'code', 'test'];

function makeConfig() {
  return {
    httpPort: 0,
    dbPath: '',
    projectId: 'test',
    workerToken: 't',
    pipeline,
    github: { mode: 'mock' as const, pollIntervalMs: 5000 },
    worker: { pingIntervalMs: 15_000, staleMs: 45_000 },
    feishu: {},
    alerts: { hitlEscalateMs: 0 },
  };
}

function makeStore() {
  let seq = 0;
  return {
    append(input: Omit<HarnessEvent, 'id' | 'seq' | 'timestamp'>): HarnessEvent {
      return {
        id: `ev_${++seq}`,
        seq,
        timestamp: new Date().toISOString(),
        ...input,
      };
    },
  };
}

function makeBus(sink: HarnessEvent[] = []) {
  return {
    events: sink,
    emitEvent(ev: HarnessEvent) {
      sink.push(ev);
    },
    onEvent() {
      return () => {};
    },
  };
}

interface GithubMock extends GithubClient {
  calls: string[];
  comments: string[];
  labels: string[][];
}

function makeGithub(issues: Issue[], opts: { failOn?: string } = {}): GithubMock {
  const calls: string[] = [];
  const comments: string[] = [];
  const labels: string[][] = [];
  const boom = () => {
    throw new Error('github write failed');
  };
  return {
    calls,
    comments,
    labels,
    async listIssues() {
      calls.push('listIssues');
      if (opts.failOn === 'listIssues') boom();
      return issues;
    },
    async addComment(n, body) {
      calls.push('addComment');
      if (opts.failOn === 'addComment') boom();
      comments.push(`#${n}: ${body}`);
    },
    async setLabels(n, l) {
      calls.push('setLabels');
      if (opts.failOn === 'setLabels') boom();
      labels.push(l);
    },
    async closeIssue() {
      calls.push('closeIssue');
      if (opts.failOn === 'closeIssue') boom();
    },
  };
}

function makeWorker(nodeId = 'w1') {
  const sent: string[] = [];
  return {
    sent,
    conn: {
      nodeId,
      socket: {
        readyState: 1,
        send(s: string) {
          sent.push(s);
        },
      },
      agents: ['mock'],
      busy: false,
      lastSeen: Date.now(),
    } as never,
  };
}

function makeScheduler(issues: Issue[], opts: { failOn?: string } = {}) {
  const sink: HarnessEvent[] = [];
  const github = makeGithub(issues, opts);
  const scheduler = new Scheduler({
    config: makeConfig(),
    store: makeStore() as never,
    bus: makeBus(sink) as never,
    github,
    circuitBreaker: new CircuitBreaker(),
  });
  return { scheduler, github, sink };
}

const openIssue = (n = 1, labels = ['node:plan']): Issue => ({
  number: n,
  title: '实现登录接口',
  body: '需要 JWT',
  labels,
  state: 'open',
});

/** 取出已派发的 run（借道 listRuns，它是锁状态的直接投影）。 */
function currentRun(scheduler: Scheduler): Run | undefined {
  return scheduler.listRuns()[0];
}

test('锁在 GitHub 回写期间仍然持有（回归：防止同一节点被重复派发）', async () => {
  const { scheduler, github } = makeScheduler([openIssue()]);
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);

  // 手动派发一个 run（不经 tick，便于精确控制时序）
  await (scheduler as never as { dispatch(w: never, i: Issue, n: string): Promise<void> }).dispatch(
    conn,
    openIssue(),
    'plan',
  );
  const runId = currentRun(scheduler)!.runId;
  assert.ok(runId);

  // 让 listIssues 挂起，模拟回写耗时窗口
  let release!: () => void;
  const gate = new Promise<void>((r) => (release = r));
  const origList = github.listIssues;
  github.listIssues = async () => {
    await gate;
    return origList();
  };

  const pending = scheduler.onRunResult(runId, 'completed', { output: 'ok' });

  // 关键断言：回写还没结束，锁必须还在
  await new Promise((r) => setTimeout(r, 10));
  assert.equal(
    scheduler.listRuns().length,
    1,
    '回写进行中锁被提前释放 —— 这会让下一轮 tick 重复派发同一节点',
  );

  release();
  await pending;

  // 回写结束后锁才释放
  assert.equal(scheduler.listRuns().length, 0, '回写完成后锁未释放');
});

test('回写成功：标签推进到下一节点，锁释放，worker 容量恢复', async () => {
  const { scheduler, github, sink } = makeScheduler([openIssue()]);
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);
  await (scheduler as never as { dispatch(w: never, i: Issue, n: string): Promise<void> }).dispatch(
    conn,
    openIssue(),
    'plan',
  );
  const runId = currentRun(scheduler)!.runId;

  await scheduler.onRunResult(runId, 'completed', { output: 'done' });

  assert.deepEqual(github.labels.at(-1), ['node:code']);
  assert.equal(scheduler.listRuns().length, 0);
  assert.equal((conn as never as { busy: boolean }).busy, false);
  assert.ok(sink.some((e) => e.event === 'issue.node.advanced'));
  assert.ok(!sink.some((e) => e.event === 'run.transition_failed'));
});

test('末节点完成：打 done 标签 + 关闭 issue（回归：closeIssue 此前从未被调用）', async () => {
  const { scheduler, github, sink } = makeScheduler([openIssue(1, ['node:test'])]);
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);
  await (scheduler as never as { dispatch(w: never, i: Issue, n: string): Promise<void> }).dispatch(
    conn,
    openIssue(1, ['node:test']),
    'test',
  );
  const runId = currentRun(scheduler)!.runId;

  await scheduler.onRunResult(runId, 'completed', { output: 'done' });

  assert.ok(github.calls.includes('closeIssue'), '流程完成后未关闭 issue，飞书卡片会谎报「已关闭」');
  assert.deepEqual(github.labels.at(-1), ['node:done']);
  assert.ok(sink.some((e) => e.event === 'issue.completed'));
});

test('观测闭环：终态事件带 attempt/endedAt/durationMs，失败事件保留 preflight 与执行面耗时；成功输出进评论', async () => {
  const { scheduler, github, sink } = makeScheduler([openIssue()]);
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);
  const dispatchPlan = () =>
    (scheduler as never as { dispatch(w: never, i: Issue, n: string): Promise<void> }).dispatch(
      conn,
      openIssue(),
      'plan',
    );

  // 第一次执行失败（模拟 preflight 环境问题），携带执行面自报耗时
  await dispatchPlan();
  await scheduler.onRunResult(currentRun(scheduler)!.runId, 'failed', {
    error: 'boom',
    durationMs: 1234,
    preflight: { passed: false, failed: ['git'] },
  });

  const failed = sink.find((e) => e.event === 'issue.node.failed')!;
  assert.equal(failed.details!.attempt, 1, '首次失败 attempt 应为 1');
  assert.equal(typeof failed.details!.endedAt, 'string');
  assert.equal(typeof failed.details!.durationMs, 'number', '失败事件缺控制面耗时');
  assert.equal(failed.details!.workerDurationMs, 1234, '执行面自报耗时未透传');
  assert.deepEqual(failed.details!.preflight, { passed: false, failed: ['git'] });

  // 同节点重派后成功：attempt = 历史失败 1 次 + 本次成功 = 2
  await dispatchPlan();
  await scheduler.onRunResult(currentRun(scheduler)!.runId, 'completed', {
    output: 'agent-final-output',
    durationMs: 4321,
  });

  const advanced = sink.find((e) => e.event === 'issue.node.advanced')!;
  assert.equal(advanced.details!.attempt, 2, '返工后成功的 attempt 应为 2');
  assert.equal(typeof advanced.details!.endedAt, 'string');
  assert.equal(typeof advanced.details!.durationMs, 'number');
  // 回归：ws.ts 曾把 output 字符串误当成 opts 对象传入，导致成功评论的输出块永远为空
  assert.ok(
    github.comments.some((c) => c.includes('agent-final-output')),
    '成功输出未写入 GitHub 评论',
  );
});

test('token 记账：transcript 真实用量优先，缺省回退注入估算；熔断累计跨失败累加', async () => {
  const { scheduler, sink } = makeScheduler([openIssue()]);
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);
  const dispatchPlan = () =>
    (scheduler as never as { dispatch(w: never, i: Issue, n: string): Promise<void> }).dispatch(
      conn,
      openIssue(),
      'plan',
    );

  // 第一次失败带 transcript 用量：tokensConsumed = input + output
  await dispatchPlan();
  await scheduler.onRunResult(currentRun(scheduler)!.runId, 'failed', {
    tokens: { input: 100, output: 20 },
  });
  const f1 = sink.filter((e) => e.event === 'issue.node.failed').at(-1)!;
  assert.equal(f1.details!.tokensConsumed, 120, '真实用量未按 input+output 记账');
  assert.deepEqual(f1.details!.tokens, { input: 100, output: 20 });

  // 第二次失败不带 tokens：回退注入估算口径
  await dispatchPlan();
  await scheduler.onRunResult(currentRun(scheduler)!.runId, 'failed', {});
  const f2 = sink.filter((e) => e.event === 'issue.node.failed').at(-1)!;
  assert.equal(f2.details!.tokensConsumed, f2.details!.inputTokens, '无 transcript 时未回退估算口径');
  assert.equal(f2.details!.tokens, undefined);

  // 成功后 advanced 事件带真实 tokens
  await dispatchPlan();
  await scheduler.onRunResult(currentRun(scheduler)!.runId, 'completed', {
    tokens: { input: 500, output: 80 },
  });
  const adv = sink.find((e) => e.event === 'issue.node.advanced')!;
  assert.deepEqual(adv.details!.tokens, { input: 500, output: 80 });
});

test('HITL 计时：熔断进入精确记录，标签摘除经 tick 差分产出 waitMs；重启后首次观察带 inferred', async () => {
  const { scheduler, sink } = makeScheduler([openIssue()]);
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);
  const dispatchPlan = () =>
    (scheduler as never as { dispatch(w: never, i: Issue, n: string): Promise<void> }).dispatch(
      conn,
      openIssue(),
      'plan',
    );
  const trackHitl = (issues: Issue[]) =>
    (scheduler as never as { trackHitl(i: Issue[]): void }).trackHitl(issues);

  // 连续失败至 DEFAULT_RETRY.maxAttempts=3 → 熔断转 HITL（onExhausted 默认 hitl）
  for (let i = 0; i < 3; i++) {
    await dispatchPlan();
    await scheduler.onRunResult(currentRun(scheduler)!.runId, 'failed', { error: 'x' });
  }
  const entered = sink.find((e) => e.event === 'hitl.entered')!;
  assert.ok(entered, '熔断转 HITL 未发 hitl.entered');
  assert.equal(entered.details!.inferred, false, '熔断路径的进入应为精确记录');
  assert.equal(entered.details!.nodeKey, 'plan');

  // 人工摘标签后的下一轮 tick：差分产出 resolved + waitMs
  trackHitl([openIssue()]);
  const resolved = sink.find((e) => e.event === 'hitl.resolved')!;
  assert.ok(resolved, '标签摘除未发 hitl.resolved');
  assert.ok((resolved.details!.waitMs as number) >= 0);
  assert.equal(resolved.details!.inferredEntry, false);

  // gateway 重启后首次观察到挂起：inferred=true，解除时 waitMs 为近似值
  trackHitl([openIssue(1, ['node:plan', 'hitl:waiting'])]);
  const entered2 = sink.filter((e) => e.event === 'hitl.entered').at(-1)!;
  assert.equal(entered2.details!.inferred, true);
  trackHitl([openIssue()]);
  const resolved2 = sink.filter((e) => e.event === 'hitl.resolved').at(-1)!;
  assert.equal(resolved2.details!.inferredEntry, true);
});

test('HITL 超时升级：超过阈值只发一次 hitl.escalated；阈值 0 时不发', async () => {
  const sink: HarnessEvent[] = [];
  const scheduler = new Scheduler({
    config: { ...makeConfig(), alerts: { hitlEscalateMs: 5 } },
    store: makeStore() as never,
    bus: makeBus(sink) as never,
    github: makeGithub([]) as never,
    circuitBreaker: new CircuitBreaker(),
  });
  const trackHitl = (issues: Issue[]) =>
    (scheduler as never as { trackHitl(i: Issue[]): void }).trackHitl(issues);
  const waiting = () => [openIssue(1, ['node:plan', 'hitl:waiting'])];

  trackHitl(waiting());
  await new Promise((r) => setTimeout(r, 8));
  trackHitl(waiting());
  trackHitl(waiting()); // 仍挂起：不得重复升级

  const escalations = sink.filter((e) => e.event === 'hitl.escalated');
  assert.equal(escalations.length, 1);
  assert.ok((escalations[0].details!.waitMs as number) >= 5);

  // 阈值 0（禁用）：不发升级
  const sink2: HarnessEvent[] = [];
  const scheduler2 = new Scheduler({
    config: { ...makeConfig(), alerts: { hitlEscalateMs: 0 } },
    store: makeStore() as never,
    bus: makeBus(sink2) as never,
    github: makeGithub([]) as never,
    circuitBreaker: new CircuitBreaker(),
  });
  (scheduler2 as never as { trackHitl(i: Issue[]): void }).trackHitl(waiting());
  await new Promise((r) => setTimeout(r, 5));
  (scheduler2 as never as { trackHitl(i: Issue[]): void }).trackHitl(waiting());
  assert.ok(!sink2.some((e) => e.event === 'hitl.escalated'));
});

test('回写抛错：不炸进程、发 transition_failed、锁仍然释放以便按标签重放', async () => {
  const { scheduler, sink } = makeScheduler([openIssue()], { failOn: 'addComment' });
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);
  await (scheduler as never as { dispatch(w: never, i: Issue, n: string): Promise<void> }).dispatch(
    conn,
    openIssue(),
    'plan',
  );
  const runId = currentRun(scheduler)!.runId;

  // 不应抛出（此前异常会上抛到 ws.ts 被静默吞掉）
  await scheduler.onRunResult(runId, 'completed', { output: 'done' });

  const ev = sink.find((e) => e.event === 'run.transition_failed');
  assert.ok(ev, '回写失败未发出 run.transition_failed 事件（notifier 死分支）');
  assert.match(String(ev!.details?.error), /github write failed/);
  assert.equal(scheduler.listRuns().length, 0, '回写失败后锁未释放，issue 会被永久锁死');
});

test('未知 runId 的结果被丢弃，不影响现有锁', async () => {
  const { scheduler } = makeScheduler([openIssue()]);
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);
  await (scheduler as never as { dispatch(w: never, i: Issue, n: string): Promise<void> }).dispatch(
    conn,
    openIssue(),
    'plan',
  );

  await scheduler.onRunResult('run_unknown', 'completed', { output: 'x' });

  assert.equal(scheduler.listRuns().length, 1, '迟到的 run.result 误清了在跑 run 的锁');
});

test('worker 断线释放其持有 run 的锁并记录 worker_lost', async () => {
  const { scheduler, sink } = makeScheduler([openIssue()]);
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);
  await (scheduler as never as { dispatch(w: never, i: Issue, n: string): Promise<void> }).dispatch(
    conn,
    openIssue(),
    'plan',
  );
  assert.equal(scheduler.listRuns().length, 1);

  scheduler.unregisterWorker('w1');

  assert.equal(scheduler.listRuns().length, 0, 'worker 断线后锁未回收，issue 会被永久锁死');
  assert.ok(sink.some((e) => e.event === 'run.worker_lost'));
});

test('worker 断线时若无在跑任务，不产生 worker_lost 噪声事件', () => {
  const { scheduler, sink } = makeScheduler([openIssue()]);
  scheduler.registerWorker(makeWorker().conn);

  scheduler.unregisterWorker('w1');

  assert.equal(sink.length, 0);
});

test('熔断计数语义：maxAttempts=N 时第 N 次失败即熔断（回归：旧实现 off-by-one，需 N+1 次）', () => {
  const cb = new CircuitBreaker();
  const policy: typeof DEFAULT_RETRY = { ...DEFAULT_RETRY, maxAttempts: 3 };

  assert.equal(cb.check('k', policy, 10).allowed, true); // 第 1 次失败
  assert.equal(cb.check('k', policy, 10).allowed, true); // 第 2 次失败
  const third = cb.check('k', policy, 10); // 第 3 次失败
  assert.equal(third.allowed, false, 'maxAttempts=3 时第 3 次失败必须熔断');
  assert.equal(third.attempt, 3);
  assert.match(third.reason!, /最大重试次数 3/);
});

test('熔断 token 维度：首次失败的用量即被记账并可触发熔断（回归：旧实现 recordTokens 在 check 前调用会被静默丢弃）', () => {
  const cb = new CircuitBreaker();
  const policy: typeof DEFAULT_RETRY = { ...DEFAULT_RETRY, maxAttempts: 10, maxTokens: 1_000 };

  const first = cb.check('k', policy, 600);
  assert.equal(first.allowed, true);
  assert.equal(cb.tokens('k'), 600, '首次失败的 token 未被记账');

  const second = cb.check('k', policy, 500);
  assert.equal(second.allowed, false);
  assert.match(second.reason!, /token 预算/);
});

test('熔断成功即清零：同节点成功后重新失败从第 1 次计数', () => {
  const cb = new CircuitBreaker();
  const policy: typeof DEFAULT_RETRY = { ...DEFAULT_RETRY, maxAttempts: 2 };

  assert.equal(cb.check('k', policy).allowed, true);
  cb.reset('k');
  assert.equal(cb.check('k', policy).allowed, true, 'reset 后应重新从第 1 次开始');
  assert.equal(cb.check('k', policy).allowed, false);
});

test('失败路径：熔断未触发时记录 issue.node.failed 并重试', async () => {
  const { scheduler, github, sink } = makeScheduler([openIssue()]);
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);
  await (scheduler as never as { dispatch(w: never, i: Issue, n: string): Promise<void> }).dispatch(
    conn,
    openIssue(),
    'plan',
  );
  const runId = currentRun(scheduler)!.runId;

  await scheduler.onRunResult(runId, 'failed', { error: 'agent 退出码 1' });

  assert.ok(sink.some((e) => e.event === 'issue.node.failed'));
  assert.ok(!sink.some((e) => e.event === 'circuit_breaker.tripped'));
  assert.match(github.comments.at(-1)!, /执行失败/);
  assert.equal(scheduler.listRuns().length, 0);
});

test('失败路径：连续失败达到上限后熔断并转 HITL', async () => {
  const { scheduler, github, sink } = makeScheduler([openIssue()]);
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);

  // DEFAULT_RETRY.maxAttempts = 3
  for (let i = 0; i < DEFAULT_RETRY.maxAttempts; i++) {
    await (
      scheduler as never as { dispatch(w: never, i: Issue, n: string): Promise<void> }
    ).dispatch(conn, openIssue(), 'plan');
    const runId = currentRun(scheduler)!.runId;
    await scheduler.onRunResult(runId, 'failed', { error: `fail ${i}` });
  }

  assert.ok(sink.some((e) => e.event === 'circuit_breaker.tripped'), '未触发熔断');
  assert.deepEqual(github.labels.at(-1), ['hitl:waiting']);
  assert.match(github.comments.at(-1)!, /熔断触发/);
});

/**
 * 以下测试守的是「策略由模板声明、经执行面回报、控制面采纳」这条链路。
 *
 * 为什么控制面不能自己读模板：架构边界要求 Gateway 不读节点模板内容、
 * 不理解业务语义。所以 run.result 是策略唯一的传递通道 —— 一旦这条链路
 * 断了（例如字段没透传），控制面会静默退回全局默认值：测试仍然全绿，
 * 但 QA 节点声明的更严预算完全失效。因此必须显式断言「模板值被采纳」。
 */

/** 连续失败直到熔断，返回最后一次的事件与 GitHub 调用记录。 */
async function failUntilTripped(
  scheduler: Scheduler,
  conn: never,
  policy: RetryPolicy,
  nodeKey = 'plan',
) {
  for (let i = 0; i < policy.maxAttempts + 2; i++) {
    await (
      scheduler as never as { dispatch(w: never, i: Issue, n: string): Promise<void> }
    ).dispatch(conn, openIssue(1, [`node:${nodeKey}`]), nodeKey);
    const runId = currentRun(scheduler);
    if (!runId) continue; // 已熔断挂起，调度不再派发
    await scheduler.onRunResult(runId.runId, 'failed', { error: `fail ${i}`, retry: policy });
  }
}

test('模板策略被采纳：max_attempts=1 时首次失败即熔断（回归：不得退回全局默认 3 次）', async () => {
  const { scheduler, github, sink } = makeScheduler([openIssue()]);
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);

  await failUntilTripped(scheduler, conn as never, {
    ...DEFAULT_RETRY,
    maxAttempts: 1,
  });

  assert.ok(sink.some((e) => e.event === 'circuit_breaker.tripped'), '模板声明 1 次即熔断未生效');
  // 只应有一次失败评论 + 一次熔断评论；若退回默认 3 次，评论数会明显更多
  const failureComments = github.comments.filter((c) => /执行失败/.test(c));
  assert.equal(failureComments.length, 0, '首次失败就应熔断，不应出现「剩余重试次数」评论');
});

test('模板策略被采纳：max_attempts=5 时前 4 次失败均不熔断', async () => {
  const { scheduler, github, sink } = makeScheduler([openIssue()]);
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);

  const policy = { ...DEFAULT_RETRY, maxAttempts: 5 };
  for (let i = 0; i < 4; i++) {
    await (
      scheduler as never as { dispatch(w: never, i: Issue, n: string): Promise<void> }
    ).dispatch(conn, openIssue(), 'plan');
    const runId = currentRun(scheduler)!.runId;
    await scheduler.onRunResult(runId, 'failed', { error: `fail ${i}`, retry: policy });
  }

  assert.ok(
    !sink.some((e) => e.event === 'circuit_breaker.tripped'),
    '模板放宽到 5 次，第 4 次失败不应熔断',
  );
  assert.equal(github.comments.filter((c) => /执行失败/.test(c)).length, 4);
  // 剩余次数按模板的 5 计算，而非全局默认 3
  assert.match(github.comments.at(-1)!, /剩余重试次数：1/);
});

test('策略缺省时退回全局默认：旧版 loop-node 不回报 retry 也能正常熔断', async () => {
  const { scheduler, sink } = makeScheduler([openIssue()]);
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);

  for (let i = 0; i < DEFAULT_RETRY.maxAttempts; i++) {
    await (
      scheduler as never as { dispatch(w: never, i: Issue, n: string): Promise<void> }
    ).dispatch(conn, openIssue(), 'plan');
    // 不传 retry，模拟旧版执行面
    await scheduler.onRunResult(currentRun(scheduler)!.runId, 'failed', { error: `fail ${i}` });
  }

  assert.ok(sink.some((e) => e.event === 'circuit_breaker.tripped'));
});

test('on_exhausted=skip：熔断后跳过该节点并推进到下一节点', async () => {
  const { scheduler, github, sink } = makeScheduler([openIssue()]);
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);

  await failUntilTripped(scheduler, conn as never, {
    ...DEFAULT_RETRY,
    maxAttempts: 2,
    onExhausted: 'skip',
  });

  assert.ok(sink.some((e) => e.event === 'circuit_breaker.tripped'));
  // 跳过 = 走正常推进路径：plan → code，而不是挂起
  assert.deepEqual(github.labels.at(-1), ['node:code'], 'skip 应推进到下一节点');
  assert.ok(
    !github.labels.some((l) => l.includes('hitl:waiting')),
    'skip 策略不应挂起等人工',
  );
  assert.match(github.comments.at(-1)!, /按策略跳过/);
});

test('on_exhausted=skip 在末节点：关闭 issue 而非挂起', async () => {
  const { scheduler, github } = makeScheduler([openIssue(1, ['node:test'])]);
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);

  await failUntilTripped(
    scheduler,
    conn as never,
    { ...DEFAULT_RETRY, maxAttempts: 1, onExhausted: 'skip' },
    'test',
  );

  assert.ok(github.calls.includes('closeIssue'), '末节点跳过后应关闭 issue');
  assert.deepEqual(github.labels.at(-1), ['node:done']);
});

test('on_exhausted=fail：熔断后终止流程，打 node:failed 且不挂起', async () => {
  const { scheduler, github, sink } = makeScheduler([openIssue()]);
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);

  await failUntilTripped(scheduler, conn as never, {
    ...DEFAULT_RETRY,
    maxAttempts: 2,
    onExhausted: 'fail',
  });

  assert.ok(sink.some((e) => e.event === 'circuit_breaker.tripped'));
  assert.deepEqual(github.labels.at(-1), ['node:failed'], 'fail 策略应终止而非推进');
  assert.ok(
    !github.labels.some((l) => l.includes('hitl:waiting')),
    'fail 策略不应挂起等人工',
  );
  assert.match(github.comments.at(-1)!, /按策略终止/);
});

test('熔断事件携带 onExhausted，便于看板区分处置方式', async () => {
  const { scheduler, sink } = makeScheduler([openIssue()]);
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);

  await failUntilTripped(scheduler, conn as never, {
    ...DEFAULT_RETRY,
    maxAttempts: 1,
    onExhausted: 'skip',
  });

  const ev = sink.find((e) => e.event === 'circuit_breaker.tripped');
  assert.equal(ev!.details?.onExhausted, 'skip');
});

test('失败事件记录本次与累计 token 用量（成本可观测）', async () => {
  const { scheduler, sink } = makeScheduler([openIssue()]);
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);

  await (
    scheduler as never as { dispatch(w: never, i: Issue, n: string): Promise<void> }
  ).dispatch(conn, openIssue(), 'plan');
  const runId = currentRun(scheduler)!.runId;
  await scheduler.onRunResult(runId, 'failed', { error: 'boom' });

  const ev = sink.find((e) => e.event === 'issue.node.failed');
  assert.ok(ev, '失败必须产出事件，否则 token 消耗无法归因');
  const d = ev!.details as Record<string, number>;
  assert.ok(d.inputTokens > 0, '本次注入用量应被记录');
  assert.equal(d.accumulatedTokens, d.inputTokens, '首次失败累计量应等于本次量');
});

/**
 * 健康检查（ping / stale / terminate）测试。
 *
 * 为什么这组必须有覆盖：光靠 WS close 事件回收 worker 是不够的。
 * worker 进程假死或 TCP 半开时，FIN/RST 都不会到达，readyState 长期停在
 * OPEN，close 回调永不触发 —— unregisterWorker 不被调用，该 worker 持有的
 * issue 去重锁就永久占住，需求卡死且无任何告警。这是「静默卡死」类故障，
 * 没有测试守住的话，回归时不会有任何报错。
 */

/** 构造可观察 send/terminate 调用的假 socket。 */
function makeFakeSocket(opts: { readyState?: number } = {}) {
  const sent: string[] = [];
  let terminated = 0;
  return {
    sent,
    get terminated() {
      return terminated;
    },
    socket: {
      readyState: opts.readyState ?? 1,
      send(s: string) {
        sent.push(s);
      },
      terminate() {
        terminated++;
      },
    },
  };
}

function healthCheck(scheduler: Scheduler) {
  return (scheduler as never as { healthCheck(): void }).healthCheck();
}

test('健康检查：在线 worker 收到应用层 ping', () => {
  const { scheduler } = makeScheduler([openIssue()]);
  const fake = makeFakeSocket();
  scheduler.registerWorker({
    nodeId: 'w1',
    socket: fake.socket as never,
    agents: ['mock'],
    busy: false,
    lastSeen: Date.now(),
  });

  healthCheck(scheduler);

  assert.equal(fake.sent.length, 1, '应发出恰好一次 ping');
  assert.deepEqual(JSON.parse(fake.sent[0]), { type: 'ping' });
  assert.equal(fake.terminated, 0, '健康 worker 不应被断开');
});

test('健康检查：超过 staleMs 无响应 → 记事件并 terminate（回归：假死 worker 永久占锁）', () => {
  const { scheduler, sink } = makeScheduler([openIssue()]);
  const fake = makeFakeSocket();
  scheduler.registerWorker({
    nodeId: 'w1',
    socket: fake.socket as never,
    agents: ['mock'],
    busy: true,
    currentRunId: 'run_x',
    // 远超 staleMs（45s）：模拟 TCP 半开
    lastSeen: Date.now() - 120_000,
  });

  healthCheck(scheduler);

  assert.equal(fake.terminated, 1, '假死 worker 必须被强制断开');
  assert.equal(fake.sent.length, 0, '已判定死亡的 worker 不应再收到 ping');
  const ev = sink.find((e) => e.event === 'worker.stale');
  assert.ok(ev, '必须留下 worker.stale 事件，否则卡死无迹可查');
  assert.equal(ev!.details?.currentRunId, 'run_x', '事件需带上被中断的 run，便于归因');
});

test('健康检查：stale 阈值内不误杀（容忍单次网络抖动）', () => {
  const { scheduler, sink } = makeScheduler([openIssue()]);
  const fake = makeFakeSocket();
  scheduler.registerWorker({
    nodeId: 'w1',
    socket: fake.socket as never,
    agents: ['mock'],
    busy: false,
    // 20s 无响应：大于 ping 间隔 15s，但小于 stale 阈值 45s
    lastSeen: Date.now() - 20_000,
  });

  healthCheck(scheduler);

  assert.equal(fake.terminated, 0, '未到 stale 阈值不得断开');
  assert.equal(fake.sent.length, 1, '应继续探活');
  assert.ok(!sink.some((e) => e.event === 'worker.stale'));
});

test('健康检查：连接正在关闭时不发 ping（避免向已关闭 socket 写入抛错）', () => {
  const { scheduler } = makeScheduler([openIssue()]);
  const fake = makeFakeSocket({ readyState: 3 /* CLOSED */ });
  scheduler.registerWorker({
    nodeId: 'w1',
    socket: fake.socket as never,
    agents: ['mock'],
    busy: false,
    lastSeen: Date.now(),
  });

  healthCheck(scheduler);

  assert.equal(fake.sent.length, 0);
});

test('健康检查：staleMs=0 时禁用假死判定，仅探活', () => {
  const { scheduler } = makeScheduler([openIssue()]);
  // 覆盖配置：0 表示禁用（与 RUN_TIMEOUT_MS=0 的语义一致）
  (scheduler as never as { deps: { config: unknown } }).deps.config = {
    ...makeConfig(),
    worker: { pingIntervalMs: 15_000, staleMs: 0 },
  };
  const fake = makeFakeSocket();
  scheduler.registerWorker({
    nodeId: 'w1',
    socket: fake.socket as never,
    agents: ['mock'],
    busy: false,
    lastSeen: Date.now() - 999_000, // 极久未响应
  });

  healthCheck(scheduler);

  assert.equal(fake.terminated, 0, 'staleMs=0 应完全禁用假死回收');
  assert.equal(fake.sent.length, 1);
});

test('健康检查：send 抛错不中断其余 worker 的巡检', () => {
  const { scheduler } = makeScheduler([openIssue()]);
  const bad = makeFakeSocket();
  bad.socket.send = () => {
    throw new Error('socket write failed');
  };
  const good = makeFakeSocket();
  scheduler.registerWorker({
    nodeId: 'bad',
    socket: bad.socket as never,
    agents: ['mock'],
    busy: false,
    lastSeen: Date.now(),
  });
  scheduler.registerWorker({
    nodeId: 'good',
    socket: good.socket as never,
    agents: ['mock'],
    busy: false,
    lastSeen: Date.now(),
  });

  // 不应抛出：一个坏连接不能让整轮健康检查失败
  assert.doesNotThrow(() => healthCheck(scheduler));
  assert.equal(good.sent.length, 1, '后续 worker 仍应被巡检到');
});

test('健康检查：terminate 抛错时不中断巡检', () => {
  const { scheduler } = makeScheduler([openIssue()]);
  const bad = makeFakeSocket();
  bad.socket.terminate = () => {
    throw new Error('terminate failed');
  };
  const good = makeFakeSocket();
  scheduler.registerWorker({
    nodeId: 'bad',
    socket: bad.socket as never,
    agents: ['mock'],
    busy: false,
    lastSeen: Date.now() - 120_000,
  });
  scheduler.registerWorker({
    nodeId: 'good',
    socket: good.socket as never,
    agents: ['mock'],
    busy: false,
    lastSeen: Date.now(),
  });

  assert.doesNotThrow(() => healthCheck(scheduler));
  assert.equal(good.sent.length, 1);
});

/**
 * busy 权威性回归。
 *
 * 此前 heartbeat 用 `load > 0` 覆写 busy，存在窄窗：gateway 发出 launch 后，
 * worker 要等模板加载、prompt 落盘完成才登记 activeRuns；若恰在此窗口收到
 * ping，回报 load=0 会把 busy 抹成 false，同一 worker 被派发第二个 run，
 * 两个 agent 并发踩同一工作目录。该窗口此前不可达（gateway 从不发 ping），
 * 健康检查上线后 ping 每 15s 一次，窗口变成真实风险。
 */
test('busy 以 launch/result 为唯一权威，heartbeat 不得覆写', async () => {
  const { scheduler } = makeScheduler([openIssue()]);
  const { conn } = makeWorker();
  scheduler.registerWorker(conn);

  await (
    scheduler as never as { dispatch(w: never, i: Issue, n: string): Promise<void> }
  ).dispatch(conn, openIssue(), 'plan');
  const typed = conn as never as { busy: boolean; lastLoad?: number };
  assert.equal(typed.busy, true, 'dispatch 后必须为忙');

  // 模拟窄窗：worker 还没登记 activeRuns，心跳回报 load=0
  typed.lastLoad = 0;
  // 健康检查与心跳都不应改动 busy
  healthCheck(scheduler);
  assert.equal(typed.busy, true, 'heartbeat/健康检查覆写 busy 会导致同一 worker 被重复派发');

  // 只有 run.result 能释放
  const runId = currentRun(scheduler)!.runId;
  await scheduler.onRunResult(runId, 'completed', { output: 'ok' });
  assert.equal(typed.busy, false, 'run.result 才是 busy 的唯一释放点');
});

test('start/stop 管理健康检查定时器，不泄漏（stop 后不再巡检）', () => {
  const { scheduler } = makeScheduler([openIssue()]);
  const fake = makeFakeSocket();
  scheduler.registerWorker({
    nodeId: 'w1',
    socket: fake.socket as never,
    agents: ['mock'],
    busy: false,
    lastSeen: Date.now(),
  });

  scheduler.start();
  scheduler.stop();

  // stop 后手动巡检仍可用（供测试），但定时器必须已清理：
  // 通过再次 stop 不抛错、且 healthTimer 已置空来间接验证
  assert.doesNotThrow(() => scheduler.stop());
  const ht = (scheduler as never as { healthTimer?: NodeJS.Timeout }).healthTimer;
  assert.equal(ht, undefined, 'stop 后定时器引用应被清理，否则进程无法退出');
});
