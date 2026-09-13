import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Scheduler, type Run } from './scheduler.js';
import type { HarnessEvent } from '@harness/shared';
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

  const pending = scheduler.onRunResult(runId, 'completed', 'ok');

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

  await scheduler.onRunResult(runId, 'completed', 'done');

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

  await scheduler.onRunResult(runId, 'completed', 'done');

  assert.ok(github.calls.includes('closeIssue'), '流程完成后未关闭 issue，飞书卡片会谎报「已关闭」');
  assert.deepEqual(github.labels.at(-1), ['node:done']);
  assert.ok(sink.some((e) => e.event === 'issue.completed'));
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
  await scheduler.onRunResult(runId, 'completed', 'done');

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

  await scheduler.onRunResult('run_unknown', 'completed', 'x');

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

  await scheduler.onRunResult(runId, 'failed', undefined, 'agent 退出码 1');

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
    await scheduler.onRunResult(runId, 'failed', undefined, `fail ${i}`);
  }

  assert.ok(sink.some((e) => e.event === 'circuit_breaker.tripped'), '未触发熔断');
  assert.deepEqual(github.labels.at(-1), ['hitl:waiting']);
  assert.match(github.comments.at(-1)!, /熔断触发/);
});
