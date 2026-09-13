import type { WebSocket } from 'ws';
import type { GatewayToWorker, HarnessEvent } from '@harness/shared';
import { estimateTokens } from '@harness/shared';
import type { GatewayConfig } from './config.js';
import type { EventStore } from './db.js';
import type { EventBus } from './bus.js';
import { detectNode, type GithubClient, type Issue } from './github.js';
import { CircuitBreaker, DEFAULT_RETRY } from './circuit-breaker.js';

export interface WorkerConn {
  nodeId: string;
  socket: WebSocket;
  agents: string[];
  busy: boolean;
  lastSeen: number;
  currentRunId?: string;
}

export interface Run {
  runId: string;
  issueNumber: number;
  nodeKey: string;
  workerId: string;
  status: 'running' | 'completed' | 'failed';
  startedAt: string;
  /**
   * 本次派发注入的上下文 token 估算值（字符数折算）。
   *
   * 为什么是估算：执行面对 Agent CLI 只有「prompt 文件路径 + 退出码」这一最小契约，
   * CLI 不回传结构化的 usage，所以拿不到 provider 报的精确 token。
   * 用 estimateTokens 做字符折算是这个契约下唯一可行的口径，
   * 且与 loop-node 侧裁剪上下文用的是同一个函数，两边口径一致。
   */
  inputTokens: number;
}

interface Deps {
  config: GatewayConfig;
  store: EventStore;
  bus: EventBus;
  github: GithubClient;
  circuitBreaker: CircuitBreaker;
}

export class Scheduler {
  private workers = new Map<string, WorkerConn>();
  private runsByIssue = new Map<number, Run>();
  private runsByRunId = new Map<string, Run>();
  private timer?: NodeJS.Timeout;
  private ticking = false;

  constructor(private deps: Deps) {}

  start() {
    const loop = () =>
      this.tick().finally(() => {
        this.timer = setTimeout(loop, this.deps.config.github.pollIntervalMs);
      });
    loop();
  }

  stop() {
    if (this.timer) clearTimeout(this.timer);
  }

  registerWorker(w: WorkerConn) {
    this.workers.set(w.nodeId, w);
  }

  unregisterWorker(nodeId: string) {
    const w = this.workers.get(nodeId);
    if (!w) return;
    this.workers.delete(nodeId);
    if (w.currentRunId) this.releaseRun(w.currentRunId, 'worker_lost');
  }

  workerCount() {
    return this.workers.size;
  }

  listRuns() {
    return [...this.runsByIssue.values()];
  }

  private emit(source: HarnessEvent['source'], event: string, extra: Partial<HarnessEvent>) {
    const ev = this.deps.store.append({
      source,
      event,
      projectId: this.deps.config.projectId,
      ...extra,
    });
    this.deps.bus.emitEvent(ev);
    return ev;
  }

  private async tick() {
    if (this.ticking) return;
    this.ticking = true;
    try {
      const issues = await this.deps.github.listIssues();
      for (const issue of issues) {
        if (this.runsByIssue.has(issue.number)) continue;
        await this.maybeDispatch(issue);
      }
    } catch (e) {
      console.error('[scheduler] tick 失败:', (e as Error).message);
    } finally {
      this.ticking = false;
    }
  }

  private async maybeDispatch(issue: Issue) {
    const { current, isNew, index } = detectNode(issue, this.deps.config.pipeline);

    if (isNew && current) {
      await this.deps.github.setLabels(issue.number, [`node:${current}`]);
      this.emit('github', 'issue.node.assigned', {
        workItemId: String(issue.number),
        details: { nodeKey: current },
      });
      return;
    }

    if (!current || index < 0) return;

    const worker = this.pickWorker();
    if (!worker) return;

    await this.dispatch(worker, issue, current);
  }

  private pickWorker(): WorkerConn | null {
    for (const w of this.workers.values()) {
      if (!w.busy && w.socket.readyState === 1) return w;
    }
    return null;
  }

  private async dispatch(worker: WorkerConn, issue: Issue, nodeKey: string) {
    const runId = `run_${issue.number}_${nodeKey}_${Date.now()}`;

    const context: Record<string, string> = {
      'issue.number': String(issue.number),
      'issue.title': issue.title,
      'issue.body': issue.body,
      'issue.labels': issue.labels.join(','),
    };

    // 与 loop-node 侧 pruneContext 使用同一个 estimateTokens，保证两边口径一致。
    const inputTokens = Object.values(context).reduce((sum, v) => sum + estimateTokens(v), 0);

    // 临界区：以下四步必须全部同步完成、先于 socket.send，中间不能出现 await。
    // 「issue 已占位 + worker 已占用」对随后任意时刻的 tick / WS 消息立刻可见。
    const run: Run = {
      runId,
      issueNumber: issue.number,
      nodeKey,
      workerId: worker.nodeId,
      status: 'running',
      startedAt: new Date().toISOString(),
      inputTokens,
    };
    this.runsByIssue.set(issue.number, run);
    this.runsByRunId.set(runId, run);
    worker.busy = true;
    worker.currentRunId = runId;

    const msg: GatewayToWorker = {
      type: 'launch',
      runId,
      workItemId: String(issue.number),
      nodeKey,
      template: '',
      context,
    };
    worker.socket.send(JSON.stringify(msg));

    this.emit('harness', 'run.dispatched', {
      runId,
      nodeId: worker.nodeId,
      workItemId: String(issue.number),
      details: { nodeKey, context, inputTokens },
    });
  }

  async onRunResult(
    runId: string,
    status: 'completed' | 'failed',
    output?: string,
    error?: string,
  ) {
    const run = this.runsByRunId.get(runId);
    if (!run) return;

    run.status = status;

    // worker 容量立即释放：它可以马上接「别的 issue」的活。
    const worker = this.workers.get(run.workerId);
    if (worker) {
      worker.busy = false;
      worker.currentRunId = undefined;
    }

    // issue 去重锁必须持有到 GitHub 回写结束。
    //
    // 为什么不能提前删：调度是「轮询 issue 标签」驱动的，回写（评论 + 推进标签）
    // 耗时数秒且中途要 await。若锁在此刻已释放，期间任意一轮 tick 都会看到
    // 「标签还是旧节点 + 内存里没有锁」，从而把同一节点重复派发出去。
    // 失败时同样在 finally 释放——标签未推进，下一轮按标签重跑该节点，
    // 这正是 at-least-once 语义的兜底路径。
    try {
      await this.transition(run, status, output, error);
    } catch (e) {
      const msg = (e as Error).message;
      console.error(`[scheduler] run ${runId} 回写 GitHub 失败，锁已释放，按标签重放:`, msg);
      this.emit('harness', 'run.transition_failed', {
        runId,
        workItemId: String(run.issueNumber),
        details: { nodeKey: run.nodeKey, error: msg },
      });
    } finally {
      this.runsByRunId.delete(runId);
      this.runsByIssue.delete(run.issueNumber);
    }
  }

  /** 按 run 终态推进流程事实源（GitHub 标签 / 评论）。锁的释放由调用方负责。 */
  private async transition(
    run: Run,
    status: 'completed' | 'failed',
    output?: string,
    error?: string,
  ) {
    const issues = await this.deps.github.listIssues();
    const issue = issues.find((i) => i.number === run.issueNumber);
    if (!issue) {
      this.emit('harness', 'run.issue_gone', {
        runId: run.runId,
        workItemId: String(run.issueNumber),
        details: { nodeKey: run.nodeKey },
      });
      return;
    }

    if (status === 'completed') {
      this.deps.circuitBreaker.reset(`${run.issueNumber}:${run.nodeKey}`);

      const nextIndex = this.deps.config.pipeline.indexOf(run.nodeKey) + 1;
      const next = this.deps.config.pipeline[nextIndex];

      await this.deps.github.addComment(
        issue.number,
        [
          `### ✅ 节点 \`${run.nodeKey}\` 执行完成`,
          '',
          '<details><summary>执行输出（尾部截取）</summary>',
          '',
          '```',
          (output ?? '').slice(-3000),
          '```',
          '</details>',
        ].join('\n'),
      );

      if (next) {
        await this.deps.github.setLabels(issue.number, [`node:${next}`]);
        this.emit('github', 'issue.node.advanced', {
          workItemId: String(issue.number),
          runId: run.runId,
          details: { from: run.nodeKey, to: next },
        });
      } else {
        await this.deps.github.setLabels(issue.number, ['node:done']);
        await this.deps.github.closeIssue(issue.number);
        await this.deps.github.addComment(issue.number, '🎉 **流程完成**：所有节点已执行完毕。');
        this.emit('github', 'issue.completed', {
          workItemId: String(issue.number),
          runId: run.runId,
        });
      }
      return;
    }

    // ---- 失败：记账 + 熔断判定在同一次调用内完成，再发失败事件 ----
    const runKey = `${run.issueNumber}:${run.nodeKey}`;

    // token 维度接线：失败路径才记账，成功即 reset。
    // 口径是「本次派发注入的上下文 token 估算值」，不是 provider 报的精确 usage
    // （最小契约下拿不到）。量级足够拦「同一节点反复重跑导致的上下文重复注入」。
    const check = this.deps.circuitBreaker.check(runKey, DEFAULT_RETRY, run.inputTokens);
    const accumulated = this.deps.circuitBreaker.tokens(runKey);

    this.emit('github', 'issue.node.failed', {
      workItemId: String(issue.number),
      runId: run.runId,
      details: {
        nodeKey: run.nodeKey,
        error: (error ?? 'unknown error').slice(0, 2000),
        inputTokens: run.inputTokens,
        accumulatedTokens: accumulated,
        attempt: check.attempt,
      },
    });

    if (!check.allowed) {
      this.emit('harness', 'circuit_breaker.tripped', {
        runId: run.runId,
        workItemId: String(run.issueNumber),
        details: { nodeKey: run.nodeKey, reason: check.reason },
      });
      await this.deps.github.addComment(
        issue.number,
        `### 🛑 熔断触发\n\n节点 \`${run.nodeKey}\` 已中止：${check.reason}\n\n需要人工介入。`,
      );
      await this.deps.github.setLabels(issue.number, ['hitl:waiting']);
      return;
    }

    await this.deps.github.addComment(
      issue.number,
      [
        `### ❌ 节点 \`${run.nodeKey}\` 执行失败`,
        '',
        '```',
        (error ?? 'unknown error').slice(0, 2000),
        '```',
        '',
        `_熔断检查通过，剩余重试次数：${DEFAULT_RETRY.maxAttempts - check.attempt}_`,
      ].join('\n'),
    );
  }

  private releaseRun(runId: string, reason: string) {
    const run = this.runsByRunId.get(runId);
    if (!run) return;
    this.emit('harness', `run.${reason}`, {
      runId,
      nodeId: run.workerId,
      workItemId: String(run.issueNumber),
    });
    this.runsByRunId.delete(runId);
    this.runsByIssue.delete(run.issueNumber);
  }
}
