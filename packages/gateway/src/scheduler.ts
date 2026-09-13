import type { WebSocket } from 'ws';
import type { GatewayToWorker, HarnessEvent } from '@harness/shared';
import type { GatewayConfig } from './config.js';
import type { EventStore } from './db.js';
import type { EventBus } from './bus.js';
import { detectNode, type GithubClient, type Issue } from './github.js';

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
}

interface Deps {
  config: GatewayConfig;
  store: EventStore;
  bus: EventBus;
  github: GithubClient;
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
    const run: Run = {
      runId,
      issueNumber: issue.number,
      nodeKey,
      workerId: worker.nodeId,
      status: 'running',
      startedAt: new Date().toISOString(),
    };
    this.runsByIssue.set(issue.number, run);
    this.runsByRunId.set(runId, run);
    worker.busy = true;
    worker.currentRunId = runId;

    const context: Record<string, string> = {
      'issue.number': String(issue.number),
      'issue.title': issue.title,
      'issue.body': issue.body,
      'issue.labels': issue.labels.join(','),
    };

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
      details: { nodeKey, context },
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

    const worker = this.workers.get(run.workerId);
    const releaseWorker = () => {
      if (worker) {
        worker.busy = false;
        worker.currentRunId = undefined;
      }
    };
    // 锁必须在 GitHub 状态推进成功后才释放：评论/打标期间轮询会看到旧标签，
    // 提前释放会导致同一节点被重复派发（at-most-once → at-least-once）。
    const releaseRunLock = () => {
      this.runsByRunId.delete(runId);
      this.runsByIssue.delete(run.issueNumber);
    };

    try {
      const issues = await this.deps.github.listIssues();
      const issue = issues.find((i) => i.number === run.issueNumber);
      if (!issue) {
        releaseRunLock();
        releaseWorker();
        return;
      }

      if (status === 'completed') {
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
            runId,
            details: { from: run.nodeKey, to: next },
          });
        } else {
          await this.deps.github.setLabels(issue.number, ['node:done']);
          await this.deps.github.addComment(issue.number, '🎉 **流程完成**：所有节点已执行完毕。');
          await this.deps.github.closeIssue(issue.number);
          this.emit('github', 'issue.completed', { workItemId: String(issue.number), runId });
        }
      } else {
        await this.deps.github.addComment(
          issue.number,
          [
            `### ❌ 节点 \`${run.nodeKey}\` 执行失败`,
            '',
            '```',
            (error ?? 'unknown error').slice(0, 2000),
            '```',
          ].join('\n'),
        );
      }
      releaseRunLock();
      releaseWorker();
    } catch (e) {
      // GitHub 回写失败（含重试后仍失败）：保留 issue 当前标签，释放锁让后续轮询重新派发该节点
      console.error('[scheduler] 回写 GitHub 失败，下轮轮询将按标签重试该节点:', (e as Error).message);
      this.emit('harness', 'run.transition_failed', {
        runId,
        workItemId: String(run.issueNumber),
        nodeId: run.workerId,
        details: { error: (e as Error).message.slice(0, 500) },
      });
      releaseRunLock();
      releaseWorker();
    }
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
