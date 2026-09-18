/**
 * 调度核心：定时轮询 GitHub open issues → detectNode 推导当前节点 → 派发给空闲 worker →
 * 收到结果后把终态回写为标签/评论。三重并发控制：
 *   - ticking：防止两轮 tick 重入；
 *   - worker.busy：每个 worker 同时只跑一个 run（容量在收到结果时立即释放）；
 *   - runsByIssue：同一 issue 的去重锁，持有到 GitHub 回写结束才释放。
 * 内存锁只是运行时去重，跨重启/崩溃的流程状态一律以 GitHub 标签为准。
 */
import type { WebSocket } from 'ws';
import type { GatewayToWorker, HarnessEvent, RetryPolicy } from '@harness/shared';
import { estimateTokens } from '@harness/shared';
import type { GatewayConfig } from './config.js';
import type { EventStore } from './db.js';
import type { EventBus } from './bus.js';
import { detectNode, HITL_WAITING, NODE_PREFIX, type GithubClient, type Issue } from './github.js';
import { CircuitBreaker, DEFAULT_RETRY } from './circuit-breaker.js';

export interface WorkerConn {
  nodeId: string;
  socket: WebSocket;
  agents: string[];
  /**
   * 是否正在执行 run。**唯一权威来源是 dispatch（置 true）与 run.result（置 false）**，
   * heartbeat 不得覆写 —— 详见 ws.ts 中 heartbeat 分支的说明。
   */
  busy: boolean;
  /** 最近一次心跳上报的负载，仅供观测/看板，不参与派发判定 */
  lastLoad?: number;
  /** 最近一次收到该 worker 任意消息的时刻，stale 判定的唯一依据 */
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
  /** 收到 run.result 的时刻；与 startedAt 配对得到控制面口径的节点耗时 */
  endedAt?: string;
  /** 控制面口径耗时：dispatch → 收到结果（含网络与排队），ms */
  durationMs?: number;
  /** agent 真实用量（transcript 口径）；缺省 = 该 run 无 transcript，只有估算可用 */
  tokens?: { input: number; output: number };
}

interface Deps {
  config: GatewayConfig;
  store: EventStore;
  bus: EventBus;
  github: GithubClient;
  circuitBreaker: CircuitBreaker;
}

export interface RunResultOptions {
  output?: string;
  error?: string;
  /**
   * 执行面从节点模板解析出的重试策略。
   * 缺省时退回 DEFAULT_RETRY —— 兼容旧版 loop-node 与未声明 retry 段的模板。
   */
  retry?: RetryPolicy;
  /** 执行面回报的前置检查摘要，失败事件据此区分环境问题与业务失败 */
  preflight?: { passed: boolean; failed: string[] };
  /** 执行面口径耗时（收到 launch → 发出结果），与控制面自算的 durationMs 并存 */
  durationMs?: number;
  /** agent 真实用量（transcript 口径）；缺省表示拿不到，回退注入估算 */
  tokens?: { input: number; output: number };
}

interface HitlRecord {
  nodeKey: string;
  enteredAt: number;
  runId?: string;
  /** true = 进入时刻非本方写入（gateway 重启后首次观察到挂起），waitMs 为近似值 */
  inferred: boolean;
  /** 超时升级告警是否已发过（每次挂起最多一次） */
  escalated?: boolean;
}

export class Scheduler {
  private workers = new Map<string, WorkerConn>();
  private runsByIssue = new Map<number, Run>();
  private runsByRunId = new Map<string, Run>();
  /** HITL 挂起追踪：issueNumber → 进入时刻。tick 中差分标签变化产生 hitl.entered/resolved */
  private hitlSince = new Map<number, HitlRecord>();
  private timer?: NodeJS.Timeout;
  private healthTimer?: NodeJS.Timeout;
  private ticking = false;
  /** 已请求停止。在途 tick 据此拒绝重排定时器，否则进程无法退出。 */
  private stopped = false;

  constructor(private deps: Deps) {}

  start() {
    // setTimeout 自链而非 setInterval：上一轮 tick 完全结束（含回写）后才排下一轮，
    // 慢轮询不会在事件循环里堆积；pollIntervalMs 是两轮之间的间隔而非固定频率
    this.stopped = false;
    const loop = () =>
      this.tick().finally(() => {
        // 必须检查 stopped：tick() 里有 await（listIssues 可能耗时数秒），
        // 若 stop() 恰在此期间被调用，它清掉的是「上一轮」的 timer，
        // 而这里的 finally 会在之后又排一个永不清除的新 timer ——
        // 事件循环因此常驻，进程退不出去（关停时 server.close 回调永不触发）。
        if (this.stopped) return;
        this.timer = setTimeout(loop, this.deps.config.github.pollIntervalMs);
      });
    loop();

    // 独立于业务轮询的健康检查：频率由 WORKER_PING_MS 决定，
    // 不能搭 tick 的便车 —— 两者周期不同，且 tick 可能因 GitHub 慢而延长。
    const { pingIntervalMs } = this.deps.config.worker;
    if (pingIntervalMs > 0) {
      this.healthTimer = setInterval(() => this.healthCheck(), pingIntervalMs);
    }
  }

  stop() {
    this.stopped = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    if (this.healthTimer) {
      clearInterval(this.healthTimer);
      this.healthTimer = undefined;
    }
  }

  /**
   * 应用层心跳与假死回收。
   *
   * 为什么光靠 WS close 事件不够：执行 PTY 的 worker 可能进程假死、
   * 或 TCP 半开（对端断电、NAT 超时、网络静默丢包）。这两种情况下
   * FIN/RST 都不会到达，socket 的 readyState 长期停在 OPEN，
   * close 回调永不触发 —— unregisterWorker 不被调用，
   * 该 worker 持有的 issue 去重锁就**永久占住**，需求卡死且无任何告警。
   *
   * 处理方式是应用层探活：定期发 ping，worker 收到后回 heartbeat 刷新
   * lastSeen；超过 staleMs 无任何消息即判定死亡，主动 terminate() 断开。
   * terminate 会触发 close 回调 → unregisterWorker → releaseRun('worker_lost')，
   * 复用既有的锁回收路径，不另造一套。
   *
   * staleMs 必须显著大于 pingIntervalMs（默认 45s vs 15s，即 3 次机会），
   * 否则一次网络抖动就会误杀健康 worker。
   */
  private healthCheck() {
    const now = Date.now();
    const { staleMs } = this.deps.config.worker;

    for (const w of [...this.workers.values()]) {
      if (staleMs > 0 && now - w.lastSeen > staleMs) {
        this.emit('harness', 'worker.stale', {
          nodeId: w.nodeId,
          runId: w.currentRunId,
          details: {
            lastSeenAgoMs: now - w.lastSeen,
            staleMs,
            currentRunId: w.currentRunId,
          },
        });
        console.warn(
          `[scheduler] worker ${w.nodeId} 已 ${now - w.lastSeen}ms 无响应（阈值 ${staleMs}ms），判定假死并断开`,
        );
        // terminate 而非 close：不等对端握手确认，半开连接下 close 会一直等不到响应
        try {
          w.socket.terminate();
        } catch (e) {
          console.error(`[scheduler] 断开假死 worker ${w.nodeId} 失败:`, (e as Error).message);
        }
        continue;
      }

      // 仍在线则探活。readyState !== OPEN 时不发：连接正在关闭，
      // close 回调会走正常回收路径，此时 send 只会抛错。
      if (w.socket.readyState === 1) {
        try {
          w.socket.send(JSON.stringify({ type: 'ping' } satisfies GatewayToWorker));
        } catch (e) {
          console.error(`[scheduler] 向 worker ${w.nodeId} 发送 ping 失败:`, (e as Error).message);
        }
      }
    }
  }

  registerWorker(w: WorkerConn) {
    this.workers.set(w.nodeId, w);
  }

  unregisterWorker(nodeId: string) {
    const w = this.workers.get(nodeId);
    if (!w) return;
    this.workers.delete(nodeId);
    // 锁恢复：worker 断连时其在跑的 run 已无结果可等，按 worker_lost 释放 issue 锁，
    // 标签仍停在当前节点，下一轮 tick 即可重新派发给其他 worker
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
    // 重入兜底：正常路径已由 start() 的自链定时器保证串行，标志位再防意外的并发调用
    if (this.ticking) return;
    this.ticking = true;
    try {
      const issues = await this.deps.github.listIssues();
      // 先差分 HITL 状态再走派发：人工摘标签的 resolved 事件先于重新派发落库，
      // 看板时间线上「解除等待 → 重新执行」的顺序才正确
      this.trackHitl(issues);
      for (const issue of issues) {
        // issue 去重锁：正在执行或正在回写的 issue 本轮直接跳过
        if (this.runsByIssue.has(issue.number)) continue;
        await this.maybeDispatch(issue);
      }
    } catch (e) {
      console.error('[scheduler] tick 失败:', (e as Error).message);
    } finally {
      this.ticking = false;
    }
  }

  /**
   * HITL 生命周期差分：每轮 tick 对比 issue 标签与本方记录。
   *
   * 进入：熔断路径在 transition 里直接写入精确时刻；其余来源（人工打 hitl: 标签、
   * gateway 重启后首次观察到挂起）按首次观察时刻近似，事件标 inferred: true。
   * 解除：上一轮挂起、本轮标签已无 hitl: → hitl.resolved + waitMs；
   * issue 关闭或从 open 列表消失同样视为解除（resolvedBy: issue_gone）。
   * 记录只在内存：跨重启的挂起无法还原进入时刻，用首次观察兜底而非静默丢失。
   */
  private trackHitl(issues: Issue[]) {
    const openNumbers = new Set(issues.map((i) => i.number));
    for (const issue of issues) {
      const suspended = detectNode(issue, this.deps.config.pipeline).suspended;
      const rec = this.hitlSince.get(issue.number);
      if (suspended && !rec) {
        const nodeKey = issue.labels.find((l) => l.startsWith(NODE_PREFIX))?.slice(NODE_PREFIX.length) ?? 'unknown';
        this.hitlSince.set(issue.number, { nodeKey, enteredAt: Date.now(), inferred: true });
        this.emit('github', 'hitl.entered', {
          workItemId: String(issue.number),
          details: { nodeKey, inferred: true },
        });
      } else if (!suspended && rec) {
        this.hitlSince.delete(issue.number);
        this.emit('github', 'hitl.resolved', {
          workItemId: String(issue.number),
          runId: rec.runId,
          details: {
            nodeKey: rec.nodeKey,
            waitMs: Date.now() - rec.enteredAt,
            inferredEntry: rec.inferred,
          },
        });
      } else if (suspended && rec) {
        // 仍在挂起：达到升级阈值且未发过 → 升级告警（白名单通知据此 @ 值班人）
        const threshold = this.deps.config.alerts?.hitlEscalateMs ?? 0;
        const waitMs = Date.now() - rec.enteredAt;
        if (threshold > 0 && !rec.escalated && waitMs >= threshold) {
          rec.escalated = true;
          this.emit('harness', 'hitl.escalated', {
            workItemId: String(issue.number),
            runId: rec.runId,
            details: { nodeKey: rec.nodeKey, waitMs, thresholdMs: threshold, inferredEntry: rec.inferred },
          });
        }
      }
    }
    for (const [number, rec] of this.hitlSince) {
      if (!openNumbers.has(number)) {
        this.hitlSince.delete(number);
        this.emit('github', 'hitl.resolved', {
          workItemId: String(number),
          runId: rec.runId,
          details: {
            nodeKey: rec.nodeKey,
            waitMs: Date.now() - rec.enteredAt,
            inferredEntry: rec.inferred,
            resolvedBy: 'issue_gone',
          },
        });
      }
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
    opts: RunResultOptions = {},
  ) {
    const run = this.runsByRunId.get(runId);
    if (!run) return;

    run.status = status;
    // 在任何 await 之前闭合时间窗：transition 内的 listIssues / 回写耗时
    // 不计入节点执行耗时，口径为「dispatch → 收到结果」
    run.endedAt = new Date().toISOString();
    run.durationMs = Date.now() - Date.parse(run.startedAt);
    if (opts.tokens) run.tokens = opts.tokens;

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
      await this.transition(run, status, opts);
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
    opts: RunResultOptions = {},
  ) {
    const { output, error } = opts;
    // 模板未声明（或旧版 loop-node 未回报）时退回全局默认，不让策略缺省变成崩溃。
    const policy: RetryPolicy = opts.retry ?? DEFAULT_RETRY;

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

    const runKey = `${run.issueNumber}:${run.nodeKey}`;

    if (status === 'completed') {
      // 总尝试次数 = 历史失败次数 + 本次成功；必须在 reset 清账前读取，
      // 一次通过时 attemptsOf 返回 0 → attempt=1
      const attempt = this.deps.circuitBreaker.attemptsOf(runKey) + 1;
      this.deps.circuitBreaker.reset(runKey);

      const outputBlock = [
        '',
        '<details><summary>执行输出（尾部截取）</summary>',
        '',
        '```',
        (output ?? '').slice(-3000),
        '```',
        '</details>',
      ].join('\n');

      // 与「熔断后跳过」共用 advance()：末节点打 done 并关闭 issue 的收尾
      // 行为只有一份实现，不会出现两条路径各自演化导致的不一致。
      await this.advance(
        issue,
        run,
        `### ✅ 节点 \`${run.nodeKey}\` 执行完成${outputBlock}`,
        attempt,
      );
      return;
    }

    // ---- 失败：记账 + 熔断判定在同一次调用内完成，再发失败事件 ----
    // token 维度接线：失败路径才记账，成功即 reset。
    // 口径优先级：worker 回报的 transcript 真实用量 > 注入上下文估算（旧版
    // loop-node 或 agent 无 transcript 时唯一可用）。真实口径覆盖 agent 多轮
    // 消耗，拦截更准；估算口径兜底拦截「同节点反复重跑的上下文重复注入」。
    const tokensConsumed = run.tokens ? run.tokens.input + run.tokens.output : run.inputTokens;
    const check = this.deps.circuitBreaker.check(runKey, policy, tokensConsumed);
    const accumulated = this.deps.circuitBreaker.tokens(runKey);

    this.emit('github', 'issue.node.failed', {
      workItemId: String(issue.number),
      runId: run.runId,
      details: {
        nodeKey: run.nodeKey,
        error: (error ?? 'unknown error').slice(0, 2000),
        inputTokens: run.inputTokens,
        accumulatedTokens: accumulated,
        tokensConsumed,
        // 两种口径并存：transcript 真实值与注入估算，看板/告警按需取用
        tokens: run.tokens,
        attempt: check.attempt,
        policy: { maxAttempts: policy.maxAttempts, onExhausted: policy.onExhausted },
        // 耗时闭环：控制面口径 dispatch→结果，及执行面自报口径
        endedAt: run.endedAt,
        durationMs: run.durationMs,
        workerDurationMs: opts.durationMs,
        // 环境问题（preflight 失败）与业务执行失败在此分流
        preflight: opts.preflight,
      },
    });

    if (!check.allowed) {
      this.emit('harness', 'circuit_breaker.tripped', {
        runId: run.runId,
        workItemId: String(run.issueNumber),
        details: {
          nodeKey: run.nodeKey,
          reason: check.reason,
          onExhausted: policy.onExhausted,
          accumulatedTokens: accumulated,
        },
      });

      // 熔断后处置由模板声明决定：转人工 / 直接失败 / 跳过该节点。
      // 为什么不能统一转人工：QA 这类非关键节点失败时，阻塞整条流程等人放行
      // 的代价高于放行；而编码节点失败静默跳过会产出错误代码。策略属于业务判断，
      // 所以由模板而非引擎决定 —— 与「业务模板 vs 通用引擎」分离原则一致。
      if (policy.onExhausted === 'skip') {
        await this.advance(
          issue,
          run,
          `节点 \`${run.nodeKey}\` 已熔断，按策略跳过：${check.reason}`,
          check.attempt,
        );
        return;
      }

      if (policy.onExhausted === 'fail') {
        await this.deps.github.addComment(
          issue.number,
          `### ⛔ 熔断触发（按策略终止）\n\n节点 \`${run.nodeKey}\` 已中止：${check.reason}`,
        );
        await this.deps.github.setLabels(issue.number, ['node:failed']);
        this.deps.circuitBreaker.reset(runKey);
        return;
      }

      // 默认 hitl：挂起等人工放行。挂起标签会让 detectNode 判定 suspended，
      // 调度不再派发该 issue，直到人工摘除标签。
      await this.deps.github.addComment(
        issue.number,
        `### 🛑 熔断触发\n\n节点 \`${run.nodeKey}\` 已中止：${check.reason}\n\n需要人工介入。`,
      );
      await this.deps.github.setLabels(issue.number, [HITL_WAITING]);
      // 进入时刻由本方写入，最精确；摘除时刻靠 trackHitl 在 tick 中对比标签差分
      this.hitlSince.set(issue.number, {
        nodeKey: run.nodeKey,
        enteredAt: Date.now(),
        runId: run.runId,
        inferred: false,
      });
      this.emit('harness', 'hitl.entered', {
        runId: run.runId,
        workItemId: String(issue.number),
        details: { nodeKey: run.nodeKey, reason: check.reason, inferred: false },
      });
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
        `_熔断检查通过，剩余重试次数：${policy.maxAttempts - check.attempt}_`,
      ].join('\n'),
    );
  }

  /**
   * 推进到 pipeline 的下一节点；已是末节点则打 done 并关闭 issue。
   * completed 与「熔断后跳过」共用，保证两条路径的收尾行为一致。
   *
   * runId 必须随事件带出：Dashboard 与通知卡片靠它把「节点流转」关联回
   * 具体那次执行，丢了就只能看到孤立的 issue 级事件。
   *
   * @param attempt 该节点累计执行次数：成功路径为「历史失败 + 1」，跳过路径为失败次数
   */
  private async advance(issue: Issue, run: Run, comment: string, attempt: number) {
    const next =
      this.deps.config.pipeline[this.deps.config.pipeline.indexOf(run.nodeKey) + 1];

    await this.deps.github.addComment(issue.number, comment);

    // 节点级观测字段：attempt 还原返工/重试，endedAt+durationMs 闭合节点耗时。
    // 执行面口径耗时保留在 worker run.completed 原始事件中（同 runId 可关联）。
    // nodeKey 必须随两个事件都带：评分按「刚执行完的节点」归属这些指标，
    // 不能让消费方从 from/to 或事件顺序反推（末节点 completed 没有 to）。
    const nodeMetrics = {
      nodeKey: run.nodeKey,
      attempt,
      endedAt: run.endedAt,
      durationMs: run.durationMs,
      tokens: run.tokens,
      // 成功路径也要带注入估算：无 transcript 时评分用它做耗时/返工的兜底口径
      inputTokens: run.inputTokens,
    };

    if (next) {
      await this.deps.github.setLabels(issue.number, [`node:${next}`]);
      this.emit('github', 'issue.node.advanced', {
        workItemId: String(issue.number),
        runId: run.runId,
        details: { from: run.nodeKey, to: next, ...nodeMetrics },
      });
      return;
    }

    await this.deps.github.setLabels(issue.number, ['node:done']);
    await this.deps.github.closeIssue(issue.number);
    this.emit('github', 'issue.completed', {
      workItemId: String(issue.number),
      runId: run.runId,
      details: nodeMetrics,
    });
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
