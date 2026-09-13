/**
 * Worker 接入通道（WebSocket，路径 /ws）：连接建立后 5s 内必须发 hello 并携带正确 token，
 * 未完成握手的连接按协议码关闭——4001 握手超时、4003 token 无效。
 * worker 的运行时事件（heartbeat / progress / result）在此先落库再广播；
 * 面向看板的实时推送走 http.ts 的 SSE，两条通道分离。
 */
import type { Server } from 'node:http';
import { WebSocketServer } from 'ws';
import type { HarnessEvent, WorkerToGateway } from '@harness/shared';
import type { GatewayConfig } from './config.js';
import type { EventStore } from './db.js';
import type { EventBus } from './bus.js';
import type { Scheduler, WorkerConn } from './scheduler.js';

interface Ctx {
  server: Server;
  config: GatewayConfig;
  store: EventStore;
  bus: EventBus;
  scheduler: Scheduler;
}

export function attachWebSocketServer(ctx: Ctx) {
  const wss = new WebSocketServer({ server: ctx.server, path: '/ws' });

  wss.on('connection', (socket) => {
    let worker: WorkerConn | null = null;
    const helloTimer = setTimeout(() => {
      if (!worker) socket.close(4001, 'hello timeout');
    }, 5000);

    const emit = (ev: Omit<HarnessEvent, 'id' | 'seq' | 'timestamp'>) => {
      ctx.bus.emitEvent(ctx.store.append(ev));
    };

    socket.on('message', async (raw) => {
      let msg: WorkerToGateway;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        return;
      }

      if (msg.type === 'hello') {
        clearTimeout(helloTimer);
        if (msg.token !== ctx.config.workerToken) {
          socket.send(JSON.stringify({ type: 'hello.reject', reason: 'invalid token' }));
          socket.close(4003, 'invalid token');
          return;
        }
        worker = {
          nodeId: msg.nodeId,
          socket,
          agents: msg.agents,
          busy: false,
          lastSeen: Date.now(),
        };
        ctx.scheduler.registerWorker(worker);
        socket.send(
          JSON.stringify({
            type: 'hello.ack',
            nodeId: msg.nodeId,
            serverTime: new Date().toISOString(),
          }),
        );
        emit({
          source: 'harness',
          event: 'worker.connected',
          projectId: ctx.config.projectId,
          nodeId: msg.nodeId,
          details: { agents: msg.agents, version: msg.version },
        });
        return;
      }

      if (!worker) return;
      // 任意消息都算「还活着」：这是 stale 判定唯一的时间依据
      worker.lastSeen = Date.now();

      switch (msg.type) {
        case 'heartbeat':
          // 只记录负载供观测，**不用它覆写 busy**。
          //
          // 为什么：busy 的权威来源是 dispatch（置 true）与 run.result（置 false）。
          // 此前这里用 `msg.load > 0` 覆写 busy，存在一个窄窗 ——
          // gateway 发出 launch 后，worker 要等模板加载、prompt 落盘完成
          // 才把 run 登记进 activeRuns；若恰在此窗口收到 ping，
          // heartbeat 回报 load=0 就会把 gateway 侧的 busy 抹成 false，
          // 同一 worker 可能被派发第二个 run，两个 agent 并发踩同一工作目录。
          //
          // 这个窄窗此前不可达（gateway 从不发 ping，worker 只在收到 ping 时
          // 才回 heartbeat）。健康检查上线后 ping 每 15s 一次，窗口变成真实风险，
          // 因此按「launch/result 为唯一权威」收敛：heartbeat 只刷 lastSeen。
          worker.lastLoad = msg.load;
          break;

        case 'run.progress':
          emit({
            source: 'worker',
            event: 'run.progress',
            projectId: ctx.config.projectId,
            runId: msg.runId,
            nodeId: worker.nodeId,
            // 进度流高频且单 chunk 可能很长，截断到 800 字，避免事件表与 SSE 流量被刷屏
            details: { chunk: msg.chunk.slice(0, 800) },
          });
          break;

        case 'run.result':
          worker.busy = false;
          worker.currentRunId = undefined;
          // worker 层原始终态事件（失败即 run.failed，不带 workItemId）；
          // scheduler 完成 GitHub 回写后会另发带 workItemId 的 issue.node.failed 语义事件
          emit({
            source: 'worker',
            event: `run.${msg.status}`,
            projectId: ctx.config.projectId,
            runId: msg.runId,
            nodeId: worker.nodeId,
            details: {
              error: msg.error,
              // 前置检查未通过即失败：与「业务执行失败」区分开，便于定位环境问题
              preflight: msg.preflight,
            },
          });
          try {
            await ctx.scheduler.onRunResult(msg.runId, msg.status, {
              output: msg.output,
              error: msg.error,
              // 控制面不读模板，策略由执行面解析后回报
              retry: msg.retry,
            });
          } catch (e) {
            // onRunResult 内部已有兜底；这里防止任何未预期异常导致 gateway 进程退出
            console.error('[ws] onRunResult 处理失败:', e);
          }
          break;
      }
    });

    socket.on('close', () => {
      clearTimeout(helloTimer);
      if (worker) {
        // 断连即注销：scheduler 会释放该 worker 在跑 run 的 issue 锁（run.worker_lost），使 issue 可被重新派发
        ctx.scheduler.unregisterWorker(worker.nodeId);
        emit({
          source: 'harness',
          event: 'worker.disconnected',
          projectId: ctx.config.projectId,
          nodeId: worker.nodeId,
        });
      }
    });
  });

  return wss;
}
