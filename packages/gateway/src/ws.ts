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
      worker.lastSeen = Date.now();

      switch (msg.type) {
        case 'heartbeat':
          worker.busy = msg.load > 0;
          break;

        case 'run.progress':
          emit({
            source: 'worker',
            event: 'run.progress',
            projectId: ctx.config.projectId,
            runId: msg.runId,
            nodeId: worker.nodeId,
            details: { chunk: msg.chunk.slice(0, 800) },
          });
          break;

        case 'run.result':
          worker.busy = false;
          worker.currentRunId = undefined;
          emit({
            source: 'worker',
            event: `run.${msg.status}`,
            projectId: ctx.config.projectId,
            runId: msg.runId,
            nodeId: worker.nodeId,
            details: { error: msg.error },
          });
          try {
            await ctx.scheduler.onRunResult(msg.runId, msg.status, msg.output, msg.error);
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
