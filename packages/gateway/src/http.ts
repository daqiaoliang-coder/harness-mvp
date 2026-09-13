/**
 * 控制面 HTTP（Express）：只读查询 API、dashboard 静态页，以及 /api/events/stream（SSE）。
 * Worker 不走 HTTP——任务派发/回报通道在 ws.ts；SSE 只服务看板这类只读订阅方，
 * 支持凭 Last-Event-ID（= 事件 seq）断线续传。
 */
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import type { GatewayConfig } from './config.js';
import type { EventStore } from './db.js';
import type { EventBus } from './bus.js';
import type { Scheduler } from './scheduler.js';

interface Ctx {
  config: GatewayConfig;
  store: EventStore;
  bus: EventBus;
  scheduler: Scheduler;
}

const here = path.dirname(fileURLToPath(import.meta.url));
const dashboardDir = path.resolve(here, '../../dashboard');

export function createHttpApp(ctx: Ctx) {
  const app = express();
  app.use(express.json());

  app.get('/api/health', (_req, res) => {
    res.json({
      ok: true,
      workers: ctx.scheduler.workerCount(),
      runs: ctx.scheduler.listRuns().length,
    });
  });

  app.get('/api/events', (req, res) => {
    const after = Number(req.query.after ?? 0);
    const limit = Number(req.query.limit ?? 500);
    res.json(ctx.store.listAfter(after, limit));
  });

  app.get('/api/runs', (_req, res) => {
    res.json(ctx.scheduler.listRuns());
  });

  app.get('/api/events/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders?.();

    const lastId = Number(
      req.headers['last-event-id'] ?? (req.query.lastEventId as string | undefined) ?? 0,
    );

    // 先按 Last-Event-ID 同步补发离线期间事件，再订阅实时总线：
    // 两步之间无 await，事件循环不会插入新写入，故既不丢事件也不产生空档
    for (const ev of ctx.store.listAfter(lastId)) {
      res.write(`id: ${ev.seq}\ndata: ${JSON.stringify(ev)}\n\n`);
    }

    const off = ctx.bus.onEvent((ev) => {
      res.write(`id: ${ev.seq}\ndata: ${JSON.stringify(ev)}\n\n`);
    });

    // SSE 注释帧保活，穿透反向代理的空闲断流；连接关闭时必须清定时器并退订，否则句柄/监听器泄漏
    const keepalive = setInterval(() => res.write(': ping\n\n'), 15_000);
    req.on('close', () => {
      clearInterval(keepalive);
      off();
    });
  });

  app.use('/dashboard', express.static(dashboardDir));
  app.get('/', (_req, res) => res.redirect('/dashboard/'));

  return app;
}
