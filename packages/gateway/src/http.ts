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

    for (const ev of ctx.store.listAfter(lastId)) {
      res.write(`id: ${ev.seq}\ndata: ${JSON.stringify(ev)}\n\n`);
    }

    const off = ctx.bus.onEvent((ev) => {
      res.write(`id: ${ev.seq}\ndata: ${JSON.stringify(ev)}\n\n`);
    });

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
