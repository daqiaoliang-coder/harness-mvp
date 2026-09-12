import { loadConfig } from './config.js';
import { EventStore } from './db.js';
import { EventBus } from './bus.js';
import { createGithubClient } from './github.js';
import { Scheduler } from './scheduler.js';
import { createHttpApp } from './http.js';
import { attachWebSocketServer } from './ws.js';

const config = loadConfig();
const store = new EventStore(config.dbPath);
const bus = new EventBus();
const github = createGithubClient(config);
const scheduler = new Scheduler({ config, store, bus, github });

const app = createHttpApp({ config, store, bus, scheduler });
const server = app.listen(config.httpPort);

server.on('error', (err: NodeJS.ErrnoException) => {
  if (err.code === 'EADDRINUSE') {
    console.error(
      `[gateway] 端口 ${config.httpPort} 已被占用，可能已有一个 gateway 在运行。\n` +
        `         可用 lsof -nP -iTCP:${config.httpPort} -sTCP:LISTEN 查看占用进程，\n` +
        `         用 kill <PID> 结束后再启动，或设置 HTTP_PORT=<其他端口>。`,
    );
  } else {
    console.error('[gateway] 服务启动失败：', err);
  }
  process.exit(1);
});

server.once('listening', () => {
  console.log(`[gateway] HTTP   http://localhost:${config.httpPort}`);
  console.log(`[gateway] SSE    http://localhost:${config.httpPort}/api/events/stream`);
  console.log(`[gateway] 看板   http://localhost:${config.httpPort}/dashboard/`);
  console.log(`[gateway] GitHub 模式 = ${config.github.mode}`);
  console.log(`[gateway] 流水线  = ${config.pipeline.join(' → ')}`);
});

const wss = attachWebSocketServer({ server, config, store, bus, scheduler });
scheduler.start();

const shutdown = () => {
  console.log('\n[gateway] 关闭中...');
  scheduler.stop();
  wss.close();
  server.close(() => process.exit(0));
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
