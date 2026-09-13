import { loadConfig } from './config.js';
import { EventStore } from './db.js';
import { EventBus } from './bus.js';
import { createGithubClient } from './github.js';
import { Scheduler } from './scheduler.js';
import { CircuitBreaker } from './circuit-breaker.js';
import { createHttpApp } from './http.js';
import { attachWebSocketServer } from './ws.js';

const config = loadConfig();
const store = new EventStore(config.dbPath);
const bus = new EventBus();
const github = createGithubClient(config);
const circuitBreaker = new CircuitBreaker();
const scheduler = new Scheduler({ config, store, bus, github, circuitBreaker });

const app = createHttpApp({ config, store, bus, scheduler });
const server = app.listen(config.httpPort, () => {
  console.log(`[gateway] HTTP   http://localhost:${config.httpPort}`);
  console.log(`[gateway] SSE    http://localhost:${config.httpPort}/api/events/stream`);
  console.log(`[gateway] 看板   http://localhost:${config.httpPort}/dashboard/`);
  console.log(`[gateway] GitHub 模式 = ${config.github.mode}`);
  console.log(`[gateway] 流水线  = ${config.pipeline.join(' → ')}`);
  console.log(`[gateway] P0 成本治理已启用：上下文预算 / 熔断 / 前置检查`);
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
