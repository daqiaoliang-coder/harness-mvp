/**
 * Gateway 组合根/启动入口：按依赖顺序构造 config → EventStore/EventBus → GitHub client /
 * 熔断器 → Scheduler → HTTP 与 WS，最后启动轮询。SIGINT/SIGTERM 触发优雅关闭，
 * 顺序为停调度（不再派发新 run）→ 关 WS（拒绝新 worker 消息）→ 关 HTTP 后退出。
 */
import { loadConfig } from './config.js';
import { EventStore } from './db.js';
import { EventBus } from './bus.js';
import { createGithubClient } from './github.js';
import { Scheduler } from './scheduler.js';
import { CircuitBreaker } from './circuit-breaker.js';
import { createHttpApp } from './http.js';
import { attachWebSocketServer } from './ws.js';
import { startFeishuNotifier } from './notifier.js';

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

// 飞书通知必须在 scheduler.start() 之前订阅：bus 事件不重放，
// 晚订阅会漏掉启动后第一轮 tick 派发的 run.dispatched。
// 未配置 webhook 时 startFeishuNotifier 内部为空操作，不阻断启动。
const stopNotifier = startFeishuNotifier({ config, bus });

scheduler.start();

/**
 * 跟踪所有活动连接（WS worker + SSE 看板 + 普通 HTTP）。
 *
 * 为什么必须自己跟踪：server.close() 只是「停止接受新连接」，
 * 它不会断开已经建立的长连接。而本服务恰恰以长连接为主 ——
 * worker 常驻 WS、看板常驻 SSE（还各带一个 15s keepalive 定时器）。
 * 只要有任一客户端连着，close 的回调就永不触发，process.exit 永不调用：
 * SIGTERM 只打印「关闭中...」然后进程挂住，Ctrl+C 退出 npm run dev
 * 会留下僵尸 gateway 继续占着端口。
 *
 * 在 connection 事件统一收集即可覆盖全部三类：WS 是 HTTP 升级而来，
 * SSE 是普通 HTTP 长响应，底层都是同一个 net.Socket。
 */
const sockets = new Set<import('node:net').Socket>();
server.on('connection', (socket) => {
  sockets.add(socket);
  socket.on('close', () => sockets.delete(socket));
});

/** 强制退出的最后兜底：销毁连接仍未能退出时，不无限期挂住。 */
const HARD_EXIT_MS = 3_000;

let shuttingDown = false;
const shutdown = (signal: string) => {
  // 幂等：SIGINT 连按两次、或 SIGTERM 与 SIGINT 相继到达时只关停一次
  if (shuttingDown) return;
  shuttingDown = true;

  console.log(`\n[gateway] 收到 ${signal}，关闭中...`);

  // 1. 停调度与健康检查，不再派发新任务
  scheduler.stop();

  // 2. 主动断开所有长连接。terminate/destroy 是立即的，不等对端握手
  for (const client of wss.clients) client.terminate();
  wss.close();
  for (const socket of sockets) socket.destroy();
  sockets.clear();

  // 3. 关闭 HTTP 服务；正常情况下此时已无连接，回调会立即触发
  server.close(() => process.exit(0));

  // 4. 兜底：若仍有连接未释放（如 keepalive 定时器持有引用），强制退出
  const hardExit = setTimeout(() => {
    console.error(`[gateway] 关停超过 ${HARD_EXIT_MS}ms 仍未完成，强制退出`);
    process.exit(1);
  }, HARD_EXIT_MS);
  // unref：该定时器不应阻止进程自然退出
  hardExit.unref();
};

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
