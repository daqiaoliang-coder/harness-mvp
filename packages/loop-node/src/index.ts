import path from 'node:path';
import fs from 'node:fs/promises';
import { WebSocket } from 'ws';
import type { GatewayToWorker, WorkerToGateway } from '@harness/shared';
import { loadConfig } from './config.js';
import { runAgent, type AgentHandle } from './pty.js';
import { loadTemplate, renderPrompt } from './templates.js';

const config = loadConfig();

interface RunSession {
  runId: string;
  handle: AgentHandle;
  /** 断线后孤儿化：kill 触发的退出不再上报（gateway 已释放该 run） */
  orphaned: boolean;
  /** 结果是否已终态上报，防止 onExit/超时路径重复发送 */
  settled: boolean;
  killTimer?: NodeJS.Timeout;
  idleTimer?: NodeJS.Timeout;
  /** 默认终止信号无效时的 SIGKILL 兜底 */
  killGuard?: NodeJS.Timeout;
}

const activeRuns = new Map<string, RunSession>();
let socket: WebSocket | null = null;

// 任何漏接的 rejection / 异常都不应杀死长驻进程：
// 宁可带着日志继续服务后续 run，也不要让一次偶发错误拖垮整个 node。
process.on('unhandledRejection', (err) => {
  console.error('[loop-node] unhandledRejection（已兜底，不退出）:', err);
});
process.on('uncaughtException', (err) => {
  console.error('[loop-node] uncaughtException（已兜底，不退出）:', err);
});

function send(msg: WorkerToGateway) {
  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(msg));
  }
}

function sendRunFailure(runId: string, error: string) {
  console.error(`[loop-node] ✖ runId=${runId} ${error}`);
  send({ type: 'run.result', runId, status: 'failed', error });
}

function clearSessionTimers(s: RunSession) {
  clearTimeout(s.killTimer);
  clearTimeout(s.idleTimer);
  clearTimeout(s.killGuard);
}

// ---------------------------------------------------------------------------
// WS 连接：指数退避重连 + 半开检测
// ---------------------------------------------------------------------------

const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30_000;
let reconnectDelay = RECONNECT_BASE_MS;

// TCP 半开防护：物理链路断开时 close 事件可能永远不来。
// 只要 45s 内没收到 gateway 任何消息（ping 每 15s 一次），就主动断连重连。
const GATEWAY_STALE_MS = 45_000;
const LIVENESS_INTERVAL_MS = 15_000;
let lastGatewayMsgAt = Date.now();
let livenessTimer: NodeJS.Timeout | undefined;

/**
 * 连接关闭/被拒时，旧 run 在 gateway 侧已经释放并可能重新派发。
 * 让旧 agent 继续跑只会在重连后与新 run 并发操作同一个 issue，
 * 因此一律孤儿化并强杀，由 gateway 按 GitHub 标签重新派发（at-least-once）。
 */
function orphanAllRuns(reason: string) {
  for (const s of activeRuns.values()) {
    s.orphaned = true;
    clearSessionTimers(s);
    console.warn(`[loop-node] 连接异常（${reason}），孤儿化并 kill runId=${s.runId}`);
    try {
      s.handle.kill('SIGKILL');
    } catch {
      /* ignore */
    }
  }
  activeRuns.clear();
}

function teardownLiveness() {
  if (livenessTimer) {
    clearInterval(livenessTimer);
    livenessTimer = undefined;
  }
}

function connect() {
  console.log(`[loop-node] 连接 ${config.gatewayUrl} ...`);
  socket = new WebSocket(config.gatewayUrl);
  lastGatewayMsgAt = Date.now();

  socket.on('open', () => {
    send({
      type: 'hello',
      nodeId: config.nodeId,
      token: config.token,
      agents: config.agents,
      version: '0.1.0',
    });
  });

  socket.on('message', async (raw) => {
    lastGatewayMsgAt = Date.now();
    let msg: GatewayToWorker;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    switch (msg.type) {
      case 'hello.ack':
        console.log(`[loop-node] ✅ 已连接，nodeId=${config.nodeId}`);
        reconnectDelay = RECONNECT_BASE_MS;
        break;
      case 'hello.reject':
        // 不 exit：token 修好或 gateway 重启后可自动恢复；close 事件会带退避触发重连。
        console.error(`[loop-node] ❌ 被拒绝：${msg.reason}（不退出，稍后重试）`);
        break;
      case 'launch':
        try {
          await handleLaunch(msg);
        } catch (err) {
          // handleLaunch 内部已对各失败点分别兜底，这里防任何漏网异常变成 rejection。
          sendRunFailure(msg.runId, `launch 处理异常: ${(err as Error).message}`);
        }
        break;
      case 'cancel':
        cancelRun(msg.runId);
        break;
      case 'ping':
        send({ type: 'heartbeat', nodeId: config.nodeId, load: activeRuns.size });
        break;
    }
  });

  socket.on('close', () => {
    teardownLiveness();
    orphanAllRuns('close');
    console.log(`[loop-node] 连接断开，${reconnectDelay}ms 后重连`);
    setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX_MS);
  });

  socket.on('error', (err) => {
    // ws 库在 error 后通常还会触发 close，重连统一由 close 处理，避免重复定时器。
    console.error('[loop-node] ws 错误:', err.message);
  });

  livenessTimer = setInterval(() => {
    if (Date.now() - lastGatewayMsgAt > GATEWAY_STALE_MS) {
      console.warn('[loop-node] 45s 未收到 gateway 消息，判定连接半开，主动断开重连');
      try {
        socket?.terminate();
      } catch {
        /* ignore */
      }
    }
  }, LIVENESS_INTERVAL_MS);
}

function cancelRun(runId: string) {
  const s = activeRuns.get(runId);
  if (!s) return;
  console.log(`[loop-node] 收到 cancel，kill runId=${runId}`);
  clearSessionTimers(s);
  try {
    s.handle.kill('SIGKILL');
  } catch {
    /* ignore */
  }
}

// agent 输出只保留尾部，防止超长会话撑爆内存与 WS 帧（gateway 也只取尾部 3000 字符）
const OUTPUT_MAX = 256 * 1024;
const OUTPUT_KEEP = 64 * 1024;

async function handleLaunch(msg: Extract<GatewayToWorker, { type: 'launch' }>) {
  const { runId, nodeKey, workItemId, context } = msg;
  console.log(`[loop-node] ▶ launch runId=${runId} node=${nodeKey} issue=#${workItemId}`);

  let template;
  try {
    template = await loadTemplate(config.templatesDir, nodeKey);
  } catch (err) {
    sendRunFailure(runId, `模板加载失败: ${(err as Error).message}`);
    return;
  }

  const workdir = path.join(config.workspaceDir, runId);
  let promptFile: string;
  try {
    const prompt = renderPrompt(template, context);
    await fs.mkdir(workdir, { recursive: true });
    promptFile = path.join(workdir, 'prompt.md');
    await fs.writeFile(promptFile, prompt, 'utf8');
  } catch (err) {
    sendRunFailure(runId, `准备工作目录失败: ${(err as Error).message}`);
    return;
  }

  const args = [...config.agentArgs, promptFile];
  let output = '';
  let timedOut: 'total' | 'idle' | null = null;

  const session: RunSession = {
    runId,
    // runAgent 同步返回 handle，紧随其后的 try/catch 中赋值
    handle: undefined as unknown as AgentHandle,
    orphaned: false,
    settled: false,
  };

  const finish = (code: number | null) => {
    if (session.settled) return;
    session.settled = true;
    clearSessionTimers(session);
    activeRuns.delete(runId);
    if (session.orphaned) return;

    const status = timedOut || code !== 0 ? 'failed' : 'completed';
    const error = timedOut
      ? timedOut === 'idle'
        ? `agent 连续 ${config.idleTimeoutMs}ms 无输出，判定卡死`
        : `agent 运行超过 ${config.runTimeoutMs}ms，被强制终止`
      : code === 0
        ? undefined
        : `agent 退出码 ${code}`;
    console.log(`[loop-node] ✔ 退出 runId=${runId} status=${status}`);
    send({ type: 'run.result', runId, status, output, error });
  };

  const onTimeout = (kind: 'total' | 'idle') => {
    if (session.settled || session.orphaned) return;
    timedOut = kind;
    console.warn(`[loop-node] ⏰ runId=${runId} 触发${kind === 'idle' ? '空闲' : '总时长'}超时，终止 agent`);
    try {
      session.handle.kill();
    } catch {
      /* ignore */
    }
    session.killGuard = setTimeout(() => {
      if (!session.settled && !session.orphaned) {
        try {
          session.handle.kill('SIGKILL');
        } catch {
          /* ignore */
        }
      }
    }, 3000);
  };

  try {
    session.handle = runAgent({
      cmd: config.agentCmd,
      args,
      cwd: workdir,
      env: {
        ...process.env,
        HARNESS_RUN_ID: runId,
        HARNESS_NODE: nodeKey,
        HARNESS_WORK_ITEM: workItemId,
      },
      onData: (chunk) => {
        if (session.settled || session.orphaned) return;
        output += chunk;
        if (output.length > OUTPUT_MAX) output = output.slice(-OUTPUT_KEEP);
        clearTimeout(session.idleTimer);
        if (config.idleTimeoutMs > 0) {
          session.idleTimer = setTimeout(() => onTimeout('idle'), config.idleTimeoutMs);
        }
        send({ type: 'run.progress', runId, chunk });
      },
      onExit: (code) => finish(code),
    });
  } catch (err) {
    sendRunFailure(runId, `agent 启动失败: ${(err as Error).message}`);
    return;
  }

  if (config.runTimeoutMs > 0) {
    session.killTimer = setTimeout(() => onTimeout('total'), config.runTimeoutMs);
  }
  if (config.idleTimeoutMs > 0) {
    session.idleTimer = setTimeout(() => onTimeout('idle'), config.idleTimeoutMs);
  }
  activeRuns.set(runId, session);
}

const shutdown = () => {
  console.log('\n[loop-node] 关闭中...');
  teardownLiveness();
  for (const s of activeRuns.values()) {
    clearSessionTimers(s);
    try {
      s.handle.kill('SIGKILL');
    } catch {
      /* ignore */
    }
  }
  activeRuns.clear();
  try {
    socket?.close();
  } catch {
    /* ignore */
  }
  process.exit(0);
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

connect();
