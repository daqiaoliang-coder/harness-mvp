import path from 'node:path';
import fs from 'node:fs/promises';
import { WebSocket } from 'ws';
import type { GatewayToWorker, WorkerToGateway } from '@harness/shared';
import { pruneContext } from '@harness/shared';
import { loadConfig } from './config.js';
import { runAgent, type AgentHandle } from './pty.js';
import { loadTemplate, renderPrompt } from './templates.js';
import { runPreflight, DEFAULT_PREFLIGHT } from './preflight.js';

const config = loadConfig();
const activeRuns = new Map<string, AgentHandle>();
let socket: WebSocket | null = null;

function send(msg: WorkerToGateway) {
  if (socket && socket.readyState === 1) {
    socket.send(JSON.stringify(msg));
  }
}

function connect() {
  console.log(`[loop-node] 连接 ${config.gatewayUrl} ...`);
  socket = new WebSocket(config.gatewayUrl);

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
    let msg: GatewayToWorker;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    switch (msg.type) {
      case 'hello.ack':
        console.log(`[loop-node] ✅ 已连接，nodeId=${config.nodeId}`);
        break;
      case 'hello.reject':
        console.error(`[loop-node] ❌ 被拒绝：${msg.reason}`);
        process.exit(1);
        break;
      case 'launch':
        await handleLaunch(msg);
        break;
      case 'cancel':
        activeRuns.get(msg.runId)?.kill();
        break;
      case 'ping':
        send({ type: 'heartbeat', nodeId: config.nodeId, load: activeRuns.size });
        break;
    }
  });

  socket.on('close', () => {
    console.log('[loop-node] 连接断开，2s 后重连');
    setTimeout(connect, 2000);
  });

  socket.on('error', (err) => {
    console.error('[loop-node] ws 错误:', err);
  });
}

async function handleLaunch(msg: Extract<GatewayToWorker, { type: 'launch' }>) {
  const { runId, nodeKey, workItemId, context } = msg;
  console.log(`[loop-node] ▶ launch runId=${runId} node=${nodeKey} issue=#${workItemId}`);

  // ---- P0: 前置检查 ----
  const preflightResults = await runPreflight(DEFAULT_PREFLIGHT);
  const blockers = preflightResults.filter((r) => r.level === 'BLOCKER' && !r.ok);
  if (blockers.length > 0) {
    const detail = blockers.map((b) => `${b.name}: ${b.message}`).join('\n');
    console.log(`[loop-node] ⛔ 前置检查未通过:\n${detail}`);
    send({
      type: 'run.result',
      runId,
      status: 'failed',
      error: `前置检查未通过:\n${detail}`,
    });
    return;
  }
  const warnings = preflightResults.filter((r) => r.level === 'WARNING' && !r.ok);
  if (warnings.length > 0) {
    console.log(
      `[loop-node] ⚠️ 前置检查告警: ${warnings.map((w) => w.name).join(', ')}`,
    );
  }

  // ---- 模板加载 ----
  let template;
  try {
    template = await loadTemplate(config.templatesDir, nodeKey);
  } catch (err) {
    send({
      type: 'run.result',
      runId,
      status: 'failed',
      error: `模板加载失败: ${(err as Error).message}`,
    });
    return;
  }

  // ---- P0: 上下文预算与裁剪 ----
  const { pruned, usage } = pruneContext(context, template.budget);
  if (usage.exceeded) {
    console.log(
      `[loop-node] ⚠️ 上下文超预算，已截断字段: ${usage.exceededFields.join(', ')}`,
    );
  }
  const prompt = renderPrompt(template, pruned);

  const workdir = path.join(config.workspaceDir, runId);
  await fs.mkdir(workdir, { recursive: true });
  const promptFile = path.join(workdir, 'prompt.md');
  await fs.writeFile(promptFile, prompt, 'utf8');

  const args = [...config.agentArgs, promptFile];
  let output = '';

  // runAgent 可能同步抛出：node-pty 的 pty.fork 在命令不存在、无执行权限、
  // spawn-helper 缺权限等场景下直接抛（posix_spawnp failed）。
  // handleLaunch 是 async，未捕获会变成 rejected promise，
  // 而 socket.on('message', async ...) 的返回值无人处理 → unhandledRejection
  // → Node 22 默认行为是**整个 loop-node 进程崩溃退出**。
  // 一个 agent 起不来不该拖死长驻的执行节点，故在此按失败 run 上报。
  let handle: AgentHandle;
  try {
    handle = runAgent({
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
        output += chunk;
        send({ type: 'run.progress', runId, chunk });
      },
      onExit: (code) => {
        activeRuns.delete(runId);
        console.log(`[loop-node] ✔ 退出 runId=${runId} code=${code}`);
        send({
          type: 'run.result',
          runId,
          status: code === 0 ? 'completed' : 'failed',
          output,
          error: code === 0 ? undefined : `agent 退出码 ${code}`,
        });
      },
    });
  } catch (err) {
    console.error(`[loop-node] ❌ agent 启动失败 runId=${runId}:`, (err as Error).message);
    send({
      type: 'run.result',
      runId,
      status: 'failed',
      error: `agent 启动失败: ${(err as Error).message}`,
    });
    return;
  }

  activeRuns.set(runId, handle);
}

/**
 * 进程级异常兜底。
 *
 * loop-node 是长驻执行节点，一次偶发错误不该让它整体退出——退出会丢掉
 * 该节点上所有在跑的 run，且需要人工重新拉起。这里只记录不退出，
 * 具体 run 的成败仍由 handleLaunch / onExit 各自按失败上报。
 *
 * 注意 unhandledRejection 在 Node 15+ 默认等同 uncaughtException 会终止进程，
 * 显式挂载监听器即可覆盖该默认行为。
 */
process.on('unhandledRejection', (reason) => {
  console.error('[loop-node] 未处理的 Promise rejection（已兜底，进程继续运行）:', reason);
});

process.on('uncaughtException', (err) => {
  console.error('[loop-node] 未捕获异常（已兜底，进程继续运行）:', err);
});

connect();
