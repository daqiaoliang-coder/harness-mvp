import path from 'node:path';
import fs from 'node:fs/promises';
import { WebSocket } from 'ws';
import type { GatewayToWorker, WorkerToGateway } from '@harness/shared';
import { loadConfig } from './config.js';
import { runAgent, type AgentHandle } from './pty.js';
import { loadTemplate, renderPrompt } from './templates.js';

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
    console.error('[loop-node] ws 错误:', err.message);
  });
}

async function handleLaunch(msg: Extract<GatewayToWorker, { type: 'launch' }>) {
  const { runId, nodeKey, workItemId, context } = msg;
  console.log(`[loop-node] ▶ launch runId=${runId} node=${nodeKey} issue=#${workItemId}`);

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

  const prompt = renderPrompt(template, context);
  const workdir = path.join(config.workspaceDir, runId);
  await fs.mkdir(workdir, { recursive: true });
  const promptFile = path.join(workdir, 'prompt.md');
  await fs.writeFile(promptFile, prompt, 'utf8');

  const args = [...config.agentArgs, promptFile];
  let output = '';

  const handle = runAgent({
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

  activeRuns.set(runId, handle);
}

connect();
