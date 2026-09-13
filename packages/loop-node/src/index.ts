/**
 * loop-node（执行面 worker）入口：不监听端口，主动外连 Gateway 的 WebSocket，
 * hello 握手声明 nodeId / agents / token；收到 launch 后依次执行
 * 前置检查 → 加载模板 → 上下文裁剪 → prompt 落盘 → node-pty 拉起 agent CLI，
 * agent 输出逐 chunk 以 run.progress 流式回报，进程退出后回 run.result。
 * worker 自身不持久化任务状态，重连后做什么完全由 Gateway 按 issue 标签重新决定。
 */
import path from 'node:path';
import fs from 'node:fs/promises';
import { WebSocket } from 'ws';
import type { GatewayToWorker, RetryPolicy, WorkerToGateway } from '@harness/shared';
import { pruneContext } from '@harness/shared';
import { loadConfig } from './config.js';
import { runAgent, type AgentHandle } from './pty.js';
import { loadTemplate, renderPrompt } from './templates.js';
import { runPreflight, resolvePreflightConfig } from './preflight.js';
import { createOutputBuffer } from './output-buffer.js';

const config = loadConfig();

/**
 * 单个在跑 run 的执行槽。
 *
 * 为什么不能只存 handle：超时兜底需要能取消定时器，终态需要「恰好一次」保证。
 * 超时 kill 与进程自然退出会竞争 —— 两者都会走到 onExit，若不设 settled 标志，
 * 同一个 run 会上报两次 run.result，Gateway 侧第二次会因找不到 run 被丢弃
 * （幸运情况）或误清接手同一 issue 的新 run 的锁（糟糕情况）。
 */
interface RunSlot {
  /**
   * agent 进程句柄。登记瞬间为 null：runAgent 返回前 slot 就要先建好，
   * 因为 onData 回调可能在 armTimeouts 之前触发。
   * 用显式可空而非类型断言，强制所有 kill 调用点做保护。
   */
  handle: AgentHandle | null;
  /** 总时长 / 空闲 / killGuard 定时器，终态时统一清理 */
  timers: Set<NodeJS.Timeout>;
  /** 终态已上报，后续 onExit 不再重复上报 */
  settled: boolean;
  /** 已发起终止：防止总时长与空闲两个超时临近触发时重复发信号、堆积补刀定时器 */
  terminating: boolean;
  /** 重置空闲计时器（每收到一块输出调用）；未启用空闲超时则为 undefined */
  resetIdle?: () => void;
}

/** 在跑 run 登记表：用于响应 gateway 的 cancel，并以其大小作为 heartbeat 负载上报 */
const activeRuns = new Map<string, RunSlot>();
/** 已被孤儿化（断连强杀）的 run：其后续退出事件不再上报，见 orphanAllRuns */
const orphanedRuns = new Set<string>();
let socket: WebSocket | null = null;

/**
 * 输出有界缓冲：超过 OUTPUT_MAX 时截为尾部 OUTPUT_KEEP。
 *
 * 为什么必须有界：agent 会话输出可能达数百 MB（长推理 + 大量工具调用日志）。
 * 无上限累积会撑爆 worker 内存，且终态 run.result 要把它整个塞进一个 WS 帧。
 * 整条链路都以「尾部即结论」为前提 —— Gateway 侧入库截 800 字符、
 * 写回 issue 评论只取尾部 3000 字符，保留全文没有意义。
 * 代价是丢失会话中段输出，需要完整 transcript 时依赖 agent 自身的本地会话文件。
 */
const OUTPUT_MAX = 256 * 1024;
const OUTPUT_KEEP = 64 * 1024;

/** 重连退避：1s 起、每次翻倍、上限 30s，收到 hello.ack 后重置 */
const BACKOFF_INITIAL_MS = 1_000;
const BACKOFF_MAX_MS = 30_000;
let backoffMs = BACKOFF_INITIAL_MS;

/** 半开自检：正常时 gateway 每 15s 发一个 ping，3 倍周期无任何消息即判定链路已死 */
const GATEWAY_PING_MS = 15_000;
const HALF_OPEN_MS = GATEWAY_PING_MS * 3;
const HALF_OPEN_SCAN_MS = GATEWAY_PING_MS;
let lastGatewayMsgAt = Date.now();
let halfOpenTimer: NodeJS.Timeout | undefined;

// 只在 OPEN(1) 态发送：断连/重连窗口期静默丢弃，避免 ws 在 CLOSING/CLOSED 态 send 抛错
function send(msg: WorkerToGateway) {
  if (socket && socket.readyState === 1) {
    socket.send(JSON.stringify(msg));
  }
}

/**
 * 断连即孤儿化：强杀所有在跑 agent，并标记其后续退出事件不再上报。
 *
 * 为什么必须强杀：Gateway 侧已按 worker_lost 释放了这些 run 的 issue 锁，
 * 下一轮轮询会把同一 issue 派给另一个 worker。若放任本地旧 agent 存活，
 * 两个 agent 会并发读写同一个工作目录/同一个仓库分支 —— 产出互相覆盖，
 * 且这种损坏不会报错，只会得到一份错误的代码。
 *
 * 为什么要标记 orphaned：被 SIGKILL 的 agent 随后会触发 onExit，
 * 而上报通道此时已断（或在重连后指向新的 gateway 会话）。
 * 不标记就会出现「Gateway 早已释放该 run，却收到它的 run.result」，
 * 轻则是噪声事件，重则误清新 run 的锁。
 */
function orphanAllRuns(reason: string) {
  if (activeRuns.size === 0) return;
  console.warn(
    `[loop-node] 连接中断（${reason}），强杀 ${activeRuns.size} 个在跑 run（Gateway 将按标签重派）`,
  );
  for (const [runId, slot] of activeRuns) {
    orphanedRuns.add(runId);
    // 先标记终态并清定时器：被强杀的 agent 随后会触发 onExit，
    // settled 让它走「不上报」分支，定时器不清则会在进程里悬挂到超时才触发
    slot.settled = true;
    clearRunTimers(slot);
    killSlot(slot, 'SIGKILL', `强杀 run ${runId}`);
    activeRuns.delete(runId);
  }
}

/**
 * 向某个 run 的 agent 进程发信号。
 *
 * handle 可能为 null（runAgent 尚未返回）—— 此时进程还不存在，无需处理。
 * kill 也可能抛错（进程已退出），同样无需处理：调用方的意图是「确保它不在跑」，
 * 进程已消失即目标达成。两种情况都不该让调用方崩溃，故统一在此吞掉。
 */
function killSlot(slot: RunSlot, signal?: string, context?: string) {
  if (!slot.handle) return;
  try {
    slot.handle.kill(signal);
  } catch (e) {
    if (context) console.error(`[loop-node] ${context} 失败:`, (e as Error).message);
  }
}

/** 清理某个 run 的全部定时器（总时长 / 空闲 / killGuard）。 */
function clearRunTimers(slot: RunSlot) {
  for (const t of slot.timers) clearTimeout(t);
  slot.timers.clear();
}

/**
 * 双超时兜底：给「失控循环」与「卡死」两种失控形态各设一道硬上限。
 *
 * 为什么两个都要：
 *  - 总时长上限防的是 agent 陷入长循环、持续产出但永不收敛（持续烧 token）；
 *  - 空闲上限防的是 agent 卡在某次工具调用/权限确认上静默等待（不产出也不退出）。
 * 只有总时长上限的话，一个卡死的 agent 要等满 60min 才被回收，
 * 期间它占着 worker 容量，整条流水线停摆。
 *
 * 两者均可设 0 禁用。触发后先发默认终止信号，3s 仍未退出则补 SIGKILL
 * —— 协作式终止给 agent 留清理机会，但不能依赖它一定响应。
 */
function armTimeouts(runId: string, slot: RunSlot, onTimeout: (reason: string) => void) {
  const KILL_GUARD_MS = 3_000;

  // 已终态就不再装定时器。当前调用路径是同步的（runAgent → set → armTimeouts
  // 之间无 await），agent 不可能已退出；但一旦将来在中间插入 await，
  // 已退出的 run 会留下永不触发的定时器，且 clearRunTimers 已经跑过、
  // 没人再清理它们。此处显式拦住这种演化风险。
  if (slot.settled) return;

  const forceKill = (reason: string) => {
    // settled = 终态已上报；terminating = 已发起终止但 agent 还没退出。
    // 两个超时（总时长 / 空闲）可能在临近时刻先后触发，
    // 不设 terminating 就会重复发信号、并堆积多个补刀定时器。
    if (slot.settled || slot.terminating) return;
    slot.terminating = true;
    console.warn(`[loop-node] ⏱ run ${runId} ${reason}，终止 agent`);
    onTimeout(reason);
    // 先发默认信号：给 agent 留清理机会（落盘、关闭子进程）
    killSlot(slot);
    // agent 可能忽略默认信号：补一刀 SIGKILL，确保进程一定被回收
    const guard = setTimeout(() => killSlot(slot, 'SIGKILL'), KILL_GUARD_MS);
    slot.timers.add(guard);
  };

  if (config.runTimeoutMs > 0) {
    slot.timers.add(
      setTimeout(
        () => forceKill(`超过总时长上限 ${config.runTimeoutMs / 1000}s`),
        config.runTimeoutMs,
      ),
    );
  }

  if (config.idleTimeoutMs > 0) {
    // 空闲计时器单独持有：每收到一块输出都要「先清旧的、再排新的」。
    // 若只 add 不 clear，长会话（agent 持续输出上千块）会堆积同样数量的
    // 悬挂定时器 —— 虽有 settled 兜底不会重复上报，但定时器本身就是泄漏。
    let idleTimer: NodeJS.Timeout | undefined;
    const resetIdle = () => {
      if (idleTimer) {
        clearTimeout(idleTimer);
        slot.timers.delete(idleTimer);
      }
      idleTimer = setTimeout(
        () => forceKill(`连续 ${config.idleTimeoutMs / 1000}s 无输出（判定卡死）`),
        config.idleTimeoutMs,
      );
      slot.timers.add(idleTimer);
    };
    resetIdle();
    slot.resetIdle = resetIdle;
  }
}

function startHalfOpenWatch() {
  stopHalfOpenWatch();
  lastGatewayMsgAt = Date.now();
  halfOpenTimer = setInterval(() => {
    const silent = Date.now() - lastGatewayMsgAt;
    if (silent < HALF_OPEN_MS) return;
    // TCP 半开：对端已消失但 close 事件永不到来，必须主动断开才能触发重连
    console.warn(
      `[loop-node] 已 ${Math.round(silent / 1000)}s 未收到 Gateway 任何消息（阈值 ${HALF_OPEN_MS / 1000}s），判定链路半开，主动断开`,
    );
    stopHalfOpenWatch();
    socket?.terminate();
  }, HALF_OPEN_SCAN_MS);
}

function stopHalfOpenWatch() {
  if (halfOpenTimer) {
    clearInterval(halfOpenTimer);
    halfOpenTimer = undefined;
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
    // 任意消息都证明链路活着，刷新半开判定的时间基准
    lastGatewayMsgAt = Date.now();

    switch (msg.type) {
      case 'hello.ack':
        console.log(`[loop-node] ✅ 已连接，nodeId=${config.nodeId}`);
        backoffMs = BACKOFF_INITIAL_MS; // 握手成功，退避归零
        startHalfOpenWatch();
        break;
      case 'hello.reject':
        // 不退出进程：token 被拒是可恢复的运维问题（配置填错、Gateway 正在轮换密钥）。
        // 退出会让长驻节点需要人工重新拉起；改为断开后带退避重试，
        // 修正配置或 Gateway 重启后自动恢复。
        console.error(
          `[loop-node] ❌ 握手被拒绝：${msg.reason}（${backoffMs / 1000}s 后重试，请检查 WORKER_TOKEN）`,
        );
        socket?.terminate();
        break;
      case 'launch':
        await handleLaunch(msg);
        break;
      case 'cancel': {
        const slot = activeRuns.get(msg.runId);
        if (slot) {
          // 主动取消同样不再上报退出事件：Gateway 是取消的发起方，
          // 它已经知道这个 run 结束了，回报反而是噪声
          orphanedRuns.add(msg.runId);
          slot.settled = true;
          clearRunTimers(slot);
          killSlot(slot);
          activeRuns.delete(msg.runId);
        }
        break;
      }
      case 'ping':
        send({ type: 'heartbeat', nodeId: config.nodeId, load: activeRuns.size });
        break;
    }
  });

  socket.on('close', () => {
    stopHalfOpenWatch();
    orphanAllRuns('连接关闭');
    const wait = backoffMs;
    // 指数退避：Gateway 长时间宕机时避免重连风暴打爆它
    backoffMs = Math.min(backoffMs * 2, BACKOFF_MAX_MS);
    console.log(`[loop-node] 连接断开，${wait / 1000}s 后重连`);
    setTimeout(connect, wait);
  });

  socket.on('error', (err) => {
    console.error('[loop-node] ws 错误:', err.message);
  });
}

async function handleLaunch(msg: Extract<GatewayToWorker, { type: 'launch' }>) {
  const { runId, nodeKey, workItemId, context } = msg;
  console.log(`[loop-node] ▶ launch runId=${runId} node=${nodeKey} issue=#${workItemId}`);

  /**
   * 模板先于前置检查加载。
   *
   * 为什么这个顺序：前置检查失败也要回报该节点的重试策略。控制面不读模板，
   * run.result 是策略唯一的传递通道；若前置检查在前、失败即 return，
   * 控制面就拿不到 retry，只能退回全局默认值 —— 环境问题本该按节点风险
   * 判定（QA 节点容忍更多次重试），却变成了统一口径。
   */
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
  const retry: RetryPolicy = template.retry;

  // ---- 前置检查 ----
  const preflightResults = await runPreflight(resolvePreflightConfig());
  const preflight = {
    passed: preflightResults.every((r) => r.ok || r.level !== 'BLOCKER'),
    failed: preflightResults.filter((r) => !r.ok).map((r) => r.name),
  };
  const blockers = preflightResults.filter((r) => r.level === 'BLOCKER' && !r.ok);
  if (blockers.length > 0) {
    const detail = blockers.map((b) => `${b.name}: ${b.message}`).join('\n');
    console.log(`[loop-node] ⛔ 前置检查未通过:\n${detail}`);
    send({
      type: 'run.result',
      runId,
      status: 'failed',
      error: `前置检查未通过:\n${detail}`,
      retry,
      preflight,
    });
    return;
  }
  const warnings = preflightResults.filter((r) => r.level === 'WARNING' && !r.ok);
  if (warnings.length > 0) {
    console.log(
      `[loop-node] ⚠️ 前置检查告警: ${warnings.map((w) => w.name).join(', ')}`,
    );
  }

  // ---- 上下文预算与裁剪 ----
  const { pruned, usage } = pruneContext(context, template.budget);
  if (usage.exceeded) {
    console.log(
      `[loop-node] ⚠️ 上下文超预算，已截断字段: ${usage.exceededFields.join(', ')}` +
        (usage.droppedFields.length
          ? `；已省略字段: ${usage.droppedFields.join(', ')}`
          : ''),
    );
  }
  const prompt = renderPrompt(template, pruned);

  const workdir = path.join(config.workspaceDir, runId);
  await fs.mkdir(workdir, { recursive: true });
  const promptFile = path.join(workdir, 'prompt.md');
  await fs.writeFile(promptFile, prompt, 'utf8');

  const args = [...config.agentArgs, promptFile];

  // ---- 输出有界缓冲（逻辑见 output-buffer.ts，可单测）----
  const outBuf = createOutputBuffer({ max: OUTPUT_MAX, keep: OUTPUT_KEEP });

  /** 超时原因：区分「总时长超限」与「连续无输出」，回报后便于归因 */
  let timeoutReason: string | undefined;

  // slot 先建、handle 后填：armTimeouts 需要 handle 才能 kill，
  // 而 onData 回调又可能早于 armTimeouts 触发（PTY 输出可同步到达），
  // 故 resetIdle 一律用可选调用，未装定时器时是 no-op。
  const slot: RunSlot = {
    handle: null,
    timers: new Set(),
    settled: false,
    terminating: false,
  };

  // runAgent 可能同步抛出：node-pty 的 pty.fork 在命令不存在、无执行权限、
  // spawn-helper 缺权限等场景下直接抛（posix_spawnp failed）。
  // handleLaunch 是 async，未捕获会变成 rejected promise，
  // 而 socket.on('message', async ...) 的返回值无人处理 → unhandledRejection
  // → Node 22 默认行为是**整个 loop-node 进程崩溃退出**。
  // 一个 agent 起不来不该拖死长驻的执行节点，故在此按失败 run 上报。
  try {
    slot.handle = runAgent({
      cmd: config.agentCmd,
      args,
      cwd: workdir,
      env: {
        ...process.env,
        // 注入 run 身份变量，供 agent 或其包装脚本在输出 / 回调中关联本次运行
        HARNESS_RUN_ID: runId,
        HARNESS_NODE: nodeKey,
        HARNESS_WORK_ITEM: workItemId,
      },
      onData: (chunk) => {
        outBuf.append(chunk);
        slot.resetIdle?.(); // 有输出即证明 agent 没卡死，空闲计时重新起算
        send({ type: 'run.progress', runId, chunk });
      },
      onExit: (code) => {
        // 终态恰好一次：超时 kill 与自然退出会竞争，pty/spawn 两条分支
        // 内部各有 exited 去重，这里再用 settled 兜一层 —— 双保险，
        // 因为重复上报 run.result 在 Gateway 侧可能误清新 run 的锁。
        if (slot.settled) return;
        slot.settled = true;
        clearRunTimers(slot);
        activeRuns.delete(runId);

        // 孤儿 run 不上报：Gateway 早已按 worker_lost 释放它的锁，
        // 此时上报轻则是噪声，重则误清接手同一 issue 的新 run 的锁。
        if (orphanedRuns.delete(runId)) {
          console.log(`[loop-node] ⚠️ run ${runId} 已孤儿化，退出事件不再上报（code=${code}）`);
          return;
        }

        // 日志必须与实际 status 一致：超时终止后 PTY 常回报 code=0，
        // 若按 code 打印「✔ 成功」，日志会显示成功而 Gateway 收到的是 failed，
        // 排障时两头对不上 —— 这种不一致比报错更难查。
        const failed = code !== 0 || timeoutReason !== undefined;
        const truncNote = outBuf.truncated ? '（输出已按尾部截断）' : '';
        if (failed) {
          console.warn(
            `[loop-node] ✖ run ${runId} 失败：${timeoutReason ?? `agent 退出码 ${code}`}${truncNote}`,
          );
        } else {
          console.log(`[loop-node] ✔ 退出 runId=${runId} code=${code}${truncNote}`);
        }
        send({
          type: 'run.result',
          runId,
          status: failed ? 'failed' : 'completed',
          output: outBuf.value,
          error: failed
            ? `${timeoutReason ?? `agent 退出码 ${code}`}${truncNote}`
            : undefined,
          retry,
          preflight,
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
      retry,
      preflight,
    });
    return;
  }

  /**
   * 堵住「启动期间断连」的竞态窗口。
   *
   * handleLaunch 是 async：preflight 检查、模板加载、prompt 落盘都要 await。
   * 若断连恰好发生在这期间，orphanAllRuns 遍历 activeRuns 时还看不到本 run
   * （尚未登记），于是不会强杀它；随后这里 set 进去，就留下一个
   * 「连接已断、却仍在跑」的 agent —— 它会和 Gateway 重派的新 run
   * 并发踩同一个工作目录，且这种损坏不报错，只产出错误代码。
   *
   * 因此登记前复查连接状态：已断则立即强杀并按孤儿处理。
   */
  if (!socket || socket.readyState !== 1) {
    console.warn(`[loop-node] ⚠️ run ${runId} 启动期间连接已断，立即强杀（避免与重派 run 并发）`);
    orphanedRuns.add(runId);
    slot.settled = true;
    killSlot(slot, 'SIGKILL');
    return;
  }

  activeRuns.set(runId, slot);
  // 登记完成后再装定时器：此时 handle 已就位，forceKill 一定能拿到它
  armTimeouts(runId, slot, (reason) => {
    timeoutReason = reason;
  });
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

/**
 * 进程级优雅退出。
 *
 * 为什么不能直接退出：loop-node 是 agent 的父进程，PTY 子进程不会随父进程
 * 自动终止。直接 exit 会留下脱离管理的 agent 继续读写工作目录 ——
 * 与断连场景同一种损坏（并发踩同一目录），而且更难发现，因为已经没有任何
 * 组件在跟踪它。因此退出前必须：停半开定时器 → 强杀全部在跑 run → 关连接。
 *
 * 不等待 agent 自行收尾：agent 可能正卡在长推理里，等待会让关停无限期挂住。
 * 残留节点统一由 Gateway 按 issue 标签重派（at-least-once）。
 */
let nodeShuttingDown = false;
const shutdownNode = (signal: string) => {
  if (nodeShuttingDown) return; // 幂等：SIGINT 连按两次只关停一次
  nodeShuttingDown = true;

  console.log(`\n[loop-node] 收到 ${signal}，关闭中...`);
  stopHalfOpenWatch();
  orphanAllRuns(signal);
  try {
    socket?.close();
  } catch {
    /* 连接可能已断，忽略 */
  }
  process.exit(0);
};

process.on('SIGINT', () => shutdownNode('SIGINT'));
process.on('SIGTERM', () => shutdownNode('SIGTERM'));

connect();
