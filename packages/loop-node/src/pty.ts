/**
 * agent 进程执行层：优先用 node-pty 以真实 TTY 拉起 agent CLI，
 * 原生模块加载失败时降级为 child_process.spawn（链路可用但丢失 TTY 能力）。
 * 对上层只暴露 onData（流式输出）/ onExit（终态退出码）两个回调，
 * 并保证 onExit 恰好触发一次；spawn 类错误按非零退出码走失败上报，不炸 node 进程。
 */
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export interface RunAgentOptions {
  cmd: string;
  args: string[];
  cwd: string;
  env: Record<string, string | undefined>;
  onData: (chunk: string) => void;
  onExit: (code: number) => void;
}

export interface AgentHandle {
  write: (data: string) => void;
  kill: (signal?: string) => void;
}

/**
 * 修复 node-pty 预编译 spawn-helper 缺失可执行位的问题。
 *
 * 为什么必须自愈：某些环境下 npm 解包会丢掉该文件的可执行位，表现为
 * agent 启动时 `posix_spawnp failed` —— 每个 run 都失败，重试耗尽后熔断
 * 转 HITL。错误信息只说 spawn 失败、不指向权限，极难定位。
 *
 * 为什么放在运行时而不是只留在 dev-real.sh：README 的快速开始是
 * `npm install && npm run dev`，根本不经过那个脚本，照文档操作必然踩坑。
 * 自愈属于「让系统在自己能力范围内恢复」，不该依赖使用者记住额外步骤。
 *
 * 失败不致命：定位不到文件或无写权限时只 warn；PTY 若真起不来，
 * 仍由 handleLaunch 按失败 run 上报，不会静默挂住。
 */
function ensureSpawnHelperExecutable() {
  // win32 走 conpty，没有 posix spawn-helper，无需处理
  if (process.platform === 'win32') return;

  const platform = process.platform.toLowerCase();
  const arch = process.arch === 'arm64' ? 'arm64' : 'x64';
  const rel = path.join('node_modules/node-pty/prebuilds', `${platform}-${arch}`, 'spawn-helper');

  // 从本模块逐级向上找 node_modules：monorepo 下依赖可能被提升到仓库根
  let dir = path.dirname(fileURLToPath(import.meta.url));
  for (let i = 0; i < 8; i++) {
    const helper = path.join(dir, rel);

    if (fs.existsSync(helper)) {
      try {
        fs.accessSync(helper, fs.constants.X_OK);
        return; // 已有可执行位，无需修复
      } catch {
        /* 落到下面的 chmod */
      }
      try {
        fs.chmodSync(helper, 0o755);
        console.warn(
          `[loop-node] 已自动修复 node-pty spawn-helper 可执行位（原缺失会导致 posix_spawnp failed）: ${helper}`,
        );
      } catch (e) {
        console.warn(
          `[loop-node] 无法修复 spawn-helper 可执行位，PTY 启动可能失败: ${(e as Error).message}`,
        );
      }
      return;
    }

    const parent = path.dirname(dir);
    if (parent === dir) return; // 已到文件系统根，仍未找到
    dir = parent;
  }
}

/**
 * 优先使用 node-pty。若原生模块不可用，回退到 child_process.spawn。
 *
 * 为什么首选 PTY：
 *  - Codex / Claude Code 这类 CLI 检测 TTY，非 TTY 下会关闭彩色输出和交互确认
 *  - 走 PTY 才能保留工具调用、权限提示、会话管理这些能力
 *  - agent 自己的 resume 命令依赖本地 transcript，必须让它以为在真实终端里
 *
 * 回退到 spawn 时链路仍然可用，但会丢失 TTY 能力。
 */
let ptyModule: typeof import('node-pty') | null = null;
try {
  ensureSpawnHelperExecutable();
  ptyModule = await import('node-pty');
} catch {
  console.warn(
    '[loop-node] node-pty 不可用，已回退到 child_process.spawn（丢失 TTY 能力）。' +
      '安装编译工具链后可 npm rebuild node-pty 恢复。',
  );
}

export function runAgent(opts: RunAgentOptions): AgentHandle {
  if (ptyModule) {
    // node-pty 的 spawn 失败（如命令不存在）会同步抛出，由调用方 try/catch。
    const proc = ptyModule.spawn(opts.cmd, opts.args, {
      name: 'xterm-256color',
      cols: 200,
      rows: 50,
      cwd: opts.cwd,
      env: opts.env as Record<string, string>,
    });
    // 终态去重：kill 与 onExit 回调存在竞争时，保证上层 onExit 只被调一次
    let exited = false;
    const emitExit = (code: number) => {
      if (exited) return;
      exited = true;
      opts.onExit(code);
    };
    proc.onData(opts.onData);
    proc.onExit(({ exitCode }) => emitExit(exitCode));
    return {
      write: (data) => proc.write(data),
      kill: (signal) => {
        try {
          proc.kill(signal as never);
        } catch {
          /* ignore */
        }
      },
    };
  }

  const proc = spawn(opts.cmd, opts.args, {
    cwd: opts.cwd,
    env: opts.env as NodeJS.ProcessEnv,
  });
  // 终态去重：spawn 分支里 'error'（如命令不存在）之后 Node 通常还会补发 'exit'，
  // 不拦截就会用后一个退出码把 127 失败终态覆盖掉
  let exited = false;
  const emitExit = (code: number) => {
    if (exited) return;
    exited = true;
    opts.onExit(code);
  };
  proc.stdout?.on('data', (d: Buffer) => opts.onData(d.toString()));
  proc.stderr?.on('data', (d: Buffer) => opts.onData(d.toString()));
  // 命令不存在 / 无执行权限等：error 事件无人监听会升级为 uncaughtException。
  // 统一按非零退出码上报，让 scheduler 走失败分支而不是拖死整个 node。
  proc.on('error', (err) => {
    console.error('[loop-node] agent 进程错误:', err.message);
    emitExit(127);
  });
  proc.on('exit', (code: number | null) => emitExit(code ?? 0));
  return {
    write: (data) => {
      proc.stdin?.write(data);
    },
    kill: (signal) => {
      try {
        proc.kill(signal as NodeJS.Signals);
      } catch {
        /* ignore */
      }
    },
  };
}
