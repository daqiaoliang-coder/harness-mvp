import { spawn } from 'node:child_process';

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
