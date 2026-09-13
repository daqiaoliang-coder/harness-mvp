/**
 * 前置环境检查（preflight）
 *
 * 在每次收到 gateway 的 launch 指令、真正执行节点之前运行（见 index.ts 的 handleLaunch）。
 * 检查项通过配置驱动，支持两种形式：
 *   - envVar：仅校验环境变量是否存在（非空），不读取其值，避免凭据泄漏到日志；
 *   - command + args：以 argv 形式执行可执行文件（execFile，不经 shell），退出码 0 视为通过。
 *
 * 检查集分两层：BASE_CHECKS 是恒定的工具链/网络检查；
 * resolvePreflightConfig 再按 PREFLIGHT_* 环境变量追加凭证、仓库访问、CLI 登录态检查，
 * 使 mock 模式零配置可跑、real 模式按需加固。
 *
 * 检查结果分级（CheckLevel）：
 *   - BLOCKER：不通过则上报 run.result: failed 并终止本次运行；
 *   - WARNING：不通过仅打印告警，不阻断执行；
 *   - INFO：参考信息（当前默认检查中未使用）。
 */
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);

/** 检查结果的严重级别，调用方据此决定是阻断运行还是仅告警 */
export type CheckLevel = 'BLOCKER' | 'WARNING' | 'INFO';

/** 单项检查的结果。ok=false 时 message 为失败原因（已截断），用于上报 gateway / 控制台展示 */
export interface CheckResult {
  name: string;
  level: CheckLevel;
  ok: boolean;
  message: string;
}

export interface PreflightCheck {
  name: string;
  level: CheckLevel;
  /** 可执行文件名（argv 形式，不经 shell） */
  command?: string;
  /** command 的参数列表 */
  args?: string[];
  /** 检查某环境变量是否已设置（凭证有效性） */
  envVar?: string;
}

export interface PreflightConfig {
  checks: PreflightCheck[];
}

/**
 * 基础检查：工具链 + 网络，任何模式都需要，恒定 BLOCKER。
 *
 * 用 argv 形式（command + args）而非 shell 字符串：检查项里的仓库 URL、
 * agent 命令等可能来自环境变量，经 shell 拼接会有命令注入风险。
 * execFile 不走 shell，参数原样传递，从根上消除注入面。
 */
export const BASE_CHECKS: PreflightCheck[] = [
  { name: 'git', level: 'BLOCKER', command: 'git', args: ['--version'] },
  { name: 'node', level: 'BLOCKER', command: 'node', args: ['--version'] },
  {
    name: 'network',
    level: 'BLOCKER',
    command: 'curl',
    args: ['-sf', '--max-time', '5', 'https://api.github.com'],
  },
];

/** 向后兼容：仅含基础检查的默认配置。 */
export const DEFAULT_PREFLIGHT: PreflightConfig = { checks: BASE_CHECKS };

/**
 * 按运行时上下文组装完整检查集。
 *
 * 凭证 / 仓库访问 / CLI 登录态属于「真实环境才需要」，由环境变量声明，
 * 未声明则不检查 —— mock 模式零配置可跑，real 模式按需加固：
 *  - PREFLIGHT_ENV_VARS=GITHUB_TOKEN,OPENAI_API_KEY  凭证有效性（BLOCKER）
 *  - PREFLIGHT_GIT_REMOTE=<repo-url>                 仓库访问权限（BLOCKER）
 *  - PREFLIGHT_AGENT_LOGIN_CHECK='gh auth status'    agent CLI 登录态（BLOCKER）
 *
 * 为什么检查项配置驱动而非写死：需要哪些凭证、访问哪个仓库、用什么命令
 * 验证登录态，都是部署相关的。引擎只提供检查框架与分级语义，具体检查项
 * 交给配置 —— 与「业务模板 vs 通用引擎」是同一套分离原则。
 */
export function resolvePreflightConfig(
  env: NodeJS.ProcessEnv = process.env,
): PreflightConfig {
  const checks: PreflightCheck[] = [...BASE_CHECKS];

  for (const v of splitList(env.PREFLIGHT_ENV_VARS)) {
    checks.push({ name: `cred:${v}`, level: 'BLOCKER', envVar: v });
  }

  // git ls-remote 一次验证「凭证有效 + 仓库可达 + 有读权限」三件事。
  const remote = env.PREFLIGHT_GIT_REMOTE?.trim();
  if (remote) {
    checks.push({
      name: 'repo-access',
      level: 'BLOCKER',
      command: 'git',
      args: ['ls-remote', '--heads', remote],
    });
  }

  // agent CLI 登录态：业务自定义验证命令（按空白拆成 argv）。
  const loginCheck = env.PREFLIGHT_AGENT_LOGIN_CHECK?.trim();
  if (loginCheck) {
    const [cmd, ...args] = loginCheck.split(/\s+/);
    checks.push({ name: 'agent-login', level: 'BLOCKER', command: cmd, args });
  }

  return { checks };
}

function splitList(raw?: string): string[] {
  return (raw ?? '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
}

/**
 * 顺序执行全部检查并收集结果。
 * 注意：单项检查失败不会抛错中断，而是记录 ok=false 后继续，
 * 这样一次运行就能把所有缺失项（如 git 和网络同时不可用）一次性暴露给调用方。
 */
export async function runPreflight(config: PreflightConfig): Promise<CheckResult[]> {
  const results: CheckResult[] = [];

  for (const check of config.checks) {
    // 环境变量检查：只判断有无，消息中只带变量名不带值，防止密钥进入日志
    if (check.envVar) {
      const val = process.env[check.envVar];
      results.push({
        name: check.name,
        level: check.level,
        ok: !!val,
        message: val ? `${check.envVar} 已设置` : `${check.envVar} 未设置`,
      });
      continue;
    }

    // 命令检查：10s 超时兜底（命令自身也可再设更短超时，如 curl 的 --max-time 5），
    // 失败信息截断到 200 字符，避免超长 stderr 撑大上报给 gateway 的 run.result
    if (check.command) {
      try {
        await execFileAsync(check.command, check.args ?? [], {
          timeout: 10_000,
          // git 在缺凭证时默认会弹交互式提示并挂起；强制非交互，让它直接失败。
          env: { ...process.env, GIT_TERMINAL_PROMPT: '0' },
        });
        results.push({ name: check.name, level: check.level, ok: true, message: '通过' });
      } catch (err) {
        results.push({
          name: check.name,
          level: check.level,
          ok: false,
          message: (err as Error).message.slice(0, 200),
        });
      }
    }
  }

  return results;
}
