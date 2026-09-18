/**
 * Gateway 配置：全部来自环境变量并带本地可跑的默认值。
 * 未显式设置 GITHUB_MODE 时，三项 GitHub 凭据齐全才走 real，否则自动回退 mock（零配置启动）；
 * workerToken 是 WS 握手的唯一鉴权凭据，默认值仅限本地开发。
 */
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export interface GatewayConfig {
  httpPort: number;
  dbPath: string;
  projectId: string;
  workerToken: string;
  pipeline: string[];
  github: {
    mode: 'real' | 'mock';
    token?: string;
    owner?: string;
    repo?: string;
    pollIntervalMs: number;
  };
  worker: {
    /** 给在线 worker 发应用层 ping 的间隔 */
    pingIntervalMs: number;
    /** 多久收不到 worker 任何消息（heartbeat/progress）就判定假死并断连释放 run 锁 */
    staleMs: number;
  };
  /** 飞书通知（方案 A：群自定义机器人 webhook；留空则关闭通知） */
  feishu: {
    webhookUrl?: string;
    /** 机器人启用了「签名校验」时对应的加签密钥 */
    webhookSecret?: string;
  };
  alerts: {
    /**
     * HITL 挂起超过该时长未解除则升级告警（每个挂起只发一次）。
     * 0 = 关闭升级；默认 30 分钟。
     */
    hitlEscalateMs: number;
  };
}

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, '../../..');

export function loadConfig(): GatewayConfig {
  const hasCreds =
    !!process.env.GITHUB_TOKEN && !!process.env.GITHUB_OWNER && !!process.env.GITHUB_REPO;
  // 显式 GITHUB_MODE 优先；缺省时按凭据完整性自动选择，防止「配了一半」却以 real 启动
  const mode = (process.env.GITHUB_MODE as 'real' | 'mock') ?? (hasCreds ? 'real' : 'mock');

  return {
    httpPort: Number(process.env.HTTP_PORT ?? 8787),
    dbPath: process.env.DB_PATH
      ? path.resolve(process.cwd(), process.env.DB_PATH)
      : path.resolve(repoRoot, 'data/events.db'),
    projectId: process.env.PROJECT_ID ?? 'demo',
    workerToken: process.env.WORKER_TOKEN ?? 'dev-token',
    pipeline: (process.env.PIPELINE ?? 'plan,code,test')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean),
    github: {
      mode,
      token: process.env.GITHUB_TOKEN,
      owner: process.env.GITHUB_OWNER,
      repo: process.env.GITHUB_REPO,
      pollIntervalMs: Number(process.env.GITHUB_POLL_MS ?? 5000),
    },
    worker: {
      pingIntervalMs: Number(process.env.WORKER_PING_MS ?? 15_000),
      staleMs: Number(process.env.WORKER_STALE_MS ?? 45_000),
    },
    feishu: {
      webhookUrl: process.env.FEISHU_WEBHOOK_URL || undefined,
      webhookSecret: process.env.FEISHU_WEBHOOK_SECRET || undefined,
    },
    alerts: {
      hitlEscalateMs: Number(process.env.HITL_ESCALATE_MS ?? 30 * 60 * 1000),
    },
  };
}
