/**
 * GitHub 事实源边界：Scheduler 只依赖 GithubClient 接口，real / mock 两个实现按配置切换，
 * 换成 Meego/Jira 等只需新增一份实现。detectNode 负责从 issue 的 node:/hitl: 标签推导
 * 流水线状态机；对 GitHub 的写操作（评论、标签）即流程状态的持久化，是调度回写的终点。
 */
import type { GatewayConfig } from './config.js';

export interface Issue {
  number: number;
  title: string;
  body: string;
  labels: string[];
  state: 'open' | 'closed';
}

export interface GithubClient {
  listIssues(): Promise<Issue[]>;
  addComment(issueNumber: number, body: string): Promise<void>;
  setLabels(issueNumber: number, harnessLabels: string[]): Promise<void>;
  closeIssue(issueNumber: number): Promise<void>;
}

/** 一次 GitHub API 逻辑调用（含内部重试）的观测记录，对应 Meego 的 raw API 证据。 */
export interface ApiCallRecord {
  method: string;
  /** 不含 host，形如 /issues?state=open 或 /issues/3/comments */
  path: string;
  /** 实际发出的 HTTP 请求次数（连接失败/5xx 重试会 >1） */
  attempts: number;
  status?: number;
  ok: boolean;
  /** 整次逻辑调用耗时（含重试等待），ms */
  latencyMs: number;
  /** 失败时的错误摘要（含状态码与响应体片段），成功时省略 */
  error?: string;
}

export type ApiCallSink = (rec: ApiCallRecord) => void;

const NODE_PREFIX = 'node:';
export { NODE_PREFIX };
/** 人工卡点标签前缀。带此前缀的 issue 表示流程已挂起，调度必须跳过。 */
const HITL_PREFIX = 'hitl:';
/** 熔断或需要人工判断时打上的挂起标签。 */
export const HITL_WAITING = `${HITL_PREFIX}waiting`;

/** Harness 自己管理的标签前缀；人工标签不在此列，setLabels 时必须原样保留。 */
export const MANAGED_PREFIXES = [NODE_PREFIX, HITL_PREFIX];

export interface NodeDetection {
  current: string | null;
  index: number;
  isNew: boolean;
  /**
   * 流程已挂起（带 hitl:waiting 标签），等待人工放行。
   *
   * 为什么必须单列这个状态：早期实现里 detectNode 只认 node: 前缀标签，
   * 熔断打上 hitl:waiting 后调度看不到任何「已挂起」信号，
   * 下一轮 tick 就把该 issue 当成待派发重新跑一遍——失败、再熔断、再重跑，
   * 形成无限循环持续烧 token，恰好抵消了熔断本身的价值。
   */
  suspended: boolean;
}

export function detectNode(issue: Issue, pipeline: string[]): NodeDetection {
  // 挂起判定优先于节点推导：人工未放行前，无论当前节点是什么都不派发。
  if (issue.labels.some((l) => l.startsWith(HITL_PREFIX))) {
    return { current: null, index: -1, isNew: false, suspended: true };
  }

  const nodeLabels = issue.labels
    .filter((l) => l.startsWith(NODE_PREFIX))
    .map((l) => l.slice(NODE_PREFIX.length));

  if (nodeLabels.includes('done')) {
    return { current: null, index: pipeline.length, isNew: false, suspended: false };
  }
  if (nodeLabels.length === 0) {
    return { current: pipeline[0] ?? null, index: 0, isNew: true, suspended: false };
  }
  const current = nodeLabels[0];
  const index = pipeline.indexOf(current);
  return {
    current: index === -1 ? null : current,
    index,
    isNew: false,
    suspended: false,
  };
}

export function createGithubClient(config: GatewayConfig, sink?: ApiCallSink): GithubClient {
  return config.github.mode === 'real' ? realClient(config, sink) : mockClient();
}

function realClient(config: GatewayConfig, sink?: ApiCallSink): GithubClient {
  const { token, owner, repo } = config.github;
  if (!token || !owner || !repo) {
    throw new Error('GITHUB_TOKEN / GITHUB_OWNER / GITHUB_REPO 必须同时提供');
  }
  const base = `https://api.github.com/repos/${owner}/${repo}`;
  const headers: Record<string, string> = {
    Authorization: `Bearer ${token}`,
    Accept: 'application/vnd.github+json',
    'User-Agent': 'harness-mvp',
    'X-GitHub-Api-Version': '2022-11-28',
  };

  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
  const MAX_ATTEMPTS = 4;

  async function call(p: string, init?: RequestInit) {
    const method = (init?.method ?? 'GET').toUpperCase();
    // openapi 证据：一次逻辑调用（含重试等待）落一条记录，在 finally 统一发出，
    // 成功/重试耗尽/连接失败三条出口都不会漏；鉴权头不记录，path 只含 issue 编号无密钥
    const startedAt = Date.now();
    let tries = 0;
    let finalStatus: number | undefined;
    let settled = false;
    let lastErr: unknown;
    const emit = () =>
      sink?.({
        method,
        path: p,
        attempts: tries,
        status: finalStatus,
        ok: settled,
        latencyMs: Date.now() - startedAt,
        error: settled
          ? undefined
          : ((lastErr as Error)?.message ?? 'unknown error').slice(0, 500),
      });
    try {
      for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
        try {
          tries += 1;
          const res = await fetch(base + p, {
            ...init,
            headers: { ...headers, ...((init?.headers as Record<string, string>) ?? {}) },
          });
          finalStatus = res.status;
          // GET 请求遇到 5xx/429 可安全重试；POST/PATCH/DELETE 已到达服务器，不自动重试
          if ((res.status >= 500 || res.status === 429) && method === 'GET' && attempt < MAX_ATTEMPTS) {
            await sleep(500 * 2 ** (attempt - 1));
            continue;
          }
          if (!res.ok) {
            throw new Error(`GitHub ${res.status} ${p}: ${await res.text()}`);
          }
          settled = true;
          return res;
        } catch (e) {
          lastErr = e;
          // 连接级失败（DNS/TLS 握手中断 ECONNRESET、网络瞬断）：请求未确认到达服务器，任何方法都可安全重试
          const isConnectionLevel =
            e instanceof TypeError || (e as { code?: string })?.code === 'ECONNRESET';
          if (attempt < MAX_ATTEMPTS && isConnectionLevel) {
            console.warn(
              `[github] 请求失败(${attempt}/${MAX_ATTEMPTS})，${500 * 2 ** (attempt - 1)}ms 后重试: ${(e as Error).message}`,
            );
            await sleep(500 * 2 ** (attempt - 1));
            continue;
          }
          throw e;
        }
      }
      throw lastErr;
    } finally {
      emit();
    }
  }

  return {
    async listIssues() {
      const res = await call('/issues?state=open&per_page=50');
      const json = (await res.json()) as any[];
      return json
        .filter((i) => !i.pull_request)
        .map<Issue>((i) => ({
          number: i.number,
          title: i.title,
          body: i.body ?? '',
          labels: i.labels.map((l: any) => (typeof l === 'string' ? l : l.name)),
          state: i.state,
        }));
    },

    async addComment(n, body) {
      await call(`/issues/${n}/comments`, {
        method: 'POST',
        body: JSON.stringify({ body }),
      });
    },

    async setLabels(n, harnessLabels) {
      // 不做全量 PUT：先读出当前标签，只删 node: 前缀的旧标签——
      // hitl: 挂起标签与人工贴的标签必须原样保留，所以不能直接覆盖整个标签集
      const all = (await (await call('/issues?state=all&per_page=50')).json()) as any[];
      const issue = all.find((i) => i.number === n);
      const existing: string[] = issue
        ? issue.labels.map((l: any) => (typeof l === 'string' ? l : l.name))
        : [];
      const stale = existing.filter((l) => l.startsWith(NODE_PREFIX));
      for (const l of stale) {
        await call(`/issues/${n}/labels/${encodeURIComponent(l)}`, { method: 'DELETE' }).catch(
          () => {},
        );
      }
      if (harnessLabels.length) {
        await call(`/issues/${n}/labels`, {
          method: 'POST',
          body: JSON.stringify({ labels: harnessLabels }),
        });
      }
    },

    async closeIssue(n: number) {
      await call(`/issues/${n}`, {
        method: 'PATCH',
        body: JSON.stringify({ state: 'closed' }),
      });
    },
  };
}

function mockClient(): GithubClient {
  const issues = new Map<number, Issue>();

  issues.set(1, {
    number: 1,
    title: '实现用户登录接口',
    body:
      '需要实现一个基于 JWT 的用户登录接口，包含单元测试。\n\n' +
      '验收标准：\n' +
      '- POST /login 返回 access token\n' +
      '- 覆盖成功 / 密码错误 / token 过期场景',
    labels: [],
    state: 'open',
  });

  issues.set(2, {
    number: 2,
    title: '为订单服务添加限流',
    body: '订单服务在高并发下会被打挂，需要加一层令牌桶限流，阈值 100 QPS。',
    labels: [],
    state: 'open',
  });

  return {
    async listIssues() {
      return [...issues.values()].filter((i) => i.state === 'open');
    },

    async addComment(n, body) {
      const preview = body.length > 160 ? body.slice(0, 160) + '…' : body;
      console.log(`\n\x1b[35m[mock-github]\x1b[0m 💬 issue #${n} 评论:\n${preview}\n`);
    },

    async setLabels(n, harnessLabels) {
      const issue = issues.get(n);
      if (!issue) return;
      const keep = issue.labels.filter((l) => !l.startsWith(NODE_PREFIX));
      issue.labels = [...keep, ...harnessLabels];
      console.log(
        `\x1b[35m[mock-github]\x1b[0m 🏷  issue #${n} labels = [${issue.labels.join(', ')}]`,
      );
    },

    async closeIssue(n) {
      const issue = issues.get(n);
      if (!issue) return;
      issue.state = 'closed';
      console.log(`\x1b[35m[mock-github]\x1b[0m 🔒 issue #${n} 已关闭`);
    },
  };
}
