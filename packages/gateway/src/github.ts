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

const NODE_PREFIX = 'node:';

export interface NodeDetection {
  current: string | null;
  index: number;
  isNew: boolean;
}

export function detectNode(issue: Issue, pipeline: string[]): NodeDetection {
  const nodeLabels = issue.labels
    .filter((l) => l.startsWith(NODE_PREFIX))
    .map((l) => l.slice(NODE_PREFIX.length));

  if (nodeLabels.includes('done')) {
    return { current: null, index: pipeline.length, isNew: false };
  }
  if (nodeLabels.length === 0) {
    return { current: pipeline[0] ?? null, index: 0, isNew: true };
  }
  const current = nodeLabels[0];
  const index = pipeline.indexOf(current);
  return { current: index === -1 ? null : current, index, isNew: false };
}

export function createGithubClient(config: GatewayConfig): GithubClient {
  return config.github.mode === 'real' ? realClient(config) : mockClient();
}

function realClient(config: GatewayConfig): GithubClient {
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
    let lastErr: unknown;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        const res = await fetch(base + p, {
          ...init,
          headers: { ...headers, ...((init?.headers as Record<string, string>) ?? {}) },
        });
        // GET 请求遇到 5xx/429 可安全重试；POST/PATCH/DELETE 已到达服务器，不自动重试
        if ((res.status >= 500 || res.status === 429) && method === 'GET' && attempt < MAX_ATTEMPTS) {
          await sleep(500 * 2 ** (attempt - 1));
          continue;
        }
        if (!res.ok) {
          throw new Error(`GitHub ${res.status} ${p}: ${await res.text()}`);
        }
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
