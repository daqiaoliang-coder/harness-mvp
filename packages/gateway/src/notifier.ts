import crypto from 'node:crypto';
import type { HarnessEvent } from '@harness/shared';
import type { GatewayConfig } from './config.js';
import type { EventBus } from './bus.js';

/**
 * 飞书通知（方案 A：群自定义机器人 webhook）。
 * 订阅 EventBus 上的关键状态事件，映射成 interactive 卡片 POST 给机器人。
 * webhook 消息不支持事后更新，所以每次状态变化发一张新卡片；
 * 未来迁移到方案 B（自建应用 OpenAPI）时可改成 patch 同一张卡片。
 */

type HeaderColor = 'blue' | 'green' | 'red' | 'orange' | 'grey';

interface CardSpec {
  color: HeaderColor;
  title: string;
  /** 卡片正文（lark_md） */
  lines: string[];
}

interface FeishuPayload {
  timestamp?: string;
  sign?: string;
  msg_type: 'interactive';
  card: {
    config: { wide_screen_mode: boolean };
    header: {
      template: HeaderColor;
      title: { tag: 'plain_text'; content: string };
    };
    elements: unknown[];
  };
}

function str(v: unknown): string | undefined {
  return typeof v === 'string' && v.length > 0 ? v : undefined;
}

function truncate(v: string, max: number): string {
  return v.length > max ? `${v.slice(0, max)}…` : v;
}

function formatTime(iso: string): string {
  const t = new Date(iso);
  return Number.isNaN(t.getTime())
    ? iso
    : t.toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false });
}

/** 把 HarnessEvent 映射成卡片规格；不在白名单内的事件返回 null */
export function buildCardSpec(ev: HarnessEvent, config: GatewayConfig): CardSpec | null {
  const d = (ev.details ?? {}) as Record<string, unknown>;
  const issue = ev.workItemId ? `#${ev.workItemId}` : undefined;
  const issueUrl =
    issue && config.github.owner && config.github.repo
      ? `https://github.com/${config.github.owner}/${config.github.repo}/issues/${ev.workItemId}`
      : undefined;
  const issueLine = issue
    ? `**Issue [${issue}](${issueUrl ?? '#'})**${issueUrl ? '' : '（mock 模式）'}`
    : undefined;

  switch (ev.event) {
    case 'run.dispatched': {
      const context = (d.context ?? {}) as Record<string, string>;
      const title = str(context['issue.title']);
      return {
        color: 'blue',
        title: `🚀 节点开始 · ${str(d.nodeKey) ?? 'unknown'}`,
        lines: [
          issueLine,
          title ? `**标题**：${truncate(title, 120)}` : undefined,
          `**节点**：\`${str(d.nodeKey) ?? '?'}\``,
          ev.nodeId ? `**Worker**：${ev.nodeId}` : undefined,
        ].filter(Boolean) as string[],
      };
    }

    case 'issue.node.advanced':
      return {
        color: 'green',
        title: `✅ 节点完成 · ${str(d.from) ?? '?'} → ${str(d.to) ?? '?'}`,
        lines: [
          issueLine,
          `**流转**：\`${str(d.from) ?? '?'}\` → \`${str(d.to) ?? '?'}\``,
        ].filter(Boolean) as string[],
      };

    case 'issue.completed':
      return {
        color: 'green',
        title: `🎉 流程完成 · Issue ${issue ?? ''} 已关闭`,
        lines: [issueLine, '所有流水线节点均已执行完毕，Issue 已自动关闭。'].filter(
          Boolean,
        ) as string[],
      };

    case 'issue.node.failed': {
      const error = str(d.error) ?? 'unknown error';
      return {
        color: 'red',
        title: `❌ 节点失败 · ${str(d.nodeKey) ?? 'unknown'}`,
        lines: [
          issueLine,
          `**节点**：\`${str(d.nodeKey) ?? '?'}\``,
          '**错误信息**：',
          '```',
          truncate(error, 500),
          '```',
        ].filter(Boolean) as string[],
      };
    }

    case 'run.transition_failed':
      return {
        color: 'orange',
        title: `⚠️ 状态回写受阻 · Issue ${issue ?? ev.runId ?? ''}`,
        lines: [
          issueLine,
          'GitHub 评论 / 打标失败，锁已释放，下轮轮询将按当前标签重试该节点。',
          '**错误信息**：',
          '```',
          truncate(str(d.error) ?? 'unknown error', 500),
          '```',
        ].filter(Boolean) as string[],
      };

    case 'run.worker_lost':
      return {
        color: 'orange',
        title: `⚠️ Worker 丢失 · 任务中断`,
        lines: [
          issueLine ? `${issueLine}（节点锁已释放，将重新派发）` : undefined,
          ev.nodeId ? `**Worker**：${ev.nodeId}` : undefined,
          ev.runId ? `**Run**：${ev.runId}` : undefined,
        ].filter(Boolean) as string[],
      };

    default:
      return null;
  }
}

/** 卡片规格 → 飞书自定义机器人 webhook 请求体（可选加签） */
export function buildPayload(spec: CardSpec, ev: HarnessEvent, secret?: string): FeishuPayload {
  const noteParts = [ev.source, ev.runId, formatTime(ev.timestamp)].filter(Boolean);
  const payload: FeishuPayload = {
    msg_type: 'interactive',
    card: {
      config: { wide_screen_mode: true },
      header: {
        template: spec.color,
        title: { tag: 'plain_text', content: truncate(spec.title, 120) },
      },
      elements: [
        {
          tag: 'div',
          text: { tag: 'lark_md', content: spec.lines.join('\n') },
        },
        { tag: 'hr' },
        {
          tag: 'note',
          elements: [{ tag: 'lark_md', content: noteParts.join(' · ') }],
        },
      ],
    },
  };

  if (secret) {
    const timestamp = Math.floor(Date.now() / 1000).toString();
    // 飞书加签规则：HMAC-SHA256，key = `${timestamp}\n${secret}`，消息体为空串
    const sign = crypto
      .createHmac('sha256', `${timestamp}\n${secret}`)
      .update('')
      .digest('base64');
    payload.timestamp = timestamp;
    payload.sign = sign;
  }

  return payload;
}

/** 自定义机器人加签（导出供测试） */
export function signTimestamp(secret: string, timestamp: string): string {
  return crypto.createHmac('sha256', `${timestamp}\n${secret}`).update('').digest('base64');
}

async function postCard(webhookUrl: string, payload: FeishuPayload): Promise<void> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 8000);
  try {
    const res = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    const text = await res.text();
    if (!res.ok) {
      console.error(`[notifier] 飞书 webhook HTTP ${res.status}: ${text.slice(0, 300)}`);
      return;
    }
    // 成功响应形如 {"StatusCode":0,"StatusMessage":"success","code":0,...}
    let body: { code?: number; StatusCode?: number; msg?: string; StatusMessage?: string };
    try {
      body = JSON.parse(text);
    } catch {
      return;
    }
    const failed =
      (typeof body.code === 'number' && body.code !== 0) ||
      (typeof body.StatusCode === 'number' && body.StatusCode !== 0);
    if (failed) {
      console.error(
        `[notifier] 飞书 webhook 返回错误: code=${body.code ?? body.StatusCode} ` +
          `${body.msg ?? body.StatusMessage ?? text.slice(0, 300)}`,
      );
    }
  } catch (e) {
    console.error('[notifier] 飞书卡片发送失败（不影响主流程）:', (e as Error).message);
  } finally {
    clearTimeout(timer);
  }
}

export interface NotifierDeps {
  config: GatewayConfig;
  bus: EventBus;
}

/** 订阅事件总线并推送卡片；返回取消订阅函数。未配置 webhook 时为空操作。 */
export function startFeishuNotifier(deps: NotifierDeps): () => void {
  const { webhookUrl, webhookSecret } = deps.config.feishu;
  if (!webhookUrl) {
    console.log('[notifier] 未配置 FEISHU_WEBHOOK_URL，飞书通知已关闭。');
    return () => {};
  }

  console.log('[notifier] 飞书通知已启用（群自定义机器人 webhook）。');
  return deps.bus.onEvent((ev) => {
    const spec = buildCardSpec(ev, deps.config);
    if (!spec) return;
    const payload = buildPayload(spec, ev, webhookSecret);
    // fire-and-forget：通知失败绝不能阻塞调度主链路
    void postCard(webhookUrl, payload);
  });
}
