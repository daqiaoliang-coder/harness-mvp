# harness-mvp 文档↔代码锚点速查

维护 README 时按此表核对事实源，避免凭记忆引用过时实现。

## README 章节落点

```
架构（mermaid + 三个分离）
├── Gateway：核心设计（6 小节）
│   1. Issues 轮询状态机        ← scheduler.ts / github.ts(detectNode) / config.ts
│   2. 事件溯源 SQLite          ← db.ts / bus.ts / http.ts(SSE)
│   3. 派发锁 at-least-once     ← scheduler.ts(runsByIssue/runsByRunId/onRunResult)
│   4. WS/SSE 分用              ← ws.ts / http.ts
│   5. GithubClient + 重试      ← github.ts(call/setLabels)
│   6. 并发控制                 ← scheduler.ts(ticking/dispatch/healthCheck)
├── loop-node（5 小节）
│   1. 外连/退避重连/孤儿化      ← index.ts(connect/orphanAllRuns/livenessTimer)
│   2. PTY 兜底                 ← pty.ts
│   3. Agent 契约与配置         ← config.ts / templates.ts / scripts/dev-real.sh
│   4. 模板机制                 ← templates.ts / templates/*.md
│   5. 上报/缓冲/超时/取消       ← index.ts(handleLaunch/finish/onTimeout)
└── 架构取舍一览（表格，每次设计变更联动检查）
```

## 关键数值（写文档前必须回源码复核，禁止照抄本表）

| 文档中的事实 | 源码位置 |
| --- | --- |
| 轮询间隔 / worker ping / stale 阈值 | packages/gateway/src/config.ts |
| run 总时长 / 空闲超时默认值 | packages/loop-node/src/config.ts |
| progress chunk 截断、输出缓冲 256K→64K | ws.ts、loop-node/index.ts |
| issue 评论尾部 3000 / 错误 2000 字符 | scheduler.ts(onRunResult) |
| GitHub 重试 4 次、指数退避 | github.ts(call) |
| SSE keepalive 15s、Last-Event-ID=seq | http.ts、db.ts(listAfter) |
| 重连退避 1s→30s、半开 45s/15s 巡检 | loop-node/index.ts 顶部常量 |

## 协议事实源

- 消息类型、字段名只认 packages/shared/src/protocol.ts
- 配置环境变量名只认两个 config.ts；.env.example 仅为样例，可能漂移
  （历史上出现过样例写 AGENT_ARGS、代码读 AGENTS 的不一致）

## 口径回收检查清单

改完任一章节后全局搜索，确认没有残留旧口径：

- [ ] 「架构取舍一览」表格是否需要新增/修改行
- [ ] .env.example 中的变量名/默认值是否与 config.ts 一致（不一致只报告，不擅改）
- [ ] README 前文「三个分离」「架构图」是否与新内容冲突
- [ ] 对话回复中引用代码用 file:/// 绝对链接 + #L 行号；README 内部用相对路径
