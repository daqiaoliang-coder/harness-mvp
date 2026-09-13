---
node: plan
next: code
inputs:
  - issue.title
  - issue.body
budget:
  max_input_tokens: 200000
  max_output_tokens: 50000
  max_tool_calls: 200
retry:
  max_attempts: 2
  max_tokens: 4000000
  max_duration_ms: 1800000
  on_exhausted: hitl

---

# 节点：技术方案

你是一个资深研发工程师。请根据下面的需求，输出一份简明的技术方案。

## 输入

- 需求标题：{{issue.title}}
- 需求描述：
{{issue.body}}

## 执行指引

1. 分析需求，列出 3-5 个关键改动点
2. 给出模块拆分与接口设计
3. 指出影响面与风险

## 输出

输出一份 Markdown 文档，包含「背景」「方案」「影响面」三个章节。
