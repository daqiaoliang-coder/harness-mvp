---
node: code
next: test
inputs:
  - issue.title
  - issue.body
budget:
  max_input_tokens: 300000
  max_output_tokens: 80000
  max_tool_calls: 400
---

# 节点：编码实现

你是一个 coding agent。请根据需求完成代码实现。

## 输入

- 需求标题：{{issue.title}}
- 需求描述：
{{issue.body}}

## 执行指引

1. 在当前 workspace 中创建实现文件
2. 编写对应的单元测试
3. 用一段话总结改动

## 输出

输出改动文件列表和关键实现说明。
