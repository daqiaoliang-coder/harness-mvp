---
node: test
next: ~
inputs:
  - issue.title
  - issue.body
budget:
  max_input_tokens: 150000
  max_output_tokens: 40000
  max_tool_calls: 300
retry:
  max_attempts: 2
  max_tokens: 30000000
  max_duration_ms: 5400000
  on_exhausted: hitl

---

# 节点：测试验证

## 输入

- 需求标题：{{issue.title}}
- 需求描述：
{{issue.body}}

## 执行指引

1. 运行测试套件
2. 输出测试报告摘要
3. 如果失败，给出失败原因和建议

## 输出

输出测试结论（PASS / FAIL）和关键指标。
