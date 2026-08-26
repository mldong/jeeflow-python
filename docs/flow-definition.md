# 流程定义 JSON 格式

流程定义使用 LogicFlow 兼容的 JSON 格式（语言无关，五语言引擎共享同一套定义文件）。

## 顶层结构

```json
{
  "name": "leave",
  "displayName": "请假审批",
  "type": "approval",
  "instanceUrl": "/form/leave",
  "nodes": [ ... ],
  "edges": [ ... ]
}
```

| 字段 | 必填 | 说明 |
|------|------|------|
| `name` | ✅ | 唯一编码 |
| `displayName` | ✅ | 显示名称 |
| `type` | ❌ | 流程分类 |
| `instanceUrl` | ❌ | 发起表单地址 |
| `nodes` | ✅ | 节点列表 |
| `edges` | ✅ | 边列表 |

## 节点类型

| 类型 | 说明 |
|------|------|
| `snaker:start` | 开始节点（每个流程有且只有一个） |
| `snaker:end` | 结束节点（单一结束） |
| `snaker:task` | 审批节点（普通参与 / 会签） |
| `snaker:decision` | 决策节点（按边表达式路由） |
| `snaker:fork` / `snaker:join` | 并行分支 / 合并 |
| `snaker:custom` | 自定义节点 |

## 任务节点属性

| 属性 | 说明 |
|------|------|
| `assignee` | 参与者（逗号分隔）；特殊值 `"applicant"` = 流程发起人 |
| `performType` | `0` 普通参与 / `1` 会签 |
| `countersignType` | `PARALLEL` / `SEQUENTIAL` / `RATIO(0.5)` |
| `countersignCompletionCondition` | 会签放行表达式（如 `#nrOfCompletedInstances>=2`）；特殊值 `ONE_VOTE_VETO` = 开启一票否决（任一成员 submitType=20 即推进整单，未配置则软拒绝不阻断） |
| `form` | 表单标识 |
| `taskType` | `0` 主办 / `1` 协办 |

> **约定**：每个流程的第一个任务节点是"发起申请"节点（`assignee="applicant"`），这是 startAndExecute 契约和"退回发起人"闭环的基础。完整属性表见 [规范 02 · 流程定义格式](../../spec/02-flow-definition)。

## 边

```json
{
  "id": "e3",
  "sourceNodeId": "decision1",
  "targetNodeId": "task2",
  "properties": { "expr": "amount > 1000" },
  "text": { "value": "金额>1000" }
}
```

- 决策分支：表达式在 `properties.expr`，分支标签在 `text.value`
- 无 `expr` 的边为默认分支

## Python 加载

```python
import json
from jeeflow.model import parse_flow_model

flow = parse_flow_model(json.loads(content))   # 过滤未知字段，生成引擎模型
```

> 演示站从本仓 `flows/` 副本加载（15 个；唯一编辑源在 `jeeflow-java` 仓 `test/resources/flows/`，`flows_resolver` 在维护者机器上执行时精确镜像进本仓，单语言用户下载即用）。完整示例与字段说明见[通用指南 02 · 流程定义](../../guides/02-flow-definition)。
