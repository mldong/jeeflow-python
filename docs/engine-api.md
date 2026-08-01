# 引擎 API

> jeeflow-python 引擎对外方法（`jeeflow/engine.py` 的 `EngineImpl`，异步风格）。语义与 Java 参考实现一一对应，方法名 snake_case。

## 构造引擎

```python
from jeeflow import EngineImpl, MemoryRepository

repo = MemoryRepository()                    # 内存仓储（演示/测试用）
engine = EngineImpl(repo, user_prov, idgen, expr_eval)
#                    ├── UserProvider（可选，注入用户信息）
#                    ├── IDGenerator（可选，ID 生成）
#                    └── ExpressionEvaluator（可选，决策/会签表达式）
```

## 核心方法

| 方法 | 说明 |
|------|------|
| `start_process_instance_by_id(define_id, operator, args=None)` | 启动流程实例（args 为流程变量） |
| `execute_process_task(task_id, operator, args=None)` | 执行任务（同意/发起/会签拒绝等） |
| `execute_and_jump_to_end(task_id, operator, args=None)` | 拒绝（REJECT=2）→ 跳结束，实例→45 |
| `execute_and_jump_task(task_id, operator, args, target_task_name=None)` | 跳转（JUMP=4）/ 退回上一步（ROLLBACK=3） |
| `execute_and_jump_to_first_task_node(task_id, operator, args=None)` | 退回发起人（ROLLBACK_TO_OPERATOR=6）→ 第一个任务节点重执行，参与者=发起人 |

```python
# 启动并自动完成申请节点（startAndExecute 契约，调用方实现）
inst = await engine.start_process_instance_by_id(1, "user1", {"amount": 500})
for task in await repo.find_doing_tasks(inst.id):
    await repo.add_task_actor(task.id, ["user1"])
    await engine.execute_process_task(task.id, "user1", {"submitType": 0})  # APPLY
```

## 变量注入

引擎每次操作自动注入用户信息到流程变量：`u_userId` / `u_realName` / `u_deptId` / `u_deptName` / `u_postId` / `u_postName`（来自 `UserProvider`），key 与 boot2 一致。

## 状态码

- 实例：`10` 进行中 / `20` 已完成 / `45` 已拒绝
- 任务：`10` 待办 / `20` 已完成 / `99` 已废弃

> submitType 全枚举行为见[设计原理 06](../../concepts/06-contracts)。
