# SPI 实现指南

> 引擎核心零依赖：仓储、用户、JSON、表达式全部走 SPI（`jeeflow/spi.py`）。接入自己的业务时实现这些接口，用 `EngineImpl` 构造注入。

## ProcessRepository（必须）

仓储是唯一必须实现的 SPI，映射 [SPEC §2](../../spec/) 的 5 张表（`wf_process_define/instance/task/task_actor/cc_instance`）：

```python
class MyRepository(ProcessRepository):
    async def find_define_by_id(self, id: int) -> Optional[ProcessDefine]: ...
    async def find_instance_by_id(self, id: int) -> Optional[ProcessInstance]: ...
    async def save_instance(self, inst: ProcessInstance) -> None: ...
    async def update_instance(self, inst: ProcessInstance) -> None: ...
    async def find_task_by_id(self, task_id: int) -> Optional[ProcessTask]: ...
    async def save_task(self, task: ProcessTask) -> None: ...
    async def update_task(self, task: ProcessTask) -> None: ...
    async def find_doing_tasks(self, instance_id: int, task_names=None) -> list[ProcessTask]: ...
    async def find_done_tasks(self, instance_id: int, task_names=None) -> list[ProcessTask]: ...
    async def find_history_tasks(self, instance_id: int) -> list[ProcessTask]: ...
    async def find_task_actors(self, task_id: int) -> list[str]: ...
    async def add_task_actor(self, task_id: int, actors: list[str]) -> None: ...
    async def remove_task_actor(self, task_id: int, actors: list[str]) -> None: ...
    async def create_cc_instance(self, instance_id: int, creator: str, *actor_ids: str) -> None: ...
    async def update_cc_status(self, instance_id: int, actor_id: str) -> None: ...
```

> 开箱即用：`MemoryRepository`（`jeeflow/memory.py`）供演示/测试；生产按上表映射到自己的数据库（可参考 demo 或 JDBC 版语义）。

## UserProvider（可选）

一次返回用户全部信息，引擎注入 `u_*` 变量：

```python
class MyUserProvider(UserProvider):
    async def get_user(self, user_id: str) -> Optional[UserInfo]:
        return UserInfo(userId=user_id, realName="张三",
                        deptId="D01", deptName="研发部",
                        postId="P01", postName="工程师")
```

## IDGenerator（可选）

```python
class MyIdGen(IDGenerator):
    def next_id(self) -> int:
        return int(time.time() * 1000)  # 简易雪花即可
```

## ExpressionEvaluator（可选）

决策/会签表达式求值（不实现则表达式分支不生效）：

```python
class MyExpr(ExpressionEvaluator):
    async def eval(self, expr: str, vars: dict) -> Any:
        return eval_expr(expr, vars)  # 简易比较器即可
```

## 示例：最小接入

```python
from jeeflow import EngineImpl

engine = EngineImpl(MyRepository(), MyUserProvider(), MyIdGen(), MyExpr())
inst = await engine.start_process_instance_by_id(define_id, operator, args)
```
