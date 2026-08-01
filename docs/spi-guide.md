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

> 开箱即用：
> - `MemoryRepository`（`jeeflow/memory.py`）供演示/测试；
> - **`JdbcRepository`（`jeeflow/repository/`）— 多数据库 JDBC 实现**：共享核心 `base.py`（SQL 逻辑唯一维护点）+ 每库一个薄适配器。按库安装依赖（核心零依赖）：

```python
# MySQL（pip install jeeflow[mysql]）
import aiomysql
from jeeflow import JdbcRepository, MySqlAdapter

pool = await aiomysql.create_pool(
    host="127.0.0.1", user="root", password="pwd", db="jeeflow",
    autocommit=True,  # 适配器要求：无事务时每条语句立即提交
)
repo = JdbcRepository(MySqlAdapter(pool))  # 关系表主键用内置时间戳 ID 生成器

# PostgreSQL（pip install jeeflow[postgres]）
# import asyncpg
# from jeeflow import JdbcRepository, PostgresAdapter
# pool = await asyncpg.create_pool("postgresql://root:pwd@127.0.0.1/jeeflow")
# repo = JdbcRepository(PostgresAdapter(pool))
```

> **新增数据库** = 写一个适配器（约 80 行，参考 `repository/mysql.py`）：实现
> `SqlAdapter`（占位符风格 + acquire/release）+ 连接包装（execute/fetchone/fetchall/
> begin/commit/rollback）。SQL 核心统一用 `?` 占位符，由适配器转换
> （MySQL `%s` / PostgreSQL `$n`）。建表 SQL 见 `tests/schema/<db>.sql`。

仓储方法自动映射 `wf_*` 5 张表（spec §2）。`content` 为流程定义 JSON，`variable` 为变量 JSON。

**事务（spec §7.4）**：`with_tx` 用 `contextvars.ContextVar` 把事务连接绑定到当前协程上下文，回调内所有仓储调用走同一连接；异常自动回滚：

```python
async def do_biz():
    await repo.save_instance(inst)
    await repo.create_cc_instance(inst.id, "zhangsan", "lisi", "wangwu")

await repo.with_tx(do_biz)
```

> 约定：**业务层是事务 owner**——先 `with_tx` 再调引擎方法，引擎核心不感知事务。

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

## 集成测试

`tests/jdbc_test.py` **双库可跑**（同一套断言，与数据库无关）：

```bash
python tests/jdbc_test.py mysql     # 开发服务器 MySQL(3306)
python tests/jdbc_test.py postgres  # 开发服务器 PostgreSQL(5432，Docker mldong-pg)
```

建表 SQL 自动从 `tests/schema/<db>.sql` 执行（IF NOT EXISTS，幂等）。已实测：mysql 20/20、postgres 20/20 全过。
