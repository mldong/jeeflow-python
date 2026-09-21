# SPI 实现指南

> 引擎核心零依赖：仓储、用户、JSON、表达式全部走 SPI（`jeeflow/spi.py`）。接入自己的业务时实现这些接口，用 `EngineImpl` 构造注入。

## ProcessRepository（必须）

仓储是唯一必须实现的 SPI，映射 [规范 01 · 数据模型](../../spec/01-data-model) 的 5 张表（`wf_process_define/instance/task/task_actor/cc_instance`）：

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
> （MySQL `%s` / PostgreSQL `$n`）。建表 SQL **各语言自带**（`tests/schema/schema-<db>.sql`，使用者单语言下载即用）。

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

## OrgUserProvider（可选，v1.6.0）

组织维度取人——内置组织 handler（部门领导/分管领导/角色）的数据源。
**业务方只实现数据接口，不写 handler**：

```python
from jeeflow.spi import OrgUserProvider

class MyOrgUserProvider(OrgUserProvider):
    async def find_dept_leaders(self, dept_id: str) -> list[str]:
        return await self.org_svc.leader_ids(dept_id)
    async def find_dept_main_leaders(self, dept_id: str) -> list[str]:
        return await self.org_svc.main_leader_ids(dept_id)
    async def find_by_role(self, role_code: str) -> list[str]:
        return await self.org_svc.user_ids_by_role(role_code)
```

注册内置 handler（注册名与 Java 类全限定名一致，流程 JSON 四语言通用）：

```python
from jeeflow import HandlerRegistry, EngineExtensions, register_builtin_assignments

registry = HandlerRegistry()
register_builtin_assignments(registry, user_prov, org_prov)   # 组织维度依赖注入
engine.set_extensions(EngineExtensions(registry=registry))
```

> 内置 handler 的**场景/配置/注意事项**见 [用户指南 07 · 参与者解析](../../guides/07-assignment-handlers.md)。

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

建表 SQL 自动从本仓 `tests/schema/` 执行（IF NOT EXISTS，幂等；维护者改 jeeflow-java 仓 resources 后跑 `jeeflow-hub/scripts/sync-schema.sh` 同步）。已实测：mysql 20/20、postgres 20/20 全过。

---

## 管理扩展与统一门面（v1.1.0）

设计稿 / 历史 / 委托由扩展仓储 SPI 提供读写（文档站 spec §10），统一门面
`flow(action, map)` 按 action 路由（spec §11.2），返回 `{code, msg, data}`，
deploy 自动版本管理，execute 按 submitType 全分发，操作人由 `args.operator` 显式传入。

扩展仓储实现（JDBC + 内存）与门面均在本仓库：
- 扩展仓储：`<repository>/jdbc/ext.*`（JDBC）、memory 内存实现
- 门面：`facade.*` / `jeeflow/facade.py` / `src/facade.ts`

三张扩展表（wf_process_design / design_his / surrogate）SQL 已随 schema 分发
（`schema-<db>.sql`，维护源 jeeflow-java resources）。

> 分页说明（v1.1.0）：核心表分页 SPI（pageDefines/pageTodoTasks 等）目前 Java 提供，
> 本语言对应分页 action 返回明确错误，计划 1.2.0 补齐；设计/委托分页全支持。

---

## 委托代理自动生效（引擎内置，默认开启）

`processSurrogate/*` 五个 action 只是台账 CRUD；真正的能力是**建任务那一刻自动应用生效中的委托**
（文档站 spec 06 §4.5「运行期语义」）。Python 引擎内置该行为：参与者解析完成后、落库前，
对每个参与者查一次 `ProcessExtRepository.get_surrogate`，命中则把代理人**并入该任务的参与者集合**
（随 `save_task` 一起写 `wf_process_task_actor`，授权人保留、任一可办）。实现见 `jeeflow/surrogate.py`。

零配置：把扩展仓储传给门面即生效（门面会自动 `engine.attach_ext_repository(ext_repo)`）。

```python
ext_repo = MemoryExtRepository()            # 或 JdbcProcessExtRepository(adapter)
engine = EngineImpl(repo, user_prov, idgen)
facade = JeeflowFacade(engine, repo, ext_repo)   # ← 委托自此自动生效
```

**显式关闭**（回到"仅台账"）两条路，任选其一：

```python
# ① 配置开关
engine.set_extensions(EngineExtensions(surrogate_enabled=False))
engine.ext.surrogate_enabled = False            # 运行期改也可以
# ② 注册空实现（也可换成自定义数据源的 SurrogateApplier）
from jeeflow.surrogate import NullSurrogateApplier
engine.set_extensions(EngineExtensions(ext_repository=ext_repo,
                                       surrogate_applier=NullSurrogateApplier()))
```

**未配置扩展仓储时静默跳过**：`ext_repository is None` 不查不抛，建单流程零影响；
查询本身报错也只记日志不外溢（委托是增强能力，不得打断建单）。

委托查询四判据（内存仓 `MemoryExtRepository` 与 SQL 仓 `JdbcProcessExtRepository` 同答案）：
空 `processName` 全流程兜底（先精确后兜底，多条命中取 id 最大者）、时间窗任一侧 NULL=不限、
`surrogate <> operator` 自委托过滤、`enabled` 只认整数 1（脏值按停用，写入侧见 `processSurrogate/save`）。
