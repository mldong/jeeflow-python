"""共享 JDBC 仓储核心——SQL 逻辑与数据库无关。

设计（多数据库维护策略）：
- 本文件是**唯一维护点**：15 个仓储方法的 SQL 逻辑、行映射、ID 生成
- SQL 占位符统一使用 `?`，由各数据库适配器（`SqlAdapter`）转换为自家风格
  （MySQL `%s` / PostgreSQL `$n` / 原生 `?`）
- 事务（spec §7.4）：`with_tx` 用 `contextvars.ContextVar` 绑定当前协程上下文的事务连接

新增数据库 = 写一个适配器（约 80 行）：实现 `SqlAdapter` + 连接包装（execute/fetchone/
fetchall/begin/commit/rollback）。参考 `mysql.py` / `postgres.py`。
"""
from __future__ import annotations

import contextlib
import contextvars
import json
import re
import time
from datetime import datetime
from typing import Any, Optional, Protocol, Sequence

from ..spi import QueryCondition

from ..model import (ProcessDefine, ProcessInstance, ProcessTask, TaskState, InstanceState, CcInstanceRow, DefineRow, InstanceRow, TaskRow, ProcessDesign, ProcessDesignHis, ProcessSurrogate, InstanceStatsRow, TaskStatsRow)
from ..spi import (IDGenerator, ProcessRepository, ProcessExtRepository,
                   normalize_actors, normalize_cc_actors, require_present_id)

# 当前协程上下文绑定的事务连接
_tx_conn_var: contextvars.ContextVar = contextvars.ContextVar("jeeflow_tx_conn", default=None)


class TsIDGenerator(IDGenerator):
    """默认 ID 生成器：时间戳毫秒 + 同毫秒递增序号（对齐 Java nextId 默认实现）"""

    def __init__(self):
        self._last = 0
        self._seq = 0

    def next_id(self) -> int:
        now = int(time.time() * 1000)
        if now == self._last:
            self._seq += 1
        else:
            self._last = now
            self._seq = 0
        return now * 1000 + self._seq


class SqlConnection(Protocol):
    """适配器返回的连接包装——最小接口"""

    async def execute(self, sql: str, args: Sequence[Any]) -> None: ...
    async def fetchone(self, sql: str, args: Sequence[Any]) -> Optional[tuple]: ...
    async def fetchall(self, sql: str, args: Sequence[Any]) -> list[tuple]: ...
    async def begin(self) -> None: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...


class SqlAdapter(Protocol):
    """数据库适配器——连接生命周期 + 占位符风格"""

    placeholder: str  # "%s" / "$n" / "?"
    async def acquire(self) -> SqlConnection: ...
    async def release(self, conn: SqlConnection) -> None: ...


def convert_placeholder(sql: str, style: str) -> str:
    """把核心 SQL 的统一 `?` 占位符转换为适配器风格"""
    if style == "%s":
        return sql.replace("?", "%s")
    if style == "$n":
        counter = [0]

        def repl(_m):
            counter[0] += 1
            return f"${counter[0]}"

        return re.sub(r"\?", repl, sql)
    return sql  # "?" 原生（如 Go database/sql 语义）


def repeat_ph(n: int) -> str:
    """生成 n 个 `?` 占位符（用于 IN 列表）"""
    return ",".join(["?"] * n)


# ═══ 列白名单（issues/05-5，与 mldong-boot2 别名一致） ═══

_TASK_WHITELIST = {
    "t.id", "t.task_name", "t.display_name", "t.task_type", "t.perform_type", "t.task_state",
    "t.operator", "t.form_key", "t.create_time", "t.finish_time", "t.expire_time",
    "t.process_instance_id", "t.task_parent_id", "t.variable",
    "pi.id", "pi.business_no", "pi.operator", "pi.create_time", "pi.state",
    "pd.name", "pd.display_name", "pd.type",
    "pta.actor_id", "pta.process_task_id",
}

_INSTANCE_WHITELIST = {
    "t.id", "t.parent_id", "t.process_define_id", "t.state", "t.business_no",
    "t.operator", "t.create_time", "t.expire_time", "t.variable",
    "pd.name", "pd.display_name", "pd.type", "pd.version",
}

_CC_WHITELIST = {
    "t.id", "t.process_define_id", "t.state", "t.business_no", "t.operator",
    "t.create_time", "t.variable",
    "pd.name", "pd.display_name", "pd.type", "pd.version",
    "cc.actor_id", "cc.state",
}

# 归属谓词列（issues/129）：这几列定义"这条记录属于谁"，空值绝不能等于"不过滤"。
# 与门面 _operator_arg 的归属落点（page_instances/done→t.operator、todo→pta.actor_id、
# cc→cc.actor_id）以及任务分页 join 出来的实例发起人列 pi.operator 一一对齐。
_OWNERSHIP_COLUMNS = {"t.operator", "pi.operator", "pta.actor_id", "cc.actor_id"}


def _effective_ownership(val) -> bool:
    """归属条件的**有效值**判据（issues/141 G1）：值非 None、`str` 型 strip 后非空、集合非空。

    与 `jeeflow/memory.py::_effective_ownership` 同名同判据——两条尺子必须逐字一致，
    判据分叉＝"同一栈 SQL 仓与内存仓两个答案"（issues/117 场景 27 立过法）。
    与 `JdbcRepository._build_where` 的 `OWNERSHIP_COLUMNS` 空值档（issues/129）同一口径，
    本函数多收的是"条件整条没给"与"空集合"两档。
    """
    if val is None:
        return False
    if isinstance(val, str) and not val.strip():
        return False
    if isinstance(val, (list, tuple, set, dict)) and len(val) == 0:
        return False
    return True


def _has_cc_ownership(actor_id, conditions) -> bool:
    """抄送分页有没有**有效**归属条件（issues/141 G1 · spec 06-facade.md §2.5）。

    本栈的归属落点有两形（Java 只有 conditions 一形）：`page_cc_instances` 的专用入参 `actor_id`，
    以及 `conditions` 里 `column == "cc.actor_id"` 的那条。任一形给了有效值即"条件齐了"；
    两形都没给 ⇒ 契约答案是**空页**，严禁退化成"这条条件不加"而放出全部实例。
    与 `memory.py::_has_cc_ownership` 同名同判据（内存仓一路的判据表见 tests/spec_test.py
    的 Test 141 G1，两仓各钉一遍）。
    """
    if _effective_ownership(actor_id):
        return True
    for cond in conditions or []:
        if getattr(cond, "column", None) == "cc.actor_id" and _effective_ownership(cond.value):
            return True
    return False


_DEFINE_WHITELIST = {
    "t.id", "t.name", "t.display_name", "t.type", "t.state", "t.version",
    "t.create_time", "t.update_time",
}


class JdbcRepository(ProcessRepository):
    """ProcessRepository 的通用 JDBC 实现——注入 SqlAdapter 对接任意数据库。"""

    def _build_where(self, conditions: list, whitelist: set) -> tuple[str, tuple]:
        """m_ 条件 WHERE 构建（issues/05-5，白名单 + 参数化，对齐 Java buildWhere）"""
        sql = ""
        args = []
        for c in conditions or []:
            if c.column not in whitelist:
                continue  # 不在白名单，丢弃
            val = c.value
            # issues/129 案 A 第二层：归属谓词列拿到空值 ⇒ 空页，而不是"这条条件不加"。
            # 只收归属列——下面那句"空值当作没填"是 m_LIKE_* 等**可选过滤**的通用放行，
            # 照字面改成"空值即空页"会把可选过滤一起改坏（同 java JdbcProcessRepository.buildWhere）。
            blank_val = val is None or (isinstance(val, str) and not val.strip())
            if blank_val and c.operator.upper() == "EQ" and c.column in _OWNERSHIP_COLUMNS:
                sql += " AND 1=0"
                continue
            if val is None or val == "":
                continue
            op = c.operator.upper()
            if op == "EQ":
                sql += f" AND {c.column} = ?"; args.append(val)
            elif op == "NE":
                sql += f" AND {c.column} <> ?"; args.append(val)
            elif op == "LIKE":
                sql += f" AND {c.column} LIKE ?"; args.append(f"%{val}%")
            elif op == "LLIKE":
                sql += f" AND {c.column} LIKE ?"; args.append(f"%{val}")
            elif op == "RLIKE":
                sql += f" AND {c.column} LIKE ?"; args.append(f"{val}%")
            elif op == "GT":
                sql += f" AND {c.column} > ?"; args.append(val)
            elif op == "GE":
                sql += f" AND {c.column} >= ?"; args.append(val)
            elif op == "LT":
                sql += f" AND {c.column} < ?"; args.append(val)
            elif op == "LE":
                sql += f" AND {c.column} <= ?"; args.append(val)
            elif op in ("IN", "NIN"):
                if isinstance(val, (list, tuple)) and len(val) > 0:
                    marks = repeat_ph(len(val))
                    sql += f" AND {c.column} {'IN' if op == 'IN' else 'NOT IN'} ({marks})"
                    args.extend(val)
        return sql, tuple(args)

    def __init__(self, adapter: SqlAdapter, id_gen: Optional[IDGenerator] = None):
        self._adapter = adapter
        self._id_gen = id_gen or TsIDGenerator()

    def _sql(self, sql: str) -> str:
        return convert_placeholder(sql, self._adapter.placeholder)

    # ── 事务（spec §7.4：contextvars 绑定连接）────────────────────────────

    async def with_tx(self, fn):
        """开启事务，回调内仓储调用走同一连接；异常回滚，成功提交。"""
        conn = await self._adapter.acquire()
        try:
            await conn.begin()
            token = _tx_conn_var.set(conn)
            try:
                result = await fn()
            except BaseException:
                await conn.rollback()
                raise
            else:
                await conn.commit()
                return result
            finally:
                _tx_conn_var.reset(token)
        finally:
            await self._adapter.release(conn)

    @contextlib.asynccontextmanager
    async def _conn(self):
        """返回当前连接：有事务绑定用事务连接，否则从适配器获取。"""
        conn = _tx_conn_var.get()
        if conn is not None:
            yield conn
        else:
            raw = await self._adapter.acquire()
            try:
                yield raw
            finally:
                await self._adapter.release(raw)

    # ── ProcessDefine ──────────────────────────────────────────────────────

    async def find_define_by_name(self, name: str) -> Optional[ProcessDefine]:
        """按流程编码查最新一条定义（id 倒序取首条，deploy 版本管理用）"""
        async with self._conn() as conn:
            row = await conn.fetchone(self._sql(
                "SELECT id, name, display_name, type, state, content, version,"
                " create_time, create_user, update_time, update_user"
                " FROM wf_process_define WHERE name = ? ORDER BY version DESC LIMIT 1"), (name,))
        if not row:
            return None
        return ProcessDefine(
            id=row[0], name=row[1], displayName=row[2], type=row[3], state=row[4],
            content=row[5].decode() if isinstance(row[5], (bytes, bytearray)) else (row[5] or ""),
            version=row[6], createTime=row[7], createUser=_user_str(row[8]), updateTime=row[9], updateUser=_user_str(row[10]),
        )

    async def find_define_by_id(self, id: int) -> Optional[ProcessDefine]:
        async with self._conn() as conn:
            row = await conn.fetchone(self._sql(
                "SELECT id, name, display_name, type, state, content, version,"
                " create_time, create_user, update_time, update_user"
                " FROM wf_process_define WHERE id = ?"), (id,))
        if not row:
            return None
        return ProcessDefine(
            id=row[0], name=row[1], displayName=row[2], type=row[3], state=row[4],
            content=row[5].decode() if isinstance(row[5], (bytes, bytearray)) else (row[5] or ""),
            version=row[6], createTime=row[7], createUser=_user_str(row[8]), updateTime=row[9], updateUser=_user_str(row[10]),
        )

    # 定义写操作（v1.0.1，集成反馈①）。SQL 与 jeeflow-java JdbcProcessRepository 对齐；
    # State/Version 零值按 Java null 语义默认 1。

    async def save_define(self, define: ProcessDefine) -> None:
        if not define.id:
            define.id = self._id_gen.next_id()
        now = datetime.now()
        if not define.createTime:
            define.createTime = now
        if not define.updateTime:
            define.updateTime = now
        if not define.createUser:
            define.createUser = define.updateUser
        async with self._conn() as conn:
            await conn.execute(self._sql(
                "INSERT INTO wf_process_define (id, name, display_name, type, state, content,"
                " version, create_time, create_user, update_time, update_user)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)"),
                (define.id, define.name, define.displayName, define.type,
                 int(define.state or 1), define.content, int(define.version or 1),
                 define.createTime, define.createUser, define.updateTime, define.updateUser))

    async def update_define(self, define: ProcessDefine) -> None:
        async with self._conn() as conn:
            await conn.execute(self._sql(
                "UPDATE wf_process_define SET name=?, display_name=?, type=?, state=?,"
                " content=?, version=?, update_time=?, update_user=? WHERE id=?"),
                (define.name, define.displayName, define.type, int(define.state or 1),
                 define.content, int(define.version or 1), datetime.now(), define.updateUser,
                 define.id))

    async def update_define_state(self, define_id: int, state: int) -> None:
        async with self._conn() as conn:
            await conn.execute(self._sql(
                "UPDATE wf_process_define SET state=?, update_time=? WHERE id=?"),
                (int(state), datetime.now(), define_id))

    async def remove_define(self, define_id: int) -> None:
        async with self._conn() as conn:
            await conn.execute(self._sql("DELETE FROM wf_process_define WHERE id=?"),
                               (define_id,))

    # ── ProcessInstance ────────────────────────────────────────────────────

    _INSTANCE_COLS = ("id, parent_id, process_define_id, state, parent_node_name,"
                      " business_no, operator, expire_time, variable,"
                      " create_time, create_user, update_time, update_user")

    async def find_instance_by_id(self, id: int) -> Optional[ProcessInstance]:
        async with self._conn() as conn:
            row = await conn.fetchone(
                self._sql(f"SELECT {self._INSTANCE_COLS} FROM wf_process_instance WHERE id = ?"), (id,))
        if not row:
            return None
        inst = ProcessInstance(
            id=row[0], parentId=row[1], defineId=row[2], state=InstanceState(row[3]),
            parentNodeName=row[4], businessNo=row[5], operator=row[6], expireTime=row[7],
            createTime=row[9], createUser=_user_str(row[10]), updateTime=row[11], updateUser=_user_str(row[12]),
        )
        if row[8]:
            inst.variables = json.loads(row[8])
        # issues/110：聚合水合——二次查 wf_process_task 装任务副本（含 actorIds），
        # 对齐 Java findTasksByInstanceId / PHP PdoProcessRepository / C# issues/89；
        # 否则门面 detail 的 tasks/activeTaskList 恒空。
        # _find_tasks_by_state 自管连接（事务上下文内经 _tx_conn_var 复用事务连接），
        # 排序沿用本仓 find_history_tasks 的 id ASC 约定
        inst.tasks = await self._find_tasks_by_state(id, None, None)
        return inst

    async def save_instance(self, inst: ProcessInstance) -> None:
        async with self._conn() as conn:
            await conn.execute(self._sql(
                "INSERT INTO wf_process_instance (id, parent_id, process_define_id, state,"
                " parent_node_name, business_no, operator, expire_time, variable,"
                " create_time, create_user, update_time, update_user) VALUES"
                " (?,?,?,?,?,?,?,?,?,?,?,?,?)"),
                (inst.id, inst.parentId, inst.defineId, int(inst.state), inst.parentNodeName,
                 inst.businessNo, inst.operator, inst.expireTime,
                 json.dumps(inst.variables, ensure_ascii=False),
                 inst.createTime, inst.createUser, inst.updateTime, inst.updateUser))

    async def update_instance(self, inst: ProcessInstance) -> None:
        async with self._conn() as conn:
            await conn.execute(self._sql(
                "UPDATE wf_process_instance SET state=?, parent_node_name=?, business_no=?,"
                " operator=?, expire_time=?, variable=?, update_time=?, update_user=?"
                " WHERE id=?"),
                (int(inst.state), inst.parentNodeName, inst.businessNo, inst.operator,
                 inst.expireTime, json.dumps(inst.variables, ensure_ascii=False),
                 inst.updateTime, inst.updateUser, inst.id))
            # v1.0.1：级联持久化聚合根内任务状态变更（同连接，spec §7.4）
            for task in inst.tasks:
                if task.id:
                    await self._update_task_with_conn(conn, task)

    # ── ProcessTask ────────────────────────────────────────────────────────

    _TASK_COLS = ("id, process_instance_id, task_name, display_name, task_type, perform_type,"
                  " task_state, operator, finish_time, expire_time, form_key, task_parent_id,"
                  " variable, create_time, create_user, update_time, update_user")

    async def find_task_by_id(self, task_id: int) -> Optional[ProcessTask]:
        async with self._conn() as conn:
            row = await conn.fetchone(
                self._sql(f"SELECT {self._TASK_COLS} FROM wf_process_task WHERE id = ?"), (task_id,))
            actors: list[str] = []
            if row:
                rows = await conn.fetchall(self._sql(
                    "SELECT actor_id FROM wf_process_task_actor WHERE process_task_id = ? ORDER BY id ASC"),
                    (task_id,))
                actors = [r[0] for r in rows]
        if not row:
            return None
        return self._task_from_row(row, actors)

    async def save_task(self, task: ProcessTask) -> None:
        async with self._conn() as conn:
            await conn.execute(self._sql(
                "INSERT INTO wf_process_task (id, process_instance_id, task_name, display_name,"
                " task_type, perform_type, task_state, operator, finish_time, expire_time, form_key,"
                " task_parent_id, variable, create_time, create_user, update_time, update_user)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"),
                (task.id, task.processInstanceId, task.taskName, task.displayName, task.taskType,
                 task.performType, int(task.taskState), task.actorId, task.finishTime, task.expireTime,
                 task.formKey, task.parentTaskId, json.dumps(task.variables, ensure_ascii=False),
                 task.createTime, task.createUser, task.updateTime, task.updateUser))
            await self._replace_task_actors(conn, task.id, task.actorIds)

    async def update_task(self, task: ProcessTask) -> None:
        async with self._conn() as conn:
            await self._update_task_with_conn(conn, task)

    async def _update_task_with_conn(self, conn: SqlConnection, task: ProcessTask) -> None:
        """用指定连接更新任务（实例级联时与实例更新同连接）"""
        await conn.execute(self._sql(
            "UPDATE wf_process_task SET task_state=?, operator=?, finish_time=?,"
            " expire_time=?, variable=?, update_time=?, update_user=? WHERE id=?"),
            (int(task.taskState), task.actorId, task.finishTime, task.expireTime,
             json.dumps(task.variables, ensure_ascii=False), task.updateTime, task.updateUser,
             task.id))

    async def _find_tasks_by_state(self, instance_id: int, state: Optional[TaskState],
                                   task_names: Optional[list[str]]) -> list[ProcessTask]:
        sql = f"SELECT {self._TASK_COLS} FROM wf_process_task WHERE process_instance_id = ?"
        args: list[Any] = [instance_id]
        if state is not None:
            sql += " AND task_state = ?"
            args.append(int(state))
        if task_names:
            sql += f" AND task_name IN ({repeat_ph(len(task_names))})"
            args.extend(task_names)
        sql += " ORDER BY id ASC"
        async with self._conn() as conn:
            rows = await conn.fetchall(self._sql(sql), args)
            actors_map: dict[int, list[str]] = {}
            if rows:
                ids = [r[0] for r in rows]
                arows = await conn.fetchall(self._sql(
                    f"SELECT process_task_id, actor_id FROM wf_process_task_actor"
                    f" WHERE process_task_id IN ({repeat_ph(len(ids))}) ORDER BY id ASC"), ids)
                for r in arows:
                    actors_map.setdefault(r[0], []).append(r[1])
        return [self._task_from_row(row, actors_map.get(row[0], [])) for row in rows]

    async def find_doing_tasks(self, instance_id: int, task_names: Optional[list[str]] = None) -> list[ProcessTask]:
        return await self._find_tasks_by_state(instance_id, TaskState.DOING, task_names)

    async def find_done_tasks(self, instance_id: int, task_names: Optional[list[str]] = None) -> list[ProcessTask]:
        return await self._find_tasks_by_state(instance_id, TaskState.DONE, task_names)

    async def find_history_tasks(self, instance_id: int) -> list[ProcessTask]:
        return await self._find_tasks_by_state(instance_id, None, None)

    def _task_from_row(self, row, actors: list[str]) -> ProcessTask:
        task = ProcessTask(
            id=row[0], processInstanceId=row[1], taskName=row[2], displayName=row[3],
            taskType=row[4], performType=row[5], taskState=TaskState(row[6]), actorId=row[7],
            finishTime=row[8], expireTime=row[9], formKey=row[10], parentTaskId=row[11],
            createTime=row[13], createUser=_user_str(row[14]), updateTime=row[15], updateUser=_user_str(row[16]),
        )
        task.actorIds = actors
        if row[12]:
            task.variables = json.loads(row[12])
        return task

    # ── TaskActor ──────────────────────────────────────────────────────────

    async def _replace_task_actors(self, conn, task_id: int, actors: list[str]) -> None:
        await conn.execute(self._sql(
            "DELETE FROM wf_process_task_actor WHERE process_task_id = ?"), (task_id,))
        await self._insert_task_actors(conn, task_id, actors)

    async def _insert_task_actors(self, conn, task_id: int, actors: list[str]) -> None:
        import datetime
        now = datetime.datetime.now()
        for a in actors:
            await conn.execute(self._sql(
                "INSERT INTO wf_process_task_actor (id, process_task_id, actor_id, create_time, create_user)"
                " VALUES (?,?,?,?,?)"), (self._id_gen.next_id(), task_id, a, now, "jeeflow"))

    async def find_task_actors(self, task_id: int) -> list[str]:
        async with self._conn() as conn:
            rows = await conn.fetchall(self._sql(
                "SELECT actor_id FROM wf_process_task_actor WHERE process_task_id = ? ORDER BY id ASC"),
                (task_id,))
            return [r[0] for r in rows]

    async def add_task_actor(self, task_id: int, actors: list[str]) -> None:
        """SQL 仓写侧兜底（issues/142 B 批 · spec 06 §2.11 要求①「两层都挡」）：
        判据与内存仓 ``MemoryRepository.add_task_actor`` **逐字同一条**（两仓分叉＝issues/117 场景 27），
        且复用 §2.10 落地的同一枚单点 ``spi.normalize_actors``，不另抄一份。

        旧形状只判重不判空、不 trim ⇒ ``""``/``"  "``/``None`` 全放行（普查实读：
        "仓储写侧普遍判重不判空"）；绕过门面直连仓储的调用方照样能把空归属值灌进
        ``wf_process_task_actor.actor_id``——那正是 issues/129 那族"空 operator 读全库"的进水口。
        """
        # 主键另判一档：task_id 缺失/空串/0 响亮报错，不得拿 ''/0 当 id 落库（§2.11 末段）
        task_id = require_present_id(task_id)
        # 归属值：逐元素 trim、空串/纯空白/None 丢弃、同次调用折叠；"0" 是正常 id 不得丢（要求④）
        actors = normalize_actors(actors)
        if not actors:
            return
        async with self._conn() as conn:
            # 追加语义（对齐 boot2/boot3，issues/03）：查已有参与者，去重后仅插入新增，不清空原参与者
            rows = await conn.fetchall(self._sql(
                "SELECT actor_id FROM wf_process_task_actor WHERE process_task_id = ? ORDER BY id ASC"),
                (task_id,))
            existing = {r[0] for r in rows}
            # 比较取 trim 后的值（要求②）：入参已归一，" 123 " 与 "123" 判成同一个人，
            # 不会因前后空格把上一轮 G2 那把写侧判重打穿成同一人两行
            to_add = [a for a in actors if a not in existing]
            if to_add:
                await self._insert_task_actors(conn, task_id, to_add)

    async def remove_task_actor(self, task_id: int, actors: list[str]) -> None:
        if not actors:
            return
        async with self._conn() as conn:
            await conn.execute(self._sql(
                f"DELETE FROM wf_process_task_actor WHERE process_task_id = ? AND actor_id IN ({repeat_ph(len(actors))})"),
                [task_id, *actors])

    # ── CcInstance（抄送）──────────────────────────────────────────────────

    async def create_cc_instance(self, instance_id: int, creator: str, *actor_ids: str) -> None:
        import datetime
        now = datetime.datetime.now()
        # issues/141 G2 写侧判重＝幂等空操作（spec 06 §4）：同一 (实例, 被抄送人) 已有 cc 行时
        # 直接跳过——不新增行、不重置未读（state 保持原值）、不更新原行时间（不碰 UPDATE，
        # create_time/update_time 逐字不变）。判重放在写侧而不是查询侧：查询保持现状不引入
        # DISTINCT（owner 2026-09-29 拍），历史重复行也不清理。
        # 与 MemoryRepository.create_cc_instance 同一条判据——两仓判据分叉＝issues/117 场景 27。
        #
        # issues/141 G10「空不创建行」（spec 06 §2.10）：入参先过 normalize_cc_actors——空串/
        # 纯空白/None 一律丢弃，**落库值取 trim 后的串**。绕过引擎漏斗（``handle_cc_actors``）
        # 直连仓储的调用方也建不出 ``actor_id=''`` 的行——空归属值正是 issues/129 那族
        # "空 operator 读全库"的病根；不 trim 则 " 123 " 与 "123" 判成两个人，把上面那句判重打穿。
        existing = await self.find_cc_actor_ids(instance_id)
        async with self._conn() as conn:
            for actor_id in normalize_cc_actors(actor_ids):
                if actor_id in existing:
                    continue
                await conn.execute(self._sql(
                    "INSERT INTO wf_process_cc_instance (id, process_instance_id, actor_id, state,"
                    " create_time, create_user, update_time, update_user)"
                    " VALUES (?,?,?,0,?,?,?,?)"),
                    (self._id_gen.next_id(), instance_id, actor_id, now, creator, now, creator))
                # 同一次调用内的重复也算"已存在"，只落一行
                existing.append(actor_id)

    async def find_cc_actor_ids(self, instance_id: int) -> list[str]:
        """issues/141 G2：某实例已有的 cc 行 actor id（写侧判重的读侧，覆写 SPI default）。
        返回**可修改**的 list——`create_cc_instance` 要在插入过程中往里追加。"""
        async with self._conn() as conn:
            rows = await conn.fetchall(self._sql(
                "SELECT actor_id FROM wf_process_cc_instance WHERE process_instance_id = ?"),
                (instance_id,))
        return [r[0] for r in rows]

    async def update_cc_status(self, instance_id: int, actor_id: str) -> None:
        """抄送置已读（issues/142 B 批 · spec 06 §2.11 写点表第 4 行「入参归一后再比」）。

        ``actor_id`` 先过 ``normalize_actors`` 单点再绑进 WHERE：① 不 trim 则 ``" lisi "`` 判成
        另一个人，已读永远打不上；② **空/纯空白/None ⇒ 直接 no-op**——这句 UPDATE 的
        ``actor_id = ?`` 一旦绑上空串，会把 ``state=1`` 批量打到历史 ``actor_id=''`` 的脏行上
        （issues/129 那族"空归属值读全库"的写侧对偶）。判空只用 ``== ""``，不用 ``if not x``
        （``"0"`` 是正常 id，必须照样能置已读）。与内存仓同判据。"""
        import datetime
        normalized = normalize_actors([actor_id])
        if not normalized:
            return
        async with self._conn() as conn:
            await conn.execute(self._sql(
                "UPDATE wf_process_cc_instance SET state=1, update_time=?"
                " WHERE process_instance_id=? AND actor_id=?"),
                (datetime.datetime.now(), instance_id, normalized[0]))

    async def page_cc_instances(self, page_num: int = 1, page_size: int = 10,
                                actor_id: Optional[str] = None,
                                conditions: Optional[list[QueryCondition]] = None) -> tuple[list[CcInstanceRow], int]:
        """我的抄送分页（v1.3.0）：cc 表 join 实例 + 定义，按抄送人过滤（对齐 Java pageCcInstances）。

        **归属条件必填**（issues/141 G1 · spec 06 §2.5）：`cc.actor_id` 没有有效条件 ⇒ **空页**。
        本仓的旧形状是那句恒绑的 `WHERE cc.actor_id = ?`：不给入参时绑 `None` ⇒ `= NULL` 恒不命中，
        "缺条件"档碰巧是空页，但**只给条件形**（`actor_id=None` + `cc.actor_id EQ 'user1'`）也被这句
        折成 0 行，而内存仓同一档返 1 行——两仓两个答案，正是 issues/117 场景 27 立过法的那一类
        （spec 06 §2.5 点名的反面教材是 php PDO 仓"LEFT JOIN 不带条件放全部实例"，本栈两仓各错一头）。
        改成显式短路后不再依赖 NULL 巧合；空值档另有 `_build_where` 的 `_OWNERSHIP_COLUMNS`
        （issues/129）兜底，本格补的是"条件整条没给"与"空集合"两档。
        """
        conds = conditions or []
        if not _has_cc_ownership(actor_id, conds):
            return [], 0
        cond_sql, cond_args = self._build_where(conds, _CC_WHITELIST)
        # 归属谓词的两形（本栈专用入参 + m_ 条件；Java 只有条件一形）：入参给了有效值时照旧
        # `cc.actor_id = ?`；只有条件给了归属值时**不能再补这句**——`actor_id=None` 会拼成
        # `cc.actor_id = NULL` 恒不命中，把合法的条件查询折成空页，与内存仓再次分叉。
        if _effective_ownership(actor_id):
            predicate, bind = " WHERE cc.actor_id = ?", (actor_id,)
        else:
            predicate, bind = " WHERE 1=1", ()
        where = (" FROM wf_process_instance t"
                 " LEFT JOIN wf_process_define pd ON t.process_define_id = pd.id"
                 " LEFT JOIN wf_process_cc_instance cc ON t.id = cc.process_instance_id"
                 + predicate + cond_sql)
        bind = (*bind, *cond_args)
        cols = ("t.id, t.parent_id, t.process_define_id, t.state, t.parent_node_name, t.business_no,"
                " t.operator, t.expire_time, t.variable, t.create_time, t.create_user,"
                " t.update_time, t.update_user, pd.name, pd.display_name, pd.version")
        async with self._conn() as conn:
            row = await conn.fetchone(self._sql("SELECT COUNT(*)" + where), bind)
            total = int(row[0]) if row else 0
            rows = await conn.fetchall(self._sql(
                f"SELECT {cols}{where} ORDER BY t.id ASC LIMIT ? OFFSET ?"),
                (*bind, page_size, (page_num - 1) * page_size))
        return [self._map_cc_row(r) for r in rows], total

    def _map_cc_row(self, r: Sequence[Any]) -> CcInstanceRow:
        import json
        variables = json.loads(r[8]) if r[8] else {}
        return CcInstanceRow(
            id=r[0], parentId=r[1], defineId=r[2], state=InstanceState(r[3]),
            parentNodeName=r[4], businessNo=r[5], operator=r[6], expireTime=r[7],
            variables=variables, createTime=r[9], createUser=_user_str(r[10]),
            updateTime=r[11], updateUser=_user_str(r[12]),
            defineName=r[13], defineDisplayName=r[14], defineVersion=r[15] or 0)

    # ── 核心表分页（v1.5.0，对齐 Java pageDefines/pageInstances/pageTodoTasks/pageDoneTasks）──

    async def page_defines(self, page_num: int = 1, page_size: int = 10,
                           conditions: Optional[list[QueryCondition]] = None) -> tuple[list[DefineRow], int]:
        cond_sql, cond_args = self._build_where(conditions or [], _DEFINE_WHITELIST)
        where = " FROM wf_process_define t WHERE 1=1" + cond_sql
        async with self._conn() as conn:
            row = await conn.fetchone(self._sql("SELECT COUNT(*)" + where), cond_args)
            total = int(row[0]) if row else 0
            rows = await conn.fetchall(self._sql(
                "SELECT id, name, display_name, type, state, version, create_time, create_user,"
                " update_time, update_user" + where + " ORDER BY t.id DESC LIMIT ? OFFSET ?"),
                (*cond_args, page_size, (page_num - 1) * page_size))
        return [DefineRow(id=r[0], name=r[1], displayName=r[2], type=r[3], state=r[4],
                          version=r[5], createTime=r[6], createUser=_user_str(r[7]),
                          updateTime=r[8], updateUser=_user_str(r[9])) for r in rows], total

    async def page_instances(self, page_num: int = 1, page_size: int = 10,
                             operator: Optional[str] = None,
                             conditions: Optional[list[QueryCondition]] = None) -> tuple[list[InstanceRow], int]:
        cond_sql, cond_args = self._build_where(conditions or [], _INSTANCE_WHITELIST)
        where = (" FROM wf_process_instance t"
                 " LEFT JOIN wf_process_define pd ON t.process_define_id = pd.id"
                 " WHERE t.operator = ?" + cond_sql)
        cols = ("t.id, t.parent_id, t.process_define_id, t.state, t.parent_node_name, t.business_no,"
                " t.operator, t.expire_time, t.variable, t.create_time, t.create_user,"
                " t.update_time, t.update_user, pd.name, pd.display_name, pd.version")
        async with self._conn() as conn:
            row = await conn.fetchone(self._sql("SELECT COUNT(*)" + where), (operator, *cond_args))
            total = int(row[0]) if row else 0
            rows = await conn.fetchall(self._sql(
                f"SELECT {cols}{where} ORDER BY t.id DESC LIMIT ? OFFSET ?"),
                (operator, *cond_args, page_size, (page_num - 1) * page_size))
        return [self._map_instance_row(r) for r in rows], total

    async def page_todo_tasks(self, page_num: int = 1, page_size: int = 10,
                              actor_id: Optional[str] = None,
                              conditions: Optional[list[QueryCondition]] = None) -> tuple[list[TaskRow], int]:
        return await self._page_tasks(page_num, page_size, False, actor_id, conditions)

    async def page_done_tasks(self, page_num: int = 1, page_size: int = 10,
                              operator: Optional[str] = None,
                              conditions: Optional[list[QueryCondition]] = None) -> tuple[list[TaskRow], int]:
        return await self._page_tasks(page_num, page_size, True, operator, conditions)

    async def _page_tasks(self, page_num: int, page_size: int, done: bool,
                          filter_val: str,
                          conditions: Optional[list[QueryCondition]] = None) -> tuple[list[TaskRow], int]:
        cond_sql, cond_args = self._build_where(conditions or [], _TASK_WHITELIST)
        where = (" FROM wf_process_task t"
                 " LEFT JOIN wf_process_instance pi ON t.process_instance_id = pi.id"
                 " LEFT JOIN wf_process_define pd ON pi.process_define_id = pd.id"
                 " LEFT JOIN wf_process_task_actor pta ON t.id = pta.process_task_id")
        if done:
            where += " WHERE t.task_state <> 10 AND t.operator = ?" + cond_sql
        else:
            where += " WHERE t.task_state = 10 AND pta.actor_id = ?" + cond_sql
        cols = ("DISTINCT t.id, t.process_instance_id, t.task_name, t.display_name, t.task_type,"
                " t.perform_type, t.task_state, t.operator, t.finish_time, t.expire_time, t.form_key,"
                " t.task_parent_id, t.variable, t.create_time, t.create_user, t.update_time, t.update_user,"
                " pd.name, pd.display_name, pd.version, pi.variable, pi.create_time")
        async with self._conn() as conn:
            row = await conn.fetchone(self._sql("SELECT COUNT(DISTINCT t.id)" + where),
                                      (filter_val, *cond_args))
            total = int(row[0]) if row else 0
            rows = await conn.fetchall(self._sql(
                f"SELECT {cols}{where} ORDER BY t.id DESC LIMIT ? OFFSET ?"),
                (filter_val, *cond_args, page_size, (page_num - 1) * page_size))
        return [self._map_task_row(r) for r in rows], total

    # ── 统计查询（v1.8.25，issues/103） ──

    async def query_instances_for_stats(self, state_in: Optional[list[int]], order_by: str = "create_time",
                                        start: Optional[datetime] = None,
                                        end: Optional[datetime] = None) -> list[InstanceStatsRow]:
        # state_in 空 = 无 state 过滤（对齐内置线：仅 overview 六计数用 stateIn）
        sql = "SELECT process_define_id, state, operator, create_time FROM wf_process_instance WHERE 1=1"
        args: list = []
        if state_in:
            ph = repeat_ph(len(state_in))
            sql += f" AND state IN ({ph})"
            args.extend(state_in)
        if start:
            sql += " AND create_time >= ?"; args.append(start)
        if end:
            sql += " AND create_time < ?"; args.append(end)
        sql += f" ORDER BY {order_by}"
        async with self._conn() as conn:
            rows = await conn.fetchall(self._sql(sql), tuple(args))
        return [InstanceStatsRow(defineId=r[0], state=r[1], operator=str(r[2] or ""), createTime=r[3]) for r in rows]

    async def query_tasks_for_stats(self, task_state: Optional[int] = None,
                                    start: Optional[datetime] = None,
                                    end: Optional[datetime] = None) -> list[TaskStatsRow]:
        sql = "SELECT operator, display_name, perform_type, create_time, finish_time, expire_time FROM wf_process_task WHERE 1=1"
        args: list = []
        if task_state is not None:
            sql += " AND task_state = ?"; args.append(task_state)
        if start:
            sql += " AND finish_time >= ?"; args.append(start)
        if end:
            sql += " AND finish_time < ?"; args.append(end)
        async with self._conn() as conn:
            rows = await conn.fetchall(self._sql(sql), tuple(args))
        return [TaskStatsRow(operator=str(r[0] or ""), displayName=r[1] or "", performType=r[2] or 0,
                             createTime=r[3], finishTime=r[4], expireTime=r[5]) for r in rows]

    async def stats_pending_and_overdue_count(self) -> tuple[int, int]:
        now = datetime.now()
        async with self._conn() as conn:
            r1 = await conn.fetchone(
                self._sql("SELECT COUNT(*) FROM wf_process_task WHERE task_state = 10"), ())
            pending = int(r1[0]) if r1 else 0
            r2 = await conn.fetchone(
                self._sql("SELECT COUNT(*) FROM wf_process_task WHERE task_state = 10 AND expire_time IS NOT NULL AND expire_time < ?"),
                (now,))
            overdue = int(r2[0]) if r2 else 0
        return pending, overdue

    async def stats_completed_task_aggregate(self) -> tuple[int, int, int, int]:
        async with self._conn() as conn:
            r = await conn.fetchone(
                self._sql("SELECT COUNT(*), SUM(CASE WHEN perform_type = 1 THEN 1 ELSE 0 END), "
                          "SUM(CASE WHEN expire_time IS NOT NULL AND finish_time <= expire_time THEN 1 ELSE 0 END), "
                          "SUM(CASE WHEN expire_time IS NOT NULL THEN 1 ELSE 0 END) "
                          "FROM wf_process_task WHERE task_state = 20"), ())
        if not r:
            return 0, 0, 0, 0
        return int(r[0] or 0), int(r[1] or 0), int(r[2] or 0), int(r[3] or 0)

    async def stats_avg_completed_duration_seconds(self, start: Optional[datetime] = None,
                                                   end: Optional[datetime] = None) -> int:
        sql = ("SELECT AVG(ts.max_finish - i.create_time) FROM wf_process_instance i "
               "INNER JOIN (SELECT process_instance_id, MAX(finish_time) AS max_finish "
               "FROM wf_process_task WHERE task_state = 20 GROUP BY process_instance_id) ts "
               "ON i.id = ts.process_instance_id WHERE i.state = 20")
        args: list = []
        if start:
            sql += " AND i.create_time >= ?"; args.append(start)
        if end:
            sql += " AND i.create_time < ?"; args.append(end)
        async with self._conn() as conn:
            r = await conn.fetchone(self._sql(sql), tuple(args))
        if not r or r[0] is None:
            return 0
        return int(r[0])

    async def stats_define_group(self, start: Optional[datetime] = None,
                                 end: Optional[datetime] = None,
                                 limit: int = 10) -> list[dict]:
        # 对齐内置线 mapper：count 全实例（无 state 过滤）、inner join define、
        # avg 仅对 state=20 且有 finish 的实例聚合（MAX(task.finish_time) - create_time）
        sql = ("SELECT pd.name, pd.display_name, COUNT(*) AS cnt, "
               "ROUND(AVG(CASE WHEN i.state = 20 AND ts.max_finish IS NOT NULL "
               "THEN TIMESTAMPDIFF(SECOND, i.create_time, ts.max_finish) END)) AS avg_dur "
               "FROM wf_process_instance i "
               "JOIN wf_process_define pd ON i.process_define_id = pd.id "
               "LEFT JOIN (SELECT process_instance_id, MAX(finish_time) AS max_finish "
               "FROM wf_process_task GROUP BY process_instance_id) ts "
               "ON i.id = ts.process_instance_id WHERE 1=1")
        args: list = []
        if start:
            sql += " AND i.create_time >= ?"; args.append(start)
        if end:
            sql += " AND i.create_time < ?"; args.append(end)
        sql += " GROUP BY i.process_define_id, pd.name, pd.display_name ORDER BY cnt DESC LIMIT ?"
        args.append(limit)
        async with self._conn() as conn:
            rows = await conn.fetchall(self._sql(sql), tuple(args))
        return [{"key": r[0] or "", "label": r[1], "count": int(r[2]),
                 "avgDurationSeconds": int(r[3]) if r[3] is not None else None} for r in rows]

    async def stats_stuck_node_group(self, limit: int = 10) -> list[dict]:
        sql = ("SELECT display_name, COUNT(*) AS cnt FROM wf_process_task "
               "WHERE task_state = 10 GROUP BY display_name ORDER BY cnt DESC LIMIT ?")
        async with self._conn() as conn:
            rows = await conn.fetchall(self._sql(sql), (limit,))
        return [{"key": r[0] or "", "label": None, "count": int(r[1]),
                 "avgDurationSeconds": None} for r in rows]

    async def stats_stuck_approver_group(self, limit: int = 10) -> list[dict]:
        sql = ("SELECT ta.actor_id, COUNT(DISTINCT t.id) AS cnt FROM wf_process_task_actor ta "
               "INNER JOIN wf_process_task t ON ta.process_task_id = t.id "
               "WHERE t.task_state = 10 GROUP BY ta.actor_id ORDER BY cnt DESC LIMIT ?")
        async with self._conn() as conn:
            rows = await conn.fetchall(self._sql(sql), (limit,))
        return [{"key": str(r[0]), "label": None, "count": int(r[1]),
                 "avgDurationSeconds": None} for r in rows]

    async def stats_completed_instance_durations(self, start: Optional[datetime] = None,
                                                 end: Optional[datetime] = None) -> list[int]:
        sql = ("SELECT ts.max_finish - i.create_time FROM wf_process_instance i "
               "INNER JOIN (SELECT process_instance_id, MAX(finish_time) AS max_finish "
               "FROM wf_process_task WHERE task_state = 20 GROUP BY process_instance_id) ts "
               "ON i.id = ts.process_instance_id WHERE i.state = 20")
        args: list = []
        if start:
            sql += " AND i.create_time >= ?"; args.append(start)
        if end:
            sql += " AND i.create_time < ?"; args.append(end)
        async with self._conn() as conn:
            rows = await conn.fetchall(self._sql(sql), tuple(args))
        return [int(r[0]) for r in rows if r[0] is not None]

    def _map_instance_row(self, r: Sequence[Any]) -> InstanceRow:
        import json
        variables = json.loads(r[8]) if r[8] else {}
        return InstanceRow(
            id=r[0], parentId=r[1], defineId=r[2], state=InstanceState(r[3]),
            parentNodeName=r[4], businessNo=r[5], operator=r[6], expireTime=r[7],
            variables=variables, createTime=r[9], createUser=_user_str(r[10]),
            updateTime=r[11], updateUser=_user_str(r[12]),
            defineName=r[13], defineDisplayName=r[14], defineVersion=r[15] or 0)

    def _map_task_row(self, r: Sequence[Any]) -> TaskRow:
        import json
        variables = json.loads(r[12]) if r[12] else {}
        return TaskRow(
            id=r[0], processInstanceId=r[1], taskName=r[2], displayName=r[3],
            taskType=r[4], performType=r[5], taskState=TaskState(r[6]), operator=r[7],
            finishTime=r[8], expireTime=r[9], formKey=r[10], taskParentId=r[11],
            variables=variables, createTime=r[13], createUser=_user_str(r[14]),
            updateTime=r[15], updateUser=_user_str(r[16]),
            processDefineName=r[17], processDefineDisplayName=r[18],
            defineVersion=r[19] or 0, instanceVariable=r[20] or "", instanceCreateTime=r[21])


def _user_str(v):
    """审计用户列归一化（issue 38 E9 防御）：BIGINT 雪花 userId 列读回 int 时转 str，
    VARCHAR 列 str 直通，NULL 保持 None"""
    return None if v is None else str(v)
