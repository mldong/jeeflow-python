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
from typing import Any, Optional, Protocol, Sequence

from ..model import ProcessDefine, ProcessInstance, ProcessTask, TaskState, InstanceState
from ..spi import IDGenerator, ProcessRepository

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


class JdbcRepository(ProcessRepository):
    """ProcessRepository 的通用 JDBC 实现——注入 SqlAdapter 对接任意数据库。"""

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
            version=row[6], createTime=row[7], createUser=row[8], updateTime=row[9], updateUser=row[10],
        )

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
            createTime=row[9], createUser=row[10], updateTime=row[11], updateUser=row[12],
        )
        if row[8]:
            inst.variables = json.loads(row[8])
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
            createTime=row[13], createUser=row[14], updateTime=row[15], updateUser=row[16],
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
        async with self._conn() as conn:
            await self._insert_task_actors(conn, task_id, actors)

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
        async with self._conn() as conn:
            for actor_id in actor_ids:
                await conn.execute(self._sql(
                    "INSERT INTO wf_process_cc_instance (id, process_instance_id, actor_id, state,"
                    " create_time, create_user, update_time, update_user)"
                    " VALUES (?,?,?,0,?,?,?,?)"),
                    (self._id_gen.next_id(), instance_id, actor_id, now, creator, now, creator))

    async def update_cc_status(self, instance_id: int, actor_id: str) -> None:
        import datetime
        async with self._conn() as conn:
            await conn.execute(self._sql(
                "UPDATE wf_process_cc_instance SET state=1, update_time=?"
                " WHERE process_instance_id=? AND actor_id=?"),
                (datetime.datetime.now(), instance_id, actor_id))
