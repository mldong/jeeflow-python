"""JDBC（MySQL / aiomysql）仓储参考实现——对齐 spec §7.4 事务约定。

事务机制（spec §7.4）：Python 使用 ``contextvars.ContextVar`` 绑定当前协程上下文的事务连接。
``with_tx`` 开启事务并把连接写入 ContextVar，回调内所有仓储方法走同一连接；
无事务上下文时从连接池获取独立连接。
"""
from __future__ import annotations

import contextlib
import contextvars
import json
import time
from typing import Any, Optional

import aiomysql

from .model import ProcessDefine, ProcessInstance, ProcessTask, TaskState, InstanceState
from .spi import IDGenerator, ProcessRepository

# 当前协程上下文绑定的事务连接
_conn_var: contextvars.ContextVar = contextvars.ContextVar("jeeflow_tx_conn", default=None)


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


class JdbcRepository(ProcessRepository):
    """ProcessRepository 的 MySQL 实现（aiomysql 连接池）。"""

    def __init__(self, pool: aiomysql.Pool, id_gen: Optional[IDGenerator] = None):
        self._pool = pool
        self._id_gen = id_gen or TsIDGenerator()

    # ── 事务（spec §7.4：contextvars 绑定连接）────────────────────────────

    async def with_tx(self, fn):
        """开启事务，回调内仓储调用走同一连接；异常回滚，成功提交。"""
        conn = await self._pool.acquire()
        try:
            await conn.begin()
            token = _conn_var.set(conn)
            try:
                result = await fn()
            except BaseException:
                await conn.rollback()
                raise
            else:
                await conn.commit()
                return result
            finally:
                _conn_var.reset(token)
        finally:
            await self._pool.release(conn)

    @contextlib.asynccontextmanager
    async def _conn_cursor(self):
        """返回 (conn, cursor)：有事务绑定走事务连接，否则走池连接。"""
        conn = _conn_var.get()
        if conn is not None:
            async with conn.cursor() as cur:
                yield conn, cur
        else:
            async with self._pool.acquire() as conn:
                async with conn.cursor() as cur:
                    yield conn, cur

    # ── ProcessDefine ──────────────────────────────────────────────────────

    async def find_define_by_id(self, id: int) -> Optional[ProcessDefine]:
        async with self._conn_cursor() as (_, cur):
            await cur.execute(
                "SELECT id, name, display_name, type, state, content, version,"
                " create_time, create_user, update_time, update_user"
                " FROM wf_process_define WHERE id = %s", (id,))
            row = await cur.fetchone()
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
        async with self._conn_cursor() as (_, cur):
            await cur.execute(f"SELECT {self._INSTANCE_COLS} FROM wf_process_instance WHERE id = %s", (id,))
            row = await cur.fetchone()
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
        async with self._conn_cursor() as (conn, cur):
            await cur.execute(
                "INSERT INTO wf_process_instance (id, parent_id, process_define_id, state,"
                " parent_node_name, business_no, operator, expire_time, variable,"
                " create_time, create_user, update_time, update_user) VALUES"
                " (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (inst.id, inst.parentId, inst.defineId, int(inst.state), inst.parentNodeName,
                 inst.businessNo, inst.operator, inst.expireTime,
                 json.dumps(inst.variables, ensure_ascii=False),
                 inst.createTime, inst.createUser, inst.updateTime, inst.updateUser))

    async def update_instance(self, inst: ProcessInstance) -> None:
        async with self._conn_cursor() as (_, cur):
            await cur.execute(
                "UPDATE wf_process_instance SET state=%s, parent_node_name=%s, business_no=%s,"
                " operator=%s, expire_time=%s, variable=%s, update_time=%s, update_user=%s"
                " WHERE id=%s",
                (int(inst.state), inst.parentNodeName, inst.businessNo, inst.operator,
                 inst.expireTime, json.dumps(inst.variables, ensure_ascii=False),
                 inst.updateTime, inst.updateUser, inst.id))

    # ── ProcessTask ────────────────────────────────────────────────────────

    _TASK_COLS = ("id, process_instance_id, task_name, display_name, task_type, perform_type,"
                  " task_state, operator, finish_time, expire_time, form_key, task_parent_id,"
                  " variable, create_time, create_user, update_time, update_user")

    async def find_task_by_id(self, task_id: int) -> Optional[ProcessTask]:
        async with self._conn_cursor() as (_, cur):
            await cur.execute(f"SELECT {self._TASK_COLS} FROM wf_process_task WHERE id = %s", (task_id,))
            row = await cur.fetchone()
            actors = []
            if row:
                await cur.execute(
                    "SELECT actor_id FROM wf_process_task_actor WHERE process_task_id = %s ORDER BY id ASC",
                    (task_id,))
                actors = [r[0] for r in await cur.fetchall()]
        if not row:
            return None
        return self._task_from_row(row, actors)

    async def save_task(self, task: ProcessTask) -> None:
        async with self._conn_cursor() as (conn, cur):
            await cur.execute(
                "INSERT INTO wf_process_task (id, process_instance_id, task_name, display_name,"
                " task_type, perform_type, task_state, operator, finish_time, expire_time, form_key,"
                " task_parent_id, variable, create_time, create_user, update_time, update_user)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (task.id, task.processInstanceId, task.taskName, task.displayName, task.taskType,
                 task.performType, int(task.taskState), task.actorId, task.finishTime, task.expireTime,
                 task.formKey, task.parentTaskId, json.dumps(task.variables, ensure_ascii=False),
                 task.createTime, task.createUser, task.updateTime, task.updateUser))
            await self._replace_task_actors(cur, task.id, task.actorIds)

    async def update_task(self, task: ProcessTask) -> None:
        async with self._conn_cursor() as (_, cur):
            await cur.execute(
                "UPDATE wf_process_task SET task_state=%s, operator=%s, finish_time=%s,"
                " expire_time=%s, variable=%s, update_time=%s, update_user=%s WHERE id=%s",
                (int(task.taskState), task.actorId, task.finishTime, task.expireTime,
                 json.dumps(task.variables, ensure_ascii=False), task.updateTime, task.updateUser,
                 task.id))

    async def _find_tasks_by_state(self, instance_id: int, state: Optional[TaskState],
                                   task_names: Optional[list[str]]) -> list[ProcessTask]:
        sql = f"SELECT {self._TASK_COLS} FROM wf_process_task WHERE process_instance_id = %s"
        args: list[Any] = [instance_id]
        if state is not None:
            sql += " AND task_state = %s"
            args.append(int(state))
        if task_names:
            sql += " AND task_name IN (%s" + ",%s" * (len(task_names) - 1) + ")"
            args.extend(task_names)
        sql += " ORDER BY id ASC"
        async with self._conn_cursor() as (_, cur):
            await cur.execute(sql, args)
            rows = await cur.fetchall()
            actors_map: dict[int, list[str]] = {}
            if rows:
                ids = [r[0] for r in rows]
                ph = ",".join(["%s"] * len(ids))
                await cur.execute(
                    f"SELECT process_task_id, actor_id FROM wf_process_task_actor"
                    f" WHERE process_task_id IN ({ph}) ORDER BY id ASC", ids)
                for r in await cur.fetchall():
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

    async def _replace_task_actors(self, cur, task_id: int, actors: list[str]) -> None:
        await cur.execute("DELETE FROM wf_process_task_actor WHERE process_task_id = %s", (task_id,))
        await self._insert_task_actors(cur, task_id, actors)

    async def _insert_task_actors(self, cur, task_id: int, actors: list[str]) -> None:
        import datetime
        now = datetime.datetime.now()
        for a in actors:
            await cur.execute(
                "INSERT INTO wf_process_task_actor (id, process_task_id, actor_id, create_time, create_user)"
                " VALUES (%s,%s,%s,%s,%s)", (self._id_gen.next_id(), task_id, a, now, "jeeflow"))

    async def find_task_actors(self, task_id: int) -> list[str]:
        async with self._conn_cursor() as (_, cur):
            await cur.execute(
                "SELECT actor_id FROM wf_process_task_actor WHERE process_task_id = %s ORDER BY id ASC",
                (task_id,))
            return [r[0] for r in await cur.fetchall()]

    async def add_task_actor(self, task_id: int, actors: list[str]) -> None:
        async with self._conn_cursor() as (_, cur):
            await self._insert_task_actors(cur, task_id, actors)

    async def remove_task_actor(self, task_id: int, actors: list[str]) -> None:
        if not actors:
            return
        ph = ",".join(["%s"] * len(actors))
        async with self._conn_cursor() as (_, cur):
            await cur.execute(
                f"DELETE FROM wf_process_task_actor WHERE process_task_id = %s AND actor_id IN ({ph})",
                [task_id, *actors])

    # ── CcInstance（抄送）──────────────────────────────────────────────────

    async def create_cc_instance(self, instance_id: int, creator: str, *actor_ids: str) -> None:
        import datetime
        now = datetime.datetime.now()
        async with self._conn_cursor() as (_, cur):
            for actor_id in actor_ids:
                await cur.execute(
                    "INSERT INTO wf_process_cc_instance (id, process_instance_id, actor_id, state,"
                    " create_time, create_user, update_time, update_user)"
                    " VALUES (%s,%s,%s,0,%s,%s,%s,%s)",
                    (self._id_gen.next_id(), instance_id, actor_id, now, creator, now, creator))

    async def update_cc_status(self, instance_id: int, actor_id: str) -> None:
        import datetime
        async with self._conn_cursor() as (_, cur):
            await cur.execute(
                "UPDATE wf_process_cc_instance SET state=1, update_time=%s"
                " WHERE process_instance_id=%s AND actor_id=%s",
                (datetime.datetime.now(), instance_id, actor_id))
