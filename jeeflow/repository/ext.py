"""扩展仓储 JDBC 参考实现（v1.1.0）——流程设计 / 设计历史 / 委托代理

与 base.py 同一套 SqlAdapter / 占位符约定；分页为单表简单过滤（filters 字段名 EQ）。
"""
from __future__ import annotations

import contextlib
from datetime import datetime
from typing import Any, Optional, Sequence

from ..model import ProcessDesign, ProcessDesignHis, ProcessSurrogate
from ..spi import IDGenerator, ProcessExtRepository
from .base import SqlAdapter, TsIDGenerator, convert_placeholder, _tx_conn_var


class JdbcProcessExtRepository(ProcessExtRepository):
    """ProcessExtRepository 的通用 JDBC 实现——注入 SqlAdapter 对接任意数据库。"""

    def __init__(self, adapter: SqlAdapter, id_gen: Optional[IDGenerator] = None):
        self._adapter = adapter
        self._id_gen = id_gen or TsIDGenerator()

    def _sql(self, sql: str) -> str:
        return convert_placeholder(sql, self._adapter.placeholder)

    @contextlib.asynccontextmanager
    async def _conn(self):
        """返回当前连接：有事务绑定用事务连接，否则从适配器获取（与 base 同一约定）"""
        conn = _tx_conn_var.get()
        if conn is not None:
            yield conn
        else:
            raw = await self._adapter.acquire()
            try:
                yield raw
            finally:
                await self._adapter.release(raw)

    def _next_id(self) -> int:
        return self._id_gen.next_id()

    # ── 流程设计 ─────────────────────────────────────────────────────────────

    _DESIGN_COLS = ("id, name, display_name, type, icon, is_deployed, remark,"
                    " create_time, create_user, update_time, update_user")

    async def find_design_by_id(self, id: int) -> Optional[ProcessDesign]:
        async with self._conn() as conn:
            row = await conn.fetchone(
                self._sql(f"SELECT {self._DESIGN_COLS} FROM wf_process_design WHERE id = ?"), (id,))
        if not row:
            return None
        return self._map_design(row)

    async def save_design(self, d: ProcessDesign) -> None:
        if not d.id:
            d.id = self._next_id()
        now = datetime.now()
        if not d.createTime:
            d.createTime = now
        if not d.updateTime:
            d.updateTime = now
        async with self._conn() as conn:
            await conn.execute(self._sql(
                "INSERT INTO wf_process_design (id, name, display_name, type, icon, is_deployed, remark,"
                " create_time, create_user, update_time, update_user) VALUES (?,?,?,?,?,?,?,?,?,?,?)"),
                (d.id, d.name, d.displayName, d.type, d.icon, d.isDeployed, d.remark,
                 d.createTime, d.createUser, d.updateTime, d.updateUser))

    async def update_design(self, d: ProcessDesign) -> None:
        async with self._conn() as conn:
            await conn.execute(self._sql(
                "UPDATE wf_process_design SET name=?, display_name=?, type=?, icon=?, is_deployed=?,"
                " remark=?, update_time=?, update_user=? WHERE id=?"),
                (d.name, d.displayName, d.type, d.icon, d.isDeployed, d.remark,
                 datetime.now(), d.updateUser, d.id))

    async def remove_design(self, id: int) -> None:
        async with self._conn() as conn:
            await conn.execute(self._sql("DELETE FROM wf_process_design WHERE id=?"), (id,))
            await conn.execute(self._sql("DELETE FROM wf_process_design_his WHERE process_design_id=?"), (id,))

    async def page_designs(self, page_num: int = 1, page_size: int = 10,
                           filters: Optional[dict] = None) -> tuple[list[ProcessDesign], int]:
        sql = f"SELECT {self._DESIGN_COLS} FROM wf_process_design t WHERE 1=1"
        count_sql = "SELECT COUNT(*) FROM wf_process_design t WHERE 1=1"
        args: list[Any] = []
        args2: list[Any] = []
        for col, val in (filters or {}).items():
            if col in ("name", "display_name", "type"):
                sql += f" AND t.{col} = ?"
                count_sql += f" AND t.{col} = ?"
                args.append(val)
                args2.append(val)
        async with self._conn() as conn:
            row = await conn.fetchone(self._sql(count_sql), args2)
            total = int(row[0]) if row else 0
            sql += " ORDER BY t.id DESC LIMIT ? OFFSET ?"
            args.extend([page_size, (page_num - 1) * page_size])
            rows = await conn.fetchall(self._sql(sql), args)
        return [self._map_design(r) for r in rows], total

    # ── 设计历史 ─────────────────────────────────────────────────────────────

    async def save_design_his(self, his: ProcessDesignHis) -> None:
        if not his.id:
            his.id = self._next_id()
        if not his.createTime:
            his.createTime = datetime.now()
        async with self._conn() as conn:
            await conn.execute(self._sql(
                "INSERT INTO wf_process_design_his (id, process_design_id, content, create_time, create_user)"
                " VALUES (?,?,?,?,?)"),
                (his.id, his.processDesignId, his.content, his.createTime, his.createUser))

    async def list_design_his(self, design_id: int) -> list[ProcessDesignHis]:
        async with self._conn() as conn:
            rows = await conn.fetchall(self._sql(
                "SELECT id, process_design_id, content, create_time, create_user"
                " FROM wf_process_design_his WHERE process_design_id = ? ORDER BY id DESC"), (design_id,))
        result = []
        for r in rows:
            content = r[2].decode() if isinstance(r[2], (bytes, bytearray)) else (r[2] or "")
            result.append(ProcessDesignHis(id=r[0], processDesignId=r[1], content=content,
                                           createTime=r[3], createUser=r[4]))
        return result

    # ── 委托代理 ─────────────────────────────────────────────────────────────

    _SURROGATE_COLS = ("id, process_name, operator, surrogate, start_time, end_time, enabled,"
                       " create_time, create_user, update_time, update_user")

    async def find_surrogate_by_id(self, id: int) -> Optional[ProcessSurrogate]:
        async with self._conn() as conn:
            row = await conn.fetchone(
                self._sql(f"SELECT {self._SURROGATE_COLS} FROM wf_process_surrogate WHERE id = ?"), (id,))
        if not row:
            return None
        return self._map_surrogate(row)

    async def save_surrogate(self, s: ProcessSurrogate) -> None:
        if not s.id:
            s.id = self._next_id()
        now = datetime.now()
        if not s.createTime:
            s.createTime = now
        if not s.updateTime:
            s.updateTime = now
        if not s.enabled:
            s.enabled = 1
        async with self._conn() as conn:
            await conn.execute(self._sql(
                "INSERT INTO wf_process_surrogate (id, process_name, operator, surrogate, start_time,"
                " end_time, enabled, create_time, create_user, update_time, update_user)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)"),
                (s.id, s.processName, s.operator, s.surrogate, s.startTime, s.endTime, s.enabled,
                 s.createTime, s.createUser, s.updateTime, s.updateUser))

    async def update_surrogate(self, s: ProcessSurrogate) -> None:
        async with self._conn() as conn:
            await conn.execute(self._sql(
                "UPDATE wf_process_surrogate SET process_name=?, operator=?, surrogate=?, start_time=?,"
                " end_time=?, enabled=?, update_time=?, update_user=? WHERE id=?"),
                (s.processName, s.operator, s.surrogate, s.startTime, s.endTime, s.enabled,
                 datetime.now(), s.updateUser, s.id))

    async def remove_surrogate(self, id: int) -> None:
        async with self._conn() as conn:
            await conn.execute(self._sql("DELETE FROM wf_process_surrogate WHERE id=?"), (id,))

    async def page_surrogates(self, page_num: int = 1, page_size: int = 10,
                              filters: Optional[dict] = None) -> tuple[list[ProcessSurrogate], int]:
        sql = f"SELECT {self._SURROGATE_COLS} FROM wf_process_surrogate t WHERE 1=1"
        count_sql = "SELECT COUNT(*) FROM wf_process_surrogate t WHERE 1=1"
        args: list[Any] = []
        args2: list[Any] = []
        for col, val in (filters or {}).items():
            if col in ("operator", "surrogate", "process_name", "enabled"):
                sql += f" AND t.{col} = ?"
                count_sql += f" AND t.{col} = ?"
                args.append(val)
                args2.append(val)
        async with self._conn() as conn:
            row = await conn.fetchone(self._sql(count_sql), args2)
            total = int(row[0]) if row else 0
            sql += " ORDER BY t.id DESC LIMIT ? OFFSET ?"
            args.extend([page_size, (page_num - 1) * page_size])
            rows = await conn.fetchall(self._sql(sql), args)
        return [self._map_surrogate(r) for r in rows], total

    async def get_surrogate(self, operator: str, process_name: str, at=None) -> Optional[ProcessSurrogate]:
        at = at or datetime.now()
        hit = await self._query_surrogate(operator, process_name, at)
        if hit:
            return hit
        return await self._query_surrogate(operator, "", at)

    async def _query_surrogate(self, operator: str, process_name: str, at) -> Optional[ProcessSurrogate]:
        sql = f"SELECT {self._SURROGATE_COLS} FROM wf_process_surrogate" \
              " WHERE operator = ? AND enabled = 1 AND surrogate <> ?"
        args: list[Any] = [operator, operator]
        if not process_name:
            sql += " AND (process_name IS NULL OR process_name = '')"
        else:
            sql += " AND process_name = ?"
            args.append(process_name)
        if at is not None:
            sql += " AND (start_time IS NULL OR start_time <= ?) AND (end_time IS NULL OR end_time >= ?)"
            args.extend([at, at])
        sql += " ORDER BY id DESC LIMIT 1"
        async with self._conn() as conn:
            rows = await conn.fetchall(self._sql(sql), args)
        return self._map_surrogate(rows[0]) if rows else None

    # ── 行映射 ───────────────────────────────────────────────────────────────

    @staticmethod
    def _map_design(r: Sequence[Any]) -> ProcessDesign:
        return ProcessDesign(id=r[0], name=r[1], displayName=r[2], type=r[3], icon=r[4],
                             isDeployed=r[5], remark=r[6], createTime=r[7], createUser=r[8],
                             updateTime=r[9], updateUser=r[10])

    @staticmethod
    def _map_surrogate(r: Sequence[Any]) -> ProcessSurrogate:
        return ProcessSurrogate(id=r[0], processName=r[1], operator=r[2], surrogate=r[3],
                                startTime=r[4], endTime=r[5], enabled=r[6], createTime=r[7],
                                createUser=r[8], updateTime=r[9], updateUser=r[10])
