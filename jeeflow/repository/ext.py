"""扩展仓储 JDBC 参考实现（v1.1.0）——流程设计 / 设计历史 / 委托代理

与 base.py 同一套 SqlAdapter / 占位符约定；分页为单表简单过滤（filters 字段名 EQ）。
"""
from __future__ import annotations

import contextlib
from datetime import datetime
from typing import Any, Optional, Sequence

from ..model import ProcessDesign, ProcessDesignHis, ProcessSurrogate
from ..spi import IDGenerator, ProcessExtRepository, QueryCondition
from ..surrogate import to_datetime   # 判定时刻归一（与内存仓同一把尺子，条款 6 双仓同答案）
from .base import SqlAdapter, TsIDGenerator, convert_placeholder, _tx_conn_var, _user_str


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
                           filters: Optional[dict] = None,
                           conditions: Optional[list[QueryCondition]] = None) -> tuple[list[ProcessDesign], int]:
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
        # m_ 条件（issues/05-5）：LIKE/EQ 等走白名单
        cond_sql, cond_args = self._build_ext_where(conditions or [], _DESIGN_WHITELIST)
        sql += cond_sql
        count_sql += cond_sql
        args.extend(cond_args)
        args2.extend(cond_args)
        async with self._conn() as conn:
            row = await conn.fetchone(self._sql(count_sql), args2)
            total = int(row[0]) if row else 0
            sql += " ORDER BY t.id DESC LIMIT ? OFFSET ?"
            args.extend([page_size, (page_num - 1) * page_size])
            rows = await conn.fetchall(self._sql(sql), args)
        return [self._map_design(r) for r in rows], total

    def _build_ext_where(self, conditions: list, whitelist: set) -> tuple[str, tuple]:
        """m_ 条件 WHERE 构建（issues/05-5，白名单 + 参数化）"""
        sql = ""
        args = []
        for c in conditions or []:
            if c.column not in whitelist:
                continue
            val = c.value
            if val is None or val == "":
                continue
            op = c.operator.upper()
            if op == "EQ":
                sql += f" AND {c.column} = ?"; args.append(val)
            elif op == "LIKE":
                sql += f" AND {c.column} LIKE ?"; args.append(f"%{val}%")
            elif op == "LLIKE":
                sql += f" AND {c.column} LIKE ?"; args.append(f"%{val}")
            elif op == "RLIKE":
                sql += f" AND {c.column} LIKE ?"; args.append(f"{val}%")
            elif op == "IN":
                if isinstance(val, (list, tuple)) and len(val) > 0:
                    marks = ",".join(["?"] * len(val))
                    sql += f" AND {c.column} IN ({marks})"
                    args.extend(val)
        return sql, tuple(args)

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
                                           createTime=r[3], createUser=_user_str(r[4])))
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
        # 显式 enabled=0 是合法值（停用委托）；缺省由门面处理（对齐 Java/Go，issues/82-7）
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
                              filters: Optional[dict] = None,
                              conditions: Optional[list[QueryCondition]] = None) -> tuple[list[ProcessSurrogate], int]:
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
        # m_ 条件（issues/82-7 委托分页，对齐 page_designs）：LIKE/EQ/IN 等走白名单
        cond_sql, cond_args = self._build_ext_where(conditions or [], _SURROGATE_WHITELIST)
        sql += cond_sql
        count_sql += cond_sql
        args.extend(cond_args)
        args2.extend(cond_args)
        async with self._conn() as conn:
            row = await conn.fetchone(self._sql(count_sql), args2)
            total = int(row[0]) if row else 0
            sql += " ORDER BY t.id DESC LIMIT ? OFFSET ?"
            args.extend([page_size, (page_num - 1) * page_size])
            rows = await conn.fetchall(self._sql(sql), args)
        return [self._map_surrogate(r) for r in rows], total

    async def get_surrogate(self, operator: str, process_name: str, at=None) -> Optional[ProcessSurrogate]:
        """生效委托查询——与内存仓 ``MemoryExtRepository.get_surrogate`` 同形同答案（06 §4.5 条款 6）。

        条款 1.4：多条同时命中时按主键 id 取**最新一条**，再交 ``ProcessSurrogate.is_effective``
        裁决。反过来写（SQL 先把 enabled/窗口/自委托滤掉、剩下的才排序）等价于"历史上出现过一条
        窗内委托就永久生效"——用户随后改停用、改到未来都不算数，这就是 issues/123 的成因。
        两个作用域各取自己最新的一条、各自裁决：精确作用域那条判否时仍要看全流程作用域的最新一条
        （"精确已过期 → 兜底全流程委托"是既有钉住的行为，不得改成判否即止）。
        """
        if operator is None:
            return None
        at = to_datetime(at) or datetime.now()   # 引擎钟：与写入侧同一把尺子（条款 5 / issues/120）
        exact = await self._query_newest_surrogate(operator, process_name)
        if exact is not None and exact.is_effective(operator, at):
            return exact
        global_ = await self._query_newest_surrogate(operator, "")
        return global_ if global_ is not None and global_.is_effective(operator, at) else None

    async def _query_newest_surrogate(self, operator: str, process_name: str) -> Optional[ProcessSurrogate]:
        """取该授权人在指定流程作用域内**最新的一条**委托；只排序取首行，**不带任何生效判据过滤**
        （``enabled = 1`` / 时间窗 / ``surrogate <> ?`` 三条谓词已撤，判据落在读出后的单条裁决里）。"""
        sql = f"SELECT {self._SURROGATE_COLS} FROM wf_process_surrogate WHERE operator = ?"
        args: list[Any] = [operator]
        if not process_name:
            sql += " AND (process_name IS NULL OR process_name = '')"
        else:
            sql += " AND process_name = ?"
            args.append(process_name)
        sql += " ORDER BY id DESC LIMIT 1"
        async with self._conn() as conn:
            rows = await conn.fetchall(self._sql(sql), args)
        return self._map_surrogate(rows[0]) if rows else None

    # ── 行映射 ───────────────────────────────────────────────────────────────

    @staticmethod
    def _map_design(r: Sequence[Any]) -> ProcessDesign:
        return ProcessDesign(id=r[0], name=r[1], displayName=r[2], type=r[3], icon=r[4],
                             isDeployed=r[5], remark=r[6], createTime=r[7], createUser=_user_str(r[8]),
                             updateTime=r[9], updateUser=_user_str(r[10]))

    @staticmethod
    def _map_surrogate(r: Sequence[Any]) -> ProcessSurrogate:
        return ProcessSurrogate(id=r[0], processName=r[1], operator=r[2], surrogate=r[3],
                                startTime=r[4], endTime=r[5], enabled=r[6], createTime=r[7],
                                createUser=_user_str(r[8]), updateTime=r[9], updateUser=_user_str(r[10]))


# ═══ 列白名单（issues/05-5，与 mldong-boot2 别名一致） ═══

_DESIGN_WHITELIST = {
    "t.id", "t.name", "t.display_name", "t.type", "t.is_deployed", "t.remark",
    "t.create_time", "t.update_time",
}

_SURROGATE_WHITELIST = {
    "t.id", "t.process_name", "t.operator", "t.surrogate", "t.enabled",
    "t.start_time", "t.end_time", "t.create_time", "t.update_time",
}
