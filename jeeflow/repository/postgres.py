"""PostgreSQL 适配器（asyncpg）——连接池 + `$n` 占位符。

> 已在开发服务器实测（PostgreSQL 16，Docker mldong-pg）。
> asyncpg 事务模型为 `conn.transaction()` 对象，begin/commit/rollback 由此映射，
> 其余接口与 mysql.py 对齐——核心 SQL 由 base.convert_placeholder 统一转换。
> 惰性引用：不安装 asyncpg 也能导入本包（类型标注仅 TYPE_CHECKING 时可见）。
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Sequence

from .base import SqlAdapter, SqlConnection

if TYPE_CHECKING:
    import asyncpg


class PostgresConnection(SqlConnection):
    """包装 asyncpg.Connection"""

    def __init__(self, conn):
        self._conn = conn
        self._tx: Optional[Any] = None

    async def execute(self, sql: str, args: Sequence[Any]) -> None:
        await self._conn.execute(sql, *args)

    async def fetchone(self, sql: str, args: Sequence[Any]) -> Optional[tuple]:
        return await self._conn.fetchrow(sql, *args)

    async def fetchall(self, sql: str, args: Sequence[Any]) -> list[tuple]:
        return await self._conn.fetch(sql, *args)

    async def begin(self) -> None:
        self._tx = self._conn.transaction()
        await self._tx.start()

    async def commit(self) -> None:
        await self._tx.commit()

    async def rollback(self) -> None:
        await self._tx.rollback()


class PostgresAdapter(SqlAdapter):
    """asyncpg 连接池适配器"""

    placeholder = "$n"

    def __init__(self, pool):
        self._pool = pool

    async def acquire(self) -> PostgresConnection:
        return PostgresConnection(await self._pool.acquire())

    async def release(self, conn: SqlConnection) -> None:
        await self._pool.release(conn._conn)
