"""MySQL 适配器（aiomysql）——连接池 + `%s` 占位符。

> 连接池必须 `autocommit=True` 创建：无事务时每条语句立即提交，
> `with_tx` 内 begin/commit/rollback 显式控制（与 Go database/sql 语义对齐）。
"""
from __future__ import annotations

from typing import Any, Optional, Sequence

import aiomysql

from .base import SqlAdapter, SqlConnection


class MysqlConnection(SqlConnection):
    """包装 aiomysql.Connection——每次操作临时 cursor"""

    def __init__(self, conn: aiomysql.Connection):
        self._conn = conn

    async def execute(self, sql: str, args: Sequence[Any]) -> None:
        async with self._conn.cursor() as cur:
            await cur.execute(sql, args)

    async def fetchone(self, sql: str, args: Sequence[Any]) -> Optional[tuple]:
        async with self._conn.cursor() as cur:
            await cur.execute(sql, args)
            return await cur.fetchone()

    async def fetchall(self, sql: str, args: Sequence[Any]) -> list[tuple]:
        async with self._conn.cursor() as cur:
            await cur.execute(sql, args)
            return await cur.fetchall()

    async def begin(self) -> None:
        await self._conn.begin()

    async def commit(self) -> None:
        await self._conn.commit()

    async def rollback(self) -> None:
        await self._conn.rollback()


class MySqlAdapter(SqlAdapter):
    """aiomysql 连接池适配器"""

    placeholder = "%s"

    def __init__(self, pool: aiomysql.Pool):
        self._pool = pool

    async def acquire(self) -> MysqlConnection:
        return MysqlConnection(await self._pool.acquire())

    async def release(self, conn: SqlConnection) -> None:
        await self._pool.release(conn._conn)
