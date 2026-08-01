"""JDBC 仓储（多数据库）——共享核心 + 数据库适配器

- `JdbcRepository` / `TsIDGenerator`（共享核心，见 base.py）
- `MySqlAdapter`（aiomysql）/ `PostgresAdapter`（asyncpg）
"""
from .base import JdbcRepository, TsIDGenerator, convert_placeholder, repeat_ph
from .mysql import MySqlAdapter, MysqlConnection
from .postgres import PostgresAdapter, PostgresConnection

__all__ = [
    "JdbcRepository", "TsIDGenerator", "convert_placeholder", "repeat_ph",
    "MySqlAdapter", "MysqlConnection",
    "PostgresAdapter", "PostgresConnection",
]
