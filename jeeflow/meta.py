"""元数据驱动的动态写入/读取规范（issues/23）——与 Java/Go 契约一致。

- ``StorageType``：字段存储类型（对齐 mldong dev_schema_field 1-5 语义）
- ``FieldMeta`` / ``TableMeta``：字段/表元数据模型
- ``IDynamicMetaProvider``：元数据提供者 SPI（写、读共用）
- ``JsonMetaProvider``：内置 JSON 配置加载器（json 标准库零依赖）
- ``MetaTableWriter``：元数据驱动的动态写入引擎（纯写职责，无元数据回落基础 writer）
- ``JdbcTableReader``：业务表查询器（读侧底层）
- ``MetaTableReader``：流程回显读取（readByProcessInstance，与写入共用元数据）
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Optional, Sequence

from .persist import DynamicTableWriter, JdbcDynamicTableWriter


# ─── StorageType ────────────────────────────────────────────────────────────────

class StorageType(IntEnum):
    """字段存储类型（mldong dev_schema_field 1-5 语义）"""
    NORMAL = 1    # 直写列
    EXPAND = 2    # 对象展开为多列（expand_fields 定义子字段列映射）
    JSON = 3      # 对象/数组序列化为 JSON 串写列
    ONE2ONE = 4   # 子表单条（外键=主表主键，同事务）
    ONE2MANY = 5  # 子表多条（外键=主表主键，同事务）


def _parse_storage_type(v) -> StorageType:
    """JSON 配置解析：支持名称（"EXPAND"）与数字（2）"""
    if isinstance(v, StorageType):
        return v
    if isinstance(v, int):
        return StorageType(v)
    return StorageType[v.upper()]


# ─── 元数据模型 ────────────────────────────────────────────────────────────────

@dataclass
class FieldMeta:
    name: str = ""                             # 表单字段名（f_ 去前缀）
    column_name: str = ""                      # 主表列名（缺省 = name 转下划线）
    storage_type: StorageType = StorageType.NORMAL
    expand_fields: dict[str, str] = field(default_factory=dict)  # EXPAND：子字段名 → 表列名
    target_table: str = ""                     # ONE2ONE/ONE2MANY：子表表名
    foreign_key: str = ""                      # 子表外键列（缺省 = 主表主键列名）

    def column(self) -> str:
        if self.column_name:
            return self.column_name
        return to_underline(self.name)


@dataclass
class TableMeta:
    table_name: str = ""
    primary_key: str = "id"
    fields: list[FieldMeta] = field(default_factory=list)

    def pk(self) -> str:
        return self.primary_key or "id"

    def find_field(self, name: str) -> Optional[FieldMeta]:
        for f in self.fields:
            if f.name.lower() == (name or "").lower():
                return f
        return None

    def find_field_by_column(self, column_name: str) -> Optional[FieldMeta]:
        for f in self.fields:
            if f.column().lower() == (column_name or "").lower():
                return f
        return None


def to_underline(name: str) -> str:
    """驼峰转下划线（companyName → company_name）"""
    out = []
    for i, c in enumerate(name):
        if c.isupper():
            if i > 0:
                out.append("_")
            out.append(c.lower())
        else:
            out.append(c)
    return "".join(out)


# ─── IDynamicMetaProvider ───────────────────────────────────────────────────────

class IDynamicMetaProvider:
    """动态元数据提供者 SPI——集成方只实现这一件事；未定义返回 None（回落表结构探测）"""

    def load_table_meta(self, table_name: str) -> Optional[TableMeta]:
        raise NotImplementedError


# ─── JsonMetaProvider ───────────────────────────────────────────────────────────

class JsonMetaProvider(IDynamicMetaProvider):
    """内置 JSON 配置加载器——从目录加载（文件名 = 表名，如 biz_leave.json）"""

    def __init__(self, dir_path: str):
        self.dir = dir_path
        self._cache: dict[str, TableMeta] = {}

    def load_table_meta(self, table_name: str) -> Optional[TableMeta]:
        if table_name in self._cache:
            return self._cache[table_name]
        path = os.path.join(self.dir, table_name + ".json")
        if not os.path.isfile(path):
            return None
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
        meta = TableMeta(
            table_name=raw.get("tableName", table_name),
            primary_key=raw.get("primaryKey") or "id",
        )
        for f in raw.get("fields", []):
            meta.fields.append(FieldMeta(
                name=f["name"],
                column_name=f.get("columnName", ""),
                storage_type=_parse_storage_type(f.get("storageType", 1)),
                expand_fields=f.get("expandFields", {}) or {},
                target_table=f.get("targetTable", ""),
                foreign_key=f.get("foreignKey", ""),
            ))
        self._cache[table_name] = meta
        return meta


# ─── MetaTableWriter（写，纯写职责） ─────────────────────────────────────────────

class MetaTableWriter(DynamicTableWriter):
    """元数据驱动的动态写入器——按 TableMeta.storageType 语义执行插入；
    无元数据时完全委托基础 writer（回落现状，零破坏）。"""

    def __init__(self, base: DynamicTableWriter, provider: IDynamicMetaProvider):
        self.base = base
        self.provider = provider

    def filter_columns(self, table_name: str, columns: Sequence[str]) -> list[str]:
        return self.base.filter_columns(table_name, columns)

    def insert(self, table_name: str, data: dict[str, Any]) -> Any:
        meta = self.provider.load_table_meta(table_name)
        if meta is None:
            return self.base.insert(table_name, data)  # 无元数据：回落现状
        sub_data: dict[str, Any] = {}
        row: dict[str, Any] = {}
        for f in meta.fields:
            v = data.get(f.name)
            if v is None:
                continue
            if f.storage_type == StorageType.JSON:
                row[f.column()] = json.dumps(v, ensure_ascii=False)
            elif f.storage_type == StorageType.EXPAND:
                self._expand_into(f, v, row)
            elif f.storage_type in (StorageType.ONE2ONE, StorageType.ONE2MANY):
                sub_data[f.name] = v
            else:
                row[f.column()] = v
        # 未消费字段（流程上下文 process_instance_id 等）直通基础 writer
        for k, v in data.items():
            if meta.find_field(k) is None:
                row.setdefault(k, v)
        self.base.fill_system_fields(row, True)
        pk = self.base.insert(table_name, row)  # 主表插入（自增/生成器返回主键）
        if pk is None:
            pk = _find_row_value(row, meta.pk())
        # 子表递归插入（外键=主表主键；继承主表 apply_user_id，issues/24）
        for name, v in sub_data.items():
            self._insert_sub_table(meta, meta.find_field(name), v, pk, data)
        return pk

    def update(self, table_name: str, data: dict[str, Any], where_column: str, where_value: Any) -> int:
        """按元数据 storageType 组装 SET 列（SYNC 同步演进，issues/24）——
        NORMAL/JSON/EXPAND 参与更新；ONE2ONE/ONE2MANY 子表不参与中途更新
        （任务推进只更新主表行状态，子表数据变动走重新提交）；未消费字段直通。"""
        meta = self.provider.load_table_meta(table_name)
        if meta is None:
            return self.base.update(table_name, data, where_column, where_value)  # 无元数据：回落基础 writer
        row: dict[str, Any] = {}
        for f in meta.fields:
            v = data.get(f.name)
            if v is None:
                continue
            if f.storage_type == StorageType.JSON:
                row[f.column()] = json.dumps(v, ensure_ascii=False)
            elif f.storage_type == StorageType.EXPAND:
                self._expand_into(f, v, row)
            elif f.storage_type in (StorageType.ONE2ONE, StorageType.ONE2MANY):
                continue  # 子表不参与中途更新
            else:
                row[f.column()] = v
        # 未消费字段（流程上下文/状态字段等）直通基础 writer
        for k, v in data.items():
            if meta.find_field(k) is None:
                row.setdefault(k, v)
        return self.base.update(table_name, row, where_column, where_value)

    def exists(self, table_name: str, biz_key: str, biz_key_value: Any) -> bool:
        return self.base.exists(table_name, biz_key, biz_key_value)

    def fill_system_fields(self, data: dict[str, Any], is_insert: bool) -> None:
        self.base.fill_system_fields(data, is_insert)

    @staticmethod
    def _expand_into(f: FieldMeta, v: Any, row: dict[str, Any]) -> None:
        if not isinstance(v, dict):
            return
        for sub, col in f.expand_fields.items():
            if v.get(sub) is not None:
                row[col] = v[sub]

    def _insert_sub_table(self, parent_meta: TableMeta, f: FieldMeta, v: Any, parent_pk: Any,
                          parent_data: dict[str, Any]) -> None:
        if parent_pk is None:
            raise ValueError(f"persist: parent primary key missing, cannot insert sub table {f.name}")
        fk = f.foreign_key or parent_meta.pk()
        if f.storage_type == StorageType.ONE2ONE:
            if isinstance(v, dict):
                self._insert_sub_row(f, v, fk, parent_pk, parent_data)
        elif isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    self._insert_sub_row(f, item, fk, parent_pk, parent_data)

    def _insert_sub_row(self, f: FieldMeta, sub_data: dict, fk: str, parent_pk: Any,
                        parent_data: dict[str, Any]) -> None:
        row = dict(sub_data)
        # issues/24：继承主表 apply_user_id（拦截器场景=流程 operator），子表单显式同名字段优先
        # （setdefault）——fill_system_fields 的用户列默认值可解析到 operator，
        # 避免 BIGINT create_user/update_user 列回落 "system" 严格模式报错
        if "apply_user_id" in parent_data:
            row.setdefault("apply_user_id", parent_data["apply_user_id"])
        row[fk] = parent_pk
        self.insert(f.target_table, row)  # 递归走子表自身元数据


# ─── JdbcTableReader（读侧底层） ────────────────────────────────────────────────

class JdbcTableReader:
    """业务表查询器——按列等值查询原始行（与写入器职责分离）"""

    def __init__(self, conn):
        self._conn = conn

    def query_first(self, table_name: str, where_column: str, value: Any) -> Optional[dict[str, Any]]:
        rows = self.query_list(table_name, where_column, value, limit=1)
        return rows[0] if rows else None

    def query_list(self, table_name: str, where_column: str, value: Any, limit: int = 0) -> list[dict[str, Any]]:
        _check(table_name)
        sql = f"SELECT * FROM {table_name} WHERE {where_column} = ?"
        if limit > 0:
            sql += f" LIMIT {limit}"
        cur = self._conn.execute(sql, (value,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _check(table_name: str) -> None:
    """表名安全校验（读侧）"""
    from .persist import _check_table_name
    _check_table_name(table_name)


# ─── MetaTableReader（读，流程回显最小闭环） ─────────────────────────────────────

class MetaTableReader:
    """元数据驱动的动态读取器——按流程实例回显业务数据。
    边界（不做）：通用条件分页 / 动态条件语法 / 数据权限 / 排序。"""

    def __init__(self, reader: JdbcTableReader, provider: IDynamicMetaProvider):
        self.reader = reader
        self.provider = provider

    def read_by_process_instance(self, table_name: str, process_instance_id: Any) -> Optional[dict[str, Any]]:
        row = self.reader.query_first(table_name, "process_instance_id", process_instance_id)
        if row is None:
            return None
        meta = self.provider.load_table_meta(table_name)
        if meta is None:
            return row  # 无元数据：原样返回（列名→值）
        return self.assemble(meta, row)

    def assemble(self, meta: TableMeta, row: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for f in meta.fields:
            v = _find_row_value(row, f.column())
            if f.storage_type == StorageType.JSON:
                if v is not None:
                    try:
                        result[f.name] = json.loads(str(v))
                    except (ValueError, TypeError):
                        result[f.name] = v
            elif f.storage_type == StorageType.EXPAND:
                obj = self._expand_from(row, f)
                if obj:
                    result[f.name] = obj
            elif f.storage_type in (StorageType.ONE2ONE, StorageType.ONE2MANY):
                sub = self._read_sub_table(meta, f, row)
                if sub is not None:
                    result[f.name] = sub
            elif v is not None:
                result[f.name] = v
        # 未在元数据中的列带出（key 统一小写）；
        # EXPAND 展开列（挂在某字段 expand_fields 映射里，对象形式已带出）不重复平铺（issues/24）
        for k, v in row.items():
            if meta.find_field_by_column(k) is None and not _is_expand_column(meta, k):
                result.setdefault(k.lower(), v)
        return result

    @staticmethod
    def _expand_from(row: dict[str, Any], f: FieldMeta) -> dict[str, Any]:
        obj = {}
        for sub, col in f.expand_fields.items():
            v = _find_row_value(row, col)
            if v is not None:
                obj[sub] = v
        return obj

    def _read_sub_table(self, parent_meta: TableMeta, f: FieldMeta, row: dict[str, Any]):
        parent_pk = _find_row_value(row, parent_meta.pk())
        if parent_pk is None:
            return None
        fk = f.foreign_key or parent_meta.pk()
        sub_meta = self.provider.load_table_meta(f.target_table)
        if f.storage_type == StorageType.ONE2ONE:
            sub = self.reader.query_first(f.target_table, fk, parent_pk)
            if sub is None:
                return None
            return self.assemble(sub_meta, sub) if sub_meta else sub
        subs = self.reader.query_list(f.target_table, fk, parent_pk)
        return [self.assemble(sub_meta, sub) if sub_meta else sub for sub in subs]


def _find_row_value(row: dict[str, Any], column_name: str) -> Any:
    for k, v in row.items():
        if k.lower() == (column_name or "").lower():
            return v
    return None


def _is_expand_column(meta: TableMeta, column: str) -> bool:
    """列是否为某字段的 EXPAND 展开列（issues/24：已消费，不重复平铺带出）"""
    for f in meta.fields:
        for col in f.expand_fields.values():
            if col.lower() == (column or "").lower():
                return True
    return False
