"""元数据驱动读写测试（issues/23 阶段①②③，SQLite 内存库全链路）"""
import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from jeeflow.meta import (
    JsonMetaProvider, MetaTableReader, MetaTableWriter, JdbcTableReader,
    StorageType, TableMeta, FieldMeta,
)
from jeeflow.persist import JdbcDynamicTableWriter


def test_json_provider():
    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, "biz_leave.json"), "w", encoding="utf-8") as f:
            f.write('''{"tableName":"biz_leave","primaryKey":"id","fields":[
                {"name":"companyName","columnName":"company_name","storageType":"NORMAL"},
                {"name":"address","storageType":2,"expandFields":{"province":"province","city":"city"}},
                {"name":"extra","storageType":"JSON"},
                {"name":"items","storageType":5,"targetTable":"biz_leave_item","foreignKey":"leave_id"}]}''')
        p = JsonMetaProvider(d)
        meta = p.load_table_meta("biz_leave")
        assert meta is not None
        assert meta.find_field("companyName").column() == "company_name"
        assert meta.find_field("address").storage_type == StorageType.EXPAND
        assert meta.find_field("items").storage_type == StorageType.ONE2MANY
        assert p.load_table_meta("no_such") is None


class _MapProvider:
    def __init__(self, metas):
        self.metas = metas
    def load_table_meta(self, table_name):
        return self.metas.get(table_name)


def _provider():
    return _MapProvider({
        "biz_leave": TableMeta(table_name="biz_leave", primary_key="id", fields=[
            FieldMeta(name="companyName"),
            FieldMeta(name="amount"),
            FieldMeta(name="extra", storage_type=StorageType.JSON),
            FieldMeta(name="address", storage_type=StorageType.EXPAND,
                      expand_fields={"province": "province", "city": "city", "detail": "detail_addr"}),
            FieldMeta(name="addressRel", storage_type=StorageType.ONE2ONE,
                      target_table="biz_leave_address", foreign_key="leave_id"),
            FieldMeta(name="items", storage_type=StorageType.ONE2MANY,
                      target_table="biz_leave_item", foreign_key="leave_id"),
        ]),
        "biz_leave_address": TableMeta(table_name="biz_leave_address", fields=[
            FieldMeta(name="province"), FieldMeta(name="city"),
            FieldMeta(name="detail", column_name="detail_addr")]),
        "biz_leave_item": TableMeta(table_name="biz_leave_item", fields=[
            FieldMeta(name="name"), FieldMeta(name="qty")]),
    })


def test_meta_full_cycle():
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE biz_leave (id INTEGER PRIMARY KEY AUTOINCREMENT, company_name TEXT,
            amount REAL, extra TEXT, province TEXT, city TEXT, detail_addr TEXT, process_instance_id INTEGER);
        CREATE TABLE biz_leave_address (id INTEGER PRIMARY KEY AUTOINCREMENT, leave_id INTEGER,
            province TEXT, city TEXT, detail_addr TEXT);
        CREATE TABLE biz_leave_item (id INTEGER PRIMARY KEY AUTOINCREMENT, leave_id INTEGER,
            name TEXT, qty INTEGER);
    """)
    provider = _provider()
    base = JdbcDynamicTableWriter(conn)
    writer = MetaTableWriter(base, provider)
    reader = MetaTableReader(JdbcTableReader(conn), provider)

    writer.insert("biz_leave", {
        "companyName": "复杂公司",
        "amount": 800,
        "extra": {"tag": "vip", "level": 3},
        "address": {"province": "广东省", "city": "深圳市", "detail": "科技园路1号"},
        "addressRel": {"province": "广东省", "city": "广州市", "detail": "天河区"},
        "items": [{"name": "电脑", "qty": 2}, {"name": "键盘", "qty": 3}],
        "process_instance_id": 888,
    })

    # 落库断言
    assert conn.execute("SELECT province, city FROM biz_leave").fetchone() == ("广东省", "深圳市")
    assert conn.execute("SELECT COUNT(1) FROM biz_leave_item").fetchone()[0] == 2

    # 回显组装
    result = reader.read_by_process_instance("biz_leave", 888)
    assert result is not None
    assert result["companyName"] == "复杂公司"
    assert result["extra"] == {"tag": "vip", "level": 3}
    assert result["address"]["city"] == "深圳市"
    assert result["addressRel"]["city"] == "广州市"
    assert len(result["items"]) == 2
    assert result["items"][0]["name"] == "电脑"
    assert result["process_instance_id"] == 888
    conn.close()


def test_meta_fallback():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE biz_leave (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, process_instance_id INTEGER)")
    provider = _MapProvider({})
    base = JdbcDynamicTableWriter(conn)
    writer = MetaTableWriter(base, provider)
    reader = MetaTableReader(JdbcTableReader(conn), provider)
    writer.insert("biz_leave", {"title": "回落", "process_instance_id": 1})
    result = reader.read_by_process_instance("biz_leave", 1)
    assert result["title"] == "回落"
    conn.close()
