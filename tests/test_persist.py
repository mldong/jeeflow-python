"""persist 动态表写入 + 流程入库拦截器测试（issues/18，SQLite 内存库全链路）"""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository, EngineExtensions
from jeeflow.memory import MemoryExtRepository
from jeeflow.model import ProcessDefine, InstanceState, UserInfo
from jeeflow.persist import JdbcDynamicTableWriter, PersistPostInterceptor
from jeeflow.spi import UserProvider, IDGenerator, ExpressionEvaluator
from jeeflow.engine import KEY_SUBMIT_TYPE

FLOW_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "jeeflow-java",
                        "jeeflow-core", "src", "test", "resources", "flows")

# ─── Test Stubs（与 spec_test 同构） ─────────────────────────────────────────────

class _TestUserProv(UserProvider):
    async def get_user(self, user_id: str):
        return UserInfo(userId=user_id, realName=f"用户{user_id}", deptId="D01",
                        deptName="测试部门", postId="P01", postName="测试岗位")

class _TestIDGen(IDGenerator):
    def __init__(self): self.n = 0
    def next_id(self) -> int:
        self.n += 1; return self.n

class _TestExprEval(ExpressionEvaluator):
    async def eval(self, expr: str, vars: dict):
        return False


def setup_db():
    conn = sqlite3.connect(":memory:")
    conn.execute("""CREATE TABLE biz_leave (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        amount REAL,
        process_instance_id INTEGER,
        apply_user_id TEXT,
        apply_dept_id TEXT,
        create_time TEXT,
        create_user TEXT,
        update_time TEXT,
        update_user TEXT,
        is_deleted INTEGER
    )""")
    return conn, JdbcDynamicTableWriter(conn)


def load_flow(repo: MemoryRepository, with_rel_table: bool = True) -> ProcessDefine:
    with open(os.path.join(FLOW_DIR, "01-simple.json"), "r", encoding="utf-8") as f:
        content = f.read()
    if with_rel_table:
        content = content.replace('"type": "approval"',
                                  '"type": "approval", "relTableName": "biz_leave"', 1)
    d = ProcessDefine(name="simple", displayName="01-simple.json", type="approval",
                      state=1, version=1, content=content)
    repo.add_define(d)
    return d


def setup_engine(repo, writer=None):
    eng = EngineImpl(repo, _TestUserProv(), _TestIDGen(), _TestExprEval())
    ic = PersistPostInterceptor(writer=writer, loader=repo.find_define_by_id)
    eng.set_extensions(EngineExtensions(interceptors=[ic]))
    return eng


async def _run_flow(eng, repo, define_id, agree=True):
    """启动 → apply（user1）→ task1（leader 同意/拒绝）"""
    inst = await eng.start_process_instance_by_id(define_id, "user1",
        {"f_title": "年假申请", "f_amount": 800.0, "u_deptId": "D01"})
    doing = await repo.find_doing_tasks(inst.id)
    assert len(doing) == 1 and doing[0].taskName == "apply"
    await repo.add_task_actor(doing[0].id, ["user1"])
    await eng.execute_process_task(doing[0].id, "user1", {KEY_SUBMIT_TYPE: 0})

    doing = await repo.find_doing_tasks(inst.id)
    assert len(doing) == 1 and doing[0].taskName == "task1"
    await repo.add_task_actor(doing[0].id, ["leader"])
    st = 1 if agree else 2
    await eng.execute_process_task(doing[0].id, "leader", {KEY_SUBMIT_TYPE: st})
    return inst


def _count(conn) -> int:
    return conn.execute("SELECT COUNT(1) FROM biz_leave").fetchone()[0]


# ─── ① 流程结束同意 → 业务表落库（f_ 去前缀 + 系统字段 + 流程上下文） ─────────

@pytest.mark.asyncio
async def test_flow_finish_persist():
    conn, writer = setup_db()
    repo = MemoryRepository()
    eng = setup_engine(repo, writer)
    df = load_flow(repo, True)
    inst = await _run_flow(eng, repo, df.id, True)

    row = conn.execute("SELECT title, amount, process_instance_id, apply_user_id, "
                       "apply_dept_id, create_user, is_deleted FROM biz_leave").fetchone()
    assert row == ("年假申请", 800.0, inst.id, "user1", "D01", "user1", 0)  # issues/19: 用户列默认值优先 operator
    assert _count(conn) == 1
    conn.close()


# ─── ② 不同意/退回 → 不入库 ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_reject_no_persist():
    conn, writer = setup_db()
    repo = MemoryRepository()
    eng = setup_engine(repo, writer)
    df = load_flow(repo, True)
    await _run_flow(eng, repo, df.id, agree=False)
    assert _count(conn) == 0
    conn.close()


# ─── ③ 未注入 writer → 静默跳过 ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_no_writer_skip():
    conn, writer = setup_db()
    repo = MemoryRepository()
    eng = setup_engine(repo, None)  # 不注入 writer
    df = load_flow(repo, True)
    await _run_flow(eng, repo, df.id, True)
    assert _count(conn) == 0
    conn.close()


# ─── ④ 未配置 relTableName → 缺省回落流程 name，表不存在 → 显性报错（配置错误快速失败） ─

@pytest.mark.asyncio
async def test_no_table_name_rejects():
    conn, writer = setup_db()
    repo = MemoryRepository()
    eng = setup_engine(repo, writer)
    df = load_flow(repo, False)
    with pytest.raises(ValueError):
        await _run_flow(eng, repo, df.id, True)
    conn.close()


# ─── ⑤ 幂等：同实例重复触发不重复插 ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_idempotent_flow():
    conn, writer = setup_db()
    repo = MemoryRepository()
    eng = setup_engine(repo, writer)
    df = load_flow(repo, True)
    inst = await _run_flow(eng, repo, df.id, True)
    assert _count(conn) == 1

    # 模拟重复触发：拦截器直接再跑一次
    ic = eng.ext.interceptors[0]
    node = type("N", (), {"type": "snaker:end"})()
    await ic.post_handle(node, inst)
    assert _count(conn) == 1
    conn.close()


# ─── ⑥ writer 全字段插入（数据 + 系统字段） ────────────────────────────────────

def test_writer_insert_full():
    conn, writer = setup_db()
    data = {"title": "年假申请", "amount": 800.0, "process_instance_id": 1,
            "apply_user_id": "user1", "apply_dept_id": "D01"}
    writer.fill_system_fields(data, True)
    writer.insert("biz_leave", data)
    row = conn.execute("SELECT title, process_instance_id, create_user, is_deleted "
                       "FROM biz_leave").fetchone()
    assert row == ("年假申请", 1, "user1", 0)  # issues/19: 用户列默认值优先 operator
    conn.close()


# ─── ⑦ writer 缺列过滤 ─────────────────────────────────────────────────────────

def test_writer_filter_columns():
    conn, writer = setup_db()
    kept = writer.filter_columns("biz_leave", ["title", "no_such_col", "amount"])
    assert kept == ["title", "amount"]
    conn.close()


# ─── ⑧ writer 类型 null / 防注入 / 表名安全 ────────────────────────────────────

def test_writer_null_and_safety():
    conn, writer = setup_db()
    writer.insert("biz_leave", {"title": "t", "amount": None, "apply_dept_id": None})
    writer.insert("biz_leave", {"title": "x'); DROP TABLE biz_leave; --"})
    assert _count(conn) == 2
    with pytest.raises(ValueError):
        writer.insert("sys_user", {"x": 1})
    with pytest.raises(ValueError):
        writer.insert("biz_leave; DROP TABLE biz_leave", {"x": 1})
    with pytest.raises(ValueError):
        writer.filter_columns("sys_user", ["x"])
    conn.close()


# ─── ⑨ writer 幂等 exists ──────────────────────────────────────────────────────

def test_writer_exists_idempotent():
    conn, writer = setup_db()
    writer.insert("biz_leave", {"title": "t", "process_instance_id": 99})
    assert writer.exists("biz_leave", "process_instance_id", 99) is True
    assert writer.exists("biz_leave", "process_instance_id", 100) is False
    conn.close()


# ─── ⑩ BIGINT 用户列（issues/19）：create_user 为 BIGINT 存 userId ────────────

@pytest.mark.asyncio
async def test_bigint_user_column():
    conn = sqlite3.connect(":memory:")
    conn.execute("""CREATE TABLE biz_settle (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        process_instance_id INTEGER,
        apply_user_id INTEGER,
        create_user INTEGER,
        update_user INTEGER,
        is_deleted INTEGER
    )""")
    writer = JdbcDynamicTableWriter(conn)
    repo = MemoryRepository()
    eng = setup_engine(repo, writer)
    with open(os.path.join(FLOW_DIR, "01-simple.json"), "r", encoding="utf-8") as f:
        content = f.read().replace('"type": "approval"',
                                   '"type": "approval", "relTableName": "biz_settle"', 1)
    df = ProcessDefine(name="simple", type="approval", state=1, version=1, content=content)
    repo.add_define(df)

    inst = await eng.start_process_instance_by_id(df.id, "123",
        {"f_title": "结算单", "u_deptId": "D01"})
    doing = await repo.find_doing_tasks(inst.id)
    await repo.add_task_actor(doing[0].id, ["123"])
    await eng.execute_process_task(doing[0].id, "123", {KEY_SUBMIT_TYPE: 0})
    doing = await repo.find_doing_tasks(inst.id)
    await repo.add_task_actor(doing[0].id, ["leader"])
    await eng.execute_process_task(doing[0].id, "leader", {KEY_SUBMIT_TYPE: 1})

    row = conn.execute("SELECT create_user, apply_user_id FROM biz_settle").fetchone()
    assert row == (123, 123), f"BIGINT 用户列应为 operator: {row}"
    conn.close()


# ─── ⑪ writer 用户列默认值：优先 apply_user_id，否则配置值回落 ────────────────

def test_writer_default_user_value():
    conn, writer = setup_db()
    data = {"title": "t", "apply_user_id": "abc"}
    writer.fill_system_fields(data, True)
    assert data["create_user"] == "abc"
    # 无 apply_user_id → 回落配置默认值
    writer.default_user_value = 0
    data2 = {"title": "t"}
    writer.fill_system_fields(data2, True)
    assert data2["create_user"] == 0
    conn.close()


# ─── ⑫ 宽松列匹配（issues/20）：驼峰表单字段 ↔ 下划线表列 ─────────────────────

def test_loose_camel_match():
    conn, writer = setup_db()
    conn.execute("ALTER TABLE biz_leave ADD COLUMN start_time TEXT")
    writer.insert("biz_leave", {"startTime": "09:00:00", "processInstanceId": 55,
                                "title": "camel"})
    row = conn.execute("SELECT start_time, process_instance_id FROM biz_leave").fetchone()
    assert row == ("09:00:00", 55), f"驼峰 key 应落到下划线列: {row}"
    kept = writer.filter_columns("biz_leave", ["startTime", "processInstanceId", "no_such"])
    assert set(kept) == {"startTime", "processInstanceId"}
    conn.close()


# ─── ⑬ 严格列匹配（issues/20）：显式开启后驼峰不再匹配 ────────────────────────

def test_strict_column_match():
    conn, writer = setup_db()
    conn.execute("ALTER TABLE biz_leave ADD COLUMN start_time TEXT")
    writer.strict_column_match = True
    writer.insert("biz_leave", {"startTime": "09:00:00", "title": "strict"})
    row = conn.execute("SELECT title, start_time FROM biz_leave").fetchone()
    assert row == ("strict", None), f"严格模式应过滤驼峰 key: {row}"
    conn.close()
