"""persist 动态表写入 + 流程入库拦截器测试（issues/18，SQLite 内存库全链路）"""
import os
import re
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository, EngineExtensions
from jeeflow.memory import MemoryExtRepository
from jeeflow.metadata import HandlerRegistry
from jeeflow.model import ProcessDefine, InstanceState, UserInfo
from jeeflow.persist import JdbcDynamicTableWriter, PersistPostInterceptor, register_persist_meta
from jeeflow.spi import UserProvider, IDGenerator, ExpressionEvaluator
from jeeflow.engine import KEY_SUBMIT_TYPE

import flows_resolver
FLOW_DIR = flows_resolver.dir()

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


# ─── ⑭ 非自增主键生成（issues/21）：TEXT 主键（雪花/应用生成）配生成器后插入成功 ──

def test_primary_key_generator():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE biz_snow (id TEXT PRIMARY KEY, title TEXT)")
    writer = JdbcDynamicTableWriter(conn)
    writer.primary_key_generator = lambda t: "snow-888"
    writer.insert("biz_snow", {"title": "snow"})
    row = conn.execute("SELECT id, title FROM biz_snow").fetchone()
    assert row == ("snow-888", "snow"), f"主键应由生成器生成: {row}"
    # data 已含主键值 → 用之，不调生成器
    writer.insert("biz_snow", {"id": "manual-1", "title": "m"})
    n = conn.execute("SELECT COUNT(1) FROM biz_snow WHERE id='manual-1'").fetchone()[0]
    assert n == 1
    conn.close()


# ─── ⑮ 非自增主键未配生成器（issues/21）：清晰报错 ─────────────────────────────

def test_missing_primary_key_generator():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE biz_snow (id TEXT PRIMARY KEY, title TEXT)")
    writer = JdbcDynamicTableWriter(conn)  # 未配置生成器
    with pytest.raises(ValueError, match="primary key generator"):
        writer.insert("biz_snow", {"title": "x"})
    conn.close()


# ─── ⑯ SYNC 同步演进（1.8.0）：发起入库 → 任务推进 → 结束定稿 ─────────────────

def _sync_define(repo: MemoryRepository, table: str) -> ProcessDefine:
    with open(os.path.join(FLOW_DIR, "01-simple.json"), "r", encoding="utf-8") as f:
        content = f.read()
    # issues/62：共享 flow 自带 field（PERMISSION_*）→ 先移除，避免与下方注入重复键（json.loads 后者覆盖前者）
    content = re.sub(r',\s*"field":\s*\{[^}]*\}', '', content, count=1)
    content = content.replace('"type": "approval"',
                              f'"type": "approval", "relTableName": "{table}", "persistMode": "SYNC"', 1)
    content = content.replace('"assignee": "leader"',
                              '"assignee": "leader", "field": {"PERMISSION_f_title": 1, "PERMISSION_amount": 2}', 1)
    content = content.replace('"id": "end"', '"id": "finish"')
    content = content.replace('"targetNodeId": "end"', '"targetNodeId": "finish"')
    d = ProcessDefine(name="simple", displayName="01-simple.json", type="approval",
                      state=1, version=1, content=content)
    repo.add_define(d)
    return d


def _sync_db():
    conn = sqlite3.connect(":memory:")
    conn.execute("""CREATE TABLE biz_sync (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        amount REAL,
        opinion TEXT,
        apply INTEGER,
        task1 INTEGER,
        finish INTEGER,
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


@pytest.mark.asyncio
async def test_sync_mode_full_cycle():
    repo = MemoryRepository()
    conn, writer = _sync_db()
    eng = EngineImpl(repo, _TestUserProv(), _TestIDGen(), _TestExprEval())
    eng.set_extensions(EngineExtensions(interceptors=[
        PersistPostInterceptor(writer=writer, loader=repo.find_define_by_id)]))
    _sync_define(repo, "biz_sync")

    # ① 发起 → INSERT（title/amount）
    inst = await eng.start_process_instance_by_id(1, "user1",
        {"f_title": "年假申请", "f_amount": 800.0, "u_deptId": "D01"})
    # ② apply 完成 → UPDATE（apply 状态=10）
    doing = await repo.find_doing_tasks(inst.id)
    await repo.add_task_actor(doing[0].id, ["user1"])
    await eng.execute_process_task(doing[0].id, "user1", {KEY_SUBMIT_TYPE: 0})
    assert conn.execute("SELECT apply FROM biz_sync").fetchone()[0] == 10
    # ③ task1（leader）→ UPDATE：title 只读不更新 / amount 可编辑更新 / opinion(tf_) / task1=10 / finish=20
    doing = await repo.find_doing_tasks(inst.id)
    await repo.add_task_actor(doing[0].id, ["leader"])
    await eng.execute_process_task(doing[0].id, "leader",
        {KEY_SUBMIT_TYPE: 1, "tf_opinion": "同意", "f_title": "修改标题", "f_amount": 999.0})
    title, amount, opinion, task1, finish = conn.execute(
        "SELECT title, amount, opinion, task1, finish FROM biz_sync").fetchone()
    assert title == "年假申请", f"只读字段不应更新: {title}"
    assert amount == 999.0, f"可编辑字段应更新: {amount}"
    assert opinion == "同意", f"tf_ 冗余未落库: {opinion}"
    assert task1 == 10
    assert finish == 20
    assert conn.execute("SELECT COUNT(1) FROM biz_sync").fetchone()[0] == 1, "先插后更应仅 1 条"
    conn.close()


@pytest.mark.asyncio
async def test_sync_mode_reject():
    repo = MemoryRepository()
    conn, writer = _sync_db()
    eng = EngineImpl(repo, _TestUserProv(), _TestIDGen(), _TestExprEval())
    eng.set_extensions(EngineExtensions(interceptors=[
        PersistPostInterceptor(writer=writer, loader=repo.find_define_by_id)]))
    _sync_define(repo, "biz_sync")

    inst = await eng.start_process_instance_by_id(1, "user1",
        {"f_title": "驳回单", "u_deptId": "D01"})
    doing = await repo.find_doing_tasks(inst.id)
    await repo.add_task_actor(doing[0].id, ["user1"])
    await eng.execute_process_task(doing[0].id, "user1", {KEY_SUBMIT_TYPE: 0})
    doing = await repo.find_doing_tasks(inst.id)
    await repo.add_task_actor(doing[0].id, ["leader"])
    await eng.execute_process_task(doing[0].id, "leader", {KEY_SUBMIT_TYPE: 2})

    title, finish, create_user = conn.execute(
        "SELECT title, finish, create_user FROM biz_sync").fetchone()
    assert title == "驳回单"
    assert finish == 45, f"驳回最终状态应为 REJECT: {finish}"
    assert create_user == "user1"
    conn.close()


# ─── ⑰ issues/26：办理提交被拒字段（只读/隐藏）不入变量——下游无权限节点无法绕过 ──

PERM3_FLOW = """{
  "name": "perm3", "displayName": "权限绕过验证", "type": "approval",
  "relTableName": "biz_perm3", "persistMode": "SYNC",
  "nodes": [
    {"id": "start", "type": "snaker:start", "properties": {}, "text": {"value": "开始"}},
    {"id": "apply", "type": "snaker:task", "properties": {"assignee": "applicant", "taskType": 0, "performType": 0}, "text": {"value": "发起申请"}},
    {"id": "approve1", "type": "snaker:task", "properties": {"assignee": "leader1", "taskType": 0, "performType": 0, "field": {"PERMISSION_f_title": 1, "PERMISSION_amount": 2}}, "text": {"value": "审批一"}},
    {"id": "approve2", "type": "snaker:task", "properties": {"assignee": "leader2", "taskType": 0, "performType": 0}, "text": {"value": "审批二"}},
    {"id": "finish", "type": "snaker:end", "properties": {}, "text": {"value": "结束"}}
  ],
  "edges": [
    {"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
    {"id": "e1", "sourceNodeId": "apply", "targetNodeId": "approve1", "properties": {}},
    {"id": "e2", "sourceNodeId": "approve1", "targetNodeId": "approve2", "properties": {}},
    {"id": "e3", "sourceNodeId": "approve2", "targetNodeId": "finish", "properties": {}}
  ]
}"""


@pytest.mark.asyncio
async def test_sync_perm_bypass():
    repo = MemoryRepository()
    conn = sqlite3.connect(":memory:")
    conn.execute("""CREATE TABLE biz_perm3 (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT, amount REAL,
        apply INTEGER, approve1 INTEGER, approve2 INTEGER, finish INTEGER,
        process_instance_id INTEGER,
        create_user TEXT, is_deleted INTEGER
    )""")
    writer = JdbcDynamicTableWriter(conn)
    eng = EngineImpl(repo, _TestUserProv(), _TestIDGen(), _TestExprEval())
    eng.set_extensions(EngineExtensions(interceptors=[
        PersistPostInterceptor(writer=writer, loader=repo.find_define_by_id)]))
    repo.add_define(ProcessDefine(name="perm3", displayName="perm3", type="approval",
                                  state=1, version=1, content=PERM3_FLOW))

    inst = await eng.start_process_instance_by_id(1, "user1",
        {"f_title": "原始标题", "f_amount": 800.0, "u_deptId": "D01"})

    async def complete_named(name, actor, args):
        doing = await repo.find_doing_tasks(inst.id)
        for d in doing:
            if d.taskName == name:
                await repo.add_task_actor(d.id, [actor])
                await eng.execute_process_task(d.id, actor, args)
                return
        raise AssertionError(f"task {name} not found")

    await complete_named("apply", "user1", {KEY_SUBMIT_TYPE: 0})
    # approve1 只读 title，提交 TRY_HACK → 引擎入口过滤 → 不入变量 → 不落库
    await complete_named("approve1", "leader1", {KEY_SUBMIT_TYPE: 1, "f_title": "TRY_HACK"})
    # approve2 无权限声明——变量无 TRY_HACK，title 保持原值
    await complete_named("approve2", "leader2", {KEY_SUBMIT_TYPE: 1, "f_amount": 999.0})

    title, amount, approve1, approve2, finish = conn.execute(
        "SELECT title, amount, approve1, approve2, finish FROM biz_perm3").fetchone()
    assert title == "原始标题", f"只读字段被拒值不应落库（下游不可绕过）: {title}"
    assert amount == 999.0
    assert (approve1, approve2, finish) == (10, 10, 20)
    conn.close()


# ─── ⑱ issues/34：定义级拦截器（postInterceptors 声明 + 注册表按名解析） ─────────

@pytest.mark.asyncio
async def test_define_level_interceptor():
    repo = MemoryRepository()
    conn = sqlite3.connect(":memory:")
    conn.execute("""CREATE TABLE biz_decl (
        id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT,
        process_instance_id INTEGER, is_deleted INTEGER
    )""")
    writer = JdbcDynamicTableWriter(conn)
    ic = PersistPostInterceptor(writer=writer, loader=repo.find_define_by_id)
    eng = EngineImpl(repo, _TestUserProv(), _TestIDGen(), _TestExprEval())
    # 注册表挂载（定义级）：名字 "persist"
    eng.set_extensions(EngineExtensions(interceptors=[], interceptor_registry={"persist": ic}))

    def load_flow(name: str, table: str, declared: str) -> ProcessDefine:
        content = f'''{{"name": "{name}", "displayName": "{name}", "type": "approval",
          "relTableName": "{table}", "persistMode": "SYNC",
          "postInterceptors": "{declared}",
          "nodes": [{{"id": "start", "type": "snaker:start", "properties": {{}}, "text": {{"value": "开始"}}}},
                    {{"id": "finish", "type": "snaker:end", "properties": {{}}, "text": {{"value": "结束"}}}}],
          "edges": [{{"id": "e0", "sourceNodeId": "start", "targetNodeId": "finish", "properties": {{}}}}]}}'''
        return ProcessDefine(name=name, displayName=name, type="approval", state=1, version=1, content=content)

    # 声明了 persist → 发起即入库
    d1 = load_flow("decl1", "biz_decl", "persist")
    repo.add_define(d1)
    inst = await eng.start_process_instance_by_id(d1.id, "user1", {"f_title": "声明流程"})
    assert conn.execute("SELECT COUNT(1) FROM biz_decl").fetchone()[0] == 1
    # 未声明 → 不触发（定义级语义：只有声明了拦截器的流程才持久化）
    d2 = load_flow("decl2", "biz_decl", "")
    repo.add_define(d2)
    await eng.start_process_instance_by_id(d2.id, "user2", {"f_title": "未声明流程"})
    assert conn.execute("SELECT COUNT(1) FROM biz_decl").fetchone()[0] == 1, "未声明拦截器的流程不应落库"
    conn.close()


# ─── ⑱.1 issues/60：定义级声明未注册 → 显式报错（不静默跳过） ───────────────────

@pytest.mark.asyncio
async def test_declared_interceptor_missing_registry():
    repo = MemoryRepository()
    eng = EngineImpl(repo, _TestUserProv(), _TestIDGen(), _TestExprEval())
    eng.set_extensions(EngineExtensions(interceptors=[], interceptor_registry={}))
    content = '''{"name": "ghost", "displayName": "幽灵拦截器", "type": "approval",
      "relTableName": "biz_ghost", "persistMode": "SYNC", "postInterceptors": "com.xxx.GhostInterceptor",
      "nodes": [{"id": "start", "type": "snaker:start", "properties": {}, "text": {"value": "开始"}},
                {"id": "finish", "type": "snaker:end", "properties": {}, "text": {"value": "结束"}}],
      "edges": [{"id": "e0", "sourceNodeId": "start", "targetNodeId": "finish", "properties": {}}]}'''
    d = ProcessDefine(name="ghost", displayName="幽灵拦截器", type="approval", state=1, version=1, content=content)
    repo.add_define(d)
    with pytest.raises(ValueError, match="未注册"):
        await eng.start_process_instance_by_id(d.id, "user1", {"f_title": "幽灵流程"})


# ─── ⑱.2 issues/60：注册助手 register_persist_meta（字典 1 项 / 全名 / 显示名 / post 组） ──

def test_register_persist_meta():
    reg = HandlerRegistry()
    register_persist_meta(reg)
    metas = reg.list_handlers("FlowInterceptor")
    assert len(metas) == 1, f"注册后 FlowInterceptor 应为 1 项: {len(metas)}"
    m = metas[0]
    assert m.className == "com.mldong.jeeflow.persist.interceptor.PersistPostInterceptor"
    assert m.displayName == "业务数据自动入库"
    assert m.group == "post"
    assert m.order == 0
    # 二次注册追加（同名覆盖语义，与 Java 一致）
    register_persist_meta(reg)
    assert len(reg.list_handlers("FlowInterceptor")) == 2


# ─── ⑲ issues/30/31：facade listByType/bizData/顶层 JSON ───────────────────────

@pytest.mark.asyncio
async def test_facade_listByType_and_topLevelJson():
    from jeeflow import JeeflowFacade
    repo = MemoryRepository()
    from jeeflow.spi import ProcessExtRepository

    class _Ext(ProcessExtRepository):
        def __init__(self):
            self.designs = {}
            self.his = {}
            self.seq = 1
        # 委托代理方法（接口要求，本测试不用）
        async def find_surrogate_by_id(self, surrogate_id): return None
        async def save_surrogate(self, s): pass
        async def update_surrogate(self, s): pass
        async def remove_surrogate(self, surrogate_id): pass
        async def page_surrogates(self, page_num, page_size, conditions=None): return [], 0
        async def get_surrogate(self, operator, process_name, now): return None
        async def find_design_by_id(self, design_id):
            return self.designs.get(design_id)
        async def save_design(self, d):
            if not d.id: d.id = self.seq; self.seq += 1
            self.designs[d.id] = d
        async def update_design(self, d): self.designs[d.id] = d
        async def remove_design(self, design_id):
            self.designs.pop(design_id, None); self.his.pop(design_id, None)
        async def page_designs(self, page_num, page_size, conditions=None):
            rows = list(self.designs.values())
            return rows, len(rows)
        async def save_design_his(self, his):
            self.his.setdefault(his.processDesignId, []).insert(0, his)
        async def list_design_his(self, design_id):
            return self.his.get(design_id, [])

    ext = _Ext()
    eng = EngineImpl(repo, _TestUserProv(), _TestIDGen(), _TestExprEval())
    facade = JeeflowFacade(eng, repo, ext)
    from jeeflow.model import ProcessDesign
    await ext.save_design(ProcessDesign(id=1, name="old", displayName="旧名", type="approval"))
    await ext.save_design(ProcessDesign(id=2, name="old2", displayName="旧名2", type="approval"))

    # 顶层 JSON 保存（无 content 字段）——issue 31
    r = await facade.flow("processDesign/updateDefine", {
        "processDesignId": 1, "operator": "user1",
        "name": "topjson", "displayName": "顶层JSON", "type": "approval",
        "relTableName": "biz_top", "nodes": [], "edges": []})
    assert r["code"] == 0, r
    d = await ext.find_design_by_id(1)
    assert d.name == "topjson"
    his = await ext.list_design_his(1)
    assert his and '"nodes"' in his[0].content, "顶层 JSON 应序列化为内容快照"

    # listByType 分组——issue 30
    r = await facade.flow("processDesign/listByType", {})
    assert r["code"] == 0, r
    groups = r["data"]
    assert "approval" in groups and groups["approval"][0]["name"] == "topjson"
    assert groups["approval"][0]["processDesignId"] == "1"  # 出口 id string（issue 38 E9）
    assert groups["approval"][0]["jsonObject"] is not None

    # 真实流程（01-simple + relTableName 注入）供 bizData 测试
    simple = open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8").read()
    simple = simple.replace('"type": "approval"', '"type": "approval", "relTableName": "biz_top"', 1)
    r = await facade.flow("processDesign/updateDefine", {
        "processDesignId": 2, "operator": "user1", "content": simple})
    assert r["code"] == 0, r
    r = await facade.flow("processDesign/deploy", {"id": 2, "operator": "user1"})
    assert r["code"] == 0, r
    define_id = r["data"]["processDefineId"]
    sr = await facade.flow("processInstance/startAndExecute",
                           {"processDefineId": define_id, "operator": "user1", "f_title": "x"})
    assert sr["code"] == 0, sr
    inst_id = sr["data"]["processInstanceId"]
    r = await facade.flow("processInstance/bizData", {"processInstanceId": inst_id})
    assert r["code"] != 0 and "meta_reader" in r["msg"], r

    class _Reader:
        def read_by_process_instance(self, table_name, process_instance_id):
            return {"tableName": table_name, "title": "业务数据"}
    facade.set_meta_reader(_Reader())
    r = await facade.flow("processInstance/bizData", {"processInstanceId": inst_id})
    assert r["code"] == 0, r
    assert r["data"]["tableName"] == "biz_top"
    assert r["data"]["title"] == "业务数据"

    # ── issues/81：listByType items 必含 processDefineState（前端发起按钮硬依赖）──
    # 场景 A：deploy 后定义 state=1 → 可发起
    r = await facade.flow("processDesign/listByType", {})
    assert r["code"] == 0, r
    item_a = next(x for x in r["data"]["approval"] if x["name"] == "simple")
    assert str(item_a["processDefineId"]) == str(define_id), item_a
    assert item_a["processDefineState"] == 1, f"启用定义 processDefineState 应为 1: {item_a}"
    # 场景 B：upAndDown 禁用 → 定义 state=0 → 前端置灰
    r = await facade.flow("processDefine/upAndDown", {"id": define_id, "opType": 0})
    assert r["code"] == 0, r
    r = await facade.flow("processDesign/listByType", {})
    assert r["code"] == 0, r
    item_b = next(x for x in r["data"]["approval"] if x["name"] == "simple")
    assert item_b["processDefineState"] == 0, f"禁用定义 processDefineState 应为 0: {item_b}"
