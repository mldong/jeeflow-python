"""audit8 R9 145-3（spec/12 §12.4 三档行为判据）· Python 栈真库档。

与 tests/jdbc_test.py 同一连接口径（默认 160 MySQL jeeflow 库，JEFFLOW_DB_* 可覆盖）：

    python tests/tx_tier_145_test.py mysql

（Windows 控制台先 `set PYTHONIOENCODING=utf-8`。）

覆盖六格：
 1. 空转档——事务内写：同事务连接读得到 / 独立连接提交前读不到 / 回滚后不存在；
    另含 join 支格：嵌套 with_tx 并入最外层（内层不提交，外层回滚连坐）
 2. 原子档——引擎入口（ExecuteProcessTask 办理腿；真实发起作前置）：抄送漏斗注入失败
    ⇒ 任务行回到 DOING＋实例仍 DOING＋cc 零残留（动作内写整体回滚）
 3. 动作档——门面复合动作（startAndExecute，入参带 f_ccActors＋tf_ccActors 两腿）：
    第二拍抄送漏斗注入失败 ⇒ 整次 dispatch 回滚（两拍的实例/任务行一并消失）；
    ⚠ 裸拼对照：**与 ③-a 同参**（fail_cc=2），仅不经门面、两次引擎入口各一拍 ⇒
    第一拍已提交、第二拍失败 ⇒ 半完成留存 = "门面不包"的修前形态（红样形态本身，不是缺陷）
 4. 归属档——嵌套并入（外层回滚连坐引擎入口写）＋并发切开（A 回滚 B 留存，互不串；
    contextvars 按 task 隔离的实测格）
 5. ext 腿——主仓+扩展仓同 ctx 同事务：回滚双消失 / 提交双落库
 6. persist 腿（C99 ⑥）——SYNC 流 + 真 PersistPostInterceptor：writer 接
    tx_conn_provider=current_tx_conn ⇒ 业务行随动作回滚（并入，注入点＝实例终结写，
    位于本拍 persist 已写业务行之后）；不接 ⇒ 行逃逸留存（§12.1 条 4 允许降级运行态＝
    集成方必须显式接线的理由）

注入点＝FlakyRepo 两处（都位于"真中途"且错误真传播）：
  - 抄送漏斗 create_cc_instance_if_absent（②③）：任务完成写之后、事件/persist 之前；
  - 实例终结写 update_instance（⑥）：办理拍的 persist 已写业务行之后（end 节点 finish 写）。

阳性对照（2026-10-11 当轮逐处摘除，跑完即字节级还原、四文件 cmp 与副本全等，复跑 38/0）：
  M1 摘 with_tx join 支 ⇒ 7 格红：①(join 连坐 n=1)＋③-a 整组(半完成留存)＋③-b(n=2)＋④(嵌套 n=1)；
  M2 摘引擎 _in_tx 包裹 ⇒ 1 格红：②"(20)"＝任务完成写留存、动作报错但未回滚；
  M3 摘门面 flow 包裹 ⇒ 5 格红：③-a 整组(第一拍留存 1＝半完成)＋③-b 连坐(n=2)；
  M4 掐 writer tx 命中(_tx_conn 恒 None) ⇒ 6 格红：①×2＋①(join)×2＋④(A 回滚行在)＋⑥-a(业务行逃逸 n=1)。
"""
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import aiomysql
import pymysql

from jeeflow import EngineImpl
from jeeflow.repository import JdbcRepository, convert_placeholder, MySqlAdapter
from jeeflow.repository.base import current_tx_conn
from jeeflow.repository.ext import JdbcProcessExtRepository
from jeeflow.extensions import EngineExtensions
from jeeflow.facade import JeeflowFacade
from jeeflow.persist import JdbcDynamicTableWriter, PersistPostInterceptor
from jeeflow.model import (InstanceState, TaskState, SubmitType, UserInfo,
                           ProcessSurrogate, ProcessDefine, ProcessInstance)

DB = sys.argv[1] if len(sys.argv) > 1 else "mysql"

_DB_HOST = os.environ.get("JEFFLOW_DB_HOST", "192.168.1.160")
_DB_PORT = int(os.environ.get("JEFFLOW_DB_PORT", "3306"))
_DB_USER = os.environ.get("JEFFLOW_DB_USER", "root")
_DB_PWD = os.environ.get("JEFFLOW_DB_PWD", "8Eli#gr#AUk")
DSN = dict(host=_DB_HOST, port=_DB_PORT, user=_DB_USER, password=_DB_PWD,
           db="jeeflow", charset="utf8mb4", autocommit=True, maxsize=5)

DEFINE_ID = 900145       # 简单流（flows/01-simple.json）
DEFINE_ID_SYNC = 900146  # SYNC 入库流（内联，relTableName=biz_tx_probe）

passed = 0
failed = 0


def check(desc, ok, detail=""):
    global passed, failed
    tag = "PASS" if ok else "FAIL"
    msg = f"  [{tag}] {desc}"
    if detail:
        msg += f" ({detail})"
    print(msg)
    if ok:
        passed += 1
    else:
        failed += 1
    return ok


def sql_of(adapter, sql):
    return convert_placeholder(sql, adapter.placeholder)


async def raw_count(adapter, sql, args=()):
    conn = await adapter.acquire()
    try:
        row = await conn.fetchone(sql_of(adapter, sql), args)
        return row[0]
    finally:
        await adapter.release(conn)


class TestIDGen:
    """时间戳 + 序号（避免与库内既有 ID 冲突；_STACK_SEQ=1 与 jdbc_test 同口径）。"""

    def __init__(self):
        import time
        self.base = int(time.time() * 1000) * 4000 + 1 * 1000
        self.n = 0

    def next_id(self):
        self.n += 1
        return self.base + self.n


class TestUserProv:
    async def get_user(self, uid):
        return UserInfo(userId=uid, realName=uid, deptId="D01", deptName="部门",
                        postId="P01", postName="岗位")


class FlakyRepo(JdbcRepository):
    """注入失败（策略：第 N 次 cc 调用报错 / 实例终结写报错）——判据是"失败了就必须整体不存在"。

    嵌入真仓储：其余方法透传；with_tx/_conn 随继承（join 支照常）。
    """

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.fail_cc = 0        # 第 N 次 create_cc_instance_if_absent 注入失败（0=不注）
        self.cc_calls = 0
        self.fail_finish = False  # 实例终结写（state != DOING）注入失败
        self.fin_hits = 0

    def reset_inject(self, fail_cc=0, fail_finish=False):
        self.fail_cc = fail_cc
        self.cc_calls = 0
        self.fail_finish = fail_finish
        self.fin_hits = 0

    async def create_cc_instance_if_absent(self, instance_id, creator, actor_ids):
        self.cc_calls += 1
        if self.fail_cc and self.cc_calls == self.fail_cc:
            raise RuntimeError("injected: cc write failed mid-action")
        return await super().create_cc_instance_if_absent(instance_id, creator, actor_ids)

    async def update_instance(self, inst):
        if self.fail_finish and int(inst.state) != int(InstanceState.DOING):
            self.fin_hits += 1
            raise RuntimeError("injected: instance finalize failed mid-action")
        return await super().update_instance(inst)


# SYNC 入库流：start → apply(发起人) → end，业务表 biz_tx_probe（f_tag → tag 列）
SYNC_FLOW = {
    "name": "py-tier-sync",
    "displayName": "R9 事务档 SYNC 流",
    "type": "approval",
    "persistMode": "SYNC",
    "relTableName": "biz_tx_probe",
    "nodes": [
        {"id": "start", "type": "snaker:start", "x": 100, "y": 200,
         "properties": {"width": 50, "height": 50}, "text": {"value": "开始"}},
        {"id": "apply", "type": "snaker:task", "x": 200, "y": 200,
         "properties": {"width": 100, "height": 50, "assignee": "applicant",
                        "taskType": 0, "performType": 0},
         "text": {"value": "发起申请"}},
        {"id": "end", "type": "snaker:end", "x": 500, "y": 200,
         "properties": {"width": 50, "height": 50}, "text": {"value": "结束"}},
    ],
    "edges": [
        {"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
        {"id": "e2", "sourceNodeId": "apply", "targetNodeId": "end", "properties": {}},
    ],
}


async def cleanup(adapter):
    """清运行态数据（四表 + 本档专用行）；**不动 define 行**——各段之间复用同一对定义。"""
    conn = await adapter.acquire()
    try:
        for did in (DEFINE_ID, DEFINE_ID_SYNC):
            await conn.execute(sql_of(adapter,
                "DELETE FROM wf_process_task_actor WHERE process_task_id IN"
                " (SELECT id FROM wf_process_task WHERE process_instance_id IN"
                " (SELECT id FROM wf_process_instance WHERE process_define_id = ?))"), [did])
            await conn.execute(sql_of(adapter,
                "DELETE FROM wf_process_cc_instance WHERE process_instance_id IN"
                " (SELECT id FROM wf_process_instance WHERE process_define_id = ?)"), [did])
            await conn.execute(sql_of(adapter,
                "DELETE FROM wf_process_task WHERE process_instance_id IN"
                " (SELECT id FROM wf_process_instance WHERE process_define_id = ?)"), [did])
            await conn.execute(sql_of(adapter,
                "DELETE FROM wf_process_instance WHERE process_define_id = ?"), [did])
        await conn.execute(sql_of(adapter,
            "DELETE FROM biz_tx_probe WHERE tag LIKE 'pytier-%%'"), [])
        await conn.execute(sql_of(adapter,
            "DELETE FROM wf_process_cc_instance WHERE actor_id LIKE 'pytier-%%'"), [])
        await conn.execute(sql_of(adapter,
            "DELETE FROM wf_process_surrogate WHERE operator = 'pytier145'"), [])
    finally:
        await adapter.release(conn)


async def purge_defines(adapter):
    """删两行测试定义（进也清、出也清：保证重跑可入）。"""
    conn = await adapter.acquire()
    try:
        for did in (DEFINE_ID, DEFINE_ID_SYNC):
            await conn.execute(sql_of(adapter,
                "DELETE FROM wf_process_define WHERE id = ?"), [did])
    finally:
        await adapter.release(conn)


async def insert_define(adapter, did, name, content):
    from datetime import datetime
    conn = await adapter.acquire()
    try:
        now = datetime.now()
        await conn.execute(sql_of(adapter,
            "INSERT INTO wf_process_define (id, name, display_name, type, state, content,"
            " version, create_time, create_user, update_time, update_user)"
            " VALUES (?,?,?,?,1,?,1,?,?,?,?)"),
            [did, name, json.loads(content)["displayName"], json.loads(content)["type"],
             content, now, "py-tier", now, "py-tier"])
    finally:
        await adapter.release(conn)


async def assert_no_action_residue(adapter, why):
    """动作失败后：实例/任务/参与者/cc 四表必须全 0（独立连接反查）。"""
    checks = [
        ("实例", "SELECT COUNT(1) FROM wf_process_instance WHERE process_define_id = ?"),
        ("任务", "SELECT COUNT(1) FROM wf_process_task WHERE process_instance_id IN"
                 " (SELECT id FROM wf_process_instance WHERE process_define_id = ?)"),
        ("参与者", "SELECT COUNT(1) FROM wf_process_task_actor WHERE process_task_id IN"
                   " (SELECT id FROM wf_process_task WHERE process_instance_id IN"
                   " (SELECT id FROM wf_process_instance WHERE process_define_id = ?))"),
        ("cc行", "SELECT COUNT(1) FROM wf_process_cc_instance WHERE process_instance_id IN"
                 " (SELECT id FROM wf_process_instance WHERE process_define_id = ?)"),
    ]
    for name, q in checks:
        n = await raw_count(adapter, q, [DEFINE_ID])
        check(f"{why}：{name} 行必须随动作整体回滚", n == 0, f"n={n}")


async def main():
    pool = await aiomysql.create_pool(**DSN)
    adapter = MySqlAdapter(pool)
    sync_conn = pymysql.connect(host=_DB_HOST, port=_DB_PORT, user=_DB_USER,
                                password=_DB_PWD, database="jeeflow", charset="utf8mb4")
    pk_gen = TestIDGen()

    def next_pk():
        return pk_gen.next_id()

    try:
        # ── 建业务表（幂等）＋清场 ──
        conn = await adapter.acquire()
        try:
            await conn.execute(sql_of(adapter,
                "CREATE TABLE IF NOT EXISTS biz_tx_probe (id bigint NOT NULL,"
                " tag varchar(64), process_instance_id bigint, PRIMARY KEY(id))"), [])
        finally:
            await adapter.release(conn)
        await cleanup(adapter)
        await purge_defines(adapter)

        # ── 定义（简单流 + SYNC 流）──
        with open(os.path.join(os.path.dirname(__file__), "..", "flows", "01-simple.json"),
                  encoding="utf-8") as f:
            simple = f.read()
        await insert_define(adapter, DEFINE_ID, "py-tier-simple", simple)
        await insert_define(adapter, DEFINE_ID_SYNC, SYNC_FLOW["name"], json.dumps(SYNC_FLOW))

        repo = FlakyRepo(adapter, TestIDGen())
        ext = JdbcProcessExtRepository(adapter, TestIDGen())

        writer = JdbcDynamicTableWriter(sync_conn, dialect="mysql")
        writer.primary_key_generator = lambda table: next_pk()
        # 集成侧标准接线（C99 ⑥）：persist 写腿并入动作事务
        writer.tx_conn_provider = current_tx_conn

        # ─── ① 空转档 ─────────────────────────────────────────────────────────
        tag = "pytier-idle"
        async def work_idle():
            await writer.a_insert("biz_tx_probe", {"id": next_pk(), "tag": tag})
            tx = current_tx_conn()
            row = await tx.fetchone(sql_of(adapter, "SELECT COUNT(1) FROM biz_tx_probe WHERE tag = ?"),
                                    [tag])
            check("空转档：同事务连接提交前必须读得到（writer 并入腿）", row[0] == 1, f"n={row[0]}")
            n = await raw_count(adapter, "SELECT COUNT(1) FROM biz_tx_probe WHERE tag = ?", [tag])
            check("空转档：独立连接提交前必须读不到", n == 0, f"n={n}")
            raise RuntimeError("rollback on purpose")
        try:
            await repo.with_tx(work_idle)
            check("空转档：回调报错必须外抛", False)
        except RuntimeError:
            check("空转档：回调报错必须外抛", True)
        n = await raw_count(adapter, "SELECT COUNT(1) FROM biz_tx_probe WHERE tag = ?", [tag])
        check("空转档：回滚后行必须不存在", n == 0, f"n={n}")

        # ①-b join 支：嵌套 with_tx 并入最外层（内层不提交；外层回滚连坐）
        tag_join = "pytier-join"
        async def work_join():
            async def inner():
                await writer.a_insert("biz_tx_probe", {"id": next_pk(), "tag": tag_join})
                n2 = await raw_count(adapter, "SELECT COUNT(1) FROM biz_tx_probe WHERE tag = ?",
                                     [tag_join])
                check("空转档(join)：内层返回时独立连接仍读不到（并入最外层，未提前提交）",
                      n2 == 0, f"n={n2}")
                return "inner-ok"
            r = await repo.with_tx(inner)
            check("空转档(join)：嵌套 with_tx 正常返回", r == "inner-ok", str(r))
            raise RuntimeError("outer rollback")
        try:
            await repo.with_tx(work_join)
            check("空转档(join)：外层报错必须外抛", False)
        except RuntimeError:
            check("空转档(join)：外层报错必须外抛", True)
        n = await raw_count(adapter, "SELECT COUNT(1) FROM biz_tx_probe WHERE tag = ?", [tag_join])
        check("空转档(join)：外层回滚必须连坐嵌套写", n == 0, f"n={n}")

        # ─── ② 原子档（引擎入口）──────────────────────────────────────────────
        await cleanup(adapter)
        repo.reset_inject(fail_cc=1)
        eng = EngineImpl(repo, TestUserProv(), TestIDGen())
        inst = await eng.start_process_instance_by_id(
            DEFINE_ID, "zhangsan", {"BUSINESS_NO": "PYTIER-ATOM"})
        doing = await repo.find_doing_tasks(inst.id)
        check("原子档：前置发起后应有 apply 待办", len(doing) == 1, str(len(doing)))
        err = None
        try:
            await eng.execute_process_task(doing[0].id, "zhangsan", {
                "submitType": int(SubmitType.AGREE), "tf_ccActors": "wangwu"})
        except RuntimeError as e:
            err = e
        check("原子档：中途注入失败后必须报错", err is not None, str(err))
        t = await repo.find_task_by_id(doing[0].id)
        check("原子档：动作失败后任务行必须回到 DOING(10)",
              int(t.taskState) == int(TaskState.DOING), str(int(t.taskState)))
        inst2 = await repo.find_instance_by_id(inst.id)
        check("原子档：动作失败后实例仍 DOING",
              int(inst2.state) == int(InstanceState.DOING), str(int(inst2.state)))
        cc = await raw_count(adapter, "SELECT COUNT(1) FROM wf_process_cc_instance"
                                      " WHERE process_instance_id = ?", [inst.id])
        check("原子档：cc 零残留", cc == 0, f"n={cc}")

        # ─── ③ 动作档（门面复合）＋裸拼对照 ───────────────────────────────────
        # ③-a 与 ③-b **同参**（fail_cc=2：第一拍 cc 成功、第二拍注入失败）——唯一差别是
        # 经不经门面：门面包裹 ⇒ 整次 dispatch 回滚（两拍写法全消失）；不包 ⇒ 第一拍留存。
        await cleanup(adapter)
        repo.reset_inject(fail_cc=2)
        fA = JeeflowFacade(eng, repo, ext)
        r = await fA.flow("processInstance/startAndExecute", {
            "processDefineId": DEFINE_ID, "operator": "zhangsan",
            "f_ccActors": "wangwu", "tf_ccActors": "lisi"})
        check("动作档：中途失败的动作不得报成功", isinstance(r, dict) and r.get("code", 0) != 0, str(r))
        await assert_no_action_residue(adapter, "动作档（门面整次 dispatch 包裹）")

        # ③-b 裸拼对照：不经门面（两次引擎入口＝两拍）。第一拍提交、第二拍失败 ⇒
        # 半完成留存（实例行=1 存活）——"门面不包"的修前形态，证明 ③-a 那格不是恒真。
        repo.reset_inject(fail_cc=2)
        instB = await eng.start_process_instance_by_id(
            DEFINE_ID, "zhangsan", {"BUSINESS_NO": "PYTIER-BARE", "f_ccActors": "wangwu"})
        doingB = await repo.find_doing_tasks(instB.id)
        err = None
        try:
            await eng.execute_process_task(doingB[0].id, "zhangsan", {
                "submitType": int(SubmitType.AGREE), "tf_ccActors": "lisi"})
        except RuntimeError as e:
            err = e
        check("裸拼对照：第二拍必须失败（注入点）", err is not None, str(err))
        n = await raw_count(adapter, "SELECT COUNT(1) FROM wf_process_instance"
                                     " WHERE process_define_id = ?", [DEFINE_ID])
        check("裸拼对照：第一拍已提交 ⇒ 实例行必须留存 1（两拍各一事务的边界证据）",
              n == 1, f"n={n}")

        # ─── ④ 归属档（嵌套并入＋并发切开）──────────────────────────────────────
        await cleanup(adapter)
        repo.reset_inject()
        err = None
        async def work_nest():
            inner_inst = await eng.start_process_instance_by_id(
                DEFINE_ID, "zhangsan", {"BUSINESS_NO": "PYTIER-NEST"})
            tx = current_tx_conn()
            row = await tx.fetchone(sql_of(adapter,
                "SELECT COUNT(1) FROM wf_process_instance WHERE process_define_id = ?"), [DEFINE_ID])
            check("归属档：嵌套事务内（同连接）应读到引擎写的实例行", row[0] == 1, f"n={row[0]}")
            raise RuntimeError("outer rollback")
        try:
            await repo.with_tx(work_nest)
        except RuntimeError as e:
            err = e
        check("归属档：外层报错必须外抛", err is not None)
        n = await raw_count(adapter, "SELECT COUNT(1) FROM wf_process_instance"
                                     " WHERE process_define_id = ?", [DEFINE_ID])
        check("归属档：外层回滚必须连坐嵌套动作（实例行实得 0）", n == 0, f"n={n}")

        # ④-b 并发切开：A 开事务写行后让出，B 在窗口内自开事务写并成功提交 ⇒
        # A 回滚行不在、B 行在（contextvars 按 task 隔离，互不串）。
        tag_a, tag_b = "pytier-conc-a", "pytier-conc-b"
        a_in_tx = asyncio.Event()
        b_committed = asyncio.Event()

        async def task_a():
            async def work():
                await writer.a_insert("biz_tx_probe", {"id": next_pk(), "tag": tag_a})
                a_in_tx.set()
                await b_committed.wait()
                raise RuntimeError("A rollback")
            await repo.with_tx(work)

        async def task_b():
            await a_in_tx.wait()
            async def work():
                await writer.a_insert("biz_tx_probe", {"id": next_pk(), "tag": tag_b})
            await repo.with_tx(work)
            b_committed.set()

        res_a = await asyncio.gather(task_a(), task_b(), return_exceptions=True)
        check("归属档：A 必须回滚报错", isinstance(res_a[0], RuntimeError), str(res_a[0]))
        n = await raw_count(adapter, "SELECT COUNT(1) FROM biz_tx_probe WHERE tag = ?", [tag_a])
        check("归属档：A 回滚后其行必须不在", n == 0, f"n={n}")
        n = await raw_count(adapter, "SELECT COUNT(1) FROM biz_tx_probe WHERE tag = ?", [tag_b])
        check("归属档：B 已提交的行必须在（并发作用域不得互盖）", n == 1, f"n={n}")

        # ─── ⑤ ext 腿（主仓+扩展仓同事务）──────────────────────────────────────
        await cleanup(adapter)
        # 号段 9301453xx 排他（go tier 工装占 9201451xx~9201454xx——2026-10-11 实测
        # 920145302 上残留 go 的 'tier-cc-b' 行）；查询再叠 actor 过滤，双向防串。
        fake_inst = 930145301
        async def work_ext():
            await repo.create_cc_instance(fake_inst, "zhangsan", "pytier-cc-r")
            await ext.save_surrogate(ProcessSurrogate(
                operator="pytier145", surrogate="lisi", processName="tier-ext-rollback",
                enabled=1, createUser="t", updateUser="t"))
            tx = current_tx_conn()
            row = await tx.fetchone(sql_of(adapter,
                "SELECT COUNT(1) FROM wf_process_cc_instance"
                " WHERE process_instance_id = ? AND actor_id = 'pytier-cc-r'"),
                [fake_inst])
            check("ext 腿：事务内主仓 cc 行应可见", row[0] == 1, f"n={row[0]}")
            row = await tx.fetchone(sql_of(adapter,
                "SELECT COUNT(1) FROM wf_process_surrogate WHERE operator = 'pytier145'"
                " AND process_name = ?"), ["tier-ext-rollback"])
            check("ext 腿：事务内扩展仓委托行应可见", row[0] == 1, f"n={row[0]}")
            raise RuntimeError("boom")
        try:
            await repo.with_tx(work_ext)
            check("ext 腿：回调报错必须外抛", False)
        except RuntimeError:
            check("ext 腿：回调报错必须外抛", True)
        n = await raw_count(adapter, "SELECT COUNT(1) FROM wf_process_cc_instance"
                                     " WHERE process_instance_id = ? AND actor_id = 'pytier-cc-r'",
                            [fake_inst])
        check("ext 腿：回滚后主仓 cc 行必须不在", n == 0, f"n={n}")
        n = await raw_count(adapter, "SELECT COUNT(1) FROM wf_process_surrogate"
                                     " WHERE operator = 'pytier145' AND process_name = ?",
                            ["tier-ext-rollback"])
        check("ext 腿：回滚后扩展仓委托行必须不在——扩展仓不是事务豁免面", n == 0, f"n={n}")

        async def work_ext_commit():
            await repo.create_cc_instance(fake_inst + 1, "zhangsan", "pytier-cc-c")
            return await ext.save_surrogate(ProcessSurrogate(
                operator="pytier145", surrogate="lisi", processName="tier-ext-commit",
                enabled=1, createUser="t", updateUser="t"))
        await repo.with_tx(work_ext_commit)
        n = await raw_count(adapter, "SELECT COUNT(1) FROM wf_process_cc_instance"
                                     " WHERE process_instance_id = ? AND actor_id = 'pytier-cc-c'",
                            [fake_inst + 1])
        check("ext 腿：提交后主仓 cc 行应在", n == 1, f"n={n}")
        n = await raw_count(adapter, "SELECT COUNT(1) FROM wf_process_surrogate"
                                     " WHERE operator = 'pytier145' AND process_name = ?",
                            ["tier-ext-commit"])
        check("ext 腿：提交后扩展仓委托行应在", n == 1, f"n={n}")

        # ─── ⑥ persist 腿（C99 ⑥）─────────────────────────────────────────────
        await cleanup(adapter)

        def build_persist_facade(writer_obj):
            ic = PersistPostInterceptor(writer=writer_obj, loader=repo.find_define_by_id)
            eng_p = EngineImpl(repo, TestUserProv(), TestIDGen())
            eng_p.set_extensions(EngineExtensions(interceptors=[ic]))
            return JeeflowFacade(eng_p, repo, ext)

        # ⑥-a 接线（writer.tx_conn_provider = current_tx_conn）⇒ 业务行并入动作事务，随失败整体回滚
        repo.reset_inject(fail_finish=True)
        fP = build_persist_facade(writer)
        tag_join = "pytier-persist-join"
        r = await fP.flow("processInstance/startAndExecute", {
            "processDefineId": DEFINE_ID_SYNC, "operator": "zhangsan", "f_tag": tag_join})
        check("persist 腿：动作应报失败（实例终结写注入）", r.get("code", 0) != 0, str(r))
        check("persist 腿：实例终结写注入确已命中", repo.fin_hits >= 1, f"hits={repo.fin_hits}")
        n = await raw_count(adapter, "SELECT COUNT(1) FROM biz_tx_probe WHERE tag = ?", [tag_join])
        check("persist 腿：并入事务的业务行必须随动作回滚（实得须 0）", n == 0, f"n={n}")

        # ⑥-b 不接线（降级运行态）⇒ 行逃逸留存：锁定对照，证明 ⑥-a 那格测的不是空气
        repo.reset_inject(fail_finish=True)
        writer_plain = JdbcDynamicTableWriter(sync_conn, dialect="mysql")
        writer_plain.primary_key_generator = lambda table: next_pk()
        fP2 = build_persist_facade(writer_plain)
        tag_escape = "pytier-persist-escape"
        r = await fP2.flow("processInstance/startAndExecute", {
            "processDefineId": DEFINE_ID_SYNC, "operator": "zhangsan", "f_tag": tag_escape})
        check("persist 腿(对照)：不接线时动作同样报失败", r.get("code", 0) != 0, str(r))
        n = await raw_count(adapter, "SELECT COUNT(1) FROM biz_tx_probe WHERE tag = ?", [tag_escape])
        check("persist 腿(对照)：未接线时行应逃逸留存 1（降级运行态红样）", n == 1, f"n={n}")

        await cleanup(adapter)
        await purge_defines(adapter)
    finally:
        sync_conn.close()
        pool.close()
        await pool.wait_closed()

    print(f"\n结果: PASS={passed} FAIL={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    assert DB == "mysql", "本档为 MySQL 真库档（与 go/其他栈的 tier 同库同口径）"
    sys.exit(asyncio.run(main()))
