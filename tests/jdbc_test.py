"""JDBC 仓储集成测试——MySQL / PostgreSQL 双库可跑。

用法：python tests/jdbc_test.py [mysql|postgres]（默认 mysql）

前置条件：
  - 开发服务器（192.168.1.160）：MySQL(3306) / PostgreSQL(5432，Docker mldong-pg)
  - 建表 SQL 自动从本仓 tests/schema/schema-<db>.sql 执行（各语言自带，IF NOT EXISTS 幂等）
测试数据固定 define ID（mysql=900002 / postgres=910002），开头清理，可重复执行。
"""
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import aiomysql
import asyncpg

from jeeflow import EngineImpl
from jeeflow.repository import JdbcRepository, TsIDGenerator, MySqlAdapter, PostgresAdapter, convert_placeholder
from jeeflow.repository.ext import JdbcProcessExtRepository
from jeeflow.model import ProcessDesign, ProcessDesignHis, ProcessSurrogate
from jeeflow.model import ProcessDefine, InstanceState, TaskState, UserInfo
from jeeflow.spi import IDGenerator, QueryCondition

DB = sys.argv[1] if len(sys.argv) > 1 else "mysql"

# 连接信息可用环境变量覆盖（使用者指向自己的库），默认开发服务器
_DB_HOST = os.environ.get("JEFFLOW_DB_HOST", "192.168.1.160")
_DB_PORT = int(os.environ.get("JEFFLOW_DB_PORT", "5432" if DB == "postgres" else "3306"))
_DB_USER = os.environ.get("JEFFLOW_DB_USER", "postgres" if DB == "postgres" else "root")
_DB_PWD = os.environ.get("JEFFLOW_DB_PWD", "8Eli#gr#AUk")

if DB == "postgres":
    DSN = dict(host=_DB_HOST, port=_DB_PORT, user=_DB_USER, password=_DB_PWD, database="jeeflow")
    DEFINE_ID = 910002
else:
    DSN = dict(host=_DB_HOST, port=_DB_PORT, user=_DB_USER, password=_DB_PWD,
               db="jeeflow", charset="utf8mb4", autocommit=True, maxsize=5)
    DEFINE_ID = 900002

import flows_resolver
FLOWS_DIR = flows_resolver.dir()
# 建表 SQL 各语言自带（维护者改 jeeflow-java 仓 resources 后用 scripts/sync-schema.sh 分发）
SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "schema")

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


# ── 环境工厂：测试代码与数据库无关，只换 pool / adapter ──────────────────────

async def make_pool():
    if DB == "postgres":
        return await asyncpg.create_pool(**DSN)
    return await aiomysql.create_pool(**DSN)


def make_adapter(pool):
    if DB == "postgres":
        return PostgresAdapter(pool)
    return MySqlAdapter(pool)


def sql_of(adapter, sql):
    """直查 SQL 统一 `?` → 适配器占位符风格（与仓储核心同一转换）"""
    return convert_placeholder(sql, adapter.placeholder)


async def close_pool(pool):
    if DB == "postgres":
        await pool.close()
    else:
        pool.close()
        await pool.wait_closed()


async def raw_count(adapter, sql, args=()):
    """直查数据库（绕过仓储）——统一连接接口，验证真实落库"""
    conn = await adapter.acquire()
    try:
        row = await conn.fetchone(sql_of(adapter, sql), args)
        return row[0]
    finally:
        await adapter.release(conn)


async def raw_rows(adapter, sql, args=()):
    """直查数据库多行取证（issues/141 G2 的①②③档要看原行 id/state/时间，单行 raw_count 照不出）"""
    conn = await adapter.acquire()
    try:
        return await conn.fetchall(sql_of(adapter, sql), args)
    finally:
        await adapter.release(conn)


async def apply_schema(adapter):
    """执行本仓 tests/schema/schema-<db>.sql 建表（IF NOT EXISTS，幂等）"""
    path = os.path.join(SCHEMA_DIR, f"schema-{DB}.sql")
    conn = await adapter.acquire()
    try:
        buf = ""
        for line in open(path, encoding="utf-8"):
            line = line.strip()
            if not line or line.startswith("--"):
                continue
            buf += line + " "
            if line.endswith(";"):
                await conn.execute(buf.rstrip(";"), [])
                buf = ""
    finally:
        await adapter.release(conn)


async def cleanup(adapter):
    conn = await adapter.acquire()
    try:
        await conn.execute(sql_of(adapter,
            "DELETE FROM wf_process_task_actor WHERE process_task_id IN"
            " (SELECT id FROM wf_process_task WHERE process_instance_id IN"
            " (SELECT id FROM wf_process_instance WHERE process_define_id = ?))"),
            [DEFINE_ID])
        await conn.execute(sql_of(adapter,
            "DELETE FROM wf_process_cc_instance WHERE process_instance_id IN"
            " (SELECT id FROM wf_process_instance WHERE process_define_id = ?)"),
            [DEFINE_ID])
        await conn.execute(sql_of(adapter,
            "DELETE FROM wf_process_task WHERE process_instance_id IN"
            " (SELECT id FROM wf_process_instance WHERE process_define_id = ?)"),
            [DEFINE_ID])
        await conn.execute(sql_of(adapter, "DELETE FROM wf_process_instance WHERE process_define_id = ?"),
                            [DEFINE_ID])
        await conn.execute(sql_of(adapter, "DELETE FROM wf_process_define WHERE id = ?"), [DEFINE_ID])
    finally:
        await adapter.release(conn)


def load_flow(name):
    with open(os.path.join(FLOWS_DIR, name), encoding="utf-8") as f:
        return f.read()


# _STACK_SEQ：本栈 T1 测试 id 的栈位（issues/118 §2.5）。三栈并行连同一台 160 MySQL 时，
# 旧式 ts_ms*1000+序号 会在同一毫秒生成完全相同的 id ⇒ 主键冲突随机复现。
# 统一公式 id = ts_ms*4000 + 栈位*1000 + 序号（序号 <1000）；python=1 / node=2 / go=3。
_STACK_SEQ = 1


class TestIDGen(IDGenerator):
    """时间戳 + 序号（避免与数据库已有 ID 冲突）"""

    def __init__(self):
        import time
        self.base = int(time.time() * 1000) * 4000 + _STACK_SEQ * 1000
        self.n = 0

    def next_id(self):
        self.n += 1
        return self.base + self.n


class TestUserProv:
    async def get_user(self, uid):
        return UserInfo(userId=uid, realName=uid, deptId="D01", deptName="部门",
                        postId="P01", postName="岗位")


async def main():
    print(f"== JdbcRepository 集成测试（{DB} @ 192.168.1.160）==")
    pool = await make_pool()
    try:
        adapter = make_adapter(pool)
        await apply_schema(adapter)
        await cleanup(adapter)
        repo = JdbcRepository(adapter, TsIDGenerator())
        eng = EngineImpl(repo, TestUserProv(), TestIDGen())

        # ── ① 插入流程定义（01-simple：start→apply(发起人)→task1(leader)→end）
        content = load_flow("01-simple.json")
        raw = json.loads(content)
        conn = await adapter.acquire()
        try:
            from datetime import datetime
            now = datetime.now()
            await conn.execute(sql_of(adapter,
                "INSERT INTO wf_process_define (id, name, display_name, type, state, content,"
                " version, create_time, create_user, update_time, update_user)"
                " VALUES (?,?,?,?,1,?,1,?,?,?,?)"),
                [DEFINE_ID, "py-simple", raw["displayName"], raw["type"], content,
                 now, "py-test", now, "py-test"])
        finally:
            await adapter.release(conn)

        # ── ② 启动：start → apply（发起人 zhangsan，applicant→发起人）
        inst = await eng.start_process_instance_by_id(DEFINE_ID, "zhangsan",
                                                      {"amount": "1000", "BUSINESS_NO": f"BIZ-{DB}-001"})
        check("启动后实例进行中", inst.state == InstanceState.DOING, str(inst.state))
        check("生成业务号", bool(inst.businessNo), inst.businessNo)
        doing = await repo.find_doing_tasks(inst.id)
        check("启动产生 apply 任务", len(doing) == 1 and doing[0].taskName == "apply",
              str([t.taskName for t in doing]))
        check("apply 参与者为发起人（applicant→发起人）",
              doing[0].actorIds == ["zhangsan"], str(doing[0].actorIds))

        # ── ③ 完成 apply（startAndExecute 语义）→ task1（leader）
        inst = await eng.execute_process_task(doing[0].id, "zhangsan")
        done = await repo.find_done_tasks(inst.id)
        check("apply 已完成且处理人是发起人",
              len(done) == 1 and done[0].taskName == "apply" and done[0].actorId == "zhangsan",
              str([(t.taskName, t.actorId) for t in done]))
        check("apply 记录完成时间", done[0].finishTime is not None)
        doing = await repo.find_doing_tasks(inst.id)
        check("产生 task1 待办", len(doing) == 1 and doing[0].taskName == "task1",
              str([t.taskName for t in doing]))
        check("task1 参与者为 leader", doing[0].actorIds == ["leader"], str(doing[0].actorIds))

        # ── ④ 完成 task1 → end → 实例完成
        inst = await eng.execute_process_task(doing[0].id, "leader", {"comment": "ok"})
        check("流程实例完成", inst.state == InstanceState.DONE, str(inst.state))

        # ── ⑤ 重新连接验证持久化（直查数据库）
        pool2 = await make_pool()
        try:
            adapter2 = make_adapter(pool2)
            repo2 = JdbcRepository(adapter2, TsIDGenerator())
            inst2 = await repo2.find_instance_by_id(inst.id)
            check("重新加载实例状态完成", inst2 is not None and inst2.state == InstanceState.DONE,
                  str(inst2.state if inst2 else None))
            check("变量 amount 持久化", inst2 and inst2.variables.get("amount") == "1000",
                  str(inst2.variables if inst2 else None))
            hist = await repo2.find_history_tasks(inst.id)
            check("历史任务 2 条", len(hist) == 2, str(len(hist)))
            check("任务参与者关系持久化", all(len(t.actorIds) > 0 for t in hist),
                  str([t.actorIds for t in hist]))
            state = await raw_count(adapter2,
                                    "SELECT state FROM wf_process_instance WHERE id = ?", [inst.id])
            check("直查数据库实例已完成", state == int(InstanceState.DONE), str(state))
        finally:
            await close_pool(pool2)

        # ── ⑥ 权限负向：非参与者操作被拒（沿用 ⑤ 的流程数据）
        inst = await eng.start_process_instance_by_id(DEFINE_ID, "zhangsan",
                                                      {"BUSINESS_NO": f"BIZ-{DB}-002"})
        doing = await repo.find_doing_tasks(inst.id)
        denied = False
        try:
            await eng.execute_process_task(doing[0].id, "hacker")
        except PermissionError:
            denied = True
        except Exception as e:
            denied = "not allowed" in str(e).lower() or "权限" in str(e)
        check("非参与者被拒", denied)
        t = await repo.find_task_by_id(doing[0].id)
        check("被拒后任务仍进行中", t is not None and t.taskState == TaskState.DOING, str(t.taskState))

        # ── ⑦ 事务（spec §7.4）：提交 / 回滚 / 事务内绑定读
        await cleanup(adapter)
        tx_inst_id = DEFINE_ID + 1
        tx_cc_id = DEFINE_ID + 1

        async def tx_commit():
            async def work():
                from datetime import datetime
                from jeeflow.model import ProcessInstance
                now = datetime.now()
                await repo.save_instance(ProcessInstance(
                    id=tx_inst_id, defineId=DEFINE_ID, state=InstanceState.DOING, operator="zhangsan",
                    businessNo="TXN-001", variables={"k": "v"}, createTime=now, updateTime=now,
                    createUser="t", updateUser="t"))
                await repo.create_cc_instance(tx_inst_id, "zhangsan", "lisi", "wangwu")
                got = await repo.find_instance_by_id(tx_inst_id)  # 事务内绑定读
                return got is not None
            return await repo.with_tx(work)

        ok = await tx_commit()
        check("事务提交落库", ok)
        n = await raw_count(adapter, "SELECT COUNT(*) FROM wf_process_instance WHERE id = ?",
                            [tx_inst_id])
        cc = await raw_count(adapter,
                             "SELECT COUNT(*) FROM wf_process_cc_instance WHERE process_instance_id = ?",
                             [tx_inst_id])
        check("实例 1 条 + 抄送 2 条", n == 1 and cc == 2, f"instance={n} cc={cc}")

        async def tx_rollback():
            async def work():
                from datetime import datetime
                from jeeflow.model import ProcessInstance
                now = datetime.now()
                await repo.save_instance(ProcessInstance(
                    id=tx_inst_id + 1, defineId=DEFINE_ID, state=InstanceState.DOING,
                    operator="zhangsan", createTime=now, updateTime=now, createUser="t",
                    updateUser="t"))
                await repo.create_cc_instance(tx_inst_id + 1, "zhangsan", "lisi")
                raise RuntimeError("boom")
            try:
                await repo.with_tx(work)
                return False
            except RuntimeError:
                return True

        ok = await tx_rollback()
        check("事务异常回滚", ok)
        n = await raw_count(adapter, "SELECT COUNT(*) FROM wf_process_instance WHERE id = ?",
                            [tx_inst_id + 1])
        cc = await raw_count(adapter,
                             "SELECT COUNT(*) FROM wf_process_cc_instance WHERE process_instance_id = ?",
                             [tx_inst_id + 1])
        check("回滚后无残留数据", n == 0 and cc == 0, f"instance={n} cc={cc}")

        # ── ⑧ 定义写操作 SPI（v1.0.1，集成反馈①）──
        d = ProcessDefine(name="py-crud", displayName="CRUD 流程", type="test",
                          state=1, version=1, content="{}", updateUser="tester")
        await repo.save_define(d)
        check("save_define 生成 ID", d.id > 0, str(d.id))
        loaded = await repo.find_define_by_id(d.id)
        check("保存后可查询", loaded is not None and loaded.name == "py-crud",
              str(loaded.name if loaded else None))
        loaded.displayName = "CRUD 流程 v2"
        loaded.content = '{"v":2}'
        await repo.update_define(loaded)
        updated = await repo.find_define_by_id(d.id)
        check("update_define 生效", updated.displayName == "CRUD 流程 v2", str(updated.displayName))
        await repo.update_define_state(d.id, 0)
        st = await repo.find_define_by_id(d.id)
        check("update_define_state 生效", st.state == 0, str(st.state))
        await repo.remove_define(d.id)
        check("remove_define 删除", await repo.find_define_by_id(d.id) is None)

        # ── ⑨ update_instance 级联持久化任务状态（v1.0.1，集成反馈②）──
        # 恢复 01-simple 定义（⑦ 事务测试前已清理）
        content = load_flow("01-simple.json")
        raw = json.loads(content)
        conn = await adapter.acquire()
        try:
            now = datetime.now()
            await conn.execute(sql_of(adapter,
                "INSERT INTO wf_process_define (id, name, display_name, type, state, content,"
                " version, create_time, create_user, update_time, update_user)"
                " VALUES (?,?,?,?,1,?,1,?,?,?,?)"),
                [DEFINE_ID, "py-simple", raw["displayName"], raw["type"], content,
                 now, "py-test", now, "py-test"])
        finally:
            await adapter.release(conn)
        inst = await eng.start_process_instance_by_id(DEFINE_ID, "zhangsan",
                                                      {"BUSINESS_NO": f"BIZ-{DB}-003"})
        reloaded = await repo.find_instance_by_id(inst.id)
        tasks = await repo.find_history_tasks(inst.id)
        check("实例加载任务", reloaded is not None and len(tasks) > 0, str(len(tasks)))
        for t in tasks:
            t.taskState = TaskState.ABANDONED
        reloaded.tasks = tasks
        await repo.update_instance(reloaded)
        after = await repo.find_history_tasks(inst.id)
        check("update_instance 级联任务状态落库",
              all(t.taskState == TaskState.ABANDONED for t in after),
              str([int(t.taskState) for t in after]))

        # ── ⑨b issues/113：门面撤回须把 doing 任务以 30（WITHDRAW）落库 ──
        # 改前撤回路径写 ABANDONED(99)，且门面级断言只验"doing 清空"→ 30/99 都满足，SQL 层无人验
        from jeeflow.facade import JeeflowFacade
        facade113 = JeeflowFacade(eng, repo, None)
        inst113 = await eng.start_process_instance_by_id(DEFINE_ID, "zhangsan",
                                                         {"BUSINESS_NO": f"BIZ-{DB}-113"})
        doing113 = await repo.find_doing_tasks(inst113.id)
        check("⑨b 撤回前存在 doing 任务", len(doing113) > 0, str(len(doing113)))
        rw = await facade113.flow("processInstance/withdraw",
                                  {"id": inst113.id, "operator": "zhangsan"})
        check("⑨b 门面撤回成功", rw["code"] == 0, str(rw))
        rows113 = await repo.find_history_tasks(inst113.id)
        check("⑨b 撤回任务落库 task_state=30（非 99）",
              len(rows113) > 0 and all(int(t.taskState) == int(TaskState.WITHDRAW) for t in rows113),
              str([int(t.taskState) for t in rows113]))
        check("⑨b 撤回后无 doing 任务", len(await repo.find_doing_tasks(inst113.id)) == 0)

        # ── ⑩ 扩展仓储：设计 CRUD + 委托生效（v1.1.0）──
        from jeeflow.memory import MemoryExtRepository  # noqa: F401
        ext_repo = JdbcProcessExtRepository(adapter, TsIDGenerator())
        d = ProcessDesign(name="pyext-design", displayName="扩展设计", type="approval",
                          createUser="t", updateUser="t")
        await ext_repo.save_design(d)
        check("save_design 生成 ID", d.id > 0, str(d.id))
        await ext_repo.save_design_his(ProcessDesignHis(processDesignId=d.id, content='{"v":1}', createUser="t"))
        await ext_repo.save_design_his(ProcessDesignHis(processDesignId=d.id, content='{"v":2}', createUser="t"))
        his_list = await ext_repo.list_design_his(d.id)
        check("设计历史倒序", len(his_list) == 2 and his_list[0].content == '{"v":2}', str(len(his_list)))
        rows, total = await ext_repo.page_designs(filters={"name": "pyext-design"})
        check("设计分页", total == 1 and len(rows) == 1, f"total={total}")
        await ext_repo.remove_design(d.id)
        check("remove_design 连带历史",
              await ext_repo.find_design_by_id(d.id) is None and len(await ext_repo.list_design_his(d.id)) == 0)

        # 委托生效查询
        s_all = ProcessSurrogate(operator="pyext-op", surrogate="agent-all", enabled=1,
                                 createUser="t", updateUser="t")
        await ext_repo.save_surrogate(s_all)
        hit = await ext_repo.get_surrogate("pyext-op", "leave")
        check("全流程委托兜底", hit is not None and hit.surrogate == "agent-all",
              str(hit.surrogate if hit else None))
        check("无委托返回 None", await ext_repo.get_surrogate("nobody", "leave") is None)
        srows, stotal = await ext_repo.page_surrogates(filters={"operator": "pyext-op"})
        check("委托分页", stotal == 1 and len(srows) == 1, f"total={stotal}")
        await ext_repo.remove_surrogate(s_all.id)
        check("remove_surrogate", await ext_repo.find_surrogate_by_id(s_all.id) is None)

        # 委托分页 m_ 条件（issues/82-7，JDBC 路径）：m_IN_processName / m_EQ_enabled
        op = "pyext-cond-op"
        cond_ids = []
        for pname, en in (("leave", 1), ("overtime", 1), ("sick", 0)):
            sc = ProcessSurrogate(operator=op, surrogate="c" + pname, processName=pname,
                                  enabled=en, createUser="t", updateUser="t")
            await ext_repo.save_surrogate(sc)
            cond_ids.append(sc.id)
        check("委托分页无条件=3", (await ext_repo.page_surrogates(filters={"operator": op}))[1] == 3)
        _, in_total = await ext_repo.page_surrogates(filters={"operator": op},
            conditions=[QueryCondition(column="t.process_name", operator="IN", value=["leave", "overtime"])])
        check("m_IN_processName 命中2", in_total == 2, f"total={in_total}")
        _, eq_total = await ext_repo.page_surrogates(filters={"operator": op},
            conditions=[QueryCondition(column="t.enabled", operator="EQ", value=1)])
        check("m_EQ_enabled=1 命中2", eq_total == 2, f"total={eq_total}")
        _, combo_total = await ext_repo.page_surrogates(filters={"operator": op},
            conditions=[QueryCondition(column="t.process_name", operator="IN", value=["sick", "overtime"]),
                        QueryCondition(column="t.enabled", operator="EQ", value=1)])
        check("m_IN+m_EQ 组合命中1", combo_total == 1, f"total={combo_total}")
        _, none_total = await ext_repo.page_surrogates(filters={"operator": op},
            conditions=[QueryCondition(column="t.process_name", operator="IN", value=["none1", "none2"])])
        check("m_IN 全不命中=0", none_total == 0, f"total={none_total}")
        for cid in cond_ids:
            await ext_repo.remove_surrogate(cid)

        # ── ⑩.5 page_cc_instances（v1.3.0 ccList 分页）──
        await repo.create_cc_instance(inst.id, "zhangsan", "lisi", "wangwu")
        ccrows, cctotal = await repo.page_cc_instances(1, 10, "lisi")
        cc_ids = {r.id for r in ccrows}
        check("page_cc_instances 命中抄送人", cctotal >= 1 and inst.id in cc_ids,
              f"total={cctotal} ids={cc_ids}")
        check("page_cc_instances 关联定义名", ccrows and ccrows[0].defineName == "py-simple",
              ccrows[0].defineName if ccrows else "EMPTY")
        cc2, cc2total = await repo.page_cc_instances(1, 10, "nobody")
        check("page_cc_instances 非抄送人空", cc2total == 0 and len(cc2) == 0, f"total={cc2total}")

        # ── ⑩.6 issues/141 G1＋G2：抄送分页归属必填 ＋ cc 写侧判重＝幂等空操作（真库一路）──
        # 判据与 T0 的 tests/spec_test.py「Test 141 G1＋G2」逐字同一条：同一栈的 SQL 仓与内存仓
        # 必须给同一个答案（spec 06-facade.md §2.5，issues/117 场景 27 那把尺子），
        # 而 G2 的四档（①不新增行 ②不重置未读 ③不刷原行时间 ④不发码 4）里①②③要在
        # **裸写入口 create_cc_instance** 上照——走漏斗时 SPI default 已把子集算好，
        # 摘掉仓储里的判重照样绿（本轮实测踩过这个洞）。④那一档由 T0 的漏斗格钉（真库不重跑事件腿）。
        from datetime import datetime as _dt141
        from jeeflow.model import ProcessInstance as _PI141
        cc141_a, cc141_b = DEFINE_ID + 7, DEFINE_ID + 8          # 独立实例 id 段（defineId 挂 DEFINE_ID，随 cleanup 清）
        cc141_now = _dt141.now()
        for _iid, _biz, _actor in ((cc141_a, "I141-MINE", "i141user1"),
                                   (cc141_b, "I141-THEIRS", "i141user2")):
            await repo.save_instance(_PI141(
                id=_iid, defineId=DEFINE_ID, state=InstanceState.DOING, operator="zhangsan",
                businessNo=_biz, variables={}, createTime=cc141_now, updateTime=cc141_now,
                createUser="py-test", updateUser="py-test"))
            await repo.create_cc_instance(_iid, "zhangsan", _actor)

        _cc_sql = ("SELECT id, actor_id, state, create_time, update_time FROM wf_process_cc_instance"
                   " WHERE process_instance_id = ? AND actor_id = ? ORDER BY id")

        # G1：归属条件必填——缺条件/空值三形 ⇒ 空页；只给条件形 ⇒ 命中（改前这一格是红的：
        # 旧代码恒拼 `WHERE cc.actor_id = ?` 绑 None ⇒ `= NULL` 恒不命中，与内存仓两个答案）
        _rows141, _t141 = await repo.page_cc_instances(1, 10, None)
        check("⑩.6 G1 缺归属条件 ⇒ 空页", _t141 == 0 and len(_rows141) == 0, f"total={_t141}")
        _rows141, _t141 = await repo.page_cc_instances(1, 10)
        check("⑩.6 G1 整条查询不带归属 ⇒ 空页", _t141 == 0 and len(_rows141) == 0, f"total={_t141}")
        for _blank in ("", "   "):
            _rows141, _t141 = await repo.page_cc_instances(1, 10, _blank)
            check(f"⑩.6 G1 空值入参 {_blank!r} ⇒ 空页", _t141 == 0 and len(_rows141) == 0, f"total={_t141}")
        _rows141, _t141 = await repo.page_cc_instances(
            1, 10, None, [QueryCondition("cc.actor_id", "EQ", None)])
        check("⑩.6 G1 条件 EQ None ⇒ 空页", _t141 == 0 and len(_rows141) == 0, f"total={_t141}")
        _rows141, _t141 = await repo.page_cc_instances(
            1, 10, None, [QueryCondition("cc.actor_id", "IN", [])])
        check("⑩.6 G1 条件 IN 空集合 ⇒ 空页", _t141 == 0 and len(_rows141) == 0, f"total={_t141}")
        _rows141, _t141 = await repo.page_cc_instances(
            1, 10, None, [QueryCondition("cc.actor_id", "EQ", "i141user1")])
        check("⑩.6 G1 只给条件形（不给入参）⇒ 命中 1 行＝与内存仓同答案",
              _t141 == 1 and len(_rows141) == 1 and _rows141[0].id == cc141_a,
              f"total={_t141} ids={[x.id for x in _rows141]}")
        _rows141, _t141 = await repo.page_cc_instances(
            1, 10, "i141user1", [QueryCondition("t.business_no", "LIKE", "")])
        check("⑩.6 G1 非归属列空值仍按没填忽略（可选过滤不许一起收掉）",
              _t141 == 1 and len(_rows141) == 1, f"total={_t141}")
        _rows141, _t141 = await repo.page_cc_instances(
            1, 10, "i141user1", [QueryCondition("t.business_no", "LIKE", "NO-SUCH")])
        check("⑩.6 G1 非归属列真值仍生效（上一格不是假绿）",
              _t141 == 0 and len(_rows141) == 0, f"total={_t141}")

        # G2①＋同调用折叠：裸写入口重复给同一个人 ⇒ 仍是 1 行、原行 id 不变（没有删旧插新）
        _before = await raw_rows(adapter, _cc_sql, [cc141_a, "i141user1"])
        await repo.create_cc_instance(cc141_a, "zhangsan", "i141user1", "i141user1")
        _after = await raw_rows(adapter, _cc_sql, [cc141_a, "i141user1"])
        check("⑩.6 G2 ①重复抄送不新增行（同一次调用内的重复也只落一行）",
              len(_before) == 1 and len(_after) == 1 and _after[0][0] == _before[0][0],
              f"before={_before} after={_after}")
        check("⑩.6 G2 ③原行 create_time/update_time 逐字不变（不碰 UPDATE、不重插）",
              _after and _after[0][3] == _before[0][3] and _after[0][4] == _before[0][4],
              f"{_before[0][3:]} → {_after[0][3:] if _after else None}")

        # G2②＋③（已读档）：真 SQL 置读 state=1 后重复抄送，既不许抹回未读也不许刷时间
        await repo.update_cc_status(cc141_a, "i141user1")
        _read = await raw_rows(adapter, _cc_sql, [cc141_a, "i141user1"])
        check("⑩.6 G2 置读后 state=1（取证基线）", _read[0][2] == 1, f"row={_read}")
        await repo.create_cc_instance(cc141_a, "zhangsan", "i141user1")
        _read2 = await raw_rows(adapter, _cc_sql, [cc141_a, "i141user1"])
        check("⑩.6 G2 ②重复抄送不把已读抹回未读（也不冒出第二行未读盖住它）",
              len(_read2) == 1 and _read2[0][2] == 1, f"row={_read2}")
        check("⑩.6 G2 ③已读行被重复抄送时 update_time 不被刷",
              _read2[0][4] == _read[0][4], f"{_read[0][4]} → {_read2[0][4]}")

        # G2 子集返回：已知人剔掉、新人留下、同调用重复折叠 ⇒ 漏斗据此只 fire 这一支
        _fresh = await repo.create_cc_instance_if_absent(
            cc141_a, "zhangsan", ["i141user1", "i141user2", "i141new", "i141new"])
        _cnt_a = int(await raw_count(adapter,
            "SELECT COUNT(*) FROM wf_process_cc_instance WHERE process_instance_id = ?", [cc141_a]))
        _actors_a = sorted(await repo.find_cc_actor_ids(cc141_a))
        _actors_b = await repo.find_cc_actor_ids(cc141_b)
        _cnt_b = int(await raw_count(adapter,
            "SELECT COUNT(*) FROM wf_process_cc_instance WHERE process_instance_id = ?", [cc141_b]))
        check("⑩.6 G2 create_cc_instance_if_absent 返回实际新建子集",
              _fresh == ["i141user2", "i141new"], f"created={_fresh}")
        check("⑩.6 G2 子集只落对应的那些行（cc141_a 共 3 行）", _cnt_a == 3, f"count={_cnt_a}")
        check("⑩.6 G2 find_cc_actor_ids 读侧＝库里的真行集",
              _actors_a == ["i141new", "i141user1", "i141user2"], f"actors={_actors_a}")
        check("⑩.6 G2 判重作用域按实例不按全局（cc141_b 上同一个人照旧只有它自己那一行）",
              _actors_b == ["i141user2"] and _cnt_b == 1, f"b={_actors_b}/{_cnt_b}")
        _actors_none = await repo.find_cc_actor_ids(9_999_999_999)
        check("⑩.6 G2 空实例读侧为空集", _actors_none == [], f"{_actors_none}")
        # 全重复 ⇒ 子集空（漏斗据此整支不发码 4）
        _again = await repo.create_cc_instance_if_absent(cc141_a, "zhangsan", ["i141user1", "i141new"])
        check("⑩.6 G2 全重复时子集为空（④不发码 4 的依据）", _again == [], f"created={_again}")

        # ── ⑩.7 issues/141 G10：空抄送人不建 cc 行（真库 SQL 仓一路）──
        # 判据与 T0 的 tests/spec_test.py「Test 141 G10」同一条（spec 06-facade.md §2.10，基准＝
        # jeeflow-java 5fbd5ac）：空串/纯空白/None 一律丢弃、落库与比较取 trim 后的值。
        # 这一层是**写侧兜底**——绕过引擎漏斗（parse_cc_actors）与门面直连仓储的调用方同样灌不进
        # 空值；只修漏斗时下面这几格全红（本轮普查实测：旧形状 ("",) 真落一条 actor_id='' 的行）。
        _cc_all = ("SELECT id, actor_id FROM wf_process_cc_instance"
                   " WHERE process_instance_id = ? ORDER BY id")
        g10_a, g10_b, g10_c, g10_d = DEFINE_ID + 17, DEFINE_ID + 18, DEFINE_ID + 19, DEFINE_ID + 20
        g10_now = _dt141.now()
        for _iid in (g10_a, g10_b, g10_c, g10_d):
            await repo.save_instance(_PI141(
                id=_iid, defineId=DEFINE_ID, state=InstanceState.DOING, operator="zhangsan",
                businessNo=f"G10-{_iid}", variables={},
                createTime=g10_now, updateTime=g10_now, createUser="py-test", updateUser="py-test"))

        await repo.create_cc_instance(g10_a, "zhangsan", "", "   ", "\t", None)
        _cnt = int(await raw_count(adapter, "SELECT COUNT(*) FROM wf_process_cc_instance"
                                          " WHERE process_instance_id = ?", [g10_a]))
        check("⑩.7 G10 裸写入口灌空串/纯空白/None ⇒ 零行（写侧兜底，不只靠漏斗）",
              _cnt == 0, f"count={_cnt}")
        check("⑩.7 G10 空档读侧也是空集",
              await repo.find_cc_actor_ids(g10_a) == [],
              f"{await repo.find_cc_actor_ids(g10_a)}")
        _sub = await repo.create_cc_instance_if_absent(g10_a, "zhangsan", ["", "  ", None])
        check("⑩.7 G10 全空入参时 SPI default 子集为空（据此不发码 4）",
              _sub == [] and int(await raw_count(adapter,
                  "SELECT COUNT(*) FROM wf_process_cc_instance WHERE process_instance_id = ?",
                  [g10_a])) == 0, f"created={_sub}")

        await repo.create_cc_instance(g10_b, "zhangsan", " 8141t1 ")
        _actors_b = await repo.find_cc_actor_ids(g10_b)
        check("⑩.7 G10 落库值取 trim 后的串", _actors_b == ["8141t1"], f"actors={_actors_b}")
        await repo.create_cc_instance(g10_b, "zhangsan", "8141t1")
        _rows_b = await raw_rows(adapter, _cc_all, [g10_b])
        check("⑩.7 G10 trim 判等与 G2 写侧判重咬合（跨调用同一人不落两行）",
              [r[1] for r in _rows_b] == ["8141t1"], f"rows={[r[1] for r in _rows_b]}")
        await repo.create_cc_instance(g10_b, "zhangsan", " 8141t2 ", "8141t2")
        _rows_b = await raw_rows(adapter, _cc_all, [g10_b])
        check("⑩.7 G10 同一次调用内两形也只落一行",
              [r[1] for r in _rows_b] == ["8141t1", "8141t2"], f"rows={[r[1] for r in _rows_b]}")

        _fresh = await repo.create_cc_instance_if_absent(
            g10_b, "zhangsan", ["", " 8141t1 ", "8141t1", None, " 8141t3 ", "8141t3"])
        check("⑩.7 G10 default 子集＝归一后的新人（空值与已有值都不进子集）",
              _fresh == ["8141t3"], f"created={_fresh}")
        _rows_b = await raw_rows(adapter, _cc_all, [g10_b])
        check("⑩.7 G10 子集只落对应那一行",
              [r[1] for r in _rows_b] == ["8141t1", "8141t2", "8141t3"],
              f"rows={[r[1] for r in _rows_b]}")

        await repo.create_cc_instance(g10_c, "zhangsan", "0")
        _actors_c = await repo.find_cc_actor_ids(g10_c)
        check("⑩.7 G10 反向哨兵：'0' 是正常 id，不得被当空值丢掉", _actors_c == ["0"],
              f"actors={_actors_c}")

        # 两仓同答案（issues/117 场景 27）：同一批带空值/带空格的入参**逐对灌进同一个实例**，
        # SQL 仓与内存仓的每一档读法都必须同答案（判据分叉只修一边时这一格红）
        from jeeflow.memory import MemoryRepository as _MemRepo141
        _mem = _MemRepo141()
        _g10_matrix = [("",), ("  ",), ("x1", ""), (" x2 ",), ("x2",), ("0",), (" 0 ",), (None,)]
        _sql_ans, _mem_ans = {}, {}
        for _i, _args in enumerate(_g10_matrix, start=1):
            await repo.create_cc_instance(g10_d, "zhangsan", *_args)
            await _mem.create_cc_instance(g10_d, "zhangsan", *_args)
            _sql_ans[_i] = sorted(r[1] for r in await raw_rows(adapter, _cc_all, [g10_d]))
            _mem_ans[_i] = sorted(str(r) for r in _mem.cc_rows_for_test(g10_d))
        check("⑩.7 G10 两仓同判据（SQL 仓与内存仓在同一批入参下必须同答案）",
              _sql_ans == _mem_ans, f"SQL={_sql_ans.get(len(_g10_matrix))} 内存={_mem_ans.get(len(_g10_matrix))}")
        check("⑩.7 G10 两仓末档读法＝归一后 3 人（空档零行、trim 判等、哨兵 0 保住）",
              _sql_ans[len(_g10_matrix)] == ["0", "x1", "x2"] and
              _mem_ans[len(_g10_matrix)] == ["0", "x1", "x2"],
              f"SQL={_sql_ans[len(_g10_matrix)]} 内存={_mem_ans[len(_g10_matrix)]}")

        # 本轮取证行按实例 id 清干净（零残留；实例行由收尾的 cleanup 按 define_id 兜）
        conn = await adapter.acquire()
        try:
            for _iid in (cc141_a, cc141_b, g10_a, g10_b, g10_c, g10_d):
                await conn.execute(
                    sql_of(adapter, "DELETE FROM wf_process_cc_instance WHERE process_instance_id = ?"),
                    [_iid])
        finally:
            await adapter.release(conn)

        # ── ⑪ find_define_by_name（v1.1.0 deploy 版本管理用）──
        latest = await repo.find_define_by_name("py-simple")
        check("find_define_by_name 命中", latest is not None and latest.name == "py-simple",
              str(latest.name if latest else None))
        check("find_define_by_name 未命中", await repo.find_define_by_name("no-such-flow") is None)

        # ── ⑫ issues/110：SQL 仓 find_instance_by_id 水合任务 → detail 任务列表非空 ──
        # 修复前：JdbcRepository.find_instance_by_id 只查实例单表，tasks 恒空，
        # 门面 processInstance/detail 的 tasks/activeTaskList 恒为空数组。
        # 对齐 Java findTasksByInstanceId / PHP / C# issues/89 聚合水合。
        from jeeflow.facade import JeeflowFacade
        inst110 = await eng.start_process_instance_by_id(
            DEFINE_ID, "zhangsan", {"BUSINESS_NO": f"BIZ-{DB}-110"})
        # 直接仓储层：水合任务 + actorIds
        hydrated = await repo.find_instance_by_id(inst110.id)
        check("SQL 仓 find_instance_by_id 水合任务非空",
              hydrated is not None and len(hydrated.tasks) > 0,
              str([t.taskName for t in hydrated.tasks] if hydrated else None))
        check("水合任务带参与者 actorIds",
              all(len(t.actorIds) > 0 for t in hydrated.tasks),
              str([t.actorIds for t in hydrated.tasks]))
        # 门面层：detail 的 tasks / activeTaskList 非空
        facade110 = JeeflowFacade(eng, repo, None)
        detail = await facade110.flow("processInstance/detail", {"id": inst110.id})
        d = detail.get("data") or {}
        check("detail tasks 非空", isinstance(d.get("tasks"), list) and len(d["tasks"]) > 0,
              str(len(d.get("tasks") or [])))
        check("detail activeTaskList 非空",
              isinstance(d.get("activeTaskList"), list) and len(d["activeTaskList"]) > 0,
              str([t.get("taskName") for t in d.get("activeTaskList") or []]))

        # ── ⑬ issues/114+115：撤回鉴权 / 转办——SQL 仓真实落库（门面级断言）──
        # 修复前：撤回零鉴权 + operator 缺省回落 user1；门面分派表从未挂 removeTaskActor（转办无从实现）。
        # 断言形状纪律：一律读回库里的持久值（state/update_user/variable/actor 行），不看"列表空不空"。
        facade_bc = JeeflowFacade(eng, repo, None)

        async def _one(sql, args=()):
            conn = await adapter.acquire()
            try:
                return await conn.fetchone(sql_of(adapter, sql), args)
            finally:
                await adapter.release(conn)

        async def _to_task1(instance_id, tag="⑬"):
            """提交申请节点 → 推进到 task1：让"发起人"与"进行中任务参与者"是两个人，
            判据 ①（发起人）与判据 ②（参与者）才可分别验证，同时留下一行已完成(20) 任务。"""
            a_task = [t for t in await repo.find_doing_tasks(instance_id) if t.taskName == "apply"][0]
            r = await facade_bc.flow("processTask/execute", {"processTaskId": a_task.id,
                                                             "operator": "zhangsan", "submitType": 1})
            if not check(f"{tag} 申请节点提交后推进", r["code"] == 0, str(r)):
                raise AssertionError(f"申请节点提交失败: {r}")
            doing = [t for t in await repo.find_doing_tasks(instance_id) if t.taskName == "task1"]
            if not check(f"{tag} 存在 task1 进行中任务", len(doing) == 1, str([t.taskName for t in doing])):
                raise AssertionError("未推进到 task1")
            return await repo.find_task_by_id(a_task.id), doing[0]

        inst114 = await eng.start_process_instance_by_id(DEFINE_ID, "zhangsan",
                                                         {"BUSINESS_NO": f"BIZ-{DB}-114"})
        apply114, task114 = await _to_task1(inst114.id)
        check("⑬ task1 参与者是 leader（非发起人）",
              await repo.find_task_actors(task114.id) == ["leader"],
              str(await repo.find_task_actors(task114.id)))

        r = await facade_bc.flow("processInstance/withdraw", {"id": inst114.id})
        check("⑬ 缺 operator 明确报错（非回落 user1）",
              r["code"] == 99999999 and "operator 必填" in r["msg"], str(r))
        r = await facade_bc.flow("processInstance/withdraw",
                                 {"id": inst114.id, "operator": "intruder"})
        check("⑬ 无关第三人撤回被拒",
              r["code"] == 99999999 and "无权限撤回该流程实例" in r["msg"], str(r))
        row = await _one("SELECT state, update_user FROM wf_process_instance WHERE id=?", [inst114.id])
        check("⑬ 拒绝后实例仍 10 且未记他人", int(row[0]) == 10 and row[1] != "intruder", str(row))
        row = await _one("SELECT task_state FROM wf_process_task WHERE id=?", [task114.id])
        check("⑬ 拒绝后任务仍 10", int(row[0]) == 10, str(row))

        r = await facade_bc.flow("processInstance/withdraw",
                                 {"id": inst114.id, "operator": "leader"})
        check("⑬ 进行中任务参与者可撤回整单", r["code"] == 0, str(r))
        row = await _one("SELECT state, update_user, operator FROM wf_process_instance WHERE id=?",
                         [inst114.id])
        check("⑬ 实例落库 30 + update_user=撤回人（发起人列不动）",
              int(row[0]) == 30 and row[1] == "leader" and row[2] == "zhangsan", str(row))
        row = await _one("SELECT task_state, update_user FROM wf_process_task WHERE id=?", [task114.id])
        check("⑬ 进行中任务落库 30 + update_user=撤回人",
              int(row[0]) == 30 and row[1] == "leader", str(row))
        row = await _one("SELECT task_state, update_user FROM wf_process_task WHERE id=?", [apply114.id])
        check("⑬ 已完成(20)任务行不被撤回改写",
              int(row[0]) == 20 and row[1] == "zhangsan", str(row))
        r = await facade_bc.flow("processTask/transfer", {"processTaskId": task114.id, "fromActor": "leader",
                                                          "toActor": "nobody", "operator": "leader"})
        check("⑬ 已撤回任务不可转办", r["code"] == 99999999 and "任务非进行中，不可转办" in r["msg"], str(r))

        inst115 = await eng.start_process_instance_by_id(DEFINE_ID, "zhangsan",
                                                         {"BUSINESS_NO": f"BIZ-{DB}-115"})
        _apply115, task115 = await _to_task1(inst115.id)
        await repo.add_task_actor(task115.id, ["zhaoliu"])  # 追加第三人：验证只摘 fromActor 那一行
        r = await facade_bc.flow("processTask/transfer", {"processTaskId": task115.id, "fromActor": "leader",
                                                          "toActor": "zhaoliu", "operator": "leader"})
        check("⑬ 目标人已是参与者 → 明确报错",
              r["code"] == 99999999 and "目标人已是该任务参与人" in r["msg"], str(r))
        r = await facade_bc.flow("processTask/transfer", {"processTaskId": task115.id, "fromActor": "leader",
                                                          "toActor": "lisi", "reason": "出差一周",
                                                          "operator": "intruder"})
        check("⑬ 越权转办被拒", r["code"] == 99999999 and "无权限转办该任务" in r["msg"], str(r))
        check("⑬ 越权失败后参与者零变动",
              await repo.find_task_actors(task115.id) == ["leader", "zhaoliu"],
              str(await repo.find_task_actors(task115.id)))
        r = await facade_bc.flow("processTask/transfer", {"processTaskId": task115.id, "fromActor": "leader",
                                                          "toActor": "lisi", "reason": "出差一周",
                                                          "operator": "leader"})
        check("⑬ 转办成功", r["code"] == 0, str(r))
        check("⑬ 只摘 fromActor 一行、其余参与人保留",
              await repo.find_task_actors(task115.id) == ["zhaoliu", "lisi"],
              str(await repo.find_task_actors(task115.id)))
        left = await raw_count(adapter, "SELECT COUNT(*) FROM wf_process_task_actor"
                                        " WHERE process_task_id = ? AND actor_id = 'leader'", [task115.id])
        check("⑬ 原办理人 actor 行确已从库中摘除", int(left) == 0, str(left))
        row = await _one("SELECT task_state, operator, variable, update_user FROM wf_process_task WHERE id=?",
                         [task115.id])
        tv = json.loads(row[2]) if row[2] else {}
        check("⑬ 任务不新建（同一行仍 DOING=10）+ operator 列恒无值（契约 06 §transfer⚠️ 严禁覆写）"
              " + update_user=操作人",
              int(row[0]) == 10 and (row[1] in (None, "")) and row[3] == "leader",
              str(row[:2] + (row[3],)))
        check("⑬ submitType=7 + tf_transferTo/tf_transferReason 落任务变量",
              int(tv.get("submitType", -1)) == 7 and tv.get("tf_transferTo") == "lisi"
              and tv.get("tf_transferReason") == "出差一周", str(row[2]))
        check("⑬ 审批记录文案可读「A 转办给 B（原因）」",
              "leader 转办给 lisi" in str(tv.get("tf_approvalComment", ""))
              and "出差一周" in str(tv.get("tf_approvalComment", "")), str(tv.get("tf_approvalComment")))
        led115 = tv.get("tf_transferHistory") or []
        check("⑬ 留痕三件之②：单跳也落 1 条 tf_transferHistory（六字段齐全）",
              len(led115) == 1 and led115[0].get("submitType") == 7
              and led115[0].get("fromActor") == "leader" and led115[0].get("toActor") == "lisi"
              and led115[0].get("reason") == "出差一周" and led115[0].get("operator") == "leader",
              json.dumps(led115, ensure_ascii=False))
        rec = await facade_bc.flow("processInstance/approvalRecord", {"id": inst115.id})
        rrow = [x for x in rec["data"] if x["taskName"] == "task1"][0]
        check("⑬ approvalRecord 读回 submitType=7",
              int((rrow["ext"] or {}).get("submitType", -1)) == 7, str(rrow["ext"]))
        rows_lisi, _ = await repo.page_todo_tasks(1, 100, "lisi")
        check("⑬ 接手人 B 待办出现该单（同一 taskId）",
              task115.id in [t.id for t in rows_lisi], str([t.id for t in rows_lisi]))
        rows_leader, _ = await repo.page_todo_tasks(1, 100, "leader")
        check("⑬ 原办理人 A 待办消失", task115.id not in [t.id for t in rows_leader],
              str([t.id for t in rows_leader]))

        # ── ⑬ 契约 06 §transfer 留痕⚠️（Node 实测复现的冒单缺陷，SQL 落库版）：
        #    转办→撤回后 operator 列仍无值，被摘走的 leader / 未办的 lisi 的「我已办」不得冒入该单 ──
        r = await facade_bc.flow("processInstance/withdraw", {"id": inst115.id, "operator": "zhangsan"})
        check("⑬ 发起人撤回已转办的单", r["code"] == 0, str(r))
        row = await _one("SELECT task_state, operator FROM wf_process_task WHERE id=?", [task115.id])
        check("⑬ 撤回后任务落 30 且 operator 列仍恒无值",
              int(row[0]) == 30 and (row[1] in (None, "")), str(row))
        for who in ("leader", "lisi"):
            drows, _ = await repo.page_done_tasks(1, 100, who)
            check(f"⑬ 转办→撤回后 {who} 的已办列表不冒入该单（他从没办过）",
                  task115.id not in [t.id for t in drows], str([t.id for t in drows]))

        # ── ⑭ issues/115 契约 06 §4 之②（fc0883a）：tf_transferHistory 追加式账本——SQL 仓跨跳 + 办结后存活 ──
        # 断言形状纪律：每步都直查 wf_process_task.variable 读回 JSON，不看内存对象、不看列表空不空。
        from datetime import datetime as _dt
        inst116 = await eng.start_process_instance_by_id(DEFINE_ID, "zhangsan",
                                                         {"BUSINESS_NO": f"BIZ-{DB}-116"})
        _apply116, task116 = await _to_task1(inst116.id, "⑭")
        hops = [("leader", "lisi", "出差一周"), ("lisi", "wangwu", "李四也不在，转王五")]
        for src, dst, why in hops:
            r = await facade_bc.flow("processTask/transfer", {"processTaskId": task116.id,
                                                              "fromActor": src, "toActor": dst,
                                                              "reason": why, "operator": src})
            check(f"⑭ {src}→{dst} 转办成功", r["code"] == 0, str(r))
        row = await _one("SELECT variable FROM wf_process_task WHERE id=?", [task116.id])
        led = json.loads(row[0]).get("tf_transferHistory") or []
        check("⑭ 两跳读回两条（追加不覆盖）", len(led) == 2, json.dumps(led, ensure_ascii=False))
        ok_fields = len(led) == 2 and all(
            sorted(h) == sorted(("submitType", "fromActor", "toActor", "reason", "time", "operator"))
            and (h["submitType"], h["fromActor"], h["toActor"], h["reason"], h["operator"])
            == (7, src, dst, why, src)
            for h, (src, dst, why) in zip(led, hops))
        check("⑭ 两条账本逐字段正确（六键 camelCase + submitType=7 + 办理人=该跳 fromActor）",
              ok_fields, json.dumps(led, ensure_ascii=False))
        check("⑭ time 为 yyyy-MM-dd HH:mm:ss 串（datetime 不进 JSON，否则 json.dumps 直接炸）",
              all(_dt.strptime(h.get("time", ""), "%Y-%m-%d %H:%M:%S") for h in led) if len(led) == 2
              else False, str([h.get("time") for h in led]))
        r = await facade_bc.flow("processTask/execute", {"processTaskId": task116.id,
                                                         "operator": "wangwu", "submitType": 1,
                                                         "tf_approvalComment": "已核实，同意"})
        check("⑭ 接手人 C 办结成功", r["code"] == 0, str(r))
        row = await _one("SELECT task_state, operator, variable FROM wf_process_task WHERE id=?",
                         [task116.id])
        tv = json.loads(row[2])
        check("⑭ 办结后槽位被 C 的提交覆盖（task_state=20 / submitType=1 / operator=wangwu，契约明说属预期）",
              int(row[0]) == 20 and int(tv.get("submitType", -1)) == 1 and row[1] == "wangwu",
              f"{row[0]}|{tv.get('submitType')}|{row[1]}")
        check("⑭ 账本在 C 办结后仍是两条且逐字段不变（实测：execute 的 vars_ 含 **task.variables，合并非整体替换）",
              len(tv.get("tf_transferHistory") or []) == 2 and tv.get("tf_transferHistory") == led,
              json.dumps(tv.get("tf_transferHistory"), ensure_ascii=False))
        check("⑭ C 自己的 tf_approvalComment 按 args 最高优先级覆盖末跳文案",
              tv.get("tf_approvalComment") == "已核实，同意", str(tv.get("tf_approvalComment")))
        st = await raw_count(adapter, "SELECT state FROM wf_process_instance WHERE id=?", [inst116.id])
        check("⑭ 实例办结落库 20", int(st) == 20, str(st))
        rec = await facade_bc.flow("processInstance/approvalRecord", {"id": inst116.id})
        rrow = [x for x in rec["data"] if x["taskName"] == "task1"][0]
        check("⑭ 审批历史读得到两跳账本（转办事实不因办结消失）",
              [h.get("toActor") for h in (rrow["ext"] or {}).get("tf_transferHistory") or []]
              == ["lisi", "wangwu"], str(rrow["ext"]))

        # ── ⑮ issues/116 批次 D：委托代理运行期自动生效 + 查询四判据（SQL 仓侧）──
        # 契约：06-facade §4.5「运行期语义」六条 / 05-spi SurrogateInterceptor / 08-compliance 用例 26+27。
        # 断言形状纪律：读回 wf_process_task_actor 真实行与仓储读回值，不看"返回码 0"；
        # 判据组与 spec_test 内存仓同名同数据同答案（同栈两仓结论不同即缺陷）。
        from datetime import timedelta
        from jeeflow.extensions import EngineExtensions
        from jeeflow.surrogate import NullSurrogateApplier

        now_dt = datetime.now()
        srg_ids = []
        _srg_seq = [DEFINE_ID * 1000]

        async def add_srg(pn, op, agent, start=None, end=None, enabled=1):
            """建一条委托台账行（显式 id 便于清理 + 决定"最新一条"次序）"""
            _srg_seq[0] += 1
            s = ProcessSurrogate(id=_srg_seq[0], processName=pn, operator=op, surrogate=agent,
                                 startTime=start, endTime=end, enabled=enabled,
                                 createUser="t", updateUser="t")
            await ext_repo.save_surrogate(s)
            srg_ids.append(s.id)
            return s

        # ⑮.1 判据①：空 processName 全流程兜底 + 精确优先 + 多条命中取最新（对齐内存仓）
        await add_srg("", "py-c1", "g1")
        hit = await ext_repo.get_surrogate("py-c1", "any-flow")
        check("⑮-①空 processName 全流程兜底", hit is not None and hit.surrogate == "g1",
              str(hit.surrogate if hit else None))
        await add_srg("leave", "py-c1", "exact")
        hit = await ext_repo.get_surrogate("py-c1", "leave")
        check("⑮-①精确命中优先于兜底", hit is not None and hit.surrogate == "exact",
              str(hit.surrogate if hit else None))
        await add_srg("", "py-c1", "g-new")
        hit = await ext_repo.get_surrogate("py-c1", "other-flow")
        check("⑮-①多条兜底取最新一条（ORDER BY id DESC，与内存仓同答案）",
              hit is not None and hit.surrogate == "g-new", str(hit.surrogate if hit else None))

        # ⑮.2 判据②：时间窗 start<=now<=end，任一侧 NULL = 该侧不限
        await add_srg("w1", "py-c2", "future", start=now_dt + timedelta(days=2),
                      end=now_dt + timedelta(days=3))
        check("⑮-②未到窗不生效", await ext_repo.get_surrogate("py-c2", "w1") is None)
        await add_srg("w2", "py-c2", "past", start=now_dt - timedelta(days=5),
                      end=now_dt - timedelta(days=4))
        check("⑮-②已过窗不生效", await ext_repo.get_surrogate("py-c2", "w2") is None)
        await add_srg("w3", "py-c2", "open-end", start=now_dt - timedelta(days=1))
        hit = await ext_repo.get_surrogate("py-c2", "w3")
        check("⑮-②end 为 NULL = 该侧不限", hit is not None and hit.surrogate == "open-end",
              str(hit.surrogate if hit else None))
        await add_srg("w4", "py-c2", "open-start", end=now_dt + timedelta(days=1))
        hit = await ext_repo.get_surrogate("py-c2", "w4")
        check("⑮-②start 为 NULL = 该侧不限", hit is not None and hit.surrogate == "open-start",
              str(hit.surrogate if hit else None))
        await add_srg("w5", "py-c2", "both-null")
        check("⑮-②两侧均 NULL = 不限",
              (await ext_repo.get_surrogate("py-c2", "w5")).surrogate == "both-null")

        # ⑮.3 判据③：自委托过滤（精确与兜底两路都要滤；内存仓此前漏此判据）
        await add_srg("self", "py-c3", "py-c3")
        check("⑮-③精确路径自委托不生效", await ext_repo.get_surrogate("py-c3", "self") is None)
        await add_srg("", "py-c3", "py-c3")
        check("⑮-③兜底路径自委托同样不生效", await ext_repo.get_surrogate("py-c3", "any-flow") is None)

        # ⑮.4 判据④：enabled 只认 1（脏值不得当启用；INT 列存不进文本，脏值方向由门面写入侧兜）
        for dirty, label in ((0, "零停用"), (None, "NULL非启用"), (2, "非1整数")):
            await add_srg("en-" + label, "py-c4", "agent", enabled=dirty)
            check(f"⑮-④{label}不生效", await ext_repo.get_surrogate("py-c4", "en-" + label) is None)
        await add_srg("en-on", "py-c4", "agent", enabled=1)
        check("⑮-④enabled=1 生效",
              (await ext_repo.get_surrogate("py-c4", "en-on")).surrogate == "agent")
        facade_srg = JeeflowFacade(eng, repo, ext_repo)  # 顺带把扩展仓储接入引擎（零配置默认生效）
        rd = await facade_srg.flow("processSurrogate/save",
                                   {"operator": "py-c4", "surrogate": "agent", "processName": "en-dirty",
                                    "enabled": "abc"})
        check("⑮-④门面存脏值不报错", rd["code"] == 0, str(rd))
        srg_ids.append(int(rd["data"]["id"]))
        rdet = await facade_srg.flow("processSurrogate/detail", {"id": rd["data"]["id"]})
        check("⑮-④脏值「abc」落库为停用（detail 读回值）",
              rdet["data"]["enabled"] == 0, str(rdet["data"]["enabled"]))
        check("⑮-④脏值委托查询不生效", await ext_repo.get_surrogate("py-c4", "en-dirty") is None)

        # ⑮.5 运行期自动生效：一条正例 + 三条同 operator 的负例同时压在 task1 参与者上
        # ⚠️ 插入顺序是**刻意的**（issues/123 / 06 §4.5 条款 1.4）：三条负例先插、正例最后插 ⇒
        #   该作用域的"最新一条"就是那条窗内 + enabled=1 的正例。反过来（正例在最旧）在
        #   条款 1.4 的新顺序下会被最新那条无效记录裁决成"不生效"，正例格子就成了假红；
        #   "最新一条无效 ⇒ 压过旧的窗内有效记录"这一形另有 ⑮.5b 与 ⑯ 的 veto 组各钉一遍。
        win_start, win_end = now_dt - timedelta(hours=1), now_dt + timedelta(hours=1)
        await add_srg("simple", "leader", "py-off", enabled=0)                             # 负例：停用
        await add_srg("simple", "leader", "py-future", start=now_dt + timedelta(days=2),
                      end=now_dt + timedelta(days=3))                                       # 负例：窗外
        await add_srg("simple", "leader", "leader")                                          # 负例：自委托
        await add_srg("simple", "leader", "py-agent", start=win_start, end=win_end)      # 正例（最新一条，键=流程模型 name）
        await add_srg("py-simple", "zhangsan", "py-decoy")  # 诱饵：流程定义 name ≠ 模型 name（键取模型 name，故不命中）
        r116 = await facade_srg.flow("processInstance/startAndExecute",
                                     {"processDefineId": DEFINE_ID, "operator": "zhangsan"})
        check("⑮ 配好委托后建单成功", r116["code"] == 0, str(r116))
        iid116 = int(r116["data"]["processInstanceId"])
        doing116 = [t for t in await repo.find_doing_tasks(iid116) if t.taskName == "task1"]
        check("⑮ 存在 task1 进行中任务", len(doing116) == 1, str([t.taskName for t in doing116]))
        task116 = doing116[0]
        actors116 = await repo.find_task_actors(task116.id)
        check("⑮ 代理人随任务并入参与者集合（授权人保留在后，停用/窗外/自委托三条不进）",
              actors116 == ["leader", "py-agent"], str(actors116))
        n_agent = await raw_count(adapter, "SELECT COUNT(*) FROM wf_process_task_actor"
                                           " WHERE process_task_id = ? AND actor_id = ?",
                                  [task116.id, "py-agent"])
        check("⑮ wf_process_task_actor 真落代理人那一行（反 Java 首版空 taskId 静默无效）",
              int(n_agent) == 1, str(n_agent))
        n_leader = await raw_count(adapter, "SELECT COUNT(*) FROM wf_process_task_actor"
                                            " WHERE process_task_id = ? AND actor_id = ?",
                                   [task116.id, "leader"])
        check("⑮ 授权人那一行仍在（委托不是转办，任一可办）", int(n_leader) == 1, str(n_leader))
        n_decoy = await raw_count(adapter, "SELECT COUNT(*) FROM wf_process_task_actor"
                                           " WHERE process_task_id = ? AND actor_id = ?",
                                  [task116.id, "py-decoy"])
        check("⑮ 停用/窗外/自委托/诱饵名(define name) 四者均无 actor 行", int(n_decoy) == 0, str(n_decoy))
        rows_agent, _ = await repo.page_todo_tasks(1, 100, "py-agent")
        check("⑮ 代理人待办分页读得到该单", task116.id in [t.id for t in rows_agent],
              str([t.id for t in rows_agent]))
        rows_leader, _ = await repo.page_todo_tasks(1, 100, "leader")
        check("⑮ 授权人待办分页仍在（未被顶掉）", task116.id in [t.id for t in rows_leader],
              str([t.id for t in rows_leader]))
        # 委托查询按「流程模型 name」（此处 simple，define name=py-simple）：apply 节点不受诱饵影响
        apply116 = [t for t in await repo.find_history_tasks(iid116) if t.taskName == "apply"][0]
        check("⑮ 按流程模型 name 查委托：诱饵（define name）不追加到 apply 参与者",
              await repo.find_task_actors(apply116.id) == ["zhangsan"],
              str(await repo.find_task_actors(apply116.id)))

        # ⑮.5b issues/123 条款 1.4（SQL 仓侧运行期 veto）：在同一 (operator, 流程名) 作用域再压
        #   一条更"新"的停用记录 ⇒ 旧的窗内有效记录 py-agent **不得被复活**，新单参与者只剩授权人。
        #   旧形状（SQL 先滤 enabled/窗口/自委托，剩下的才 ORDER BY id DESC）会把 py-agent 捞回来 ⇒ 红。
        #   查完按 id 删掉，⑮.5 的正例形状与 ⑮.7 的"台账 4 条"计数都不被带坏。
        veto_row = await add_srg("simple", "leader", "py-veto-off", enabled=0)
        r_veto = await facade_srg.flow("processInstance/startAndExecute",
                                       {"processDefineId": DEFINE_ID, "operator": "zhangsan"})
        check("⑮.5b 压上停用那条后建单成功", r_veto["code"] == 0, str(r_veto))
        _t_veto = [t for t in await repo.find_doing_tasks(int(r_veto["data"]["processInstanceId"]))
                   if t.taskName == "task1"][0]
        actors_veto = await repo.find_task_actors(_t_veto.id)
        check("⑮.5b 最新一条不生效 ⇒ 压过旧的窗内有效委托（SQL 仓侧不得复活 py-agent）",
              actors_veto == ["leader"], str(actors_veto))
        await ext_repo.remove_surrogate(veto_row.id)
        check("⑮.5b veto 行按 id 清掉（回到 ⑮.5 正例形状）",
              await ext_repo.find_surrogate_by_id(veto_row.id) is None)

        # ⑮.6 未配置扩展仓储：建单不被打断（缺仓储属正常部署形态，不得抛"未配置扩展仓储"）
        eng_noext = EngineImpl(repo, TestUserProv(), TestIDGen())
        facade_noext = JeeflowFacade(eng_noext, repo, None)
        r_no = await facade_noext.flow("processInstance/startAndExecute",
                                       {"processDefineId": DEFINE_ID, "operator": "zhangsan"})
        check("⑮ 未配扩展仓储建单成功（不打断）", r_no["code"] == 0, str(r_no))
        _t_no = [t for t in await repo.find_doing_tasks(int(r_no["data"]["processInstanceId"]))
                 if t.taskName == "task1"][0]
        check("⑮ 缺仓储时参与者原样（无委托应用）",
              await repo.find_task_actors(_t_no.id) == ["leader"],
              str(await repo.find_task_actors(_t_no.id)))
        # 引擎级异常兜底：数据源抛错也不打断建单（委托是增强能力）
        class _BoomExt:
            async def get_surrogate(self, *a, **kw):
                raise RuntimeError("模拟扩展仓储不可用")

        eng_boom = EngineImpl(repo, TestUserProv(), TestIDGen())
        eng_boom.set_extensions(EngineExtensions(ext_repository=_BoomExt()))
        r_boom = await eng_boom.start_process_instance_by_id(
            DEFINE_ID, "zhangsan", {"BUSINESS_NO": f"BIZ-{DB}-boom"})
        doing_boom = [t for t in await repo.find_doing_tasks(r_boom.id) if t.taskName == "apply"]
        check("⑮ 扩展仓储抛错时建单不被打断（异常只记录不外溢，参与者原样）",
              len(doing_boom) == 1 and await repo.find_task_actors(doing_boom[0].id) == ["zhangsan"],
              str([t.taskName for t in doing_boom]))

        # ⑮.7 显式关闭：① 配置开关（已接入的扩展仓储须跨 set_extensions 保留）
        eng.set_extensions(EngineExtensions(surrogate_enabled=False))
        check("⑮ set_extensions 保留门面接入的扩展仓储", eng.ext.ext_repository is ext_repo)
        r_off = await facade_srg.flow("processInstance/startAndExecute",
                                      {"processDefineId": DEFINE_ID, "operator": "zhangsan"})
        check("⑮ 开关关闭后建单成功", r_off["code"] == 0, str(r_off))
        _t_off = [t for t in await repo.find_doing_tasks(int(r_off["data"]["processInstanceId"]))
                  if t.taskName == "task1"][0]
        check("⑮ 开关关闭 → 回到仅台账（代理人不进参与者）",
              await repo.find_task_actors(_t_off.id) == ["leader"],
              str(await repo.find_task_actors(_t_off.id)))
        rows_pg, total_pg = await ext_repo.page_surrogates(filters={"operator": "leader"})
        check("⑮ 关闭的是运行期应用，台账照旧查得到", total_pg == 4,
              f"total={total_pg} rows={[r.surrogate for r in rows_pg]}")
        # ⑮.7b 显式关闭：② 注册空实现
        eng.set_extensions(EngineExtensions(ext_repository=ext_repo,
                                            surrogate_applier=NullSurrogateApplier()))
        r_null = await facade_srg.flow("processInstance/startAndExecute",
                                       {"processDefineId": DEFINE_ID, "operator": "zhangsan"})
        _t_null = [t for t in await repo.find_doing_tasks(int(r_null["data"]["processInstanceId"]))
                   if t.taskName == "task1"][0]
        check("⑮ 注册空实现同样回到仅台账", await repo.find_task_actors(_t_null.id) == ["leader"],
              str(await repo.find_task_actors(_t_null.id)))

        # ── ⑯ issues/116 批次 D 收尾：条款 1.4「多条命中取 id 最大」的**打乱序夹具**对拍
        #    （SQL 仓侧；内存仓侧同一份数据 + 同一份期望见 spec_test 同名用例，
        #     数据与期望单点维护在 tests/surrparity.py）
        # ⚠️ 已知事实（Java/Go 实测踩到）：SQL 侧 "打乱 id 序" **没有判别力**——InnoDB 对
        #    `WHERE operator=? ORDER BY id DESC` 本就按主键序回行，插入序在结果里根本不出现，
        #    打乱与否答案都一样。真正钉住条款 1.4 的是 SQL 里的 `ORDER BY id DESC` 子句本身
        #    （去掉它就退化成"取物理首行"）；打乱序起作用的是内存侧。两侧仍各跑一遍并对同一答案负责。
        try:
            from tests import surrparity          # 以仓根为 sys.path 跑
        except ImportError:                       # pragma: no cover
            import surrparity                     # 从 tests/ 目录直跑
        parity_base = DEFINE_ID * 1000 + 1000     # Python 栈自己的段（避开 ⑮ 的 DEFINE_ID*1000+n）
        _now_ms = now_dt.replace(microsecond=(now_dt.microsecond // 1000) * 1000)  # 与 DATETIME(3) 同精度

        def _rep(desc, ok, detail=""):
            return check(f"⑯ {desc}", ok, detail)

        parity_ids = await surrparity.run_parity(ext_repo, _now_ms, parity_base, _rep)
        srg_ids.extend(parity_ids)
        # 读回库里真值自证：期望行确实按显式 id 落在 wf_process_surrogate（含 process_name IS NULL 行）。
        # 条数由**共用夹具**推导（不在此另造一份期望），SQL 侧真存 NULL 才算这行落库成功。
        null_ids = [parity_base + r.id_off for r in surrparity.rows() if r.process_name is None]
        n_null_flow = await raw_count(
            adapter, "SELECT COUNT(*) FROM wf_process_surrogate WHERE process_name IS NULL AND id IN ("
            + ",".join(["?"] * len(null_ids)) + ")", null_ids)
        check("⑯ process_name=NULL 的兜底行真落库（SQL 侧 NULL 与 '' 同属兜底）",
              int(n_null_flow) == len(null_ids), f"{n_null_flow}/{len(null_ids)}")

        # ── ⑰ issues/142 B 批（spec 06-facade.md §2.11）：任务参与者写侧归属值归一——真库取证
        #    本栈普查实读（issues/142 §2 B 表 python 行）：门面/引擎的**数组腿** `[str(x) for x in v]`
        #    不 trim、不丢空、None 串化成字符串 "None"，逗号串腿才 strip＋过滤；两仓 add_task_actor
        #    **判重不判空** ⇒ ""/"  "/None 全放行。owner 拍「两形同判据＋写侧兜底＋trim＋哨兵」。
        #    断言纪律同 ⑬：一律直读 wf_process_task_actor / wf_process_cc_instance 的真实行，
        #    不看"返回码 0"——返回码 0 但把脏值灌进归属列，正是本案要钉死的形状。
        async def _actors_of(task_id):
            return [r[0] for r in await raw_rows(
                adapter, "SELECT actor_id FROM wf_process_task_actor WHERE process_task_id=?"
                         " ORDER BY id ASC", [task_id])]

        async def _exec(sql, args=()):
            conn = await adapter.acquire()
            try:
                await conn.execute(sql_of(adapter, sql), args)
            finally:
                await adapter.release(conn)

        inst142 = await eng.start_process_instance_by_id(
            DEFINE_ID, "zhangsan", {"BUSINESS_NO": f"BIZ-{DB}-142b"})
        _a142, t142 = await _to_task1(inst142.id, tag="⑰")
        check("⑰ 基线：task1 参与者为 leader（真库行）", await _actors_of(t142.id) == ["leader"],
              str(await _actors_of(t142.id)))

        # ⑰.1 门面 addCandidate：数组腿与逗号串腿**同一判据**（trim／丢空／折叠＋哨兵 "0"）
        r = await facade_bc.flow("processTask/addCandidate",
                                 {"processTaskId": t142.id,
                                  "actorIds": [" 142a ", "", "  ", None, "142a", "0"]})
        check("⑰ addCandidate 数组腿归一后 code=0", r["code"] == 0, str(r))
        check("⑰ 真落库＝trim＋丢空＋同次折叠＋哨兵 '0'（无 ''/'  '/'None' 行）",
              await _actors_of(t142.id) == ["leader", "142a", "0"], str(await _actors_of(t142.id)))
        r = await facade_bc.flow("processTask/surrogate",
                                 {"processTaskId": t142.id, "actorIds": " 142b ,, 142c , 142b"})
        check("⑰ surrogate 逗号串形同判据（两形两把尺子＝本案主病灶）",
              r["code"] == 0 and await _actors_of(t142.id) == ["leader", "142a", "0", "142b", "142c"],
              str(await _actors_of(t142.id)))

        # ⑰.2 空入参档：丢完为空 ⇒ 与既有"空 actorIds"同档报错（不新造码/文案）＋零副作用
        for bad in ([""], ["  "], [None], "", "   ", " , "):
            r = await facade_bc.flow("processTask/addCandidate",
                                     {"processTaskId": t142.id, "actorIds": bad})
            if not check(f"⑰ actorIds={bad!r} 与空 actorIds 同档报错",
                         r["code"] == 99999999 and "actorIds 缺失" in r["msg"], str(r)):
                break
        check("⑰ 空档零副作用：参与者一条没变",
              await _actors_of(t142.id) == ["leader", "142a", "0", "142b", "142c"],
              str(await _actors_of(t142.id)))
        n_blank = await raw_count(
            adapter, "SELECT COUNT(*) FROM wf_process_task_actor a JOIN wf_process_task t"
                     " ON a.process_task_id = t.id WHERE t.process_instance_id = ?"
                     " AND (a.actor_id = '' OR a.actor_id IS NULL OR TRIM(a.actor_id) = '')",
            [inst142.id])
        check("⑰ 本实例零空归属值行（''／纯空白／NULL 一律没灌进去）", int(n_blank) == 0, str(n_blank))

        # ⑰.3 主键另判一档：processTaskId 缺失/空串/0 ⇒ 响亮报错，不得拿 ''/0 当 id 落库
        for bad_id in (0, "", "   ", None, "0"):
            r = await facade_bc.flow("processTask/addCandidate",
                                     {"processTaskId": bad_id, "actorIds": ["142x"]})
            if not check(f"⑰ 主键 {bad_id!r} 缺失/非法必须报错",
                         r["code"] == 99999999 and "processTaskId" in r["msg"], str(r)):
                break
        raised = 0
        for bad_id in (0, "", "   ", None, "0"):
            try:
                await repo.add_task_actor(bad_id, ["142x"])
            except ValueError:
                raised += 1
        check("⑰ 仓储写侧同样挡空主键（绕过门面直连仓储也报 ValueError）", raised == 5, f"{raised}/5")

        # ⑰.4 transfer：fromActor/toActor 归一后再用（权限比较／摘加落库／留痕三处都吃 trim）
        r = await facade_bc.flow("processTask/transfer",
                                 {"processTaskId": t142.id, "fromActor": " 142a ",
                                  "toActor": " 142d ", "operator": "142a"})
        check("⑰ transfer：' 142a ' 与 '142a' 是同一个人（归一后才判得对）", r["code"] == 0, str(r))
        check("⑰ transfer 摘/加都写 trim 后的值",
              await _actors_of(t142.id) == ["leader", "0", "142b", "142c", "142d"],
              str(await _actors_of(t142.id)))
        row142 = await _one("SELECT variable FROM wf_process_task WHERE id=?", [t142.id])
        tv142 = json.loads(row142[0]) if row142[0] else {}
        hop142 = (tv142.get("tf_transferHistory") or [{}])[-1]
        check("⑰ 留痕 tf_transferTo／账本条目都是 trim 后的值",
              tv142.get("tf_transferTo") == "142d" and hop142.get("fromActor") == "142a"
              and hop142.get("toActor") == "142d", json.dumps(tv142, ensure_ascii=False)[:200])

        # ⑰.5 消费腿 tf_nextNodeOperator 数组形（engine._resolve_actors 那把第二尺子）
        inst142c = await eng.start_process_instance_by_id(
            DEFINE_ID, "zhangsan", {"BUSINESS_NO": f"BIZ-{DB}-142c"})
        apply142c = [t for t in await repo.find_doing_tasks(inst142c.id) if t.taskName == "apply"][0]
        await repo.add_task_actor(apply142c.id, ["zhangsan"])
        await eng.execute_process_task(apply142c.id, "zhangsan",
                                       {"submitType": 1,
                                        "tf_nextNodeOperator": [" 9001 ", "", None, "9001", "0"]})
        task142c = [t for t in await repo.find_doing_tasks(inst142c.id) if t.taskName == "task1"]
        check("⑰ nextNodeOperator 数组腿真落库同判据",
              len(task142c) == 1 and await _actors_of(task142c[0].id) == ["9001", "0"],
              str(await _actors_of(task142c[0].id)) if task142c else "无 task1")

        # ⑰.6 updateCCStatus 的 operator：归一后再比；空 operator 是 no-op，不碰历史脏行
        await repo.create_cc_instance(inst142.id, "zhangsan", " 142cc ")
        await _exec("INSERT INTO wf_process_cc_instance (id, process_instance_id, actor_id, state)"
                    " VALUES (?,?,?,0)", [9_430_110, inst142.id, ""])
        for blank in ("", "   ", "\t", None):
            await repo.update_cc_status(inst142.id, blank)
        states = {r[0]: r[1] for r in await raw_rows(
            adapter, "SELECT actor_id, state FROM wf_process_cc_instance WHERE process_instance_id=?",
            [inst142.id])}
        check("⑰ 空 operator ⇒ no-op（历史脏行 actor_id='' 不被批量打勾）",
              states == {"142cc": 0, "": 0}, str(states))
        await repo.update_cc_status(inst142.id, " 142cc ")
        states = {r[0]: r[1] for r in await raw_rows(
            adapter, "SELECT actor_id, state FROM wf_process_cc_instance WHERE process_instance_id=?",
            [inst142.id])}
        check("⑰ ' 142cc ' 归一后打得上已读，脏行仍不动",
              states == {"142cc": 1, "": 0}, str(states))
        await _exec("DELETE FROM wf_process_cc_instance WHERE id = ?", [9_430_110])

        # ── ⑱ issues/137 A · 裁定 A（批二 §3-4）：实例行 expire_time 的**真库腿** ──────────
        # 病灶形状：本栈发起腿**一处都没给** wf_process_instance.expire_time 赋过值（该列恒 NULL），
        # 本轮补的是"定义顶层 expireTime 表达式 → 求值 → 写实例行"这一处写点（基准＝java
        # JeeflowEngineImpl.java:93-96 ＋ boot2 ProcessInstanceServiceImpl.java:157-160）。
        # 这一段独有的卖点＝**内存绿 ≠ 落库绿**，两条只能在真库上证：
        #   ① 列里进的必须是**求值结果（DATETIME 时刻）**，不是表达式原串——160 这台 MySQL 的
        #      @@sql_mode 含 STRICT_TRANS_TABLES，把 '2h' 绑进 DATETIME(3) 列是**服务端硬错**
        #      （兄弟栈 rust/php 本轮实测服务端给 1292 / 22007
        #      "Incorrect datetime value: '2h' for column 'expire_time'"），整条发起腿直接炸；
        #      SQLite/内存仓两种假仓都不会报这个错，所以 T0 全绿也可能真库红。
        #   ② 定义没配／算不出 ⇒ 列必须 **IS NULL**（不是空串、不是 now()）。
        # 夹具自带定义行（id 段与既有各段错开），**根上**配 expireTime、节点一律不配
        # ⇒ 实例那一列是这一段唯一变量，任务行的 expire_time 恒 NULL 不会互相冒充。
        from datetime import datetime as _dt137
        _now137 = _dt137.now()
        _NOKEY137 = object()      # 「根上不写 expireTime 键」这一档的哨兵

        def _flow137a(root, name: str) -> str:
            """start → approve(leader) → end，**根上**带/不带 expireTime（root=_NOKEY137 即不写键）"""
            raw = {"name": name, "displayName": "实例到期真库", "type": "approval",
                   "nodes": [
                       {"id": "start", "type": "snaker:start", "properties": {}, "text": {"value": "开始"}},
                       {"id": "approve", "type": "snaker:task",
                        "properties": {"assignee": "leader", "taskType": 0, "performType": 0},
                        "text": {"value": "approve"}},
                       {"id": "end", "type": "snaker:end", "properties": {}, "text": {"value": "结束"}}],
                   "edges": [
                       {"id": "e1", "sourceNodeId": "start", "targetNodeId": "approve", "properties": {}},
                       {"id": "e2", "sourceNodeId": "approve", "targetNodeId": "end", "properties": {}}]}
            if root is not _NOKEY137:
                raw["expireTime"] = root
            return json.dumps(raw, ensure_ascii=False)

        async def _seed137a(did: int, dname: str, content: str):
            await _exec("INSERT INTO wf_process_define (id, name, display_name, type, state, content,"
                        " version, create_time, create_user, update_time, update_user)"
                        " VALUES (?,?,?,?,1,?,1,?,?,?,?)",
                        [did, dname, "实例到期真库", "approval", content,
                         _now137, "py-test", _now137, "py-test"])

        async def _inst_cols137a(iid):
            """直查数据库取实例行的 (expire_time, create_time)——不看仓储返回体，只看库里的值"""
            rows = await raw_rows(adapter, "SELECT expire_time, create_time"
                                           " FROM wf_process_instance WHERE id=?", [iid])
            return (rows[0][0], rows[0][1]) if rows else (None, None)

        # 判据前提：这台库确实是严格模式（不是就如实报出来，别让"没报错"被当成证据）
        _mode = str((await raw_rows(adapter, "SELECT @@sql_mode", [])) [0][0])
        check("⑱ 前提：服务端 @@sql_mode 含 STRICT_TRANS_TABLES（原串进 datetime 列才会硬错）",
              "STRICT_TRANS_TABLES" in _mode.upper(), _mode)

        # 本段专用定义号段（mysql 900060–900065 / postgres 910060–910065），先清一遍再灌：
        # 上一轮若中途崩过（真库腿是**写库**操作），残留行会让本轮主键撞车 1062。
        _ids137 = [DEFINE_ID + n for n in (60, 61, 62, 63, 64, 65)]

        async def _purge137():
            for _did in _ids137:
                await _exec("DELETE FROM wf_process_task_actor WHERE process_task_id IN"
                            " (SELECT id FROM wf_process_task WHERE process_instance_id IN"
                            " (SELECT id FROM wf_process_instance WHERE process_define_id = ?))", [_did])
                await _exec("DELETE FROM wf_process_task WHERE process_instance_id IN"
                            " (SELECT id FROM wf_process_instance WHERE process_define_id = ?)", [_did])
                await _exec("DELETE FROM wf_process_cc_instance WHERE process_instance_id IN"
                            " (SELECT id FROM wf_process_instance WHERE process_define_id = ?)", [_did])
                await _exec("DELETE FROM wf_process_instance WHERE process_define_id = ?", [_did])
                await _exec("DELETE FROM wf_process_define WHERE id = ?", [_did])

        await _purge137()

        # ⑱.1 正向：根上配 "2h" ⇒ 真库列里是时刻，且同行 expire − create ≈ 2h
        await _seed137a(_ids137[0], "py-expire137a-rel", _flow137a("2h", "py-expire137a-rel"))
        _inst137_rel = None
        try:
            _inst137_rel = await eng.start_process_instance_by_id(_ids137[0], "zhangsan",
                                                                 {"BUSINESS_NO": f"BIZ-{DB}-137a"})
            _err137 = ""
        except Exception as e:
            _err137 = f"{type(e).__name__}: {e}"
        check("⑱ 发起腿真库不炸（把 '2h' 原串绑进 DATETIME(3) 列＝服务端硬错，见 rust/php 1292/22007）",
              _inst137_rel is not None, _err137)
        if _inst137_rel is not None:
            _exp137, _cre137 = await _inst_cols137a(_inst137_rel.id)
            check("⑱ 真库列存的是求值结果（datetime），不是表达式原串",
                  _exp137 is not None and not isinstance(_exp137, str), repr(_exp137))
            _delta137 = (_exp137 - _cre137).total_seconds() if (_exp137 and _cre137) else None
            check("⑱ 同行 expire − create ≈ 7200s（不是 now 占位的 0s）",
                  _delta137 is not None and 2 * 3600 - 5 <= _delta137 <= 2 * 3600 + 60,
                  f"{_delta137}s expire={_exp137} create={_cre137}")
            _back137 = await repo.find_instance_by_id(_inst137_rel.id)
            check("⑱ 仓储读回与直查列同一时刻（列真进了 SELECT 映射）",
                  _back137 is not None and _back137.expireTime == _exp137,
                  f"直查 {_exp137} / 读回 {getattr(_back137, 'expireTime', None)}")

        # ⑱.2 变量档真库腿：根上写变量名，发起参数给时刻文本 ⇒ 列里就是那一刻
        _d137_var = DEFINE_ID + 61
        await _seed137a(_d137_var, "py-expire137a-var", _flow137a("dueAt", "py-expire137a-var"))
        _want137 = _dt137(2026, 12, 31, 10, 0, 0)
        _inst137_var = await eng.start_process_instance_by_id(
            _d137_var, "zhangsan", {"dueAt": "2026-12-31 10:00:00"})
        _exp137v, _ = await _inst_cols137a(_inst137_var.id)
        check("⑱ 变量档吃**发起参数**：真库列＝2026-12-31 10:00:00",
              _exp137v == _want137, repr(_exp137v))

        # ⑱.3 没配 / 算不出 ⇒ 真库列 IS NULL（不写空串、不写 now）
        for _label, _root, _did in (("键缺失", _NOKEY137, DEFINE_ID + 62),
                                    ("空串", "", DEFINE_ID + 63),
                                    ("误配", "not-a-time", DEFINE_ID + 64),
                                    ("负数档", "-5h", DEFINE_ID + 65)):
            _content = _flow137a(_root, f"py-expire137a-{_did}")
            await _seed137a(_did, f"py-expire137a-n{_did}", _content)
            _inst137n = await eng.start_process_instance_by_id(_did, "zhangsan")
            _exp137n, _cre137n = await _inst_cols137a(_inst137n.id)
            check(f"⑱ {_label} 那一档真库列必须是 NULL（对照列 create_time 已落库）",
                  _exp137n is None and _cre137n is not None, repr(_exp137n))

        # ⑱ 收尾：本段自带的定义与实例一律按 define id 级联清掉（不碰别人的数据）
        await _purge137()

        # 清理本轮委托台账行（按显式 id，不碰别人的数据）
        conn = await adapter.acquire()
        try:
            for sid in srg_ids:
                await conn.execute(sql_of(adapter, "DELETE FROM wf_process_surrogate WHERE id = ?"),
                                   [sid])
        finally:
            await adapter.release(conn)
        left = 0
        for sid in srg_ids:
            left += int(await raw_count(adapter,
                                        "SELECT COUNT(*) FROM wf_process_surrogate WHERE id = ?",
                                        [sid]))
        check("⑮ 委托台账行按 id 清理完毕（零残留）", left == 0, f"残留 {left} 行 / 本轮 {len(srg_ids)} 行")

        # 清理测试残留
        await cleanup(adapter)
        conn = await adapter.acquire()
        try:
            await conn.execute(sql_of(adapter, "DELETE FROM wf_process_instance WHERE id = ?"),
                               [tx_inst_id])
            await conn.execute(sql_of(adapter,
                               "DELETE FROM wf_process_cc_instance WHERE process_instance_id = ?"),
                               [tx_inst_id])
        finally:
            await adapter.release(conn)
    finally:
        await close_pool(pool)

    print(f"\nPython JDBC 集成测试（{DB}）: {passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    asyncio.run(main())
