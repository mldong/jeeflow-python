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


class TestIDGen(IDGenerator):
    """时间戳 + 序号（避免与数据库已有 ID 冲突）"""

    def __init__(self):
        import time
        self.base = int(time.time() * 1000) * 1000
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
        win_start, win_end = now_dt - timedelta(hours=1), now_dt + timedelta(hours=1)
        await add_srg("py-simple", "leader", "py-agent", start=win_start, end=win_end)      # 正例
        await add_srg("py-simple", "leader", "py-off", enabled=0)                          # 负例：停用
        await add_srg("py-simple", "leader", "py-future", start=now_dt + timedelta(days=2),
                      end=now_dt + timedelta(days=3))                                       # 负例：窗外
        await add_srg("py-simple", "leader", "leader")                                      # 负例：自委托
        await add_srg("simple", "zhangsan", "py-decoy")  # 诱饵：content name ≠ 流程定义 name
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
        check("⑮ 停用/窗外/自委托/诱饵名 四者均无 actor 行", int(n_decoy) == 0, str(n_decoy))
        rows_agent, _ = await repo.page_todo_tasks(1, 100, "py-agent")
        check("⑮ 代理人待办分页读得到该单", task116.id in [t.id for t in rows_agent],
              str([t.id for t in rows_agent]))
        rows_leader, _ = await repo.page_todo_tasks(1, 100, "leader")
        check("⑮ 授权人待办分页仍在（未被顶掉）", task116.id in [t.id for t in rows_leader],
              str([t.id for t in rows_leader]))
        # 委托查询按「流程定义 name」（此处 py-simple，content name=simple）：apply 节点不受诱饵影响
        apply116 = [t for t in await repo.find_history_tasks(iid116) if t.taskName == "apply"][0]
        check("⑮ 按流程定义 name 查委托：诱饵（content name）不追加到 apply 参与者",
              await repo.find_task_actors(apply116.id) == ["zhangsan"],
              str(await repo.find_task_actors(apply116.id)))

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
