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
from jeeflow.spi import IDGenerator

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

FLOWS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "jeeflow-java",
                         "jeeflow-core", "src", "test", "resources", "flows")
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

        # ── ⑪ find_define_by_name（v1.1.0 deploy 版本管理用）──
        latest = await repo.find_define_by_name("py-simple")
        check("find_define_by_name 命中", latest is not None and latest.name == "py-simple",
              str(latest.name if latest else None))
        check("find_define_by_name 未命中", await repo.find_define_by_name("no-such-flow") is None)

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
