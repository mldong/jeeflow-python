"""JDBC（MySQL）仓储集成测试——对齐 Go jdbc_test。

前置条件：
  - 开发服务器 MySQL（192.168.1.160:3306，库 jeeflow）
  - 5 张 wf_* 表已建
测试数据固定 define ID=900002，开头清理，可重复执行。
"""
import asyncio
import json
import os
import sys

import aiomysql

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl
from jeeflow.jdbc import JdbcRepository, TsIDGenerator
from jeeflow.model import ProcessDefine, InstanceState, TaskState, UserInfo
from jeeflow.spi import IDGenerator

DSN = dict(host="192.168.1.160", port=3306, user="root", password="8Eli#gr#AUk",
           db="jeeflow", charset="utf8mb4", autocommit=True, maxsize=5)
DEFINE_ID = 900002

FLOWS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "jeeflow-java",
                         "jeeflow-core", "src", "test", "resources", "flows")

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


async def cleanup(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM wf_process_task_actor WHERE process_task_id IN"
                " (SELECT id FROM wf_process_task WHERE process_instance_id IN"
                " (SELECT id FROM wf_process_instance WHERE process_define_id = %s))", (DEFINE_ID,))
            await cur.execute(
                "DELETE FROM wf_process_cc_instance WHERE process_instance_id IN"
                " (SELECT id FROM wf_process_instance WHERE process_define_id = %s)", (DEFINE_ID,))
            await cur.execute(
                "DELETE FROM wf_process_task WHERE process_instance_id IN"
                " (SELECT id FROM wf_process_instance WHERE process_define_id = %s)", (DEFINE_ID,))
            await cur.execute("DELETE FROM wf_process_instance WHERE process_define_id = %s", (DEFINE_ID,))
            await cur.execute("DELETE FROM wf_process_define WHERE id = %s", (DEFINE_ID,))


def load_flow(name):
    with open(os.path.join(FLOWS_DIR, name), encoding="utf-8") as f:
        return f.read()


async def main():
    pool = await aiomysql.create_pool(**DSN)
    try:
        await cleanup(pool)
        repo = JdbcRepository(pool, TsIDGenerator())
        eng = EngineImpl(repo, TestUserProv(), TestIDGen())

        # ── ① 插入流程定义（01-simple：start→apply(applicant)→task1(leader)→end）
        content = load_flow("01-simple.json")
        raw = json.loads(content)
        define = ProcessDefine(id=DEFINE_ID, name="py-simple", displayName=raw["displayName"],
                               type=raw["type"], state=1, content=content, version=1)
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                from datetime import datetime
                now = datetime.now()
                await cur.execute(
                    "INSERT INTO wf_process_define (id, name, display_name, type, state, content,"
                    " version, create_time, create_user, update_time, update_user)"
                    " VALUES (%s,%s,%s,%s,1,%s,1,%s,%s,%s,%s)",
                    (define.id, define.name, define.displayName, define.type, define.content,
                     now, "py-test", now, "py-test"))

        # ── ② 启动：start → apply（applicant）
        inst = await eng.start_process_instance_by_id(DEFINE_ID, "zhangsan",
                                                      {"amount": "1000", "BUSINESS_NO": "BIZ-PY-001"})
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

        # ── ⑤ 重新连接验证持久化
        pool2 = await aiomysql.create_pool(**DSN)
        try:
            repo2 = JdbcRepository(pool2, TsIDGenerator())
            inst2 = await repo2.find_instance_by_id(inst.id)
            check("重新加载实例状态完成", inst2 is not None and inst2.state == InstanceState.DONE,
                  str(inst2.state if inst2 else None))
            check("变量 amount 持久化", inst2 and inst2.variables.get("amount") == "1000",
                  str(inst2.variables if inst2 else None))
            hist = await repo2.find_history_tasks(inst.id)
            check("历史任务 2 条", len(hist) == 2, str(len(hist)))
            check("任务参与者关系持久化", all(len(t.actorIds) > 0 for t in hist),
                  str([t.actorIds for t in hist]))
            async with pool2.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT state FROM wf_process_instance WHERE id = %s", (inst.id,))
                    row = await cur.fetchone()
            check("直查数据库实例已完成", row and row[0] == int(InstanceState.DONE), str(row))
        finally:
            pool2.close()
            await pool2.wait_closed()

        # ── ⑥ 权限负向：非参与者操作被拒（沿用 ⑤ 的流程数据）
        inst = await eng.start_process_instance_by_id(DEFINE_ID, "zhangsan", {"BUSINESS_NO": "BIZ-PY-002"})
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
        await cleanup(pool)

        async def tx_commit():
            async def work():
                from datetime import datetime
                from jeeflow.model import ProcessInstance
                now = datetime.now()
                await repo.save_instance(ProcessInstance(
                    id=900003, defineId=DEFINE_ID, state=InstanceState.DOING, operator="zhangsan",
                    businessNo="TXN-PY-001", variables={"k": "v"}, createTime=now, updateTime=now,
                    createUser="t", updateUser="t"))
                await repo.create_cc_instance(900003, "zhangsan", "lisi", "wangwu")
                got = await repo.find_instance_by_id(900003)  # 事务内绑定读
                return got is not None
            ok = await repo.with_tx(work)
            return ok
        ok = await tx_commit()
        check("事务提交落库", ok)
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM wf_process_instance WHERE id = 900003")
                n = (await cur.fetchone())[0]
                await cur.execute("SELECT COUNT(*) FROM wf_process_cc_instance WHERE process_instance_id = 900003")
                cc = (await cur.fetchone())[0]
        check("实例 1 条 + 抄送 2 条", n == 1 and cc == 2, f"instance={n} cc={cc}")

        async def tx_rollback():
            async def work():
                from datetime import datetime
                from jeeflow.model import ProcessInstance
                now = datetime.now()
                await repo.save_instance(ProcessInstance(
                    id=900004, defineId=DEFINE_ID, state=InstanceState.DOING, operator="zhangsan",
                    createTime=now, updateTime=now, createUser="t", updateUser="t"))
                await repo.create_cc_instance(900004, "zhangsan", "lisi")
                raise RuntimeError("boom")
            try:
                await repo.with_tx(work)
                return False
            except RuntimeError:
                return True
        ok = await tx_rollback()
        check("事务异常回滚", ok)
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM wf_process_instance WHERE id = 900004")
                n = (await cur.fetchone())[0]
                await cur.execute("SELECT COUNT(*) FROM wf_process_cc_instance WHERE process_instance_id = 900004")
                cc = (await cur.fetchone())[0]
        check("回滚后无残留数据", n == 0 and cc == 0, f"instance={n} cc={cc}")

        # 清理测试残留
        await cleanup(pool)
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM wf_process_instance WHERE id = 900003")
                await cur.execute("DELETE FROM wf_process_cc_instance WHERE process_instance_id = 900003")
    finally:
        pool.close()
        await pool.wait_closed()

    print(f"\nPython JDBC 集成测试: {passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    asyncio.run(main())
