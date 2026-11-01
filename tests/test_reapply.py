"""「退回修改、重提直返」验收（Python 栈）。

覆盖：基本直返、条件不重算、未汇合并行等待、多分支分别退回、重复退回、并发重提、
回归（无标记走原路）与非法配置的负向。
"""
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository  # noqa: E402
from jeeflow.model import ProcessDefine, TaskState, InstanceState, UserInfo  # noqa: E402
from jeeflow.spi import IDGenerator, ExpressionEvaluator  # noqa: E402

import flows_resolver  # noqa: E402


class IDGen(IDGenerator):
    def __init__(self):
        self.n = 0

    def next_id(self):
        self.n += 1
        return self.n


class UserProv:
    async def get_user(self, uid):
        return UserInfo(userId=uid, realName=uid, deptId="D01", deptName="部门",
                        postId="P01", postName="岗位")


class Expr(ExpressionEvaluator):
    async def eval(self, expr, vars):
        # 共享夹具用裸名 amount，issues/241 夹具用表单字段 f_amount
        amt = vars.get("f_amount") if "f_amount" in expr else vars.get("amount")
        if amt is not None:
            if expr in ("amount > 1000", "f_amount > 1000"):
                return float(amt) > 1000
            if expr in ("amount <= 1000", "f_amount <= 1000"):
                return float(amt) <= 1000
        return False


def setup():
    repo = MemoryRepository()
    eng = EngineImpl(repo, UserProv(), IDGen(), Expr())
    return eng, repo


def register(repo, fname):
    with open(os.path.join(flows_resolver.dir(), fname), "r", encoding="utf-8") as f:
        raw = json.loads(f.read())
    d = ProcessDefine(name=raw.get("name", fname), displayName=raw.get("displayName", fname),
                      type=raw.get("type", ""), state=1, content=json.dumps(raw, ensure_ascii=False))
    repo.add_define(d)
    return d


async def start(eng, repo, define_id, operator="applicant", args=None):
    inst = await eng.start_process_instance_by_id(define_id, operator, args)
    for t in await repo.find_doing_tasks(inst.id):
        if t.taskName == "apply":
            await repo.add_task_actor(t.id, [operator])
            await eng.execute_process_task(t.id, operator)
    return await repo.find_instance_by_id(inst.id)


async def doing(repo, iid, name):
    for t in await repo.find_doing_tasks(iid):
        if t.taskName == name:
            return t
    return None


async def doing_all(repo, iid):
    return await repo.find_doing_tasks(iid)


async def act(repo, task, operator):
    await repo.add_task_actor(task.id, [operator])
    if operator not in task.actorIds:
        task.actorIds.append(operator)


async def execute(eng, repo, task, operator, submit):
    await act(repo, task, operator)
    return await eng.execute_process_task(task.id, operator, {"submitType": submit})


async def ret(eng, repo, task, operator):
    await act(repo, task, operator)
    return await eng.execute_and_jump_to_first_task_node(task.id, operator, {"submitType": 6})


# ─── 用例 ────────────────────────────────────────────────────────────────────

def test_simple_direct_return():
    async def case():
        eng, repo = setup()
        d = register(repo, "14-return-to-source.json")
        inst = await start(eng, repo, d.id)
        a = await doing(repo, inst.id, "taskA")
        await ret(eng, repo, a, "userA")
        app = await doing(repo, inst.id, "apply")
        assert app is not None, "applicant task not created"
        assert app.variables.get("_reapplyContext"), app.variables
        assert (await doing(repo, inst.id, "taskB")) is None
        await execute(eng, repo, app, "applicant", 5)
        new_a = await doing(repo, inst.id, "taskA")
        assert new_a is not None
        assert new_a.parentTaskId == app.id
        assert (await doing(repo, inst.id, "taskB")) is None
        await execute(eng, repo, new_a, "userA", 1)
        b = await doing(repo, inst.id, "taskB")
        assert b is not None
        await execute(eng, repo, b, "userB", 1)
        final = await repo.find_instance_by_id(inst.id)
        assert final.state == InstanceState.DONE
    asyncio.run(case())


def test_reject_other_submit_types():
    async def case():
        eng, repo = setup()
        d = register(repo, "14-return-to-source.json")
        inst = await start(eng, repo, d.id)
        await ret(eng, repo, await doing(repo, inst.id, "taskA"), "userA")
        app = await doing(repo, inst.id, "apply")
        await act(repo, app, "applicant")
        try:
            await eng.execute_process_task(app.id, "applicant", {"submitType": 1})
            raise AssertionError("agree on direct-return applicant task must fail")
        except ValueError:
            pass
        assert (await doing(repo, inst.id, "taskA")) is None
    asyncio.run(case())


def test_fork_waits_for_returned_branch():
    async def case():
        eng, repo = setup()
        d = register(repo, "15-return-to-source-fork.json")
        inst = await start(eng, repo, d.id)
        a = await doing(repo, inst.id, "taskA")
        b = await doing(repo, inst.id, "taskB")
        assert a and b
        await ret(eng, repo, a, "userA")
        assert await doing(repo, inst.id, "taskB") is not None
        await execute(eng, repo, b, "userB", 1)
        assert (await doing(repo, inst.id, "taskC")) is None, "join must wait while applicant edits"
        app = await doing(repo, inst.id, "apply")
        await execute(eng, repo, app, "applicant", 5)
        new_a = await doing(repo, inst.id, "taskA")
        assert new_a is not None
        assert (await doing(repo, inst.id, "taskB")) is None
        assert (await doing(repo, inst.id, "taskC")) is None, "reapply alone must not release join"
        await execute(eng, repo, new_a, "userA", 1)
        assert await doing(repo, inst.id, "taskC") is not None
        assert len(await doing_all(repo, inst.id)) == 1, "join releases exactly once"
    asyncio.run(case())


def test_two_branches_return_independently():
    async def case():
        eng, repo = setup()
        d = register(repo, "15-return-to-source-fork.json")
        inst = await start(eng, repo, d.id)
        await ret(eng, repo, await doing(repo, inst.id, "taskA"), "userA")
        await ret(eng, repo, await doing(repo, inst.id, "taskB"), "userB")
        apps = await repo.find_doing_tasks(inst.id, ["apply"])
        assert len(apps) == 2, f"two applicant tasks expected, got {len(apps)}"
        app_a = next((x for x in apps if (x.variables.get("_reapplyContext") or {}).get("sourceNode") == "taskA"), None)
        app_b = next((x for x in apps if (x.variables.get("_reapplyContext") or {}).get("sourceNode") == "taskB"), None)
        assert app_a and app_b
        await execute(eng, repo, app_a, "applicant", 5)
        rest = await repo.find_doing_tasks(inst.id, ["apply"])
        assert len(rest) == 1, "restoring A must leave B's applicant task"
        await execute(eng, repo, await doing(repo, inst.id, "taskA"), "userA", 1)
        assert (await doing(repo, inst.id, "taskC")) is None
        await execute(eng, repo, app_b, "applicant", 5)
        await execute(eng, repo, await doing(repo, inst.id, "taskB"), "userB", 1)
        assert await doing(repo, inst.id, "taskC") is not None
    asyncio.run(case())


def test_condition_not_reevaluated():
    async def case():
        eng, repo = setup()
        d = register(repo, "16-return-to-source-decision.json")
        inst = await start(eng, repo, d.id, args={"amount": 5000})
        high = await doing(repo, inst.id, "taskHigh")
        assert high is not None, "amount>1000 should route to taskHigh"
        await ret(eng, repo, high, "userH")
        app = await doing(repo, inst.id, "apply")
        await act(repo, app, "applicant")
        await eng.execute_process_task(app.id, "applicant", {"submitType": 5, "amount": 100})
        assert await doing(repo, inst.id, "taskHigh") is not None
        assert (await doing(repo, inst.id, "taskLow")) is None, "pre-source condition must not be re-evaluated"
    asyncio.run(case())


def test_repeated_return_binds_its_own_round():
    async def case():
        eng, repo = setup()
        d = register(repo, "14-return-to-source.json")
        inst = await start(eng, repo, d.id)
        await ret(eng, repo, await doing(repo, inst.id, "taskA"), "userA")
        await execute(eng, repo, await doing(repo, inst.id, "apply"), "applicant", 5)
        round2 = await doing(repo, inst.id, "taskA")
        await ret(eng, repo, round2, "userA")
        app2 = await doing(repo, inst.id, "apply")
        rc = app2.variables.get("_reapplyContext") or {}
        assert str(rc.get("sourceTaskId")) == str(round2.id), (rc, round2.id)
    asyncio.run(case())


def test_concurrent_reapply_only_once():
    async def case():
        eng, repo = setup()
        d = register(repo, "14-return-to-source.json")
        inst = await start(eng, repo, d.id)
        await ret(eng, repo, await doing(repo, inst.id, "taskA"), "userA")
        app = await doing(repo, inst.id, "apply")
        await act(repo, app, "applicant")

        async def one():
            try:
                await eng.execute_process_task(app.id, "applicant", {"submitType": 5})
                return True
            except Exception:
                return False

        results = await asyncio.gather(one(), one())
        assert results.count(True) == 1, results
        a2 = await repo.find_doing_tasks(inst.id, ["taskA"])
        assert len(a2) == 1
    asyncio.run(case())


def test_legacy_reapply_unchanged():
    async def case():
        eng, repo = setup()
        d = register(repo, "04-fork-join.json")
        inst = await start(eng, repo, d.id)
        await ret(eng, repo, await doing(repo, inst.id, "taskA"), "userA")
        app = await doing(repo, inst.id, "apply")
        assert not app.variables.get("_reapplyContext")
        await execute(eng, repo, app, "applicant", 5)
        assert len(await doing_all(repo, inst.id)) == 3, "legacy reapply should re-fork"
    asyncio.run(case())


def test_marked_applicant_rejects_jump_entries():
    """带直返标记的申请待办走真实跳转入口（命名跳转/退回上一步/拒绝/再退发起人）必须失败且无写入。"""
    async def case():
        eng, repo = setup()
        d = register(repo, "14-return-to-source.json")
        inst = await start(eng, repo, d.id)
        await ret(eng, repo, await doing(repo, inst.id, "taskA"), "userA")
        app = await doing(repo, inst.id, "apply")
        await act(repo, app, "applicant")
        before = len((await repo.find_instance_by_id(inst.id)).tasks)

        async def must_fail(coro):
            try:
                await coro
                raise AssertionError("marked applicant task must be rejected")
            except ValueError as e:
                assert "只接受重新提交" in str(e), e

        await must_fail(eng.execute_and_jump_task(app.id, "applicant", {"submitType": 4},
                                                   target_task_name="taskB"))
        await must_fail(eng.execute_and_jump_task(app.id, "applicant", {"submitType": 3}))
        await must_fail(eng.execute_and_jump_to_end(app.id, "applicant", {"submitType": 2}))
        await must_fail(eng.execute_and_jump_to_first_task_node(app.id, "applicant", {"submitType": 6}))

        assert len((await repo.find_instance_by_id(inst.id)).tasks) == before, "拒绝后不得新增任务行"
        again = await repo.find_task_by_id(app.id)
        assert again.taskState == TaskState.DOING and again.variables.get("_reapplyContext")

        await execute(eng, repo, again, "applicant", 5)
        assert (await doing(repo, inst.id, "taskA")) is not None
    asyncio.run(case())


def test_nested_fork_branch_ctx_persists_across_requests():
    """嵌套归属：_branchCtx 必须真落库（仓储读回非空），内层等待跨请求/重建引擎仍生效。"""
    async def case():
        eng, repo = setup()
        d = register(repo, "18-return-to-source-nested.json")
        inst = await start(eng, repo, d.id)

        # 落库判据：从仓储重新读出的分支根任务必须带 _branchCtx（不是内存对象自证）
        task_a = await doing(repo, inst.id, "taskA")
        task_b = await doing(repo, inst.id, "taskB")
        assert task_a.variables.get("_branchCtx"), task_a.variables
        assert task_b.variables.get("_branchCtx"), task_b.variables

        await execute(eng, repo, task_a, "userA", 1)
        task_x = await doing(repo, inst.id, "taskX")
        task_y = await doing(repo, inst.id, "taskY")
        assert len(task_x.variables.get("_branchCtx") or []) == 2, task_x.variables
        assert len(task_y.variables.get("_branchCtx") or []) == 2, task_y.variables

        await ret(eng, repo, task_x, "userX")
        assert (await doing(repo, inst.id, "apply")) is not None

        # 另一内层分支办完：内层汇合不得放行
        await execute(eng, repo, task_y, "userY", 1)
        assert (await doing(repo, inst.id, "taskA2")) is None

        # 重建引擎（模拟跨请求/重启）：等待关系只靠仓储状态仍在
        eng2 = EngineImpl(repo, UserProv(), IDGen(), Expr())
        app = await doing(repo, inst.id, "apply")
        await execute(eng2, repo, app, "applicant", 5)
        new_x = await doing(repo, inst.id, "taskX")
        assert new_x is not None and new_x.parentTaskId == app.id
        assert (await doing(repo, inst.id, "taskY")) is None, "不重建内层另一支"
        assert (await doing(repo, inst.id, "taskA2")) is None, "重提本身不放行内层汇合"

        await execute(eng2, repo, new_x, "userX", 1)
        a2 = await doing(repo, inst.id, "taskA2")
        assert a2 is not None, "内层汇合应放行"

        # 外层：A 支到汇合点，B 未办完不得放行
        await execute(eng2, repo, a2, "userA", 1)
        assert (await doing(repo, inst.id, "taskC")) is None
        await execute(eng2, repo, await doing(repo, inst.id, "taskB"), "userB", 1)
        assert (await doing(repo, inst.id, "taskC")) is not None
    asyncio.run(case())


# ─── issues/241：highLight 重放腿不得按重提后的实例变量选边 ──────────────────

I241_JOIN_FLOW = {
    "name": "i241-return-highlight-join",
    "displayName": "直返重提高亮（汇合后分支）",
    "type": "approval",
    "nodes": [
        {"id": "start", "type": "snaker:start"},
        {"id": "apply", "type": "snaker:task",
         "properties": {"assignee": "applicant", "taskType": 0, "performType": 0}},
        {"id": "fork1", "type": "snaker:fork", "properties": {}},
        {"id": "taskA", "type": "snaker:task",
         "properties": {"assignee": "userA", "taskType": 0, "performType": 0}},
        {"id": "taskB", "type": "snaker:task",
         "properties": {"assignee": "userB", "taskType": 0, "performType": 0}},
        {"id": "join1", "type": "snaker:join", "properties": {}},
        {"id": "decision1", "type": "snaker:decision", "properties": {"expr": "f_amount > 1000"}},
        {"id": "taskHigh", "type": "snaker:task",
         "properties": {"assignee": "director", "taskType": 0, "performType": 0,
                        "reapplyMode": "RETURN_TO_SOURCE"}},
        {"id": "taskLow", "type": "snaker:task",
         "properties": {"assignee": "manager", "taskType": 0, "performType": 0}},
        {"id": "end", "type": "snaker:end"},
    ],
    "edges": [
        {"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
        {"id": "e1", "sourceNodeId": "apply", "targetNodeId": "fork1", "properties": {}},
        {"id": "e2", "sourceNodeId": "fork1", "targetNodeId": "taskA", "properties": {}},
        {"id": "e3", "sourceNodeId": "fork1", "targetNodeId": "taskB", "properties": {}},
        {"id": "e4", "sourceNodeId": "taskA", "targetNodeId": "join1", "properties": {}},
        {"id": "e5", "sourceNodeId": "taskB", "targetNodeId": "join1", "properties": {}},
        {"id": "e6", "sourceNodeId": "join1", "targetNodeId": "decision1", "properties": {}},
        {"id": "e7", "sourceNodeId": "decision1", "targetNodeId": "taskHigh",
         "properties": {"expr": "f_amount > 1000"}},
        {"id": "e8", "sourceNodeId": "decision1", "targetNodeId": "taskLow",
         "properties": {"expr": "f_amount <= 1000"}},
        {"id": "e9", "sourceNodeId": "taskHigh", "targetNodeId": "end", "properties": {}},
        {"id": "e10", "sourceNodeId": "taskLow", "targetNodeId": "end", "properties": {}},
    ],
}


def register_content(repo, raw):
    d = ProcessDefine(name=raw["name"], displayName=raw["displayName"], type=raw["type"],
                      state=1, content=json.dumps(raw, ensure_ascii=False))
    repo.add_define(d)
    return d


def test_high_light_not_light_unvisited_branch_after_reapply():
    """issues/241：直返重提后实例变量已改（6666 → 666），重放腿仍须按任务行佐证走高额分支。

    决策节点的入边来源刻意放成 join（网关不产任务行）——任务行快照实例变量的栈
    在「任务行前置」夹具上会被旧的最老行合并偶然救活，join 入边让那条通道失效。
    """
    async def case():
        from jeeflow.facade import JeeflowFacade
        eng, repo = setup()
        d = register_content(repo, I241_JOIN_FLOW)
        inst = await start(eng, repo, d.id, args={"f_amount": 6666})
        for name, actor in (("taskA", "userA"), ("taskB", "userB")):
            t = await doing(repo, inst.id, name)
            assert t is not None, f"夹具自证：并行分支 {name} 应就绪"
            await execute(eng, repo, t, actor, 1)
        high = await doing(repo, inst.id, "taskHigh")
        assert high is not None, "夹具自证：汇合后 6666 应走 taskHigh"
        await ret(eng, repo, high, "director")
        app = await doing(repo, inst.id, "apply")
        assert app is not None, "退回后应有申请待办"
        await act(repo, app, "zhangsan")
        await eng.execute_process_task(app.id, "zhangsan", {"submitType": 5, "f_amount": 666})
        assert await doing(repo, inst.id, "taskHigh") is not None, "重提必须回原来源节点"

        facade = JeeflowFacade(eng, repo)
        r = await facade.flow("processInstance/highLight", {"id": inst.id})
        assert r.get("code") == 0, r
        data = r["data"]
        edges = data.get("historyEdgeNames") or []
        nodes = data.get("historyNodeNames") or []
        assert "e7" in edges, f"汇合后的实际分支 e7 必须点亮（实际 {edges}）"
        assert "e8" not in edges, f"未走分支 e8 不得点亮（实际 {edges}）"
        assert "e10" not in edges, f"未走分支 e10 不得点亮（实际 {edges}）"
        assert "taskLow" not in nodes, f"未走节点 taskLow 不得进历史（实际 {nodes}）"
        assert "end" not in nodes, f"未走节点 end 不得进历史（实际 {nodes}）"
    asyncio.run(case())
