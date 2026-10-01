"""jeeflow SPEC 合规测试 — Python 版（boot2 兼容）"""
import json
import os
import sqlite3
import sys
import pytest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository, EventType, ProcessEvent, FlowInterceptor, EngineExtensions
from jeeflow.engine import KEY_AUTO_GEN_TITLE, KEY_CC_ACTORS
from jeeflow.facade import JeeflowFacade
from jeeflow.memory import MemoryExtRepository
from jeeflow.repository.ext import JdbcProcessExtRepository
from jeeflow.surrogate import (NullSurrogateApplier, hydrate_enabled, surrogate_enabled_on,
                               to_datetime)
from jeeflow.model import (ProcessDefine, ProcessDesign, ProcessDesignHis, ProcessInstance,
                           ProcessSurrogate, ProcessTask, TaskState, InstanceState, UserInfo,
                           parse_flow_model)
from jeeflow.spi import UserProvider, IDGenerator, ExpressionEvaluator, QueryCondition, ProcessRepository

import flows_resolver
FLOW_DIR = flows_resolver.dir()


# ─── Test Stubs ──────────────────────────────────────────────────────────────────

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
        amt = vars.get("amount")
        if amt is not None:
            if expr == "amount > 1000": return float(amt) > 1000
            if expr == "amount <= 1000": return float(amt) <= 1000
        return False


def setup():
    repo = MemoryRepository()
    eng = EngineImpl(repo, _TestUserProv(), _TestIDGen(), _TestExprEval())
    return eng, repo


def load_flow(repo: MemoryRepository, filename: str) -> ProcessDefine:
    path = os.path.join(FLOW_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    d = ProcessDefine(name=filename, displayName=filename, type="test", state=1, content=content)
    repo.add_define(d)
    return d


async def _start_and_execute(eng, repo, define_id, operator, args=None):
    """模拟 boot2 startAndExecute：启动后自动完成申请节点"""
    inst = await eng.start_process_instance_by_id(define_id, operator, args)
    doing = await repo.find_doing_tasks(inst.id)
    for task in doing:
        if task.taskName == "apply":
            await repo.add_task_actor(task.id, [operator])
            await eng.execute_process_task(task.id, operator)
    return inst


# ─── Test 01: Simple Flow ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_01_simple_flow():
    eng, repo = setup()
    df = load_flow(repo, "01-simple.json")
    inst = await _start_and_execute(eng, repo, df.id, "applicant")
    # issue 29：autoGenTitle 自动生成验证
    assert KEY_AUTO_GEN_TITLE in inst.variables, f"autoGenTitle should be in instance variables: {inst.variables.keys()}"
    assert inst.variables[KEY_AUTO_GEN_TITLE], "autoGenTitle should not be empty"
    doing = await repo.find_doing_tasks(inst.id)
    assert len(doing) == 1 and doing[0].taskName == "task1"

    await repo.add_task_actor(doing[0].id, ["leader"])
    inst = await eng.execute_process_task(doing[0].id, "leader")
    assert inst.state == InstanceState.DONE


# ─── Test 02: Multi-task Flow ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_02_multi_task():
    eng, repo = setup()
    df = load_flow(repo, "02-multi-task.json")
    inst = await _start_and_execute(eng, repo, df.id, "applicant")
    doing = await repo.find_doing_tasks(inst.id)
    assert len(doing) == 1 and doing[0].taskName == "task1"

    await repo.add_task_actor(doing[0].id, ["leader"])
    await eng.execute_process_task(doing[0].id, "leader")
    doing = await repo.find_doing_tasks(inst.id)
    assert len(doing) == 1 and doing[0].taskName == "task2"

    await repo.add_task_actor(doing[0].id, ["manager"])
    await eng.execute_process_task(doing[0].id, "manager")
    doing = await repo.find_doing_tasks(inst.id)
    assert len(doing) == 1 and doing[0].taskName == "task3"

    await repo.add_task_actor(doing[0].id, ["boss"])
    inst = await eng.execute_process_task(doing[0].id, "boss")
    assert inst.state == InstanceState.DONE


# ─── Test 03: Decision Expression ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_03_decision_expr():
    eng, repo = setup()
    df = load_flow(repo, "03-decision-expr.json")
    inst = await _start_and_execute(eng, repo, df.id, "applicant", {"amount": 3000})
    doing = await repo.find_doing_tasks(inst.id)
    assert doing[0].taskName == "task1"  # 填写报销单
    await repo.add_task_actor(doing[0].id, ["leader"])
    await eng.execute_process_task(doing[0].id, "leader")
    doing = await repo.find_doing_tasks(inst.id)
    assert len(doing) == 1 and doing[0].taskName == "task2"  # 金额>1000 → 经理审批
    await repo.add_task_actor(doing[0].id, ["manager"])
    inst = await eng.execute_process_task(doing[0].id, "manager")
    assert inst.state == InstanceState.DONE


# ─── Test 04: Fork/Join ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_04_fork_join():
    eng, repo = setup()
    df = load_flow(repo, "04-fork-join.json")
    inst = await _start_and_execute(eng, repo, df.id, "applicant")
    doing = await repo.find_doing_tasks(inst.id)
    assert len(doing) == 2  # fork → taskA + taskB
    tA = next(t for t in doing if t.taskName == "taskA")
    tB = next(t for t in doing if t.taskName == "taskB")

    await repo.add_task_actor(tA.id, ["userA"])
    await eng.execute_process_task(tA.id, "userA")
    inst = await repo.find_instance_by_id(inst.id)
    assert inst.state == InstanceState.DOING

    await repo.add_task_actor(tB.id, ["userB"])
    inst = await eng.execute_process_task(tB.id, "userB")
    assert inst.state == InstanceState.DONE


# ─── Test 05: Countersign Parallel ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_05_countersign_parallel():
    eng, repo = setup()
    df = load_flow(repo, "05-countersign-parallel.json")
    inst = await _start_and_execute(eng, repo, df.id, "applicant")
    doing = await repo.find_doing_tasks(inst.id)
    assert len(doing) == 3  # 3 parallel countersign tasks

    for a in ["userA", "userB", "userC"]:
        d = await repo.find_doing_tasks(inst.id)
        task = d[0]
        await repo.add_task_actor(task.id, [a])
        await eng.execute_process_task(task.id, a)

    inst = await repo.find_instance_by_id(inst.id)
    assert inst.state == InstanceState.DONE


# ─── Test 06: Countersign Sequential ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_06_countersign_sequential():
    eng, repo = setup()
    df = load_flow(repo, "06-countersign-sequential.json")
    inst = await _start_and_execute(eng, repo, df.id, "applicant")
    doing = await repo.find_doing_tasks(inst.id)
    assert len(doing) == 1
    task = doing[0]

    await repo.add_task_actor(task.id, ["userA"])
    await eng.execute_process_task(task.id, "userA")
    doing = await repo.find_doing_tasks(inst.id)
    assert len(doing) == 1
    task = doing[0]

    await repo.add_task_actor(task.id, ["userB"])
    inst = await eng.execute_process_task(task.id, "userB")
    assert inst.state == InstanceState.DONE


# ─── Test 07: Countersign Ratio ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_07_countersign_ratio():
    eng, repo = setup()
    df = load_flow(repo, "07-countersign-ratio.json")
    inst = await _start_and_execute(eng, repo, df.id, "applicant")
    doing = await repo.find_doing_tasks(inst.id)
    assert len(doing) == 4  # 4 parallel tasks

    for a in ["userA", "userB", "userC", "userD"]:
        d = await repo.find_doing_tasks(inst.id)
        task = d[0]
        await repo.add_task_actor(task.id, [a])
        await eng.execute_process_task(task.id, a)

    inst = await repo.find_instance_by_id(inst.id)
    assert inst.state == InstanceState.DONE


# ─── Test 08: Reject (boot2 style: jump back to apply) ──────────────────────────

@pytest.mark.asyncio
async def test_08_reject():
    eng, repo = setup()
    df = load_flow(repo, "02-multi-task.json")
    inst = await _start_and_execute(eng, repo, df.id, "applicant")
    doing = await repo.find_doing_tasks(inst.id)
    assert doing[0].taskName == "task1"

    # leader 驳回，跳回 apply
    await repo.add_task_actor(doing[0].id, ["leader"])
    inst = await eng.execute_and_jump_task(doing[0].id, "leader", target_task_name="apply")
    # 应有新的 apply 待办给 applicant
    doing = await repo.find_doing_tasks(inst.id)
    assert len(doing) == 1 and doing[0].taskName == "apply"
    assert doing[0].actorIds == ["applicant"]  # applicant 解析为发起人


# ─── Test 09: Actor Not Allowed ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_09_actor_not_allowed():
    eng, repo = setup()
    df = load_flow(repo, "02-multi-task.json")
    inst = await _start_and_execute(eng, repo, df.id, "applicant")
    doing = await repo.find_doing_tasks(inst.id)
    await repo.add_task_actor(doing[0].id, ["leader"])

    with pytest.raises(ValueError, match="not allowed"):
        await eng.execute_process_task(doing[0].id, "intruder")


# ─── Test 10: Interceptor & Events ──────────────────────────────────────────────

class _TestInterceptor(FlowInterceptor):
    def __init__(self, pre_fn, post_fn, order=0):
        self._pre = pre_fn; self._post = post_fn; self._order = order
    async def pre_handle(self, node, inst) -> bool:
        self._pre[0] = True; return True
    async def post_handle(self, node, inst):
        self._post[0] = True
    @property
    def order(self): return self._order


@pytest.mark.asyncio
async def test_10_interceptor_and_events():
    eng, repo = setup()
    df = load_flow(repo, "01-simple.json")

    pre_called = [False]; post_called = [False]
    events = []
    codes = []

    async def on_event(evt: ProcessEvent):
        # spec 11-events §11.3：跨栈判据用**规范名**，码值是 A 套整型附带数值（旧 C 套字符串名作废）
        events.append(evt.name)
        codes.append(evt.code)

    eng.set_extensions(EngineExtensions(
        interceptors=[_TestInterceptor(pre_called, post_called, order=1)],
        event_listener=on_event,
    ))

    inst = await eng.start_process_instance_by_id(df.id, "applicant", None)
    assert "PROCESS_INSTANCE_START" in events

    # 自动完成 apply 节点
    doing = await repo.find_doing_tasks(inst.id)
    await repo.add_task_actor(doing[0].id, ["applicant"])
    await eng.execute_process_task(doing[0].id, "applicant")

    # 完成 task1 → end
    doing = await repo.find_doing_tasks(inst.id)
    await repo.add_task_actor(doing[0].id, ["leader"])
    await eng.execute_process_task(doing[0].id, "leader")

    assert pre_called[0], "pre_handle not called"
    assert post_called[0], "post_handle not called"
    # issues/100（任务落库后 fire，对齐 Java CreateTaskHandler）× issues/132（规范名+整型码）。
    # 完整序列：start → [apply 任务 PROCESS_TASK_START, apply 自动完成 TASK_COMPLETE]
    #         → [task1 PROCESS_TASK_START, task1 完成 TASK_COMPLETE] → PROCESS_INSTANCE_END
    # （旧断言写作 PROCESS_START/TASK_CREATE/PROCESS_FINISH 字符串名，spec §11.6 已把它们
    #   并到 A 套规范名：PROCESS_START→PROCESS_INSTANCE_START、TASK_CREATE→PROCESS_TASK_START、
    #   PROCESS_FINISH→PROCESS_INSTANCE_END；序列结构与条数不变）
    assert events == [
        "PROCESS_INSTANCE_START",
        "PROCESS_TASK_START",
        "TASK_COMPLETE",
        "PROCESS_TASK_START",
        "TASK_COMPLETE",
        "PROCESS_INSTANCE_END",
    ], f"unexpected event sequence, got {events}"
    assert codes == [1, 3, 5, 3, 5, 2], f"A 套码序列不符，got {codes}"
    assert events.count("PROCESS_TASK_START") == 2  # apply 节点任务 + task1 各一次
    assert events.count("TASK_COMPLETE") == 2


@pytest.mark.asyncio
async def test_assignee_variable_resolution():
    """assignee 变量解析（v1.0.1，集成反馈③）：token 即变量 key，命中用值、未命中字面量；tf_nextNodeOperator 优先"""
    eng, repo = setup()
    d = load_flow(repo, "11-assignee-vars.json")

    # ① deptLeader 变量命中 → 参与者 = 变量值
    inst = await eng.start_process_instance_by_id(d.id, "applicant", {"deptLeader": "L001"})
    doing = await repo.find_doing_tasks(inst.id)
    await repo.add_task_actor(doing[0].id, ["applicant"])
    await eng.execute_process_task(doing[0].id, "applicant")
    doing = await repo.find_doing_tasks(inst.id)
    assert doing[0].taskName == "task1", doing[0].taskName
    assert doing[0].actorIds == ["L001"], f"变量命中应解析为变量值: {doing[0].actorIds}"

    # ② 静态字面量 userA,userB（变量未命中）
    await eng.execute_process_task(doing[0].id, "L001")
    doing = await repo.find_doing_tasks(inst.id)
    assert doing[0].taskName == "task2", doing[0].taskName
    assert doing[0].actorIds == ["userA", "userB"], f"静态字面量参与者: {doing[0].actorIds}"

    # ③ 变量未传入 → token 字面量回退（对齐 boot3 args.get(token, token)）
    d = load_flow(repo, "11-assignee-vars.json")
    inst = await eng.start_process_instance_by_id(d.id, "applicant")
    doing = await repo.find_doing_tasks(inst.id)
    await repo.add_task_actor(doing[0].id, ["applicant"])
    await eng.execute_process_task(doing[0].id, "applicant")
    doing = await repo.find_doing_tasks(inst.id)
    assert doing[0].actorIds == ["deptLeader"], f"未命中应回退字面量: {doing[0].actorIds}"

    # ④ tf_nextNodeOperator 优先于 assignee
    d = load_flow(repo, "11-assignee-vars.json")
    inst = await eng.start_process_instance_by_id(d.id, "applicant")
    doing = await repo.find_doing_tasks(inst.id)
    await repo.add_task_actor(doing[0].id, ["applicant"])
    await eng.execute_process_task(doing[0].id, "applicant", {"tf_nextNodeOperator": "BOSS1,BOSS2"})
    doing = await repo.find_doing_tasks(inst.id)
    assert doing[0].actorIds == ["BOSS1", "BOSS2"], f"tf_nextNodeOperator 应优先: {doing[0].actorIds}"


@pytest.mark.asyncio
async def test_system_execute_flow_auto():
    """系统代执行 flow.auto / flow.admin（v1.0.1，集成反馈④）：放行 + 跳过用户注入"""
    eng, repo = setup()
    d = load_flow(repo, "11-assignee-vars.json")
    inst = await eng.start_process_instance_by_id(d.id, "applicant", {"deptLeader": "L001"})
    doing = await repo.find_doing_tasks(inst.id)

    # ① flow.auto 非参与者身份放行（startAndExecute 契约）
    inst = await eng.execute_process_task(doing[0].id, "flow.auto")
    doing = await repo.find_doing_tasks(inst.id)
    assert doing[0].taskName == "task1", f"flow.auto 应放行执行: {doing[0].taskName}"

    # ② 跳过 UserProvider 注入：u_userId 不会被替换成 flow.auto
    reloaded = await repo.find_instance_by_id(inst.id)
    assert reloaded.variables.get("u_userId") == "applicant", f"flow.auto 应跳过用户注入: {reloaded.variables.get('u_userId')}"

    # ③ flow.admin 放行
    inst = await eng.execute_process_task(doing[0].id, "flow.admin")
    doing = await repo.find_doing_tasks(inst.id)
    assert doing[0].taskName == "task2", f"flow.admin 应放行执行: {doing[0].taskName}"


@pytest.mark.asyncio
async def test_facade_deploy_version():
    """门面路由（v1.1.0，spec §12 #15）：deploy 版本管理 / 启停 / 删除"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        content = f.read()

    r = await facade.flow("processDefine/deploy", {"content": content})
    assert r["code"] == 0, r
    define_id = int(r["data"]["processDefineId"])
    d1 = await repo.find_define_by_id(define_id)
    assert d1.version == 0, f"首次部署 version = {d1.version}, want 0"

    r = await facade.flow("processDefine/deploy", {"content": content})
    assert r["code"] == 0, r
    latest = await repo.find_define_by_name("simple")
    assert latest.version == 1, f"二次部署 version = {latest.version}, want 1"

    r = await facade.flow("processDefine/upAndDown", {"id": define_id, "state": 0})
    assert r["code"] == 0, r
    assert (await repo.find_define_by_id(define_id)).state == 0

    r = await facade.flow("processDefine/remove", {"id": define_id})
    assert r["code"] == 0, r
    assert await repo.find_define_by_id(define_id) is None


@pytest.mark.asyncio
async def test_facade_instance_task_and_withdraw():
    """门面路由：发起即提交 / 执行任务 / 撤回级联"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        content = f.read()
    r = await facade.flow("processDefine/deploy", {"content": content})
    define_id = r["data"]["processDefineId"]

    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": define_id, "operator": "zhangsan", "amount": "1000"})
    assert r["code"] == 0, r
    instance_id = int(r["data"]["processInstanceId"])

    doing = await repo.find_doing_tasks(instance_id)
    assert len(doing) == 1 and doing[0].taskName == "task1", [t.taskName for t in doing]
    r = await facade.flow("processTask/execute",
                          {"processTaskId": doing[0].id, "operator": "leader", "submitType": 1})
    assert r["code"] == 0, r
    inst = await repo.find_instance_by_id(instance_id)
    assert inst.state == InstanceState.DONE, f"实例应完成: {inst.state}"

    # withdraw 级联撤回 doing → 任务态 30（WITHDRAW）
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": define_id, "operator": "zhangsan"})
    instance_id2 = int(r["data"]["processInstanceId"])
    before = await repo.find_doing_tasks(instance_id2)
    assert len(before) >= 1, "撤回前应有 doing 任务"
    r = await facade.flow("processInstance/withdraw", {"id": instance_id2, "operator": "zhangsan"})
    assert r["code"] == 0, r
    after = await repo.find_doing_tasks(instance_id2)
    assert len(after) == 0, f"撤回应清空 doing 任务: {after}"
    # issues/113：原 doing 任务须落 30，不能落 99——"doing 清空"两种码值都满足，抓不到该缺陷
    for t in before:
        stored = await repo.find_task_by_id(t.id)
        assert stored is not None, f"撤回后任务应仍可读到: {t.id}"
        assert stored.taskState == TaskState.WITHDRAW, \
            f"撤回任务态应=30(WITHDRAW)，实测 {stored.taskState}（99 是废弃码，两码不得混用）"
    inst2 = await repo.find_instance_by_id(instance_id2)
    assert inst2.state == InstanceState.WITHDRAW, f"实例态应=30: {inst2.state}"


@pytest.mark.asyncio
async def test_facade_design_and_surrogate():
    """门面路由：设计保存/详情/发布 + 委托增查删"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        content = f.read()

    r = await facade.flow("processDesign/save",
                          {"name": "leave", "displayName": "请假流程", "content": content,
                           "operator": "zhangsan"})
    assert r["code"] == 0, r
    design_id = r["data"]["id"]

    r = await facade.flow("processDesign/detail", {"id": design_id})
    assert r["code"] == 0, r
    assert r["data"]["jsonObject"] is not None
    assert len(r["data"]["his"]) == 1

    r = await facade.flow("processDesign/deploy", {"id": design_id, "operator": "zhangsan"})
    assert r["code"] == 0, r
    assert int(r["data"]["processDefineId"]) > 0

    r = await facade.flow("processSurrogate/save",
                          {"operator": "zhangsan", "surrogate": "lisi", "processName": "leave"})
    assert r["code"] == 0, r
    surrogate_id = r["data"]["id"]
    hit = await facade._ext.get_surrogate("zhangsan", "leave")
    assert hit is not None and hit.surrogate == "lisi"

    r = await facade.flow("processSurrogate/page", {"operator": "zhangsan"})
    assert r["code"] == 0 and r["data"]["recordCount"] == 1, r

    r = await facade.flow("processSurrogate/remove", {"id": surrogate_id})
    assert r["code"] == 0, r


@pytest.mark.asyncio
async def test_facade_surrogate_effective_window_and_enabled():
    """委托生效判断（issues/82-12，对齐 Java 基准）：时间窗 startTime/endTime +
    enabled 过滤。5 条委托各对应一个时间态：在窗/未到/已过/无窗(enabled=0)/无窗(enabled=1)，
    每条查询只命中其中一条（processName 精确区分）→ 不依赖仓储返回顺序。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    op = "winop"

    async def save(sur, pn, start=None, end=None, enabled=1):
        args = {"operator": op, "surrogate": sur, "processName": pn, "enabled": enabled}
        if start:
            args["startTime"] = start
        if end:
            args["endTime"] = end
        r = await facade.flow("processSurrogate/save", args)
        assert r["code"] == 0, r

    # A 在窗（2026-08-01 ~ 08-31）
    await save("sA", "winA", "2026-08-01 00:00:00", "2026-08-31 23:59:59")
    # B 未到（2026-09-01 起）
    await save("sB", "winB", "2026-09-01 00:00:00")
    # C 已过（07-31 止）
    await save("sC", "winC", end="2026-07-31 23:59:59")
    # D 无窗但停用（enabled=0）
    await save("sD", "winD", enabled=0)
    # E 无窗且启用（enabled=1）
    await save("sE", "winE")

    at = datetime(2026, 8, 15, 12, 0, 0)
    hit = await facade._ext.get_surrogate(op, "winA", at)
    assert hit is not None and hit.surrogate == "sA", "在窗委托应生效"
    assert await facade._ext.get_surrogate(op, "winB", at) is None, "未到窗委托不应生效"
    assert await facade._ext.get_surrogate(op, "winC", at) is None, "已过窗委托不应生效"
    assert await facade._ext.get_surrogate(op, "winD", at) is None, "enabled=0 不应生效"
    hit = await facade._ext.get_surrogate(op, "winE", at)
    assert hit is not None and hit.surrogate == "sE", "无窗启用委托应生效（NULL=不限）"
    assert await facade._ext.get_surrogate(op, "winZ", at) is None, "无匹配流程应返回 None"

    # 换时间验证窗口边界随时间变化：B 在 9 月生效、A 在 9 月失效
    at_sep = datetime(2026, 9, 15, 12, 0, 0)
    hit = await facade._ext.get_surrogate(op, "winB", at_sep)
    assert hit is not None and hit.surrogate == "sB", "9 月：B 进入窗口应生效"
    assert await facade._ext.get_surrogate(op, "winA", at_sep) is None, "9 月：A 已出窗口不应生效"


@pytest.mark.asyncio
async def test_facade_surrogate_detail_and_update():
    """委托编辑链路（issues/77）：save（前端空格格式时间窗）→ detail 回显 →
    update 改字段 → detail 再回显 + 负向 id 不存在"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())

    r = await facade.flow("processSurrogate/save",
                          {"operator": "zhangsan", "surrogate": "lisi", "processName": "leave",
                           "startTime": "2026-08-01 00:00:00", "endTime": "2026-08-31 23:59:59",
                           "enabled": 1})
    assert r["code"] == 0, r
    surrogate_id = r["data"]["id"]

    # detail 回显：行结构齐全 + 时间格式化
    r = await facade.flow("processSurrogate/detail", {"id": surrogate_id})
    assert r["code"] == 0, r
    d = r["data"]
    assert d["processName"] == "leave" and d["operator"] == "zhangsan" and d["surrogate"] == "lisi", d
    assert d["startTime"] == "2026-08-01 00:00:00" and d["endTime"] == "2026-08-31 23:59:59", d

    # update：改代理人/时间窗/启用状态（不带 operator，授权人应保留）
    r = await facade.flow("processSurrogate/update",
                          {"id": surrogate_id, "surrogate": "wangwu", "processName": "leave",
                           "startTime": "2026-09-01 00:00:00", "endTime": "2026-09-30 23:59:59",
                           "enabled": 0})
    assert r["code"] == 0, r
    assert r["data"]["id"] == surrogate_id, r

    # detail 再回显：变更生效 + 授权人未被清空
    r = await facade.flow("processSurrogate/detail", {"id": surrogate_id})
    assert r["code"] == 0, r
    d = r["data"]
    assert d["surrogate"] == "wangwu" and d["operator"] == "zhangsan" and d["enabled"] == 0, d
    assert d["startTime"] == "2026-09-01 00:00:00" and d["endTime"] == "2026-09-30 23:59:59", d

    # 仓储侧同步（update 真的写了）
    s = await facade._ext.find_surrogate_by_id(int(surrogate_id))
    assert s is not None and s.surrogate == "wangwu" and s.enabled == 0, s

    # 负向：id 不存在
    r = await facade.flow("processSurrogate/detail", {"id": 99999})
    assert r["code"] == 99999999, r
    r = await facade.flow("processSurrogate/update", {"id": 99999, "surrogate": "wangwu"})
    assert r["code"] == 99999999, r
    # 负向：update 缺 id
    r = await facade.flow("processSurrogate/update", {"surrogate": "wangwu"})
    assert r["code"] == 99999999, r


@pytest.mark.asyncio
async def test_facade_surrogate_remove_batch_ids():
    """委托删除（issues/95）：前端「我的委托」行内与批量删除统一发 {ids}（行内 = 长度 1
    的数组），此前六语言门面只读单数 {id} → 该页删除整体不可用；单 {id} 保留兼容
    （移动端 workflow.uts 发这个）。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())

    async def save(op, agent, name):
        r = await facade.flow("processSurrogate/save",
                              {"operator": op, "surrogate": agent, "processName": name})
        assert r["code"] == 0, r
        return int(r["data"]["id"])

    a = await save("zhangsan", "lisiA", "leaveA")
    b = await save("zhangsan", "lisiB", "leaveB")
    r = await facade.flow("processSurrogate/remove", {"ids": [a, b]})
    assert r["code"] == 0, r
    assert await facade._ext.find_surrogate_by_id(a) is None
    assert await facade._ext.find_surrogate_by_id(b) is None

    # 行内删除：前端同样走 {ids}，长度 1
    c = await save("lisiC", "lisiD", "leaveC")
    r = await facade.flow("processSurrogate/remove", {"ids": [c]})
    assert r["code"] == 0, r
    assert await facade._ext.find_surrogate_by_id(c) is None

    # 单 {id} 兼容形态回归
    d = await save("zhangsan", "lisiE", "leaveD")
    r = await facade.flow("processSurrogate/remove", {"id": d})
    assert r["code"] == 0, r
    assert await facade._ext.find_surrogate_by_id(d) is None


@pytest.mark.asyncio
async def test_facade_remove_empty_ids_rejected():
    """{ids}/{id} 缺失或空数组一律报错，禁止静默成功（issues/95 §5②，六语言统一口径）。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    cases = [
        ("processSurrogate/remove", {"ids": []}),
        ("processSurrogate/remove", {"surrogate": "lisi"}),
        ("processSurrogate/remove", {"ids": [123, None]}),
        ("processDefine/remove", {"ids": []}),
        ("processDesign/remove", {"ids": []}),
        ("processDefine/upAndDown", {"ids": [], "opType": 0}),
    ]
    for action, args in cases:
        r = await facade.flow(action, args)
        assert r["code"] == 99999999, (action, args, r)
        assert "id 缺失或非法" in r["msg"], (action, args, r)


@pytest.mark.asyncio
async def test_facade_surrogate_page_in_and_eq_conditions():
    """委托分页 m_ 条件（issues/82-7，五语言基准测试）：m_IN_processName / m_EQ_enabled。
    显式 enabled=0 不得被仓储吞掉（Go/Python 旧 bug：or 1 / truthy 默认把停用变启用）。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())

    # 3 条委托：leave(启用) / overtime(启用) / sick(停用)
    r = await facade.flow("processSurrogate/save",
                          {"operator": "zhangsan", "surrogate": "lisi",
                           "processName": "leave", "enabled": 1})
    assert r["code"] == 0, r
    r = await facade.flow("processSurrogate/save",
                          {"operator": "zhangsan", "surrogate": "wangwu",
                           "processName": "overtime", "enabled": 1})
    assert r["code"] == 0, r
    r = await facade.flow("processSurrogate/save",
                          {"operator": "zhangsan", "surrogate": "zhaoliu",
                           "processName": "sick", "enabled": 0})
    assert r["code"] == 0, r

    # 无过滤：3 条
    r = await facade.flow("processSurrogate/page", {"operator": "zhangsan"})
    assert r["code"] == 0 and r["data"]["recordCount"] == 3, r

    # m_IN_processName：IN 列表命中 2 条
    r = await facade.flow("processSurrogate/page",
                          {"operator": "zhangsan", "m_IN_processName": ["leave", "overtime"]})
    assert r["code"] == 0, r
    d = r["data"]
    assert d["recordCount"] == 2, d
    names = [row["processName"] for row in d["rows"]]
    assert "leave" in names and "overtime" in names, names

    # m_EQ_enabled：启用过滤命中 2 条（依赖 enabled=0 未被吞）
    r = await facade.flow("processSurrogate/page",
                          {"operator": "zhangsan", "m_EQ_enabled": 1})
    assert r["code"] == 0 and r["data"]["recordCount"] == 2, r

    # m_IN + m_EQ 组合：sick/overtime 中仅启用 → 1 条（overtime）
    r = await facade.flow("processSurrogate/page",
                          {"operator": "zhangsan",
                           "m_IN_processName": ["sick", "overtime"], "m_EQ_enabled": 1})
    assert r["code"] == 0, r
    d = r["data"]
    assert d["recordCount"] == 1 and d["rows"][0]["processName"] == "overtime", d

    # 负向：IN 全不命中 / EQ 无匹配 → 0 条
    r = await facade.flow("processSurrogate/page",
                          {"operator": "zhangsan", "m_IN_processName": ["none1", "none2"]})
    assert r["code"] == 0 and r["data"]["recordCount"] == 0, r
    r = await facade.flow("processSurrogate/page",
                          {"operator": "zhangsan", "m_EQ_enabled": 2})
    assert r["code"] == 0 and r["data"]["recordCount"] == 0, r


@pytest.mark.asyncio
async def test_facade_errors():
    """门面错误路径：未知 action / 缺扩展仓储"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, None)
    r = await facade.flow("foo/bar", {})
    assert r["code"] == 99999999, r
    r = await facade.flow("processDesign/page", {})
    assert r["code"] == 99999999, r


@pytest.mark.asyncio
async def test_facade_view_endpoints():
    """门面视图端点（v1.2.0，spec §12 #16-18）"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        content = f.read()
    r = await facade.flow("processDefine/deploy", {"content": content})
    define_id = r["data"]["processDefineId"]

    # getLastByName
    r = await facade.flow("processDefine/getLastByName", {"processDefineName": "simple"})
    assert r["code"] == 0 and r["data"]["name"] == "simple", r

    # startAndExecute → 视图端点
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": define_id, "operator": "zhangsan"})
    instance_id = r["data"]["processInstanceId"]

    r = await facade.flow("processInstance/approvalRecord", {"id": instance_id})
    assert r["code"] == 0 and len(r["data"]) == 2, r  # apply + task1

    r = await facade.flow("processInstance/highLight", {"id": instance_id})
    assert r["code"] == 0, r
    assert "task1" in r["data"]["activeNodeNames"], r
    assert "apply" in r["data"]["historyNodeNames"], r

    r = await facade.flow("processInstance/getAssigneeTextData", {"id": instance_id})
    assert r["code"] == 0 and len(r["data"]) == 1, r  # task1 → leader

    doing = await repo.find_doing_tasks(int(instance_id))
    r = await facade.flow("processTask/detail", {"id": doing[0].id, "operator": "leader"})
    assert r["code"] == 0 and r["data"]["executable"] is True, r
    assert r["data"]["taskModel"] is not None, r
    # issues/62：taskModel 补 form/ext（字段权限）
    tm = r["data"]["taskModel"]
    assert tm["form"] == "leave-form", tm
    assert tm["ext"]["PERMISSION_f_leaveType"] == 1, tm
    assert tm["ext"]["PERMISSION_days"] == 2, tm

    r = await facade.flow("processTask/latest", {"processInstanceId": instance_id})
    assert r["code"] == 0 and r["data"]["taskName"] == "task1", r

    # 抄送：创建 + 已读 + 列表（ccList v1.3.0 补齐）
    r = await facade.flow("processInstance/createCCInstance",
                          {"processInstanceId": instance_id, "operator": "zhangsan",
                           "actorIds": ["lisi"]})
    assert r["code"] == 0, r
    r = await facade.flow("processInstance/updateCCStatus",
                          {"processInstanceId": instance_id, "operator": "lisi"})
    assert r["code"] == 0, r
    r = await facade.flow("processInstance/ccList", {"operator": "lisi"})
    assert r["code"] == 0 and len(r["data"]["rows"]) == 1, r

    # 加签/转交
    r = await facade.flow("processTask/addCandidate",
                          {"processTaskId": doing[0].id, "actorIds": ["zhaoliu"]})
    assert r["code"] == 0, r
    actors = await repo.find_task_actors(doing[0].id)
    assert "zhaoliu" in actors, actors

    # candidatePage：未配置钩子报错；配置后可用
    r = await facade.flow("processTask/candidatePage", {"processTaskId": doing[0].id})
    assert r["code"] == 99999999, r
    facade.set_user_search(lambda q: (([{"userId": "u1", "realName": "用户1"}], 1)))
    r = await facade.flow("processTask/candidatePage", {"processTaskId": doing[0].id})
    assert r["code"] == 0 and r["data"]["recordCount"] == 1, r


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

# ─── Test 03.5: highLight 决策分支表达式过滤（issues/06） ─────────────────────

@pytest.mark.asyncio
async def test_highlight_filters_decision_branch():
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, None)
    df = load_flow(repo, "03-decision-expr.json")
    # amount=500 → 走「amount <= 1000」分支（task3），task2 分支未执行
    inst = await _start_and_execute(eng, repo, df.id, "applicant", {"amount": 500})
    doing = await repo.find_doing_tasks(inst.id)
    for t in doing:
        if t.taskName == "task1":
            await repo.add_task_actor(t.id, ["leader"])
            await eng.execute_process_task(t.id, "leader")
    doing = await repo.find_doing_tasks(inst.id)
    for t in doing:
        if t.taskName == "task3":
            await repo.add_task_actor(t.id, ["director"])
            await eng.execute_process_task(t.id, "director")

    r = await facade.flow("processInstance/highLight", {"id": inst.id})
    assert r["code"] == 0, r
    hl = r["data"]
    assert "e4" in hl["historyEdgeNames"] and "e6" in hl["historyEdgeNames"], hl
    assert "e3" not in hl["historyEdgeNames"] and "e5" not in hl["historyEdgeNames"], hl
    assert "task2" not in hl["historyNodeNames"], hl
    assert "task3" in hl["historyNodeNames"], hl

# ─── Test 05-1: 三个 detail 返回 jsonObject ───────────────────────────────────

@pytest.mark.asyncio
async def test_detail_json_object():
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, None)
    df = load_flow(repo, "01-simple.json")

    r = await facade.flow("processDefine/detail", {"id": df.id})
    assert r["code"] == 0 and r["data"].get("jsonObject"), r

    inst = await _start_and_execute(eng, repo, df.id, "applicant")
    r = await facade.flow("processInstance/detail", {"id": inst.id})
    assert r["code"] == 0 and r["data"].get("jsonObject"), r

    doing = await repo.find_doing_tasks(inst.id)
    r = await facade.flow("processTask/detail", {"id": doing[0].id, "operator": "applicant"})
    assert r["code"] == 0 and r["data"].get("jsonObject"), r


@pytest.mark.asyncio
async def test_m_query_params():
    """issues/05-5：m_ 前缀查询参数（m_LIKE_name / m_pd_LIKE_displayName / m_t_LIKE_displayName）"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        c1 = f.read()
    with open(os.path.join(FLOW_DIR, "02-multi-task.json"), encoding="utf-8") as f:
        c2 = f.read()
    await facade.flow("processDefine/deploy", {"content": c1})
    await facade.flow("processDefine/deploy", {"content": c2})

    # 无别名 → 默认主表别名 t（t.name / t.display_name）
    r = await facade.flow("processDefine/page", {"m_LIKE_name": "simple"})
    assert r["code"] == 0, r
    assert len(r["data"]["rows"]) == 1 and r["data"]["rows"][0]["name"] == "simple", r

    r = await facade.flow("processDefine/page", {"m_LIKE_displayName": "简单"})
    assert r["code"] == 0, r
    assert len(r["data"]["rows"]) == 1, r

    r = await facade.flow("processDefine/page", {"m_LIKE_displayName": "流程"})
    assert r["code"] == 0, r
    assert len(r["data"]["rows"]) == 2, r

    # 实例列表：m_pd_LIKE_displayName（别名 pd → pd.display_name）
    d1 = await repo.find_define_by_name("simple")
    await facade.flow("processInstance/startAndExecute",
                      {"processDefineId": d1.id, "operator": "zhangsan"})
    r = await facade.flow("processInstance/page",
                          {"operator": "zhangsan", "m_pd_LIKE_displayName": "简单"})
    assert r["code"] == 0, r
    assert len(r["data"]["rows"]) == 1, r
    r = await facade.flow("processInstance/page",
                          {"operator": "zhangsan", "m_pd_LIKE_displayName": "zzz"})
    assert r["code"] == 0, r
    assert len(r["data"]["rows"]) == 0, r

    # issues/82-6：实例列表按编码搜 m_pd_LIKE_name（别名 pd → pd.name）
    r = await facade.flow("processInstance/page",
                          {"operator": "zhangsan", "m_pd_LIKE_name": "simple"})
    assert r["code"] == 0, r
    assert len(r["data"]["rows"]) == 1, r
    r = await facade.flow("processInstance/page",
                          {"operator": "zhangsan", "m_pd_LIKE_name": "zzz"})
    assert r["code"] == 0, r
    assert len(r["data"]["rows"]) == 0, r

    # 任务列表：m_t_LIKE_displayName（别名 t → t.display_name）
    r = await facade.flow("processTask/todoList",
                          {"operator": "leader", "m_t_LIKE_displayName": "审批"})
    assert r["code"] == 0, r
    assert len(r["data"]["rows"]) == 1, r
    r = await facade.flow("processTask/todoList",
                          {"operator": "leader", "m_t_LIKE_displayName": "zzz"})
    assert r["code"] == 0, r
    assert len(r["data"]["rows"]) == 0, r

    # 设计列表：无别名 m_LIKE_name（process-design 页）
    # 82-9：save 带 remark/icon，page 行应回显（设计页回显字段，对齐 Java/Go）
    await facade.flow("processDesign/save",
                      {"name": "leave", "displayName": "请假流程", "content": c1, "operator": "zhangsan",
                       "icon": "icon-echo", "remark": "回显验证备注"})
    r = await facade.flow("processDesign/page", {"m_LIKE_name": "leave"})
    assert r["code"] == 0, r
    assert len(r["data"]["rows"]) == 1, r
    row = r["data"]["rows"][0]
    assert row["remark"] == "回显验证备注", f"designPage remark 应回显保存值: {row.get('remark')}"
    assert row["icon"] == "icon-echo", f"designPage icon 应回显保存值: {row.get('icon')}"


# ─── issues/129：空串 operator 与缺键同档，归属谓词空值不得读全库 ───────────────────

async def _rows_of(facade, action, args):
    """走门面的分页出口取行数（code 非 0 直接红）"""
    r = await facade.flow(action, args)
    assert r["code"] == 0, (action, args, r)
    return r["data"]["rows"]


async def _seed_two_users(eng, repo, facade):
    """造"user1 有自己的行 + zhangsan 也有自己的行"——空串档若折成不过滤，两者行数立刻不同"""
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        c1 = f.read()
    await facade.flow("processDefine/deploy", {"content": c1})
    d1 = await repo.find_define_by_name("simple")
    await facade.flow("processInstance/startAndExecute", {"processDefineId": d1.id, "operator": "user1"})
    r = await facade.flow("processInstance/startAndExecute", {"processDefineId": d1.id, "operator": "zhangsan"})
    assert r["code"] == 0, r
    await facade.flow("processInstance/createCCInstance", {
        "processInstanceId": r["data"]["processInstanceId"],
        "operator": "zhangsan", "actorIds": ["user1", "zhangsan"]})


async def test_facade_empty_operator_is_same_as_absent_key():
    """issues/129 案 A 第一层：`{"operator":""}`（含全空白）视同未传，一并回落 demo 缺省 user1。

    修前 `str(args.get("operator", "user1"))` 的缺省只在**键不存在**时生效 ⇒ 空串原样穿过；
    内存仓储 `if operator and ...` 又把空串折成"这次不过滤" ⇒ 我的列表读全库
    （160 python demo 实测：空串档 25 行 vs user1 档 4 行，行上是别人的 operator）。
    """
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    await _seed_two_users(eng, repo, facade)

    # 正向对照：user1 档非空，否则下面"三档相等"是 0==0 的自等假绿
    assert len(await _rows_of(facade, "processInstance/page", {"operator": "user1"})) == 1
    assert len(await _rows_of(facade, "processInstance/ccList", {"operator": "user1"})) == 1
    assert len(await _rows_of(facade, "processTask/doneList", {"operator": "user1"})) >= 1
    assert len(await _rows_of(facade, "processTask/todoList", {"operator": "leader"})) >= 1

    for action in ("processInstance/page", "processTask/todoList",
                   "processTask/doneList", "processInstance/ccList"):
        empty = len(await _rows_of(facade, action, {"operator": ""}))
        blank = len(await _rows_of(facade, action, {"operator": "   "}))
        absent = len(await _rows_of(facade, action, {}))
        as_user1 = len(await _rows_of(facade, action, {"operator": "user1"}))
        assert empty == absent, f"{action} 空串档应＝缺键档: empty={empty} absent={absent}"
        assert empty == as_user1, f"{action} 空串档应＝显式 user1 档: empty={empty} user1={as_user1}"
        assert empty == blank, f"{action} 全空白应与空串同档: empty={empty} blank={blank}"

    # 反向哨兵：待办在 leader 手里，user1 档 0 行。谁把"空值"实现成"不加条件"（本 issue 的生产
    # 症状），空串档就会读出 leader 那条 ⇒ 这两格挡的是"假修"。
    assert len(await _rows_of(facade, "processTask/todoList", {"operator": ""})) == 0
    assert len(await _rows_of(facade, "processTask/todoList", {"operator": "leader"})) >= 1


async def test_memory_repo_blank_ownership_yields_empty_page():
    """issues/129 案 A 第二层（内存仓储）：绕过门面直接传空串归属值 ⇒ 空页。

    同时钉住既有 SPI：`None` 是"本次不带归属过滤"（与 java 里"压根没加这条条件"同形），
    不动它——只有"显式传了个空串"才是病灶。
    """
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    await _seed_two_users(eng, repo, facade)

    for blank in ("", "   ", "\t"):
        rows, total = await repo.page_instances(1, 50, blank)
        assert (len(rows), total) == (0, 0), f"实例归属列空串应得空页，实得 {len(rows)}/{total}: {blank!r}"
        rows, total = await repo.page_todo_tasks(1, 50, blank)
        assert (len(rows), total) == (0, 0), f"待办归属列空串应得空页，实得 {len(rows)}/{total}: {blank!r}"
        rows, total = await repo.page_done_tasks(1, 50, blank)
        assert (len(rows), total) == (0, 0), f"已办归属列空串应得空页，实得 {len(rows)}/{total}: {blank!r}"
        rows, total = await repo.page_cc_instances(1, 50, blank)
        assert (len(rows), total) == (0, 0), f"抄送归属列空串应得空页，实得 {len(rows)}/{total}: {blank!r}"

    # 正向对照：真值命中，"恒 0"不是假绿
    rows, total = await repo.page_instances(1, 50, "user1")
    assert (len(rows), total) == (1, 1), (len(rows), total)
    rows, total = await repo.page_cc_instances(1, 50, "user1")
    assert (len(rows), total) == (1, 1), (len(rows), total)
    # 既有 SPI 语义回归：None＝不带归属过滤（两单都回来）
    rows, total = await repo.page_instances(1, 50, None)
    assert total == 2, f"None 仍应是不带归属过滤（本栈既有语义）: {total}"


def test_jdbc_build_where_ownership_blank_is_empty_page():
    """issues/129 案 A 第二层（SQL 仓储）：m_ 条件里归属列拿空值 ⇒ `AND 1=0` 而非"这条不加"。

    只验拼出的 WHERE 文本，不连库（JDBC 套的真实读写在 tests/jdbc_test.py，本机无 MySQL 时跳过）。
    同时留一格改动面哨兵：**非归属列**的空值仍走通用放行（可选过滤不许改成空页）。
    """
    from jeeflow.repository.base import JdbcRepository
    from jeeflow.spi import QueryCondition

    repo = JdbcRepository.__new__(JdbcRepository)  # _build_where 不碰 self/连接，纯拼串
    inst_wl = {"t.operator", "t.business_no", "pd.name"}

    for val in ("", "   ", None):
        sql, args = repo._build_where([QueryCondition("t.operator", "EQ", val)], inst_wl)
        assert sql == " AND 1=0", f"归属列空值应拼成空页条件，实得 {sql!r}: {val!r}"
        assert args == (), f"空页条件不该带绑定参数: {args}"

    # 非归属列空值：通用放行保持原样（整条不加）
    sql, args = repo._build_where([QueryCondition("t.business_no", "LIKE", "")], inst_wl)
    assert sql == "", f"可选过滤的空值仍应被忽略，实得 {sql!r}"
    # 非归属列空值 + 归属列真值：归属照常生效，可选那条被忽略
    sql, args = repo._build_where(
        [QueryCondition("t.operator", "EQ", "user1"), QueryCondition("t.business_no", "LIKE", "")], inst_wl)
    assert sql == " AND t.operator = ?" and args == ("user1",), (sql, args)
    # 归属列的**非空值**不受影响
    sql, args = repo._build_where([QueryCondition("t.operator", "EQ", "user1")], inst_wl)
    assert sql == " AND t.operator = ?" and args == ("user1",), (sql, args)
    # 归属列走非 EQ 操作符时不接管（本 issue 只收 EQ 归属谓词）
    sql, args = repo._build_where([QueryCondition("t.operator", "LIKE", "")], inst_wl)
    assert sql == "", f"LIKE 空值仍走通用放行，实得 {sql!r}"


@pytest.mark.asyncio
async def test_design_deploy_redeploy_is_deployed():
    """issues/08：部署/重新部署/设计稿变更的 is_deployed 状态同步"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        content = f.read()
    with open(os.path.join(FLOW_DIR, "02-multi-task.json"), encoding="utf-8") as f:
        content2 = f.read()

    # 保存（含内容快照）→ 未部署
    r = await facade.flow("processDesign/save", {"name": "leave08", "displayName": "请假流程08",
                                                 "content": content, "operator": "zhangsan"})
    assert r["code"] == 0, r
    design_id = int(r["data"]["id"])
    assert (await facade._ext.find_design_by_id(design_id)).isDeployed == 0

    # 部署 → is_deployed=1
    r = await facade.flow("processDesign/deploy", {"id": design_id, "operator": "zhangsan"})
    assert r["code"] == 0, r
    define_id = r["data"]["processDefineId"]
    assert (await facade._ext.find_design_by_id(design_id)).isDeployed == 1
    version_after_deploy = (await repo.find_define_by_id(int(define_id))).version

    # 重新部署 → 同一 defineId + is_deployed=1
    r = await facade.flow("processDesign/redeploy", {"id": design_id, "operator": "zhangsan"})
    assert r["code"] == 0, r
    assert r["data"]["processDefineId"] == define_id, r
    assert (await facade._ext.find_design_by_id(design_id)).isDeployed == 1
    # issues/59：redeploy 是替换语义，version 必须保持
    assert (await repo.find_define_by_id(int(define_id))).version == version_after_deploy

    # 设计稿内容变更（updateDefine，不同 content）→ 新快照 + is_deployed=0 + name 同步
    r = await facade.flow("processDesign/updateDefine", {"processDesignId": design_id,
                                                         "content": content2, "operator": "zhangsan"})
    assert r["code"] == 0, r
    design = await facade._ext.find_design_by_id(design_id)
    assert design.isDeployed == 0, r
    assert design.name == "multi-task", design.name
    assert len(await facade._ext.list_design_his(design_id)) == 2

    # 基本信息修改（update）→ is_deployed 不变
    r = await facade.flow("processDesign/update", {"id": design_id, "displayName": "改名08",
                                                   "operator": "zhangsan"})
    assert r["code"] == 0, r
    design = await facade._ext.find_design_by_id(design_id)
    assert design.displayName == "改名08" and design.isDeployed == 0

    # 部署 → 再置 1
    r = await facade.flow("processDesign/deploy", {"id": design_id, "operator": "zhangsan"})
    assert r["code"] == 0, r
    assert (await facade._ext.find_design_by_id(design_id)).isDeployed == 1

    # issues/59 强回归：把定义 version 抬到 >0 后 redeploy 必须保持
    define_id2 = int(r["data"]["processDefineId"])
    def_v1 = await repo.find_define_by_id(define_id2)
    def_v1.version = 5
    await repo.update_define(def_v1)
    r = await facade.flow("processDesign/redeploy", {"id": design_id, "operator": "zhangsan"})
    assert r["code"] == 0, r
    assert (await repo.find_define_by_id(define_id2)).version == 5


@pytest.mark.asyncio
async def test_form_data_contract():
    """issues/15：formData / taskFormData / 审批记录 ext 契约"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, None)
    df = load_flow(repo, "01-simple.json")

    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": df.id, "operator": "zhangsan",
                           "f_reasonType": "休假", "f_amount": 500})
    assert r["code"] == 0, r
    inst_id = r["data"]["processInstanceId"]

    r = await facade.flow("processInstance/detail", {"id": inst_id})
    assert r["code"] == 0, r
    data = r["data"]
    form_data = data.get("formData") or {}
    assert form_data.get("f_reasonType") == "休假", r
    assert form_data.get("reasonType") == "休假", r
    assert data.get("name") == "01-simple.json", r
    assert data.get("displayName"), r
    assert "version" in data, r

    # 执行任务（tf_ 前缀变量）→ doneList 行 taskFormData + approvalRecord ext
    r = await facade.flow("processTask/todoList", {"operator": "leader"})
    task_id = r["data"]["rows"][0]["id"]
    r = await facade.flow("processTask/execute",
                          {"processTaskId": task_id, "operator": "leader", "tf_approvalComment": "同意"})
    assert r["code"] == 0, r

    r = await facade.flow("processTask/doneList", {"operator": "leader"})
    tfd = r["data"]["rows"][0].get("taskFormData") or {}
    assert tfd.get("tf_approvalComment") == "同意", r
    assert tfd.get("approvalComment") == "同意", r

    r = await facade.flow("processInstance/approvalRecord", {"id": inst_id})
    assert r["code"] == 0, r
    assert any(row.get("ext") is not None for row in r["data"]), r


@pytest.mark.asyncio
async def test_snowflake_id_string_roundtrip():
    """Java 雪花 id（>2^53）跨语言共享（issue 38 E9）：入口字符串精确 + 出口 string"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        content = f.read()
    snow = 2084320543834124290
    await repo.save_define(ProcessDefine(id=snow, name="snow-flow", displayName="雪花流程", type="approval",
                                   state=1, content=content, version=1,
                                   createTime=__import__("datetime").datetime.now(),
                                   updateTime=__import__("datetime").datetime.now(),
                                   createUser="", updateUser=""))
    # 前端回传字符串雪花 id → 引擎精确解析（_to_int 无损）
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": str(snow), "operator": "user1"})
    assert r["code"] == 0, r
    # 出口 id 必须是 string（JS number 无法承载雪花值）
    assert isinstance(r["data"]["processInstanceId"], str), r
    # 列表行 id 也为 string
    r2 = await facade.flow("processDefine/page", {"pageNum": 1, "pageSize": 10})
    assert r2["code"] == 0, r2
    row = [x for x in r2["data"]["rows"] if x["name"] == "snow-flow"][0]
    assert row["id"] == str(snow), row


@pytest.mark.asyncio
async def test_design_detail_his_ids_stringified():
    """issues/76：processDesign/detail 嵌套 his 列表 id 字符串化——dataclass
    列表曾绕过出口 _stringify_ids（asdict 前不认），his[].id / processDesignId
    以 19 位 int 外泄（奇数尾被 float64 四舍五入 off-by-one）。"""
    eng, repo = setup()
    ext = MemoryExtRepository()
    facade = JeeflowFacade(eng, repo, ext)

    snow = 17769128440810003  # 19 位，>2^53，奇数尾
    await ext.save_design(ProcessDesign(id=snow, name="his-flow", displayName="历史流程",
                                        type="approval", isDeployed=0))
    # 两条 his：id 各不相同且都是雪花量级（第二条 +1 验证逐条精确）
    await ext.save_design_his(ProcessDesignHis(id=snow, processDesignId=snow,
                                               content='{"v":2}', createUser="t"))
    await ext.save_design_his(ProcessDesignHis(id=snow - 1, processDesignId=snow,
                                               content='{"v":1}', createUser="t"))

    r = await facade.flow("processDesign/detail", {"id": str(snow)})
    assert r["code"] == 0, r
    d = r["data"]
    # 主 id 字符串（既有契约，回归锚点）
    assert d["id"] == str(snow) and isinstance(d["id"], str), d
    # his 列表必须已是普通 dict（asdict 后），且 id 键为精确字符串
    his = d["his"]
    assert len(his) == 2, d
    for h in his:
        assert isinstance(h, dict), f"his 项应为 dict（dataclass 已被出口 hook 转换）: {type(h)}"
        assert isinstance(h["id"], str), f"his[].id 应为字符串: {h!r}"
        assert isinstance(h["processDesignId"], str), f"his[].processDesignId 应为字符串: {h!r}"
    ids = [h["id"] for h in his]
    # 逐条精确十进制（顺序非契约点）——若 float64 舍入改写奇数尾，字符串值会不同
    assert sorted(ids) == [str(snow - 1), str(snow)], ids
    assert all(h["processDesignId"] == str(snow) for h in his)


@pytest.mark.asyncio
async def test_highlight_node_progress():
    """highLight nodeProgress 成员进度回显（issue 41）：顺序会签进行中/推进/完成"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    with open(os.path.join(FLOW_DIR, "06-countersign-sequential.json"), encoding="utf-8") as f:
        content = f.read()
    r0 = await facade.flow("processDefine/deploy", {"content": content})
    assert r0["code"] == 0, r0
    r1 = await facade.flow("processInstance/startAndExecute",
                           {"processDefineId": r0["data"]["processDefineId"], "operator": "user1"})
    assert r1["code"] == 0, r1
    instance_id = r1["data"]["processInstanceId"]

    hl = await facade.flow("processInstance/highLight", {"id": instance_id})
    assert hl["code"] == 0, hl
    np = hl["data"]["nodeProgress"]
    # 历史节点 apply：发起人 done
    assert np["apply"]["members"][0]["id"] == "user1"
    assert np["apply"]["members"][0]["done"] is True
    # 顺序会签进行中：type=SEQUENTIAL、userA active、userB 无标记
    assert np["task1"]["type"] == "SEQUENTIAL", np["task1"]
    m = np["task1"]["members"]
    assert m[0]["id"] == "userA" and m[0].get("active") is True, m
    # 姓名走 UserProvider SPI 解析（_TestUserProv realName = '用户' + id）
    assert m[0]["name"] == "用户userA", m
    assert m[1]["id"] == "userB" and "done" not in m[1] and "active" not in m[1], m
    # 推进会签：userA done → userB active
    doing = await repo.find_doing_tasks(int(instance_id))
    await repo.add_task_actor(doing[0].id, ["userA"])
    r = await facade.flow("processTask/execute",
                          {"processTaskId": doing[0].id, "operator": "userA", "submitType": 1})
    assert r["code"] == 0, r
    np2 = (await facade.flow("processInstance/highLight", {"id": instance_id}))["data"]["nodeProgress"]
    m2 = np2["task1"]["members"]
    assert m2[0].get("done") is True and m2[1].get("active") is True, m2
    # 全部完成 → 全部 done
    doing2 = await repo.find_doing_tasks(int(instance_id))
    await repo.add_task_actor(doing2[0].id, ["userB"])
    r = await facade.flow("processTask/execute",
                          {"processTaskId": doing2[0].id, "operator": "userB", "submitType": 1})
    assert r["code"] == 0, r
    np3 = (await facade.flow("processInstance/highLight", {"id": instance_id}))["data"]["nodeProgress"]
    m3 = np3["task1"]["members"]
    assert m3[0].get("done") is True and m3[1].get("done") is True and "active" not in m3[1], m3


@pytest.mark.asyncio
async def test_facade_task_detail_perform_type_numeric():
    """taskDetail performType/taskType 出口数字契约（issues/78）：
    普通 0 / 会签 1，与 Java 修复后五语言一致（出口必须是数字，非枚举 name）"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())

    # 普通流程：task1 performType=0 / taskType=0
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        content = f.read()
    r = await facade.flow("processDefine/deploy", {"content": content})
    assert r["code"] == 0, r
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": r["data"]["processDefineId"], "operator": "zhangsan"})
    assert r["code"] == 0, r
    doing = await repo.find_doing_tasks(int(r["data"]["processInstanceId"]))
    assert doing, "应有进行中任务"
    d = await facade.flow("processTask/detail", {"id": doing[0].id, "operator": "leader"})
    assert d["code"] == 0, d
    assert d["data"]["performType"] == 0, f"普通任务 performType 应=0: {d['data']['performType']}"
    assert d["data"]["taskType"] == 0, f"普通任务 taskType 应=0: {d['data']['taskType']}"

    # 会签流程：task1 performType=1
    with open(os.path.join(FLOW_DIR, "06-countersign-sequential.json"), encoding="utf-8") as f:
        cs_content = f.read()
    r2 = await facade.flow("processDefine/deploy", {"content": cs_content})
    assert r2["code"] == 0, r2
    r3 = await facade.flow("processInstance/startAndExecute",
                           {"processDefineId": r2["data"]["processDefineId"], "operator": "user1"})
    assert r3["code"] == 0, r3
    cs_doing = await repo.find_doing_tasks(int(r3["data"]["processInstanceId"]))
    assert cs_doing, "会签应有进行中任务"
    cs = await facade.flow("processTask/detail", {"id": cs_doing[0].id, "operator": "userA"})
    assert cs["code"] == 0, cs
    assert cs["data"]["performType"] == 1, f"会签任务 performType 应=1（非 'COUNTERSIGN'）: {cs['data']['performType']}"


@pytest.mark.asyncio
async def test_perform_type_string_compat():
    """performType 字符串兼容（issue 42）：'ALL' 面板格式会签行为与数字 1 一致"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    with open(os.path.join(FLOW_DIR, "05-countersign-parallel.json"), encoding="utf-8") as f:
        content = f.read()
    # 面板格式：performType 存 'ALL' 字符串
    r0 = await facade.flow("processDefine/deploy", {"content": content.replace('"performType": 1', '"performType": "ALL"')})
    assert r0["code"] == 0, r0
    r1 = await facade.flow("processInstance/startAndExecute",
                           {"processDefineId": r0["data"]["processDefineId"], "operator": "user1"})
    assert r1["code"] == 0, r1
    doing = await repo.find_doing_tasks(int(r1["data"]["processInstanceId"]))
    cs = [t.actorIds[0] for t in doing if t.taskName == "task1"]
    assert len(cs) == 3, f"ALL 格式应生成 3 个会签任务: {cs}"
    assert sorted(cs) == ["userA", "userB", "userC"], cs
    # nodeProgress 对 ALL 格式同样识别为会签
    hl = await facade.flow("processInstance/highLight", {"id": r1["data"]["processInstanceId"]})
    assert hl["code"] == 0, hl
    assert hl["data"]["nodeProgress"]["task1"]["type"] == "PARALLEL", hl["data"]["nodeProgress"]


@pytest.mark.asyncio
async def test_e2e_feedback_regression():
    """E2E 反馈回归（issues 53/52/56/50/54）：撤回状态 30 / performType 落库 / 抄送 / design page / upAndDown 批量"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        content = f.read()
    # 56：发起时抄送
    r0 = await facade.flow("processDefine/deploy", {"content": content})
    r1 = await facade.flow("processInstance/startAndExecute",
                           {"processDefineId": r0["data"]["processDefineId"], "operator": "user1",
                            "f_ccActors": "wangqiang,zhaomin"})
    assert r1["code"] == 0, r1
    cc_rows, cc_total = await repo.page_cc_instances(1, 10, "wangqiang")
    assert cc_total >= 1, f"抄送应创建: {cc_total}"
    # 53：撤回状态 30
    wr = await facade.flow("processInstance/withdraw", {"id": r1["data"]["processInstanceId"], "operator": "user1"})
    assert wr["code"] == 0, wr
    after = await repo.find_instance_by_id(int(r1["data"]["processInstanceId"]))
    assert after.state == InstanceState.WITHDRAW, f"撤回状态应=30: {after.state}"
    # 52：会签 performType 落库
    with open(os.path.join(FLOW_DIR, "05-countersign-parallel.json"), encoding="utf-8") as f:
        cs_content = f.read()
    r2 = await facade.flow("processDefine/deploy", {"content": cs_content})
    r3 = await facade.flow("processInstance/startAndExecute",
                           {"processDefineId": r2["data"]["processDefineId"], "operator": "user1"})
    cs_doing = await repo.find_doing_tasks(int(r3["data"]["processInstanceId"]))
    cs = [t for t in cs_doing if t.taskName == "task1"]
    assert len(cs) == 3 and all(t.performType == 1 for t in cs), f"会签任务 performType 应=1: {[t.performType for t in cs]}"
    # issues/113：会签实例整单撤回时，3 条 doing 会签任务同样落 30——99 留给一票否决的废弃路径
    cs_iid = int(r3["data"]["processInstanceId"])
    cw = await facade.flow("processInstance/withdraw", {"id": cs_iid, "operator": "user1"})
    assert cw["code"] == 0, cw
    for t in cs:
        stored = await repo.find_task_by_id(t.id)
        assert stored is not None and stored.taskState == TaskState.WITHDRAW, \
            f"撤回会签任务态应=30(WITHDRAW)，实测 {getattr(stored, 'taskState', None)}"
    left = await repo.find_doing_tasks(cs_iid)
    assert not left, f"会签实例撤回后仍剩 {len(left)} 条 doing 任务"
    # 54：upAndDown 批量 {ids, opType}
    r4 = await facade.flow("processDefine/upAndDown",
                           {"ids": [r0["data"]["processDefineId"], r2["data"]["processDefineId"]], "opType": 0})
    assert r4["code"] == 0, r4
    d1 = await repo.find_define_by_id(int(r0["data"]["processDefineId"]))
    assert d1.state == 0, f"批量停用应生效: {d1.state}"
    # 50：design page id 字符串化
    r5 = await facade.flow("processDesign/page", {"pageNum": 1, "pageSize": 10})
    assert r5["code"] == 0, r5
    if r5["data"]["rows"]:
        assert isinstance(r5["data"]["rows"][0]["id"], str), f"design id 应为 string: {r5['data']['rows'][0]['id']}"


@pytest.mark.asyncio
async def test_page_envelope_five_keys():
    """issues/64：门面分页必须五键 pageNum/pageSize/rows/recordCount/totalPage"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        content = f.read()
    r0 = await facade.flow("processDefine/deploy", {"content": content})
    assert r0["code"] == 0, r0
    r = await facade.flow("processDefine/page", {"pageNum": 1, "pageSize": 1})
    assert r["code"] == 0, r
    data = r["data"]
    for k in ("pageNum", "pageSize", "rows", "recordCount", "totalPage"):
        assert k in data, f"缺 {k}: {data}"
    assert data["pageNum"] == 1
    assert data["pageSize"] == 1
    assert data["recordCount"] >= 1
    assert data["totalPage"] == data["recordCount"]  # pageSize=1
    eng0, repo0 = setup()
    empty = await JeeflowFacade(eng0, repo0, MemoryExtRepository()).flow(
        "processDefine/page", {"pageNum": 1, "pageSize": 10})
    assert empty["code"] == 0, empty
    assert empty["data"]["recordCount"] == 0
    assert empty["data"]["totalPage"] == 0


# ═══ 82-1 时间格式 + 82-3 列表行 instanceExt 容器（对齐 Node spec it 19）═══

TIME_RE = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"


@pytest.mark.asyncio
async def test_list_row_time_format_and_instance_ext():
    """issues/82-1：列表行时间 yyyy-MM-dd HH:mm:ss（无 T，Python 此前完全缺）
    issues/82-3：列表行 instanceExt / ext 容器（待办/已办/我发起/抄送）"""
    import re
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        content = f.read()
    r0 = await facade.flow("processDefine/deploy", {"content": content})
    assert r0["code"] == 0, r0
    r1 = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": r0["data"]["processDefineId"], "operator": "zhangsan"})
    assert r1["code"] == 0, r1
    instance_id = int(r1["data"]["processInstanceId"])

    # todoList：ext + instanceExt + version + 时间格式
    r2 = await facade.flow("processTask/todoList", {"operator": "leader"})
    assert r2["code"] == 0, r2
    assert len(r2["data"]["rows"]) > 0, r2
    row = r2["data"]["rows"][0]
    assert isinstance(row.get("ext"), dict), row
    assert isinstance(row.get("instanceExt"), dict), row
    assert row.get("version") is not None, row
    assert re.match(TIME_RE, row["createTime"]), f"createTime 应 yyyy-MM-dd HH:mm:ss（无 T）: {row['createTime']}"
    assert "T" not in row["createTime"], row["createTime"]

    # 完成任务 → doneList：finishTime 同样格式化
    doing = await repo.find_doing_tasks(instance_id)
    assert len(doing) == 1 and doing[0].taskName == "task1", doing
    r_exec = await facade.flow("processTask/execute",
                               {"processTaskId": doing[0].id, "operator": "leader", "submitType": 1})
    assert r_exec["code"] == 0, r_exec
    r3 = await facade.flow("processTask/doneList", {"operator": "leader"})
    assert r3["code"] == 0, r3
    assert len(r3["data"]["rows"]) > 0, r3
    drow = r3["data"]["rows"][0]
    assert isinstance(drow.get("ext"), dict), drow
    assert isinstance(drow.get("instanceExt"), dict), drow
    assert drow.get("version") is not None, drow
    assert re.match(TIME_RE, drow["finishTime"]), f"finishTime 应 yyyy-MM-dd HH:mm:ss: {drow['finishTime']}"
    assert re.match(TIME_RE, drow["createTime"]), f"createTime 应 yyyy-MM-dd HH:mm:ss: {drow['createTime']}"

    # instancePage：ext（实例变量对象，对齐 Java/Go 契约：实例行无 instanceExt 键）+ 时间格式
    r4 = await facade.flow("processInstance/page", {"operator": "zhangsan"})
    assert r4["code"] == 0, r4
    assert len(r4["data"]["rows"]) > 0, r4
    irow = r4["data"]["rows"][0]
    assert isinstance(irow.get("ext"), dict), irow
    assert irow.get("displayName"), irow
    assert irow.get("version") is not None, irow
    assert re.match(TIME_RE, irow["createTime"]), f"实例行时间应 yyyy-MM-dd HH:mm:ss: {irow['createTime']}"

    # ccList：ext + 时间格式
    r5 = await facade.flow("processInstance/createCCInstance",
                           {"processInstanceId": instance_id, "operator": "zhangsan", "actorIds": ["lisi"]})
    assert r5["code"] == 0, r5
    r6 = await facade.flow("processInstance/ccList", {"operator": "lisi"})
    assert r6["code"] == 0, r6
    assert len(r6["data"]["rows"]) > 0, r6
    crow = r6["data"]["rows"][0]
    assert isinstance(crow.get("ext"), dict), crow
    assert re.match(TIME_RE, crow["createTime"]), f"抄送行时间应 yyyy-MM-dd HH:mm:ss: {crow['createTime']}"


# ═══ 82-5 task detail 任务级 ext.isFirstTaskNode（前端 detail.vue 双兜底）═══

@pytest.mark.asyncio
async def test_task_detail_ext_is_first_task_node():
    """issues/82-5：task detail 补任务级 ext.isFirstTaskNode（对齐 Java 1912456）
    场景 1：startAndExecute 自动完成 apply → 剩 task1（DOING，非首节点）→ False
    场景 2：直接启动（不自动完成 apply）→ apply 为首任务节点且 DOING → True"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        content = f.read()
    r0 = await facade.flow("processDefine/deploy", {"content": content})
    assert r0["code"] == 0, r0
    define_id = int(r0["data"]["processDefineId"])

    # 场景 1：startAndExecute → task1 DOING 非首节点
    r1 = await facade.flow("processInstance/startAndExecute",
                           {"processDefineId": define_id, "operator": "zhangsan"})
    assert r1["code"] == 0, r1
    instance_id = int(r1["data"]["processInstanceId"])
    task1_id = await _doing_task_id(repo, instance_id, "task1")
    assert task1_id, "应有 task1 进行中任务"
    r = await facade.flow("processTask/detail", {"id": task1_id, "operator": "leader"})
    assert r["code"] == 0, r
    ext = r["data"]["ext"]
    assert isinstance(ext, dict), r["data"]
    assert ext["isFirstTaskNode"] is False, ext

    # 场景 2：直接启动（不走 startAndExecute 的自动完成）→ apply 为首任务节点且 DOING
    eng2, repo2 = setup()
    facade2 = JeeflowFacade(eng2, repo2, MemoryExtRepository())
    df = ProcessDefine(name="simple", displayName="简单流程", type="approval", state=1)
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        df.content = f.read()
    repo2.add_define(df)
    inst2 = await eng2.start_process_instance_by_id(df.id, "zhangsan", None)
    apply_id = await _doing_task_id(repo2, inst2.id, "apply")
    assert apply_id, "apply 应为进行中任务"
    r = await facade2.flow("processTask/detail", {"id": apply_id, "operator": "zhangsan"})
    assert r["code"] == 0, r
    ext2 = r["data"]["ext"]
    assert isinstance(ext2, dict), r["data"]
    assert ext2["isFirstTaskNode"] is True, ext2


# ═══ 按 id 查"记录不存在"负向（对齐 PHP 6 处模板 / Java 1912456）═══

@pytest.mark.asyncio
async def test_detail_by_id_not_found():
    """issues/82 负向：define/instance/design/task 按 id 查不存在 → 99999999 + 明确 msg"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())

    r = await facade.flow("processDefine/detail", {"id": 999999999999999999})
    assert r["code"] == 99999999, r
    assert "流程定义不存在" in r["msg"], r

    r = await facade.flow("processInstance/detail", {"id": 999999999999999999})
    assert r["code"] == 99999999, r
    assert "流程实例不存在" in r["msg"], r

    r = await facade.flow("processDesign/detail", {"id": 999999999999999999})
    assert r["code"] == 99999999, r
    assert "流程设计不存在" in r["msg"], r

    r = await facade.flow("processTask/detail", {"id": 999999999999999999, "operator": "leader"})
    assert r["code"] == 99999999, r
    assert "任务不存在" in r["msg"], r


@pytest.mark.asyncio
async def test_create_cc_instance_empty_actors():
    """issues/82 负向：抄送空 actors 报错（对齐 Java/Go/PHP 基准）。
    createCCInstance 空/缺失 actorIds → 99999999 + msg 含 'actorIds 缺失'。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())

    # 空 actorIds list
    r = await facade.flow("processInstance/createCCInstance",
                          {"processInstanceId": 123, "operator": "user1", "actorIds": []})
    assert r["code"] == 99999999, r
    assert "actorIds 缺失" in r["msg"], r

    # 负向边界：actorIds 键完全缺失同样报错
    r = await facade.flow("processInstance/createCCInstance",
                          {"processInstanceId": 123, "operator": "user1"})
    assert r["code"] == 99999999, r
    assert "actorIds 缺失" in r["msg"], r


@pytest.mark.asyncio
async def test_snowflake_id_precision_guard():
    """issues/82 负向（对齐 Go TestSnowflakeIDPrecision / Node toId / Java toLong / issues/38 E9）：
    雪花 id 精度守卫。浮点型 id 超 2^53（json 解析 / 调用方 float 已丢精度）→ 显性报错，
    不 int() 静默截断；字符串雪花 id → 精确解析（无该定义 → 报 'define not found' 含原始 id）。
    注：Python json 整数本为任意精度 int 精确，故此路径仅在显式传 float 时触发（防御性对齐五语言）。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())

    # ① 浮点雪花 id（> 2^53，精度已丢）→ 显性报错
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": 2084320543834124288.0, "operator": "user1"})
    assert r["code"] == 99999999, r
    assert "超出 float64 精确范围" in r["msg"], r

    # ② 字符串雪花 id → 精确解析（无该定义 → define not found 含原始完整 id，且不崩溃）
    SNOW = "2084320543834124290"
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": SNOW, "operator": "user1"})
    assert r["code"] == 99999999, r
    assert SNOW in r["msg"], f"字符串应精确解析（消息应含原始雪花 id）: {r['msg']}"


# ═══ execute submitType 2/3/4/5/6/20 门面行为（issues/79，前端按钮全量暴露路径）═══

async def _start_multi_task_at(facade, repo, name: str) -> int:
    """02-multi-task：发起（apply 自动完成）→ 推进到名为 name 的任务节点"""
    with open(os.path.join(FLOW_DIR, "02-multi-task.json"), encoding="utf-8") as f:
        r0 = await facade.flow("processDefine/deploy", {"content": f.read()})
    assert r0["code"] == 0, r0
    r1 = await facade.flow("processInstance/startAndExecute",
                           {"processDefineId": r0["data"]["processDefineId"], "operator": "zhangsan"})
    assert r1["code"] == 0, r1
    instance_id = int(r1["data"]["processInstanceId"])
    order = ["task1", "task2", "task3"]
    actor = ["leader", "manager", "boss"]
    target = order.index(name)
    for i in range(target):
        doing = await repo.find_doing_tasks(instance_id)
        tid = next((t.id for t in doing if t.taskName == order[i]), None)
        assert tid, f"应推进到 {order[i]}"
        await repo.add_task_actor(tid, [actor[i]])
        r = await facade.flow("processTask/execute",
                              {"processTaskId": tid, "operator": actor[i], "submitType": 1})
        assert r["code"] == 0, r
    return instance_id


async def _doing_task_id(repo, instance_id: int, name: str):
    for t in await repo.find_doing_tasks(instance_id):
        if t.taskName == name:
            return t.id
    return None


@pytest.mark.asyncio
async def test_facade_execute_submit_type_behavior():
    """issues/79：submitType 3/4/5/6 + 负向（对齐 Java 参考实现断言）"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())

    # ── submitType=3 ROLLBACK（血缘版 issues/121 P2）：task2 退回 → 复活 task1 那条历史行，
    #    参与者＝task1 的原办结人 leader（不是执行回退的 manager），实例保持 DOING(10)
    rb = await _start_multi_task_at(facade, repo, "task2")
    t2 = await _doing_task_id(repo, rb, "task2")
    await repo.add_task_actor(t2, ["manager"])
    r = await facade.flow("processTask/execute",
                          {"processTaskId": t2, "operator": "manager", "submitType": 3})
    assert r["code"] == 0, r
    rb_task1 = await _doing_task_id(repo, rb, "task1")
    assert rb_task1, "ROLLBACK 应在 task1 产生新待办"
    rb1_actors = await repo.find_task_actors(rb_task1)
    assert "leader" in rb1_actors and "manager" not in rb1_actors, \
        f"血缘版：复活行 actor 应为 task1 原办结人 leader，不该是执行回退的 manager：{rb1_actors}"
    assert (await repo.find_instance_by_id(rb)).state == InstanceState.DOING

    # ── submitType=4 JUMP：task3 跳转 apply（首任务节点 = start 直接后继，assignee 强制发起人）
    jp = await _start_multi_task_at(facade, repo, "task3")
    t3 = await _doing_task_id(repo, jp, "task3")
    await repo.add_task_actor(t3, ["boss"])
    jl = await facade.flow("processTask/jumpAbleTaskNameList", {"processInstanceId": jp})
    assert jl["code"] == 0, jl
    jump_values = [m["value"] for m in jl["data"]]
    assert "task1" in jump_values and "apply" in jump_values, jump_values
    r = await facade.flow("processTask/execute",
                          {"processTaskId": t3, "operator": "boss", "submitType": 4, "taskName": "apply"})
    assert r["code"] == 0, r
    jp_apply = await _doing_task_id(repo, jp, "apply")
    assert jp_apply, "JUMP 应在 apply（首任务节点）产生新待办"
    assert await repo.find_task_actors(jp_apply) == ["zhangsan"], "跳首任务节点 assignee 强制为发起人"
    assert (await repo.find_instance_by_id(jp)).state == InstanceState.DOING

    # ── 负向：JUMP taskName 不存在 → 99999999 + 「无法找到节点模型」
    jn = await _start_multi_task_at(facade, repo, "task2")
    t2n = await _doing_task_id(repo, jn, "task2")
    await repo.add_task_actor(t2n, ["manager"])
    jr = await facade.flow("processTask/execute",
                           {"processTaskId": t2n, "operator": "manager", "submitType": 4, "taskName": "no-such-node"})
    assert jr["code"] == 99999999, jr
    assert "无法找到节点模型" in str(jr["msg"]), jr["msg"]

    # ── submitType=5 RE_APPLY：task1 重新提交（前端 detail 抽屉场景，含 f_ 表单 + tf_nextNodeOperator）
    ra = await _start_multi_task_at(facade, repo, "task1")
    t1r = await _doing_task_id(repo, ra, "task1")
    await repo.add_task_actor(t1r, ["leader"])
    r = await facade.flow("processTask/execute",
                          {"processTaskId": t1r, "operator": "leader", "submitType": 5,
                           "tf_nextNodeOperator": "manager", "f_leaveType": "annual"})
    assert r["code"] == 0, r
    doing_after = await repo.find_doing_tasks(ra)
    assert len(doing_after) == 1 and doing_after[0].taskName == "task2", doing_after
    assert await repo.find_task_actors(doing_after[0].id) == ["manager"], "tf_nextNodeOperator 应覆盖 task2 处理人"
    inst_ra = await repo.find_instance_by_id(ra)
    assert inst_ra.variables.get("f_leaveType") == "annual", "f_ 表单字段应落实例变量"
    assert inst_ra.state == InstanceState.DOING

    # ── submitType=6 ROLLBACK_TO_OPERATOR：task3 退回发起人 → apply 重执行、actor=发起人 zhangsan
    ro = await _start_multi_task_at(facade, repo, "task3")
    t3o = await _doing_task_id(repo, ro, "task3")
    await repo.add_task_actor(t3o, ["boss"])
    r = await facade.flow("processTask/execute",
                          {"processTaskId": t3o, "operator": "boss", "submitType": 6})
    assert r["code"] == 0, r
    ro_apply = await _doing_task_id(repo, ro, "apply")
    assert ro_apply, "ROLLBACK_TO_OPERATOR 应重执行首个任务节点 apply"
    assert await repo.find_task_actors(ro_apply) == ["zhangsan"], "退回发起人 assignee 强制为发起人"
    assert (await repo.find_instance_by_id(ro)).state == InstanceState.DOING

    # ── 负向：非处理人执行被拒（NOT_ALLOWED_EXECUTE）
    na = await _start_multi_task_at(facade, repo, "task1")
    t1n = await _doing_task_id(repo, na, "task1")
    nr = await facade.flow("processTask/execute",
                           {"processTaskId": t1n, "operator": "hacker", "submitType": 1})
    assert nr["code"] == 99999999, nr
    assert "not allowed" in str(nr["msg"]), nr["msg"]


@pytest.mark.asyncio
async def test_facade_execute_reject():
    """issues/79：submitType=2 REJECT 门面参数路径（对齐 Java/Go/PHP）"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    inst_id = await _start_multi_task_at(facade, repo, "task1")
    t1 = await _doing_task_id(repo, inst_id, "task1")
    await repo.add_task_actor(t1, ["leader"])
    r = await facade.flow("processTask/execute",
                          {"processTaskId": t1, "operator": "leader", "submitType": 2})
    assert r["code"] == 0, r
    assert (await repo.find_instance_by_id(inst_id)).state == InstanceState.REJECT
    assert len(await repo.find_doing_tasks(inst_id)) == 0, "REJECT 后应无 DOING 任务"


async def _doing_task_id_by_actor(repo, instance_id: int, name: str, actor: str):
    """会签场景：同节点多个 DOING 任务（每 actor 一个），按 actor 定位"""
    for t in await repo.find_doing_tasks(instance_id):
        if t.taskName != name:
            continue
        if actor in (t.actorIds or []):
            return t.id
    return None


@pytest.mark.asyncio
async def test_facade_execute_countersign_disagree_soft():
    """issues/91：未配 ONE_VOTE_VETO 时 submitType=20 为软拒绝（06 串行推进到下一成员）"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    # 06-countersign-sequential：apply 自动完成 → task1 串行会签（逐人创建，先 userA）
    with open(os.path.join(FLOW_DIR, "06-countersign-sequential.json"), encoding="utf-8") as f:
        r0 = await facade.flow("processDefine/deploy", {"content": f.read()})
    assert r0["code"] == 0, r0
    r1 = await facade.flow("processInstance/startAndExecute",
                           {"processDefineId": r0["data"]["processDefineId"], "operator": "user1"})
    assert r1["code"] == 0, r1
    instance_id = int(r1["data"]["processInstanceId"])
    task_a = await _doing_task_id_by_actor(repo, instance_id, "task1", "userA")
    assert task_a, "会签节点应有 userA 的 DOING 任务"
    await repo.add_task_actor(task_a, ["userA"])
    # submitType=20（未配 ONE_VOTE_VETO → 软拒绝）：flag 记录，流程不阻断，串行推进到下一成员
    r = await facade.flow("processTask/execute",
                          {"processTaskId": task_a, "operator": "userA", "submitType": 20})
    assert r["code"] == 0, r
    inst = await repo.find_instance_by_id(instance_id)
    assert inst.state == InstanceState.DOING, f"软拒绝后实例应保持 DOING(10)，继续等 userB: {inst.state}"
    assert int(inst.variables.get("countersignDisagreeFlag")) == 1, "countersignDisagreeFlag=1 应落实例变量"
    done_a = await repo.find_task_by_id(task_a)
    assert done_a.taskState == TaskState.DONE, "软拒绝任务应正常完成"
    assert int(done_a.variables.get("countersignDisagreeFlag")) == 1, "countersignDisagreeFlag=1 应落任务变量"
    assert done_a.actorId == "userA", "否决人应记录为实际操作人 userA"
    # 软拒绝推进串行会签到下一成员：userB 任务应被创建且 DOING
    assert await _doing_task_id_by_actor(repo, instance_id, "task1", "userB"), \
        "软拒绝后串行会签应推进到 userB（DOING）"


@pytest.mark.asyncio
async def test_facade_execute_countersign_one_vote_veto():
    """issues/91：13（并行 + ONE_VOTE_VETO）→ 任一成员 submitType=20 一票否决
    → 会签节点立即推进 end（实例 DONE），其余 DOING 会签任务废弃(ABANDONED 99)"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    with open(os.path.join(FLOW_DIR, "13-countersign-one-vote-veto.json"), encoding="utf-8") as f:
        r0 = await facade.flow("processDefine/deploy", {"content": f.read()})
    assert r0["code"] == 0, r0
    r1 = await facade.flow("processInstance/startAndExecute",
                           {"processDefineId": r0["data"]["processDefineId"], "operator": "user1"})
    assert r1["code"] == 0, r1
    instance_id = int(r1["data"]["processInstanceId"])
    # 并行会签全员预创建：userA/userB/userC 三个 DOING 任务
    task_a = await _doing_task_id_by_actor(repo, instance_id, "task1", "userA")
    task_b = await _doing_task_id_by_actor(repo, instance_id, "task1", "userB")
    task_c = await _doing_task_id_by_actor(repo, instance_id, "task1", "userC")
    assert task_a and task_b and task_c, "并行会签应预创建 userA/userB/userC 三个 DOING 任务"
    await repo.add_task_actor(task_a, ["userA"])
    # userA 会签不同意（已配 ONE_VOTE_VETO → 一票否决）
    r = await facade.flow("processTask/execute",
                          {"processTaskId": task_a, "operator": "userA", "submitType": 20})
    assert r["code"] == 0, r
    inst = await repo.find_instance_by_id(instance_id)
    assert inst.state == InstanceState.DONE, f"一票否决后会签节点应立即推进 end（实例 DONE 20）: {inst.state}"
    assert int(inst.variables.get("countersignDisagreeFlag")) == 1, "countersignDisagreeFlag=1 应落实例变量"
    done_a = await repo.find_task_by_id(task_a)
    assert done_a.taskState == TaskState.DONE, "否决任务应已完成"
    assert done_a.actorId == "userA", "否决人应记录为实际操作人 userA"
    # 否决应废弃其余成员（ABANDONED 99）
    for tid in (task_b, task_c):
        tk = await repo.find_task_by_id(tid)
        assert tk and tk.taskState == TaskState.ABANDONED, f"否决应废弃其余成员任务为 ABANDONED(99): id={tid}"
    assert len(await repo.find_doing_tasks(instance_id)) == 0, "否决后应无 DOING 任务"


@pytest.mark.asyncio
async def test_facade_execute_countersign_disagree_parallel_soft():
    """issues/91：05 并行（未配 ONE_VOTE_VETO）submitType=20 软拒绝
    ——否决者任务完成、flag 记录、流程不阻断，其余成员仍 DOING"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    with open(os.path.join(FLOW_DIR, "05-countersign-parallel.json"), encoding="utf-8") as f:
        r0 = await facade.flow("processDefine/deploy", {"content": f.read()})
    assert r0["code"] == 0, r0
    r1 = await facade.flow("processInstance/startAndExecute",
                           {"processDefineId": r0["data"]["processDefineId"], "operator": "user1"})
    assert r1["code"] == 0, r1
    instance_id = int(r1["data"]["processInstanceId"])
    task_a = await _doing_task_id_by_actor(repo, instance_id, "task1", "userA")
    task_b = await _doing_task_id_by_actor(repo, instance_id, "task1", "userB")
    task_c = await _doing_task_id_by_actor(repo, instance_id, "task1", "userC")
    assert task_a and task_b and task_c, "并行会签应预创建 userA/userB/userC 三个 DOING 任务"
    await repo.add_task_actor(task_a, ["userA"])
    # userA 会签不同意（未配 ONE_VOTE_VETO → 软拒绝）
    r = await facade.flow("processTask/execute",
                          {"processTaskId": task_a, "operator": "userA", "submitType": 20})
    assert r["code"] == 0, r
    inst = await repo.find_instance_by_id(instance_id)
    assert inst.state == InstanceState.DOING, f"并行软拒绝后实例应保持 DOING(10)，等 userB/userC: {inst.state}"
    assert int(inst.variables.get("countersignDisagreeFlag")) == 1, "countersignDisagreeFlag=1 应落实例变量"
    done_a = await repo.find_task_by_id(task_a)
    assert done_a.taskState == TaskState.DONE, "软拒绝任务应正常完成"
    for tid in (task_b, task_c):
        tk = await repo.find_task_by_id(tid)
        assert tk and tk.taskState == TaskState.DOING, f"软拒绝不应废弃其余成员，应保持 DOING: id={tid}"


# ─── issues/96 §4B：门面「入口批量参数形态」矩阵（4 action × 4 态）──────────────────
#
# 补测理由（issues/96 §1）：既有套件对 remove/启停一律发单数 {id}，引擎就算完全不认
# 前端真实载荷 {ids} 也照样全绿——issues/95 六语言全绿仍漏检的根因。本矩阵把入参形态
# 本身钉成断言，四态固定为：
#   态1 {ids:[a,b]} 两个真实 id → 成功且事后回查两条都取不到（前端真实载荷）
#   态2 {id:c}                → 旧形态仍生效（防修 bad；移动端 workflow.uts 发这个）
#   态3 {ids:[]}              → 必须报错，禁止静默成功（含 {ids:[], id:真id} —— ids 优先，
#                                不得回落到单条，否则空数组静默又回来了）
#   态4 {ids:[""]} / 含 None   → 必须报错，且整批不生效（校验前置，不许半途删一半）
#                                另配 {ids:[非法,真id], id:真id} 一格：纯 {ids:[]} 在"只读 id"
#                                的旧实现下也会因回落而报错（恒真），带上 id 才测得出 ids 分支
#                                —— 实测旧实现该格回 code=0 并静默删掉真记录。
# Python 额外前科：_processDefine_remove 当年连批量都没有（issues/95 §6），故 define 方向
# （remove + upAndDown）两格单独落实。


@pytest.mark.asyncio
async def test_facade_surrogate_remove_ids_form_matrix():
    """矩阵①processSurrogate/remove（issues/95 本体 / issues/96 §4B）"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())

    async def save(agent, name):
        r = await facade.flow("processSurrogate/save",
                              {"operator": "zhangsan", "surrogate": agent, "processName": name,
                               "startTime": "2026-08-01 00:00:00", "endTime": "2026-08-31 23:59:59",
                               "enabled": 1})
        assert r["code"] == 0, r
        return int(r["data"]["id"])

    async def gone(sid):
        """事后回查：门面 detail 与仓储两条通道都取不到"""
        r = await facade.flow("processSurrogate/detail", {"id": sid})
        assert r["code"] == 99999999, (sid, r)
        assert await facade._ext.find_surrogate_by_id(sid) is None, sid

    # 态1：{ids} 批量（vben5 process-surrogate/index.vue 勾选删除的真实载荷）
    a = await save("lisiA", "mxLeaveA")
    b = await save("lisiB", "mxLeaveB")
    r = await facade.flow("processSurrogate/remove", {"ids": [a, b]})
    assert r["code"] == 0, r
    await gone(a)
    await gone(b)

    # 态2：单数 {id} 旧形态回归保护
    c = await save("lisiC", "mxLeaveC")
    r = await facade.flow("processSurrogate/remove", {"id": c})
    assert r["code"] == 0, r
    await gone(c)

    # 态3：空数组报错，且带合法 id 也不得回落
    d = await save("lisiD", "mxLeaveD")
    for args in ({"ids": []}, {"ids": [], "id": d}):
        r = await facade.flow("processSurrogate/remove", args)
        assert r["code"] == 99999999, (args, r)
        assert "id 缺失或非法" in r["msg"], (args, r)
    assert await facade._ext.find_surrogate_by_id(d) is not None, f"空 ids 报错不得动数据: {d}"

    # 态4：空串 / 含 None → 报错且整批不生效（带 id 回落格同样必须报错）
    e = await save("lisiE", "mxLeaveE")
    for args in ({"ids": [""]}, {"ids": [e, None]}, {"ids": [e, ""]}, {"ids": [e, None], "id": e}):
        r = await facade.flow("processSurrogate/remove", args)
        assert r["code"] == 99999999, (args, r)
        assert "id 缺失或非法" in r["msg"], (args, r)
    assert await facade._ext.find_surrogate_by_id(e) is not None, f"含非法值应整批拒绝: {e}"
    # 同一批换成合法 ids 仍可删除（证明上一步只是没收到 id，不是该记录删不掉）
    assert (await facade.flow("processSurrogate/remove", {"ids": [e]}))["code"] == 0
    await gone(e)


@pytest.mark.asyncio
async def test_facade_design_remove_ids_form_matrix():
    """矩阵②processDesign/remove（issues/28 已下沉批量，但零入口用例）"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        content = f.read()

    async def save(name):
        r = await facade.flow("processDesign/save",
                              {"name": name, "displayName": f"矩阵{name}", "content": content,
                               "operator": "zhangsan"})
        assert r["code"] == 0, r
        return int(r["data"]["id"])

    async def gone(design_id):
        r = await facade.flow("processDesign/detail", {"id": design_id})
        assert r["code"] == 99999999, (design_id, r)
        assert await facade._ext.find_design_by_id(design_id) is None, design_id

    # 态1
    a = await save("mxDesignA")
    b = await save("mxDesignB")
    r = await facade.flow("processDesign/remove", {"ids": [a, b]})
    assert r["code"] == 0, r
    await gone(a)
    await gone(b)

    # 态2
    c = await save("mxDesignC")
    r = await facade.flow("processDesign/remove", {"id": c})
    assert r["code"] == 0, r
    await gone(c)

    # 态3
    d = await save("mxDesignD")
    for args in ({"ids": []}, {"ids": [], "id": d}):
        r = await facade.flow("processDesign/remove", args)
        assert r["code"] == 99999999, (args, r)
        assert "id 缺失或非法" in r["msg"], (args, r)
    assert await facade._ext.find_design_by_id(d) is not None, f"空 ids 报错不得动数据: {d}"

    # 态4：空串 / 含 None → 报错且整批不生效（带 id 回落格同样必须报错）
    e = await save("mxDesignE")
    for args in ({"ids": [""]}, {"ids": [e, None]}, {"ids": [e, ""]}, {"ids": [e, None], "id": e}):
        r = await facade.flow("processDesign/remove", args)
        assert r["code"] == 99999999, (args, r)
        assert "id 缺失或非法" in r["msg"], (args, r)
    assert await facade._ext.find_design_by_id(e) is not None, f"含非法值应整批拒绝: {e}"
    assert (await facade.flow("processDesign/remove", {"ids": [e]}))["code"] == 0
    await gone(e)


@pytest.mark.asyncio
async def test_facade_define_remove_ids_form_matrix():
    """矩阵③processDefine/remove —— Python 独有漏项（issues/95 §6：五语言有批量、
    Python 连分支都没有），故单独成格。构造沿用同文件方式：deploy 同名流程两次
    → 两条真实 define（version 0/1），再 getLastByName 复核整个 name 已空。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        content = f.read()

    async def deploy():
        r = await facade.flow("processDefine/deploy", {"content": content})
        assert r["code"] == 0, r
        return int(r["data"]["processDefineId"])

    async def gone(define_id):
        r = await facade.flow("processDefine/detail", {"id": define_id})
        assert r["code"] == 99999999, (define_id, r)
        assert await repo.find_define_by_id(define_id) is None, define_id

    # 态1：同名两条版本一次删掉
    a = await deploy()
    b = await deploy()
    assert a != b, f"两次 deploy 应生成两条定义: {a}, {b}"
    r = await facade.flow("processDefine/remove", {"ids": [a, b]})
    assert r["code"] == 0, r
    await gone(a)
    await gone(b)
    # 名称维度复核：simple 已无任何定义
    r = await facade.flow("processDefine/getLastByName", {"processDefineName": "simple"})
    assert r["code"] == 99999999, r

    # 态2
    c = await deploy()
    r = await facade.flow("processDefine/remove", {"id": c})
    assert r["code"] == 0, r
    await gone(c)

    # 态3
    d = await deploy()
    for args in ({"ids": []}, {"ids": [], "id": d}):
        r = await facade.flow("processDefine/remove", args)
        assert r["code"] == 99999999, (args, r)
        assert "id 缺失或非法" in r["msg"], (args, r)
    assert await repo.find_define_by_id(d) is not None, f"空 ids 报错不得动数据: {d}"

    # 态4：空串 / 含 None → 报错且整批不生效（带 id 回落格同样必须报错）
    for args in ({"ids": [""]}, {"ids": [d, None]}, {"ids": [d, ""]}, {"ids": [d, None], "id": d}):
        r = await facade.flow("processDefine/remove", args)
        assert r["code"] == 99999999, (args, r)
        assert "id 缺失或非法" in r["msg"], (args, r)
    assert await repo.find_define_by_id(d) is not None, f"含非法值应整批拒绝: {d}"
    assert (await facade.flow("processDefine/remove", {"ids": [d]}))["code"] == 0
    await gone(d)


@pytest.mark.asyncio
async def test_facade_define_up_and_down_ids_form_matrix():
    """矩阵④processDefine/upAndDown（issues/54 E26 批量 + issues/95 收敛进 _id_list）。
    ⚠️ 关键坑：本 action 除 ids 外还要求 opType/state——不带就先撞 `opType/state 缺失或非法`，
    "空 ids 报错"就成了恒真断言。故每一态都带合法 opType，并另加一格专钉 state 校验本身。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        simple_content = f.read()
    with open(os.path.join(FLOW_DIR, "02-multi-task.json"), encoding="utf-8") as f:
        multi_content = f.read()

    async def deploy(content):
        r = await facade.flow("processDefine/deploy", {"content": content})
        assert r["code"] == 0, r
        return int(r["data"]["processDefineId"])

    async def state_of(define_id):
        d = await repo.find_define_by_id(define_id)
        assert d is not None, define_id
        return d.state

    DISABLE, ENABLE = 0, 1

    # 态1：{ids} 批量停用 → 两条 state 都变 0（用两个不同 name，getLastByName 各自唯一命中）
    a = await deploy(simple_content)     # name=simple
    b = await deploy(multi_content)      # name=multi-task
    assert await state_of(a) == ENABLE and await state_of(b) == ENABLE, "部署后应为启用态"
    r = await facade.flow("processDefine/upAndDown", {"ids": [a, b], "opType": DISABLE})
    assert r["code"] == 0, r
    assert await state_of(a) == DISABLE and await state_of(b) == DISABLE, "批量停用应对两条都生效"
    for name in ("simple", "multi-task"):
        row = await facade.flow("processDefine/getLastByName", {"processDefineName": name})
        assert row["code"] == 0 and row["data"]["state"] == DISABLE, (name, row)

    # 态2：单数 {id} + state（旧形态，test_facade_deploy_version 同款）→ 只作用这一条
    r = await facade.flow("processDefine/upAndDown", {"id": a, "state": ENABLE})
    assert r["code"] == 0, r
    assert await state_of(a) == ENABLE, "单 {id} 旧形态应仍生效"
    assert await state_of(b) == DISABLE, f"单条操作不得波及其他定义: {b}"

    # 态3：空数组报错（带合法 opType 才测得到 ids）；且 ids 优先不得回落到 id
    for args in ({"ids": [], "opType": ENABLE}, {"ids": [], "opType": ENABLE, "id": a}):
        r = await facade.flow("processDefine/upAndDown", args)
        assert r["code"] == 99999999, (args, r)
        assert "id 缺失或非法" in r["msg"], (args, r)
    assert await state_of(a) == ENABLE and await state_of(b) == DISABLE, "空 ids 报错不得动数据"

    # 态4：空串 / 含 None → 报错且整批不生效（带 id 回落格同样必须报错；opType 全程合法）
    for args in ({"ids": [""], "opType": DISABLE},
                 {"ids": [b, None], "opType": DISABLE},
                 {"ids": [b, ""], "opType": DISABLE},
                 {"ids": [b, None], "opType": DISABLE, "id": b}):
        r = await facade.flow("processDefine/upAndDown", args)
        assert r["code"] == 99999999, (args, r)
        assert "id 缺失或非法" in r["msg"], (args, r)
    assert await state_of(b) == DISABLE, f"含非法值应整批拒绝: {b}"
    # 同一批换成合法 ids 仍可生效（证明上一步是没收到 id，不是这条改不动）
    assert (await facade.flow("processDefine/upAndDown", {"ids": [b], "opType": ENABLE}))["code"] == 0
    assert await state_of(b) == ENABLE

    # 另一格：state/opType 自身缺失的报错文案不被 ids 断言掩盖（先撞 state 校验）
    r = await facade.flow("processDefine/upAndDown", {"ids": [a, b]})
    assert r["code"] == 99999999, r
    assert "opType/state 缺失或非法" in r["msg"], r
    r = await facade.flow("processDefine/upAndDown", {"ids": [], })
    assert r["code"] == 99999999, r
    assert "opType/state 缺失或非法" in r["msg"], r


# ─── Test 102: CC_CREATE 抄送知会事件（issues/102，对齐 Java/Go/Node/PHP）────────────

@pytest.mark.asyncio
async def test_cc_create_event_per_actor():
    """issues/102：CC 实例落库后逐抄送人 fire CC_CREATE，ccActorId 直传事件体——
    发起路径（f_ccActors）与手动 createCCInstance 路径同语义（在 start 事务内 fire）"""
    eng, repo = setup()
    df = load_flow(repo, "01-simple.json")
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())

    cc_events = []
    async def on_event(evt: ProcessEvent):
        if evt.type == EventType.CC_CREATE:
            cc_events.append(evt)

    eng.set_extensions(EngineExtensions(event_listener=on_event))

    # ① 发起路径：f_ccActors="alice,bob" → 逐抄送人 2 个事件，事件体携带实例 id 与抄送人 id
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": df.id, "operator": "applicant",
                           "f_ccActors": "alice,bob"})
    assert r["code"] == 0, r
    inst_id = int(r["data"]["processInstanceId"])
    assert [e.ccActorId for e in cc_events] == ["alice", "bob"], \
        f"CC_CREATE 应逐抄送人 fire，实际 {[e.ccActorId for e in cc_events]}"
    assert all(e.instanceId == inst_id for e in cc_events), \
        f"CC_CREATE instanceId 应为发起实例，实际 {[(e.instanceId,) for e in cc_events]}"

    # ② 手动路径：createCCInstance → 再 1 个事件，同字段语义
    r = await facade.flow("processInstance/createCCInstance",
                          {"processInstanceId": inst_id, "operator": "applicant",
                           "actorIds": ["carol"]})
    assert r["code"] == 0, r
    assert [e.ccActorId for e in cc_events] == ["alice", "bob", "carol"], \
        f"手动 CC 应再 fire 1 个，实际 {[e.ccActorId for e in cc_events]}"
    assert cc_events[-1].instanceId == inst_id


@pytest.mark.asyncio
async def test_cc_create_event_no_listener_pure_incremental():
    """issues/102 纯增量红线：未注册监听器（ext 为空）时带 CC 发起/手动 CC
    与上一版逐字节一致——不报错、不抛事件、cc 实例照常落库"""
    eng, repo = setup()
    df = load_flow(repo, "01-simple.json")
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    # 不 set_extensions：ext 保持 None

    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": df.id, "operator": "applicant",
                           "f_ccActors": "alice,bob"})
    assert r["code"] == 0, r
    inst_id = int(r["data"]["processInstanceId"])

    r = await facade.flow("processInstance/createCCInstance",
                          {"processInstanceId": inst_id, "operator": "applicant",
                           "actorIds": ["carol"]})
    assert r["code"] == 0, r

    # cc 数据照常在（数据在、事件静默）：3 行抄送实例
    r = await facade.flow("processInstance/ccList", {"operator": "alice"})
    assert r["code"] == 0 and len(r["data"]["rows"]) == 1, r
    r = await facade.flow("processInstance/ccList", {"operator": "carol"})
    assert r["code"] == 0 and len(r["data"]["rows"]) == 1, r


# ═══ issues/103 统计三 action 测试 ═══

@pytest.mark.asyncio
async def test_event_listener_exception_isolated():
    """issues/104 P2：单监听器异常不影响引擎主流程（异常不外溢）"""
    eng, repo = setup()

    async def on_event(evt: ProcessEvent):
        raise RuntimeError("boom")

    eng.set_extensions(EngineExtensions(event_listener=on_event))
    df = load_flow(repo, "01-simple.json")
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": df.id, "operator": "applicant"})
    assert r["code"] == 0, "监听器异常不应影响发起主流程"


# ─── Test 127＋132: 事件代码腿（spec 11-events §11.3 码表 / §11.5 订阅形状 / §11.7 抄送联动）──

def _is_subsequence(want: list, got: list) -> bool:
    """want 是否**按顺序**出现在 got 中（spec §11.8 L2-30 判据：只断"出现过"不算过）"""
    it = iter(got)
    return all(x in it for x in want)


@pytest.mark.asyncio
async def test_event_codes_are_a_set_integers():
    """spec §11.3/§11.6：本栈从 C 套字符串名整型化到 A 套 1..9（1/2/3 不变，4 号位由 Java
    死码 PROCESS_TASK_END 让位给 CC_CREATE，5..9 本轮新增）。旧名以**同码别名**保留兼容。"""
    assert {e.name: int(e) for e in EventType} == {
        "PROCESS_INSTANCE_START": 1, "PROCESS_INSTANCE_END": 2, "PROCESS_TASK_START": 3,
        "CC_CREATE": 4, "TASK_COMPLETE": 5, "TASK_REJECT": 6, "TASK_TRANSFER": 7,
        "TASK_WITHDRAW": 8, "INSTANCE_TERMINATED": 9,
    }
    assert [int(e) for e in EventType] == [1, 2, 3, 4, 5, 6, 7, 8, 9]  # 别名不进成员表
    # 旧名兼容别名＝同一个成员（集成层旧代码 EventType.TASK_CREATE 仍可用）
    assert EventType.PROCESS_START is EventType.PROCESS_INSTANCE_START
    assert EventType.TASK_CREATE is EventType.PROCESS_TASK_START
    assert EventType.PROCESS_FINISH is EventType.PROCESS_INSTANCE_END
    assert EventType.PROCESS_REJECT is EventType.PROCESS_INSTANCE_END
    assert EventType.CC_CREATE == 4


@pytest.mark.asyncio
async def test_event_recorder_canonical_sequence_and_cc_branch():
    """spec §11.8 L2-30：一条流从发起到办结**按顺序**收到
    [PROCESS_INSTANCE_START, PROCESS_TASK_START, TASK_COMPLETE, PROCESS_INSTANCE_END]
    ＋ 抄送支 CC_CREATE（发起 f_ccActors／办理 tf_ccActors 两条腿都算数，issues/127）。

    同时逐码验"fire 必在落库之后"（spec §11.2 原则 3）——监听器**当场反查仓储**读得到
    那一行（任务行/cc 行/实例终态），读不到即红。
    """
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "02-multi-task.json")

    names: list[str] = []
    codes: list[int] = []
    cc_actors: list[str] = []
    persisted: dict[str, bool] = {}

    async def recorder(evt: ProcessEvent):
        names.append(evt.name)
        codes.append(evt.code)
        if evt.type is EventType.PROCESS_TASK_START:
            t = await repo.find_task_by_id(evt.taskId)          # 码 3：任务行已落库
            persisted["task_start"] = (t is not None and list(t.actorIds) == list(evt.actors)
                                       and evt.sourceId == evt.taskId
                                       and set(evt.data) >= {"instanceId", "taskId", "actors"})
        elif evt.type is EventType.CC_CREATE:
            _rows, total = await repo.page_cc_instances(1, 10, evt.ccActorId)  # 码 4：cc 行已落库
            cc_actors.append(evt.ccActorId)
            persisted[f"cc:{evt.ccActorId}"] = (total >= 1 and evt.sourceId == evt.instanceId
                                                and evt.data.get("ccActorId") == evt.ccActorId)
        elif evt.type is EventType.PROCESS_INSTANCE_END:
            inst = await repo.find_instance_by_id(evt.instanceId)  # 码 2：终态已落库
            persisted["instance_end"] = (inst is not None and int(inst.state) == evt.state
                                         and evt.data.get("state") == evt.state)

    eng.set_extensions(EngineExtensions(event_listeners=[recorder]))

    # ① 发起腿：f_ccActors → 建 cc 行 + 逐人 CC_CREATE
    r = await facade.flow("processInstance/startAndExecute", {
        "processDefineId": define_id, "operator": "applicant", "f_ccActors": "alice,bob"})
    assert r["code"] == 0, r
    iid = int(r["data"]["processInstanceId"])

    # ② 办理腿：tf_ccActors（issues/127 病灶——此前本栈根本没有这条腿，cc 行不建、事件不发）
    doing = await repo.find_doing_tasks(iid)
    assert doing and doing[0].taskName == "task1", doing
    r = await facade.flow("processTask/execute", {
        "processTaskId": doing[0].id, "operator": "leader", "submitType": 1, "tf_ccActors": "carol"})
    assert r["code"] == 0, r
    # 数据腿：办理时抄送真的落了 cc 行（ccList 出口读得到）
    for who in ("alice", "bob", "carol"):
        rows = (await facade.flow("processInstance/ccList", {"operator": who}))["data"]["rows"]
        assert len(rows) == 1 and int(rows[0]["id"]) == iid, (who, rows)

    # ③ 走完流程到办结
    for name, op in (("task2", "manager"), ("task3", "boss")):
        doing = await repo.find_doing_tasks(iid)
        assert doing and doing[0].taskName == name, doing
        r = await facade.flow("processTask/execute", {
            "processTaskId": doing[0].id, "operator": op, "submitType": 1})
        assert r["code"] == 0, r

    # ── 序列判据（按顺序，不是"出现过"）──
    assert names[0] == "PROCESS_INSTANCE_START" and names[-1] == "PROCESS_INSTANCE_END", names
    want = ["PROCESS_INSTANCE_START", "PROCESS_TASK_START", "TASK_COMPLETE", "PROCESS_INSTANCE_END"]
    assert _is_subsequence(want, names), f"规范名序列不符: {names}"
    assert _is_subsequence([1, 3, 5, 2], codes), f"A 套码序列不符: {codes}"
    assert _is_subsequence(["CC_CREATE"], names), f"抄送支缺失: {names}"
    # 抄送支逐人 fire（spec §11.3 码 4「逐抄送人 fire 一次」），且发起腿在实例发起后、办理腿在办理后
    assert cc_actors == ["alice", "bob", "carol"], cc_actors
    assert names.count("CC_CREATE") == 3
    # 每一支 fire 时对应数据都已落库（监听器当场反查得到）
    assert persisted == {"task_start": True, "cc:alice": True, "cc:bob": True,
                         "cc:carol": True, "instance_end": True}, persisted
    # 载荷键按 §11.3 camelCase：终态事件的 state 是落库后的整数（办结 20）
    assert (await repo.find_instance_by_id(iid)).state == InstanceState.DONE


# ─── issues/127 契约位置：抄送腿在**引擎执行路径**里（spec §11.7「与任务更新同事务建 cc 行」）──

class _WriteSpyRepo(MemoryRepository):
    """记录**写库顺序**的内存仓——证 cc 行的写点落在 ``engine.*`` 这次调用的**内部**。

    本栈的事务约定是 ``JdbcProcessRepository.with_tx``（``contextvars`` 绑连接，spec 05 §7.4）：
    调用方把 ``engine.start/execute`` 包进 ``with_tx`` 时，只有写点在引擎调用栈内，
    cc 行才与实例/任务写库同处一个事务。门面在 ``engine.*`` **返回之后**建 cc 行（本轮之前的形状）
    ⇒ 该事务盖不到 cc 行，且不用门面的调用方根本不建 cc 行。
    """

    def __init__(self):
        super().__init__()
        self.writes: list[str] = []

    async def save_instance(self, inst):
        self.writes.append("save_instance")
        return await super().save_instance(inst)

    async def update_instance(self, inst):
        self.writes.append("update_instance")
        return await super().update_instance(inst)

    async def update_task(self, task):
        self.writes.append("update_task")
        return await super().update_task(task)

    async def create_cc_instance(self, instance_id: int, creator: str, *actor_ids: str):
        self.writes.append(f"create_cc_instance:{','.join(actor_ids)}")
        return await super().create_cc_instance(instance_id, creator, *actor_ids)

    def first_cc_write(self) -> int:
        idx = [i for i, w in enumerate(self.writes) if w.startswith("create_cc_instance")]
        assert idx, f"cc 行从未落库，写序={self.writes}"
        return idx[0]


@pytest.mark.asyncio
async def test_cc_leg_lives_in_engine_execution_path():
    """判据①＋②：**直连引擎 API**（不经门面）带 ``f_ccActors``／``tf_ccActors`` 也建 cc 行、
    逐人 fire CC_CREATE(4)，且 fire 那一刻 cc 行**已在仓里**（spec §11.2 原则 3／§11.7）。

    基准＝Java ``JeeflowEngineImpl.handleCcActors``（在 ``executeProcessTask`` 的 ``runInTx`` 里）。
    本轮之前本栈两条腿都在 ``facade``，且排在 ``engine.*`` 返回之后 ⇒ 引擎直用形态（集成层的
    WSGI↔async 桥、以及任何 ``EngineImpl`` 直调）传 cc 参数**静默零副作用**，跨栈与 go/node 分叉。
    """
    repo = _WriteSpyRepo()
    eng = EngineImpl(repo, _TestUserProv(), _TestIDGen(), _TestExprEval())
    df = load_flow(repo, "02-multi-task.json")

    cc_events: list[ProcessEvent] = []
    rows_at_fire: list[int] = []

    async def recorder(evt: ProcessEvent):
        if evt.type is EventType.CC_CREATE:
            _r, total = await repo.page_cc_instances(1, 10, evt.ccActorId)
            rows_at_fire.append(total)          # 判据②：fire 时 cc 行必须已落库（摘掉落库只 fire ⇒ 红）
            cc_events.append(evt)

    eng.set_extensions(EngineExtensions(event_listeners=[recorder]))

    # ① 发起腿·直连引擎：逗号串带空项与重复项 ⇒ trim / 丢空 / 按出现顺序去重后逐人 fire
    inst = await eng.start_process_instance_by_id(df.id, "applicant",
                                                  {"f_ccActors": "alice, bob ,,alice"})
    assert [e.ccActorId for e in cc_events] == ["alice", "bob"], \
        f"直连引擎的发起腿应建 cc 并逐人 fire，实际 {[e.ccActorId for e in cc_events]}"
    assert all(e.instanceId == inst.id and e.sourceId == inst.id for e in cc_events), \
        [e.instanceId for e in cc_events]
    assert rows_at_fire and all(t >= 1 for t in rows_at_fire), f"fire 时 cc 行还没落库: {rows_at_fire}"
    # 写序：实例行 insert 先于 cc 行（同一次调用内，故包住的 with_tx 一并盖到）
    assert "save_instance" in repo.writes
    assert repo.writes.index("save_instance") < repo.first_cc_write(), repo.writes

    # ② 办理腿·直连引擎：tf_ccActors 与任务更新同一次调用，cc 行写点在 update_task 之后
    apply = (await repo.find_doing_tasks(inst.id))[0]
    await repo.add_task_actor(apply.id, ["applicant"])
    repo.writes.clear(); cc_events.clear(); rows_at_fire.clear()
    await eng.execute_process_task(apply.id, "applicant",
                                   {"submitType": 0, "tf_ccActors": ["carol", "dave", "carol"]})
    assert [e.ccActorId for e in cc_events] == ["carol", "dave"], \
        f"直连引擎的办理腿应建 cc 并逐人 fire，实际 {[e.ccActorId for e in cc_events]}"
    assert all(e.instanceId == inst.id for e in cc_events)
    assert rows_at_fire and all(t >= 1 for t in rows_at_fire), f"fire 时 cc 行还没落库: {rows_at_fire}"
    assert "update_task" in repo.writes, repo.writes
    assert repo.writes.index("update_task") < repo.first_cc_write(), \
        f"cc 行必须与任务更新同序（在 update_task 之后、同一次调用内）: {repo.writes}"

    # ③ 不带 cc 参数的发起/办理：零副作用（纯增量红线，不得凭空建 cc 行）
    repo.writes.clear()
    inst2 = await eng.start_process_instance_by_id(df.id, "applicant2")
    assert not any(w.startswith("create_cc_instance") for w in repo.writes), repo.writes
    doing2 = (await repo.find_doing_tasks(inst2.id))[0]
    await repo.add_task_actor(doing2.id, ["applicant2"])
    repo.writes.clear()
    await eng.execute_process_task(doing2.id, "applicant2", {"submitType": 0})
    assert not any(w.startswith("create_cc_instance") for w in repo.writes), repo.writes


@pytest.mark.asyncio
async def test_cc_three_paths_share_one_engine_funnel():
    """判据③：发起 ``f_ccActors``／办理 ``tf_ccActors``／手动 ``createCCInstance`` **三条路径
    只有一处实现**——都进 ``engine.handle_cc_actors`` 那一个漏斗，门面不留第二份建行/fire 代码。

    证法＝引擎漏斗打桩计数（三条路径各命中一次、参数逐一对上）＋ 仓储写点计数
    （每条路径只写一次 cc 行）＋ 门面侧旧实现已摘除。
    """
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")

    funnel: list[list[str]] = []
    create_calls: list[list[str]] = []
    original_funnel = eng.handle_cc_actors
    original_create = repo.create_cc_instance

    async def funnel_spy(instance_id, operator, cc_actors):
        actors = await original_funnel(instance_id, operator, cc_actors)
        if actors:                      # 不带 cc 参数的引擎调用（返回 []）不算"抄送动作"
            funnel.append(list(actors))
        return actors

    async def create_spy(instance_id, creator, *actor_ids):
        create_calls.append(list(actor_ids))
        return await original_create(instance_id, creator, *actor_ids)

    eng.handle_cc_actors = funnel_spy
    repo.create_cc_instance = create_spy

    # 路径一：发起 f_ccActors
    iid = await _start(facade, define_id, "zhangsan")
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": define_id, "operator": "user9",
                           "f_ccActors": "p1,p2"})
    assert r["code"] == 0, r
    # 路径二：办理 tf_ccActors
    doing = await repo.find_doing_tasks(int(r["data"]["processInstanceId"]))
    assert doing, "发起后应有待办"
    r = await facade.flow("processTask/execute",
                          {"processTaskId": doing[0].id, "operator": "leader",
                           "submitType": 1, "tf_ccActors": "p3"})
    assert r["code"] == 0, r
    # 路径三：手动 createCCInstance（重复项去重后只一人）
    r = await facade.flow("processInstance/createCCInstance",
                          {"processInstanceId": iid, "operator": "zhangsan",
                           "actorIds": ["p4", "p4"]})
    assert r["code"] == 0, r

    assert funnel == [["p1", "p2"], ["p3"], ["p4"]], f"三条路径应各命中漏斗一次: {funnel}"
    assert create_calls == [["p1", "p2"], ["p3"], ["p4"]], \
        f"每条路径只应写一次 cc 行（没有第二处建行实现）: {create_calls}"
    # 门面侧不再有抄送实现：抄送落库 + fire 只在引擎那一处
    assert not hasattr(JeeflowFacade, "_handle_cc_actors"), "门面仍留着自己的抄送入口（第二处实现）"
    assert not hasattr(JeeflowFacade, "_notify_cc_create"), "门面仍自己 fire CC_CREATE（第二处实现）"


# ─── spec §11.7 边界 2：办理抄送腿的覆盖面**只有 executeProcessTask 一条** ──────────────

async def _cc_leg_probe(at: str, actor: str, submit_type: int, *, extra: dict = None,
                        cc: str = None, via_facade: bool = True):
    """02-multi-task 推进到 ``at`` 节点 → 执行一次办理动作，只采集**这一次调用**内的事件名、
    CC_CREATE 的 ccActorId、cc 写点、该实例的 cc 行数。

    ``via_facade=False`` 走**直连引擎 API**（不经门面）——覆盖面判据必须落在引擎里（§11.7 边界 1
    同一条理由：挂在门面＝直连引擎的调用方形状不同）。cc 参数用 ``tf_ccActors``。
    """
    repo = _WriteSpyRepo()
    eng = EngineImpl(repo, _TestUserProv(), _TestIDGen(), _TestExprEval())
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    names: list[str] = []
    cc_actors: list[str] = []

    def recorder(evt: ProcessEvent):
        names.append(evt.name)
        if evt.type is EventType.CC_CREATE:
            cc_actors.append(evt.ccActorId)

    eng.set_extensions(EngineExtensions(event_listeners=[recorder]))
    iid = await _start_multi_task_at(facade, repo, at)
    tid = await _doing_task_id(repo, iid, at)
    assert tid, f"应推进到 {at}"
    await repo.add_task_actor(tid, [actor])
    names.clear(); cc_actors.clear(); repo.writes.clear()

    args: dict = {"submitType": submit_type}
    args |= extra or {}
    if cc is not None:
        args[KEY_CC_ACTORS] = cc
    if via_facade:
        r = await facade.flow("processTask/execute", {"processTaskId": tid, "operator": actor, **args})
        assert r["code"] == 0, r
    else:
        # 直连引擎：门面 submitType 分发的同一张表（facade._processTask_execute）
        if submit_type == 2:
            await eng.execute_and_jump_to_end(tid, actor, args)
        elif submit_type == 3:
            await eng.execute_and_jump_task(tid, actor, args)
        elif submit_type == 4:
            await eng.execute_and_jump_task(tid, actor, args, extra["taskName"])
        elif submit_type == 6:
            await eng.execute_and_jump_to_first_task_node(tid, actor, args)
        else:
            await eng.execute_process_task(tid, actor, args)
    return (names, cc_actors,
            [w for w in repo.writes if w.startswith("create_cc_instance")],
            repo._cc.get(iid))


@pytest.mark.asyncio
async def test_cc_leg_positive_execute_process_task_fires_exactly_n():
    """正向格（spec §11.7 边界 2 的"该发的那一支"）：``executeProcessTask`` 带 ``tf_ccActors``
    ⇒ **恰好 N 支 CC_CREATE(4) ＋ N 行 cc**（N＝去重后抄送人数；重复项折一行一事件）。

    门面与直连引擎两种姿势都要过（钩子在引擎里，不在门面）。
    """
    for via in (True, False):
        tag = "门面" if via else "直连引擎"
        names, cc_actors, cc_writes, cc_rows = await _cc_leg_probe(
            "task1", "leader", 1, cc="carol,dave,carol", via_facade=via)
        assert cc_actors == ["carol", "dave"], f"{tag}：应逐抄送人各 fire 一支 4，实际 {cc_actors}"
        assert names.count("CC_CREATE") == 2, f"{tag}：N=2 ⇒ 恰好 2 支 4，实际 {names}"
        assert cc_writes == ["create_cc_instance:carol,dave"], \
            f"{tag}：cc 行应一次写、每人一行: {cc_writes}"
        assert cc_rows == ["carol", "dave"], f"{tag}：仓里应是 N=2 行 cc: {cc_rows}"
        # 这一档原有的 5＋下一节点 3 腿照旧在（cc 是纯增量，不顶掉别的码）
        assert "TASK_COMPLETE" in names and "PROCESS_TASK_START" in names, names


@pytest.mark.asyncio
async def test_cc_leg_narrow_coverage_jump_reject_builds_no_cc():
    """四档负向格（spec §11.7 边界 2 钉死的覆盖面）：``executeAndJumpToEnd``（拒绝/跳转）、
    ``jumpTask``/``rollback``、退回发起人这几条腿带 ``tf_ccActors`` ⇒ **0 支 4 且 0 行 cc**，
    并且它们原有的 5/6/2 事件腿形状**逐字不变**。

    判据原文（jeeflow-doc spec/11-events.md §11.7 两条本轮钉死的边界 2）：
    「**覆盖面以 Java 基准为准，只算 `executeProcessTask` 一条**。`executeAndJumpTask` /
    `jumpToEnd` / `rollbackToOperator` 这类跳转·回退 action 带的 `tf_ccActors`
    **本轮不建 cc、不发 `CC_CREATE`**；……**单栈自行放宽＝跨栈分叉**」。
    形状对照＝同一档动作"不带 cc"跑一遍、"带 cc"再跑一遍，两次事件名序列必须完全相等
    （不是"我以为的样子"，是这一档自己跟自己比）。
    """
    #: 每档的既有事件腿形状（本轮之前实测值；带 cc 后必须一字不差）
    shapes = {
        2: ["TASK_REJECT", "PROCESS_INSTANCE_END"],        # 拒绝到终点（jumpToEnd）
        3: ["TASK_REJECT", "PROCESS_TASK_START"],          # ROLLBACK 空 target（血缘回退）
        4: ["TASK_COMPLETE", "PROCESS_TASK_START"],        # JUMP 命名 target
        6: ["TASK_REJECT", "PROCESS_TASK_START"],          # 退回发起人
    }
    for submit_type, shape in shapes.items():
        # 只有 JUMP 档需要命名 target；其余档不带 taskName，免得把无关键塞进流程变量
        extra = {"taskName": "apply"} if submit_type == 4 else {}
        for via in (True, False):
            tag = f"submitType={submit_type}{'门面' if via else '直连引擎'}"
            base_names, *_ = await _cc_leg_probe(
                "task3", "boss", submit_type, extra=extra, via_facade=via)
            names, cc_actors, cc_writes, cc_rows = await _cc_leg_probe(
                "task3", "boss", submit_type, extra=extra,
                cc="carol,dave,carol", via_facade=via)
            # ① 0 支 4
            assert cc_actors == [] and "CC_CREATE" not in names, \
                f"{tag}：跳转·回退档带 tf_ccActors 不该 fire 码 4，实际 {names}"
            # ② 0 行 cc
            assert cc_writes == [] and not cc_rows, \
                f"{tag}：跳转·回退档带 tf_ccActors 不该建 cc 行，实际 {cc_writes}/{cc_rows}"
            # ③ 原有 5/6/2 事件腿形状不许变（与"不带 cc"的同档动作逐字对照）
            assert base_names == shape, f"{tag}：无 cc 基准形状漂了 {base_names} != {shape}"
            assert names == shape, f"{tag}：带 cc 后形状变了 {names} != {base_names}"


@pytest.mark.asyncio
async def test_cc_leg_untouched_start_and_manual_paths():
    """回归格（本轮不许动的两支）：发起腿 ``f_ccActors`` 与门面手动 ``createCCInstance``
    仍各自建 cc 行 + 逐人 fire 码 4 —— 收窄只针对办理腿的覆盖面。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "02-multi-task.json")

    names: list[str] = []
    eng.set_extensions(EngineExtensions(event_listeners=[lambda evt: names.append(evt.name)]))
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": define_id, "operator": "zhangsan",
                           "f_ccActors": "alice,bob"})
    assert r["code"] == 0, r
    iid = int(r["data"]["processInstanceId"])
    assert names.count("CC_CREATE") == 2, f"发起腿应 fire 2 支 4: {names}"
    assert repo._cc.get(iid) == ["alice", "bob"], repo._cc.get(iid)

    names.clear()
    r = await facade.flow("processInstance/createCCInstance",
                          {"processInstanceId": iid, "operator": "zhangsan",
                           "actorIds": ["mallory"]})
    assert r["code"] == 0, r
    assert names == ["CC_CREATE"], f"手动腿应 fire 一支 4: {names}"
    assert repo._cc.get(iid) == ["alice", "bob", "mallory"], repo._cc.get(iid)


@pytest.mark.asyncio
async def test_task_reject_and_complete_are_exclusive():
    """spec §11.3 码 5/6 互斥：同一动作走退回就不再 fire「办掉」；退发起人/会签软拒绝同归 6。
    实例终态另发码 2，载荷 state 分办结(20)/拒绝(45)（§11.6 收口旧 Finish/Reject 拆分）。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")

    evts: list[ProcessEvent] = []
    eng.set_extensions(EngineExtensions(event_listeners=[lambda evt: evts.append(evt)]))

    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": define_id, "operator": "applicant"})
    assert r["code"] == 0, r
    iid = int(r["data"]["processInstanceId"])
    doing = await repo.find_doing_tasks(iid)
    assert doing[0].taskName == "task1"

    # 同意：只发 5（apply 自动完成那一支 submitType=0 也是"办掉"；此处没有 6）
    r = await facade.flow("processTask/execute", {"processTaskId": doing[0].id,
                                                  "operator": "leader", "submitType": 1})
    assert r["code"] == 0, r
    assert [e.data["submitType"] for e in evts if e.type is EventType.TASK_COMPLETE] == [0, 1], \
        [e.name for e in evts]
    assert not [e for e in evts if e.type is EventType.TASK_REJECT], [e.name for e in evts]

    # 拒绝：只发 6，不再补发 5（互斥）；实例终态发 2 且 state=45
    iid2 = int((await facade.flow("processInstance/startAndExecute",
                                  {"processDefineId": define_id, "operator": "user2"}))
               ["data"]["processInstanceId"])
    doing2 = await repo.find_doing_tasks(iid2)
    task1_id = doing2[0].id
    evts.clear()
    r = await facade.flow("processTask/execute", {"processTaskId": task1_id,
                                                  "operator": "leader", "submitType": 2})
    assert r["code"] == 0, r
    names = [e.name for e in evts]
    assert "TASK_REJECT" in names and "TASK_COMPLETE" not in names, names
    rej = next(e for e in evts if e.type is EventType.TASK_REJECT)
    assert rej.code == 6 and rej.sourceId == int(task1_id)
    assert rej.data["submitType"] == 2 and rej.data["instanceId"] == iid2
    assert rej.data["operator"] == "leader" and rej.data["taskId"] == int(task1_id)
    end = next(e for e in evts if e.type is EventType.PROCESS_INSTANCE_END)
    assert end.code == 2 and end.state == int(InstanceState.REJECT)
    assert (await repo.find_instance_by_id(iid2)).state == InstanceState.REJECT

    # 直连引擎的退回入口（args 里**没有** submitType）也必须发 6 不发 5：
    # 档由"调用的是哪个引擎方法"决定，不靠调用方塞参数（残留 submitType 也不得误判）
    iid3 = int((await facade.flow("processInstance/startAndExecute",
                                   {"processDefineId": define_id, "operator": "user3"}))
                ["data"]["processInstanceId"])
    doing3 = await repo.find_doing_tasks(iid3)
    evts.clear()
    await eng.execute_and_jump_to_end(doing3[0].id, "leader")
    names3 = [e.name for e in evts]
    assert names3 == ["TASK_REJECT", "PROCESS_INSTANCE_END"], names3
    assert evts[0].data["submitType"] == 2, evts[0].data


@pytest.mark.asyncio
async def test_transfer_fires_task_transfer_after_persist():
    """spec §11.3 码 7：转办在参与者被替换**并落库之后** fire，载荷带 fromActor/toActor/operator"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")
    task1 = (await repo.find_doing_tasks(iid))[0]

    seen: list[ProcessEvent] = []
    readback: dict[str, bool] = {}

    async def listener(evt: ProcessEvent):
        if evt.type is not EventType.TASK_TRANSFER:
            return
        seen.append(evt)
        actors = await repo.find_task_actors(evt.taskId)   # fire 时替换已落库
        readback["actors"] = (evt.fromActor not in actors and evt.toActor in actors)

    eng.set_extensions(EngineExtensions(event_listeners=[listener]))
    r = await facade.flow("processTask/transfer", {"processTaskId": task1.id,
                                                   "fromActor": "leader", "toActor": "lisi",
                                                   "reason": "出差", "operator": "leader"})
    assert r["code"] == 0, r
    assert len(seen) == 1 and seen[0].code == 7, [e.name for e in seen]
    assert seen[0].sourceId == int(task1.id)
    assert seen[0].data == {"instanceId": iid, "taskId": int(task1.id), "fromActor": "leader",
                            "toActor": "lisi", "operator": "leader", "taskName": "task1"}, seen[0].data
    assert readback == {"actors": True}, readback


@pytest.mark.asyncio
async def test_withdraw_fires_task_withdraw_once_after_persist():
    """spec §11.3 码 8：撤回把实例写 30、任务行更新完后 **每轮只 fire 一次**（不逐任务）"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "02-multi-task.json")
    iid = await _start(facade, define_id, "zhangsan")
    # 并行会签场景才会一次撤多行；01/02 系是单行，另造一个多 doing 的实例更稳妥：
    # 这里直接验"每轮一次"＋落库后 fire，多行档由 cs 流程另测（见下方 count 断言）
    seen: list[ProcessEvent] = []
    readback: dict[str, bool] = {}

    async def listener(evt: ProcessEvent):
        if evt.type is not EventType.TASK_WITHDRAW:
            return
        seen.append(evt)
        inst = await repo.find_instance_by_id(evt.instanceId)   # fire 时实例已是 30
        readback["state"] = (inst is not None and int(inst.state) == int(InstanceState.WITHDRAW))

    eng.set_extensions(EngineExtensions(event_listeners=[listener]))
    r = await facade.flow("processInstance/withdraw", {"id": iid, "operator": "zhangsan"})
    assert r["code"] == 0, r
    assert len(seen) == 1 and seen[0].code == 8, [e.name for e in seen]
    assert seen[0].sourceId == iid and seen[0].data["operator"] == "zhangsan"
    assert readback == {"state": True}, readback

    # 并行会签多行撤回：仍只 fire 一次（每轮撤回只 fire 一次，不逐任务）
    cs_define = await _deploy(facade, "05-countersign-parallel.json")
    cs_iid = await _start(facade, cs_define, "user1")
    doing = await repo.find_doing_tasks(cs_iid)
    assert len(doing) > 1, [t.taskName for t in doing]
    seen.clear()
    r = await facade.flow("processInstance/withdraw", {"id": cs_iid, "operator": "user1"})
    assert r["code"] == 0, r
    assert len(seen) == 1, f"多行撤回应只 fire 一次，实得 {len(seen)}"


@pytest.mark.asyncio
async def test_two_listeners_of_same_code_are_both_called():
    """spec §11.5「一次 fire 必须把该事件送给**全部**已注册监听器，不得后注册覆盖前注册」——
    issues/132 本栈单回调病灶的收口判据：同一个 CC_CREATE 码挂两个监听器，两个都要被调到。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")

    first: list = []
    second: list = []
    eng.set_extensions(EngineExtensions(event_listeners=[
        lambda evt: first.append((evt.name, evt.ccActorId)) if evt.type is EventType.CC_CREATE else None,
        lambda evt: second.append((evt.name, evt.ccActorId)) if evt.type is EventType.CC_CREATE else None,
    ]))
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": define_id, "operator": "applicant",
                           "f_ccActors": "u1,u2"})
    assert r["code"] == 0, r
    assert first == [("CC_CREATE", "u1"), ("CC_CREATE", "u2")], first
    assert second == first, f"同码第二个监听器没被调到: {second}"

    # 运行期再追加第三个（引擎入口 add_event_listener）：追加不覆盖，三个监听器同权
    third: list = []
    eng.add_event_listener(lambda evt: third.append(evt.ccActorId)
                           if evt.type is EventType.CC_CREATE else None)
    r = await facade.flow("processInstance/createCCInstance",
                          {"processInstanceId": int(r["data"]["processInstanceId"]),
                           "operator": "applicant", "actorIds": ["u3"]})
    assert r["code"] == 0, r
    assert [x for x in first if x[1] == "u3"] and [x for x in second if x[1] == "u3"]
    # 第三个监听器是运行期才追加的 ⇒ 只收得到追加之后发生的那一支（前两支不回放）
    assert third == ["u3"], third


@pytest.mark.asyncio
async def test_listener_exception_isolated_per_listener():
    """spec §11.5 异常隔离（issues/104 P2）：单个监听器抛异常只记日志，
    ① 不回滚主流程 ② **不中断后续监听器**——旧单回调形态无从表达这条，本轮列表化后必测"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")

    def boom(evt: ProcessEvent):
        raise RuntimeError("boom")

    tail: list = []
    eng.set_extensions(EngineExtensions(event_listeners=[boom,
                                                          lambda evt: tail.append(evt.name)]))
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": define_id, "operator": "applicant",
                           "f_ccActors": "alice"})
    assert r["code"] == 0, f"监听器异常不得影响主流程: {r}"
    # ① 主流程数据在
    iid = int(r["data"]["processInstanceId"])
    assert len((await facade.flow("processInstance/ccList",
                                  {"operator": "alice"}))["data"]["rows"]) == 1
    # ② 后续监听器照收每一个事件（含抛异常那一支同码的 CC_CREATE）
    assert tail[0] == "PROCESS_INSTANCE_START", tail
    assert tail.count("CC_CREATE") == 1, tail
    assert len(tail) >= 5 and "PROCESS_TASK_START" in tail and "TASK_COMPLETE" in tail, tail


@pytest.mark.asyncio
async def test_legacy_single_callable_listener_is_still_supported():
    """旧形状兼容（issues/132 迁移路径·本栈选"保留兼容位"而不是直接删）：
    三壳的 ``EngineExtensions(event_listener=cb)`` 写法在本轮不改，必须仍收得到全部事件；
    与 ``event_listeners`` 混用时旧回调排最前，同一个 callable 挂两处只调一次。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")

    legacy: list = []
    added: list = []
    legacy_fn = lambda evt: legacy.append(evt.name)
    eng.set_extensions(EngineExtensions(event_listener=legacy_fn,
                                        event_listeners=[lambda evt: added.append(evt.name),
                                                         legacy_fn]))
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": define_id, "operator": "applicant",
                           "f_ccActors": "alice"})
    assert r["code"] == 0, r
    assert legacy and legacy == added, (legacy, added)   # 旧回调一条不少（同一 callable 不重复调）
    assert legacy[0] == "PROCESS_INSTANCE_START" and "CC_CREATE" in legacy, legacy

    # 注册顺序＝回调顺序：旧回调在最前，列表按其注册序
    order: list = []
    eng2, repo2 = setup()
    facade2 = JeeflowFacade(eng2, repo2, MemoryExtRepository())
    define2 = await _deploy(facade2, "01-simple.json")

    async def l1(evt): order.append("l1")
    async def l2(evt): order.append("l2")
    eng2.set_extensions(EngineExtensions(event_listener=lambda evt: order.append("legacy"),
                                         event_listeners=[l1, l2]))
    await facade2.flow("processInstance/startAndExecute",
                       {"processDefineId": define2, "operator": "applicant"})
    assert order[:3] == ["legacy", "l1", "l2"], order


def _stats_setup():
    """stats 测试专用 setup：返回 (eng, repo, facade)"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    return eng, repo, facade


def _seed_stats(repo: MemoryRepository):
    """向内存仓储注入可控的统计测试数据，返回 define"""
    from datetime import timedelta
    now = datetime.now()
    d = ProcessDefine(name="stats-flow", displayName="统计测试流程", type="oa",
                      state=1, content="{}")
    repo.add_define(d)

    def _mk_inst(inst_id, state, operator, create_time, expire_time=None):
        inst = ProcessInstance(id=inst_id, defineId=d.id, state=state,
                               operator=operator, createTime=create_time,
                               expireTime=expire_time)
        repo._instances[inst_id] = inst

    def _mk_task(task_id, inst_id, task_name, display_name, task_state,
                 operator="", perform_type=0, create_time=None,
                 finish_time=None, expire_time=None):
        t = ProcessTask(id=task_id, processInstanceId=inst_id,
                        taskName=task_name, displayName=display_name,
                        taskState=task_state, actorId=operator,
                        performType=perform_type,
                        createTime=create_time or now,
                        finishTime=finish_time, expireTime=expire_time)
        repo._tasks[task_id] = t

    # 实例 1：已完成（DOING→DONE），耗时 3600s
    ct1 = now - timedelta(hours=2)
    ft1 = now - timedelta(hours=1)
    _mk_inst(100, InstanceState.DONE, "alice", ct1)
    _mk_task(1, 100, "task1", "审批节点A", TaskState.DONE,
             operator="bob", perform_type=0,
             create_time=ct1, finish_time=ft1)
    repo._actors[1] = ["bob"]

    # 实例 2：进行中（DOING），有一个 doing 任务（stuck）
    ct2 = now - timedelta(hours=1)
    _mk_inst(101, InstanceState.DOING, "charlie", ct2)
    _mk_task(2, 101, "task2", "审批节点B", TaskState.DOING,
             operator="", perform_type=0, create_time=ct2,
             expire_time=now - timedelta(minutes=30))  # 已过期
    repo._actors[2] = ["dave", "eve"]

    # 实例 3：已驳回
    ct3 = now - timedelta(days=1)
    _mk_inst(102, InstanceState.REJECT, "alice", ct3)

    # 实例 4：已完成，会签任务（performType=1）
    ct4 = now - timedelta(days=2)
    ft4 = now - timedelta(days=2, hours=-3)  # +3h → 10800s
    _mk_inst(103, InstanceState.DONE, "frank", ct4)
    _mk_task(3, 103, "cs1", "会签节点", TaskState.DONE,
             operator="gina", perform_type=1,
             create_time=ct4, finish_time=ft4)
    repo._actors[3] = ["gina", "hank"]

    return d


@pytest.mark.asyncio
async def test_stats_overview_empty_db():
    """issues/103 空库边界：overview 全 0，不 NPE"""
    eng, repo, facade = _stats_setup()
    r = await facade.flow("processInstance/stats/overview", {})
    assert r["code"] == 0, r
    d = r["data"]
    for k in ("total", "inProgress", "completed", "rejected", "withdrawn",
              "suspended", "todayNew", "pendingTaskCount", "overdueTaskCount"):
        assert d[k] == 0, f"{k}={d[k]}"
    assert d["avgDurationSeconds"] == 0
    assert d["rejectRate"] == 0.0
    assert d["countersignRate"] == 0.0
    assert d["onTimeRate"] == 0.0


@pytest.mark.asyncio
async def test_stats_overview_with_data():
    """issues/103 overview 13 字段验证"""
    eng, repo, facade = _stats_setup()
    _seed_stats(repo)

    r = await facade.flow("processInstance/stats/overview", {})
    assert r["code"] == 0, r
    d = r["data"]
    assert d["total"] == 4
    # todayNew：种子 ct1/ct2 为 now-2h/now-1h 相对时间，跨午夜运行时部分落昨日 → 动态算
    _now = datetime.now()
    _today_cnt = sum(1 for inst in repo._instances.values()
                     if getattr(inst.createTime, "date", None)
                     and inst.createTime.date() == _now.date())
    assert d["inProgress"] == 1   # state=10
    assert d["completed"] == 2    # state=20 (inst 100, 103)
    assert d["rejected"] == 1     # state=45
    assert d["withdrawn"] == 0
    assert d["suspended"] == 0
    assert d["todayNew"] == _today_cnt, f"todayNew={d['todayNew']} expect={_today_cnt}"  # E：不过滤 state
    assert d["pendingTaskCount"] == 1  # task 2 is DOING
    assert d["overdueTaskCount"] == 1  # task 2 expire < now

    # avgDurationSeconds: inst 100 = 3600s, inst 103 = 10800s → avg = 7200
    assert d["avgDurationSeconds"] == 7200, d["avgDurationSeconds"]

    # rejectRate: 1 / max(1, 2+1) = 0.3333
    assert d["rejectRate"] == 0.3333, d["rejectRate"]

    # countersignRate: 1 performType=1 / 2 total DONE tasks = 0.5
    assert d["countersignRate"] == 0.5, d["countersignRate"]

    # onTimeRate: no expire_time on DONE tasks → 0/0 → 0.0
    assert d["onTimeRate"] == 0.0, d["onTimeRate"]


@pytest.mark.asyncio
async def test_stats_trend_empty_db():
    """issues/103 空库 trend：连续桶全 0"""
    eng, repo, facade = _stats_setup()
    r = await facade.flow("processInstance/stats/trend", {
        "granularity": "day",
        "start": "2026-09-01 00:00:00",
        "end": "2026-09-03 00:00:00",
    })
    assert r["code"] == 0, r
    # A：data 本体为裸数组（无 {granularity, series} 包装）
    series = r["data"]
    assert isinstance(series, list)
    assert len(series) == 3  # 3 days
    for s in series:
        assert s["started"] == 0
        assert s["finished"] == 0


@pytest.mark.asyncio
async def test_stats_trend_day_granularity():
    """issues/103 trend day 粒度：桶连续，计数正确"""
    eng, repo, facade = _stats_setup()
    _seed_stats(repo)
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    yesterday_str = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    two_days_ago = (now - timedelta(days=2)).strftime("%Y-%m-%d")

    r = await facade.flow("processInstance/stats/trend", {
        "granularity": "day",
        "start": f"{two_days_ago} 00:00:00",
        "end": f"{today_str} 23:59:59",
    })
    assert r["code"] == 0, r
    series = r["data"]
    assert isinstance(series, list)
    assert len(series) >= 3  # at least 3 days

    # 桶格式验证
    for s in series:
        assert len(s["bucket"]) == 10  # yyyy-MM-dd

    # started 合计应等于 4（4 个实例）
    total_started = sum(s["started"] for s in series)
    assert total_started == 4, f"total_started={total_started}"


@pytest.mark.asyncio
async def test_stats_trend_all_granularities():
    """issues/103 trend 4 种粒度都不报错且桶连续"""
    eng, repo, facade = _stats_setup()
    _seed_stats(repo)

    for gran in ("hour", "day", "week", "month"):
        r = await facade.flow("processInstance/stats/trend", {
            "granularity": gran,
            "start": "2026-08-01 00:00:00",
            "end": "2026-09-03 23:59:59",
        })
        assert r["code"] == 0, f"{gran}: {r}"
        series = r["data"]
        assert isinstance(series, list) and len(series) > 0, f"{gran} should have buckets"
        # 每个桶都有 bucket/started/finished 三字段
        for s in series:
            assert "bucket" in s and "started" in s and "finished" in s


@pytest.mark.asyncio
async def test_stats_trend_invalid_granularity():
    """issues/103 非法 granularity → code!=0"""
    eng, repo, facade = _stats_setup()
    r = await facade.flow("processInstance/stats/trend", {
        "granularity": "abc",
        "start": "2026-09-01 00:00:00",
        "end": "2026-09-03 00:00:00",
    })
    assert r["code"] != 0, r


@pytest.mark.asyncio
async def test_stats_trend_missing_required_params():
    """issues/103 C 自证：缺 start / 缺 end → code!=0，不静默回退不限时间"""
    eng, repo, facade = _stats_setup()
    r1 = await facade.flow("processInstance/stats/trend",
                           {"granularity": "day", "end": "2026-09-03 00:00:00"})
    assert r1["code"] != 0, "missing start should fail"
    r2 = await facade.flow("processInstance/stats/trend",
                           {"granularity": "day", "start": "2026-09-01 00:00:00"})
    assert r2["code"] != 0, "missing end should fail"


@pytest.mark.asyncio
async def test_stats_group_empty_db():
    """issues/103 空库 group：所有维度返回空数组"""
    eng, repo, facade = _stats_setup()
    for dim in ("state", "define", "category", "approver", "applicant",
                "node", "stuckNode", "stuckApprover", "durationBucket"):
        r = await facade.flow("processInstance/stats/group", {"dimension": dim})
        assert r["code"] == 0, f"{dim}: {r}"
        # A：data 本体为裸数组（无 {dimension, rows} 包装）
        rows = r["data"]
        assert isinstance(rows, list), f"{dim} data should be a bare array"
        if dim == "durationBucket":
            assert len(rows) == 4  # 固定 4 桶
            assert [row["key"] for row in rows] == ["sameDay", "1to3d", "3to7d", "over7d"]
            for row in rows:
                assert row["count"] == 0
        else:
            assert len(rows) == 0, f"{dim} should be empty, got {rows}"


@pytest.mark.asyncio
async def test_stats_group_state_dimension():
    """issues/103 group state 维度"""
    eng, repo, facade = _stats_setup()
    _seed_stats(repo)
    r = await facade.flow("processInstance/stats/group", {"dimension": "state"})
    assert r["code"] == 0, r
    rows = r["data"]  # A：裸数组
    state_map = {row["key"]: row["count"] for row in rows}
    assert state_map.get("20") == 2  # DONE
    assert state_map.get("10") == 1  # DOING
    assert state_map.get("45") == 1  # REJECT
    # count 降序
    counts = [row["count"] for row in rows]
    assert counts == sorted(counts, reverse=True)


@pytest.mark.asyncio
async def test_stats_group_define_dimension():
    """issues/103 group define 维度：key=code name, label=displayName, 含 avgDurationSeconds"""
    eng, repo, facade = _stats_setup()
    d = _seed_stats(repo)
    r = await facade.flow("processInstance/stats/group", {"dimension": "define"})
    assert r["code"] == 0, r
    rows = r["data"]  # A：裸数组
    assert len(rows) == 1
    row = rows[0]
    assert row["key"] == "stats-flow"
    assert row["label"] == "统计测试流程"
    assert row["count"] == 4
    # avgDurationSeconds = 7200 (same as overview)
    assert row["avgDurationSeconds"] == 7200, row["avgDurationSeconds"]


@pytest.mark.asyncio
async def test_stats_group_category_dimension():
    """issues/103 group category 维度：按 define.type 分组"""
    eng, repo, facade = _stats_setup()
    _seed_stats(repo)
    r = await facade.flow("processInstance/stats/group", {"dimension": "category"})
    assert r["code"] == 0, r
    rows = r["data"]  # A：裸数组
    assert len(rows) == 1
    assert rows[0]["key"] == "oa"
    assert rows[0]["count"] == 4


@pytest.mark.asyncio
async def test_stats_group_approver_dimension():
    """issues/103 group approver 维度：task.operator 且 task_state=DONE"""
    eng, repo, facade = _stats_setup()
    _seed_stats(repo)
    r = await facade.flow("processInstance/stats/group", {"dimension": "approver"})
    assert r["code"] == 0, r
    rows = r["data"]  # A：裸数组
    op_map = {row["key"]: row["count"] for row in rows}
    assert op_map.get("bob") == 1
    assert op_map.get("gina") == 1


@pytest.mark.asyncio
async def test_stats_group_applicant_dimension():
    """issues/103 group applicant 维度：instance.operator"""
    eng, repo, facade = _stats_setup()
    _seed_stats(repo)
    r = await facade.flow("processInstance/stats/group", {"dimension": "applicant"})
    assert r["code"] == 0, r
    rows = r["data"]  # A：裸数组
    op_map = {row["key"]: row["count"] for row in rows}
    assert op_map.get("alice") == 2  # inst 100, 102
    assert op_map.get("charlie") == 1
    assert op_map.get("frank") == 1


@pytest.mark.asyncio
async def test_stats_group_node_dimension():
    """issues/103 group node 维度：task.display_name 且 task_state=DONE，含 avgDurationSeconds"""
    eng, repo, facade = _stats_setup()
    _seed_stats(repo)
    r = await facade.flow("processInstance/stats/group", {"dimension": "node"})
    assert r["code"] == 0, r
    rows = r["data"]  # A：裸数组
    node_map = {row["key"]: row for row in rows}
    assert "审批节点A" in node_map
    assert node_map["审批节点A"]["count"] == 1
    assert node_map["审批节点A"]["avgDurationSeconds"] == 3600
    assert "会签节点" in node_map
    assert node_map["会签节点"]["count"] == 1
    assert node_map["会签节点"]["avgDurationSeconds"] == 10800


@pytest.mark.asyncio
async def test_stats_group_stuck_node_dimension():
    """issues/103 group stuckNode 维度：task_state=DOING 按 display_name"""
    eng, repo, facade = _stats_setup()
    _seed_stats(repo)
    r = await facade.flow("processInstance/stats/group", {"dimension": "stuckNode"})
    assert r["code"] == 0, r
    rows = r["data"]  # A：裸数组
    assert len(rows) == 1
    assert rows[0]["key"] == "审批节点B"
    assert rows[0]["count"] == 1


@pytest.mark.asyncio
async def test_stats_group_stuck_approver_dimension():
    """issues/103 group stuckApprover 维度：task_state=DOING 的 task_actor 每 actor 一行"""
    eng, repo, facade = _stats_setup()
    _seed_stats(repo)
    r = await facade.flow("processInstance/stats/group", {"dimension": "stuckApprover"})
    assert r["code"] == 0, r
    rows = r["data"]  # A：裸数组
    actor_counts = {row["key"]: row["count"] for row in rows}
    assert actor_counts.get("dave") == 1
    assert actor_counts.get("eve") == 1


@pytest.mark.asyncio
async def test_stats_group_duration_bucket():
    """issues/103 group durationBucket：固定 4 桶、定序、零填充"""
    eng, repo, facade = _stats_setup()
    _seed_stats(repo)
    r = await facade.flow("processInstance/stats/group", {"dimension": "durationBucket"})
    assert r["code"] == 0, r
    rows = r["data"]  # A：裸数组
    assert len(rows) == 4
    keys = [row["key"] for row in rows]
    assert keys == ["sameDay", "1to3d", "3to7d", "over7d"]
    # inst 100: 3600s → sameDay; inst 103: 10800s → sameDay
    total = sum(row["count"] for row in rows)
    assert total == 2  # 2 completed instances
    assert rows[0]["count"] == 2  # sameDay has both
    assert rows[1]["count"] == 0
    assert rows[2]["count"] == 0
    assert rows[3]["count"] == 0


@pytest.mark.asyncio
async def test_stats_group_invalid_dimension():
    """issues/103 非法 dimension → code!=0"""
    eng, repo, facade = _stats_setup()
    r = await facade.flow("processInstance/stats/group", {"dimension": "bogus"})
    assert r["code"] != 0, r


@pytest.mark.asyncio
async def test_stats_group_limit():
    """issues/103 group limit 参数：Top N 截断"""
    eng, repo, facade = _stats_setup()
    _seed_stats(repo)
    r = await facade.flow("processInstance/stats/group",
                          {"dimension": "state", "limit": 2})
    assert r["code"] == 0, r
    rows = r["data"]  # A：裸数组
    assert len(rows) == 2


@pytest.mark.asyncio
async def test_stats_overview_state_in_respected():
    """issues/103 B 自证：stateIn 生效；E：todayNew 不受 stateIn 影响"""
    eng, repo, facade = _stats_setup()
    _seed_stats(repo)
    r = await facade.flow("processInstance/stats/overview", {"stateIn": [10]})
    assert r["code"] == 0, r
    d = r["data"]
    assert d["total"] == 1   # 仅 inst 101（DOING）
    assert d["inProgress"] == 1
    assert d["completed"] == 0
    _now = datetime.now()
    _today_cnt = sum(1 for inst in repo._instances.values()
                     if getattr(inst.createTime, "date", None)
                     and inst.createTime.date() == _now.date())
    assert d["todayNew"] == _today_cnt, "todayNew ignores stateIn (E)"


@pytest.mark.asyncio
async def test_stats_overview_with_start_end_filter():
    """issues/103 overview start/end 过滤：按 instance.create_time"""
    eng, repo, facade = _stats_setup()
    _seed_stats(repo)
    now = datetime.now()
    # 只看最近 90 分钟 → 只命中 inst 101（DOING，创建于 60 分钟前）
    start = (now - timedelta(minutes=90)).strftime("%Y-%m-%d %H:%M:%S")
    end = (now + timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
    r = await facade.flow("processInstance/stats/overview",
                          {"start": start, "end": end})
    assert r["code"] == 0, r
    d = r["data"]
    assert d["total"] == 1
    assert d["inProgress"] == 1
    assert d["completed"] == 0


@pytest.mark.asyncio
async def test_stats_regression_existing_actions():
    """issues/103 回归：stats 加入后不影响已有 action"""
    eng, repo, facade = _stats_setup()
    df = load_flow(repo, "01-simple.json")
    r = await facade.flow("processDefine/deploy", {"content": open(
        os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8").read()})
    assert r["code"] == 0, r
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": df.id, "operator": "zhangsan"})
    assert r["code"] == 0, r


# ═══ issues/114 撤回鉴权 + issues/115 转办（契约 06 §processInstance/withdraw / §processTask/transfer）═══

async def _deploy(facade, filename: str) -> int:
    with open(os.path.join(FLOW_DIR, filename), encoding="utf-8") as f:
        r = await facade.flow("processDefine/deploy", {"content": f.read()})
    assert r["code"] == 0, r
    return int(r["data"]["processDefineId"])


async def _start(facade, define_id: int, operator: str) -> int:
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": define_id, "operator": operator})
    assert r["code"] == 0, r
    return int(r["data"]["processInstanceId"])


@pytest.mark.asyncio
async def test_withdraw_operator_hard_required():
    """issues/114：operator 缺失/空串 → 明确报错，绝不回落 user1；失败路径零副作用"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")

    for args in ({"id": iid}, {"id": iid, "operator": ""}, {"id": iid, "operator": "   "}):
        r = await facade.flow("processInstance/withdraw", args)
        assert r["code"] == 99999999, r
        assert "operator 必填" in r["msg"], r

    # 报错后实例/任务原样不动（缺省回落 user1 的旧实现会在此处把单子撤掉并记成 user1）
    inst = await repo.find_instance_by_id(iid)
    assert inst.state == InstanceState.DOING, f"缺 operator 不得撤回: {inst.state}"
    assert inst.operator == "zhangsan"
    assert len(await repo.find_doing_tasks(iid)) == 1, "缺 operator 不得动 doing 任务"


@pytest.mark.asyncio
async def test_withdraw_ownership_three_criteria():
    """issues/114：三条归属判据（发起人 / 任一进行中任务参与者 / flow.auto·flow.admin）+ update_user 回写"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")
    doing_before = await repo.find_doing_tasks(iid)
    apply_before = [t for t in await repo.find_history_tasks(iid) if t.taskName == "apply"][0]

    # ① 无关第三人：拒绝 + 零副作用
    r = await facade.flow("processInstance/withdraw", {"id": iid, "operator": "intruder"})
    assert r["code"] == 99999999 and "无权限撤回该流程实例" in r["msg"], r
    assert (await repo.find_instance_by_id(iid)).state == InstanceState.DOING
    assert len(await repo.find_doing_tasks(iid)) == 1

    # ② 进行中任务参与者（leader 不是发起人）可撤回**整单**，update_user 回写真实撤回人
    r = await facade.flow("processInstance/withdraw", {"id": iid, "operator": "leader"})
    assert r["code"] == 0 and r["data"] is None, r
    inst = await repo.find_instance_by_id(iid)
    assert inst.state == InstanceState.WITHDRAW, f"实例态应=30: {inst.state}"
    assert inst.updateUser == "leader", f"实例 update_user 应回写撤回人: {inst.updateUser}"
    assert inst.operator == "zhangsan", "发起人字段不得被撤回改写"
    stored = await repo.find_task_by_id(doing_before[0].id)
    assert stored.taskState == TaskState.WITHDRAW, f"撤回任务态应=30: {stored.taskState}"
    assert stored.updateUser == "leader", f"任务 update_user 应回写撤回人: {stored.updateUser}"
    # 已完成(20) 的任务行不得被撤回改写（含 update_user）
    apply_after = await repo.find_task_by_id(apply_before.id)
    assert apply_after.taskState == TaskState.DONE, f"已完成任务应保持 20: {apply_after.taskState}"
    assert apply_after.updateUser == apply_before.updateUser, \
        f"已完成任务 update_user 不应被改写: {apply_after.updateUser} != {apply_before.updateUser}"

    # ③ 发起人自己可撤回
    iid2 = await _start(facade, define_id, "zhangsan")
    r = await facade.flow("processInstance/withdraw", {"id": iid2, "operator": "zhangsan"})
    assert r["code"] == 0, r
    assert (await repo.find_instance_by_id(iid2)).state == InstanceState.WITHDRAW

    # ④ flow.auto / flow.admin 放行（大小写不敏感，沿用 isAllowed 既有约定）
    for op in ("flow.auto", "flow.admin", "FLOW.ADMIN"):
        iidn = await _start(facade, define_id, "zhangsan")
        r = await facade.flow("processInstance/withdraw", {"id": iidn, "operator": op})
        assert r["code"] == 0, (op, r)
        assert (await repo.find_instance_by_id(iidn)).updateUser == op, f"{op} 撤回人应落库"


@pytest.mark.asyncio
async def test_withdraw_by_countersign_member_withdraws_whole_order():
    """issues/114：会签任一成员（参与者判据）可撤回整单——3 条 doing 会签任务全部落 30"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "05-countersign-parallel.json")
    iid = await _start(facade, define_id, "zhangsan")
    members = {a: await _doing_task_id_by_actor(repo, iid, "task1", a) for a in ("userA", "userB", "userC")}
    assert all(members.values()), f"会签三成员任务应齐全: {members}"
    r = await facade.flow("processInstance/withdraw", {"id": iid, "operator": "userB"})
    assert r["code"] == 0, r
    for actor, tid in members.items():
        t = await repo.find_task_by_id(tid)
        assert t.taskState == TaskState.WITHDRAW, f"{actor} 的会签任务应落 30: {t.taskState}"
    assert not await repo.find_doing_tasks(iid), "整单撤回后不得残留 doing 任务"


@pytest.mark.asyncio
async def test_transfer_moves_actor_and_records_submit_type_7():
    """issues/115：转办摘原人 + 追加新人（同一 DOING 任务）+ submitType=7 留痕 + 待办挪位"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")
    task1 = (await repo.find_doing_tasks(iid))[0]
    # 先加签第三人：验证转办只摘 fromActor 那一行，其余参与人不受影响
    assert (await facade.flow("processTask/surrogate",
                              {"processTaskId": task1.id, "actorIds": ["zhaoliu"]})).get("code") == 0
    assert await repo.find_task_actors(task1.id) == ["leader", "zhaoliu"]
    todo_leader = (await facade.flow("processTask/todoList", {"operator": "leader"}))["data"]["rows"]
    assert [t["id"] for t in todo_leader] == [str(task1.id)], todo_leader

    r = await facade.flow("processTask/transfer", {"processTaskId": task1.id, "fromActor": "leader",
                                                   "toActor": "lisi", "reason": "出差，请李四代办",
                                                   "operator": "leader"})
    assert r["code"] == 0 and r["data"] is None, r

    # 参与者：leader 那一行摘走、zhaoliu 保留、lisi 追加
    assert await repo.find_task_actors(task1.id) == ["zhaoliu", "lisi"]
    # 任务不新建：同一 id 仍 DOING，高亮/节点进度不变
    stored = await repo.find_task_by_id(task1.id)
    assert stored.id == task1.id and stored.taskState == TaskState.DOING, \
        f"转办后任务应保持同一 DOING 行: {stored.taskState}"
    hl = await facade.flow("processInstance/highLight", {"id": iid})
    assert "task1" in hl["data"]["activeNodeNames"], hl["data"]
    # 待办从 A 挪到 B
    assert [t["id"] for t in (await facade.flow("processTask/todoList",
                                                {"operator": "lisi"}))["data"]["rows"]] == [str(task1.id)]
    assert str(task1.id) not in [t["id"] for t in (await facade.flow(
        "processTask/todoList", {"operator": "leader"}))["data"]["rows"]], "原办理人待办应消失"
    # 留痕：审批记录 submitType=7 + tf_transferTo/tf_transferReason + 可读文案；
    # 契约 06 §transfer 留痕⚠️：operator/actor_id 列恒无值（严禁覆写），办理人经 update_user + 账本承载
    rec = await facade.flow("processInstance/approvalRecord", {"id": iid})
    assert rec["code"] == 0, rec
    row = [x for x in rec["data"] if x["taskName"] == "task1"][0]
    ext = row["ext"]
    assert int(ext["submitType"]) == 7, f"转办留痕 submitType 应=7: {ext}"
    assert ext["tf_transferTo"] == "lisi" and ext["tf_transferReason"] == "出差，请李四代办", ext
    assert "leader 转办给 lisi" in ext["tf_approvalComment"], ext
    assert "出差，请李四代办" in ext["tf_approvalComment"], ext
    assert stored.actorId in ("", None), f"转办后 DOING 任务 actor_id 应恒无值: {stored.actorId!r}"
    assert row["operator"] in ("", None), f"审批记录 operator 列读回应为空（严禁覆写）: {row}"
    assert stored.updateUser == "leader", f"办理人经 update_user 承载: {stored.updateUser}"
    assert stored.variables["submitType"] == 7 and stored.variables["tf_transferTo"] == "lisi"
    # 留痕三件之②：单跳同样要落一条追加式账本（不是只有多跳才写）
    assert [h["toActor"] for h in ext["tf_transferHistory"]] == ["lisi"], \
        f"tf_transferHistory 应有且仅有 1 条首跳记录: {ext}"

    # 回归：接手人能正常办完该单
    r = await facade.flow("processTask/execute",
                          {"processTaskId": task1.id, "operator": "lisi", "submitType": 1})
    assert r["code"] == 0, r
    assert (await repo.find_instance_by_id(iid)).state == InstanceState.DONE


@pytest.mark.asyncio
async def test_transfer_negative_matrix():
    """issues/115：转办四类明确报错 + operator 必填 + auto/admin 代转"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")
    tid = (await repo.find_doing_tasks(iid))[0].id

    async def bad(payload, keyword):
        r = await facade.flow("processTask/transfer", payload)
        assert r["code"] == 99999999, (payload, r)
        assert keyword in r["msg"], (payload, r)
        return r

    # ① 参数缺失
    await bad({"fromActor": "leader", "toActor": "lisi", "operator": "leader"}, "processTaskId")
    await bad({"processTaskId": tid, "toActor": "lisi", "operator": "leader"}, "fromActor 必填")
    await bad({"processTaskId": tid, "fromActor": "leader", "operator": "leader"}, "toActor 必填")
    # ② operator 必填
    await bad({"processTaskId": tid, "fromActor": "leader", "toActor": "lisi"}, "operator 必填")
    # ③ 越权：C 转别人的单（参与者表不得被改动）
    await bad({"processTaskId": tid, "fromActor": "leader", "toActor": "lisi", "operator": "intruder"},
              "无权限转办该任务")
    assert await repo.find_task_actors(tid) == ["leader"], "越权失败后参与者不得变动"
    # ④ fromActor 不在参与者里
    await bad({"processTaskId": tid, "fromActor": "nosuch", "toActor": "lisi", "operator": "nosuch"},
              "原办理人不是该任务参与人")
    # ⑤ toActor 已是参与者（明确报错，不静默成功）
    await bad({"processTaskId": tid, "fromActor": "leader", "toActor": "leader", "operator": "leader"},
              "目标人已是该任务参与人")
    assert await repo.find_task_actors(tid) == ["leader"]

    # ⑥ flow.admin / flow.auto 可代转（放行分支）
    r = await facade.flow("processTask/transfer", {"processTaskId": tid, "fromActor": "leader",
                                                   "toActor": "boss", "operator": "flow.admin"})
    assert r["code"] == 0, r
    assert await repo.find_task_actors(tid) == ["boss"]

    # ⑦ 任务非进行中：办完再转 → 明确报错，且不改写已完成行
    r = await facade.flow("processTask/execute",
                          {"processTaskId": tid, "operator": "boss", "submitType": 1})
    assert r["code"] == 0, r
    done = await repo.find_task_by_id(tid)
    assert done.taskState == TaskState.DONE
    r = await facade.flow("processTask/transfer", {"processTaskId": tid, "fromActor": "boss",
                                                   "toActor": "lisi", "operator": "boss"})
    assert r["code"] == 99999999 and "任务非进行中，不可转办" in r["msg"], r
    again = await repo.find_task_by_id(tid)
    assert again.taskState == TaskState.DONE and again.actorIds == ["boss"], \
        f"失败转办不得改写已完成任务: {again.taskState} {again.actorIds}"


@pytest.mark.asyncio
async def test_transfer_countersign_only_moves_own_row():
    """issues/115：会签节点转办只摘 fromActor 一行，其余成员待办不受影响"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "05-countersign-parallel.json")
    iid = await _start(facade, define_id, "zhangsan")
    task_a = await _doing_task_id_by_actor(repo, iid, "task1", "userA")
    task_b = await _doing_task_id_by_actor(repo, iid, "task1", "userB")
    assert task_a and task_b, f"会签成员任务应存在: A={task_a} B={task_b}"

    r = await facade.flow("processTask/transfer", {"processTaskId": task_a, "fromActor": "userA",
                                                   "toActor": "userD", "reason": "转岗",
                                                   "operator": "userA"})
    assert r["code"] == 0, r
    assert await repo.find_task_actors(task_a) == ["userD"], "A 的那一行应换成 userD"
    assert await repo.find_task_actors(task_b) == ["userB"], "其余会签成员不受影响"
    moved = await repo.find_task_by_id(task_a)
    assert moved.taskState == TaskState.DOING and moved.variables["tf_transferTo"] == "userD"
    assert not await _doing_task_id_by_actor(repo, iid, "task1", "userA"), "userA 待办应消失"
    assert await _doing_task_id_by_actor(repo, iid, "task1", "userD") == task_a


@pytest.mark.asyncio
async def test_surrogate_still_append_only():
    """issues/115 回归：加签（surrogate/addCandidate）仍是"只追加不清空"，与 transfer 区分"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")
    tid = (await repo.find_doing_tasks(iid))[0].id

    for action in ("processTask/surrogate", "processTask/addCandidate"):
        r = await facade.flow(action, {"processTaskId": tid, "actorIds": [f"extra-{action[-6:]}"]})
        assert r["code"] == 0, (action, r)
    actors = await repo.find_task_actors(tid)
    assert actors[0] == "leader", f"加签原人必须保留: {actors}"
    assert len(actors) == 3, actors
    # 原人仍可办理（未摘走）
    r = await facade.flow("processTask/execute",
                          {"processTaskId": tid, "operator": "leader", "submitType": 1})
    assert r["code"] == 0, r


@pytest.mark.asyncio
async def test_transfer_multi_hop_ledger_survives_completion():
    """契约 06 §4 之②（fc0883a 新增）：tf_transferHistory 是跨跳**追加式**账本。

    A→B、B→C 两跳各留一条，C 办结（submitType=1 覆盖末跳「槽位」，契约明说属预期）后
    账本必须仍是两条——这一条断言同时实测钉死「execute 的
    vars_ = {**base_vars, **task.variables, **args} 是合并含自身，不是整体替换」。
    """
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")
    tid = (await repo.find_doing_tasks(iid))[0].id

    hops = [("leader", "lisi", "出差一周"), ("lisi", "wangwu", "李四也不在，转王五")]
    for src, dst, why in hops:
        r = await facade.flow("processTask/transfer", {"processTaskId": tid, "fromActor": src,
                                                       "toActor": dst, "reason": why,
                                                       "operator": src})
        assert r["code"] == 0, (src, dst, r)

    ledger = (await repo.find_task_by_id(tid)).variables["tf_transferHistory"]
    assert isinstance(ledger, list) and len(ledger) == 2, \
        f"两跳应追加为两条（只追加不覆盖）: {ledger}"
    for (src, dst, why), entry in zip(hops, ledger):
        assert entry["submitType"] == 7, entry
        assert entry["fromActor"] == src and entry["toActor"] == dst, entry
        assert entry["reason"] == why, entry
        assert entry["operator"] == src, entry
        # time 走本栈统一 yyyy-MM-dd HH:mm:ss 串（datetime 直接进 json 会炸序列化）
        assert datetime.strptime(entry["time"], "%Y-%m-%d %H:%M:%S"), entry
        assert set(entry) == {"submitType", "fromActor", "toActor", "reason", "time", "operator"}, entry
    # 便捷键 + 末跳文案只留末跳（契约 §4 之①③）
    assert (await repo.find_task_by_id(tid)).variables["tf_transferTo"] == "wangwu"

    # C 办结：槽位被本次提交参数覆盖属预期，账本不得随之消失
    r = await facade.flow("processTask/execute",
                          {"processTaskId": tid, "operator": "wangwu", "submitType": 1,
                           "tf_approvalComment": "已核实，同意"})
    assert r["code"] == 0, r
    assert (await repo.find_instance_by_id(iid)).state == InstanceState.DONE
    stored = await repo.find_task_by_id(tid)
    assert stored.taskState == TaskState.DONE, stored.taskState
    assert int(stored.variables["submitType"]) == 1, \
        f"合并序 args 最高：C 的 1 应覆盖转办的 7（契约 §5）: {stored.variables['submitType']}"
    assert stored.variables["tf_approvalComment"] == "已核实，同意", "C 自己的意见覆盖末跳文案"
    led_after = stored.variables.get("tf_transferHistory") or []
    assert len(led_after) == 2, f"办结后账本仍必须两条（转办事实不得随槽位消失）: {led_after}"
    assert led_after == ledger, f"办结后两跳账本应逐字段原样存活: {led_after}"
    # 同一实例的两行任务里，只有被转办那条带账本（不污染兄弟任务）
    apply_task = [t for t in (await repo.find_history_tasks(iid)) if t.taskName == "apply"][0]
    assert "tf_transferHistory" not in apply_task.variables, apply_task.variables
    # 审批历史读回：记录读作 C 的同意，转办事实在 ext 账本里
    rec = await facade.flow("processInstance/approvalRecord", {"id": iid})
    row = [x for x in rec["data"] if x["taskName"] == "task1"][0]
    assert int(row["ext"]["submitType"]) == 1, row["ext"]
    assert [h["toActor"] for h in row["ext"]["tf_transferHistory"]] == ["lisi", "wangwu"], row["ext"]


@pytest.mark.asyncio
async def test_transfer_never_overwrites_operator_column_done_list_clean():
    """契约 06 §transfer 留痕⚠️（778340a 固化，Node 实测复现）：转办严禁覆写 actor_id 列——
    进行中任务该列恒无值是既有不变量；写进被摘走的人，该单撤回后离开 DOING 但列值留着，
    page_done_tasks（state <> DOING AND operator = ?）会让他凭空看到从没办过的「我已办」单。
    断言全落持久值/读回值。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")
    tid = (await repo.find_doing_tasks(iid))[0].id

    r = await facade.flow("processTask/transfer", {"processTaskId": tid, "fromActor": "leader",
                                                   "toActor": "lisi", "reason": "出差",
                                                   "operator": "leader"})
    assert r["code"] == 0, r
    mid = await repo.find_task_by_id(tid)
    assert mid.actorId in ("", None), f"转办后持久任务行 actor_id 应恒无值: {mid.actorId!r}"
    assert mid.updateUser == "leader", "办理人经 update_user 承载"
    assert mid.variables["tf_transferHistory"][0]["operator"] == "leader", "真操作人在账本"

    # 发起人撤回：任务离开 DOING(→30)，operator 列不被污染
    r = await facade.flow("processInstance/withdraw", {"id": iid, "operator": "zhangsan"})
    assert r["code"] == 0, r
    after = await repo.find_task_by_id(tid)
    assert after.taskState == TaskState.WITHDRAW, f"撤回后应落 30（判据生效前提）: {after.taskState}"
    assert after.actorId in ("", None), f"撤回后 actor_id 列仍恒无值: {after.actorId!r}"
    # 被摘走的 leader 与未办的 lisi：doneList 都不含该单（冒单即缺陷实证形态）
    for who in ("leader", "lisi"):
        rd = await facade.flow("processTask/doneList", {"operator": who, "pageSize": 100})
        assert rd["code"] == 0, rd
        ids = [t["id"] for t in rd["data"]["rows"]]
        assert str(tid) not in ids and tid not in ids, \
            f"转办→撤回后 {who} 的「我已办」冒入该单（他从没办过）: {ids}"


# ═══ 批次 D · issues/116：委托代理运行期自动生效（引擎内置 · 默认开启 · 可显式关闭）═══
# 契约：06-facade §4.5「运行期语义」六条 + 05-spi「SurrogateInterceptor」+ 08-compliance 用例 26/27

def _win(days_before: int = 1, days_after: int = 1) -> tuple[str, str]:
    """相对「今天」的时间窗（用例不随日历过期）"""
    now = datetime.now()
    return ((now - timedelta(days=days_before)).strftime("%Y-%m-%d %H:%M:%S"),
            (now + timedelta(days=days_after)).strftime("%Y-%m-%d %H:%M:%S"))


@pytest.mark.asyncio
async def test_surrogate_runtime_appends_agent_into_persisted_actors():
    """用例 26 正向：窗口内配 leader→lisi，新单到达 task1 时**代理人真进参与者表**
    （断言落在读回的持久值上，不是返回码）；授权人那一行保留（任一可办）。
    ⚠️ 反例形态：Java 首版靠"事后 add_task_actor 补写"且打在未分配的 taskId 上静默无效，
    本用例的 find_task_actors 读回值正是该缺陷的照妖镜。"""
    eng, repo = setup()
    ext = MemoryExtRepository()
    facade = JeeflowFacade(eng, repo, ext)  # 零配置：传了扩展仓储即默认生效
    start, end = _win()
    r = await facade.flow("processSurrogate/save",
                          {"operator": "leader", "surrogate": "lisi", "processName": "simple",
                           "startTime": start, "endTime": end})
    assert r["code"] == 0, r
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")

    task1 = (await repo.find_doing_tasks(iid))[0]
    assert task1.taskName == "task1", task1.taskName
    actors = await repo.find_task_actors(task1.id)
    assert actors == ["leader", "lisi"], f"代理人应随任务一起进参与者集合（原人保留在后）: {actors}"
    # 任务行的主办理人不被代理人顶掉（issues/115 同口径：actor_id 列恒无值，参与者以 actor 表为准）
    stored = await repo.find_task_by_id(task1.id)
    assert stored.actorId in ("", None), f"追加代理人不得覆写 actor_id: {stored.actorId!r}"
    # 双方待办都看得到这单（任一可办）
    for who in ("leader", "lisi"):
        rt = await facade.flow("processTask/todoList", {"operator": who})
        assert [t["id"] for t in rt["data"]["rows"]] == [str(task1.id)], f"{who} 待办: {rt['data']['rows']}"
    # 台账不受运行期影响（仍是那条委托）
    rp = await facade.flow("processSurrogate/page", {"operator": "leader"})
    assert rp["data"]["recordCount"] == 1, rp["data"]


@pytest.mark.asyncio
async def test_surrogate_applier_dedupe_and_empty_guard():
    """内置应用器：代理人已是参与者 → 不重复；空参与者/空代理人 → 原样返回"""
    eng, repo = setup()
    ext = MemoryExtRepository()
    facade = JeeflowFacade(eng, repo, ext)
    start, end = _win()
    assert (await facade.flow("processSurrogate/save",
                              {"operator": "leader", "surrogate": "lisi", "processName": "simple",
                               "startTime": start, "endTime": end}))["code"] == 0
    assert (await facade.flow("processSurrogate/save",
                              {"operator": "zhangsan", "surrogate": "  ", "processName": "simple",
                               "startTime": start, "endTime": end}))["code"] == 0
    applier = ext_repo_applier(ext)
    # 代理人已在名单 → 不重复插行
    assert await applier.expand(["leader", "lisi"], "simple") == ["leader", "lisi"]
    # 未命中 → 原样；空集合 → 空
    assert await applier.expand(["leader"], "other-flow-none") == ["leader"]
    assert await applier.expand([], "simple") == []
    # 代理人为空白串 → 不追加（引擎侧兜住脏数据，不往参与者表插空行）
    assert await applier.expand(["zhangsan"], "simple") == ["zhangsan"]
    # 正常追加：原人在前、代理人在后（授权人保留）
    assert await applier.expand(["leader"], "simple") == ["leader", "lisi"]
    # 引擎建单同样不重复：task1 参与者恰好两行
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")
    actors = await repo.find_task_actors((await repo.find_doing_tasks(iid))[0].id)
    assert actors == ["leader", "lisi"], actors


def ext_repo_applier(ext):
    from jeeflow.surrogate import ExtRepositorySurrogateApplier
    return ExtRepositorySurrogateApplier(ext)


@pytest.mark.asyncio
async def test_surrogate_runtime_negative_window_disabled_self():
    """用例 26 负向：窗外 / enabled=0 / 代理人为空 → 代理人**不**进参与者集合"""
    now = datetime.now()
    fmt = "%Y-%m-%d %H:%M:%S"
    cases = [
        ("窗外-未开始", {"startTime": (now + timedelta(days=2)).strftime(fmt),
                         "endTime": (now + timedelta(days=3)).strftime(fmt)}),
        ("窗外-已过期", {"startTime": "2020-01-01 00:00:00", "endTime": "2020-12-31 23:59:59"}),
        ("enabled=0", {"startTime": now.strftime(fmt), "endTime": (now + timedelta(days=1)).strftime(fmt),
                       "enabled": 0}),
        ("代理人为空", {"startTime": now.strftime(fmt), "endTime": (now + timedelta(days=1)).strftime(fmt),
                       "surrogate": ""}),
    ]
    for label, extra in cases:
        eng, repo = setup()
        ext = MemoryExtRepository()
        facade = JeeflowFacade(eng, repo, ext)
        args = {"operator": "leader", "surrogate": "lisi", "processName": "simple"}
        args.update(extra)
        r = await facade.flow("processSurrogate/save", args)
        assert r["code"] == 0, (label, r)
        define_id = await _deploy(facade, "01-simple.json")
        iid = await _start(facade, define_id, "zhangsan")
        task1 = (await repo.find_doing_tasks(iid))[0]
        actors = await repo.find_task_actors(task1.id)
        assert actors == ["leader"], f"{label}：代理人不该收到这单，实测参与者 {actors}"
        rt = await facade.flow("processTask/todoList", {"operator": "lisi"})
        assert rt["data"]["rows"] == [], f"{label}：lisi 待办应为空 {rt['data']['rows']}"


@pytest.mark.asyncio
async def test_surrogate_runtime_without_ext_repo_does_not_break_start():
    """用例 26：未配置 IProcessExtRepository → 建单不被打断（静默跳过，不得抛"未配置扩展仓储"）"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, None)  # 无扩展仓储
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")  # _start 内部断言 code==0
    task1 = (await repo.find_doing_tasks(iid))[0]
    assert await repo.find_task_actors(task1.id) == ["leader"], "缺仓储时参与者原样"
    assert eng.ext is None or eng.ext.ext_repository is None, "未传扩展仓储不应凭空造数据源"
    # 委托类 action 仍按原契约明确报错（运行期静默 ≠ 门面静默）
    assert (await facade.flow("processSurrogate/page", {}))["code"] == 99999999


@pytest.mark.asyncio
async def test_surrogate_runtime_explicit_disable_two_routes():
    """用例 26：显式关闭 → 回到"仅台账"。两条关闭路都要有效：
    ① 配置开关 EngineExtensions(surrogate_enabled=False)
    ② 注册空实现 EngineExtensions(surrogate_applier=NullSurrogateApplier())"""
    start, end = _win()

    # ① 开关关闭：在门面构造**之后**设扩展体（验证已接入的扩展仓储跨 set_extensions 保留）
    eng, repo = setup()
    ext = MemoryExtRepository()
    facade = JeeflowFacade(eng, repo, ext)
    assert (await facade.flow("processSurrogate/save",
                              {"operator": "leader", "surrogate": "lisi", "processName": "simple",
                               "startTime": start, "endTime": end}))["code"] == 0
    eng.set_extensions(EngineExtensions(surrogate_enabled=False))
    assert eng.ext.ext_repository is ext, "set_extensions 须保留门面已接入的扩展仓储"
    iid = await _start(facade, await _deploy(facade, "01-simple.json"), "zhangsan")
    task1 = (await repo.find_doing_tasks(iid))[0]
    assert await repo.find_task_actors(task1.id) == ["leader"], "关闭后代理人不得进参与者集合"
    assert await ext.get_surrogate("leader", "simple") is not None, "关闭的是运行期应用，台账仍在"

    # ② 注册空实现
    eng2, repo2 = setup()
    ext2 = MemoryExtRepository()
    facade2 = JeeflowFacade(eng2, repo2, ext2)
    assert (await facade2.flow("processSurrogate/save",
                               {"operator": "leader", "surrogate": "lisi", "processName": "simple",
                                "startTime": start, "endTime": end}))["code"] == 0
    eng2.set_extensions(EngineExtensions(ext_repository=ext2, surrogate_applier=NullSurrogateApplier()))
    iid2 = await _start(facade2, await _deploy(facade2, "01-simple.json"), "zhangsan")
    task2 = (await repo2.find_doing_tasks(iid2))[0]
    assert await repo2.find_task_actors(task2.id) == ["leader"], "空实现注册后不应用委托"


@pytest.mark.asyncio
async def test_surrogate_runtime_engine_direct_and_full_flow_fallback():
    """引擎直用（不经门面）也内置生效：attach_ext_repository + 判据① 空 processName 全流程兜底。
    同时验证代理人自身不再级联委托（A→B、B→C 时 C 不收到）。"""
    eng, repo = setup()
    ext = MemoryExtRepository()
    now = datetime.now()
    await ext.save_surrogate(ProcessSurrogate(operator="leader", surrogate="lisi", processName="",
                                              startTime=now - timedelta(days=1), endTime=now + timedelta(days=1)))
    await ext.save_surrogate(ProcessSurrogate(operator="lisi", surrogate="wangwu", processName="", enabled=1))
    eng.attach_ext_repository(ext)
    df = load_flow(repo, "01-simple.json")  # 定义 name=文件名，委托走空 processName 兜底
    inst = await _start_and_execute(eng, repo, df.id, "zhangsan")
    task1 = (await repo.find_doing_tasks(inst.id))[0]
    actors = await repo.find_task_actors(task1.id)
    assert actors == ["leader", "lisi"], f"兜底委托应命中且代理人不级联: {actors}"

    # 关掉开关（引擎直用形态）
    eng.ext.surrogate_enabled = False
    inst2 = await _start_and_execute(eng, repo, df.id, "zhangsan")
    task2 = (await repo.find_doing_tasks(inst2.id))[0]
    assert await repo.find_task_actors(task2.id) == ["leader"], "关闭后回到仅台账"


@pytest.mark.asyncio
async def test_surrogate_query_four_criteria_memory_repo():
    """用例 27 内存仓侧：四判据须与 SQL 仓（jdbc_test ⑮）同答案。
    此前内存仓缺判据③ 自委托过滤、判据④ 用 `!= 1` 松判、多条命中取首条（SQL 取最新）→ 同栈两仓分叉。"""
    ext = MemoryExtRepository()
    now = datetime.now()
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, ext)

    async def add(pn, sur, start=None, end=None, enabled=1, op="boss"):
        s = ProcessSurrogate(operator=op, surrogate=sur, processName=pn,
                             startTime=start, endTime=end, enabled=enabled)
        await ext.save_surrogate(s)
        return s

    async def add_api(pn, sur, enabled=1, op="boss"):
        """同 `add`，但**经门面写侧**（API 形状 `processSurrogate/save`）。

        写侧 `_to_int` 在落库前把 `"1"` / `True` 归一成整数 1、把 `""` / `"abc"` 落成 0，
        而 `add` 走的是 SPI 仓储 `save_surrogate`——自 owner 2026-09-29 拍"内存仓统一到 node 侧"后，
        那条边界只把**规范整数串**（`"1"`/`"0"`/`"2"`）落成 int，`"abc"` 这类脏值原样留着。
        判据④自 issues/130 案 A 起始终只认整数 1（两档边界归一都在**边界**，不在判据里），
        所以"④ `"1"` 等价启用"由**写侧**（门面或内存仓 save 边界）兑现 —— 门面这一档就用它来钉。
        """
        r = await facade.flow("processSurrogate/save",
                              {"operator": op, "surrogate": sur, "processName": pn,
                               "enabled": enabled})
        assert r["code"] == 0, r
        return r

    # ① 精确优先 → 未命中回落空 processName 兜底（各组用独立授权人，免被兜底行串味）
    await add("", "g1", op="c1")
    hit = await ext.get_surrogate("c1", "any-flow")
    assert hit is not None and hit.surrogate == "g1", "① 空 processName 兜底"
    await add("leave", "exact", op="c1")
    hit = await ext.get_surrogate("c1", "leave")
    assert hit.surrogate == "exact", "① 精确命中优先于兜底"
    assert (await ext.get_surrogate("c1", "")).surrogate in ("g1", "exact"), "① 传空名走兜底"

    # ① 多条兜底命中 → 取 id 最大（对齐 SQL ORDER BY id DESC LIMIT 1）
    await add("", "g-new", op="c1")
    hit = await ext.get_surrogate("c1", "other-flow")
    assert hit.surrogate == "g-new", f"多条命中应取最新一条（与 SQL 仓同答案）: {hit.surrogate}"

    # ② 时间窗：任一侧 None = 该侧不限；两侧都越界则不生效
    await add("win", "future", start=now + timedelta(days=2), end=now + timedelta(days=3), op="c2")
    assert await ext.get_surrogate("c2", "win") is None, "② 未到窗"
    await add("win2", "past", start=now - timedelta(days=5), end=now - timedelta(days=4), op="c2")
    assert await ext.get_surrogate("c2", "win2") is None, "② 已过窗"
    await add("win3", "open-end", start=now - timedelta(days=1), op="c2")
    assert (await ext.get_surrogate("c2", "win3")).surrogate == "open-end", "② end 不限"
    await add("win4", "open-start", end=now + timedelta(days=1), op="c2")
    assert (await ext.get_surrogate("c2", "win4")).surrogate == "open-start", "② start 不限"
    await add("win5", "text-window", op="c2",
              start=(now - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
              end=(now + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"))
    assert (await ext.get_surrogate("c2", "win5")).surrogate == "text-window", "② 文本窗同样判定"
    await add("win6", "text-out", start="2020-01-01 00:00:00", end="2020-12-31 23:59:59", op="c2")
    assert await ext.get_surrogate("c2", "win6") is None, "② 文本窗越界不生效"

    # ③ 自委托过滤（同栈此前只有 SQL 仓有该过滤，内存仓漏 → 同数据两仓不同答案）
    await add("self", "c3", op="c3")
    assert await ext.get_surrogate("c3", "self") is None, "③ 精确路径自己委托给自己不生效"
    await add("", "c3", op="c3")  # 全流程自委托：兜底路径也必须过滤
    assert await ext.get_surrogate("c3", "any-flow") is None, "③ 兜底路径同样过滤自委托"

    # ④ enabled 只认 1（脏值不得当启用；SQL 列 INT 存不进文本，两仓同答案口径）
    for dirty, why in ((0, "零停用"), (None, "NULL非启用"), ("abc", "脏值"), (2, "非1整数")):
        await add("en-" + why, "agent", enabled=dirty, op="c4")
        assert await ext.get_surrogate("c4", "en-" + why) is None, f"④ {why} 不得生效"
    # "1" 等价启用属**写侧边界**语义（案 A：判据只认整数，门面 _to_int 先把 "1" 折成整数 1 再落库；
    # 内存仓 save_surrogate 的写侧边界同形，见 test_surrogate_memory_write_boundary_normalizes_canonical_int）。
    # 绕过两条写侧边界的原值（update_surrogate 那条不归一出口）一律停用，
    # 见 test_surrogate_enabled_strict_integer_one_dirty_matrix。
    await add_api("en-str", "agent", enabled="1", op="c4")
    assert (await ext.get_surrogate("c4", "en-str")).surrogate == "agent", '④ "1" 等价启用'
    assert surrogate_enabled_on("abc") is False and surrogate_enabled_on(1) is True
    assert to_datetime("2026-13-45") is None, "不可解析时间 = 该侧不限"


# ─── issues/130 案 A：判据④「只认整数 1」的脏值矩阵（owner 2026-09-28 拍板）─────────

_DIRTY_ENABLED = [
    ("整数 2（非 1 值）", 2),
    ("字符串 '1'（本案收窄的分叉点）", "1"),
    ("浮点 1.0（同上）", 1.0),
    ("布尔 True（Python 里 True == 1 恒成立）", True),
    ("不可解析文本 'x'", "x"),
    ("None（NULL）", None),
    ("空串 ''", ""),
    ("负整数 -1", -1),
    # 边界还原只认规范整数串 `-?(0|[1-9]\d*)`：下面三档看着"接近 1"，`int()`/浮点转换都会折成 1，
    # 但按案 A 既不还原也不生效（内存仓没有驱动，原值就是这些字符串；SQL 仓的还原规则见
    # test_surrogate_hydrate_enabled_boundary_rules / …_sql_driver_stringified_…）
    ("字符串 '1.0'（带小数点，非规范整数串）", "1.0"),
    ("字符串 ' 1'（带空格，非规范整数串）", " 1"),
    ("字符串 '01'（前导零，非规范整数串）", "01"),
]


async def _row_and_start(enabled):
    """把给定 ``enabled`` **原值**盖进台账，建一条窗内委托并发起一单。

    写姿逐字对齐 jeeflow-node ``__tests__/spec.test.ts`` 的脏值矩阵（owner 2026-09-29 拍
    「python 内存仓统一到 node 侧」之后，台账原值唯一的显形路径）：
    先 ``save_surrogate`` 落一行，再用**不做写侧归一的** ``update_surrogate`` 把原值整行盖回。
    内存仓写侧边界自本轮起把**规范整数串** ``'1'`` 折成整数 1（它建模的就是 INT 列，SQL 那一步
    由数据库做），而 ``update_surrogate`` 是那条有意不对称的原值出口（issues/130 §2）。

    仍**不经**门面 ``processSurrogate/save`` 的 ``_to_int`` 归一 ⇒ 判据④本身照旧被钉住；
    每档下面那句"台账里真的是这个值/类型"的前置自检保证负向断言不是空转。
    """
    eng, repo = setup()
    ext = MemoryExtRepository()
    facade = JeeflowFacade(eng, repo, ext)
    now = datetime.now()
    s = ProcessSurrogate(operator="leader", surrogate="lisi", processName="simple",
                         startTime=now - timedelta(days=1), endTime=now + timedelta(days=1),
                         enabled=enabled)
    await ext.save_surrogate(s)
    s.enabled = enabled             # 写侧边界可能已把 '1' 折成整数 1 ⇒ 盖回调用方原值
    await ext.update_surrogate(s)   # 整行覆盖、不归一（node memory-ext.updateSurrogate 同形）
    iid = await _start(facade, await _deploy(facade, "01-simple.json"), "zhangsan")
    return repo, ext, facade, (await repo.find_doing_tasks(iid))[0]


@pytest.mark.asyncio
async def test_surrogate_enabled_strict_integer_one_dirty_matrix():
    """issues/130 案 A：``enabled`` 只认**整数 1**，脏值矩阵逐档断"委托不生效"。

    ⚠️ 判据打在**委托是否命中**的行为上（三处读数），不打在 ``surrogate_enabled_on`` 的返回值上：
    ① 建单后读回**持久化的参与者集合**——代理人真没混进 actor 表才算数（issues/116 首版正是
       只在内存里绿了、落库静默无效）；② 代理人待办列表为空；③ 台账查询 ``get_surrogate`` 判否。
    正向整数 1 单独一档把这三处断言**反向**钉一遍，证明负向档不是"根本没数据"的空转。
    """
    for label, dirty in _DIRTY_ENABLED + [("整数 0（契约停用值）", 0)]:
        repo, ext, facade, task1 = await _row_and_start(dirty)
        actors = await repo.find_task_actors(task1.id)
        assert actors == ["leader"], f"{label}：代理人不得进参与者集合，实测 {actors}"
        assert await ext.get_surrogate("leader", "simple") is None, f"{label}：台账查询不得命中"
        rt = await facade.flow("processTask/todoList", {"operator": "lisi"})
        assert rt["data"]["rows"] == [], f"{label}：lisi 待办应为空 {rt['data']['rows']}"
        # 停用 ≠ 删档；且原值得是原判据的那个形状（否则矩阵被写入侧偷偷归一了）
        rows, total = await ext.page_surrogates(1, 10, {"operator": "leader"})
        assert total == 1, f"{label}：台账行须还在（判据只判生效，不判存在）"
        assert type(rows[0].enabled) is type(dirty) and rows[0].enabled == dirty, \
            f"{label}：SPI 仓储存的原值被改写了，实测 {rows[0].enabled!r}"

    # 正向对照：整数 1 → 三处读数全部反向成立
    repo, ext, facade, task1 = await _row_and_start(1)
    actors = await repo.find_task_actors(task1.id)
    assert actors == ["leader", "lisi"], f"整数 1 必须生效，实测 {actors}"
    hit = await ext.get_surrogate("leader", "simple")
    assert hit is not None and hit.surrogate == "lisi", f"整数 1 台账应命中: {hit}"
    rt = await facade.flow("processTask/todoList", {"operator": "lisi"})
    assert [t["id"] for t in rt["data"]["rows"]] == [str(task1.id)], f"lisi 待办: {rt['data']['rows']}"

    # 谓词本体档位收口（辅助层：行为三处已各自钉过，这里只钉"接受集合 = {整数 1}"本身）
    for dirty in (2, "1", 1.0, True, "x", None, "", -1, 0):
        assert surrogate_enabled_on(dirty) is False, f"判据④：{dirty!r} 不得算启用"
    assert surrogate_enabled_on(1) is True, "判据④：只有整数 1 算启用"


@pytest.mark.asyncio
async def test_surrogate_memory_write_boundary_normalizes_canonical_int():
    """任务 2（owner 2026-09-29 拍「python 内存仓统一到 node 侧」）：内存仓的**委托写入边界**
    把规范整数串归一成 int（``'1'→1``、``'0'→0``），**判据④本体一个字不放宽**。

    为什么这是契约面而不是"把测试改绿"：``wf_process_surrogate.enabled`` 是 INT 列
    （``tests/schema/schema-mysql.sql``），SQL 仓那侧 "1" 进列就被数据库折成整数 1；内存仓是同一张
    列的替身，却没人做这一步 ⇒ 同一条 ``'1'`` 两栈两答案（node 归一、python 停用），正是
    issues/130 的遗留分叉。归一补在**边界**（写侧），不是补在**判据**里——
    ``surrogate_enabled_on`` 仍只认整数 1（下面的负向档 + 脏值矩阵都在钉这一条）。

    与 node ``memory-ext.saveSurrogate`` 的两处同形：
    ① 只认规范整数串 ``-?(0|[1-9]\\d*)``（``'1.0'``/``' 1'``/``'01'``/``'+1'``/``'abc'`` 原样留着）；
    ② **只有 save 归一**，``update_surrogate`` 整行覆盖不归一（有意不对称，issues/130 §2 的原值出口）。
    """
    ext = MemoryExtRepository()
    now = datetime.now()

    async def put(enabled, pn):
        s = ProcessSurrogate(operator="boss", surrogate="agent", processName=pn,
                             startTime=now - timedelta(days=1), endTime=now + timedelta(days=1),
                             enabled=enabled)
        await ext.save_surrogate(s)
        return await ext.find_surrogate_by_id(s.id)

    # ① 规范整数串 ⇒ 写侧落成 int；生效与否仍由**值**决定（还原只补类型，不放值）
    row = await put("1", "wb-one")
    assert type(row.enabled) is int and row.enabled == 1, f"写侧应把 '1' 落成整数 1: {row.enabled!r}"
    assert (await ext.get_surrogate("boss", "wb-one")).surrogate == "agent", "'1' 归一后按整数 1 生效"
    row = await put("0", "wb-zero")
    assert type(row.enabled) is int and row.enabled == 0, f"写侧应把 '0' 落成整数 0: {row.enabled!r}"
    assert await ext.get_surrogate("boss", "wb-zero") is None, "'0' 归一是补类型，不是补成启用"
    row = await put("2", "wb-two")
    assert type(row.enabled) is int and row.enabled == 2, f"'2' 同样还原成整数 2: {row.enabled!r}"
    assert await ext.get_surrogate("boss", "wb-two") is None, "整数 2 非 1 ⇒ 停用（还原≠判宽）"

    # ② 非规范整数串 / 非字符串 ⇒ **原样留着**交判据停用（接受集合没有放宽）
    for keep in ("1.0", " 1", "1 ", "01", "+1", "1abc", "abc", "", "true"):
        row = await put(keep, f"wb-raw-{abs(hash(keep))}")
        assert row.enabled == keep and type(row.enabled) is str, \
            f"非规范整数串 {keep!r} 不得被写侧归一，实测 {row.enabled!r}({type(row.enabled).__name__})"
        assert await ext.get_surrogate("boss", f"wb-raw-{abs(hash(keep))}") is None, \
            f"非规范整数串 {keep!r} 停用"
    for keep in (True, False, 1.0, 2.5, None, [1]):
        pn = f"wb-obj-{abs(hash(str(keep)))}"
        row = await put(keep, pn)
        assert row.enabled == keep or (keep is None and row.enabled is None), \
            f"非字符串入参 {keep!r} 原样透传，实测 {row.enabled!r}"
        assert await ext.get_surrogate("boss", pn) is None, f"{keep!r} 不得算启用"

    # ③ 正向对照：整数 1 直存照常生效（写侧归一不是唯一能让委托生效的路）
    row = await put(1, "wb-int-one")
    assert type(row.enabled) is int and row.enabled == 1
    assert (await ext.get_surrogate("boss", "wb-int-one")).surrogate == "agent"

    # ④ update_surrogate **不归一**（与 save 有意不对称＝原值显形出口，脏值矩阵走这一路）
    ext2 = MemoryExtRepository()
    s = ProcessSurrogate(operator="boss", surrogate="agent", processName="wb-upd",
                         startTime=now - timedelta(days=1), endTime=now + timedelta(days=1), enabled=1)
    await ext2.save_surrogate(s)
    s.enabled = "1"
    await ext2.update_surrogate(s)
    back = await ext2.find_surrogate_by_id(s.id)
    assert back.enabled == "1" and type(back.enabled) is str, \
        f"update 是整行覆盖的原值出口，不得归一，实测 {back.enabled!r}"
    assert (await ext2.get_surrogate("boss", "wb-upd")) is None, \
        "update 归一与否都不放宽判据：台账里是文本 '1' 时判据④仍判停用（本档钉的就是不归一这一半）"

    # ⑤ 判据本体没动（放宽判据 = 把这条红掉）
    assert surrogate_enabled_on("1") is False, "判据④仍不吃串：归一只发生在写侧边界，不在判据里"


# ─── issues/130 案 A 的**读侧另一半**：驱动串化在仓储边界还原（对齐 php cf93d8f）────────

# 建表列类型对齐 tests/schema/schema-mysql.sql:113-128（`enabled INT NULL DEFAULT 1`）：
# 刻意用 INTEGER 声明 enabled，让 SQLite 的列亲和性与 MySQL INT 列同形（数字文本入库折成整数，
# 'abc' 这类存不进去的原样留文本）——"驱动把整数列回读成字符串"才是本节的唯一变量。
_SURROGATE_DDL = ("CREATE TABLE wf_process_surrogate ("
                  " id INTEGER PRIMARY KEY, process_name TEXT, operator TEXT, surrogate TEXT,"
                  " start_time TEXT, end_time TEXT, enabled INTEGER DEFAULT 1, create_time TEXT,"
                  " create_user TEXT, update_time TEXT, update_user TEXT)")


class _SqliteConn:
    """`repository.base.SqlConnection` 的 sqlite3 实现（同步驱动套 async 壳，与 MysqlConnection 同形状）。

    时间列一律用契约文本 `yyyy-MM-dd HH:mm:ss` 绑定（调用方显式给 createTime/updateTime），
    不依赖 sqlite3 的 datetime 默认适配器（Python 3.12 起已标记弃用、后续版本会移除），
    免得本格变成版本红。
    """

    def __init__(self, raw):
        self._raw = raw

    async def execute(self, sql, args):
        self._raw.execute(sql, tuple(args))
        self._raw.commit()

    async def fetchone(self, sql, args):
        return self._raw.execute(sql, tuple(args)).fetchone()

    async def fetchall(self, sql, args):
        return self._raw.execute(sql, tuple(args)).fetchall()

    async def begin(self):
        pass    # autocommit 语义（与 aiomysql pool autocommit=True 同），事务由 with_tx 显式控制

    async def commit(self):
        self._raw.commit()

    async def rollback(self):
        pass


class _SqliteAdapter:
    """委托表单表适配器（placeholder `?` = 核心 SQL 原生风格，无需转换）"""

    placeholder = "?"

    def __init__(self, raw):
        self._conn = _SqliteConn(raw)

    async def acquire(self):
        return self._conn

    async def release(self, conn):
        pass


class _StringifyDriverConn:
    """**假驱动层**：把委托行里 `enabled` 那一列换成**字符串**回读——复现"整数列到宿主手里成了 `'1'`"
    这一类驱动/接入层边界事实（PHP 同栈实证：PDO 缓冲查询 `ATTR_EMULATE_PREPARES` /
    `ATTR_STRINGIFY_FETCHES` 把数值列一律回读成字符串；Python 侧 aiomysql/asyncpg 默认按列类型转成 int，
    但文本协议经代理/网关把列类型报成 VAR_STRING、列类型漂移、遗留 VARCHAR 台账、业务方自己拼行都给出 `'1'`）。

    刻意只串化 `enabled` 一列（`_SURROGATE_COLS` 第 7 列）：id 精度、时间列类型那些是别的驱动议题，
    混进来会让本节的变异信号失真（摘掉 `hydrate_enabled` 时只有委托这格红）。
    """

    _ENABLED_INDEX = 6      # id, process_name, operator, surrogate, start_time, end_time, **enabled**, ...
    _SURROGATE_WIDTH = 11   # COUNT(*) 等窄行原样放行

    def __init__(self, inner):
        self._inner = inner

    @classmethod
    def _wire_row(cls, sql, row):
        if row is None or len(row) != cls._SURROGATE_WIDTH or "wf_process_surrogate" not in sql:
            return row
        v = row[cls._ENABLED_INDEX]
        if isinstance(v, bool) or not isinstance(v, int):
            return row
        out = list(row)
        out[cls._ENABLED_INDEX] = str(v)
        return tuple(out)

    async def execute(self, sql, args):
        return await self._inner.execute(sql, args)

    async def fetchone(self, sql, args):
        return self._wire_row(sql, await self._inner.fetchone(sql, args))

    async def fetchall(self, sql, args):
        return [self._wire_row(sql, r) for r in await self._inner.fetchall(sql, args)]

    async def begin(self):
        await self._inner.begin()

    async def commit(self):
        await self._inner.commit()

    async def rollback(self):
        await self._inner.rollback()


class _StringifyDriverAdapter:
    placeholder = "?"

    def __init__(self, inner):
        self._inner = inner

    async def acquire(self):
        return _StringifyDriverConn(await self._inner.acquire())

    async def release(self, conn):
        await self._inner.release(conn._inner)


def _surrogate_sql_ext(stringify_driver: bool) -> tuple:
    """真 SQLite + 真 `JdbcProcessExtRepository` SQL；`stringify_driver=True` 时中间插一层假驱动。"""
    raw = sqlite3.connect(":memory:")
    raw.execute(_SURROGATE_DDL)
    base = _SqliteAdapter(raw)
    adapter = _StringifyDriverAdapter(base) if stringify_driver else base
    return raw, JdbcProcessExtRepository(adapter, _TestIDGen())


def test_surrogate_hydrate_enabled_boundary_rules():
    """**边界还原纯函数**判据（案 A 的另一半）：只把**规范整数串**换回 int，判据本身不吃串。

    与"判据放宽"的分界就在这里：`'2'` 被还原成整数 `2`，判据④照样停用——还原只补**类型**、不放**值**；
    而 `'1.0'` / `' 1'` / `'01'` / `'1abc'` 不是规范整数串，原样返回 ⇒ 停用（`int()` 强转换个地方做
    就会把它们全折成 1，那才是把 `(int)` 搬个家的假修复）。
    """
    # ① 还原：规范整数串 → int（值不变，只是类型回来）
    for text, want in (("1", 1), ("0", 0), ("2", 2), ("-1", -1), ("10", 10), ("1234567890123", 1234567890123)):
        got = hydrate_enabled(text)
        assert got == want and type(got) is int, f"边界还原：{text!r} 应换回整数 {want}，实测 {got!r}"

    # ② 不还原：非规范整数串 + 非字符串一律原样返回（连类型都不动）
    for keep in ("1.0", " 1", "1 ", "01", "+1", "1abc", "abc", "", " ", ".", "0x1", "١"):
        got = hydrate_enabled(keep)
        assert got == keep and type(got) is str, f"边界不还原：{keep!r} 须原样返回，实测 {got!r}"
    for keep in (1, 0, 2, -1, 1.0, 0.0, True, False, None, [1], {"enabled": 1}):
        got = hydrate_enabled(keep)
        assert got is keep or got == keep and type(got) is type(keep), \
            f"边界只动字符串列值：{keep!r} 被改写成 {got!r}"

    # ③ 还原 + 判据④的连携：交判据前完成，判据本身一个字不吃串
    assert surrogate_enabled_on(hydrate_enabled("1")) is True, "驱动串化的整数 1 在边界还原后必须算启用"
    for dirty in ("2", "0", "-1", "1.0", " 1", "01", "abc", "", "1abc"):
        assert surrogate_enabled_on(hydrate_enabled(dirty)) is False, \
            f"边界还原不得把 {dirty!r} 变成启用（判据④只认整数 1）"
    for non_str in (1.0, True, None, [1]):
        assert surrogate_enabled_on(hydrate_enabled(non_str)) is False, \
            f"边界还原不吃 {non_str!r}，判据④同样停用"

    # ④ SPI 实现侧义务（issues/130 §2）：自定义仓储传非整数按停用，要生效得自己先还原
    row = ProcessSurrogate(operator="boss", surrogate="lisi", enabled="1")
    assert row.is_effective("boss") is False, "判据不吃串：自定义 SPI 仓储原样传 '1' 就是停用"
    row.enabled = hydrate_enabled(row.enabled)
    assert row.is_effective("boss") is True, "实现侧在装行处先还原，才交得出'与内置 SQL 仓同答案'"


@pytest.mark.asyncio
async def test_surrogate_sql_driver_stringified_enabled_is_hydrated_at_boundary():
    """案 A 读侧的**驱动边界格**：SQL 路整数列被驱动回读成字符串 `'1'` 时，委托必须照常命中。

    为什么用"真 SQLite + 假驱动层"而不是真库：本栈 pytest 不落真 MySQL（`tests/jdbc_test.py` 要
    160 那台机，本机恒不可用），而"INT 列回读成字符串"是**驱动层**事实、不是 SQL 事实——
    所以 SQL 与行映射全程真跑（含 `ORDER BY id DESC LIMIT 1`），只在取回行后把 `enabled` 换成字符串，
    等价复现 PHP 侧 `ATTR_STRINGIFY_FETCHES` 的形态。⚠️ 变异对照（本格的判别力自证，已实测）：摘掉
    `repository/ext._map_surrogate` 里的 `hydrate_enabled` → ① 当场翻红（该用例其后的档随之中断），
    而脏值矩阵与其余各档全绿；只收窄判据不补边界还原，这类驱动的宿主上委托会**整体静默判废且零告警**。
    """
    # ① 真表落整数 1 → 驱动给 '1' → 仍须命中（判据前已在边界还原）
    raw, ext = _surrogate_sql_ext(stringify_driver=True)
    s = ProcessSurrogate(operator="opOne", surrogate="agentOne", processName="flowOne", enabled=1,
                         createTime="2026-01-01 00:00:00", updateTime="2026-01-01 00:00:00")
    await ext.save_surrogate(s)
    seed = raw.execute("SELECT typeof(enabled), enabled FROM wf_process_surrogate WHERE id=?",
                       (s.id,)).fetchone()
    assert seed == ("integer", 1), f"种子自证：真表列值须是整数 1（驱动串化才是唯一变量），实测 {seed}"
    hit = await ext.get_surrogate("opOne", "flowOne")
    assert hit is not None and hit.surrogate == "agentOne", \
        "驱动把 INT 列回读成字符串 '1' 时 SQL 路仍须命中（案 A 的驱动边界还原，漏做＝这类宿主的委托整体判废）"
    assert type(hit.enabled) is int and hit.enabled == 1, \
        f"交判据④之前，行里的 enabled 须已在仓储边界还原成整数，实测 {hit.enabled!r}"

    # ② 台账读回同一条路：装行处补类型，'1' 不得漏进门面 detail/page 的 JSON
    back = await ext.find_surrogate_by_id(s.id)
    assert type(back.enabled) is int, f"find_surrogate_by_id 装行须还原成 int，实测 {back.enabled!r}"
    rows, total = await ext.page_surrogates(1, 10, {"operator": "opOne"})
    assert total == 1 and type(rows[0].enabled) is int, \
        f"page_surrogates 装行须还原成 int（否则前端拿到字符串），实测 total={total} {rows and rows[0].enabled!r}"

    # ③ 还原只补类型不放值：驱动给 '2' → 整数 2 → 判据④照样停用
    s2 = ProcessSurrogate(id=900002, operator="opTwo", surrogate="agentTwo", processName="flowTwo",
                          enabled=2, createTime="2026-01-01 00:00:00", updateTime="2026-01-01 00:00:00")
    await ext.save_surrogate(s2)
    assert raw.execute("SELECT typeof(enabled) FROM wf_process_surrogate WHERE id=900002").fetchone()[0] \
        == "integer", "种子自证：③ 那行真表是整数 2（驱动给 '2'）"
    assert await ext.get_surrogate("opTwo", "flowTwo") is None, \
        "边界还原不是把 (int) 强转换个地方做：'2' 还原成整数 2 后判据④仍停用"

    # ④ 真表列值是文本脏值：边界原样交判据 ⇒ 停用
    raw.execute("INSERT INTO wf_process_surrogate (id, process_name, operator, surrogate, enabled)"
                " VALUES (900001, 'flowTxt', 'opTxt', 'agentTxt', 'abc')")
    txt_seed = raw.execute("SELECT typeof(enabled), enabled FROM wf_process_surrogate WHERE id=900001").fetchone()
    assert txt_seed == ("text", "abc"), f"种子自证：INT 列存不进 'abc'，真表里就是文本，实测 {txt_seed}"
    assert await ext.get_surrogate("opTxt", "flowTxt") is None, \
        "边界只认规范整数串：真表列值是文本 'abc' 仍判停用"

    # ⑤ 双仓同答案（用例 27 / 06 §4.5 条款 6）不因收窄 + 边界还原而破：同一个列值两仓同一个结论
    mem = MemoryExtRepository()
    await mem.save_surrogate(ProcessSurrogate(operator="opOne", surrogate="agentOne",
                                              processName="flowOne", enabled=1))
    mem_hit = await mem.get_surrogate("opOne", "flowOne")
    assert mem_hit is not None and mem_hit.surrogate == hit.surrogate, \
        f"同一份「整数 1」列值，内存仓与串化驱动的 SQL 仓必须同结论：SQL={hit} 内存={mem_hit}"

    # ⑥ 同一个 **Python 入参** "1"：**两仓同结论**（owner 2026-09-29 拍「内存仓统一到 node 侧」）。
    #    SQL 路靠 INT 列的列亲和性把 '1' 折成整数 1；内存仓没有驱动，那一步由 save_surrogate 的
    #    **写侧边界**补上（本台账建模的就是那张 INT 列，node memory-ext.saveSurrogate 同形）。
    #    两仓都在**边界**补类型、判据④一个字不动 ⇒ 台账里是整数 1 就生效；非规范串（'1.0' / ' 1' /
    #    '01' / 'abc'）与布尔/浮点仍原样留着判停用（脏值矩阵走 update_surrogate 那条不归一的出口）。
    s3 = ProcessSurrogate(id=900003, operator="opCoerce", surrogate="agentCoerce",
                         processName="flowCoerce", enabled="1",
                         createTime="2026-01-01 00:00:00", updateTime="2026-01-01 00:00:00")
    await ext.save_surrogate(s3)     # 绕过门面，原值进写侧边界
    coerced = raw.execute("SELECT typeof(enabled), enabled FROM wf_process_surrogate WHERE id=900003").fetchone()
    assert coerced == ("integer", 1), f"列亲和性自证：INT 列把 '1' 折成整数 1，实测 {coerced}"
    assert (await ext.get_surrogate("opCoerce", "flowCoerce")).surrogate == "agentCoerce", \
        "列里真是整数 1 就必须生效（不管它是写侧归一还是列亲和性折出来的）"
    mem_raw = MemoryExtRepository()
    await mem_raw.save_surrogate(s3)
    mem_row = await mem_raw.find_surrogate_by_id(s3.id)
    assert type(mem_row.enabled) is int and mem_row.enabled == 1, \
        f"内存仓写侧边界须把规范整数串 '1' 落成整数 1（补的就是 SQL 那侧列亲和性做的一步），" \
        f"实测 {mem_row.enabled!r}"
    assert (await mem_raw.get_surrogate("opCoerce", "flowCoerce")).surrogate == "agentCoerce", \
        "同一份 '1' 两仓必须同结论——本轮收掉的正是这个跨栈分叉（python 不归一 / node 归一）"

    # ⑦ 对照组：同一套 SQL、同一份列值，只是**不插**假驱动层（＝本栈 aiomysql/asyncpg 的默认形态）
    #    也必须命中 ⇒ 证明 ① 那格的唯一变量就是"驱动给串"，而不是假驱动层自带了生效能力。
    _, ext_plain = _surrogate_sql_ext(stringify_driver=False)
    p = ProcessSurrogate(id=900004, operator="opPlain", surrogate="agentPlain", processName="flowPlain",
                         enabled=1, createTime="2026-01-01 00:00:00", updateTime="2026-01-01 00:00:00")
    await ext_plain.save_surrogate(p)
    plain_hit = await ext_plain.get_surrogate("opPlain", "flowPlain")
    assert plain_hit is not None and type(plain_hit.enabled) is int and plain_hit.surrogate == "agentPlain", \
        f"原生类型驱动那侧同样命中且列值仍是整数（两形态同答案）: {plain_hit}"


@pytest.mark.asyncio
async def test_facade_surrogate_save_dirty_enabled_is_off():
    """判据④ 写入侧：显式传脏值（"abc"）→ 停用；未传 → 契约默认 1（两者不得同解）"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    r = await facade.flow("processSurrogate/save",
                          {"operator": "boss", "surrogate": "agent", "processName": "leave",
                           "enabled": "abc"})
    assert r["code"] == 0, r
    s = await facade._ext.find_surrogate_by_id(int(r["data"]["id"]))
    assert s.enabled == 0, f'脏值 enabled="abc" 不得当启用，实测落库 {s.enabled}'
    assert await facade._ext.get_surrogate("boss", "leave") is None, "脏值委托不生效"

    r2 = await facade.flow("processSurrogate/save",
                           {"operator": "boss2", "surrogate": "agent2", "processName": "leave"})
    s2 = await facade._ext.find_surrogate_by_id(int(r2["data"]["id"]))
    assert s2.enabled == 1, f"未传 enabled 按契约默认 1: {s2.enabled}"
    assert (await facade._ext.get_surrogate("boss2", "leave")).surrogate == "agent2"

    # 边界：显式 "1" 字符串等价 1；**空串属脏值 → 0**（契约 06 §4.5 条款 5 写侧新措辞，
    # 此前本栈把空串当未传落 1，与 Java/PHP/C# 反向）
    r3 = await facade.flow("processSurrogate/save",
                           {"operator": "boss3", "surrogate": "agent3", "processName": "leave",
                            "enabled": "1"})
    assert (await facade._ext.find_surrogate_by_id(int(r3["data"]["id"]))).enabled == 1
    r4 = await facade.flow("processSurrogate/save",
                           {"operator": "boss4", "surrogate": "agent4", "processName": "leave",
                            "enabled": ""})
    s4 = await facade._ext.find_surrogate_by_id(int(r4["data"]["id"]))
    assert s4.enabled == 0, f'空串 enabled 属脏值，不得当启用（实测落 {s4.enabled}）'
    assert await facade._ext.get_surrogate("boss4", "leave") is None, "空串委托不生效"
    # 布尔入参按 true→1 / false→0（跨栈同形，不得抛错）
    for flag, want in ((True, 1), (False, 0)):
        rb = await facade.flow("processSurrogate/save",
                               {"operator": "bossb", "surrogate": "agentb", "processName": "leave",
                                "enabled": flag})
        assert rb["code"] == 0, rb
        assert (await facade._ext.find_surrogate_by_id(int(rb["data"]["id"]))).enabled == want,             f"布尔 {flag!r} 应落 {want}"


# ═══ 批次 D 收尾 · 06-facade §4.5 条款 1.1 / 条款 1 覆盖范围 / 条款 1.4 判别力 ═══
# 用例构造姿势对齐 Go 栈 engine/surrogate_test.go（同口径同形状）：
# 自建流程 JSON（可控的 name 形态）+ 引擎直用 + **断言一律落在读回的持久参与者行上**。

_MISSING = object()


def _flow_json(specs, name=_MISSING) -> str:
    """线性流程 start → specs… → end（节点 id 即 taskName）。
    spec 支持两种写法：``(id, assignee)`` 普通任务 / ``(id, assignee, countersignType)`` 会签。
    ``name`` 原样写入 JSON（因此 "   " 纯空白、" padded " 首尾空白这些条款 1.1 的形态都能精确构造）；
    不传 = **不带 name 键**；传 None = ``"name": null``。"""
    nodes = [{'id': 'start', 'type': 'snaker:start', 'properties': {}, 'text': {'value': '开始'}}]
    edges = []
    prev = 'start'
    for spec in specs:
        tid, assignee = spec[0], spec[1]
        cs = spec[2] if len(spec) > 2 else ""
        props = {"assignee": assignee, "taskType": 0, "performType": 0}
        if cs:
            props = {"assignee": assignee, "taskType": 0, "performType": "1", "countersignType": cs}
        nodes.append({'id': tid, 'type': 'snaker:task', 'properties': props, 'text': {'value': tid}})
        edges.append({'id': f"e_{prev}_{tid}", 'sourceNodeId': prev, 'targetNodeId': tid,
                      'properties': {}})
        prev = tid
    nodes.append({'id': 'end', 'type': 'snaker:end', 'properties': {}, 'text': {'value': '结束'}})
    edges.append({'id': f"e_{prev}_end", 'sourceNodeId': prev, 'targetNodeId': 'end',
                  'properties': {}})
    raw = {"displayName": "委托测试", "type": "approval", "nodes": nodes, "edges": edges}
    if name is not _MISSING:
        raw = {"name": name, **raw}          # 原样写入（含空白串 / null / 键缺失三态）
    return json.dumps(raw, ensure_ascii=False)


def _seed_define(repo: MemoryRepository, define_name: str, content: str) -> int:
    """直接落定义行（**绕过门面 deploy 的 def.setName(model.name) 不变量**）——
    条款 1.1 的诱饵/回落形态只有在"define.name ≠ 模型 name"时才造得出来，
    与内置版导入链路（定义行自带 name）同形。"""
    d = ProcessDefine(name=define_name, displayName="委托测试", type="test", state=1, content=content)
    repo.add_define(d)
    return d.id


def _surr_harness(define_name: str, content: str, ext=None):
    """引擎直用 + 接入扩展仓储（零配置默认生效）"""
    eng, repo = setup()
    ext = ext if ext is not None else MemoryExtRepository()
    eng.attach_ext_repository(ext)
    return eng, repo, ext, _seed_define(repo, define_name, content)


async def _put_surr(ext, operator: str, agent: str, pname: str, enabled: int = 1):
    now = datetime.now()
    s = ProcessSurrogate(operator=operator, surrogate=agent, processName=pname,
                         startTime=now - timedelta(hours=1), endTime=now + timedelta(hours=1),
                         enabled=enabled)
    await ext.save_surrogate(s)
    return s


async def _doing_actors(repo, inst_id: int, node: str, want_tasks: int = 1) -> list[str]:
    """读回某节点进行中任务的**持久参与者行**（不看内存对象、不看待办列表空不空）"""
    doing = [t for t in await repo.find_doing_tasks(inst_id) if t.taskName == node]
    assert len(doing) == want_tasks, f"节点 {node} 进行中任务数 = {len(doing)}, want {want_tasks}"
    return await repo.find_task_actors(doing[0].id)


# ─── 条款 1.1：processName 取值口径（trim 判空 + 回落定义行 + 传出去必 trim）────

@pytest.mark.asyncio
async def test_surrogate_process_name_prefers_model_name():
    """诱饵行钉住"取的到底是哪一头"：模型 name 与 define.name **不一致**时两个名字各配一条
    委托、指向不同代理人——取错那头必然选错人（断言落在读回的 actor 行上）。"""
    content = _flow_json([("t1", "nm-zhang")], name="model-surr116")
    eng, repo, ext, def_id = _surr_harness("define-surr116", content)
    await _put_surr(ext, "nm-zhang", "from-model-agent", "model-surr116")
    await _put_surr(ext, "nm-zhang", "from-define-agent", "define-surr116")

    inst = await eng.start_process_instance_by_id(def_id, "boss1")
    actors = await _doing_actors(repo, inst.id, "t1")
    assert actors == ["nm-zhang", "from-model-agent"], \
        f"条款 1.1 必须取流程模型 name（内置版迁移基线 processModel.getName()）：读回 {actors}"


@pytest.mark.asyncio
@pytest.mark.parametrize("what,model_name,define_name,agent", [
    ("模型 name 纯空白", "   ", "blankdef-surr116", "blank-agent"),
    ("模型 name 制表符+空格", "\t ", "tabdef-surr116", "tab-agent"),
    ("模型 name 空串", "", "emptydef-surr116", "empty-agent"),
    ("模型不带 name 键", _MISSING, "nodef-surr116", "nokey-agent"),
    ("模型 name 为 null", None, "nulldef-surr116", "null-agent"),
    # 回落值本身也 trim：define.name 带首尾空白时台账存的是干净名，不 trim 就查不到
    ("回落值带首尾空白（define.name 也 trim）", "  ", "  spaced-def-surr116  ", "spaced-def-agent"),
])
async def test_surrogate_process_name_falls_back_to_define_name(what, model_name, define_name, agent):
    """条款 1.1「未带」= 键缺失 / null / 空串 / **仅空白** → 全部回落 wf_process_define.name 并命中。
    这正是本轮修的跨栈分叉：改前 ``flow.name or def_.name`` 把 "   " 当假值以外的有效名传给
    查询 ⇒ 该流程自己配的委托一条也查不到（只剩全流程兜底行能命中），用户视角＝委托静默失效。"""
    specs = [("t1", "fb-zhang")]
    content = _flow_json(specs, name=model_name)
    eng, repo, ext, def_id = _surr_harness(define_name, content)
    # 台账里存的是 **trim 后**的定义行 name（不 trim 就查不到 ⇒ 断的正是回落值也要 trim）
    await _put_surr(ext, "fb-zhang", agent, define_name.strip())
    inst = await eng.start_process_instance_by_id(def_id, "boss1")
    actors = await _doing_actors(repo, inst.id, "t1")
    assert actors == ["fb-zhang", agent], \
        f"条款 1.1「{what}」应回落 wf_process_define.name={define_name.strip()!r} 并命中，读回 {actors}"


@pytest.mark.asyncio
async def test_surrogate_process_name_is_trimmed_before_query():
    """模型 name 带首尾空白 → **传给委托查询的必须是 trim 后的值**（``" 名 "`` 与 ``"名"``
    必须命中同一条）。在假扩展仓储里捕获真实入参，防止"引擎内部 trim 了又怎样"的口头断言。"""
    class _CaptureExt(MemoryExtRepository):
        def __init__(self):
            super().__init__()
            self.queries: list[tuple[str, str]] = []

        async def get_surrogate(self, operator, process_name, at=None):
            self.queries.append((operator, process_name))
            return await super().get_surrogate(operator, process_name, at)

    cap = _CaptureExt()
    content = _flow_json([("t1", "pd-zhang")], name="  padded-surr116  ")
    eng, repo, _, def_id = _surr_harness("paddeddef-surr116", content, ext=cap)
    await _put_surr(cap, "pd-zhang", "pd-agent", "padded-surr116")   # 台账存干净名

    inst = await eng.start_process_instance_by_id(def_id, "boss1")
    actors = await _doing_actors(repo, inst.id, "t1")
    assert actors == ["pd-zhang", "pd-agent"], \
        f"模型 name 首尾空白须 trim 后再查委托（台账存的是 padded-surr116），读回 {actors}"
    assert cap.queries, "未捕获到任何 get_surrogate 调用，用例空转"
    bad = [(i + 1, q) for i, q in enumerate(cap.queries) if q[1] != "padded-surr116"]
    assert not bad, f"传给委托查询的流程名必须是 trim 后的值，实测未 trim 入参：{bad}"


@pytest.mark.asyncio
async def test_surrogate_define_name_fallback_reads_define_once_not_per_task():
    """条款 1.1 尾注：回落路径要读定义行（部分栈含 content BLOB），**逐次 execution 解析一次后
    复用，不要逐任务解析**。本栈起点（start / _prepare_execute_task）已把 def_.name 记进缓存 ⇒
    fork 出两个任务节点时读定义行总次数恒为 **2**：① 发起取 content、② 拦截器解析（issue 34 的
    defineId 缓存）。回落贡献 **0 次**；若回落不缓存、逐任务解析，实测会是 4 次。"""
    class _CountRepo(MemoryRepository):
        def __init__(self):
            super().__init__()
            self.define_reads = 0

        async def find_define_by_id(self, id):
            self.define_reads += 1
            return await super().find_define_by_id(id)

    content = json.dumps({
        "name": "   ", "displayName": "委托测试", "type": "approval",
        "nodes": [
            {"id": "start", "type": "snaker:start", "properties": {}, "text": {"value": "开始"}},
            {"id": "fork", "type": "snaker:fork", "properties": {}, "text": {"value": "并行"}},
            {"id": "ta", "type": "snaker:task", "properties": {"assignee": "ck-zhang", "taskType": 0,
                                                              "performType": 0}, "text": {"value": "ta"}},
            {"id": "tb", "type": "snaker:task", "properties": {"assignee": "ck-wang", "taskType": 0,
                                                              "performType": 0}, "text": {"value": "tb"}},
            {"id": "join", "type": "snaker:join", "properties": {}, "text": {"value": "汇聚"}},
            {"id": "end", "type": "snaker:end", "properties": {}, "text": {"value": "结束"}}],
        "edges": [
            {"id": "e0", "sourceNodeId": "start", "targetNodeId": "fork", "properties": {}},
            {"id": "e1", "sourceNodeId": "fork", "targetNodeId": "ta", "properties": {}},
            {"id": "e2", "sourceNodeId": "fork", "targetNodeId": "tb", "properties": {}},
            {"id": "e3", "sourceNodeId": "ta", "targetNodeId": "join", "properties": {}},
            {"id": "e4", "sourceNodeId": "tb", "targetNodeId": "join", "properties": {}},
            {"id": "e5", "sourceNodeId": "join", "targetNodeId": "end", "properties": {}}]},
        ensure_ascii=False)
    repo = _CountRepo()
    eng = EngineImpl(repo, _TestUserProv(), _TestIDGen(), _TestExprEval())
    ext = MemoryExtRepository()
    eng.attach_ext_repository(ext)
    def_id = _seed_define(repo, "cachedef-surr116", content)
    await _put_surr(ext, "ck-zhang", "ck-agent-a", "cachedef-surr116")
    await _put_surr(ext, "ck-wang", "ck-agent-b", "cachedef-surr116")

    inst = await eng.start_process_instance_by_id(def_id, "boss1")
    assert await _doing_actors(repo, inst.id, "ta") == ["ck-zhang", "ck-agent-a"], "分支 A 回落命中"
    assert await _doing_actors(repo, inst.id, "tb") == ["ck-wang", "ck-agent-b"], "分支 B 回落命中"
    assert repo.define_reads == 2, \
        (f"回落读定义行必须缓存复用、不得逐任务解析：find_define_by_id 次数 = {repo.define_reads}, "
         f"want 2（① 发起取 content ② 拦截器解析；回落贡献 0 次，逐任务解析会是 4 次）")


# ─── 条款 1「覆盖范围」：每条建任务路径各留一条独立用例 ──────────────────────────
#
# 跳转(JUMP) / 回退(ROLLBACK) / 串行会签的每一步推进 三条路径各一条，**专属流程名 +
# 专属参与者 + 专属代理人**（多条用例绝不共用代理人，否则某路径失能时看不出谁红），
# 并在断言前先做"起点自证"（委托只配在本路径新建任务的参与者身上 ⇒ 代理人只可能来自本路径）。
# 挂点归属与"单路径注掉"实测结论（本轮逐条注一遍跑全量，恢复后 md5 核对逐字节回到改前）：
#   · 串行会签推进 = execute_process_task 的 SEQUENTIAL 分支调用点（**独占**）
#       → 只注它：1 failed = test_surrogate_applies_on_sequential_countersign_advance
#   · ROLLBACK     = _create_task_with_actors 的四个调用点（只被 ROLLBACK 用到，**独占**）
#       → 只注它们：1 failed = test_surrogate_applies_on_rollback_path
#   · JUMP         = _execute_node → _create_task，与"发起 / 办理推进 / 跳首节点"**共用同一挂点**：
#       → 注掉共用挂点 = 14 failed（发起 + 条款 1.1 全家 + JUMP + 回退的起点自证），做不到"只红自己"
#       → 故 JUMP 的单路径失能用**路径内注入**验证（在 jump 分支把流程名换成不存在的名字）：
#         1 failed = test_surrogate_applies_on_jump_path —— 该用例确实在钉这条路径

@pytest.mark.asyncio
async def test_surrogate_applies_on_jump_path():
    """路径 1/3 跳转 JUMP（execute_and_jump_task 带 target）：委托只配在**跳转目标节点**的
    参与者身上 ⇒ 发起产生的 j1 拿不到代理人（起点自证），j2 里的代理人只能由跳转路径写入。"""
    content = _flow_json([("j1", "jmp-zhang"), ("j2", "jmp-wang")], name="surrjump116")
    eng, repo, ext, def_id = _surr_harness("surrjump116", content)
    await _put_surr(ext, "jmp-wang", "jmp-agent", "surrjump116")

    inst = await eng.start_process_instance_by_id(def_id, "boss1")
    assert await _doing_actors(repo, inst.id, "j1") == ["jmp-zhang"], \
        "起点自证：发起产生的 j1 不该出现代理人（jmp-zhang 无委托）"
    j1 = [t for t in await repo.find_doing_tasks(inst.id) if t.taskName == "j1"][0]
    await eng.execute_and_jump_task(j1.id, "jmp-zhang", {"comment": "跳转"}, "j2")
    assert await _doing_actors(repo, inst.id, "j2") == ["jmp-wang", "jmp-agent"], \
        "条款 1「跳转(JUMP)」：跳转新建的任务未并入代理人（期望 [jmp-wang jmp-agent]）"


@pytest.mark.asyncio
async def test_surrogate_applies_on_rollback_path():
    """路径 2/3 回退 ROLLBACK（execute_and_jump_task 空 target）：issues/121 P2 血缘版——复活 b1
    那条历史行，参与者＝该行办结人 rbk-zhang（不是执行回退的 rbk-wang）。台账延后到 b1 建单之后
    再配 ⇒ 起点自证仍然成立；诱饵配在 rbk-wang 身上 ⇒ 新行里出现 rbk-decoy 就说明用错了人。"""
    content = _flow_json([("b1", "rbk-zhang"), ("b2", "rbk-wang")], name="surrback116")
    eng, repo, ext, def_id = _surr_harness("surrback116", content)

    inst = await eng.start_process_instance_by_id(def_id, "boss1")
    assert await _doing_actors(repo, inst.id, "b1") == ["rbk-zhang"],         "起点自证：台账还没配，发起产生的 b1 不该有任何代理人"
    b1 = [t for t in await repo.find_doing_tasks(inst.id) if t.taskName == "b1"][0]
    await eng.execute_process_task(b1.id, "rbk-zhang")
    await _put_surr(ext, "rbk-zhang", "rbk-agent", "surrback116")    # 该行办结人的委托
    await _put_surr(ext, "rbk-wang", "rbk-decoy", "surrback116")     # 诱饵：执行回退的人
    b2 = [t for t in await repo.find_doing_tasks(inst.id) if t.taskName == "b2"][0]
    await eng.execute_and_jump_task(b2.id, "rbk-wang", None, "")
    # 原 b1 已 DONE，b1 上唯一的进行中任务就是复活出来的那一条
    assert await _doing_actors(repo, inst.id, "b1") == ["rbk-zhang", "rbk-agent"],         "条款 1「回退(ROLLBACK)」：复活行应＝该行办结人 + 其代理人，且不得带执行回退人的代理人"


@pytest.mark.asyncio
async def test_surrogate_applies_on_sequential_countersign_advance():
    """路径 3/3 串行会签的每一步推进（execute_process_task 的 SEQUENTIAL 分支，引擎侧独立调用点）：
    只给**第二步**成员配委托 ⇒ 第一步任务拿不到代理人（起点自证）。
    顺带钉条款 1.3：代理人只进当一步任务，不得扩 operatorList 投票名册、不得改票数。"""
    content = _flow_json([("cs", "seq-zhang,seq-wang", "SEQUENTIAL")], name="surrseq116")
    eng, repo, ext, def_id = _surr_harness("surrseq116", content)
    await _put_surr(ext, "seq-wang", "seq-agent", "surrseq116")

    inst = await eng.start_process_instance_by_id(def_id, "boss1")
    assert await _doing_actors(repo, inst.id, "cs") == ["seq-zhang"], \
        "起点自证：第一步任务不该出现代理人（seq-zhang 无委托）"
    step1 = [t for t in await repo.find_doing_tasks(inst.id) if t.taskName == "cs"][0]
    await eng.execute_process_task(step1.id, "seq-zhang")
    second = [t for t in await repo.find_doing_tasks(inst.id) if t.taskName == "cs"]
    assert len(second) == 1, f"串行会签推进后应恰好一条进行中任务: {len(second)}"
    assert str(second[0].variables.get("loopCounter_cs")) == "1", \
        f"自证：断言对象必须是串行会签第 2 步，实读 loopCounter_cs = {second[0].variables.get('loopCounter_cs')}"
    # 落库读回（不是内存对象）
    assert await repo.find_task_actors(second[0].id) == ["seq-wang", "seq-agent"], \
        "条款 1「串行会签的每一步推进」：推进出的下一步任务未并入代理人（期望 [seq-wang seq-agent]）"
    # 条款 1.3：投票名册与票数不得因代理人改变
    assert second[0].variables.get("operatorList_cs") == ["seq-zhang", "seq-wang"], \
        f"条款 1.3：代理人不得进投票名册: {second[0].variables.get('operatorList_cs')}"
    assert second[0].variables.get("nrOfInstances_cs") == 2, \
        f"条款 1.3：票数不得因代理人改变: {second[0].variables.get('nrOfInstances_cs')}"


# ─── 条款 1.4 判别力：打乱 id 序的夹具，内存仓侧（SQL 仓侧见 jdbc_test ⑯）───────

@pytest.mark.asyncio
async def test_surrogate_query_shuffled_id_parity_memory():
    """四判据 + 条款 1.4「多条命中取 id 最大」在**内存仓**侧对拍：数据集与期望表来自
    tests/surrparity.py（与真机 SQL 仓 ⑯ 同一份，期望值只写一处）。
    此前本栈夹具的 id 按插入序单调递增 ⇒ "取遍历首条/末条"的错实现也会绿（假绿形状）。"""
    try:
        from tests import surrparity        # 以仓根为 sys.path 跑（pytest 默认）
    except ImportError:                     # pragma: no cover
        import surrparity                   # 从 tests/ 目录直跑

    ext = MemoryExtRepository()
    now = datetime.now().replace(microsecond=(datetime.now().microsecond // 1000) * 1000)  # 与 SQL DATETIME(3) 同精度
    failures: list[str] = []

    def report(desc: str, ok: bool, detail: str = "") -> bool:
        if not ok:
            failures.append(f"{desc} ({detail})" if detail else desc)
        return ok

    await surrparity.run_parity(ext, now, surrparity.MEM_BASE_ID, report)
    assert not failures, "内存仓与共用期望表不一致：\n  " + "\n  ".join(failures)


# ─── issues/123 · 条款 1.4「取最新一条再裁决」：运行期四形 veto（形状照 Java 6feeae6）────
#
# Java 参考实现的 SurrogateAutoApplyTest 新增 4 格在这里同形复刻：
# **同一作用域内**先配一条「窗内 + enabled=1」的有效旧记录，再配一条更"新"的不生效记录，
# 断言代理人**不并入**、且旧的那条不得被复活。
# 旧形状（先按判据滤掉不生效的、再从剩下的取最新）在这四格上必然把旧记录捞回来 ⇒ 红；
# 变异对照实测见本轮收口记录（改回旧形状：恰好这 4 格红，其余全绿）。

async def _actors_after_newest_row(op: str, **newest) -> list[str]:
    """夹具：先插窗内有效的**旧**记录，再插一条由 ``newest`` 描述的**新**记录，
    发起一单后读回 t1 的**持久参与者行**（不看内存对象、不看待办列表空不空）。"""
    pname = f"surr123-{op}"
    content = _flow_json([("t1", op)], name=pname)
    eng, repo, ext, def_id = _surr_harness(pname, content)
    now = datetime.now()
    await ext.save_surrogate(ProcessSurrogate(
        operator=op, surrogate=f"{op}-older-agent", processName=pname,   # 旧：窗内 + enabled=1
        startTime=now - timedelta(days=1), endTime=now + timedelta(days=1), enabled=1))
    await ext.save_surrogate(ProcessSurrogate(                            # 新：由它裁决
        operator=op, surrogate=newest.get("surrogate", f"{op}-new-agent"), processName=pname,
        startTime=newest.get("start", now - timedelta(days=1)),
        endTime=newest.get("end", now + timedelta(days=1)),
        enabled=newest.get("enabled", 1)))
    # 种子自证：两条台账确实都在（否则"不并入"会因为"根本没数据"空转通过，issues/113 教训）
    assert len(ext._surrogates) == 2, f"夹具应有 2 条委托台账，实测 {len(ext._surrogates)}"
    inst = await eng.start_process_instance_by_id(def_id, "boss1")
    return await _doing_actors(repo, inst.id, "t1")


async def _assert_newest_invalid(op: str, why: str, **newest) -> None:
    actors = await _actors_after_newest_row(op, **newest)
    assert actors == [op], \
        f"{why} ⇒ 最新一条不生效时不得并入代理人，更不得复活更旧的那条有效委托：读回 {actors}"


@pytest.mark.asyncio
async def test_surrogate_newest_out_of_window_beats_older_effective_one():
    """窗外（未到窗）：最新一条把窗口推到未来 ⇒ 旧的那条窗内委托不得再替用户做主。"""
    now = datetime.now()
    await _assert_newest_invalid("v123-future", "最新一条窗外",
                                 start=now + timedelta(days=1), end=now + timedelta(days=2))


@pytest.mark.asyncio
async def test_surrogate_newest_disabled_beats_older_effective_one():
    """enabled=0：用户把委托停用后，历史上那条窗内委托不得继续生效。"""
    await _assert_newest_invalid("v123-off", "最新一条 enabled=0", enabled=0)


@pytest.mark.asyncio
async def test_surrogate_newest_dirty_enabled_beats_older_effective_one():
    """enabled 脏值 2：契约只认 1（06 §4.5 条款 5 读侧白名单式判定），脏值同样判否。"""
    await _assert_newest_invalid("v123-dirty", "最新一条 enabled 脏值 2（契约：只认 1）", enabled=2)


@pytest.mark.asyncio
async def test_surrogate_newest_self_delegation_beats_older_effective_one():
    """自委托：自己委托给自己不新增、不重复，也不得让旧的有效委托复活。"""
    await _assert_newest_invalid("v123-self", "最新一条是自己委托给自己", surrogate="v123-self")


@pytest.mark.asyncio
async def test_surrogate_newest_effective_applies_and_keeps_original():
    """正向对照（同一夹具）：最新一条 = 窗内 + enabled=1 ⇒ 代理人并入、原人保留；
    且并入的是**最新那条**的代理人（更旧那条不再参与择优）。"""
    op = "v123-valid"
    actors = await _actors_after_newest_row(op)
    assert actors == [op, f"{op}-new-agent"], \
        f"最新一条有效时须并入其代理人并保留授权人（不取更旧那条）：读回 {actors}"




# ─── Test 121-P1: 建单不变量 task_parent_id 与行级 isFirstTaskNode ────────────────

@pytest.mark.asyncio
async def test_i121_p1_lineage_written_on_create():
    """夹具是 apply→task1→task2→task3 四级链。两步流里"上一节点"与"首任务节点"同格，
    断言恒真、抓不到缺陷，所以必须用 ≥3 个任务节点的流程。"""
    eng, repo = setup()
    df = load_flow(repo, "02-multi-task.json")
    inst = await eng.start_process_instance_by_id(df.id, "applicant")

    apply = (await repo.find_doing_tasks(inst.id))[0]
    assert apply.taskName == "apply"
    assert apply.parentTaskId == 0, "发起那条 execution 没有当前任务 ⇒ parent 落 0（不是 None）"
    assert apply.variables["isFirstTaskNode"] is True, "首任务节点行应落 isFirstTaskNode=True"
    await repo.add_task_actor(apply.id, ["applicant"])
    await eng.execute_process_task(apply.id, "applicant")

    prev = apply
    for name, who in (("task1", "leader"), ("task2", "manager"), ("task3", "boss")):
        t = (await repo.find_doing_tasks(inst.id))[0]
        assert t.taskName == name, f"期望 {name}，实得 {t.taskName}"
        assert t.parentTaskId == prev.id, f"{name}.parent 应为刚办结的 {prev.taskName}.id"
        assert t.variables["isFirstTaskNode"] is False, "非首节点必须 False（否则'parent=0 当首节点'这类假判据蒙得过）"
        await repo.add_task_actor(t.id, [who])
        await eng.execute_process_task(t.id, who)
        prev = t

    # 本案真正要的那格：血缘版回退读的是已办结的历史行，标记必须随行存活
    his = await repo.find_task_by_id(apply.id)
    doing_ids = {t.id for t in await repo.find_doing_tasks(inst.id)}
    assert his.id not in doing_ids, "apply 应已办结"
    assert his.variables["isFirstTaskNode"] is True, "历史行标记必须还在（现算版在历史行上恒 False）"
    assert his.parentTaskId == 0, "历史行的血缘指针不应被后续路径覆写"

    # 门面出口：行上值优先 → 历史行也报 True；缺键（存量行）才回退现算 → False
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    r = await facade.flow("processInstance/detail", {"id": inst.id})
    assert r["code"] == 0, r
    ext_apply = [x for x in r["data"]["tasks"] if x["taskName"] == "apply"][0]["ext"]
    assert ext_apply["isFirstTaskNode"] is True, "已办结的 apply 行出口应给行上值 true"

    his.variables.pop("isFirstTaskNode")
    await repo.update_task(his)
    r2 = await facade.flow("processInstance/detail", {"id": inst.id})
    assert r2["code"] == 0, r2
    ext2 = [x for x in r2["data"]["tasks"] if x["taskName"] == "apply"][0]["ext"]
    assert ext2["isFirstTaskNode"] is False, "缺键的存量历史行回退现算（仅进行中口径）⇒ False，且不得报错"

@pytest.mark.asyncio
async def test_i121_p2_rollback_lineage_and_negatives():
    """issues/121 P2：① 无血缘（parent 为 0，以及 P1 之前老行的 None 形状）⇒ 20010007；
    ② 血缘前驱跨不过 fork（boot2 canRejected 遇 fork/join/start 跳过该入边不再深入）⇒ 20010008；
    （正向落点/参与者由上面两处旧语义用例改血缘版后一并钉住。）"""
    eng, repo = setup()
    df = load_flow(repo, "02-multi-task.json")
    inst = await eng.start_process_instance_by_id(df.id, "applicant")
    apply = (await repo.find_doing_tasks(inst.id))[0]
    assert apply.taskName == "apply"
    assert apply.parentTaskId == 0, "前置条件：发起那条 parent 应为 0"
    try:
        await eng.execute_and_jump_task(apply.id, "applicant", None, "")
        assert False, "无血缘必须报错，不得静默不建单"
    except ValueError as e:
        assert "上一步任务ID为空，无法驳回至上一步处理" in str(e) and "2001000" not in str(e), f"msg 应为固定文案且不含内部码：{e}"

    # 老行形状：parent=None
    old = apply
    old.parentTaskId = None
    await repo.update_task(old)
    try:
        await eng.execute_and_jump_task(apply.id, "applicant", None, "")
        assert False, "parent=None 必须报 20010007"
    except ValueError as e:
        assert "上一步任务ID为空，无法驳回至上一步处理" in str(e) and "2001000" not in str(e), f"实得：{e}"

    # ② fork 分支行退到 fork 之前的节点
    eng2, repo2 = setup()
    df2 = load_flow(repo2, "04-fork-join.json")
    inst2 = await eng2.start_process_instance_by_id(df2.id, "applicant")
    apply2 = (await repo2.find_doing_tasks(inst2.id))[0]
    await repo2.add_task_actor(apply2.id, ["applicant"])
    await eng2.execute_process_task(apply2.id, "applicant")
    branch = [t for t in await repo2.find_doing_tasks(inst2.id) if t.taskName == "taskA"][0]
    assert branch.parentTaskId, "前置条件：分支行的 parent 应已由 P1 写入"
    assert branch.actorIds, "前置条件：分支行应有参与者，否则会被权限校验先挡下"
    try:
        await eng2.execute_and_jump_task(branch.id, branch.actorIds[0], None, "")
        assert False, "血缘前驱跨不过 fork 时必须报 20010008"
    except ValueError as e:
        assert "无法驳回至上一步处理，请确认上一步骤并非fork、join、suprocess以及会签任务" in str(e) and "2001000" not in str(e), f"实得：{e}"


# ─── issues/126 案 A · 任务行 expire_time 由**建单路径**按节点到期表达式真算 ───────
#
# 基准＝boot2 内置版的三处写（ProcessTaskServiceImpl:213 普通建单 / :386 回退新建 /
# :524 会签建单——:524 那处**串行推进也回调它**，故会签首位与推进出的下一位是两次写）
# + jeeflow 多出的会签并行分支，共**五处**同一把尺子（卡面 §1.8 更正：五处不是四处）；
# Java 参考实现是 jeeflow-java d9e9397 的 ProcessInstance.applyExpireTime + FlowUtil.processTime，
# 第五处由 cb541d4 的 applyNodeExpireTime 补上。
# 本栈原形状是**一句都不赋**（engine 建单只写 createTime，expire_time 恒 NULL）
# ⇒ 配了到期表达式的节点在逾期统计里永不逾期，且跨栈各给一个答案（本案病灶）。
#
# ⚠️ 判据是**同一行内 expire − create ≈ 表达式偏移**，不是"这一列非空"——
#    只判非空就会被 now() 占位写法蒙过（建单即逾期，差值≈0），那正是病灶的形状。
#
# ⚠️ 写点④（回退/跳转新建）的表达式来源是**被回退掉的那个节点**（boot2 rejectTask 的 current，
#    本栈 task.taskName），不是复活行落地的 prev；所以回退那几格的夹具给成**两个节点两份表达式**
#    （落地 b1＝1d / 当前 b2＝3h）——只配一个节点时取错节点也照样绿，判不出来（09-28 二轮更正）。

def _expire_flow(specs, name: str = "expire126") -> str:
    """线性流程 start → specs… → end。
    spec = (节点 id, assignee, 到期表达式[, 会签类型])；
    到期表达式传 ``_MISSING`` = **不写 expireTime 键**（"节点没配"的第一档），
    传 ``""`` = 写了空串（第二档），其余原样进 properties（设计器 JSON 同位置）。"""
    nodes = [{'id': 'start', 'type': 'snaker:start', 'properties': {}, 'text': {'value': '开始'}}]
    edges = []
    prev = 'start'
    for spec in specs:
        tid, assignee, expr = spec[0], spec[1], spec[2]
        cs = spec[3] if len(spec) > 3 else ""
        props = {"assignee": assignee, "taskType": 0, "performType": "1" if cs else 0}
        if cs:
            props["countersignType"] = cs
        if expr is not _MISSING:
            props["expireTime"] = expr
        nodes.append({'id': tid, 'type': 'snaker:task', 'properties': props, 'text': {'value': tid}})
        edges.append({'id': f"e_{prev}_{tid}", 'sourceNodeId': prev, 'targetNodeId': tid,
                      'properties': {}})
        prev = tid
    nodes.append({'id': 'end', 'type': 'snaker:end', 'properties': {}, 'text': {'value': '结束'}})
    edges.append({'id': f"e_{prev}_end", 'sourceNodeId': prev, 'targetNodeId': 'end',
                  'properties': {}})
    return json.dumps({"name": name, "displayName": "到期时间测试", "type": "approval",
                       "nodes": nodes, "edges": edges}, ensure_ascii=False)


def _expire_harness(content: str, define_name: str = "expire126", user_prov=None):
    """引擎直用 + 直接落定义行（到期表达式只在本组用例里出现，flows/ 副本一律不配）"""
    repo = MemoryRepository()
    eng = EngineImpl(repo, user_prov or _TestUserProv(), _TestIDGen(), _TestExprEval())
    return eng, repo, _seed_define(repo, define_name, content)


async def _row(repo, inst_id: int, node: str, want_tasks: int = 1):
    doing = [t for t in await repo.find_doing_tasks(inst_id) if t.taskName == node]
    assert len(doing) == want_tasks, f"节点 {node} 进行中任务数 = {len(doing)}, want {want_tasks}"
    return doing[0]


def _assert_expire_after_create(row, want_seconds: int, why: str) -> float:
    """同行差值判据（不拿 now 当基准）。带宽 [N−5s, N+60s] 同 Java 用例：
    Java 的 toLocalDateTime(Date) 落在**秒级**而行上 createTime 带毫秒 ⇒ 实测 7199.x s；
    本栈反过来（process_time 里的 now() 晚于建单那一刻的 now）⇒ 差值略大于 N。
    带宽只用来夹住量纲，**绝不用来放过 now() 占位**（那种写法差值≈0）。"""
    assert row.expireTime is not None, f"{why}：配了到期表达式的行必须带 expire_time"
    create, expire = to_datetime(row.createTime), to_datetime(row.expireTime)
    assert create is not None and expire is not None, \
        f"{why}：行上的时间列读不回（create={row.createTime!r} expire={row.expireTime!r}）"
    delta = (expire - create).total_seconds()
    assert want_seconds - 5 <= delta <= want_seconds + 60, \
        f"{why}：同行 expire − create = {delta}s，want ≈{want_seconds}s；差值≈0 就是 now() 占位（建单即逾期）"
    return delta


@pytest.mark.asyncio
async def test_i126_relative_expression_applies_at_creation():
    """正向①/写点①「普通建单」：节点配 "2h" ⇒ 到期时间＝建单那一刻 + 2 小时。
    四档后缀全喂一遍（s/m/h/d），d 档钉的是"日历加天"而不是乘 86400 秒。"""
    for expr, seconds in (("2h", 2 * 3600), ("90s", 90), ("30m", 1800), ("1d", 86400)):
        eng, repo, def_id = _expire_harness(_expire_flow([("approve", "leader", expr)]))
        inst = await eng.start_process_instance_by_id(def_id, "applicant")
        _assert_expire_after_create(await _row(repo, inst.id, "approve"), seconds,
                                    f'普通建单配 "{expr}"')


@pytest.mark.asyncio
async def test_i126_expression_naming_a_variable_takes_its_value():
    """正向②：表达式是个变量名 ⇒ 取**实例变量**里该变量的值当到期时间（process_time 档①）。
    这格同时钉住建单三处的变量源＝实例变量：写错成行上那份的话这里读不到 dueAt，
    差值判据会直接报"没带 expire_time"。"""
    eng, repo, def_id = _expire_harness(_expire_flow([("approve", "leader", "dueAt")]))
    inst = await eng.start_process_instance_by_id(def_id, "applicant",
                                                  {"dueAt": "2026-12-31 10:00:00"})
    row = await _row(repo, inst.id, "approve")
    assert to_datetime(row.expireTime) == datetime(2026, 12, 31, 10, 0, 0), \
        f"变量档取的是实例变量里那份值，实得 {row.expireTime!r}"


@pytest.mark.asyncio
async def test_i126_variable_tier_accepts_three_value_shapes():
    """档①的三种值形状（Java 的 Date / Long / String 三档）：datetime 对象、毫秒时间戳、
    契约格式文本必须给出**同一时刻**；本栈写时间用 naive 本地钟，与 datetime 同档。"""
    want = datetime(2026, 12, 31, 10, 0, 0)
    content = _expire_flow([("approve", "leader", "due")])
    for label, value in (("datetime 对象", want),
                         ("毫秒时间戳", int(want.timestamp() * 1000)),
                         ("契约格式文本", "2026-12-31 10:00:00")):
        eng, repo, def_id = _expire_harness(content)
        inst = await eng.start_process_instance_by_id(def_id, "applicant", {"due": value})
        got = to_datetime((await _row(repo, inst.id, "approve")).expireTime)
        assert got is not None and abs((got - want).total_seconds()) < 1, \
            f"{label} 档应解析成 {want}，实得 {got!r}"


@pytest.mark.asyncio
async def test_i126_variable_tier_beats_relative_tier():
    """易错点②：变量档**优先于**相对档 —— args 里真有个键叫 "2h" 时取的是变量值，
    不是 now+2h（把两档顺序写反的栈会在这格报红）。"""
    eng, repo, def_id = _expire_harness(_expire_flow([("approve", "leader", "2h")]))
    inst = await eng.start_process_instance_by_id(def_id, "applicant",
                                                  {"2h": "2030-01-01 00:00:00"})
    got = to_datetime((await _row(repo, inst.id, "approve")).expireTime)
    assert got == datetime(2030, 1, 1, 0, 0, 0), f"变量档应压过相对档，实得 {got!r}"


@pytest.mark.asyncio
async def test_i126_unknown_variable_value_type_falls_through():
    """易错点①「落穿」：变量存在但值类型不认识（list / float）⇒ **继续**走相对档，
    不是提前 return None（Java/C# 都是落穿；改成提前返回的栈这两格都红）。"""
    for expr, value, seconds in (("2h", ["not-a-time"], 2 * 3600),
                                 ("3h", 7200.5, 3 * 3600)):
        eng, repo, def_id = _expire_harness(_expire_flow([("approve", "leader", expr)]))
        inst = await eng.start_process_instance_by_id(def_id, "applicant", {expr: value})
        _assert_expire_after_create(await _row(repo, inst.id, "approve"), seconds,
                                    f"值类型 {type(value).__name__} 应落穿到相对档 {expr}")


@pytest.mark.asyncio
async def test_i126_unconfigured_node_keeps_column_null():
    """负向①：节点没配 ⇒ 这一列必须留 NULL，不许造默认值（含不许写 now()）。
    三档形状都要判：键缺失、空串、纯空白（空白串 Java 也不当"没配"，而是交给
    processTime 求值 ⇒ 解析不出 ⇒ 同样是 NULL，出口形状一致）。"""
    for label, expr in (("键缺失", _MISSING), ("空串", ""), ("纯空白", "   ")):
        eng, repo, def_id = _expire_harness(_expire_flow([("approve", "leader", expr)]))
        inst = await eng.start_process_instance_by_id(def_id, "applicant")
        row = await _row(repo, inst.id, "approve")
        assert row.expireTime is None, f"{label} 这一档不得被赋任何时间，实得 {row.expireTime!r}"


@pytest.mark.asyncio
async def test_i126_unparsable_expression_stays_null():
    """负向②：表达式解析不出来 ⇒ NULL，而不是退回 now()（那等于静默造一个"建单即逾期"的值）。
    逐档喂与 Java 同款拒收形状：非时间串、相对档前缀不是整数（"xh"）、只到日、ISO 带 T、
    复合相对串（"2h30m"）。"""
    for expr in ("not-a-time", "xh", "2026-12-31", "2026-12-31T10:00:00", "2h30m"):
        eng, repo, def_id = _expire_harness(_expire_flow([("approve", "leader", expr)]))
        inst = await eng.start_process_instance_by_id(def_id, "applicant", {"dueAt": "x"})
        row = await _row(repo, inst.id, "approve")
        assert row.expireTime is None, f"表达式 {expr!r} 解析不出必须 NULL，实得 {row.expireTime!r}"


# ─── issues/137 D · 相对档前缀须**非负**整数（owner 2026-10-01 拍"判非负"）───────────

@pytest.mark.asyncio
async def test_i137d_negative_relative_expression_stays_null():
    """建单路径判点，形状照 Java 基准 ``ExpireTimeOnCreateTest.negativeRelativeExpressionStaysNull``
    （jeeflow-java ``1649955``）：四档（``s``/``m``/``h``/``d``）各喂一个负数前缀 ⇒ 行照常建，
    但 ``expire_time`` 必须留 NULL —— 判负后走的是**既有落穿分支**（档 2 不匹配 → 档 3 解析不出
    → None），不是新造的第三条出口，更不是退化成 now()（issues/126 红线）。
    放行 ``-5h`` 得到的是一个**过去**的时刻 ⇒ 新建的行当场即逾期，比"没配到期时间"更难发现。
    ``d`` 档单独一格是必需的：本栈它是 naive 墙上钟加天数（对齐 Java ``Calendar.add(DAY_OF_MONTH)``），
    负数＝历日倒退，与 s/m/h 的秒级加法不同形，按档分开才拦得住"只改一档"的实现。
    末格 ``+2h`` 是**正向对照**（≈now+7200s）：没有它，上面四格会被"相对档整档返回 None"
    这种错误实现也判绿（恒真），也证明判负没顺手把加号一起裁掉。"""
    for i, expr in enumerate(("-5h", "-5d", "-30s", "-45m")):
        eng, repo, def_id = _expire_harness(_expire_flow([("approve", "leader", expr)]),
                                            f"expire137d_neg{i}")
        inst = await eng.start_process_instance_by_id(def_id, "applicant")
        row = await _row(repo, inst.id, "approve")
        assert row.expireTime is None, \
            f"负数相对档 {expr!r} 应落穿成 NULL，实得 {row.expireTime!r}（放行＝建单即逾期）"

    eng, repo, def_id = _expire_harness(_expire_flow([("approve", "leader", "+2h")]), "expire137d_plus")
    inst = await eng.start_process_instance_by_id(def_id, "applicant")
    _assert_expire_after_create(await _row(repo, inst.id, "approve"), 2 * 3600, '正向对照配 "+2h"')


def test_i137d_process_time_tier_matrix_around_negative_prefix():
    """求值器档位矩阵（直调 ``process_time``，不借建单路径），把"判负只发生在档 2 前缀解析**之后**"
    的覆盖面钉全：
    ① 四档负数前缀 ⇒ ``None``（判的是 `is None`，因此既不可能是异常、也不可能是 now 或回拨后的时刻）；
    ② 正向对照 ``+2h``/``+1d``/``2h``/``2d`` 照旧算得出 ≈now+N —— 这一组保证 ① 不是恒真，
       并钉住"只裁负**不裁加号**"：本栈 ``_INT_PREFIX`` 的 ``[+-]?`` 原样保留（各栈整数解析
       python ``[+-]?``、node ``[-+]?\\d+``、php ``[+-]?\\d{1,18}``、go ``Atoi`` 都收 '+'，
       把加号裁掉等于新造一处跨栈分叉）；
    ③ 变量档与绝对档不受影响：键名就叫 ``-5h`` 的变量照旧取变量值（档 1 优先于档 2，顺序没动），
       ``"2026-12-31 10:00:00"`` 照旧解析成该时刻；
    ④ 坏前缀行为不变（本来就是落穿）：``"xh"`` 前缀非整数、``"2.5h"`` 是小数 ⇒ 仍是 ``None``。"""
    from jeeflow.engine import process_time

    for expr in ("-5h", "-5d", "-30s", "-45m", "-1s", "-99999d"):
        assert process_time(expr, None) is None, f"负数相对档 {expr!r} 应落穿成 None"

    for expr, want in (("+2h", 2 * 3600), ("+1d", 86400), ("2h", 2 * 3600),
                       ("2d", 2 * 86400), ("+30s", 30), ("+45m", 2700)):
        got = process_time(expr, None)
        assert got is not None, f"正向对照 {expr!r} 不该被判负误伤"
        delta = (got - datetime.now()).total_seconds()
        assert want - 5 <= delta <= want + 5, f"{expr!r} 偏移 = {delta}s，want ≈{want}s"

    assert process_time("-5h", {"-5h": "2028-08-08 08:08:08"}) == datetime(2028, 8, 8, 8, 8, 8), \
        "变量档优先于相对档：命中同名变量时取变量值，不受判负影响"
    assert process_time("2026-12-31 10:00:00", None) == datetime(2026, 12, 31, 10, 0, 0), \
        "绝对档照旧解析成功"
    for expr in ("xh", "2.5h"):
        assert process_time(expr, None) is None, f"坏前缀 {expr!r} 的既有落穿行为被改动了"


# ─── issues/137 E · 相对档前缀**允许两端空白**（owner 2026-10-01 拍"统一 trim"）───────────
# 契约（jeeflow-doc spec/04 §「相对档前缀允许两端空白」）：各栈在**判整数之前**裁掉前缀的两端空白，
# 之后才走 137 D 的"非负"那一档。基准＝jeeflow-java `bf1f401`（`FlowUtil.parseIntOrNull` 里
# `Integer.parseInt(text.trim())`）。动机是各栈整数解析对空白的容忍度天然不同（go `Atoi` 前显式
# `TrimSpace`、rust `.trim()`、.NET `TryParse` 与本栈 `int()` 默认就收，java `parseInt` 偏偏不收）
# ⇒ 不裁就是"同一份流程定义在别家有到期时间、这一家没有"。
# 裁的边界只到**前缀**，三条分界逐栈一致：① `" 2h"`/`"2 h"`（空格在前缀区内、末位仍是单位符）照样算得出；
# ② `"2h "`（单位符后带空白）末位不是 s/m/h/d、认不出单位 ⇒ 按误配落穿 ⇒ None；③ `" 2.5h"` trim 后
# 仍是小数误配 ⇒ 仍落穿（trim ≠ 把"裁空白"做成"裁容错"）。变量档与绝对档的串本身**不 trim**。

@pytest.mark.asyncio
async def test_i137e_padded_relative_prefix_still_applies():
    """建单路径判点，形状照 Java 基准 ``ExpireTimeOnCreateTest.paddedRelativePrefixStillApplies``
    （jeeflow-java ``bf1f401``）：① 前带空格 / 数字与单位符之间带空格两形都**照样算得出**
    （同行差值≈2h，判据仍是差值不是"非空"，故 now() 占位照样拦得住）；② 单位符后面带空白 ⇒ 认不出
    单位 ⇒ 仍 NULL —— 这一格专门挡"把 trim 做成整串去空白"（``expr.strip()`` 后再判末位）那种顺手放宽；
    ③ ``" 2.5h"`` 裁完仍是小数、④ ``" -5h"`` 裁完判负照旧生效 ⇒ 两档都仍落穿；
    末段**正向对照**（不带空格的 ``2h`` / ``+2h``）保证①那两格不是恒真。"""
    # ① 空格落在前缀区内、末位仍是单位符 ⇒ 两形都算得出 now+2h
    for i, expr in enumerate((" 2h", "2 h")):
        eng, repo, def_id = _expire_harness(_expire_flow([("approve", "leader", expr)]),
                                            f"expire137e_pad{i}")
        inst = await eng.start_process_instance_by_id(def_id, "applicant")
        _assert_expire_after_create(await _row(repo, inst.id, "approve"), 2 * 3600,
                                    f'前缀带空白的相对档 "{expr}"')

    # ② 单位符后面还带空白 ⇒ 末位不是 s/m/h/d ⇒ 落穿绝对档 ⇒ None（整串去空白是另一件没立过法的事）
    for i, expr in enumerate(("2h ", "\t+2h ", "2h\t", " 2h  ")):
        eng, repo, def_id = _expire_harness(_expire_flow([("approve", "leader", expr)]),
                                            f"expire137e_tail{i}")
        inst = await eng.start_process_instance_by_id(def_id, "applicant")
        row = await _row(repo, inst.id, "approve")
        assert row.expireTime is None, \
            f"{expr!r} 末位是空白、认不出单位 ⇒ 应落穿成 NULL，实得 {row.expireTime!r}"

    # ③ 裁空白 ≠ 裁容错（小数/非整数前缀裁完还是误配）；④ 判负（137 D）在 trim 之后照旧生效
    for i, expr in enumerate((" 2.5h", " xh", " -5h", " -5d")):
        eng, repo, def_id = _expire_harness(_expire_flow([("approve", "leader", expr)]),
                                            f"expire137e_bad{i}")
        inst = await eng.start_process_instance_by_id(def_id, "applicant")
        row = await _row(repo, inst.id, "approve")
        assert row.expireTime is None, \
            f"{expr!r} 裁完仍是误配/负数 ⇒ 应落穿成 NULL，实得 {row.expireTime!r}"

    # 正向对照：不带空格的 2h / +2h 照旧算得出（上面①不是恒真，也证明裁空白没误伤加号档）
    for i, expr in enumerate(("2h", "+2h")):
        eng, repo, def_id = _expire_harness(_expire_flow([("approve", "leader", expr)]),
                                            f"expire137e_plain{i}")
        inst = await eng.start_process_instance_by_id(def_id, "applicant")
        _assert_expire_after_create(await _row(repo, inst.id, "approve"), 2 * 3600,
                                    f'正向对照（无空白）"{expr}"')

    # 变量档不 trim：带空白的表达式名去 args 里取键 ⇒ 取不到 ⇒ 按既有落穿路径 ⇒ None
    # （若实现顺手把整串 trim 成键名，这里就会"突然取到值"，本格当场红）
    eng, repo, def_id = _expire_harness(_expire_flow([("approve", "leader", " dueAt ")]),
                                        "expire137e_varkey")
    inst = await eng.start_process_instance_by_id(def_id, "applicant",
                                                  {"dueAt": "2026-12-31 10:00:00"})
    row = await _row(repo, inst.id, "approve")
    assert row.expireTime is None, \
        f"变量档的键名不 trim：\" dueAt \" 取不到 dueAt ⇒ 落穿 ⇒ NULL，实得 {row.expireTime!r}"


def test_i137e_process_time_tier_matrix_around_padded_prefix():
    """求值器档位矩阵（直调 ``process_time``，不借建单路径），把"trim 只发生在档 2 的前缀切片上"
    钉到每个档位：① 四档 s/m/h/d 的带空前缀都算得出（含制表符，``strip()`` 与 java ``trim()`` 同覆盖）；
    ② 单位符后带空白一律 None；③ 裁完仍是误配的一律 None；④ 负数裁完照旧被判负拦下。
    另钉两档**不 trim** 的边界：键名带空白 ⇒ 变量档取不到（对照：不带空白时取得到，保证不是恒真）；
    绝对档串带空白 ⇒ 仍 None（对照：不带空白时解析成功）。"""
    from jeeflow.engine import process_time

    # ① 前缀区内带空白（h/m/s 三档）：秒级带宽 ±5s
    for expr, want in ((" 2h", 2 * 3600), ("2 h", 2 * 3600), ("\t+3h", 3 * 3600),
                       (" 30s", 30), (" 45m", 2700)):
        got = process_time(expr, None)
        assert got is not None, f"前缀带空白的 {expr!r} 必须照样算得出"
        delta = (got - datetime.now()).total_seconds()
        assert want - 5 <= delta <= want + 5, f"{expr!r} 偏移 = {delta}s，want ≈{want}s"

    # ① 天档同款（走日历加天，跨夏令时是 23/25 小时 ⇒ 带宽 ±3600s，与本栈 126 组天档判据一致）
    for expr, want in ((" 1d", 86400), ("1 d", 86400)):
        got = process_time(expr, None)
        assert got is not None, f"天档带空白的 {expr!r} 必须照样算得出"
        delta = (got - datetime.now()).total_seconds()
        assert want - 3600 <= delta <= want + 3600, f"{expr!r} 偏移 = {delta}s，want ≈{want}s（日历加天）"

    # ② 单位符后面带空白 ⇒ 末位不是 s/m/h/d ⇒ 认不出单位 ⇒ 落穿（整串去空白是另一件事）
    for expr in ("2h ", "2h\t", "\t+2h ", " 2h  ", "30s ", "1d "):
        assert process_time(expr, None) is None, f"单位符后带空白的 {expr!r} 应落穿成 None"

    # ③ trim 之后仍是误配 ⇒ 仍落穿（trim 不是裁容错）
    for expr in (" 2.5h", " xh", " 3hh", "+ 2h"):
        assert process_time(expr, None) is None, f"裁完仍是误配的 {expr!r} 应落穿成 None"

    # ④ 判负（137 D）在 trim **之后**照常生效：加了裁空白不能把负号一并绕过去
    for expr in (" -5h", " -5d", "\t-30s", "- 5h"):
        assert process_time(expr, None) is None, f"负数带空白的 {expr!r} 应判负落穿成 None"

    # 正向对照（不带空白，保证上面四组不是恒真）
    for expr in ("2h", "+2h", "30s", "1d"):
        assert process_time(expr, None) is not None, f"无空白的 {expr!r} 照旧该算得出"

    # 变量档：串本身不 trim ⇒ 带空白的键名取不到值（对照：不带空白取得到）
    want_at = datetime(2028, 8, 8, 8, 8, 8)
    assert process_time(" dueAt ", {"dueAt": want_at}) is None, \
        "变量档不 trim：\" dueAt \" 不等于键 \"dueAt\" ⇒ 取不到值 ⇒ 按既有路径落穿"
    assert process_time("dueAt", {"dueAt": want_at}) == want_at, \
        "对照：不带空白的键名照旧取值（上一格不是恒真）"

    # 绝对档：串本身同样不 trim ⇒ 带空白的合法时间串仍解析不出（对照：不带空白解析成功）
    assert process_time(" 2026-12-31 10:00:00", None) is None, \
        "绝对档不 trim：前导空白的串仍按解析不出处理 ⇒ None"
    assert process_time("2026-12-31 10:00:00", None) == datetime(2026, 12, 31, 10, 0, 0), \
        "对照：绝对档不带空白照旧成功 ⇒ 第 3 档没被这次裁空白牵连"


@pytest.mark.asyncio
async def test_i126_parallel_countersign_applies_to_every_member():
    """写点③「并行会签全员」：Java 的并行循环是**每位成员**都算，不是只算首位。
    同一流程里没配的 apply 行同时充当对照——配了才有，不是一律赋。"""
    eng, repo, def_id = _expire_harness(_expire_flow(
        [("apply", "applicant", _MISSING), ("cs", "userA,userB,userC", "2h", "PARALLEL")]))
    inst = await eng.start_process_instance_by_id(def_id, "applicant")
    apply = await _row(repo, inst.id, "apply")
    assert apply.expireTime is None, "对照：未配的 apply 行必须 NULL"
    await eng.execute_process_task(apply.id, "applicant")
    members = [t for t in await repo.find_doing_tasks(inst.id) if t.taskName == "cs"]
    assert [t.actorIds for t in members] == [["userA"], ["userB"], ["userC"]], \
        "前置：并行会签应全员建行（行上 actorIds 即参与者）"
    for t in members:
        _assert_expire_after_create(t, 2 * 3600, f"并行会签成员 {t.actorIds}")


@pytest.mark.asyncio
async def test_i126_sequential_first_and_advanced_member_both_get_expire():
    """写点②「串行会签首位成员」＋写点⑤「推进出的下一位成员」：两行都按节点表达式算。
    判据＝首成员与第二成员各自**同一行内** expire − create ≈ 2h（带宽 [2h−5s, 2h+60s]）。

    ⚠️ 本格的期望是**被参考实现改过来的**（原来钉的是"推进档留 NULL"），不是为了让测试变绿：
       基准侧 boot2 的串行推进是**回调** createCountersignTask（ProcessTaskServiceImpl:485，
       内含 :524 那处到期写）⇒ 第二、三位成员同样带到期时间；java 同批把这一支补上第五处写点
       （commit cb541d4，绕过 createTask 直建 ⇒ 聚合根开 applyNodeExpireTime 上同一把尺子，
       用例形状同款：首成员与推进第二成员都带到期）。
       卡面 §1.8「写点是五处，不是四处」即此。留 NULL 才是跨栈分叉。"""
    eng, repo, def_id = _expire_harness(_expire_flow(
        [("apply", "applicant", _MISSING), ("cs", "userA,userB", "2h", "SEQUENTIAL")]))
    inst = await eng.start_process_instance_by_id(def_id, "applicant")
    apply = await _row(repo, inst.id, "apply")
    await eng.execute_process_task(apply.id, "applicant")
    first = await _row(repo, inst.id, "cs")          # 先断"行读到了"
    assert first.actorIds == ["userA"]
    _assert_expire_after_create(first, 2 * 3600, "串行会签首位成员")
    await eng.execute_process_task(first.id, "userA")
    second = await _row(repo, inst.id, "cs")         # 先断"推进出的那一行读到了"
    assert second.actorIds == ["userB"], "前置：串行会签应推进到下一位"
    _assert_expire_after_create(second, 2 * 3600, "串行会签推进出的第二成员")


@pytest.mark.asyncio
async def test_i126_sequential_without_expression_leaves_both_rows_null():
    """上一格的负向同夹具（去掉 expireTime）：首成员与推进出的第二成员**都**留空，
    即写点⑤不得为"没配的节点"造任何默认值。
    ⚠️ "行没读到"与"值为空"分开断：行由 :func:`_row` 断（数不对就报"进行中任务数 = 0"），
       值由 expireTime is None 断。合成一句的话推进本身坏掉这格也会恒真。"""
    eng, repo, def_id = _expire_harness(_expire_flow(
        [("apply", "applicant", _MISSING),
         ("cs", "userA,userB", _MISSING, "SEQUENTIAL")]))
    inst = await eng.start_process_instance_by_id(def_id, "applicant")
    apply = await _row(repo, inst.id, "apply")
    await eng.execute_process_task(apply.id, "applicant")
    first = await _row(repo, inst.id, "cs", want_tasks=1)
    assert first.actorIds == ["userA"], "前置：首位成员建行"
    assert first.expireTime is None, f"未配的节点不得造默认值，实得 {first.expireTime!r}"
    await eng.execute_process_task(first.id, "userA")
    second = await _row(repo, inst.id, "cs", want_tasks=1)
    assert second.actorIds == ["userB"], "前置：串行会签应推进到下一位"
    assert second.expireTime is None, f"推进档同理留空，实得 {second.expireTime!r}"


@pytest.mark.asyncio
async def test_i126_rollback_new_row_gets_expire_from_node_expression():
    """写点④「回退/跳转新建」：复活的那一行按**被回退掉的那个节点**（b2＝boot2 rejectTask 里的
    current）的表达式重算，**不是**复活行落地的 b1（＝prev；form 等数据类字段仍照 prev 走，
    boot2 就是这个形状）。基准逐字：ProcessTaskServiceImpl.rejectTask :363
    current = model.getNode(currentTask.getTaskName()) → :385 expireTime =
    ((TaskModel)current).getExpireTime() → :387 setExpireTime(processTime(expireTime, hisVariable))。
    两个节点配成**不同**的偏移（落地 1d / 当前 3h）就是这格的牙：错接成落地节点会算出 ≈1d，
    带宽 [3h−5s, 3h+60s] 当场报红。"""
    eng, repo, def_id = _expire_harness(
        _expire_flow([("b1", "rbk-zhang", "1d"), ("b2", "rbk-wang", "3h")], "expire126rbk"))
    inst = await eng.start_process_instance_by_id(def_id, "applicant")
    b1 = await _row(repo, inst.id, "b1")
    _assert_expire_after_create(b1, 24 * 3600, "对照：原始 b1 行按落地节点自己的表达式（写点①）")
    await eng.execute_process_task(b1.id, "rbk-zhang")
    b2 = await _row(repo, inst.id, "b2")
    _assert_expire_after_create(b2, 3 * 3600, "对照：b2 由写点①按它自己的 3h 建单")
    await eng.execute_and_jump_task(b2.id, "rbk-wang", None, "")
    revived = [t for t in await repo.find_doing_tasks(inst.id) if t.taskName == "b1"
               and t.id != b1.id]
    assert len(revived) == 1, f"前置：回退应新建恰好一条 b1 行，实得 {len(revived)}"
    _assert_expire_after_create(revived[0], 3 * 3600, "回退新建取被回退掉的那个节点（b2 的 3h）")


@pytest.mark.asyncio
async def test_i126_rollback_ignores_landing_node_expression():
    """写点④的反向钉：**被回退掉的那个节点（b2）没配**表达式、复活行落地的 b1 配了 "1d"
    ⇒ 复活行的 expire_time 必须留 NULL。这格是"取错节点"最直接的判据：
    表达式来源写成落地节点就会算出 ≈1d（不是空）。"""
    eng, repo, def_id = _expire_harness(
        _expire_flow([("b1", "rbk-zhang", "1d"), ("b2", "rbk-wang", _MISSING)], "expire126rbneg"))
    inst = await eng.start_process_instance_by_id(def_id, "applicant")
    b1 = await _row(repo, inst.id, "b1")
    _assert_expire_after_create(b1, 24 * 3600, "前置：1d 确实配在落地节点上（写点①读得到它）")
    await eng.execute_process_task(b1.id, "rbk-zhang")
    b2 = await _row(repo, inst.id, "b2")
    assert b2.expireTime is None, "前置：b2 没配 ⇒ NULL"
    await eng.execute_and_jump_task(b2.id, "rbk-wang", None, "")
    revived = [t for t in await repo.find_doing_tasks(inst.id) if t.taskName == "b1"
               and t.id != b1.id]
    assert len(revived) == 1, f"前置：回退应新建恰好一条 b1 行，实得 {len(revived)}"
    assert revived[0].expireTime is None, \
        f"被回退掉的节点没配表达式 ⇒ 复活行必须留空，实得 {revived[0].expireTime!r}" \
        "（≈1d 就说明表达式取成了落地节点）"


class _TimeNamedUserProv(_TestUserProv):
    """把指定用户的 realName 造成"合法时刻文本"。u_* 只落在**行上**（实例变量写回按
    issues/97 排除 u_*），所以它是一对天然的可区分探针：拿它当到期表达式，
    读随行那份变量 ⇒ 得到那个时刻；误读实例变量 ⇒ 得到发起人的 realName（解析不出 ⇒ NULL）。"""
    TIME_NAMES = {"rbk-op": "2027-01-15 09:00:00"}

    async def get_user(self, user_id: str):
        u = await super().get_user(user_id)
        if user_id in self.TIME_NAMES:
            u.realName = self.TIME_NAMES[user_id]
        return u


@pytest.mark.asyncio
async def test_i126_rollback_uses_row_variables_not_instance_variables():
    """卡面 §1「变量源两档」的判别格：回退新建用**随行拷贝那份**变量（boot2 的 hisVariable），
    建单三处用**实例变量**。探针 u_realName 只在行上有效 ⇒
    两档写反就在"回退新建拿到 NULL"这一侧直接报红。
    表达式 "u_realName" 配在**被回退掉的那个节点**（b2，＝写点④的取值来源）上，于是同一份表达式
    在这一格里同时给出正反对照：b2 自己那行（写点①读实例变量）⇒ NULL，
    复活行（写点④读随行那份）⇒ rbk-op 的时刻 ⇒ 变量源与节点来源两档一起钉住。"""
    content = _expire_flow([("b1", "rbk-op", _MISSING), ("b2", "rbk-wang", "u_realName")],
                           "expire126src")
    eng, repo, def_id = _expire_harness(content, "expire126src", user_prov=_TimeNamedUserProv())
    inst = await eng.start_process_instance_by_id(def_id, "applicant")
    b1 = await _row(repo, inst.id, "b1")
    assert b1.expireTime is None, "对照：b1 没配表达式 ⇒ NULL"
    await eng.execute_process_task(b1.id, "rbk-op")
    b2 = await _row(repo, inst.id, "b2")
    assert b2.expireTime is None, \
        "起点自证：同一表达式走建单写点①读的是**实例变量**，那里 u_realName 是发起人的（解析不出 ⇒ NULL）"
    await eng.execute_and_jump_task(b2.id, "rbk-wang", None, "")
    inst_after = await repo.find_instance_by_id(inst.id)
    assert inst_after.variables.get("u_realName") != "2027-01-15 09:00:00", \
        "前置：探针必须只活在行上，否则这格没有判别力"
    revived = [t for t in await repo.find_doing_tasks(inst.id) if t.taskName == "b1"
               and t.id != b1.id]
    assert len(revived) == 1, f"前置：回退应新建恰好一条 b1 行，实得 {len(revived)}"
    assert to_datetime(revived[0].expireTime) == datetime(2027, 1, 15, 9, 0, 0), \
        f"回退新建必须按随行那份变量取值，实得 {revived[0].expireTime!r}"


# ─── issues/134 案 A · 撤回的实例状态守卫（内部码 20010009）───────────────────────
#
# 契约（八栈逐字统一，owner 2026-09-28 拍板 A）：撤回作用于**实例**时，实例状态不是 10(进行中)
# 一律拒——被拒时状态不得被改写、不落库。改前对已办结(20)/已终止(40) 的实例调撤回会静默改写成
# 30，"已办列表 / 按状态聚合的统计"凭空改历史且用户看不到任何报错（本案病灶）。
# 落点＝聚合根 ProcessInstance.withdraw（jeeflow/model.py），门面已把那句上提到任务行循环之前
# ⇒ 守卫排在任务行层面既有保护（20/40 行不改写，保持原样）之前。
# 出口＝issues/121 口径：门面吞内部码 ⇒ code=99999999 ＋ msg **逐字** `流程实例非进行中，无法撤回`，
# 不拼码、不加前缀。
# 权威＝jeeflow-java 参考实现（WfErrEnum.WITHDRAW_INSTANCE_NOT_DOING + ProcessInstance.withdraw）
# ＋ jeeflow-doc/docs/spec/06-facade.md §processInstance/withdraw。
# ⚠️ 40(强行终止) 档由常规流程路径造不出（issues/134 §5.2 同款豁免：集成层 gate 也造不出），
#    故按案要求走引擎栈内单测钉死；20 档走真实办结夹具。

_I134_WITHDRAW_MSG = "流程实例非进行中，无法撤回"   # 内部码 20010009 的固定文案（八栈逐字一致）


async def _i134_finished_instance(facade, repo) -> int:
    """夹具：01-simple 办到终态 ⇒ 实例 state=20、doing 任务清空（真实办结，非手搓状态）"""
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")
    doing = await repo.find_doing_tasks(iid)
    assert [t.taskName for t in doing] == ["task1"], f"前置：应停在 task1: {[t.taskName for t in doing]}"
    r = await facade.flow("processTask/execute",
                          {"processTaskId": doing[0].id, "operator": "leader", "submitType": 1})
    assert r["code"] == 0, r
    inst = await repo.find_instance_by_id(iid)
    assert inst.state == InstanceState.DONE, f"前置：夹具应是已办结(20)，实得 {inst.state}"
    return iid


@pytest.mark.asyncio
async def test_i134_withdraw_rejects_finished_instance():
    """负向 state=20（已办结）：断码 99999999 ＋ 文案逐字相等 ＋ 实例/任务行零改写、不落库"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    iid = await _i134_finished_instance(facade, repo)
    inst_before = await repo.find_instance_by_id(iid)
    rows_before = {t.id: (int(t.taskState), t.updateUser, t.updateTime)
                   for t in await repo.find_history_tasks(iid)}

    r = await facade.flow("processInstance/withdraw", {"id": iid, "operator": "zhangsan"})
    assert r["code"] == 99999999, f"已办结实例撤回必须显式报错，实得 {r}"
    assert r["msg"] == _I134_WITHDRAW_MSG, \
        f"文案须逐字相等（不拼码、不加前缀），实得 {r['msg']!r}"
    assert "2001000" not in r["msg"], f"内部码不进 msg（issues/121 口径），实得 {r['msg']!r}"

    inst_after = await repo.find_instance_by_id(iid)
    assert inst_after.state == InstanceState.DONE, \
        f"被拒后实例不得被静默改写成 30（本案病灶）: {inst_after.state}"
    assert inst_after.updateTime == inst_before.updateTime, "被拒后实例 update_time 不得动"
    assert inst_after.updateUser == inst_before.updateUser, "被拒后实例 update_user 不得动"
    rows_after = {t.id: (int(t.taskState), t.updateUser, t.updateTime)
                  for t in await repo.find_history_tasks(iid)}
    assert rows_after == rows_before, "被拒后任务行必须零改写（含 update_user/update_time）"


@pytest.mark.asyncio
async def test_i134_withdraw_rejects_terminated_instance_and_keeps_doing_rows():
    """负向 state=40（强行终止，栈内单测钉）：同样拒；且 doing 任务行不被改写成 30、不消失"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")
    doing = await repo.find_doing_tasks(iid)
    assert len(doing) == 1, f"前置：应有一条 doing 行: {len(doing)}"
    # 40 档造不出常规路径（案 §5.2 豁免）⇒ 仓储层直接置态，其余字段不动
    inst = await repo.find_instance_by_id(iid)
    inst.state = InstanceState.INTERRUPT
    await repo.update_instance(inst)
    before = await repo.find_instance_by_id(iid)
    assert before.state == InstanceState.INTERRUPT, f"前置：夹具应为 40，实得 {before.state}"

    r = await facade.flow("processInstance/withdraw", {"id": iid, "operator": "zhangsan"})
    assert r["code"] == 99999999, f"已终止实例撤回必须显式报错，实得 {r}"
    assert r["msg"] == _I134_WITHDRAW_MSG, f"文案须逐字相等，实得 {r['msg']!r}"

    after = await repo.find_instance_by_id(iid)
    assert after.state == InstanceState.INTERRUPT, f"被拒后实例仍应是 40: {after.state}"
    assert after.updateUser == before.updateUser and after.updateTime == before.updateTime, \
        "被拒后实例 update_* 不得动"
    left = await repo.find_doing_tasks(iid)
    assert [t.id for t in left] == [t.id for t in doing], \
        f"被拒后 doing 行不得消失（改前会整单落 30）: {[t.id for t in left]}"
    row = await repo.find_task_by_id(doing[0].id)
    assert row.taskState == TaskState.DOING, f"doing 行不应被改写成 30: {row.taskState}"
    assert row.updateUser == doing[0].updateUser, "doing 行 update_user 不得被记成撤回人"


@pytest.mark.asyncio
async def test_i134_withdraw_still_succeeds_on_doing_instance():
    """正向对照 state=10：守卫没写反——进行中实例撤回仍 code=0，整单落 30（既有语义不动）"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")
    doing = await repo.find_doing_tasks(iid)

    r = await facade.flow("processInstance/withdraw", {"id": iid, "operator": "zhangsan"})
    assert r["code"] == 0 and r["data"] is None, \
        f"进行中实例撤回必须照旧成功（守卫写反成「只允许非 10」时这一格报红）: {r}"
    inst = await repo.find_instance_by_id(iid)
    assert inst.state == InstanceState.WITHDRAW, f"撤回成功应落 30: {inst.state}"
    assert inst.updateUser == "zhangsan", "update_user 应回写撤回人"
    assert not await repo.find_doing_tasks(iid), "撤回后不得残留 doing 任务"
    row = await repo.find_task_by_id(doing[0].id)
    assert row.taskState == TaskState.WITHDRAW, f"doing 行应落 30: {row.taskState}"


def test_i134_aggregate_guard_covers_every_non_doing_state():
    """聚合根一层（jeeflow/model.py ProcessInstance.withdraw）的判别力：
    20/30/40/45/50/99 六档全拒且**不写任何字段**；10 档（含仓储水合可能给的裸 int）照旧放行。"""
    now = datetime(2026, 9, 28, 10, 0, 0)
    non_doing = [InstanceState.DONE, InstanceState.WITHDRAW, InstanceState.INTERRUPT,
                 InstanceState.REJECT, InstanceState.PENDING, InstanceState.ABANDON]
    # 裸 int 档：守卫必须按"值等于 10"判，不得用 `is`（历史数据/自定义仓储可能给 int）
    non_doing += [20, 40, 99]
    for st in non_doing:
        inst = ProcessInstance(id=1, defineId=1, state=st, operator="zhangsan",
                               updateUser="init", updateTime=None)
        with pytest.raises(ValueError) as ei:
            inst.withdraw(now)
        assert str(ei.value) == _I134_WITHDRAW_MSG, f"{st} 档文案应逐字相等: {ei.value}"
        assert inst.state == st, f"{st} 档被拒后 state 不得被改写: {inst.state}"
        assert inst.updateTime is None, f"{st} 档被拒后 update_time 不得被写: {inst.updateTime}"
        assert inst.updateUser == "init", f"{st} 档被拒后 update_user 不得被写: {inst.updateUser}"

    for doing in (InstanceState.DOING, 10):
        inst = ProcessInstance(id=1, defineId=1, state=doing, operator="zhangsan")
        inst.withdraw(now)
        assert inst.state == InstanceState.WITHDRAW, f"{doing} 档应照旧撤回成 30: {inst.state}"
        assert inst.updateTime == now


@pytest.mark.asyncio
async def test_i134_second_withdraw_on_withdrawn_instance_is_rejected():
    """负向 state=30（二次撤回）：首撤照旧成功，二撤被拒且**不得把 update_user 改成第二次操作人**。
    撤回人用 flow.admin 哨兵（归属判据③放行）⇒ 报错只可能来自状态守卫，不被鉴权分支抢先命中
    （对齐 Java WithdrawInstanceStateGuardTest 门面级两档）。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")

    r1 = await facade.flow("processInstance/withdraw", {"id": iid, "operator": "zhangsan"})
    assert r1["code"] == 0, f"前置：首撤（进行中）应成功: {r1}"
    assert (await repo.find_instance_by_id(iid)).updateUser == "zhangsan"

    r2 = await facade.flow("processInstance/withdraw", {"id": iid, "operator": "flow.admin"})
    assert r2["code"] == 99999999, f"已撤回(30) 实例二次撤回必须被拒: {r2}"
    assert r2["msg"] == _I134_WITHDRAW_MSG, f"文案须逐字相等，实得 {r2['msg']!r}"
    after = await repo.find_instance_by_id(iid)
    assert after.state == InstanceState.WITHDRAW
    assert after.updateUser == "zhangsan", \
        f"被拒的那次不得把撤回人改成第二次操作人: {after.updateUser}"


@pytest.mark.asyncio
async def test_i134_guard_sits_after_operator_and_ownership_branches():
    """回归：本案守卫不污染既有失败文案与分支序（issues/114 两支仍排在状态守卫之前命中），
    且三条负向（缺 operator／越权／非进行中）全都不改状态、不落库。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    iid = await _i134_finished_instance(facade, repo)
    before = await repo.find_instance_by_id(iid)
    rows_before = {t.id: (int(t.taskState), t.updateUser) for t in await repo.find_history_tasks(iid)}

    r = await facade.flow("processInstance/withdraw", {"id": iid})
    assert r["code"] == 99999999 and "operator 必填" in r["msg"], r
    r = await facade.flow("processInstance/withdraw", {"id": iid, "operator": "nobody"})
    assert r["code"] == 99999999 and "无权限撤回该流程实例" in r["msg"], \
        f"鉴权分支应仍排在状态守卫之前: {r}"
    r = await facade.flow("processInstance/withdraw", {"id": iid, "operator": "zhangsan"})
    assert r["code"] == 99999999 and r["msg"] == _I134_WITHDRAW_MSG, r

    after = await repo.find_instance_by_id(iid)
    assert after.state == InstanceState.DONE, f"三条负向后实例仍应是 20: {after.state}"
    assert (after.updateUser, after.updateTime) == (before.updateUser, before.updateTime)
    rows_after = {t.id: (int(t.taskState), t.updateUser) for t in await repo.find_history_tasks(iid)}
    assert rows_after == rows_before, "三条负向后任务行零改写"


# ─── issues/139 · 流程定义 JSON 解析失败的出口 msg 形状（八栈同批 · python 腿）──────────────
# 判据源：jeeflow-hub `issues/139-….md`（owner 拍"修"）＋ issues/121 那轮定的口径——
#   失败信封只出逐字固定文案（code=99999999 ＋ 一句中文），内部码与内部异常细节一律不进 msg。
# 逐字基准＝Java 参考实现 `jeeflow-core/src/main/java/com/mldong/jeeflow/parser/ModelParser.java:47`
#   `throw new RuntimeException("读取流程定义 JSON 失败", e)`（原始异常只作 cause），
#   C# `ModelParser.cs:45/:54` 同句佐证，node 腿已按此收口（facade.ts + __tests__/spec.test.ts）。
#   Java 的 deploy / processDefine·redeploy / processDesign·redeploy 三条腿都走同一个
#   ModelParser.parse ⇒ 本栈三条腿也收敛到单一解析点 `JeeflowFacade._parse_define_content`
#   （改前：design redeploy 腿拼 `f"…: {e}"`；另两条腿压根没包，`flow()` 顶层 str(e) 把
#    JSONDecodeError 原文「Expecting value: line 1 column 10 (char 9)」直接送进出口）。
# 反闸写法照 node/issues/121：单断"包含"对"带前缀/带尾巴"恒绿 ⇒ 逐字等值 ＋ 逐项禁泄漏 ＋ 正向对照。

_I139_MSG = "读取流程定义 JSON 失败"          # 逐字＝Java 参考实现原文，八栈可比对

#: 出口 msg 一律不得出现的内部细节（异常类名／解析器文本／堆栈／文件路径／SQL／入参片段）
_I139_LEAKS = ["Error", "Exception", "JSONDecode", "Traceback", 'File "', "json.loads",
               "Expecting", "char", "line", "column", "at ", ".json", "/", "\\",
               "SELECT", "INSERT", "SENTINEL", "nodes", "{", "["]

_I139_BAD_TRUNCATED = '{"name":"bad139","nodes":['          # 截断 JSON
_I139_BAD_SENTINEL = '{"nodes":[SENTINEL_泄漏面_139]}'       # 非法且内容里带可辨识片段


def _i139_facade():
    eng, repo = setup()
    return eng, repo, JeeflowFacade(eng, repo, MemoryExtRepository())


def _i139_assert_verbatim(r: dict, leg: str):
    """失败信封三件：code 固定 ＋ msg 逐字等值 ＋ 逐项禁泄漏（改前的两种病灶都红）。"""
    assert r["code"] == 99999999, f"{leg} 坏内容必须被拒: {r}"
    assert r["msg"] == _I139_MSG, (
        f"{leg} 出口 msg 要逐字等值——改前病灶是把原始异常文本拼进 msg"
        f"（design redeploy 腿拼 f-string，另两条腿裸送 JSONDecodeError 原文），本格即红: "
        f"{r['msg']!r}")
    for leak in _I139_LEAKS:
        assert leak not in r["msg"], f"{leg} msg 不得含内部细节 {leak!r}: {r['msg']!r}"


@pytest.mark.asyncio
async def test_i139_design_redeploy_parse_fail_msg_is_verbatim():
    """病灶腿 processDesign/redeploy：坏快照 → 逐字文案零细节，且解析失败排在任何写库之前
    （既不落定义行、也不把设计置成已部署）。"""
    eng, repo, facade = _i139_facade()
    for tag, bad in (("截断", _I139_BAD_TRUNCATED), ("带片段", _I139_BAD_SENTINEL)):
        r0 = await facade.flow("processDesign/save", {"name": f"bad139-{tag}",
                                                      "displayName": f"坏内容139-{tag}",
                                                      "content": bad, "operator": "zhangsan"})
        assert r0["code"] == 0, f"前置：save 不校验内容合法性，坏 JSON 也该入库回 id: {r0}"
        design_id = int(r0["data"]["id"])
        assert len(await facade._ext.list_design_his(design_id)) == 1, \
            "前置：内容快照已入库（本用例打的是「有快照但解析失败」那条腿）"

        r = await facade.flow("processDesign/redeploy", {"id": design_id, "operator": "zhangsan"})
        _i139_assert_verbatim(r, f"processDesign/redeploy（{tag}）")
        assert "SENTINEL" not in r["msg"], f"msg 不得把设计稿片段透出来: {r['msg']!r}"

        assert not await repo.find_define_by_name(f"bad139-{tag}"), "解析失败不得留下流程定义行"
        design = await facade._ext.find_design_by_id(design_id)
        assert design.isDeployed == 0, f"被拒后设计仍是未部署(0): {design.isDeployed}"


@pytest.mark.asyncio
async def test_i139_deploy_family_parse_fail_shares_same_verbatim_msg():
    """另两条腿 processDefine/deploy、processDefine/redeploy（含经 _deploy 的 processDesign/deploy）
    与病灶腿同句逐字——Java 三条腿共用一个 ModelParser.parse，本栈共用一个解析点。"""
    eng, repo, facade = _i139_facade()
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        good = f.read()

    r = await facade.flow("processDefine/deploy", {"content": _I139_BAD_SENTINEL,
                                                   "operator": "zhangsan"})
    _i139_assert_verbatim(r, "processDefine/deploy")

    # 空内容同样是解析失败：改前这里裸送 JSONDecodeError 原文「Expecting value: line 1 column 1 (char 0)」
    r = await facade.flow("processDefine/deploy", {"content": "", "operator": "zhangsan"})
    _i139_assert_verbatim(r, "processDefine/deploy（content 为空）")

    r_ok = await facade.flow("processDefine/deploy", {"content": good, "operator": "zhangsan"})
    assert r_ok["code"] == 0, r_ok
    define_id = int(r_ok["data"]["processDefineId"])
    r = await facade.flow("processDefine/redeploy", {"processDefineId": define_id,
                                                     "content": _I139_BAD_TRUNCATED,
                                                     "operator": "zhangsan"})
    _i139_assert_verbatim(r, "processDefine/redeploy")
    untouched = await repo.find_define_by_id(define_id)
    assert untouched.content == good, "被拒的 redeploy 不得改写既有定义内容"

    r0 = await facade.flow("processDesign/save", {"name": "bad139-deploy", "displayName": "坏稿139",
                                                  "content": _I139_BAD_TRUNCATED,
                                                  "operator": "zhangsan"})
    assert r0["code"] == 0, r0
    r = await facade.flow("processDesign/deploy", {"id": int(r0["data"]["id"]),
                                                   "operator": "zhangsan"})
    _i139_assert_verbatim(r, "processDesign/deploy（走 _deploy 同一点）")


@pytest.mark.asyncio
async def test_i139_parse_fail_keeps_original_exception_as_cause():
    """原始异常不进 msg，但要留在错误对象上（``raise ... from e`` ⇒ ``__cause__``）：
    排查侧仍拿得到解析器细节，只是不跨出口。"""
    with pytest.raises(ValueError) as ei:
        JeeflowFacade._parse_define_content(_I139_BAD_TRUNCATED)
    assert str(ei.value) == _I139_MSG, f"异常文本本身也要逐字: {str(ei.value)!r}"
    assert isinstance(ei.value.__cause__, json.JSONDecodeError), \
        f"原始解析异常应作 __cause__ 留在错误对象上: {ei.value.__cause__!r}"


@pytest.mark.asyncio
async def test_i139_valid_content_legs_still_succeed():
    """正向对照（防"改成无条件抛"）：三条腿喂合法内容仍 code=0，msg 仍是"成功"。"""
    eng, repo, facade = _i139_facade()
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        good = f.read()

    r = await facade.flow("processDefine/deploy", {"content": good, "operator": "zhangsan"})
    assert r["code"] == 0 and r["msg"] == "成功", f"正向 deploy 不得被新文案分支拦掉: {r}"
    define_id = int(r["data"]["processDefineId"])

    r = await facade.flow("processDefine/redeploy", {"processDefineId": define_id,
                                                     "content": good, "operator": "zhangsan"})
    assert r["code"] == 0, f"正向 processDefine/redeploy: {r}"

    r0 = await facade.flow("processDesign/save", {"name": "good139", "displayName": "合法139",
                                                  "content": good, "operator": "zhangsan"})
    assert r0["code"] == 0, r0
    design_id = int(r0["data"]["id"])
    r = await facade.flow("processDesign/deploy", {"id": design_id, "operator": "zhangsan"})
    assert r["code"] == 0 and r["data"]["processDefineId"], f"正向 processDesign/deploy: {r}"
    r = await facade.flow("processDesign/redeploy", {"id": design_id, "operator": "zhangsan"})
    assert r["code"] == 0 and r["data"]["processDefineId"], f"正向 processDesign/redeploy: {r}"
    assert (await facade._ext.find_design_by_id(design_id)).isDeployed == 1, "正向仍置已部署(1)"
    assert await repo.find_define_by_id(int(r["data"]["processDefineId"])), "正向定义行确实落库"


# ─── issues/137 B · 零调用者建单函数 _create_task_with_actors 的契约形状 ─────────────────
# 案文（jeeflow-hub/issues/137-….md §4 B）：owner 拍「**不删，补用例钉住**」——本栈
#   `EngineImpl._create_task_with_actors` 自 issues/121 P2（回退改走血缘版 `_rollback_to_parent`）
#   起零调用者（grep 全仓只剩定义处 + spec_test 一条陈旧注释），但它是「显式参与者建单」
#   这条形状的留档位：会签逐人拆行、普通一行承载多参与者、建单前应用委托、落库后 fire 码 3。
#   本格不接线、不改语义，只钉「直调该函数的建单产物与主路径 `_create_task` 逐维一致」。
#   已知缺口在函数 docstring 与本节注释同步留档：本函数**没有** issues/126 案 A 的到期写点①
#   （不调 `_apply_expire_time`，与 rust 侧 `reject_task` 同款 deferred）⇒ 用例不给节点配
#    expireTime，也不把该维并进一致性断言；将来复活它先补写点再谈全维一致。

_I137B_FIELDS = ("taskName", "displayName", "formKey", "taskType", "performType",
                 "taskState", "parentTaskId", "createUser", "updateUser")


async def _i137b_shapes(repo, rows) -> list[dict]:
    """任务行 → 可比对形状。参与者读**持久参与者行**（与 issues/116 那族用例同口径，
    不看内存对象），按参与者排序保证两批逐行对齐。"""
    out = []
    for t in rows:
        shape = {f: getattr(t, f) for f in _I137B_FIELDS}
        shape["actorIds"] = list(await repo.find_task_actors(t.id))
        shape["isFirstTaskNode"] = (t.variables or {}).get("isFirstTaskNode")
        out.append(shape)
    return sorted(out, key=lambda s: s["actorIds"])


def _i137b_node(content: str, node_id: str):
    """同一份 content 解析出节点对象——直调入参与主路径拿到的节点是同一形状。"""
    return next(n for n in parse_flow_model(json.loads(content)).nodes if n.id == node_id)


@pytest.mark.asyncio
async def test_i137b_create_task_with_actors_matches_main_path():
    """直调 vs 主路径：普通·单人 / 普通·多参与者 / 并行会签·逐人拆行三档，
    建单产物逐维一致（行数、参与者、performType、parentTaskId、isFirstTaskNode、留痕字段）。"""
    for idx, (specs, actors) in enumerate((
            ([("t1", "zhang")], ["zhang"]),                       # 普通·单人
            ([("t1", "zhang,li")], ["zhang", "li"]),               # 普通·多参与者任一可办
            ([("t1", "zhang,li", "PARALLEL")], ["zhang", "li"]),   # 并行会签·逐人拆行
    )):
        pname = f"i137b-shape{idx}"
        content = _flow_json(specs, name=pname)
        eng, repo, _ext, def_id = _surr_harness(pname, content)
        node = _i137b_node(content, "t1")

        # ① 主路径：start → _execute_node → _create_task（参与者由 assignee 解析而来）
        inst_main = await eng.start_process_instance_by_id(def_id, "boss1")
        main_rows = await _i137b_shapes(repo, await repo.find_doing_tasks(inst_main.id))

        # ② 直调：另一实例上绕开 _resolve_actors，把同一批参与者显式喂进去
        inst_direct = await eng.start_process_instance_by_id(def_id, "boss1")
        before = {t.id for t in await repo.find_doing_tasks(inst_direct.id)}
        await eng._create_task_with_actors(node, inst_direct, "boss1", {}, list(actors),
                                           process_name=pname, parent_id=0, is_first=True)
        direct_rows = [t for t in await repo.find_doing_tasks(inst_direct.id)
                       if t.id not in before]
        direct_shapes = await _i137b_shapes(repo, direct_rows)

        assert len(direct_shapes) == len(main_rows), (
            f"{pname}：直调建单行数 {len(direct_shapes)} ≠ 主路径 {len(main_rows)}"
            f"（会签逐人拆行 / 普通一行的分档语义必须一致）\n主路径={main_rows}\n直调={direct_shapes}")
        for m, d in zip(main_rows, direct_shapes):
            assert d == m, (f"{pname}：直调与主路径建单产物逐维不一致\n主路径={m}\n直调={d}")


@pytest.mark.asyncio
async def test_i137b_create_task_with_actors_applies_surrogate_and_fires_task_start():
    """函数 docstring 承诺的两件事同样与主路径一致：建单前并入委托人（issues/116）＋
    落库后逐任务 fire PROCESS_TASK_START（码 3，载荷 actors＝落库那份）。"""
    pname = "i137b-surr"
    content = _flow_json([("t1", "zhang")], name=pname)
    eng, repo, ext, def_id = _surr_harness(pname, content)
    await _put_surr(ext, "zhang", "i137b-agent", pname)
    evts: list = []
    eng.set_extensions(EngineExtensions(event_listeners=[evts.append]))

    inst_main = await eng.start_process_instance_by_id(def_id, "boss1")
    main_actors = await _doing_actors(repo, inst_main.id, "t1")

    inst_direct = await eng.start_process_instance_by_id(def_id, "boss1")
    before = {t.id for t in await repo.find_doing_tasks(inst_direct.id)}
    await eng._create_task_with_actors(_i137b_node(content, "t1"), inst_direct, "boss1", {},
                                       ["zhang"], process_name=pname, parent_id=0, is_first=True)
    new_rows = [t for t in await repo.find_doing_tasks(inst_direct.id) if t.id not in before]
    assert len(new_rows) == 1, f"直调应建一行普通任务，实得 {len(new_rows)}"
    direct_actors = await repo.find_task_actors(new_rows[0].id)

    assert direct_actors == main_actors, (
        f"「建单前同样应用委托」两档不一致：直调 {direct_actors} vs 主路径 {main_actors}")
    assert direct_actors == ["zhang", "i137b-agent"], \
        f"直调那一行应既保留授权人又并入代理人（任一可办）: {direct_actors}"

    starts = [e for e in evts if e.type is EventType.PROCESS_TASK_START]
    assert len(starts) == 3, f"两次发起各 1 条 + 直调 1 条，实得 {len(starts)} 条码 3"
    last = starts[-1]
    assert (last.instanceId, last.taskId) == (inst_direct.id, new_rows[0].id), \
        f"直调的码 3 必须落在它自己新建的那行上: {last}"
    assert list(last.actors) == direct_actors, \
        f"码 3 载荷 actors 取落库那份（含代理人）: {last.actors}"


# ═══ Test 141 G1＋G2：抄送分页归属条件必填 ＋ cc 写侧判重＝幂等空操作 ═══════════════
#
# 立法逐字依据（jeeflow-doc/docs/spec）：
# · **06-facade.md §2.5「抄送分页同一条尺子」**（issues/141 G1 · 2026-09-29 owner 拍）：
#   `pageCcInstances` 这类"抄送我"取数入口，归属条件（`cc.actor_id`）**必填**——条件缺失或为空值时
#   **返回空页**，不得退化成"这条不加"而返回全部实例；这条义务要**同时钉在 SQL 仓与内存仓**上
#   （同一栈两仓必须同答案＝issues/117 场景 27 那把尺子），只修一边不算修完。
# · **06-facade.md §4「写侧判重＝幂等空操作」**（issues/141 G2 · owner 拍）：同一 `(实例, 被抄送人)`
#   已存在 cc 行时再次抄送 ⇒ ①不新增行 ②不重置未读状态（state）③不更新原行时间
#   ④**不 fire CC_CREATE（码 4）**（11-events.md §11.2 原则 1「码值表达发生了什么事实」）。
#   逐人 fire 的入参＝**实际新建的子集**，子集为空整支不发。判重在写侧：查询侧不引入 `DISTINCT`、
#   历史重复行不清理（owner 拍为接受既成事实）。
# · 形状基准＝jeeflow-java 本地 commit `3d1fc98`（`CcPageOwnershipTest`／`CcWriteIdempotentTest`／
#   `JdbcCcOwnershipIdempotentTest`）——同一套判据表在本栈**内存仓与 SQL 仓各钉一遍**。
#
# 本栈与 java 的一处形状差（不是判据差）：java 的归属只有 `PageQuery.conditions` 一形，本栈
# `page_cc_instances` 多一个**专用入参 `actor_id`**（门面 ccList 走的就是它）。两形任一给了有效值
# 即"归属条件齐了"，两形都没给才算缺——`_has_cc_ownership` 里两仓同判据。

_CC141_TABLE_DDL = (
    # 本案 SQL 仓一路用到的三张表（列名逐字对齐 tests/schema/schema-mysql.sql 与 base.py 的 SQL）；
    # 形状照 _surrogate_sql_ext 的先例：真 SQLite ＋ 真 JdbcRepository，不用内存假仓。
    "CREATE TABLE wf_process_define (id INTEGER PRIMARY KEY, name TEXT, display_name TEXT,"
    " type TEXT, state INTEGER, content TEXT, version INTEGER, create_time TEXT, create_user TEXT,"
    " update_time TEXT, update_user TEXT)",
    "CREATE TABLE wf_process_instance (id INTEGER PRIMARY KEY, parent_id INTEGER,"
    " process_define_id INTEGER, state INTEGER, parent_node_name TEXT, business_no TEXT,"
    " operator TEXT, expire_time TEXT, variable TEXT, create_time TEXT, create_user TEXT,"
    " update_time TEXT, update_user TEXT)",
    "CREATE TABLE wf_process_cc_instance (id INTEGER PRIMARY KEY, process_instance_id INTEGER,"
    " actor_id TEXT, state INTEGER, create_time TEXT, create_user TEXT, update_time TEXT,"
    " update_user TEXT)",
)


def _cc141_sql_repo():
    """真 SQLite ＋ 真 `JdbcRepository`——G1/G2 的 SQL 仓一路在 **T0** 就得钉住（只钉内存仓
    ＝spec 06 §2.5 明写的"只修一边不算修完"），T1 那一路见 tests/jdbc_test.py 的 ⑩.6 段。"""
    from jeeflow.repository.base import JdbcRepository
    raw = sqlite3.connect(":memory:")
    for ddl in _CC141_TABLE_DDL:
        raw.execute(ddl)
    return raw, JdbcRepository(_SqliteAdapter(raw), _TestIDGen())


def _cc141_cc_rows(raw, instance_id, actor_id=None):
    """cc 表取证：某实例（可指定人）的真实行 [id, actor_id, state, create_time, update_time]。"""
    sql = ("SELECT id, actor_id, state, create_time, update_time FROM wf_process_cc_instance"
           " WHERE process_instance_id=?")
    args: list = [instance_id]
    if actor_id is not None:
        sql += " AND actor_id=?"
        args.append(actor_id)
    return raw.execute(sql + " ORDER BY id", tuple(args)).fetchall()


async def _cc141_seed(repo, pairs, *, sql_repo: bool, define_name: str = "i141"):
    """两仓共用的灌数据姿势：每对 (抄送人, business_no) 一条实例 ＋ 一行 cc，**直连仓储写侧**
    （把判据钉在"分页/写侧"上而不是抄送流程上）。business_no 一律给非空值——空值列在 SQL 三值逻辑里
    是"恒不命中"档，会让"非归属列空值仍被忽略"那一格在两仓各说各话（java 同款留档）。"""
    now = datetime.now()
    if sql_repo:
        d = ProcessDefine(name=define_name, displayName="抄送归属流程", type="test", state=1,
                          version=1, content="{}", createUser="zhangsan", updateUser="zhangsan")
        await repo.save_define(d)
    else:
        d = ProcessDefine(name=define_name, displayName="抄送归属流程", type="test", state=1,
                          version=1, content="{}")
        repo.add_define(d)
    mine = None
    for idx, (actor, biz) in enumerate(pairs):
        iid = 9_410_000 + idx if sql_repo else 0     # SQL 仓显式 id；内存仓交给自增序列
        inst = ProcessInstance(id=iid, defineId=d.id, state=InstanceState.DOING, operator="zhangsan",
                               businessNo=biz, variables={}, createTime=now, updateTime=now,
                               createUser="zhangsan", updateUser="zhangsan")
        await repo.save_instance(inst)
        await repo.create_cc_instance(inst.id, "zhangsan", actor)
        if actor == "user1":
            mine = inst.id
    return d.id, mine


# 判据表（spec 06 §2.5 的 G1 那一套）：标签 / actor_id 入参 / conditions / 契约答案行数。
# ⚠️ 表里**故意没有** "cc.actor_id IN 非空集合（不给入参）"这一格：内存仓的 IN 分支按**标量列**
# 语义实现（`str(v) not in [...]`，v 是集合时永不命中），SQL 仓则拼真 `IN (...)`——那是 issues/05-5
# 通用条件匹配的既有分叉（java 内存仓同样分叉），不属 G1 的范围，本案不新增依赖它的断言。
_CC141_G1_TABLE = (
    ("正向对照：归属入参给有效值 ⇒ 只出我的那一行", "user1", None, 1),
    ("缺条件：入参 None 且一条条件都不给 ⇒ 空页", None, [], 0),
    ("空串入参 ⇒ 空页（issues/129 那一档，G1 后同判据）", "", [], 0),
    ("全空白入参与空串同档 ⇒ 空页", "   ", [], 0),
    ("整条查询都不带（默认参数）⇒ 空页", None, None, 0),
    ("条件形给有效值（入参不给）⇒ 命中，两仓同答案", None,
     [QueryCondition("cc.actor_id", "EQ", "user1")], 1),
    ("条件形 EQ 空串 ⇒ 空页", None, [QueryCondition("cc.actor_id", "EQ", "")], 0),
    ("条件形 EQ 全空白 ⇒ 空页", None, [QueryCondition("cc.actor_id", "EQ", "   ")], 0),
    ("条件形 EQ None ⇒ 空页", None, [QueryCondition("cc.actor_id", "EQ", None)], 0),
    ("条件形 IN 空集合 ⇒ 空页（空集＝没有人）", None,
     [QueryCondition("cc.actor_id", "IN", [])], 0),
    ("归属有效 ＋ 非归属列空值 ⇒ 空值仍按没填忽略", "user1",
     [QueryCondition("t.business_no", "LIKE", "")], 1),
)

_CC141_G1_PAIRS = [("user1", "I141-MINE"), ("user2", "I141-THEIRS")]


@pytest.mark.asyncio
async def test_i141_g1_memory_cc_page_requires_ownership_condition():
    """G1（内存仓）：`page_cc_instances` 归属条件必填，缺则空页。

    改前的红格是"缺条件/整条不带"那两档——本仓旧形状只放"有 cc 行的实例"，`actor_id=None`
    时把它们**全部**放出（旧代码 `if _ownership_blank(actor_id)` 只收空串，None 被当成
    "本次不带归属过滤"）。SQL 仓一路见下一格，两仓逐格比对见 `..._two_repos_same_answer`。
    """
    repo = MemoryRepository()
    _, mine = await _cc141_seed(repo, _CC141_G1_PAIRS, sql_repo=False)
    assert mine, "夹具应能读出 user1 那条实例 id"

    rows, total = await repo.page_cc_instances(1, 50, "user1")
    assert (len(rows), total) == (1, 1), f"带归属条件应只出我的那 1 条: {len(rows)}/{total}"
    assert rows[0].id == mine, f"命中的应是我的实例: {rows[0].id} != {mine}"
    assert rows[0].businessNo == "I141-MINE", rows[0].businessNo

    for tag, args in (("零条件 + 空 conditions", (1, 50, None, [])),
                      ("整条查询都不带默认参数", (1, 10))):
        rows, total = await repo.page_cc_instances(*args)
        assert (len(rows), total) == (0, 0), f"{tag}：缺归属条件必须返回空页: {len(rows)}/{total}"


@pytest.mark.asyncio
async def test_i141_g1_memory_cc_page_blank_and_non_ownership_rows():
    """G1（内存仓）：空值三形＋空 IN 与"整条没给"同档；**非归属列**的空值放行不许一起收掉。

    改前在这一格红：条件形 `cc.actor_id EQ None`——内存仓的 `_match_conditions` 对 `expect is None`
    是"整条跳过"（放行），旧归属判据又只收空串 ⇒ `actor_id=None` ＋空值条件把两条实例全放出。
    其余空值档（入参 ""/"   "/条件 ""/"   "/IN 空集）旧代码碰巧也给 0，G1 后一律走同一条短路。
    """
    repo = MemoryRepository()
    await _cc141_seed(repo, _CC141_G1_PAIRS, sql_repo=False)

    for blank in ("", "   ", "\t\n"):
        rows, total = await repo.page_cc_instances(1, 50, blank)
        assert (len(rows), total) == (0, 0), f"空串档应空页: {blank!r} → {len(rows)}/{total}"
    for conds in ([QueryCondition("cc.actor_id", "EQ", "")],
                  [QueryCondition("cc.actor_id", "EQ", "   ")],
                  [QueryCondition("cc.actor_id", "EQ", None)],
                  [QueryCondition("cc.actor_id", "IN", [])]):
        rows, total = await repo.page_cc_instances(1, 50, None, conds)
        assert (len(rows), total) == (0, 0), f"空值条件档应空页: {conds} → {len(rows)}/{total}"

    # 改动面哨兵：只收归属谓词。非归属列（m_LIKE_business_no 这类可选过滤）传空串仍按"没填"忽略
    rows, total = await repo.page_cc_instances(
        1, 50, "user1", [QueryCondition("t.business_no", "LIKE", "")])
    assert (len(rows), total) == (1, 1), f"非归属列空值仍应被忽略: {len(rows)}/{total}"
    # 反向：非归属列给了**真值**时照旧生效（证明上面那格不是"条件全被忽略"的假绿）
    rows, total = await repo.page_cc_instances(
        1, 50, "user1", [QueryCondition("t.business_no", "LIKE", "NO-SUCH")])
    assert (len(rows), total) == (0, 0), f"非归属列真值仍应过滤: {len(rows)}/{total}"


@pytest.mark.asyncio
async def test_i141_g1_sql_cc_page_requires_ownership_condition():
    """G1（SQL 仓，真 SQLite）：同一套判据表逐格钉一遍。

    改前的红格是**条件形**那一档：旧代码无条件拼 `WHERE cc.actor_id = ?` 并绑定 `actor_id` 入参，
    入参为 None 时那句是 `cc.actor_id = NULL`（SQL 三值逻辑恒不命中）⇒ 明明给了合法的
    `cc.actor_id EQ 'user1'` 条件也返 0 行，而内存仓返 1 行——两仓两个答案，正是本条要收的。
    "缺条件"档旧代码碰巧也返 0（因为同一句 NULL），G1 后由显式短路给同一答案，不再依赖 NULL 巧合。
    """
    raw, repo = _cc141_sql_repo()
    _, mine = await _cc141_seed(repo, _CC141_G1_PAIRS, sql_repo=True)

    rows, total = await repo.page_cc_instances(1, 50, "user1")
    assert (len(rows), total) == (1, 1), f"带归属入参应只出我的那 1 条: {len(rows)}/{total}"
    assert rows[0].id == mine, (rows[0].id, mine)
    assert rows[0].defineName == "i141", rows[0].defineName   # join 定义列照旧在

    rows, total = await repo.page_cc_instances(1, 50, None,
                                               [QueryCondition("cc.actor_id", "EQ", "user1")])
    assert (len(rows), total) == (1, 1), f"条件形同样应命中: {len(rows)}/{total}"

    rows, total = await repo.page_cc_instances(1, 50)
    assert (len(rows), total) == (0, 0), f"缺归属条件必须空页: {len(rows)}/{total}"
    rows, total = await repo.page_cc_instances(1, 50, None, [])
    assert (len(rows), total) == (0, 0), f"空 conditions 同样空页: {len(rows)}/{total}"
    # 短路必须真的短路：库里两条实例都在，缺条件时连 SQL 都不该放出行（不是靠 NULL 巧合）
    n_inst = raw.execute("SELECT COUNT(*) FROM wf_process_instance").fetchone()[0]
    assert n_inst == 2, f"夹具应真有 2 条实例: {n_inst}"


@pytest.mark.asyncio
async def test_i141_g1_sql_cc_page_blank_and_non_ownership_rows():
    """G1（SQL 仓）：空值三形＋空 IN ⇒ 空页；非归属列空值仍按没填忽略。

    改前在这一格红：末尾那格"只给条件形＋非归属列空值"——恒绑的 `WHERE cc.actor_id = ?`（None ⇒
    `= NULL`）把合法的 `cc.actor_id EQ 'user1'` 折成 0 行，与内存仓的 1 行分叉。
    """
    _raw, repo = _cc141_sql_repo()
    await _cc141_seed(repo, _CC141_G1_PAIRS, sql_repo=True)

    for blank in ("", "   ", "\t\n"):
        rows, total = await repo.page_cc_instances(1, 50, blank)
        assert (len(rows), total) == (0, 0), f"空串档应空页: {blank!r} → {len(rows)}/{total}"
    for conds in ([QueryCondition("cc.actor_id", "EQ", "")],
                  [QueryCondition("cc.actor_id", "EQ", "   ")],
                  [QueryCondition("cc.actor_id", "EQ", None)],
                  [QueryCondition("cc.actor_id", "IN", [])]):
        rows, total = await repo.page_cc_instances(1, 50, None, conds)
        assert (len(rows), total) == (0, 0), f"空值条件档应空页: {conds} → {len(rows)}/{total}"

    rows, total = await repo.page_cc_instances(
        1, 50, "user1", [QueryCondition("t.business_no", "LIKE", "")])
    assert (len(rows), total) == (1, 1), f"非归属列空值仍应被忽略: {len(rows)}/{total}"
    rows, total = await repo.page_cc_instances(
        1, 50, None, [QueryCondition("cc.actor_id", "EQ", "user1"),
                      QueryCondition("t.business_no", "LIKE", "")])
    assert (len(rows), total) == (1, 1), f"条件形＋可选空值同样应命中: {len(rows)}/{total}"


@pytest.mark.asyncio
async def test_i141_g1_cc_page_two_repos_same_answer():
    """G1 的**两仓同答案**那一半：同一份数据、同一张判据表，SQL 仓与内存仓逐格读数必须相等。

    只钉一边不算修完（spec 06 §2.5 原话），而"各自都绿"也不等于"两边一致"——本格把整张表喂给
    两仓后逐格比 rows/total，再逐格比契约期望值（两个方向都钉，任一侧单独漂就红）。
    """
    mem = MemoryRepository()
    await _cc141_seed(mem, _CC141_G1_PAIRS, sql_repo=False, define_name="i141")
    _raw, sql = _cc141_sql_repo()
    await _cc141_seed(sql, _CC141_G1_PAIRS, sql_repo=True, define_name="i141")

    for label, actor_id, conds, want in _CC141_G1_TABLE:
        m_rows, m_total = await mem.page_cc_instances(1, 50, actor_id, conds)
        s_rows, s_total = await sql.page_cc_instances(1, 50, actor_id, conds)
        assert (m_total, s_total) == (want, want), f"{label}：契约期望 {want} 行，实得 内存 {m_total} / SQL {s_total}"
        assert len(m_rows) == len(s_rows) == want, f"{label}：rows 数与 total 同口径，实得 内存 {len(m_rows)} / SQL {len(s_rows)}"
        assert sorted(r.businessNo for r in m_rows) == sorted(r.businessNo for r in s_rows), \
            f"{label}：两仓命中的必须是同一批实例（按 businessNo 比，两仓各用自己的 id 段）"


@pytest.mark.asyncio
async def test_i141_g1_facade_cc_list_still_passes_through_facade():
    """改动面哨兵：门面 ccList 恒挂归属条件（`_operator_arg` 归一 → actor_id 入参），
    G1 的收紧不该让这个出口少一行；缺键/空串档照旧回落 demo 缺省 user1（issues/129 第一层）。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")
    r = await facade.flow("processInstance/createCCInstance",
                          {"processInstanceId": iid, "operator": "zhangsan", "actorIds": ["user1"]})
    assert r["code"] == 0, r

    for args in ({"operator": "user1"}, {"operator": ""}, {"operator": "   "}, {}):
        r = await facade.flow("processInstance/ccList", args)
        assert r["code"] == 0, (args, r)
        assert len(r["data"]["rows"]) == 1, f"门面 ccList 应恒出 1 行（缺省回落 user1）: {args} {r['data']}"
    r = await facade.flow("processInstance/ccList", {"operator": "nobody"})
    assert r["code"] == 0 and len(r["data"]["rows"]) == 0, r


# ── G2 夹具 ────────────────────────────────────────────────────────────────────

def _cc141_tick():
    """让"原行时间被刷新"与"没被刷新"在断言上分得开（datetime.now 逐次取值，留 10ms 余量）。"""
    import time
    time.sleep(0.01)


def _cc141_harness(repo=None):
    """引擎＋门面＋事件 sink：sink 只收 CC_CREATE(4)，"重复抄送没有新事件"就断在这里。"""
    if repo is None:
        repo = MemoryRepository()
    eng = EngineImpl(repo, _TestUserProv(), _TestIDGen(), _TestExprEval())
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    cc_events: list = []
    eng.set_extensions(EngineExtensions(event_listeners=[
        lambda evt: cc_events.append(evt) if evt.type is EventType.CC_CREATE else None]))
    return eng, repo, facade, cc_events


async def _cc141_manual(facade, iid, *actors):
    """门面手动腿 processInstance/createCCInstance（三条入口之一）。"""
    r = await facade.flow("processInstance/createCCInstance",
                          {"processInstanceId": iid, "operator": "zhangsan",
                           "actorIds": list(actors)})
    assert r["code"] == 0, r
    return r


async def _cc141_started(facade, **flow_args):
    """01-simple 走 startAndExecute（申请节点自动办结 ⇒ 停在 task1），返回 (define_id, 实例 id)。"""
    define_id = await _deploy(facade, "01-simple.json")
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": define_id, "operator": "zhangsan", **flow_args})
    assert r["code"] == 0, r
    return define_id, int(r["data"]["processInstanceId"])


@pytest.mark.asyncio
async def test_i141_g2_memory_first_cc_creates_rows_and_fires_per_actor():
    """正向对照（首轮该发的还是要发）：全新的一次抄送照旧逐人建行、逐人 fire 码 4、新行未读。

    摘掉 G2 的那一支（`create_cc_instance_if_absent` 换成旧的 `create_cc_instance`）本格不该红；
    把子集 fire 写成"整支不发"也会在这里红——它钉的是"判重不许把首轮吃掉"。
    """
    _eng, repo, facade, cc_events = _cc141_harness()
    _define_id, iid = await _cc141_started(facade)
    cc_events.clear()
    _cc141_tick()

    await _cc141_manual(facade, iid, "6101", "6102")

    assert await repo.find_cc_actor_ids(iid) == ["6101", "6102"], \
        "全新抄送应逐人落行（顺序与入参一致）"
    assert [e.ccActorId for e in cc_events] == ["6101", "6102"], \
        f"全新抄送应逐人 fire 码 4: {[e.ccActorId for e in cc_events]}"
    assert all(e.instanceId == iid for e in cc_events), [e.instanceId for e in cc_events]
    rows = repo.cc_rows_for_test(iid)
    assert [str(r) for r in rows] == ["6101", "6102"], rows
    assert all(r.state == 0 for r in rows), f"新建行应一律未读: {[(str(r), r.state) for r in rows]}"
    assert all(r.create_time is not None and r.update_time is not None for r in rows), rows


@pytest.mark.asyncio
async def test_i141_g2_memory_repeat_cc_adds_no_row_and_fires_nothing():
    """①不新增行 ＋ ④不 fire 码 4：手动腿连发两次同一个人。

    改前红在④：引擎旧代码 `create_cc_instance(...)` 后**无条件**按原始入参全量 fire
    （spec §11.2 原则 1「码=事实」被破——重复抄送根本没发生"创建"）。
    """
    _eng, repo, facade, cc_events = _cc141_harness()
    _define_id, iid = await _cc141_started(facade)
    await _cc141_manual(facade, iid, "6201")
    assert await repo.find_cc_actor_ids(iid) == ["6201"], "首次抄送落 1 行"
    assert len(cc_events) == 1, f"首次抄送 fire 1 次: {len(cc_events)}"

    cc_events.clear()
    _cc141_tick()
    await _cc141_manual(facade, iid, "6201")

    created = await repo.create_cc_instance_if_absent(iid, "zhangsan", ["6201"])
    assert created == [], f"重复抄送的实际新建子集应为空: {created}"
    assert await repo.find_cc_actor_ids(iid) == ["6201"], "①重复抄送不得新增行"
    assert len(repo.cc_rows_for_test(iid)) == 1, "①重复抄送后行数仍是 1"
    assert cc_events == [], "④没发生创建就不得发码 4（spec 11.2 原则 1「码=事实」）"

    # 仓储侧自己也要顶住：绕过漏斗**裸调** create_cc_instance 同样不得新增行
    await repo.create_cc_instance(iid, "zhangsan", "6201")
    assert len(repo.cc_rows_for_test(iid)) == 1, \
        f"①判重写在仓储写侧，裸调也不许多出一行: {repo.cc_rows_for_test(iid)}"


@pytest.mark.asyncio
async def test_i141_g2_memory_repeat_cc_does_not_reset_unread():
    """②不重置未读：置已读后重复抄送，state 必须仍是已读（owner 明确"不需要重置"）。

    本档在 G2 之前**照不出来**：内存仓旧 `update_cc_status` 是 `pass`、cc 只存 actor id 串，
    既没有 state 也没有时间——java 为同一理由把它的内存仓升级成行模型，本仓同理。
    判据的"假修"对照面是把判重写成 upsert（重置 state），见本仓报告里的还原读数。
    """
    _eng, repo, facade, _events = _cc141_harness()
    _define_id, iid = await _cc141_started(facade)
    await _cc141_manual(facade, iid, "6301")
    r = await facade.flow("processInstance/updateCCStatus",
                          {"processInstanceId": iid, "operator": "6301"})
    assert r["code"] == 0, r
    assert repo.cc_rows_for_test(iid)[0].state == 1, "置读后 state 应为 1（内存仓与 SQL 仓同语义）"

    _cc141_tick()
    await _cc141_manual(facade, iid, "6301")

    rows = repo.cc_rows_for_test(iid)
    assert len(rows) == 1 and str(rows[0]) == "6301", f"①不新增行（也不该冒出第二行盖住已读行）: {rows}"
    assert rows[0].state == 1, f"②重复抄送不得把已读抹回未读: {rows[0].state}"

    # 绕过漏斗裸调仓储写入口：同一判据必须同样成立（判重写在仓储，不是只写在子集计算里）
    _cc141_tick()
    await repo.create_cc_instance(iid, "zhangsan", "6301")
    rows = repo.cc_rows_for_test(iid)
    assert len(rows) == 1, f"②裸调也不许多出行: {rows}"
    assert rows[0].state == 1, f"②裸调不得把已读抹回未读: {rows[0].state}"


@pytest.mark.asyncio
async def test_i141_g2_memory_repeat_cc_does_not_touch_row_times():
    """③不更新原行时间：create_time/update_time 逐字不变（跳过式判重不走 UPDATE、也不删旧插新）。"""
    _eng, repo, facade, _events = _cc141_harness()
    _define_id, iid = await _cc141_started(facade)
    await _cc141_manual(facade, iid, "6401")
    before = repo.cc_rows_for_test(iid)[0]
    create_time, update_time, row_id = before.create_time, before.update_time, id(before)
    assert create_time is not None

    _cc141_tick()
    await _cc141_manual(facade, iid, "6401")

    after = repo.cc_rows_for_test(iid)[0]
    assert id(after) == row_id, "③原行必须还是那一行（没有删旧插新）"
    assert after.create_time == create_time, f"③不得刷新原行 create_time: {after.create_time}"
    assert after.update_time == update_time, f"③不得刷新原行 update_time: {after.update_time}"

    # 绕过漏斗裸调仓储写入口：时间照旧逐字不变
    _cc141_tick()
    await repo.create_cc_instance(iid, "zhangsan", "6401")
    bare = repo.cc_rows_for_test(iid)[0]
    assert (bare.create_time, bare.update_time) == (create_time, update_time),         f"③裸调重复抄送不得刷时间: {(create_time, update_time)} → {(bare.create_time, bare.update_time)}"


@pytest.mark.asyncio
async def test_i141_g2_memory_repeat_cc_fires_only_new_subset():
    """④的子集档：第二次给「已知人＋新人」⇒ 只为新人建行、只为新人 fire（入参＝实际新建子集）。

    改前红在这里：旧代码把**原始请求**（6501,6503）整个拿去 fire，
    6501 一行都没新建却收到码 4——"一行一事件"的粒度被破（spec §11.3 码 4）。
    """
    _eng, repo, facade, cc_events = _cc141_harness()
    _define_id, iid = await _cc141_started(facade)
    await _cc141_manual(facade, iid, "6501", "6502")
    assert await repo.find_cc_actor_ids(iid) == ["6501", "6502"], "首轮 2 行"
    assert len(cc_events) == 2, f"首轮 fire 2 次: {len(cc_events)}"

    cc_events.clear()
    _cc141_tick()
    await _cc141_manual(facade, iid, "6501", "6503")

    assert [e.ccActorId for e in cc_events] == ["6503"], \
        f"逐人 fire 的入参应是实际新建的子集: {[e.ccActorId for e in cc_events]}"
    assert await repo.find_cc_actor_ids(iid) == ["6501", "6502", "6503"], "实际新建的 cc 行也只有那一行"


@pytest.mark.asyncio
async def test_i141_g2_memory_duplicate_within_one_call_collapses():
    """同一次调用里重复给同一个人 ⇒ 也按幂等处理（一行一次提醒），判重在入参侧同样折叠。

    走仓储直连（不经引擎的 `parse_cc_actors` 去重），这样"同一次调用折叠"这条义务
    钉在**仓储**上——引擎侧的去重挡不住绕过引擎直调仓储的调用方。
    """
    eng, repo, _facade, cc_events = _cc141_harness()
    iid = 9_420_001   # cc 写侧不 join 实例表，直连仓储用一个固定实例 id 即可
    cc_events.clear()

    # 裸写入口（一个调用里给两次同一个人）
    await repo.create_cc_instance(iid, "zhangsan", "6601", "6601")
    assert await repo.find_cc_actor_ids(iid) == ["6601"],         f"同一次调用内的重复不新增第二行: {await repo.find_cc_actor_ids(iid)}"

    # 漏斗入口（同一批里既有已知人又有重复新人）：子集折叠后才拿去 fire
    created = await repo.create_cc_instance_if_absent(iid, "zhangsan", ["6601", "6602", "6602"])
    await eng._notify_cc_create(iid, created)

    assert created == ["6602"], f"子集应折叠同一次调用内的重复、并剔掉已存在的 6601: {created}"
    assert await repo.find_cc_actor_ids(iid) == ["6601", "6602"], "两形入口折叠到同一行集"
    assert len(repo.cc_rows_for_test(iid)) == 2, repo.cc_rows_for_test(iid)
    assert [e.ccActorId for e in cc_events] == ["6602"], f"每人一次提醒、重复项不再提醒: {cc_events}"


@pytest.mark.asyncio
async def test_i141_g2_engine_cc_legs_share_the_same_dedup_rule():
    """两条腿共用同一条判据：发起 `f_ccActors` 已建的人，办理 `tf_ccActors` 再给一次
    ⇒ 不新增行、不 fire；同批里的新人照旧建行＋fire（spec §11.7「三条入口同一支」）。"""
    eng, repo, facade, cc_events = _cc141_harness()
    define_id = await _deploy(facade, "01-simple.json")
    inst = await eng.start_process_instance_by_id(define_id, "zhangsan",
                                                  {"f_ccActors": "7001"})
    iid = inst.id
    assert await repo.find_cc_actor_ids(iid) == ["7001"], "发起腿落 1 行"
    assert [e.ccActorId for e in cc_events] == ["7001"], "发起腿 fire 1 次"

    cc_events.clear()
    _cc141_tick()
    doing = await repo.find_doing_tasks(iid)
    assert doing, "发起后应有待办"
    await repo.add_task_actor(doing[0].id, ["zhangsan"])
    await eng.execute_process_task(doing[0].id, "zhangsan", {"submitType": 0})
    doing = await repo.find_doing_tasks(iid)
    assert doing, "申请节点办结后应有 task1 待办"
    await repo.add_task_actor(doing[0].id, ["leader"])
    await eng.execute_process_task(doing[0].id, "leader",
                                   {"submitType": 1, "tf_ccActors": "7001,7002"})

    assert await repo.find_cc_actor_ids(iid) == ["7001", "7002"], \
        f"办理腿只为新人 7002 建行（7001 已有行）: {await repo.find_cc_actor_ids(iid)}"
    assert [e.ccActorId for e in cc_events] == ["7002"], \
        f"办理腿只 fire 实际新建的子集: {[e.ccActorId for e in cc_events]}"


@pytest.mark.asyncio
async def test_i141_g2_string_and_collection_forms_share_the_dedup_rule():
    """两形态入参（`Collection` 逐元素 / 逗号串 `str`）共用同一条判重腿：
    发起腿给集合、办理腿给逗号串，重叠的人仍只有一行、只 fire 一次。"""
    eng, repo, facade, cc_events = _cc141_harness()
    define_id = await _deploy(facade, "01-simple.json")
    inst = await eng.start_process_instance_by_id(define_id, "zhangsan",
                                                  {"f_ccActors": ["7101", "7102"]})
    iid = inst.id
    assert await repo.find_cc_actor_ids(iid) == ["7101", "7102"], "集合形态照旧逐人建行"
    assert [e.ccActorId for e in cc_events] == ["7101", "7102"], "集合形态照旧逐人 fire"

    cc_events.clear()
    _cc141_tick()
    doing = await repo.find_doing_tasks(iid)
    await repo.add_task_actor(doing[0].id, ["zhangsan"])
    await eng.execute_process_task(doing[0].id, "zhangsan", {"submitType": 0})
    doing = await repo.find_doing_tasks(iid)
    await repo.add_task_actor(doing[0].id, ["leader"])
    await eng.execute_process_task(doing[0].id, "leader",
                                   {"submitType": 1, "tf_ccActors": "7101, 7103 ,7101"})

    assert await repo.find_cc_actor_ids(iid) == ["7101", "7102", "7103"], \
        f"逗号串形态与集合形态判重同一条: {await repo.find_cc_actor_ids(iid)}"
    assert [e.ccActorId for e in cc_events] == ["7103"], \
        f"两形态混用也只为新人 fire: {[e.ccActorId for e in cc_events]}"


@pytest.mark.asyncio
async def test_i141_g2_dedup_is_scoped_to_instance_not_global():
    """反向哨兵：判重不许把"这个实例上没抄送过的人"也吃掉——不同实例上的同一个人各自建行、各 fire。

    把判重的读侧写成全局集合（`SELECT actor_id FROM wf_process_cc_instance` 少带 WHERE）
    就会在这一格红，而①②③④四档全是绿的。
    """
    _eng, repo, facade, cc_events = _cc141_harness()
    define_id = await _deploy(facade, "01-simple.json")
    first = await _start(facade, define_id, "zhangsan")
    second = await _start(facade, define_id, "lisi")
    assert first != second
    cc_events.clear()

    await _cc141_manual(facade, first, "6701")
    _cc141_tick()
    await _cc141_manual(facade, second, "6701")

    assert await repo.find_cc_actor_ids(first) == ["6701"], "实例一有自己的 cc 行"
    assert await repo.find_cc_actor_ids(second) == ["6701"], "实例二不受实例一影响，同一个人照样建行"
    assert [e.instanceId for e in cc_events] == [first, second], \
        f"两个实例各 fire 一次: {[(e.ccActorId, e.instanceId) for e in cc_events]}"


@pytest.mark.asyncio
async def test_i141_g2_sql_repo_write_side_is_idempotent():
    """G2（SQL 仓一路，真 SQLite）：①不新增行（含原行 id 不变）②不重置未读 ③不刷原行时间
    ＋同一次调用内重复折叠 ＋ `find_cc_actor_ids` 读侧反映真实行集 ＋ 返回实际新建子集。
    断言一律直查 `wf_process_cc_instance` 的真实行——只看返回值或只看内存对象都不作数。

    ⚠️ ①②③与"同调用折叠"四档都走**裸写入口 `create_cc_instance`**（不经漏斗）：漏斗那侧的
    子集由 SPI default 先算好，`create_cc_instance` 收到的本来就是一批新人——把仓储里的判重
    摘掉，走漏斗的断言照样绿（本轮实测：还原病灶后 `-k i141` 仍 19 passed，就是这个洞）。
    判重义务写在仓储里，就必须由裸写入口把它照出来；漏斗侧的子集档另有两格盯着。
    """
    raw, repo = _cc141_sql_repo()
    iid = 9_421_001

    await repo.create_cc_instance(iid, "zhangsan", "8101", "8102")
    assert [r[1] for r in _cc141_cc_rows(raw, iid)] == ["8101", "8102"], "首轮逐人建行"
    first_row = _cc141_cc_rows(raw, iid, "8101")[0]
    assert first_row[2] == 0, f"新行应未读: {first_row}"

    _cc141_tick()
    await repo.create_cc_instance(iid, "zhangsan", "8101")
    rows = _cc141_cc_rows(raw, iid)
    assert [r[1] for r in rows] == ["8101", "8102"], f"①重复抄送不得新增行: {rows}"
    after = _cc141_cc_rows(raw, iid, "8101")[0]
    assert after[0] == first_row[0], "①原行 id 不变（没有删旧插新）"
    assert after[3:] == first_row[3:], f"③原行 create_time/update_time 逐字不变: {first_row[3:]} → {after[3:]}"

    # 同一次调用内的重复也只落一行（判重在仓储写侧，不吃引擎 parse_cc_actors 的去重）
    await repo.create_cc_instance(iid, "zhangsan", "8104", "8104")
    assert [r[1] for r in _cc141_cc_rows(raw, iid)] == ["8101", "8102", "8104"], "同调用折叠成一行"

    # ②不重置未读：真 SQL 置读（state=1）后重复抄送，state 必须仍是 1，也不许冒出第二行未读
    await repo.update_cc_status(iid, "8102")
    read_row = _cc141_cc_rows(raw, iid, "8102")[0]
    assert read_row[2] == 1, "置读后 state 应为 1"
    _cc141_tick()
    await repo.create_cc_instance(iid, "zhangsan", "8102")
    rows = _cc141_cc_rows(raw, iid, "8102")
    assert len(rows) == 1 and rows[0][2] == 1, f"②不得把已读抹回未读、也不得多出一行未读: {rows}"
    assert rows[0][4] == read_row[4], f"③已读行的 update_time 不得被重复抄送刷掉: {read_row[4]} → {rows[0][4]}"

    # 读侧：find_cc_actor_ids 反映真实行集（判重依据不能是内存猜测）
    assert await repo.find_cc_actor_ids(iid) == ["8101", "8102", "8104"], "读侧＝库里的真行"
    assert await repo.find_cc_actor_ids(9_421_999) == [], "空实例没有 cc 行"

    # 漏斗侧：子集＝实际新建的人（新人档），且只新建那一行
    created = await repo.create_cc_instance_if_absent(iid, "zhangsan", ["8101", "8103"])
    assert created == ["8103"], f"子集只含新人: {created}"
    assert sorted(r[1] for r in _cc141_cc_rows(raw, iid)) == ["8101", "8102", "8103", "8104"], \
        "①不新增重复行"
    assert await repo.create_cc_instance_if_absent(iid, "zhangsan", ["8101", "8102"]) == [], \
        "全重复 ⇒ 子集空（漏斗据此整支不发码 4）"


@pytest.mark.asyncio
async def test_i141_g2_sql_repo_funnel_fires_only_new_subset():
    """G2（SQL 仓＋引擎漏斗）：子集为空整支不发码 4；有新人时只 fire 新人。

    引擎腿在两条入口上是同一支 `handle_cc_actors`（本栈的 cc 腿已在引擎里），
    这一格把"fire 用子集"这件事在 **SQL 仓**上也钉一遍——只对内存仓钉等于放过两仓分叉。
    """
    _raw, repo = _cc141_sql_repo()
    eng, _repo, _facade, cc_events = _cc141_harness(repo=repo)
    iid = 9_422_001

    await eng.handle_cc_actors(iid, "zhangsan", "8501,8502")
    assert [e.ccActorId for e in cc_events] == ["8501", "8502"], "首轮逐人 fire"

    cc_events.clear()
    _cc141_tick()
    await eng.handle_cc_actors(iid, "zhangsan", ["8501", "8502"])       # 全重复
    assert cc_events == [], f"子集为空 ⇒ 整支不 fire（不空转）: {[e.ccActorId for e in cc_events]}"

    _cc141_tick()
    await eng.handle_cc_actors(iid, "zhangsan", "8501,8503")            # 一半新人
    assert [e.ccActorId for e in cc_events] == ["8503"], \
        f"两仓同样只 fire 实际新建子集: {[e.ccActorId for e in cc_events]}"
    assert await repo.find_cc_actor_ids(iid) == ["8501", "8502", "8503"]


@pytest.mark.asyncio
async def test_i141_g2_sql_repo_query_side_still_has_no_distinct():
    """owner 拍的边界：判重只在**写侧**，查询侧不加 DISTINCT、历史重复行也不清理。

    这一格钉的是"没被顺手改宽"：手工插两行重复（模拟存量脏数据）后
    `page_cc_instances` 仍按 SQL 现状出行数（各栈维持现状，内存仓天然一实例一行——
    这条不对称 owner 已拍为接受既成事实，见 spec 06 §4 末段）。
    """
    raw, repo = _cc141_sql_repo()
    iid = 9_423_001
    now = datetime.now()
    await repo.save_instance(ProcessInstance(
        id=iid, defineId=0, state=InstanceState.DOING, operator="zhangsan",
        businessNo="I141-DUP", variables={}, createTime=now, updateTime=now,
        createUser="zhangsan", updateUser="zhangsan"))
    for rid in (9_423_101, 9_423_102):
        raw.execute("INSERT INTO wf_process_cc_instance (id, process_instance_id, actor_id, state,"
                    " create_time, create_user, update_time, update_user)"
                    " VALUES (?,?,'8601',0,?,'zhangsan',?,'zhangsan')", (rid, iid, now, now))
    raw.commit()

    rows, total = await repo.page_cc_instances(1, 10, "8601")
    assert (len(rows), total) == (2, 2), f"查询侧不引入 DISTINCT：历史脏行仍按 2 行出: {len(rows)}/{total}"
    # 但写侧今后不再新增第三行
    assert await repo.create_cc_instance_if_absent(iid, "zhangsan", ["8601"]) == []
    assert len(_cc141_cc_rows(raw, iid)) == 2, "写侧判重只保证今后不再新增重复行"


@pytest.mark.asyncio
async def test_i141_g2_spi_defaults_keep_third_party_repos_on_old_behaviour():
    """SPI 形状（issues/141 G2）：`find_cc_actor_ids` 的 default＝空集（不判重），
    未覆写它的第三方仓储走 default ⇒ 行为与旧 `create_cc_instance` 逐字一致（全量建行、全量返回），
    源码兼容不破。同时钉住"自带两仓必须覆写"，否则两仓两个答案在写侧重演。"""
    assert MemoryRepository.find_cc_actor_ids is not ProcessRepository.find_cc_actor_ids, \
        "内存仓未覆写 find_cc_actor_ids（判重不生效）"
    from jeeflow.repository.base import JdbcRepository
    assert JdbcRepository.find_cc_actor_ids is not ProcessRepository.find_cc_actor_ids, \
        "SQL 仓未覆写 find_cc_actor_ids（判重不生效）"

    class _ThirdPartyRepo(MemoryRepository):
        """模拟只实现 `create_cc_instance` 的第三方仓储：把 SPI default 显式装回来。"""
        find_cc_actor_ids = ProcessRepository.find_cc_actor_ids

        def __init__(self):
            super().__init__()
            self.create_calls: list[tuple] = []

        async def create_cc_instance(self, instance_id, creator, *actor_ids):
            self.create_calls.append(actor_ids)
            await MemoryRepository.create_cc_instance(self, instance_id, creator, *actor_ids)

    repo = _ThirdPartyRepo()
    assert await repo.find_cc_actor_ids(1) == [], "default 返回空集＝不判重"
    created = await repo.create_cc_instance_if_absent(7, "zhangsan", ["a", "b", "a"])
    assert created == ["a", "b"], f"default 只折叠同一次调用内的重复、不做跨行判重: {created}"
    assert repo.create_calls == [("a", "b")], f"default 把子集原样交给 create_cc_instance: {repo.create_calls}"


# ═══ Test 141 G10：空抄送人不建 cc 行（spec 06-facade.md §2.10 · owner 2026-09-29 拍「空不创建行」）
#
# 立法逐字依据（jeeflow-doc/docs/spec/06-facade.md §2.10，只读）：三条入口（发起 ``f_ccActors``／
# 办理 ``tf_ccActors``／门面手动 ``createCCInstance``）解析抄送人集合时，**空串、纯空白、数组里的
# 空元素一律丢弃**；丢完为空 ⇒ 不建任何 cc 行、也**不 fire 码 4**；逗号串与数组两形同判据。
# 四点实现要求：① **两层都挡**（漏斗归一＋写侧 ``create_cc_instance`` 自己也丢，只修漏斗则绕过
# 引擎/门面直连仓储的调用方照样灌空值）；② **落库与比较一律取 trim 后的值**（``" 123 "`` 与
# ``"123"`` 同一个人，不 trim 会把上一轮 G2 的写侧判重打穿成同一人两行）；③ 手动腿丢完为空时与
# **本仓既有的"空 actorIds"档同判**（本栈＝99999999 ＋ ``processInstanceId/actorIds 缺失``，
# 沿用不新造码/文案）；④ **反向哨兵**：``"0"`` 这类"看起来像空"的正常 id 不得被当空值丢掉。
#
# 形状基准＝jeeflow-java commit ``5fbd5ac``（``StringUtils.normalizeCcActors`` 一支归一腿 ＋
# handleCcActors／createCCInstance 各自先归一再判空 ＋ SPI default createCcInstanceIfAbsent ＋
# Jdbc/Memory 两仓 createCcInstance 写侧兜底）。
#
# 本栈现状普查（2026-09-30 实测，明细见本轮报告）：漏斗 ``parse_cc_actors`` **早已**逐元素
# trim/丢空/去重（java 那侧曾 ``"".split(",")`` 落一条 ``actor_id=''`` 的行、按本条更正；python
# 没有这个洞），G10 真正缺的是——写侧三层（SPI default／内存仓／SQL 仓 裸调 ``create_cc_instance``
# 灌空值照样落行）＋ trim 判等（``" 123 "`` 与 ``"123"`` 两行）＋ 手动腿的**数组形空元素**档
# （``_to_str_list`` 的 list 分支只 ``str()`` 不丢 ⇒ ``[""]`` 走成"报成功但一行没建"）。

def test_i141_g10_funnel_parse_cc_actors_drops_blanks_and_trims():
    """漏斗归一（普查表钉成断言）：逗号串与数组**两形同判据**——空串/纯空白/空元素丢弃、
    值取 trim 后的串、同一次调用折叠重复；反向哨兵 ``"0"`` 不得被当空值丢掉（要求 ④）。

    摘掉 ``parse_cc_actors`` 的 trim/丢空（写回 ``raw`` 原样返回）这一格立刻红。
    """
    from jeeflow.engine import parse_cc_actors
    # 逗号串腿
    assert parse_cc_actors("") == [], '"" 归一为空集（java 旧形状这里会落一条 actor_id="" 的行）'
    assert parse_cc_actors("   ") == []
    assert parse_cc_actors(" , ") == []
    assert parse_cc_actors("a,,b") == ["a", "b"], "空段丢弃、有效项保留"
    assert parse_cc_actors("a,") == ["a"], "尾随逗号带出的空段丢弃"
    assert parse_cc_actors(" a ") == ["a"], "落库/比较值取 trim 后的串"
    assert parse_cc_actors(" 8123 , 8123 ") == ["8123"], "trim 后同一个人 ⇒ 折叠成一项"
    # 数组腿
    assert parse_cc_actors(["a", "", "  "]) == ["a"]
    assert parse_cc_actors([""]) == []
    assert parse_cc_actors(["  ", "\t"]) == []
    assert parse_cc_actors(None) == []
    assert parse_cc_actors([]) == []
    # 反向哨兵
    assert parse_cc_actors("0") == ["0"], '"0" 是正常 id，不是空值'
    assert parse_cc_actors(["0", ""]) == ["0"]
    assert parse_cc_actors([" 0 "]) == ["0"]


@pytest.mark.asyncio
async def test_i141_g10_start_leg_blank_cc_creates_no_row_and_no_fire():
    """发起腿 ``f_ccActors``：空串/纯空白/全空元素数组 ⇒ **零 cc 行、零码 4**；
    混着给时只留有效项（丢完为空与"没带抄送"逐字同形，spec §2.10＋§11.2 原则 1）。"""
    from jeeflow.engine import KEY_CC_ACTORS_START
    for blank in ("", "   ", ["", "  "], ["  "], ["\t"], None):
        eng, repo, facade, cc_events = _cc141_harness()
        define_id = await _deploy(facade, "01-simple.json")
        inst = await eng.start_process_instance_by_id(define_id, "zhangsan",
                                                      {KEY_CC_ACTORS_START: blank})
        assert await repo.find_cc_actor_ids(inst.id) == [], f"空抄送不得建行: {blank!r}"
        assert repo.cc_rows_for_test(inst.id) == [], f"空抄送不得建行: {blank!r}"
        assert cc_events == [], f"空抄送不得 fire 码 4: {blank!r} {[e.ccActorId for e in cc_events]}"
    # 混给：只留有效项、且值取 trim 后的串
    eng, repo, facade, cc_events = _cc141_harness()
    define_id = await _deploy(facade, "01-simple.json")
    inst = await eng.start_process_instance_by_id(define_id, "zhangsan",
                                                  {KEY_CC_ACTORS_START: " 7101 ,, 7102 ,"})
    assert await repo.find_cc_actor_ids(inst.id) == ["7101", "7102"]
    assert [e.ccActorId for e in cc_events] == ["7101", "7102"]


@pytest.mark.asyncio
async def test_i141_g10_handle_leg_blank_cc_creates_no_row_and_no_fire():
    """办理腿 ``tf_ccActors``：与发起腿同一条判据（只修一条腿＝跨栈分叉）。"""
    from jeeflow.engine import KEY_CC_ACTORS
    for blank in ("", "   ", ["", "  "], ["  "]):
        eng, repo, facade, cc_events = _cc141_harness()
        define_id = await _deploy(facade, "01-simple.json")
        inst = await eng.start_process_instance_by_id(define_id, "zhangsan")
        doing = await repo.find_doing_tasks(inst.id)
        await repo.add_task_actor(doing[0].id, ["zhangsan"])
        await eng.execute_process_task(doing[0].id, "zhangsan", {"submitType": 0})
        doing = await repo.find_doing_tasks(inst.id)
        assert doing, "申请节点办结后应有 task1 待办"
        await repo.add_task_actor(doing[0].id, ["leader"])
        cc_events.clear()
        await eng.execute_process_task(doing[0].id, "leader",
                                       {"submitType": 1, KEY_CC_ACTORS: blank})
        assert await repo.find_cc_actor_ids(inst.id) == [], f"办理腿空抄送不得建行: {blank!r}"
        assert cc_events == [], f"办理腿空抄送不得 fire 码 4: {blank!r}"
    # 混给：空元素丢弃、有效项照常
    eng, repo, facade, cc_events = _cc141_harness()
    define_id = await _deploy(facade, "01-simple.json")
    inst = await eng.start_process_instance_by_id(define_id, "zhangsan")
    doing = await repo.find_doing_tasks(inst.id)
    await repo.add_task_actor(doing[0].id, ["zhangsan"])
    await eng.execute_process_task(doing[0].id, "zhangsan", {"submitType": 0})
    doing = await repo.find_doing_tasks(inst.id)
    await repo.add_task_actor(doing[0].id, ["leader"])
    cc_events.clear()
    await eng.execute_process_task(doing[0].id, "leader",
                                   {"submitType": 1, KEY_CC_ACTORS: ["", " 7103 ", "", "7104"]})
    assert await repo.find_cc_actor_ids(inst.id) == ["7103", "7104"]
    assert [e.ccActorId for e in cc_events] == ["7103", "7104"]


@pytest.mark.asyncio
async def test_i141_g10_manual_leg_empty_after_drop_is_the_missing_actorids_case():
    """手动腿（要求 ③）：``actorIds`` 丢完为空 ⇒ 与本仓**既有的"空 actorIds"档同判**
    ——99999999 ＋ msg 含 ``actorIds 缺失``（逐字沿用 ``test_create_cc_instance_empty_actors``
    那一档，不新造错误码/文案），并且零行零 fire。

    旧形状：``[""]`` 经 ``_to_str_list`` 得到**非空 list** ⇒ 过了判空闸门、进了漏斗被丢成空集
    ⇒ "报成功但一行没建"（与 java 的 ``return error("actorIds 缺失")`` 分叉）。"""
    for bad in ([""], ["  "], ["", "  "], ["\t"], "", "   ", " , "):
        eng, repo, facade, cc_events = _cc141_harness()
        r = await facade.flow("processInstance/createCCInstance",
                              {"processInstanceId": 500, "operator": "zhangsan", "actorIds": bad})
        assert r["code"] == 99999999, f"{bad!r} 应与空 actorIds 同档报错: {r}"
        assert "actorIds 缺失" in r["msg"], f"{bad!r} 沿用既有文案: {r}"
        assert await repo.find_cc_actor_ids(500) == []
        assert cc_events == []
    # 既有那两档不破（回归）：真空 list／缺键同样报同一句
    eng, repo, facade, _ev = _cc141_harness()
    for bad_args in ({"actorIds": []}, {}):
        r = await facade.flow("processInstance/createCCInstance",
                              {"processInstanceId": 500, "operator": "zhangsan", **bad_args})
        assert r["code"] == 99999999 and "actorIds 缺失" in r["msg"], (bad_args, r)


@pytest.mark.asyncio
async def test_i141_g10_manual_leg_keeps_valid_and_trim_matches_write_side_dedup():
    """手动腿正向：数组里的空元素丢弃、有效项 trim 后落库；**trim 判等与 G2 写侧判重咬合**
    （要求 ②）——先抄 ``" 8123 "`` 再抄 ``"8123"`` ⇒ 库里只有 8123 一行、第二次不新增也不 fire。"""
    eng, repo, facade, cc_events = _cc141_harness()
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")
    cc_events.clear()

    await _cc141_manual(facade, iid, "a", "", "  ")
    assert await repo.find_cc_actor_ids(iid) == ["a"], "空元素丢弃、有效项保留"
    assert [e.ccActorId for e in cc_events] == ["a"]

    cc_events.clear()
    await _cc141_manual(facade, iid, " 8123 ")
    assert await repo.find_cc_actor_ids(iid) == ["a", "8123"], \
        f"落库值取 trim 后的串: {await repo.find_cc_actor_ids(iid)}"
    assert [e.ccActorId for e in cc_events] == ["8123"]

    cc_events.clear()
    _cc141_tick()
    await _cc141_manual(facade, iid, "8123")
    assert await repo.find_cc_actor_ids(iid) == ["a", "8123"], \
        "同一人不许因前后空格落两行（G10 的 trim 与 G2 的判重同一条尺子）"
    assert cc_events == [], f"没发生创建 ⇒ 不发码 4: {cc_events}"


@pytest.mark.asyncio
async def test_i141_g10_memory_repo_write_side_blocks_blanks():
    """写侧兜底（要求 ①，内存仓）：绕过引擎漏斗/门面**裸调** ``create_cc_instance`` 灌空值 ⇒
    照样建不出行；同批里的有效项照常落。只修漏斗时这一档是漏的（本轮普查实测：旧形状
    ``("",)``／``("  ",)`` 各落一条 ``CcRow('')``/``CcRow('  ')``）。"""
    repo = MemoryRepository()
    await repo.create_cc_instance(1, "zhangsan", "", "   ", "\t", None)
    assert repo.cc_rows_for_test(1) == [], "空串/纯空白/None 一律不落行"
    await repo.create_cc_instance(2, "zhangsan", "6101", "", "  ")
    assert [str(r) for r in repo.cc_rows_for_test(2)] == ["6101"], "混给时只丢空元素"


@pytest.mark.asyncio
async def test_i141_g10_memory_repo_write_side_trims():
    """写侧兜底＋trim（内存仓，要求 ②）：``" 8102 "`` 与 ``"8102"`` 是同一个人 ⇒ 只一行，
    且落库值是 trim 后的串；**同一次调用内**两形也只一行（不 trim 就把 G2 判重打穿成两行）。"""
    repo = MemoryRepository()
    await repo.create_cc_instance(3, "zhangsan", " 8102 ")
    assert [str(r) for r in repo.cc_rows_for_test(3)] == ["8102"], "落库值取 trim 后的串"
    await repo.create_cc_instance(3, "zhangsan", "8102")
    assert [str(r) for r in repo.cc_rows_for_test(3)] == ["8102"], "跨调用判重不吃空格"
    await repo.create_cc_instance(3, "zhangsan", " 8103 ", "8103")
    assert [str(r) for r in repo.cc_rows_for_test(3)] == ["8102", "8103"], "同调用折叠"
    assert await repo.find_cc_actor_ids(3) == ["8102", "8103"]


@pytest.mark.asyncio
async def test_i141_g10_spi_default_drops_blanks_and_trims():
    """写侧兜底（要求 ①，SPI default ``create_cc_instance_if_absent``）：未覆写
    ``find_cc_actor_ids`` 的第三方仓储走 default ⇒ 空档不进子集（连 ``create_cc_instance``
    都不该被调），比较与返回的子集取 trim 后的值。"""
    class _ThirdPartyRepo(MemoryRepository):
        find_cc_actor_ids = ProcessRepository.find_cc_actor_ids

        def __init__(self):
            super().__init__()
            self.create_calls: list[tuple] = []

        async def create_cc_instance(self, instance_id, creator, *actor_ids):
            self.create_calls.append(actor_ids)
            await MemoryRepository.create_cc_instance(self, instance_id, creator, *actor_ids)

    repo = _ThirdPartyRepo()
    assert await repo.create_cc_instance_if_absent(9, "zhangsan", ["", "  ", "\t", None]) == []
    assert repo.create_calls == [], "全空入参 ⇒ 子集空 ⇒ 不写库、不 fire"
    created = await repo.create_cc_instance_if_absent(
        9, "zhangsan", [" 8201 ", "", "8201", None, "8202"])
    assert created == ["8201", "8202"], f"子集＝归一后的值＋折叠重复: {created}"
    assert repo.create_calls == [("8201", "8202")], repo.create_calls


@pytest.mark.asyncio
async def test_i141_g10_sql_repo_write_side_blocks_blanks_and_trims():
    """写侧兜底（要求 ①②，SQL 仓一路真 SQLite）：裸调 ``create_cc_instance`` 灌空值 ⇒ 零行；
    ``" 8301 "`` 与 ``"8301"`` 判同一人只一行。断言直查 ``wf_process_cc_instance`` 的真实行。"""
    raw, repo = _cc141_sql_repo()
    iid = 9_421_010
    await repo.create_cc_instance(iid, "zhangsan", "", "  ", "\t", None)
    assert _cc141_cc_rows(raw, iid) == [], "空串/纯空白/None 一律不落行"

    await repo.create_cc_instance(iid, "zhangsan", " 8301 ")
    assert [r[1] for r in _cc141_cc_rows(raw, iid)] == ["8301"], "落库值取 trim 后的串"
    await repo.create_cc_instance(iid, "zhangsan", "8301")
    assert [r[1] for r in _cc141_cc_rows(raw, iid)] == ["8301"], "同一人不许因空格落两行"
    await repo.create_cc_instance(iid, "zhangsan", "8302", "", " 8302 ", "8303")
    assert [r[1] for r in _cc141_cc_rows(raw, iid)] == ["8301", "8302", "8303"]

    created = await repo.create_cc_instance_if_absent(
        iid, "zhangsan", ["", " 8303 ", "8304", None])
    assert created == ["8304"], f"子集只含归一后的新人: {created}"
    assert [r[1] for r in _cc141_cc_rows(raw, iid)] == ["8301", "8302", "8303", "8304"]


@pytest.mark.asyncio
async def test_i141_g10_reverse_sentinel_zero_like_ids_survive_every_layer():
    """反向哨兵（要求 ④）：``"0"`` 这类"看起来像空"的正常 id 在**每一层**都不许被丢掉——
    漏斗／内存仓／SQL 仓／SPI default／门面手动腿。判据只认 ``strip()`` 后是否为空串，
    写成 ``if not actor`` 就会在这一格红。"""
    from jeeflow.engine import parse_cc_actors
    assert parse_cc_actors(["0"]) == ["0"]

    repo = MemoryRepository()
    await repo.create_cc_instance(11, "zhangsan", "0")
    assert [str(r) for r in repo.cc_rows_for_test(11)] == ["0"], "内存仓不得丢掉 0"

    raw, sql = _cc141_sql_repo()
    await sql.create_cc_instance(11, "zhangsan", "0")
    assert [r[1] for r in _cc141_cc_rows(raw, 11)] == ["0"], "SQL 仓不得丢掉 0"
    assert await sql.create_cc_instance_if_absent(11, "zhangsan", ["0"]) == [], \
        "已存在的人判重生效（判据是库里已有行，不是值是 0）"

    eng, _repo, facade, cc_events = _cc141_harness()
    r = await facade.flow("processInstance/createCCInstance",
                          {"processInstanceId": 500, "operator": "zhangsan", "actorIds": ["0"]})
    assert r["code"] == 0, f'"0" 不是空值，不该走"actorIds 缺失"档: {r}'
    assert [e.ccActorId for e in cc_events] == ["0"]


@pytest.mark.asyncio
async def test_i141_g10_two_repos_same_answer():
    """两仓同答案（issues/117 场景 27 那把尺子）：同一批"带空值/带空格"的入参逐对灌进内存仓与
    SQL 仓，落出的 actor 集必须逐字相同——只修一边（本轮普查的真实风险面）在这一格红。"""
    matrix = [("",), ("  ",), ("a", ""), (" b ",), ("b",), ("0",), (" 0 ",), (None,)]
    mem = MemoryRepository()
    raw, sql = _cc141_sql_repo()
    for i, args in enumerate(matrix, start=1):
        await mem.create_cc_instance(i, "zhangsan", *args)
        await sql.create_cc_instance(i, "zhangsan", *args)
    mem_ans = {i: [str(r) for r in mem.cc_rows_for_test(i)] for i in range(1, len(matrix) + 1)}
    sql_ans = {i: [r[1] for r in _cc141_cc_rows(raw, i)] for i in range(1, len(matrix) + 1)}
    assert mem_ans == sql_ans, f"两仓判据分叉:\n 内存仓 {mem_ans}\n SQL 仓 {sql_ans}"
    assert mem_ans[1] == [] and mem_ans[2] == [] and mem_ans[8] == [], "空档两仓都是零行"
    assert mem_ans[3] == ["a"] and mem_ans[4] == ["b"] and mem_ans[5] == ["b"], \
        f"trim 判等两仓同尺: {mem_ans}"
    assert mem_ans[6] == ["0"] and mem_ans[7] == ["0"], "哨兵 0 两仓都保住"


# ═══ Test 141 G9：记录类（snaker:custom）节点没有参与者是正常形态（spec 02 §6.1）═══════════
#
# 立法逐字依据（jeeflow-doc/docs/spec/02-flow-definition.md §6.1，只读）——
# owner 2026-09-29 原话：「这个得根据任务类型来，自定义类型这种记录类的，不会有参与人，是正常行为。」
# ⇒ "参与者解析为空"按**节点类型分判**：
#   · 任务类（task／approval，含会签）：仍建 DOING 行（G5 那一支本轮不动）；
#   · 记录类（custom／带 clazz 的自定义节点）：**不得建 DOING 行**，执行 clazz、落一条
#     历史/已完成行（task_state=20）、令牌继续流转。
# 三条禁止形状里本轮撤掉的是 python 上一笔 commit `ac8b557` 引入的②「兜底把行挂给当前操作人」
# （伪造一条他不该收到的待办）。形状基准＝jeeflow-java `model/CustomModel.java`
# （exec ⇒ 执行 clazz ⇒ `createHistoryTask`（FINISHED）⇒ `runOutTransition`）。
#
# ⚠️ 一处刻意的栈分歧（不是判据分歧，报告里单列）：java/C# 在 clazz 不可解析时**显式报错**；
# python 夹具里的 clazz 是 JVM 类名（`com.mldong.jeeflow.test.TestCustomHandler`），本栈无从解析，
# 报错会让任何沿用共享夹具的流程必然失败 ⇒ 本栈未注册时记 WARNING 后继续"落历史行＋推进"。

def _i141g9_harness():
    """记录类节点夹具：引擎＋门面＋**全量**事件 sink＋可按名注册 clazz 处理器的 registry。"""
    from jeeflow import HandlerRegistry
    repo = MemoryRepository()
    eng = EngineImpl(repo, _TestUserProv(), _TestIDGen(), _TestExprEval())
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    events: list = []
    reg = HandlerRegistry()
    eng.set_extensions(EngineExtensions(event_listeners=[lambda e: events.append(e)], registry=reg))
    return eng, repo, facade, events, reg


@pytest.mark.asyncio
async def test_i141_g9_custom_node_is_record_not_todo_and_flow_continues():
    """核心形状（夹具 08-custom-node.json：start→apply(task)→custom1(custom)→end）：
    发起后 custom1 **不建 DOING 行**、落一条 **DONE 历史行**、令牌走到 end ⇒ 实例 state=20。

    改前（HEAD `f6fc3b4`，参与者为空时兜底给当前操作人建 DOING）这一格红在②③：
    实测形状是 custom1 `task_state=10`／actors=['userB']，实例停在 state=10。"""
    _eng, repo, facade, events, _reg = _i141g9_harness()
    define_id = await _deploy(facade, "08-custom-node.json")
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": define_id, "operator": "userB"})
    assert r["code"] == 0, r
    iid = int(r["data"]["processInstanceId"])

    inst = await repo.find_instance_by_id(iid)
    tasks = {t.taskName: t for t in inst.tasks}
    assert "custom1" in tasks, f"记录类节点不得丢留痕: {list(tasks)}"
    # ② 禁止"兜底挂当前操作人的 DOING 行"
    assert int(tasks["custom1"].taskState) == 20, \
        f"custom 节点必须落历史/已完成行（不许当任务类建待办）: {tasks['custom1'].taskState}"
    assert not await repo.find_doing_tasks(iid), f"custom 节点不得产生 DOING 行: {await repo.find_doing_tasks(iid)}"
    # ③ 禁止"直接跳过不建行"——令牌继续推进到 end
    assert int(inst.state) == 20, f"流程应随记录类节点执行后继续推进到办结，实得 state={inst.state}"
    # 当前操作人那里不该多出这条待办
    todo = await facade.flow("processTask/todoList", {"operator": "userB", "pageNum": 1, "pageSize": 50})
    names = [row.get("taskName") for row in (todo.get("data") or {}).get("rows") or []]
    assert "custom1" not in names, f"伪造待办又回来了: {names}"


@pytest.mark.asyncio
async def test_i141_g9_custom_history_row_carries_invariants_and_no_task_start_event():
    """历史行的建单不变量与事件形状：parentTaskId＝刚办结的那个任务（非 0）、
    行级 isFirstTaskNode 在、`finish_time` 落了；**不发**码 3（PROCESS_TASK_START）——
    对齐 java `persistTasks` 只对新建 DOING 单走 `notifyTaskStart`，`createHistoryTask` 那支不进。"""
    _eng, repo, facade, events, _reg = _i141g9_harness()
    define_id = await _deploy(facade, "08-custom-node.json")
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": define_id, "operator": "userB"})
    iid = int(r["data"]["processInstanceId"])
    inst = await repo.find_instance_by_id(iid)
    apply_task = next(t for t in inst.tasks if t.taskName == "apply")
    custom = next(t for t in inst.tasks if t.taskName == "custom1")

    assert custom.parentTaskId == apply_task.id and custom.parentTaskId, \
        f"历史行要带 parent（issues/121 P1 建单不变量）: {custom.parentTaskId} vs {apply_task.id}"
    assert custom.variables.get("isFirstTaskNode") is False, "custom1 不是首任务节点"
    assert apply_task.variables.get("isFirstTaskNode") is True, "apply 是首任务节点（对照）"
    assert custom.finishTime is not None, "已完成行应有 finishTime"
    assert custom.displayName == "通知外部系统", f"行显示名取节点 text: {custom.displayName}"

    starts = [e for e in events if e.type is EventType.PROCESS_TASK_START]
    assert all(int(e.taskId) != custom.id for e in starts), \
        f"记录类行不得发码 3（码 3 表达新待办产生）: {[(e.taskId, e.taskName) for e in starts]}"
    assert any(e.type is EventType.PROCESS_INSTANCE_END for e in events), "实例办结仍要发码 2"


@pytest.mark.asyncio
async def test_i141_g9_custom_clazz_handler_runs_and_return_lands_in_vars():
    """clazz 执行腿：按名注册的处理器被调用（java `CustomModel.exec` 的 IHandler 那支同形），
    返回值写进执行变量的 `val` 指定键；未指定 `val` 时回落缺省键 `custom_return_val`
    （java FlowConst.CUSTOM_RETURN_VAL）。异步与同步两种 handle 都吃。"""
    from jeeflow.extensions import ICustomHandler
    from jeeflow.engine import KEY_CUSTOM_RETURN_VAL

    seen: list = []

    class _AsyncHandler(ICustomHandler):
        async def handle(self, node, instance, operator, vars_):
            seen.append((node.id, operator))
            return "customExecuted"

    eng, repo, facade, _events, reg = _i141g9_harness()
    reg.register_custom("com.mldong.jeeflow.test.TestCustomHandler", _AsyncHandler())
    define_id = await _deploy(facade, "08-custom-node.json")
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": define_id, "operator": "userB"})
    iid = int(r["data"]["processInstanceId"])
    assert seen == [("custom1", "userB")], f"handler 应按 clazz 名解析并执行一次: {seen}"
    inst = await repo.find_instance_by_id(iid)
    # 夹具的 custom1 properties 里 "val": "customResult" ⇒ 返回值落 customResult
    assert inst.variables.get("customResult") == "customExecuted", \
        f"返回值应写进 val 指定的键: {inst.variables}"
    assert KEY_CUSTOM_RETURN_VAL not in inst.variables, "给了 val 就不该再落到缺省键"


@pytest.mark.asyncio
async def test_i141_g9_unregistered_clazz_falls_back_to_record_and_continue():
    """本栈刻意的分歧档（报告单列）：clazz 未注册 ⇒ **不报错**，仍然落历史行＋流程继续推进。
    java/C# 这里是 `throw 自定义模型[class=...]实例化对象失败`；python 的夹具 clazz 是 JVM 类名、
    本栈无从解析，报错＝沿用共享夹具的流程在 python 必然失败（栈限制非语义缺陷）。
    这一格钉住"分歧只在 clazz 那一支，形状（DONE 行＋推进）不许跟着漂"。"""
    _eng, repo, facade, _events, _reg = _i141g9_harness()   # 不注册任何 custom handler
    define_id = await _deploy(facade, "08-custom-node.json")
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": define_id, "operator": "userB"})
    assert r["code"] == 0, f"未注册 clazz 不得把流程打断: {r}"
    inst = await repo.find_instance_by_id(int(r["data"]["processInstanceId"]))
    custom = next(t for t in inst.tasks if t.taskName == "custom1")
    assert int(custom.taskState) == 20 and int(inst.state) == 20, \
        f"仍要落历史行＋推进: row={custom.taskState} inst={inst.state}"


@pytest.mark.asyncio
async def test_i141_g9_task_node_with_empty_actors_still_creates_doing_row():
    """分流的另一半（回归哨兵）：**任务类**节点参与者为空 ⇒ 照旧建一行 DOING（G5 的口径本轮不动，
    owner 指令"这条不变"）——改动只把记录类那一侧搬走，不许顺手把任务类也变成不建行。

    夹具：start → apply(task, assignee=applicant，被 startAndExecute 自动办结)
    → task2(task，**不给 assignee/handler** ⇒ `_resolve_actors` 返回空) → end。"""
    eng, repo, facade, _events, _reg = _i141g9_harness()
    content = json.dumps({
        "name": "g9-task-empty-actors", "displayName": "任务类空参与者", "type": "approval",
        "nodes": [
            {"id": "start", "type": "snaker:start", "text": {"value": "开始"}, "properties": {}},
            {"id": "apply", "type": "snaker:task", "text": {"value": "发起申请"},
             "properties": {"assignee": "applicant"}},
            {"id": "task2", "type": "snaker:task", "text": {"value": "没人可派的审批"},
             "properties": {}},
            {"id": "end", "type": "snaker:end", "text": {"value": "结束"}, "properties": {}},
        ],
        "edges": [{"id": "e1", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
                  {"id": "e2", "sourceNodeId": "apply", "targetNodeId": "task2", "properties": {}},
                  {"id": "e3", "sourceNodeId": "task2", "targetNodeId": "end", "properties": {}}],
    })
    r = await facade.flow("processDefine/deploy", {"content": content})
    assert r["code"] == 0, r
    did = int(r["data"]["processDefineId"])
    node2 = next(n for n in parse_flow_model(json.loads(content)).nodes if n.id == "task2")
    inst_probe = await repo.find_instance_by_id(1) or ProcessInstance(id=1, defineId=did, operator="zhangsan")
    assert await eng._resolve_actors(node2, inst_probe, "zhangsan", {}) == [], \
        "夹具前提：task2 的参与者解析必须真的是空集，否则这一格照不到兜底那一支"

    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": did, "operator": "zhangsan"})
    assert r["code"] == 0, r
    iid = int(r["data"]["processInstanceId"])
    doing = await repo.find_doing_tasks(iid)
    assert [t.taskName for t in doing] == ["task2"], \
        f"任务类节点解析不到人也要留一行可办待办（本轮不许把这条一起改掉）: {doing}"
    inst = await repo.find_instance_by_id(iid)
    assert int(inst.state) == 10, "任务类空参与者仍停在进行中"


# ═══ Test 142 A 批（python 两腿）：任务类零参与者**建行不挂人** ＋ clazz 两档分开 ═══════════
#
# 立法依据（jeeflow-doc/docs/spec/02-flow-definition.md，只读）——
#   §6.1 硬结论 1：「"参与者为空 ⇒ 兜底挂给当前操作人"这种写法**八栈一律不许有**」。
#     上一笔 commit `f9f6ca8` 只按 §6.1 拆干净了**记录类**那一腿，任务类这条腿当时的注释留话
#     "若也适用于任务类，需要 owner 另行拍板"——owner 2026-09-30 拍了：**适用于任务类**
#     （issues/142 §5 第 3 问）。⇒ 撤掉 `engine._create_task` 的
#     `fallback = operator or inst.operator`，改成与 java `CreateTaskHandler`
#     （:38-63 无条件建单，actors 为空也 `instance.createTask(...)`）**逐字同形**的
#     "建一行 DOING、参与者为空集合"。旧形状的两个病灶各自有牙钉着：
#       ① 兜底挂当前操作人 ＝ **伪造**一条他不该收到的待办（他自己能办掉一个没指派给他的节点）；
#       ② `if not fallback: return` ＝ 操作人为空时既不建行也不推进 ⇒ 实例停在 state=10
#          却零可办行，正是 §6.1 点名的死锁黑洞（本栈那一支曾为消 demo_reset 红引入）。
#     ⚠️ "零参与者"与"参与者为空就不建单"是两件事：要的是**建行且不挂人**
#     （go/node/rust/moon 现在是"一行不建"那一侧，本栈不许跟过去）。
#   §6.2 第 2 条：clazz 解析不了 ⇒ 记日志 + 照常落历史行 + 令牌继续流转；
#     **"未注册处理器"与"clazz 为空串"要分档**（两条都继续，但日志文案分别可诊断）；
#     处理器**自身执行失败**不在豁免内 ⇒ 照旧外抛（那是业务错误不是配错形状）。
#     上一版只有 `elif clazz:` 一档 ⇒ 空串/缺失 clazz **静默无日志**。
# 这一批**不顶任何既有格**：`test_i141_g9_task_node_with_empty_actors_still_creates_doing_row`
# 只钉"行建出来"（不钉参与者），撤兜底后照样绿；下面每一格都在改前实测会红（见交付报告）。

_I142_START_NODE = {"id": "start", "type": "snaker:start", "text": {"value": "开始"}, "properties": {}}
_I142_APPLY_NODE = {"id": "apply", "type": "snaker:task", "text": {"value": "发起申请"},
                    "properties": {"assignee": "applicant"}}
_I142_END_NODE = {"id": "end", "type": "snaker:end", "text": {"value": "结束"}, "properties": {}}


def _i142_chain(mid_nodes: list) -> tuple:
    """start → apply(applicant，被 startAndExecute 自动办结) → *中间节点* → end。

    中间节点**刻意不接在 start 后面**：门面的 `_startAndExecute` 会对"发起后所有 DOING 行"
    补 `add_task_actor(当前操作人)` 再自动办理（那是 applicant 的申请腿机制，八栈同形、本轮不动），
    首节点直接就是待测节点会让那层机制先替我们把人挂上，照不到引擎的形状。"""
    nodes = [_I142_START_NODE, _I142_APPLY_NODE] + mid_nodes + [_I142_END_NODE]
    seq = ["start", "apply"] + [n["id"] for n in mid_nodes] + ["end"]
    edges = [{"id": f"e{i}", "sourceNodeId": a, "targetNodeId": b, "properties": {}}
             for i, (a, b) in enumerate(zip(seq, seq[1:]), start=1)]
    return nodes, edges


async def _i142_start(facade, name: str, nodes: list, edges: list) -> int:
    """部署＋发起一气呵成，回实例 id（operator 固定 zhangsan＝"当前操作人"，兜底 once 挂的就是他）。"""
    content = json.dumps({"name": name, "displayName": name, "type": "approval",
                          "nodes": nodes, "edges": edges})
    r = await facade.flow("processDefine/deploy", {"content": content})
    assert r["code"] == 0, r
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": int(r["data"]["processDefineId"]), "operator": "zhangsan"})
    assert r["code"] == 0, f"发起不得被零参与者/clazz 配错打断: {r}"
    return int(r["data"]["processInstanceId"])


@pytest.mark.asyncio
async def test_i142_hr1_zero_actor_task_row_is_created_but_not_attached_to_operator():
    """硬结论 1 的正腿：任务类节点参与者解析为空 ⇒ **有这一行**（task_state=10）、
    参与者是**真空集合**、**当前操作人不在里面**（最后这条才是撤兜底的牙——只数行数照不出来，
    旧兜底形状同样是"一行 DOING"，只是行上挂着 zhangsan）。"""
    _eng, repo, facade, events, _reg = _i141g9_harness()
    nodes, edges = _i142_chain([{"id": "task2", "type": "snaker:task",
                                 "text": {"value": "没人可派的审批"}, "properties": {}}])
    iid = await _i142_start(facade, "i142-hr1-empty-actors", nodes, edges)

    doing = await repo.find_doing_tasks(iid)
    assert [t.taskName for t in doing] == ["task2"], \
        f"任务类零参与者必须建一行 DOING（不许回成『一行不建』那一侧）: {doing}"
    t2 = doing[0]
    assert int(t2.taskState) == 10, f"建行状态必须是进行中: {t2.taskState}"
    # 参与者＝真空集：既不是 ["zhangsan"]（旧兜底），也不是 [""]（空串归属值，issues/142 B 表那族垃圾）
    assert list(t2.actorIds) == [], f"零参与者行的 actorIds 必须是空集: {t2.actorIds}"
    assert await repo.find_task_actors(t2.id) == [], \
        f"落库的 wf_process_task_actor 必须零行: {await repo.find_task_actors(t2.id)}"
    assert "zhangsan" not in t2.actorIds and t2.actorId != "zhangsan", \
        f"当前操作人不在这一行的参与者里——兜底挂人＝伪造一条他不该收到的待办: {t2.actorIds}"

    # token 没被丢掉：实例照旧停在"进行中"，等 addTaskActor/transfer 补人或 flow.admin 逃生
    inst = await repo.find_instance_by_id(iid)
    assert int(inst.state) == 10, f"实例应停在进行中（既不丢令牌也不办结）: {inst.state}"

    # 用户可感知面：zhangsan 的待办列表里**查不到**这一行（伪造待办的形状没了）
    todo = await facade.flow("processTask/todoList",
                             {"operator": "zhangsan", "pageNum": 1, "pageSize": 50})
    names = [row.get("taskName") for row in (todo.get("data") or {}).get("rows") or []]
    assert "task2" not in names, f"伪造待办又回来了: {names}"

    # 码 3（PROCESS_TASK_START）照发（"新待办产生"这一事实成立），但载荷 actors 为空——事件侧不臆造人
    starts = [e for e in events if e.type is EventType.PROCESS_TASK_START and int(e.taskId) == int(t2.id)]
    assert len(starts) == 1, f"零参与者行仍要发一次码 3: {[(e.taskId, e.taskName) for e in events]}"
    assert list(starts[0].actors) == [], f"码 3 载荷不得带上臆造的参与者: {starts[0].actors}"


@pytest.mark.asyncio
async def test_i142_hr1_zero_actor_row_is_not_executable_and_does_not_reenter_create():
    """硬结论 1 的负腿 ＋ 死循环复核：零参与者行是"**看得见、办不动**"的显性堵点。
    ① 当前操作人办不动它（旧兜底形状下他能一键办掉一个没指派给他的节点——那才是真危害）；
    ② 办失败**不产生第二行**、也不重入建单（自动推进/会签计数那类逻辑不被零参与者行反复唤醒）。"""
    _eng, repo, facade, _events, _reg = _i141g9_harness()
    nodes, edges = _i142_chain([{"id": "task2", "type": "snaker:task",
                                 "text": {"value": "没人可派的审批"}, "properties": {}}])
    iid = await _i142_start(facade, "i142-hr1-not-executable", nodes, edges)
    t2 = (await repo.find_doing_tasks(iid))[0]

    r = await facade.flow("processTask/execute",
                          {"processTaskId": str(t2.id), "operator": "zhangsan", "submitType": 1})
    assert r["code"] != 0, f"零参与者行不该被非参与者办掉（兜底回来就会红在这一格）: {r}"
    assert "not allowed" in str(r.get("msg", "")), f"报错应是参与者判据: {r}"

    after = await repo.find_doing_tasks(iid)
    assert [t.id for t in after] == [t2.id], f"失败路径不得再建一行/重入建单: {after}"
    assert await repo.find_task_actors(t2.id) == [], \
        "办失败不得顺手把人补进参与者（那等于把兜底换个位置塞回来）"
    assert int((await repo.find_instance_by_id(iid)).state) == 10, "实例仍停在进行中"


@pytest.mark.asyncio
async def test_i142_hr1_empty_operator_still_leaves_a_row_not_the_zero_row_black_hole():
    """§6.1 死锁黑洞的**另一半**（上一笔 commit 的注释里那支 `if not fallback: return`）：
    操作人也为空时旧形状"**既不建行也不推进**" ⇒ 实例停在 state=10、库里一行可办行都没有，
    用户在任何列表里都看不见这一格，只能靠对账捞回来。撤兜底后必须是"**有一行、行上没人**"。

    走**引擎直用**（不经门面）：门面的 `_startAndExecute` 会对发起后的 DOING 行补
    `add_task_actor(当前操作人)` 再自动办理（applicant 申请腿机制，八栈同形、本轮不动），
    经它发起就把这一支照不到了；同理中间节点直接挂在 start 后面也会被那层机制补人。"""
    eng, repo, facade, _events, _reg = _i141g9_harness()
    nodes = [_I142_START_NODE,
             {"id": "task2", "type": "snaker:task", "text": {"value": "没人可派也没操作人"},
              "properties": {}},
             _I142_END_NODE]
    edges = [{"id": "e1", "sourceNodeId": "start", "targetNodeId": "task2", "properties": {}},
             {"id": "e2", "sourceNodeId": "task2", "targetNodeId": "end", "properties": {}}]
    r = await facade.flow("processDefine/deploy",
                          {"content": json.dumps({"name": "i142-hr1-no-operator",
                                                  "displayName": "无操作人零参与者", "type": "approval",
                                                  "nodes": nodes, "edges": edges})})
    assert r["code"] == 0, r
    did = int(r["data"]["processDefineId"])

    inst = await eng.start_process_instance_by_id(did, "", {})
    assert inst is not None and int(inst.state) == 10, f"实例应停在进行中: {inst.state}"
    doing = await repo.find_doing_tasks(inst.id)
    assert [t.taskName for t in doing] == ["task2"], \
        f"操作人为空也必须留一行 DOING（旧形状这里 return ⇒ 零可办行的死锁黑洞）: {doing}"
    assert list(doing[0].actorIds) == [] and await repo.find_task_actors(doing[0].id) == [], \
        f"这一行不挂人、也不落空串归属值: {doing[0].actorIds}"


@pytest.mark.asyncio
async def test_i142_hr1_zero_actor_row_blocks_join_without_reentry():
    """任务里点名的"别把状态机弄成死循环"那一问：fork 出两条边，一条有人办、一条**零参与者**，
    办结有人那条后 ⇒
      · JOIN **不前推**（`find_doing_tasks` 非空，零参与者行自己就是那条 DOING，与 java 同形）；
      · 不重入建单（join 之后的节点没被建出、任务行总数不涨、码 3 不重复 fire）。
    即这一行是"卡住但安静"的堵点，不是自动重入的引信——推进的唯一驱动是 `execute_process_task`，
    而零参与者行谁也办不动（`ProcessTask.is_allowed` 对空集恒 False）。

    走引擎直用（`start_process_instance_by_id` ＋ `execute_process_task`），
    免得门面的发起自动申请腿把那层补人机制混进来。"""
    eng, repo, facade, events, _reg = _i141g9_harness()
    nodes = [
        _I142_START_NODE,
        {"id": "fork", "type": "snaker:fork", "text": {"value": "并行"}, "properties": {}},
        {"id": "tok", "type": "snaker:task", "text": {"value": "有人办"},
         "properties": {"assignee": "leader"}},
        {"id": "tempty", "type": "snaker:task", "text": {"value": "没人办"}, "properties": {}},
        {"id": "join", "type": "snaker:join", "text": {"value": "汇合"}, "properties": {}},
        {"id": "after", "type": "snaker:task", "text": {"value": "汇合之后"},
         "properties": {"assignee": "boss"}},
        _I142_END_NODE,
    ]
    pairs = [("start", "fork"), ("fork", "tok"), ("fork", "tempty"),
             ("tok", "join"), ("tempty", "join"), ("join", "after"), ("after", "end")]
    edges = [{"id": f"e{i}", "sourceNodeId": a, "targetNodeId": b, "properties": {}}
             for i, (a, b) in enumerate(pairs, start=1)]
    r = await facade.flow("processDefine/deploy",
                          {"content": json.dumps({"name": "i142-hr1-join", "displayName": "并行含零参与者",
                                                  "type": "approval", "nodes": nodes, "edges": edges})})
    assert r["code"] == 0, r
    did = int(r["data"]["processDefineId"])

    inst = await eng.start_process_instance_by_id(did, "zhangsan", {})
    doing = {(t.taskName): t for t in await repo.find_doing_tasks(inst.id)}
    assert set(doing) == {"tok", "tempty"}, f"fork 两臂都该建行: {sorted(doing)}"
    assert list(doing["tempty"].actorIds) == [], f"零参与者臂不挂人: {doing['tempty'].actorIds}"
    rows_before, starts_before = len(repo._tasks), sum(
        1 for e in events if e.type is EventType.PROCESS_TASK_START)

    inst2 = await eng.execute_process_task(doing["tok"].id, "leader", {"submitType": 1})
    left = await repo.find_doing_tasks(inst2.id)
    assert [t.taskName for t in left] == ["tempty"], f"有人那条办完后只剩零参与者那条卡着: {left}"
    all_rows = await repo.find_history_tasks(inst2.id)
    assert not [t for t in all_rows if t.taskName == "after"], \
        "JOIN 不得因为『另一臂没人』就当前推进（卡住是设计如此），更不得反复重入建 after 行"
    assert len(repo._tasks) == rows_before, \
        f"任务行不得增长（重入建单的唯一可见痕迹）: {rows_before} → {len(repo._tasks)}"
    assert sum(1 for e in events if e.type is EventType.PROCESS_TASK_START) == starts_before, \
        "重入建单会重复 fire 码 3——不得出现"
    assert int(inst2.state) == 10, f"实例停在进行中（显性堵点，不是黑洞）: {inst2.state}"


@pytest.mark.asyncio
async def test_i142_hr1_zero_actor_countersign_still_creates_exactly_one_row():
    """§6.1 表第一行的"含会签"那一半：会签节点（PARALLEL／SEQUENTIAL）参与者为空时
    **同样建一行**、`perform_type` 仍是 1、参与者空集。
    ⚠️ 形状对照：java `createCountersignTasks` 在空 list 上并不等价——PARALLEL 那支 for 循环
    吃空列表 ⇒ **零行**（§6.1 禁的死锁形状），SEQUENTIAL 那支 `actorIds.get(0)` ⇒
    IndexOutOfBoundsException（打断建单）。两档都是**基准自身缺口**（案文 §1 A 段同款），
    本栈按 §6.1 正文"任务类含会签 ⇒ 建待办行、参与者可以为零"落一行不挂人，不跟基准一起炸。"""
    for ct in ("PARALLEL", "SEQUENTIAL"):
        _eng, repo, facade, _events, _reg = _i141g9_harness()
        nodes, edges = _i142_chain([{"id": "cs", "type": "snaker:task",
                                     "text": {"value": f"没人可签的会签（{ct}）"},
                                     "properties": {"performType": 1, "countersignType": ct}}])
        iid = await _i142_start(facade, f"i142-hr1-cs-{ct}", nodes, edges)

        doing = await repo.find_doing_tasks(iid)
        assert [t.taskName for t in doing] == ["cs"], f"[{ct}] 零参与者会签仍要留一行: {doing}"
        assert len(doing) == 1, f"[{ct}] 只建一行，不得按空成员列表铺开: {doing}"
        t = doing[0]
        assert int(t.performType) == 1, \
            f"[{ct}] 会签档不许因为零参与者掉回普通任务（perform_type 列要真）: {t.performType}"
        assert list(t.actorIds) == [] and await repo.find_task_actors(t.id) == [], \
            f"[{ct}] 参与者必须是空集（不是 [''] 也不是 ['zhangsan']）: {t.actorIds}"
        assert int((await repo.find_instance_by_id(iid)).state) == 10, f"[{ct}] 实例停在进行中"


@pytest.mark.asyncio
async def test_i142_62_clazz_unregistered_and_clazz_empty_are_two_separate_logs(caplog):
    """§6.2 第 2 条的"分档"：
    · 档 1 clazz 非空但**未注册** ⇒ 日志带 clazz 值、文案指"未注册处理器"；
    · 档 2 clazz **空串／属性缺失** ⇒ 日志指"未配置 clazz"（定义配错），文案里不该有"未注册"；
    两档共同点：**照常落 DONE 历史行 + 令牌继续流转**，且记录类行仍**不发码 3**。
    改前只有 `elif clazz:` 一档 ⇒ 档 2 静默零日志（这一格照的就是这个）。"""
    import logging

    async def _run(node_id: str, props: dict, flow_name: str):
        _eng, repo, facade, events, _reg = _i141g9_harness()   # 不注册任何 custom handler
        nodes, edges = _i142_chain([{"id": node_id, "type": "snaker:custom",
                                     "text": {"value": "留痕节点"}, "properties": props}])
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            iid = await _i142_start(facade, flow_name, nodes, edges)
        inst = await repo.find_instance_by_id(iid)
        row = next((t for t in inst.tasks if t.taskName == node_id), None)
        warnings = [rec.getMessage() for rec in caplog.records if rec.levelno >= logging.WARNING
                    and "custom 节点" in rec.getMessage()]
        return row, inst, events, warnings

    # ── 档 1：clazz 非空但未注册 ──────────────────────────────────────────────
    row, inst, events, warns = await _run("c1", {"clazz": "com.example.NoSuchHandler"},
                                          "i142-62-unregistered")
    assert any("未注册处理器" in m and "clazz=com.example.NoSuchHandler" in m for m in warns), \
        f"档 1 应有可诊断到 clazz 值的未注册日志: {warns}"
    assert not any("未配置 clazz" in m for m in warns), f"档 1 不该被报成配置缺失: {warns}"
    assert row is not None, f"clazz 解析不了仍要落历史行（不得丢留痕）: {inst.tasks}"
    assert int(row.taskState) == 20, f"记录类只落 DONE 行: {row.taskState}"
    assert int(inst.state) == 20, f"令牌照旧续流到 end: {inst.state}"
    assert not [e for e in events if e.type is EventType.PROCESS_TASK_START
                and int(e.taskId) == int(row.id)], "记录类行仍不得发码 3"

    # ── 档 2a：clazz 为空串 ─────────────────────────────────────────────────
    row, inst, events, warns = await _run("c2", {"clazz": ""}, "i142-62-empty-clazz")
    assert any("未配置 clazz" in m for m in warns), f"档 2（空串）应有独立一档日志: {warns}"
    assert not any("未注册处理器" in m for m in warns), \
        f"档 2 不该跟档 1 合成同一句话（两病要分别可诊断）: {warns}"
    assert row is not None and int(row.taskState) == 20 and int(inst.state) == 20, \
        f"档 2 同样照常落历史行＋续流: row={row and row.taskState} inst={inst.state}"
    assert not [e for e in events if e.type is EventType.PROCESS_TASK_START
                and int(e.taskId) == int(row.id)], "记录类行仍不得发码 3"

    # ── 档 2b：clazz 属性整个缺失 ───────────────────────────────────────────
    row, inst, _ev, warns = await _run("c3", {}, "i142-62-missing-clazz")
    assert any("未配置 clazz" in m for m in warns), f"缺失档与空串档同判（都算定义没配）: {warns}"
    assert row is not None and int(row.taskState) == 20 and int(inst.state) == 20, \
        f"缺失档同样落行＋续流: row={row and row.taskState} inst={inst.state}"


@pytest.mark.asyncio
async def test_i142_62_custom_handler_own_exception_still_propagates():
    """负向对照：§6.2 第 2 条的豁免**只覆盖"clazz 解析不了"**（配错形状），
    处理器**自身执行失败**（解析到了、跑炸了）照旧外抛——不许顺手降级成"记日志继续"，
    那是业务错误，吞掉就把数据写脏了。"""
    from jeeflow.extensions import ICustomHandler

    class _Boom(ICustomHandler):
        def handle(self, node, instance, operator, vars_):
            raise RuntimeError("handler-boom")

    eng, repo, facade, _events, reg = _i141g9_harness()
    reg.register_custom("com.example.Boom", _Boom())
    nodes, edges = _i142_chain([{"id": "c1", "type": "snaker:custom",
                                 "text": {"value": "会炸的留痕节点"},
                                 "properties": {"clazz": "com.example.Boom"}}])
    r = await facade.flow("processDefine/deploy",
                          {"content": json.dumps({"name": "i142-62-boom", "displayName": "处理器炸",
                                                  "type": "approval", "nodes": nodes, "edges": edges})})
    assert r["code"] == 0, r
    did = int(r["data"]["processDefineId"])

    # 引擎直用：异常必须原样外抛。注意记录类节点在 apply **之后**，
    # 所以要把申请腿办掉才会真的走到 c1（`start_process_instance_by_id` 只建到 apply 那行）。
    inst = await eng.start_process_instance_by_id(did, "zhangsan", {})
    apply_task = (await repo.find_doing_tasks(inst.id))[0]
    with pytest.raises(RuntimeError, match="handler-boom"):
        await eng.execute_process_task(apply_task.id, "zhangsan", {"submitType": 1})

    # 门面腿：不外抛但**绝不报成功**。出口 msg 是固定文案 `流程处理失败`，handler 自己写的原文
    # 只进日志与错误对象（issues/137 §3-1 · spec 06-facade §2.12：集成方 provider 写的原文属
    # **内部实现细节**，判据按「谁写的这段文案」判——不是引擎写的就不外透）。
    # ⚠️ 改前这一格断言的是 `"handler-boom" in msg`，等于把泄漏机制当成了判据的一部分；
    # 本格的**真实意图**是"处理器自身炸不得被吞成成功"，那由 `code != 0` 与上面 :7016 的
    # **引擎直用腿**（`pytest.raises(RuntimeError, match="handler-boom")`，打的是引擎 API 不是门面出口，
    # 不受 §2.12 约束、原样保留）共同承担，两者改后都仍成立 ⇒ 这是**改读法**不是改判据
    # （owner 2026-10-02 拍板；java 基准同形状也出固定文案，`isForeignDetail` 第 5 条：
    #  抛出点 `com.example.Boom` 不在引擎主包 ⇒ 判内部）。
    r2 = await facade.flow("processInstance/startAndExecute",
                           {"processDefineId": did, "operator": "zhangsan"})
    assert r2["code"] != 0, f"处理器自身炸不得被吞成成功: {r2}"
    assert str(r2.get("msg", "")) == "流程处理失败", \
        f"集成方 handler 的原文属内部细节，出口只给固定文案（spec 06 §2.12）: {r2}"


# ═══ Test 142 B 批：任务参与者写侧归属值归一（契约 06 §2.11）═══════════════════════
#
# 立法逐字依据（jeeflow-doc/docs/spec/06-facade.md §2.11，只读；普查底稿 issues/142 §2 B 表
# python 那一行）——owner 2026-09-30 原话：「八栈一起收：两形同判据＋写侧兜底＋trim＋哨兵」。
# §2.11 把 §2.10（抄送侧，本栈已由 `spi.normalize_cc_actors` 那一枚单点落地）的四点实现要求
# **逐字搬到任务侧**，覆盖写点：
#   `processTask/addCandidate`＋`surrogate`／`processTask/transfer` 的 fromActor·toActor／
#   `f_nextNodeOperator`·`tf_nextNodeOperator`（逗号串与数组两形）／`updateCCStatus` 的 operator／
#   两仓 `add_task_actor` 的仓储写侧兜底。
# 四条硬要求：① 两层都挡（漏斗＋仓储写侧，只修门面则绕过门面直连仓储的调用方照样灌空值）；
#   ② 落库与比较一律取 **trim 后的值**；③ 空入参档**沿用本栈既有的"缺参数"错误信封**
#   （`processTaskId/actorIds 缺失`，不新造码/文案）；④ 反向哨兵 `"0"` 不得被丢掉，且
#   **严禁语言自带的假值判据**（python 的 `if not x` 会吃掉 `'0'`）——判空一律 `str(x).strip() == ""`。
# 主键类参数**另判一档**：`processTaskId` 缺失/空串必须响亮报错，不得拿 `''`/`0` 当 id 落库。
# 判据单点：spec 明文「不要再抄第二份」——本栈把 §2.10 那一枚改名成通用的 `normalize_actors`，
# cc 支继续走**同一个对象**（`normalize_cc_actors` 留作别名），全仓不得出现第二把尺子。

_I142B_TABLE_DDL = (
    # §2.11 的 SQL 通道用到的两张表（列名逐字对齐 tests/schema/schema-mysql.sql 与 base.py 的 SQL）；
    # 形状照 _cc141_sql_repo 的先例：真 SQLite ＋真 JdbcRepository，不用内存假仓。
    "CREATE TABLE wf_process_task_actor (id INTEGER PRIMARY KEY, process_task_id INTEGER,"
    " actor_id TEXT, create_time TEXT, create_user TEXT)",
    "CREATE TABLE wf_process_cc_instance (id INTEGER PRIMARY KEY, process_instance_id INTEGER,"
    " actor_id TEXT, state INTEGER DEFAULT 0, create_time TEXT, create_user TEXT,"
    " update_time TEXT, update_user TEXT)",
)


def _i142b_sql_repo():
    """真 SQLite ＋真 `JdbcRepository`——§2.11 的"两仓同一判据"必须在 SQL 通道也钉住
    （只钉内存仓＝spec §2.10 要求①明写的"只修一层不算修完"，也复现 issues/117 场景 27
    那把"同栈两仓两个答案"的尺子）。"""
    from jeeflow.repository.base import JdbcRepository
    raw = sqlite3.connect(":memory:")
    for ddl in _I142B_TABLE_DDL:
        raw.execute(ddl)
    return raw, JdbcRepository(_SqliteAdapter(raw), _TestIDGen())


def _i142b_actor_rows(raw, task_id) -> list[str]:
    """actor 表取证：某任务落库的**真实** actor_id 行（按插入序）——不看返回码，只看库里的行。"""
    return [r[0] for r in raw.execute(
        "SELECT actor_id FROM wf_process_task_actor WHERE process_task_id=?"
        " ORDER BY id ASC", (task_id,)).fetchall()]


def _i142b_cc_states(raw, instance_id):
    """cc 表取证：[(actor_id, state)]，供 updateCCStatus 的归一腿与"空 operator 不打脏行"两档。"""
    return [(r[0], r[1]) for r in raw.execute(
        "SELECT actor_id, state FROM wf_process_cc_instance WHERE process_instance_id=?"
        " ORDER BY id ASC", (instance_id,)).fetchall()]


# ── 判据点：单点复用，不许有第二份 ──────────────────────────────────────────────

def test_i142_b1_single_point_is_reused_not_recopied():
    """§2.11 收尾那句「复用 §2.10 已落地的那一枚单点，不要再抄第二份」钉成断言：
    cc 支（引擎漏斗 `parse_cc_actors`）与任务支（门面 `_to_actor_ids`）必须**逐输入同答案**，
    且 `normalize_cc_actors` 就是 `normalize_actors` 同一个对象（改名而非另立）。"""
    from jeeflow.engine import parse_cc_actors
    from jeeflow.spi import normalize_actors, normalize_cc_actors

    assert normalize_cc_actors is normalize_actors, \
        "cc 支必须继续走同一枚（别名同一对象），另立一份判据迟早分叉（spec §2.11 点名 php 实证）"

    matrix = [None, [], (), "", "   ", " , ", "a,,b", "a,", ["a", "", "  ", None],
              [1, " 1 "], ["0"], "0", [" 8123 ", "8123"], ("a", "a"), {"a": 1}]
    for raw in matrix:
        expect = normalize_actors(raw)
        assert parse_cc_actors(raw) == expect, f"cc 漏斗与单点分叉: {raw!r}"
        assert JeeflowFacade._to_actor_ids(raw) == expect, f"门面任务腿与单点分叉: {raw!r}"
        assert JeeflowFacade._to_str_list(raw) == expect, f"旧私有名指向了另一把尺子: {raw!r}"


def test_i142_b1_array_leg_criteria_trim_drop_fold_and_zero_sentinel():
    """本栈普查实测的病灶那一格（issues/142 §2 B 表 python 行）：`_to_str_list` 数组腿是
    `[str(x) for x in v]` ⇒ **不 trim、不丢空、None 串化成字符串 "None"**，只有逗号串腿才
    strip＋过滤。改后两形**同一判据**：逐元素 trim、空串/纯空白/None 丢弃、同一次调用折叠、
    数字元素 str() 后仍 trim；**"0" 这类"看起来像空"的正常 id 不得丢**。"""
    from jeeflow.spi import normalize_actors, normalize_actor_value

    assert normalize_actors(["a", "", "  ", None, "  b "]) == ["a", "b"]
    assert normalize_actors([None]) == []
    assert normalize_actors(["", ""]) == []
    assert normalize_actors(["\t", "\n "]) == [], "纯空白与空串同档（§2.10 逐字搬到任务侧）"
    assert "None" not in normalize_actors([None, "x"]), "None 绝不能再被串化成字符串 \"None\""
    assert normalize_actors([123, " 123 "]) == ["123"], "数字元素 str() 后仍 trim＋折叠"
    assert normalize_actors([" 0 ", "0", "00", "a"]) == ["0", "00", "a"], \
        "反向哨兵（要求④）：'0'/'00'/'a' 是三个不同的人，判重不得松散、判空不得吃 '0'"
    assert normalize_actors(("a", "a", " a ")) == ["a"], "同一次调用内折叠重复（trim 后才判得准）"
    assert normalize_actors(["zhangsan", None, "lisi"]) == ["zhangsan", "lisi"]

    # 逗号串腿（原本只有这条腿是对的）——两形同判据
    assert normalize_actors("") == []
    assert normalize_actors("   ") == []
    assert normalize_actors(" , ") == []
    assert normalize_actors("a,,b") == ["a", "b"]
    assert normalize_actors("a,") == ["a"]
    assert normalize_actors(" 8123 , 8123 ") == ["8123"], "trim 判等：' 8123 ' 与 '8123' 同一个人"
    assert normalize_actors("0") == ["0"]

    assert normalize_actors(None) == []
    assert normalize_actors([]) == []
    assert normalize_actors(0) == ["0"], "标量 0 也是一个人，不得被假值判据折成没填"

    # 单个归属值（transfer 的 fromActor/toActor、updateCCStatus 的 operator）
    assert normalize_actor_value(" 8123 ") == "8123"
    assert normalize_actor_value("  7101") == "7101"
    assert normalize_actor_value(None) == ""
    assert normalize_actor_value("   ") == ""
    assert normalize_actor_value("\t") == ""
    assert normalize_actor_value("0") == "0", "哨兵：单个值 '0' 不是空"
    assert normalize_actor_value(0) == "0", "哨兵：int 0 归一成 '0'，不得当成没填"


# ── 门面腿：addCandidate / surrogate ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_i142_b1_facade_add_candidate_and_surrogate_two_forms_same_ruler():
    """要求①②④（addCandidate 与 surrogate 同体两条 action）：数组里的空串/纯空白/None 丢弃、
    有效项 trim 后落库、同次调用折叠；逗号串形同判据；**"0" 必须保住**。

    改前红在本格：`[" 8123 ", "", None, "8123"]` 走成 `[" 8123 ", "", "None", "8123"]`
    ——不 trim（与既有行判成两个人，把 issues/141 G2 那把写侧判重打穿）、空串落库、
    None 变成字符串 "None" 落进归属列（正是 issues/129 那族"空归属值读全库"的进水口）。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")
    tid = (await repo.find_doing_tasks(iid))[0].id
    assert await repo.find_task_actors(tid) == ["leader"]

    r = await facade.flow("processTask/addCandidate",
                          {"processTaskId": tid, "actorIds": [" 8123 ", "", "  ", None, "8123"]})
    assert r["code"] == 0, r
    assert await repo.find_task_actors(tid) == ["leader", "8123"], \
        f"trim＋丢空＋同次折叠：{await repo.find_task_actors(tid)}"

    r = await facade.flow("processTask/surrogate", {"processTaskId": tid, "actorIds": " 9101 ,, 9102 ,"})
    assert r["code"] == 0, r
    assert await repo.find_task_actors(tid) == ["leader", "8123", "9101", "9102"], \
        "逗号串与数组两形同判据（只修一条腿＝跨形分叉）"

    r = await facade.flow("processTask/addCandidate", {"processTaskId": tid, "actorIds": ["0", " 0 "]})
    assert r["code"] == 0, r
    assert await repo.find_task_actors(tid) == ["leader", "8123", "9101", "9102", "0"], \
        f'哨兵 "0" 必须落库且只一行：{await repo.find_task_actors(tid)}'

    # 原人保留（issues/115 回归：加签是"只追加不清空"，本批不得顺手改语义）
    r = await facade.flow("processTask/execute",
                          {"processTaskId": tid, "operator": "leader", "submitType": 1})
    assert r["code"] == 0, r


@pytest.mark.asyncio
async def test_i142_b1_facade_add_candidate_empty_after_drop_is_the_missing_params_case():
    """要求③：`actorIds` 丢完为空 ⇒ 与本栈**既有的"空 actorIds"档同判**
    （99999999 ＋ msg 含 `actorIds 缺失`，沿用 `_taskAddActor` 现成信封，不新造码/文案），
    且零副作用——参与者集合一条没变。

    改前红在本格：`[""]` 经旧 `_to_str_list` 得到**非空 list** ⇒ 过了判空闸门、进了仓储写侧
    落一条 `actor_id=''` 的行 ⇒ "报成功但灌进脏值"。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")
    tid = (await repo.find_doing_tasks(iid))[0].id

    for bad in ([""], ["  "], ["", "  "], ["\t"], [None], [None, ""], "", "   ", " , "):
        r = await facade.flow("processTask/addCandidate", {"processTaskId": tid, "actorIds": bad})
        assert r["code"] == 99999999, f"{bad!r} 应与空 actorIds 同档报错: {r}"
        assert "actorIds 缺失" in r["msg"], f"{bad!r} 沿用既有文案: {r}"
    assert await repo.find_task_actors(tid) == ["leader"], "空档必须零副作用"

    # 既有那两档不破（回归）：真空 list／缺键同样报同一句
    for bad_args in ({"actorIds": []}, {}):
        r = await facade.flow("processTask/addCandidate", {"processTaskId": tid, **bad_args})
        assert r["code"] == 99999999 and "actorIds 缺失" in r["msg"], (bad_args, r)


@pytest.mark.asyncio
async def test_i142_b1_facade_process_task_id_is_the_other_tier():
    """主键**另判一档**（§2.11 末段）：归属值为空 ⇒ 丢弃；`processTaskId` 缺失/空串 ⇒ 响亮报错，
    严禁拿 `''`/`0` 当 id 往下落库（静默接受会把脏数据钉进表里）。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")
    tid = (await repo.find_doing_tasks(iid))[0].id

    for args in ({"actorIds": ["x"]}, {"processTaskId": "", "actorIds": ["x"]},
                 {"processTaskId": "   ", "actorIds": ["x"]}, {"processTaskId": 0, "actorIds": ["x"]},
                 {"processTaskId": None, "actorIds": ["x"]}, {"processTaskId": "abc", "actorIds": ["x"]}):
        r = await facade.flow("processTask/addCandidate", args)
        assert r["code"] == 99999999, f"主键缺失/非法必须报错: {args} → {r}"
        assert "processTaskId" in r["msg"], f"沿用既有'缺参数'文案: {args} → {r}"
        r = await facade.flow("processTask/surrogate", args)
        assert r["code"] == 99999999 and "processTaskId" in r["msg"], (args, r)

    # 零参与者行也没被顺手建出来（'id=0 落库'那一档）
    assert await repo.find_task_actors(tid) == ["leader"]
    assert await repo.find_task_actors(0) == []


# ── 门面腿：transfer ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_i142_b1_transfer_normalizes_from_and_to_actor():
    """§2.11 写点表第 2 行：transfer 的 `fromActor`/`toActor` **归一后再用**（普查：各栈只
    `isBlank` 判必填、存的是未 trim 的原值）。三处都得吃到 trim：
    ① 权限比较（` operator != from_actor`）② 摘人/加人的值③ 留痕（tf_transferTo／文案／账本）。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")
    tid = (await repo.find_doing_tasks(iid))[0].id

    r = await facade.flow("processTask/transfer",
                          {"processTaskId": tid, "fromActor": " leader ", "toActor": " lisi ",
                           "reason": " 出差一周 ", "operator": " leader "})
    assert r["code"] == 0, f"归一后 fromActor 与 operator 是同一个人，不该判成越权: {r}"
    assert await repo.find_task_actors(tid) == ["lisi"], \
        f"摘/加都取 trim 后的值: {await repo.find_task_actors(tid)}"
    task = await repo.find_task_by_id(tid)
    assert task.variables["tf_transferTo"] == "lisi", task.variables
    assert "leader 转办给 lisi" in task.variables["tf_approvalComment"], task.variables
    hop = task.variables["tf_transferHistory"][-1]
    assert hop["fromActor"] == "leader" and hop["toActor"] == "lisi", hop

    # 哨兵：toActor="0"（含 int 0）是正常 id，不得被假值判据折成"必填"报错
    for zero_like in ("0", 0, " 0 "):
        eng2, repo2 = setup()
        facade2 = JeeflowFacade(eng2, repo2, MemoryExtRepository())
        did2 = await _deploy(facade2, "01-simple.json")
        iid2 = await _start(facade2, did2, "zhangsan")
        tid2 = (await repo2.find_doing_tasks(iid2))[0].id
        r = await facade2.flow("processTask/transfer",
                               {"processTaskId": tid2, "fromActor": "leader",
                                "toActor": zero_like, "operator": "leader"})
        assert r["code"] == 0, f'"0" 不是空值，不该走"toActor 必填"档: {zero_like!r} → {r}'
        assert await repo2.find_task_actors(tid2) == ["0"], \
            f"落库值归一（int 0 ⇒ '0'）: {await repo2.find_task_actors(tid2)}"

    # 空值档仍是既有文案（要求③：不新造码/文案）
    for bad_args, keyword in (({"fromActor": "  ", "toActor": "lisi", "operator": "leader"}, "fromActor 必填"),
                              ({"fromActor": "", "toActor": "lisi", "operator": "leader"}, "fromActor 必填"),
                              ({"fromActor": "leader", "toActor": "   ", "operator": "leader"}, "toActor 必填"),
                              ({"fromActor": "leader", "toActor": "\t", "operator": "leader"}, "toActor 必填"),
                              ({"fromActor": "leader", "toActor": "lisi"}, "operator 必填"),
                              ({"fromActor": "leader", "toActor": "lisi", "operator": "  "}, "operator 必填")):
        bad_args = {"processTaskId": tid, **bad_args}
        r = await facade.flow("processTask/transfer", bad_args)
        assert r["code"] == 99999999 and keyword in r["msg"], (bad_args, r)
    assert await repo.find_task_actors(tid) == ["0"] or await repo.find_task_actors(tid) == ["lisi"], \
        "负向档不得改动参与者"


# ── 消费腿：f_ / tf_nextNodeOperator 两形 ───────────────────────────────────────

@pytest.mark.asyncio
async def test_i142_b1_next_node_operator_array_leg_same_ruler_as_comma_leg():
    """§2.11 写点表第 3 行（`engine._resolve_actors` 的 next_op 腿，普查实读同款第二把尺子）：
    逗号串腿 trim＋丢空，**数组腿 `[str(a) for a in next_op]` 不 trim、不丢空、None 变 "None"**。
    改后两形同判据（数组元素不得被静默丢弃或串化成类型名/`"None"`）。"""
    for value, expect in (
            (["BOSS1", "", "  ", None, " BOSS2 "], ["BOSS1", "BOSS2"]),
            (["BOSS1", "BOSS1", " BOSS1 "], ["BOSS1"]),
            ("BOSS1,, BOSS2,", ["BOSS1", "BOSS2"]),
            (["0"], ["0"]),
            ("0", ["0"]),
            ([1, " 1 ", 2], ["1", "2"]),
    ):
        eng, repo = setup()
        facade = JeeflowFacade(eng, repo, MemoryExtRepository())
        define_id = await _deploy(facade, "01-simple.json")
        inst = await eng.start_process_instance_by_id(define_id, "zhangsan")
        apply_task = (await repo.find_doing_tasks(inst.id))[0]
        await repo.add_task_actor(apply_task.id, ["zhangsan"])
        await eng.execute_process_task(apply_task.id, "zhangsan",
                                       {"submitType": 0, "tf_nextNodeOperator": value})
        doing = await repo.find_doing_tasks(inst.id)
        assert len(doing) == 1, f"办理后应有 task1 待办: {doing}"
        assert await repo.find_task_actors(doing[0].id) == expect, \
            f"nextNodeOperator {value!r} 两形同判据: {await repo.find_task_actors(doing[0].id)}"


@pytest.mark.asyncio
async def test_i142_b1_start_leg_f_next_node_operator_is_the_same_ruler():
    """发起腿 `f_nextNodeOperator`（门面转成 tf_ 后进同一支消费腿）：数组里的空元素/None
    不得落进归属列，"0" 必须保住。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": define_id, "operator": "zhangsan",
                           "f_nextNodeOperator": [" 9001 ", "", None, "9001", "0"]})
    assert r["code"] == 0, r
    iid = int(r["data"]["processInstanceId"])
    doing = await repo.find_doing_tasks(iid)
    assert doing, "发起后应有 task1 待办"
    assert await repo.find_task_actors(doing[0].id) == ["9001", "0"], \
        f"{await repo.find_task_actors(doing[0].id)}"


@pytest.mark.asyncio
async def test_i142_b1_next_node_operator_all_blank_falls_back_to_node_assignee():
    """全空档：`tf_nextNodeOperator` 丢完为空 ⇒ 与"没带这个参数"同形 ⇒ 回落到节点 assignee
    （01-simple 的 task1 是 `leader`）。旧形状把 `[""]` 当成有效指派 ⇒ 落一条 `actor_id=''`。"""
    for value in ([""], ["  ", None], "", "   ", ["\t"]):
        eng, repo = setup()
        facade = JeeflowFacade(eng, repo, MemoryExtRepository())
        define_id = await _deploy(facade, "01-simple.json")
        inst = await eng.start_process_instance_by_id(define_id, "zhangsan")
        apply_task = (await repo.find_doing_tasks(inst.id))[0]
        await repo.add_task_actor(apply_task.id, ["zhangsan"])
        await eng.execute_process_task(apply_task.id, "zhangsan",
                                       {"submitType": 0, "tf_nextNodeOperator": value})
        doing = await repo.find_doing_tasks(inst.id)
        assert len(doing) == 1, f"空指派不得打断建单: {value!r}"
        assert await repo.find_task_actors(doing[0].id) == ["leader"], \
            f"空指派应回落 assignee（{value!r}）: {await repo.find_task_actors(doing[0].id)}"


@pytest.mark.asyncio
async def test_i142_b1_rollback_leg_uses_the_same_ruler():
    """`_sync_resolve_actors`（ROLLBACK 腿的 next_op 支）是普查点名的**同款第二把尺子**
    （`engine.py` 两处数组臂逐字一样），必须走同一枚单点。"""
    from jeeflow.engine import KEY_NEXT_NODE_OPERATOR
    eng, repo = setup()
    node = _i137b_node(load_flow(repo, "01-simple.json").content, "task1")
    for value, expect in (
            (["BOSS1", "", None, " BOSS1 "], ["BOSS1"]),
            ("BOSS2, ,BOSS3", ["BOSS2", "BOSS3"]),
            (["0"], ["0"]),
            # 全空档 ⇒ 与"没带这个参数"同形 ⇒ 回落节点 assignee（01-simple 的 task1 是 leader）。
            # 旧形状这一格返回 ['None', ''] ⇒ 两条 actor_id='None'/'' 的脏行直接落进归属列。
            ([None, ""], ["leader"]),
            ([" 9001 "], ["9001"]),
    ):
        inst = ProcessInstance(id=1, defineId=1, operator="zhangsan",
                               variables={KEY_NEXT_NODE_OPERATOR: value})
        assert eng._sync_resolve_actors(node, inst, "zhangsan") == expect, \
            f"退回腿与办理腿分叉: {value!r}"


# ── 仓储写侧兜底：两仓同一判据 ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_i142_b1_memory_repo_write_side_blocks_blanks_and_trims():
    """要求①（内存仓）：绕过门面/引擎**裸调** `add_task_actor` ⇒ 空串/纯空白/None 一律不落，
    落库值取 trim 后的串，同次调用与跨调用都判重。"""
    repo = MemoryRepository()
    await repo.add_task_actor(501, ["", "  ", "\t", None])
    assert await repo.find_task_actors(501) == [], "空值一律不落"
    await repo.add_task_actor(501, [" 8123 ", "", "8123", None])
    assert await repo.find_task_actors(501) == ["8123"], f"trim＋折叠: {await repo.find_task_actors(501)}"
    await repo.add_task_actor(501, ["8123"])
    assert await repo.find_task_actors(501) == ["8123"], "跨调用判重不吃空格"
    await repo.add_task_actor(501, ["0", "00", "a"])
    assert await repo.find_task_actors(501) == ["8123", "0", "00", "a"], "哨兵三人都保住"
    await repo.add_task_actor(502, " 9101 ,, 9102 ,")
    assert await repo.find_task_actors(502) == ["9101", "9102"], "裸调逗号串形同判据"


@pytest.mark.asyncio
async def test_i142_b1_sql_repo_write_side_blocks_blanks_and_trims():
    """要求①②（SQL 仓一路真 SQLite）：同一批入参在 SQL 通道给出**与内存仓相同**的答案，
    断言直读 `wf_process_task_actor` 的真实行。"""
    raw, sql = _i142b_sql_repo()
    await sql.add_task_actor(701, ["", "  ", "\t", None])
    assert _i142b_actor_rows(raw, 701) == [], "空值一律不落行"
    await sql.add_task_actor(701, [" 8123 ", "", None])
    assert _i142b_actor_rows(raw, 701) == ["8123"], "落库值取 trim 后的串"
    await sql.add_task_actor(701, ["8123"])
    assert _i142b_actor_rows(raw, 701) == ["8123"], "同一人不许因前后空格落两行"
    await sql.add_task_actor(701, [" 8123 ", "8123", "0", "00", "a"])
    assert _i142b_actor_rows(raw, 701) == ["8123", "0", "00", "a"], \
        f"同次调用折叠＋哨兵保住: {_i142b_actor_rows(raw, 701)}"
    await sql.add_task_actor(702, " 9101 ,, 9102 ,")
    assert _i142b_actor_rows(raw, 702) == ["9101", "9102"]


@pytest.mark.asyncio
async def test_i142_b1_two_repos_same_answer():
    """两仓同答案（issues/117 场景 27 那把尺子，spec §2.11 明写"含 createCcInstance 同款写侧兜底"）：
    同一批"带空值/带空格/像空"的入参逐对灌进内存仓与 SQL 仓，落出的 actor 集必须逐字相同——
    只修一边在这一格红。"""
    matrix = [[""], ["  "], ["a", ""], [" b "], ["b"], ["0"], [" 0 "], [None],
              ["c", "c", " c "], [123, " 123 "], ["0", "00"]]
    mem = MemoryRepository()
    raw, sql = _i142b_sql_repo()
    for i, actors in enumerate(matrix, start=1):
        await mem.add_task_actor(i, actors)
        await sql.add_task_actor(i, actors)
    mem_ans = {i: await mem.find_task_actors(i) for i in range(1, len(matrix) + 1)}
    sql_ans = {i: _i142b_actor_rows(raw, i) for i in range(1, len(matrix) + 1)}
    assert mem_ans == sql_ans, f"两仓判据分叉:\n 内存仓 {mem_ans}\n SQL 仓 {sql_ans}"
    assert mem_ans[1] == [] and mem_ans[2] == [] and mem_ans[8] == [], "空档两仓都是零行"
    assert mem_ans[3] == ["a"] and mem_ans[4] == ["b"] and mem_ans[5] == ["b"], "trim 判等两仓同尺"
    assert mem_ans[6] == ["0"] and mem_ans[7] == ["0"], "哨兵 0 两仓都保住"
    assert mem_ans[10] == ["123"], "数字元素 str() 后仍 trim 并折叠"
    assert mem_ans[11] == ["0", "00"], "'0' 与 '00' 是两个人（判重不得松散）"


@pytest.mark.asyncio
async def test_i142_b1_repo_write_side_rejects_blank_task_id():
    """主键另判一档也在**仓储写侧**落一层（绕过门面直连仓储的调用方同样不得把 `''`/`0` 当 id
    钉进表里）：两仓一律响亮报错。"""
    mem = MemoryRepository()
    raw, sql = _i142b_sql_repo()
    for bad_id in (None, "", "   ", 0, "0"):
        for label, repo in (("内存仓", mem), ("SQL 仓", sql)):
            with pytest.raises(ValueError) as ei:
                await repo.add_task_actor(bad_id, ["zhangsan"])
            assert "processTaskId" in str(ei.value), f"{label} 沿用主键缺失文案: {ei.value}"
    assert _i142b_actor_rows(raw, 0) == [] and await mem.find_task_actors(0) == []


# ── updateCCStatus 的 operator（同一枚尺子换到已读写侧） ────────────────────────

@pytest.mark.asyncio
async def test_i142_b1_update_cc_status_operator_normalized_and_blank_is_noop():
    """§2.11 写点表第 4 行：`updateCCStatus` 的 `operator` **入参归一后再比**——否则
    `" lisi "` 判成另一个人（已读打不上），而空 operator 会把 `state=1` 打到历史
    `actor_id=''` 的脏行上（issues/129 那族"空归属值"的读侧对偶）。"""
    eng, repo = setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy(facade, "01-simple.json")
    iid = await _start(facade, define_id, "zhangsan")
    await repo.create_cc_instance(iid, "zhangsan", "lisi")
    assert repo.cc_rows_for_test(iid)[0].state == 0

    r = await facade.flow("processInstance/updateCCStatus",
                          {"processInstanceId": iid, "operator": " lisi "})
    assert r["code"] == 0, r
    assert repo.cc_rows_for_test(iid)[0].state == 1, "归一后再比：' lisi ' 就是 lisi"

    # 写侧兜底（内存仓）：裸调仓储的空 operator 是 no-op，不把已读打到任何行上；有效值 trim 后再比
    mem2 = MemoryRepository()
    await mem2.create_cc_instance(61, "zhangsan", " 8123 ")
    assert [str(r) for r in mem2.cc_rows_for_test(61)] == ["8123"], "cc 写侧本就 trim（G10 已落）"
    for blank in ("", "   ", "\t", None):
        await mem2.update_cc_status(61, blank)
    assert mem2.cc_rows_for_test(61)[0].state == 0, "空 operator 不得打勾任何行"
    await mem2.update_cc_status(61, "8123 ")
    assert mem2.cc_rows_for_test(61)[0].state == 1, "内存仓写侧同样 trim 后再比"

    # 写侧兜底（SQL 仓）：历史脏行（actor_id=''）不得被空 operator 批量打勾
    raw, sql = _i142b_sql_repo()
    iid2 = 9_430_001
    await sql.create_cc_instance(iid2, "zhangsan", "lisi")
    raw.execute("INSERT INTO wf_process_cc_instance (id, process_instance_id, actor_id, state)"
                " VALUES (?,?,?,0)", (9_430_999, iid2, ""))
    assert ("", 0) in _i142b_cc_states(raw, iid2), "夹具：一行历史脏数据"
    for blank in ("", "   ", "\t", None):
        await sql.update_cc_status(iid2, blank)
    assert _i142b_cc_states(raw, iid2) == [("lisi", 0), ("", 0)], \
        f"空 operator 必须 no-op（脏行不被批量打勾）: {_i142b_cc_states(raw, iid2)}"
    await sql.update_cc_status(iid2, " lisi ")
    assert _i142b_cc_states(raw, iid2) == [("lisi", 1), ("", 0)], "SQL 仓写侧同样 trim 后再比"



# ═══ issues/141 G4 义务 2 · 未知节点档的可诊断日志（python 腿 · 批二 §3-5）═══════════════════
#
# 契约逐字（jeeflow-doc/docs/spec/02-flow-definition.md:113-124「类型键的三条义务」第 2 条）：
#   「未知档不得静默丢节点：类型不在表里时，必须**记一条可诊断日志（带节点 id 与实得类型串）**
#    再决定跳过，不允许"静默丢节点＋连带丢它的出边"。」
# 同文件 :107-111（owner 2026-10-01 二拍「子流程暂不进契约面」）：六栈**不补** snaker:subProcess 档，
# 设计器画出的子流程节点就靠这条未知档日志被显式暴露 ⇒ 日志不是装饰，是那条裁定唯一的可诊断面。
#
# 落点是**执行期**（`engine._execute_node` 那条 if/elif 走完没人认领的 else 支），不是解析期：
# 本栈 `parse_flow_model` 压根不按类型过滤节点（model.py 那张"表"只是常量族，没有 java
# `ModelParser` 的"查不到解析器就 continue"臂），节点一路留在模型里 ⇒ 未知类型只有执行令牌
# 撞上它时才可观测。挂在解析期反而会跟着每一次 start/execute 各打一遍（那才是刷屏）。
#
# 本轮**只加日志**：后半"指向被丢弃节点的边落穿停住、不许打崩办理"已由 issues/143 在 java/php/c#
# 落过，本栈本来就是"按 id 现查目标、查不到就停"那一派，行为已对 ⇒ 下面顺带钉住行为没漂。

_UNKNOWN_TYPE_MARKER = "不在类型表里"


def _unknown_type_warnings(records):
    """从 caplog.records 里只挑本条判据那句（其余 WARNING 是别处的诊断，比如 custom clazz 未注册）。"""
    return [r.getMessage() for r in records if _UNKNOWN_TYPE_MARKER in r.getMessage()]


@pytest.mark.asyncio
async def test_i141_g4_unknown_node_type_logs_one_diagnosable_warning(caplog):
    """未知档矩阵：每个实得类型串都**必须**留下一条同时带 nodeId 与 type 原串的日志。
    两要素各钉一半——只打 nodeId 查不出为什么被吞，只打 type 不知道是哪个节点。
    行为侧同钉：不建行、不推进、不打崩办理（spec/02 义务 2 后半＋issues/143 口径）。"""
    import logging

    vectors = [
        # (实得类型串, 为什么这一档必须被暴露)
        ("snaker:subProcess", "设计器实际输出的驼峰子流程串——owner 二拍暂不进契约面，全靠这条日志显影"),
        ("snaker:subprocess", "契约里那条小写档同样不在表里（查表大小写敏感，不许偷偷认）"),
        ("snaker:Task", "拼错大小写：不许再塌成 Custom/Task 任一档"),
        ("task", "裸名（没带 snaker: 前缀）不在本栈表里"),
        ("snaker:not-a-node", "纯杜撰"),
        ("", "type 缺失／空串"),
    ]
    for raw_type, why in vectors:
        _eng, repo, facade, _events, _reg = _i141g9_harness()
        nodes, edges = _i142_chain([{"id": "ghost", "type": raw_type,
                                     "text": {"value": "没人认领的节点"}, "properties": {}}])
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            iid = await _i142_start(facade, f"i141-g4-unknown-{raw_type or 'empty'}", nodes, edges)
        msgs = _unknown_type_warnings(caplog.records)

        assert len(msgs) == 1, \
            f"[{raw_type or '<空串>'}] 未知档应恰好留一条可诊断日志（{why}），实得 {len(msgs)} 条: {msgs}"
        m = msgs[0]
        assert "nodeId=ghost" in m, f"[{raw_type}] 日志必须带节点 id，否则不知道哪个节点被吞: {m}"
        assert f"type={raw_type}" in m, \
            f"[{raw_type}] 日志必须带**实得类型串原文**（子流程裁定靠它暴露）: {m}"
        assert "[jeeflow]" in m, f"日志得走本栈既有的 [jeeflow] 前缀惯例，方便横扫: {m}"

        # 行为零改动（本轮只加日志）：节点不建行、令牌停住、办理没被打崩
        inst = await repo.find_instance_by_id(iid)
        assert int(inst.state) == 10, f"[{raw_type}] 实例停在进行中（不许炸、也不许假办结）: {inst.state}"
        assert not await repo.find_doing_tasks(iid), f"[{raw_type}] 未知节点不产生待办行"
        assert [t for t in inst.tasks if t.taskName == "ghost"] == [], \
            f"[{raw_type}] 未知节点连历史行都不该有（没被解析成任何模型）"


@pytest.mark.asyncio
async def test_i141_g4_known_node_types_emit_no_unknown_warning(caplog):
    """反向哨兵：表里那 7 档**一律不许**被报成"不在类型表里"。
    这一格咬的是两种混过判据的写法——① 把日志无条件打出去（只在未知分支里才该响）；
    ② 用"if/elif 走完没人认领"当未知判据（`snaker:start` 也在表里，却恰好没有执行分支，
    令牌真走到它头上时会被误报成未知档——两个病得分开诊断，判据是 model.KNOWN_NODE_TYPES）。"""
    import logging

    for known in ("snaker:task", "snaker:decision", "snaker:fork", "snaker:join",
                  "snaker:end", "snaker:custom", "snaker:start"):
        _eng, repo, facade, _events, _reg = _i141g9_harness()
        props = {"assignee": "boss"} if known == "snaker:task" else {}
        nodes, edges = _i142_chain([{"id": "known1", "type": known,
                                     "text": {"value": "表里的档"}, "properties": props}])
        with caplog.at_level(logging.WARNING):
            caplog.clear()
            iid = await _i142_start(facade, f"i141-g4-known-{known}", nodes, edges)
        msgs = _unknown_type_warnings(caplog.records)
        assert msgs == [], f"[{known}] 是类型表里的档，不该出现未知档日志（无条件打日志／else 兜底都在这红）: {msgs}"
        assert await repo.find_instance_by_id(iid) is not None, f"[{known}] 办理没被打崩"


# ═══ issues/137 A · 裁定 A（批二 §3-4）· 实例行 expire_time ＝ 定义**顶层**表达式的求值结果 ═══════
#
# 立法依据：jeeflow-hub/docs/goal-批二-引擎收尾发版轮-启动词.md §3-4 ＋ issues/137 A（owner 拍 A 案：
# 实例级 expire_time 是**定义级表达式的求值结果**，不是表达式原串、也不是 now()）。
# 基准＝java：JeeflowEngineImpl.java:93-96（发起时取流程定义**顶层** ``model.getExpireTime()``，
# 非空才 ``instance.setExpireTime(FlowUtil.processTime(expireTime, args))``）
#      ＋ boot2 内置版 ProcessInstanceServiceImpl.java:157-160（先判非空再写，同一形状）。
# spec 02:21/55「流程期望完成时间」＝流程 JSON **根上**那个 expireTime 键（与节点 properties 里那份
# 是两个位置，任务行读 properties、实例行读根，两列各判各的）。
#
# 本栈原形状＝**这一处写点压根没有**：``FlowModel`` 连根上的 expireTime 都不读（没有那个字段），
# 引擎发起腿也一句没赋 ⇒ ``wf_process_instance.expire_time`` 从来没被写过（恒 NULL）。
# 本轮补两半：① ``FlowModel.expireTime`` ＋ ``parse_flow_model`` 读根键；② 发起腿在 ``save_instance``
# **之前**用**既有那把尺子**（``process_time``，套在 ``_apply_expire_time`` 的"非空才写"守卫里）
# 求值后落到实例行——不新造第二把尺子，档位顺序与语义一字不动。
#
# ⚠️ 判据全部打在**仓储读回的持久行**上（``repo.find_instance_by_id``／SQLite 直查列），
#    不看引擎返回的那个聚合对象——issues/113 的形状：只有读回值能证明这一列真进了库。
# ⚠️ 判据打在**值**上（具体时刻 / 同行 expire−create 带宽 / NULL），**不是"非空"空判**：
#    "非空"既放过 now() 占位（差值≈0＝新建即逾期，issues/126 病灶），也放过搬原串。
#
# 五条判据 → 格子对照：
#  ① 进列的是**求值结果（时刻）**不是原串 → ``..._is_a_moment_not_the_raw_expression``（相对档＋变量档
#     两格对"搬原串"这一刀敏感：``"2h"``/``"dueAt"`` 原串都算不出时刻。绝对档那格原串与时刻同形，
#     抓不到这一刀，故**故意不拿它当主判据**，只当档位未被改动的对照）。
#     真库那一刀（160 MySQL ``@@sql_mode`` 含 STRICT_TRANS_TABLES，rust/php 本轮实测服务端给
#     1292/22007 ``Incorrect datetime value: '2h' for column 'expire_time'``）另见
#     ``test_i137a_column_lands_in_the_sql_repository`` 的 SQL 通道格 ＋ tests/jdbc_test.py 的 ⑱ 段。
#  ② 求值的 args＝**发起参数**（已注入用户信息与 autoGenTitle 的那份）
#     → ``..._takes_the_start_args`` ＋ ``..._takes_the_injected_copy_not_the_raw_caller_dict``
#  ③ 定义没配（缺键／空串／纯空白／null）⇒ 该列保持 NULL，不赋 now()、不赋空串
#     → ``..._not_configured_keeps_the_column_null``
#  ④ 配了但算不出（误配／负数档）⇒ NULL，沿用既有落穿语义，不许兜底 now
#     → ``..._unparsable_or_negative_keeps_the_column_null``
#  ⑤ 求值器复用既有那一枚（档位顺序与语义一字不改）
#     → ``..._same_ruler_as_task_rows`` ＋ ``..._variable_tier_beats_relative_tier_on_the_instance_row``

_I137A_SQL_DDL = (
    # 发起腿 SQL 通道用到的四张表，列名逐字对齐 tests/schema/schema-mysql.sql 与 repository/base.py 的 SQL；
    # 形状照 _cc141_sql_repo / _i142b_sql_repo 的先例：真 SQLite ＋ 真 JdbcRepository，不用内存假仓。
    "CREATE TABLE wf_process_define (id INTEGER PRIMARY KEY, name TEXT, display_name TEXT,"
    " type TEXT, state INTEGER, content TEXT, version INTEGER, create_time TEXT, create_user TEXT,"
    " update_time TEXT, update_user TEXT)",
    "CREATE TABLE wf_process_instance (id INTEGER PRIMARY KEY, parent_id INTEGER,"
    " process_define_id INTEGER, state INTEGER, parent_node_name TEXT, business_no TEXT,"
    " operator TEXT, expire_time TEXT, variable TEXT, create_time TEXT, create_user TEXT,"
    " update_time TEXT, update_user TEXT)",
    "CREATE TABLE wf_process_task (id INTEGER PRIMARY KEY, process_instance_id INTEGER,"
    " task_name TEXT, display_name TEXT, task_type INTEGER, perform_type INTEGER,"
    " task_state INTEGER, operator TEXT, finish_time TEXT, expire_time TEXT, form_key TEXT,"
    " task_parent_id INTEGER, variable TEXT, create_time TEXT, create_user TEXT,"
    " update_time TEXT, update_user TEXT)",
    "CREATE TABLE wf_process_task_actor (id INTEGER PRIMARY KEY, process_task_id INTEGER,"
    " actor_id TEXT, create_time TEXT, create_user TEXT)",
)


def _i137a_sql_repo():
    """真 SQLite ＋ 真 ``JdbcRepository``（内存仓绿 ≠ 落库绿，这一路钉的是**列真的进了库**）。"""
    from jeeflow.repository.base import JdbcRepository
    raw = sqlite3.connect(":memory:")
    for ddl in _I137A_SQL_DDL:
        raw.execute(ddl)
    return raw, JdbcRepository(_SqliteAdapter(raw), _TestIDGen())


def _i137a_content(root=_MISSING, specs=(("approve", "leader", _MISSING),),
                   name: str = "expire137a") -> str:
    """根上带/不带 expireTime 的流程 JSON。
    节点一律**不配**到期表达式（specs 的 expr 默认 ``_MISSING``）⇒ 实例那一列是本组唯一变量，
    任务行恒 NULL，两列的判据不会互相冒充。
    ``root`` 传 ``_MISSING`` ＝ 根上不写这个键；传 ``None``/``""``/``"   "`` 原样进 JSON。"""
    raw = json.loads(_expire_flow(list(specs), name))
    if root is not _MISSING:
        raw["expireTime"] = root
    return json.dumps(raw, ensure_ascii=False)


async def _i137a_start(root=_MISSING, args=None, define_name: str = "expire137a",
                       specs=(("approve", "leader", _MISSING),)):
    """发起一条流，返回 (repo, 引擎返回值, **仓储读回的持久行**)。
    判据只吃第三项——引擎那个聚合对象上挂着没落库的字段也算"有值"（issues/113 形状）。"""
    eng, repo, def_id = _expire_harness(_i137a_content(root, specs, define_name), define_name)
    inst = await eng.start_process_instance_by_id(def_id, "zhangsan", dict(args or {}))
    row = await repo.find_instance_by_id(inst.id)
    assert row is not None, "夹具自证：实例行没读回（" + define_name + "）"
    return repo, inst, row


def _i137a_moment(value, who: str):
    """值级判据的前半：这一列存的必须是**时刻**（datetime），不是别的什么。
    搬原串那一刀在这里就断掉：``"2h"`` / ``"dueAt"`` 进列后 to_datetime 认不出 ⇒ None ⇒ 红。"""
    assert value is not None, f"{who}：实例行 expire_time 为 NULL（写点没生效？）"
    got = to_datetime(value)
    assert got is not None, \
        f"{who}：expire_time 存的不是时刻，实得 {value!r}（把表达式原串搬进 datetime 列＝" \
        f"真库上是 1292/22007 Incorrect datetime value，内存里也只是假绿）"
    return got


@pytest.mark.asyncio
async def test_i137a_instance_expire_is_a_moment_not_the_raw_expression():
    """判据①：进列的是**求值结果**（时刻），不是表达式原串。
    相对档钉"同行 expire − create ≈ 偏移"（带宽 [N−5s, N+60s]，同 issues/126 那把尺子）——
    只判"非空"会被 now() 占位蒙过（差值≈0＝建单即逾期），判原串搬进来则连减法都做不了。"""
    for expr, seconds, who in (("2h", 2 * 3600, "小时档"), ("90s", 90, "秒档"),
                               ("30m", 30 * 60, "分钟档"), ("1d", 86400, "天档")):
        _repo, _inst, row = await _i137a_start(expr, define_name=f"expire137a-{who}")
        assert row.createTime is not None, f"{who}：对照列 create_time 就该有值"
        create, expire = to_datetime(row.createTime), _i137a_moment(row.expireTime, f"{who} 配 {expr!r}")
        delta = (expire - create).total_seconds()
        assert seconds - 5 <= delta <= seconds + 60, \
            f"{who} 配 {expr!r}：同行 expire − create = {delta}s，want ≈{seconds}s；" \
            f"差值≈0 就是 now() 占位，差值算不出就是原串（实得 {row.expireTime!r}）"
    # 绝对档对照：原串与时刻同形，这一档**抓不到搬原串那一刀**，只证明第 3 档没被改动
    _repo, _inst, row = await _i137a_start("2026-12-31 10:00:00", define_name="expire137a-abs")
    assert to_datetime(row.expireTime) == datetime(2026, 12, 31, 10, 0, 0), \
        f"绝对档应原样算成那一刻，实得 {row.expireTime!r}"


@pytest.mark.asyncio
async def test_i137a_variable_tier_takes_the_start_args():
    """判据②：求值用的 args ＝**发起参数**。表达式是个变量名 ⇒ 取该变量的值当到期时刻。
    三种值形状（datetime 对象 / 毫秒时间戳 / 契约格式文本）必须给出**同一时刻**（与任务行那三档同判据）。"""
    want = datetime(2026, 12, 31, 10, 0, 0)
    for label, value in (("契约格式文本", "2026-12-31 10:00:00"),
                         ("毫秒时间戳", int(want.timestamp() * 1000)),
                         ("datetime 对象", want)):
        _repo, _inst, row = await _i137a_start("dueAt", {"dueAt": value},
                                               define_name=f"expire137a-var-{label}")
        got = _i137a_moment(row.expireTime, f"变量档（{label}）")
        assert abs((got - want).total_seconds()) < 1, \
            f"变量档（{label}）应取 args 里那份值得 {want}，实得 {got!r}" \
            f"（拿不到 args 就说明写点用的不是发起参数那份）"


@pytest.mark.asyncio
async def test_i137a_variable_tier_beats_relative_tier_on_the_instance_row():
    """判据⑤（档位顺序）：实例行同样**变量档优先于相对档** —— args 里真有个键叫 "2h" 时
    取的是变量值那一刻，不是 now+2h。这一格同时挡住"给实例行新造一把只认相对档的尺子"。"""
    _repo, _inst, row = await _i137a_start("2h", {"2h": "2030-01-01 00:00:00"},
                                           define_name="expire137a-order")
    got = _i137a_moment(row.expireTime, "变量档压过相对档")
    assert got == datetime(2030, 1, 1, 0, 0, 0), \
        f"实例行的档位顺序与任务行分叉了：want 2030-01-01 00:00:00，实得 {got!r}"


@pytest.mark.asyncio
async def test_i137a_takes_the_injected_copy_not_the_raw_caller_dict():
    """判据②（取哪一份的实读结论）：args 必须是**注入用户信息与 autoGenTitle 之后**的那份。
    夹具：根上表达式写作变量名 ``autoGenTitle``，同时 caller 在 args 里塞一个**同名**的假时刻。
    引擎发起腿在求值前已经把 ``autoGenTitle`` 覆写成 "<实名>的<流程名>-yyyy-MM-dd HH:mm"（标题串），
    那份值解析不出 ⇒ 该列 NULL。写点若取的是注入**前**的 caller dict，这里会读出 2030-06-01 08:30:00。
    基准侧同判据：java JeeflowEngineImpl 是 ``addUserInfoToArgs``/``addAutoGenTitle`` **就地**改过
    的 ``args`` 才递给 ``FlowUtil.processTime``（顺序不可换）。
    正向对照同格给出：没被注入覆写的键（dueAt）照样取得到值 ⇒ 这一格不是"永远 NULL"的恒真判据。"""
    _repo, _inst, row = await _i137a_start(KEY_AUTO_GEN_TITLE,
                                           {KEY_AUTO_GEN_TITLE: "2030-06-01 08:30:00"},
                                           define_name="expire137a-injected")
    assert row.expireTime is None, \
        f"实例级求值必须吃**注入之后**那份参数：autoGenTitle 已被引擎改写成标题串（解析不出 ⇒ NULL），" \
        f"读出 {row.expireTime!r} 说明取的是注入前的 caller dict"
    assert row.variables.get(KEY_AUTO_GEN_TITLE) != "2030-06-01 08:30:00", \
        "夹具自证：注入确实覆写了这个键（否则上一句恒真）"

    _repo2, _inst2, row2 = await _i137a_start("dueAt", {"dueAt": "2030-06-01 08:30:00"},
                                              define_name="expire137a-notinjected")
    assert to_datetime(row2.expireTime) == datetime(2030, 6, 1, 8, 30), \
        f"正向对照：没被注入覆写的变量照样取到值，实得 {row2.expireTime!r}"


@pytest.mark.asyncio
async def test_i137a_not_configured_keeps_the_column_null():
    """判据③：定义**没配**顶层 expireTime（JSON 缺键 / 空串 / 纯空白 / null）⇒ 该列保持 NULL。
    三档都不许赋 now()、不许赋空串（空串进 DATETIME 列在真库上是硬错，在内存里是"看着有值"的假绿）。
    对照列 create_time 必须非空——否则"这一列空"是整行没落库，判不出档位。"""
    for label, root in (("键缺失", _MISSING), ("空串", ""), ("纯空白", "   "), ("null", None)):
        _repo, _inst, row = await _i137a_start(root, define_name=f"expire137a-none-{label}")
        assert row.createTime is not None, f"{label}：对照列 create_time 应有值（整行得先落进库）"
        assert row.expireTime is None, \
            f"根上 {label} 时实例行不得被赋任何时间，实得 {row.expireTime!r}"


@pytest.mark.asyncio
async def test_i137a_unparsable_or_negative_keeps_the_column_null():
    """判据④：配了但**算不出**（误配 / 负数相对档）⇒ NULL，沿用既有落穿语义，不许兜底 now()。
    负数档放行＝建单即逾期（算出一个过去时刻），兜底 now＝建单即逾期（差值 0），两种病都在这格红。"""
    for expr in ("not-a-time", "xh", "12x3h", "2.5h", "-5h", "-5d", "-30s",
                 "2026-13-31 10:00:00", "2O26-12-31 10:00:00"):
        _repo, _inst, row = await _i137a_start(expr, define_name=f"expire137a-bad")
        assert row.createTime is not None, f"误配 {expr!r}：对照列 create_time 应有值"
        assert row.expireTime is None, \
            f"表达式 {expr!r} 算不出必须留 NULL，实得 {row.expireTime!r}" \
            f"（≈create_time＝兜了 now()，早于 create_time＝放行了负数档）"


@pytest.mark.asyncio
async def test_i137a_same_ruler_as_task_rows():
    """判据⑤：实例行与任务行**共用同一把尺子**（``process_time``，没有第二枚）。
    同一份表达式同时写在根上与节点 properties 上 ⇒ 两行读出**同一时刻**；
    再与直调 ``process_time`` 的返回值逐字对齐——尺子被换掉/包了兜底，这三方就分叉。"""
    # 绝对/变量档可精确对齐
    specs = (("approve", "leader", "dueAt"),)
    eng_repo, eng_inst, row = await _i137a_start("dueAt", {"dueAt": "2026-12-31 10:00:00"},
                                                 specs=specs, define_name="expire137a-shared")
    task_rows = [t for t in await eng_repo.find_doing_tasks(eng_inst.id) if t.taskName == "approve"]
    assert len(task_rows) == 1, f"夹具自证：approve 进行中行应恰好 1 条，实得 {len(task_rows)}"
    want = datetime(2026, 12, 31, 10, 0, 0)
    assert to_datetime(row.expireTime) == want, f"实例行：want {want}，实得 {row.expireTime!r}"
    assert to_datetime(task_rows[0].expireTime) == want, \
        f"任务行：want {want}，实得 {task_rows[0].expireTime!r}（两行不同尺子＝分叉）"
    from jeeflow.engine import process_time as _pt
    assert _pt("dueAt", {"dueAt": "2026-12-31 10:00:00"}) == want, "尺子本身的行为被改动了"

    # 相对档：同一枚尺子给两行的偏移量同档（都是 now+2h，差值只来自取时的毫秒级先后）
    eng_repo2, eng_inst2, row2 = await _i137a_start("2h", specs=(("approve", "leader", "2h"),),
                                                    define_name="expire137a-shared-rel")
    task2 = [t for t in await eng_repo2.find_doing_tasks(eng_inst2.id) if t.taskName == "approve"][0]
    inst_at = _i137a_moment(row2.expireTime, "共用尺子（相对档）· 实例行")
    task_at = _i137a_moment(task2.expireTime, "共用尺子（相对档）· 任务行")
    assert abs((inst_at - task_at).total_seconds()) <= 5, \
        f"同一份 \"2h\" 在实例行与任务行上给出 {inst_at} / {task_at}，差 >5s ⇒ 两处用了不同的尺子"


@pytest.mark.asyncio
async def test_i137a_column_lands_in_the_sql_repository():
    """判据①＋落库：走**真 SQL 通道**（SQLite ＋ 真 JdbcRepository），直查 ``expire_time`` 那一列。
    内存仓的 deepcopy 让"字段有值"廉价成立；这一格钉的是 INSERT 的位置参真把**时刻**带进了列。
    搬原串那一刀在这里同样红：``'2h'`` 落进列后 to_datetime 认不出（真库上更硬——MySQL
    STRICT_TRANS_TABLES 直接报 1292/22007 Incorrect datetime value，见 jdbc_test.py ⑱ 段）。"""
    from jeeflow.repository.base import JdbcRepository
    raw, repo = _i137a_sql_repo()
    eng = EngineImpl(repo, _TestUserProv(), _TestIDGen())
    now = datetime.now()

    async def _start(root, define_name):
        d = ProcessDefine(name=define_name, displayName="实例到期", type="test", state=1, version=1,
                          content=_i137a_content(root, name=define_name),
                          createTime=now, updateTime=now, createUser="t", updateUser="t")
        await repo.save_define(d)
        inst = await eng.start_process_instance_by_id(d.id, "zhangsan", {"amount": "1"})
        return raw.execute("SELECT expire_time, create_time FROM wf_process_instance WHERE id=?",
                           (inst.id,)).fetchone(), await repo.find_instance_by_id(inst.id)

    col, back = await _start("2h", "i137a-sql")
    assert col is not None, "SQL 通道夹具自证：实例行没落库"
    stored, created = col[0], col[1]
    assert stored is not None and stored != "2h", \
        f"列里必须是求值结果，实得 {stored!r}（原串进 DATETIME 列在真库上是硬错）"
    at, ct = to_datetime(stored), to_datetime(created)
    assert at is not None, f"列值解析不成时刻（原串搬运？）：{stored!r}"
    delta = (at - ct).total_seconds()
    assert 2 * 3600 - 5 <= delta <= 2 * 3600 + 60, \
        f"库里同行 expire − create = {delta}s，want ≈7200s（实得 expire={stored!r} create={created!r}）"
    assert to_datetime(back.expireTime) == at, \
        f"仓储读回与直查列不一致（列没进 SELECT 映射？）：直查 {stored!r} 读回 {back.expireTime!r}"

    # 没配那一档：落库必须是 NULL 列（不是空串、不是 now）
    col2, back2 = await _start(_MISSING, "i137a-sql-none")
    assert col2[0] is None, f"定义没配顶层 expireTime 时列必须 NULL，实得 {col2[0]!r}"
    assert back2.expireTime is None, f"仓储读回同判 NULL，实得 {back2.expireTime!r}"
    assert col2[1] is not None, "对照列 create_time 应有值（整行确实落了库）"
