"""jeeflow SPEC 合规测试 — Python 版（boot2 兼容）"""
import json
import os
import sys
import pytest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository, EventType, ProcessEvent, FlowInterceptor, EngineExtensions
from jeeflow.engine import KEY_AUTO_GEN_TITLE
from jeeflow.facade import JeeflowFacade
from jeeflow.memory import MemoryExtRepository
from jeeflow.surrogate import NullSurrogateApplier, surrogate_enabled_on, to_datetime
from jeeflow.model import (ProcessDefine, ProcessDesign, ProcessDesignHis, ProcessInstance,
                           ProcessSurrogate, ProcessTask, TaskState, InstanceState, UserInfo)
from jeeflow.spi import UserProvider, IDGenerator, ExpressionEvaluator

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

    async def on_event(evt: ProcessEvent):
        events.append(evt.type.value)

    eng.set_extensions(EngineExtensions(
        interceptors=[_TestInterceptor(pre_called, post_called, order=1)],
        event_listener=on_event,
    ))

    inst = await eng.start_process_instance_by_id(df.id, "applicant", None)
    assert "PROCESS_START" in events

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
    # issues/100：任务落库后 fire TASK_CREATE（对齐 Java CreateTaskHandler，含 apply 节点任务）。
    # 完整序列：start → [apply 任务 TASK_CREATE, apply 自动完成 TASK_COMPLETE]
    #         → [task1 TASK_CREATE, task1 完成 TASK_COMPLETE] → PROCESS_FINISH
    assert events == [
        "PROCESS_START",
        "TASK_CREATE",
        "TASK_COMPLETE",
        "TASK_CREATE",
        "TASK_COMPLETE",
        "PROCESS_FINISH",
    ], f"unexpected event sequence, got {events}"
    assert events.count("TASK_CREATE") == 2  # apply 节点任务 + task1 各一次
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

    async def add(pn, sur, start=None, end=None, enabled=1, op="boss"):
        s = ProcessSurrogate(operator=op, surrogate=sur, processName=pn,
                             startTime=start, endTime=end, enabled=enabled)
        await ext.save_surrogate(s)
        return s

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
    await add("en-str", "agent", enabled="1", op="c4")
    assert (await ext.get_surrogate("c4", "en-str")).surrogate == "agent", '④ "1" 等价启用'
    assert surrogate_enabled_on("abc") is False and surrogate_enabled_on(1) is True
    assert to_datetime("2026-13-45") is None, "不可解析时间 = 该侧不限"


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

