"""jeeflow SPEC 合规测试 — Python 版（boot2 兼容）"""
import json
import os
import sys
import pytest
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository, EventType, ProcessEvent, FlowInterceptor, EngineExtensions
from jeeflow.engine import KEY_AUTO_GEN_TITLE
from jeeflow.facade import JeeflowFacade
from jeeflow.memory import MemoryExtRepository
from jeeflow.model import (ProcessDefine, ProcessDesign, ProcessDesignHis, ProcessTask,
                           TaskState, InstanceState, UserInfo)
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

    # withdraw 级联废弃 doing
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": define_id, "operator": "zhangsan"})
    instance_id2 = r["data"]["processInstanceId"]
    r = await facade.flow("processInstance/withdraw", {"id": instance_id2, "operator": "zhangsan"})
    assert r["code"] == 0, r
    after = await repo.find_doing_tasks(instance_id2)
    assert len(after) == 0, f"撤回应废弃 doing 任务: {after}"


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

    # ── submitType=3 ROLLBACK：task2 退回上一步 → task1 新待办（actor=退回操作人），实例保持 DOING(10)
    rb = await _start_multi_task_at(facade, repo, "task2")
    t2 = await _doing_task_id(repo, rb, "task2")
    await repo.add_task_actor(t2, ["manager"])
    r = await facade.flow("processTask/execute",
                          {"processTaskId": t2, "operator": "manager", "submitType": 3})
    assert r["code"] == 0, r
    rb_task1 = await _doing_task_id(repo, rb, "task1")
    assert rb_task1, "ROLLBACK 应在 task1 产生新待办"
    assert "manager" in await repo.find_task_actors(rb_task1), "退回任务 actor 应为退回操作人 manager"
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
