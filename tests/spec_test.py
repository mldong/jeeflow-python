"""jeeflow SPEC 合规测试 — Python 版（boot2 兼容）"""
import json
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository, EventType, ProcessEvent, FlowInterceptor, EngineExtensions
from jeeflow.facade import JeeflowFacade
from jeeflow.memory import MemoryExtRepository
from jeeflow.model import ProcessDefine, ProcessTask, TaskState, InstanceState, UserInfo
from jeeflow.spi import UserProvider, IDGenerator, ExpressionEvaluator

FLOW_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "jeeflow-java",
                        "jeeflow-core", "src", "test", "resources", "flows")


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
    assert len(events) == 4, f"expected 4 events (start+apply+task1+finish), got {len(events)}: {events}"
    assert "PROCESS_START" in events
    assert "TASK_COMPLETE" in events  # at least once
    assert "PROCESS_FINISH" in events


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
    define_id = r["data"]["processDefineId"]
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
    instance_id = r["data"]["processInstanceId"]

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
    assert r["data"]["processDefineId"] > 0

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

    doing = await repo.find_doing_tasks(instance_id)
    r = await facade.flow("processTask/detail", {"id": doing[0].id, "operator": "leader"})
    assert r["code"] == 0 and r["data"]["executable"] is True, r
    assert r["data"]["taskModel"] is not None, r

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
    await facade.flow("processDesign/save",
                      {"name": "leave", "displayName": "请假流程", "content": c1, "operator": "zhangsan"})
    r = await facade.flow("processDesign/page", {"m_LIKE_name": "leave"})
    assert r["code"] == 0, r
    assert len(r["data"]["rows"]) == 1, r


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
    design_id = r["data"]["id"]
    assert (await facade._ext.find_design_by_id(design_id)).isDeployed == 0

    # 部署 → is_deployed=1
    r = await facade.flow("processDesign/deploy", {"id": design_id, "operator": "zhangsan"})
    assert r["code"] == 0, r
    define_id = r["data"]["processDefineId"]
    assert (await facade._ext.find_design_by_id(design_id)).isDeployed == 1

    # 重新部署 → 同一 defineId + is_deployed=1
    r = await facade.flow("processDesign/redeploy", {"id": design_id, "operator": "zhangsan"})
    assert r["code"] == 0, r
    assert r["data"]["processDefineId"] == define_id, r
    assert (await facade._ext.find_design_by_id(design_id)).isDeployed == 1

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
