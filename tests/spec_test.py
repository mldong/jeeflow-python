"""jeeflow SPEC 合规测试 — Python 版（boot2 兼容）"""
import json
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository, EventType, ProcessEvent, FlowInterceptor, EngineExtensions
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


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
