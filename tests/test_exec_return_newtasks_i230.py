"""audit8 R92（issues/230 拍 A）：python 执行族四法回传「本次动作产出的任务行」。

基准＝java ``JeeflowEngineImpl`` 四法返回 ``exec.getProcessTaskList()``，``exec == null`` 时
``Collections.emptyList()`` ⇒ 空档必须是**非 None 的空列表**（对外 ``[]``）。start 族两法保留返回实例
（spec04 :9/:24 未动）。作用域＝``contextvars``（见 ``jeeflow/engine.py`` 的 ``_NEW_TASKS`` 注释）。

八档各打一件事，防"改了签名但没人证明语义"与"拿现成查询糊过去"：
  ① 串行链＝每一步返回行逐一对齐该步新落的待办（同 id、同实例、已在仓储）；
  ② 扇出＝并行会签一次动作产 3 行必须逐行回传（摘掉任一追加腿在对应档红）；
  ③ 反向哨兵＝会签中办掉一张、同节点另两张仍待办 ⇒ 本次产出 0 行、返回必须为空列表
     ——这一档专治"直接 return find_doing_tasks"的假实现（那种写法 ①② 全绿而本档红）；
  ④ 终止＝末环办理使实例完成，返回空列表（非 None）且实例确为完成态；
  ⑤ 回退复活＝``execute_and_jump_task`` 无目标名那半（血缘复活上一环）产出的行必须回传；
  ⑥ 跳转档＝带目标名产出行回传、``execute_and_jump_to_end`` 空列表、目标名不存在仍显性抛；
  ⑦ 并发隔离＝同一 EngineImpl 两条实例 asyncio.gather 并行办理，返回值不得互串；
  ⑧ 未知 countersignType 档＝仓库 16 份定义喂不到 default 支 ⇒ 就地合成一份钉那条追加腿；
  ⑨ 回归哨兵＝返回值与仓储里的同一行，建单没被写两遍；
  ⑩ 串行会签两腿＝首位建单腿与推进建单腿各给一条判据（并行档代不了）。

全档走内存仓（无真库腿），门票 ``pytest`` 自动收。
"""
import asyncio
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository
from jeeflow.model import InstanceState, ProcessDefine, TaskState
from jeeflow.spi import ExpressionEvaluator, IDGenerator, UserProvider

import flows_resolver
FLOW_DIR = flows_resolver.dir()


class _UserProv(UserProvider):
    async def get_user(self, user_id: str):
        return None


class _IDGen(IDGenerator):
    def __init__(self):
        self.n = 0

    def next_id(self) -> int:
        self.n += 1
        return self.n


class _EvalFalse(ExpressionEvaluator):
    async def eval(self, expr: str, vars: dict):
        return False


def setup():
    repo = MemoryRepository()
    return EngineImpl(repo, _UserProv(), _IDGen(), _EvalFalse()), repo


def load_flow(repo, filename: str, content: str = None) -> ProcessDefine:
    if content is None:
        with open(os.path.join(FLOW_DIR, filename), "r", encoding="utf-8") as f:
            content = f.read()
    d = ProcessDefine(name=filename, displayName=filename, type="test", state=1, content=content)
    repo.add_define(d)
    return d


async def act(eng, repo, task, operator, args=None):
    """补齐参与者后办理（与既有夹具同法），返回执行族的返回值"""
    await repo.add_task_actor(task.id, [operator])
    if operator not in (task.actorIds or []):
        task.actorIds = list(task.actorIds or []) + [operator]
        await repo.save_task(task)
    return await eng.execute_process_task(task.id, operator, args or {})


async def pick(repo, instance_id, task_name=None, actor=None):
    doing = await repo.find_doing_tasks(instance_id)
    for t in doing:
        if task_name and t.taskName != task_name:
            continue
        if actor and actor not in (t.actorIds or []) and t.actorId != actor:
            continue
        return t
    raise AssertionError("前置缺待办 %r/%r，实得 %r" % (task_name, actor, [t.taskName for t in doing]))


async def assert_rows(repo, instance_id, rows, want_ids, tag):
    assert isinstance(rows, list), "%s：返回值必须是列表（java emptyList 同档，不得 None）" % tag
    assert sorted(str(r.id) for r in rows) == sorted(str(i) for i in want_ids), \
        "%s：返回行 id 集合应等于本次动作产出的集合" % tag
    for r in rows:
        assert str(r.processInstanceId) == str(instance_id), "%s：返回行 %s 实例号应等于 %s" % (tag, r.id, instance_id)
        assert await repo.find_task_by_id(r.id) is not None, "%s：返回行必须已在仓储里（id=%s）" % (tag, r.id)


@pytest.mark.asyncio
async def test_i230_serial_chain_returns_each_steps_rows():
    """① 串行链：每一步返回行＝该步新建的待办行"""
    eng, repo = setup()
    df = load_flow(repo, "02-multi-task.json")
    inst = await eng.start_process_instance_by_id(df.id, "alice", {})
    for task_name, operator in [("apply", "alice"), ("task1", "leader"), ("task2", "manager"), ("task3", "boss")]:
        cur = await pick(repo, inst.id, task_name)
        rows = await act(eng, repo, cur, operator)
        after = await repo.find_doing_tasks(inst.id)
        await assert_rows(repo, inst.id, rows, [t.id for t in after], "%s→" % task_name)
        inst = await repo.find_instance_by_id(inst.id)
    assert inst.state == int(InstanceState.DONE), "02-multi-task 四步办完实例应完成"


@pytest.mark.asyncio
async def test_i230_parallel_countersign_returns_every_row():
    """② 扇出：并行会签一次动作产三行必须逐行回传"""
    eng, repo = setup()
    df = load_flow(repo, "05-countersign-parallel.json")
    inst = await eng.start_process_instance_by_id(df.id, "alice", {})
    rows = await act(eng, repo, await pick(repo, inst.id, "apply"), "alice")
    assert len(rows) == 3, "会签建单应回传 3 行，实得 %d 行" % len(rows)
    assert [r.taskName for r in rows] == ["task1", "task1", "task1"]
    after = await repo.find_doing_tasks(inst.id)
    await assert_rows(repo, inst.id, rows, [t.id for t in after], "会签扇出")


@pytest.mark.asyncio
async def test_i230_reverse_sentinel_zero_output_returns_empty():
    """③ 反向哨兵：同节点仍有余单但本次零产出 ⇒ 返回空列表（假实现在此红）"""
    eng, repo = setup()
    df = load_flow(repo, "05-countersign-parallel.json")
    inst = await eng.start_process_instance_by_id(df.id, "alice", {})
    await act(eng, repo, await pick(repo, inst.id, "apply"), "alice")
    first = await pick(repo, inst.id, "task1", "userA")
    rows = await eng.execute_process_task(first.id, "userA", {})
    remain = await repo.find_doing_tasks(inst.id)
    assert len(remain) == 2, "前置：会签办掉一张应还剩两张待办，实得 %d" % len(remain)
    assert isinstance(rows, list) and rows == [], (
        "本次动作没有新建任务，返回应是空列表，实得 %r（若实现直接 return find_doing_tasks 会在此红"
        "＝把仍待办的他人单当本次产出）" % [r.taskName for r in rows])


@pytest.mark.asyncio
async def test_i230_terminal_returns_empty_list_not_none():
    """④ 终止档：末环返回空列表而不是 None，实例完成"""
    eng, repo = setup()
    df = load_flow(repo, "01-simple.json")
    inst = await eng.start_process_instance_by_id(df.id, "alice", {})
    rows1 = await act(eng, repo, await pick(repo, inst.id, "apply"), "alice")
    assert len(rows1) == 1, "前置：办理 apply 应产出一行"
    inst = await repo.find_instance_by_id(inst.id)
    assert inst.state != int(InstanceState.DONE), "前置：办末环前实例不应已完成"
    rows = await act(eng, repo, await pick(repo, inst.id), "leader")
    assert isinstance(rows, list), "末环返回值必须是列表（不得 None）"
    assert rows == [], "实例已到 end 不应有产出行，实得 %d 行" % len(rows)
    inst = await repo.find_instance_by_id(inst.id)
    assert inst.state == int(InstanceState.DONE), "末环办完实例应完成"


@pytest.mark.asyncio
async def test_i230_rollback_revived_row_is_returned():
    """⑤ 回退复活档：血缘复活的那一行必须回传"""
    eng, repo = setup()
    df = load_flow(repo, "02-multi-task.json")
    inst = await eng.start_process_instance_by_id(df.id, "alice", {})
    await act(eng, repo, await pick(repo, inst.id, "apply"), "alice")
    task1 = await pick(repo, inst.id, "task1")
    await repo.add_task_actor(task1.id, ["leader"])
    rows = await eng.execute_and_jump_task(task1.id, "leader", {})
    after = await repo.find_doing_tasks(inst.id)
    await assert_rows(repo, inst.id, rows, [t.id for t in after], "回退复活")
    assert [r.taskName for r in rows] == ["apply"], "应复活上一环 apply，实得 %r" % [r.taskName for r in rows]


@pytest.mark.asyncio
async def test_i230_jump_family_rows_and_explicit_error():
    """⑥ 跳转档：带目标名回传产出行／跳结束回空列表／目标名不存在仍显性抛"""
    eng, repo = setup()
    df = load_flow(repo, "02-multi-task.json")
    inst = await eng.start_process_instance_by_id(df.id, "alice", {})
    apply = await pick(repo, inst.id, "apply")
    await repo.add_task_actor(apply.id, ["alice"])
    rows = await eng.execute_and_jump_task(apply.id, "alice", {}, "task3")
    after = await repo.find_doing_tasks(inst.id)
    await assert_rows(repo, inst.id, rows, [t.id for t in after], "跳转 task3")
    assert [r.taskName for r in rows] == ["task3"]

    eng2, repo2 = setup()
    df2 = load_flow(repo2, "02-multi-task.json")
    inst2 = await eng2.start_process_instance_by_id(df2.id, "alice", {})
    apply2 = await pick(repo2, inst2.id, "apply")
    rows2 = await eng2.execute_and_jump_to_end(apply2.id, "alice", {})
    assert isinstance(rows2, list) and rows2 == [], "跳结束不建单，返回应是空列表"
    assert await repo2.find_doing_tasks(inst2.id) == [], "跳结束后不应有待办"

    # 负向档：目标名不存在必须显性抛（签名变化不该吞掉这条既有义务）
    eng3, repo3 = setup()
    df3 = load_flow(repo3, "02-multi-task.json")
    inst3 = await eng3.start_process_instance_by_id(df3.id, "alice", {})
    apply3 = await pick(repo3, inst3.id, "apply")
    with pytest.raises(Exception, match=r"根据节点名称\[nope\]无法找到节点模型"):
        await eng3.execute_and_jump_task(apply3.id, "alice", {}, "nope")


@pytest.mark.asyncio
async def test_i230_first_task_node_jump_returns_row():
    """⑥b 跳回首任务节点：产出行回传且 assignee＝发起人（对齐 java isFirstTaskName 那半）"""
    eng, repo = setup()
    df = load_flow(repo, "02-multi-task.json")
    inst = await eng.start_process_instance_by_id(df.id, "alice", {})
    # 链＝apply→task1→task2→task3：站到 task2 需要办两步
    await act(eng, repo, await pick(repo, inst.id, "apply"), "alice")
    await act(eng, repo, await pick(repo, inst.id, "task1"), "leader")
    task2 = await pick(repo, inst.id, "task2")
    await repo.add_task_actor(task2.id, ["alice"])
    rows = await eng.execute_and_jump_to_first_task_node(task2.id, "alice", {})
    after = await repo.find_doing_tasks(inst.id)
    await assert_rows(repo, inst.id, rows, [t.id for t in after], "跳首任务节点")
    assert len(rows) == 1 and rows[0].taskName == "apply"
    assert "alice" in (rows[0].actorIds or []) or rows[0].actorId == "alice", \
        "跳首任务节点时 assignee 强制为发起人，实得 %r/%r" % (rows[0].actorId, rows[0].actorIds)


@pytest.mark.asyncio
async def test_i230_concurrent_instances_do_not_cross_contaminate():
    """⑦ 并发隔离：同一引擎两条实例并行办理，返回值不得互串"""
    eng, repo = setup()
    df = load_flow(repo, "02-multi-task.json")
    i1 = await eng.start_process_instance_by_id(df.id, "alice", {})
    i2 = await eng.start_process_instance_by_id(df.id, "bob", {})
    a1 = await pick(repo, i1.id, "apply")
    a2 = await pick(repo, i2.id, "apply")
    await repo.add_task_actor(a1.id, ["alice"])
    await repo.add_task_actor(a2.id, ["bob"])
    r1, r2 = await asyncio.gather(
        eng.execute_process_task(a1.id, "alice", {}),
        eng.execute_process_task(a2.id, "bob", {}),
    )
    assert len(r1) == 1 and len(r2) == 1, "各实例本次应只回传一行"
    assert str(r1[0].processInstanceId) == str(i1.id), "实例一的返回行必须属实例一"
    assert str(r2[0].processInstanceId) == str(i2.id), "实例二的返回行必须属实例二"


@pytest.mark.asyncio
async def test_i230_unknown_countersign_type_returns_all_rows():
    """⑧ 未知会签类型档：全员预创建的多行必须逐行回传（仓库定义喂不到 default 支⇒合成）"""
    eng, repo = setup()
    with open(os.path.join(FLOW_DIR, "06-countersign-sequential.json"), "r", encoding="utf-8") as f:
        raw = json.loads(f.read())
    for n in raw.get("nodes", []):
        if (n.get("properties") or {}).get("countersignType") == "SEQUENTIAL":
            n["properties"]["countersignType"] = "RATIO"
    df = load_flow(repo, "i230-cs-unknown", content=json.dumps(raw))
    inst = await eng.start_process_instance_by_id(df.id, "alice", {})
    rows = await act(eng, repo, await pick(repo, inst.id, "apply"), "alice")
    assert len(rows) == 2, "未知会签类型＝全员预创建，userA/userB 两行都要回传，实得 %d" % len(rows)
    after = await repo.find_doing_tasks(inst.id)
    await assert_rows(repo, inst.id, rows, [t.id for t in after], "未知会签档")


@pytest.mark.asyncio
async def test_i230_returned_rows_are_the_same_objects_as_persisted():
    """⑨ 回归哨兵：返回值与仓储里的同一行，建单没被写两遍"""
    eng, repo = setup()
    df = load_flow(repo, "01-simple.json")
    inst = await eng.start_process_instance_by_id(df.id, "alice", {})
    rows = await act(eng, repo, await pick(repo, inst.id, "apply"), "alice")
    doing = await repo.find_doing_tasks(inst.id)
    assert len(doing) == 1, "动作后待办仍是一行（返回值来自同一批建行，不重复写入）"
    assert doing[0].id == rows[0].id
    assert doing[0].taskName == "task1"
    assert int(doing[0].taskState) == int(TaskState.DOING)


@pytest.mark.asyncio
async def test_i230_sequential_countersign_both_legs_return_rows():
    """⑩ 串行会签两腿各自回传：首位建单腿（_create_task 的 SEQUENTIAL 支）＋推进建单腿
    （execute_process_task 的 SEQUENTIAL 分支）。摘掉任一条追加腿只在本档现形，
    ②（并行档）代不了这个签。"""
    eng, repo = setup()
    df = load_flow(repo, "06-countersign-sequential.json")
    inst = await eng.start_process_instance_by_id(df.id, "alice", {})
    # a) 办理 apply ⇒ 串行会签只建**首位**成员那一行
    first = await act(eng, repo, await pick(repo, inst.id, "apply"), "alice")
    assert len(first) == 1, "串行会签首位应只建一行，实得 %d" % len(first)
    assert first[0].taskName == "task1"
    await assert_rows(repo, inst.id, first, [t.id for t in await repo.find_doing_tasks(inst.id)], "串行首位")
    # b) 办完首位 ⇒ 推进建出第二位成员那一行
    second = await act(eng, repo, first[0], "userA")
    assert len(second) == 1, "串行推进应回传下一位成员一行，实得 %d" % len(second)
    assert second[0].taskName == "task1", "推进出的仍是会签节点的行"
    await assert_rows(repo, inst.id, second, [t.id for t in await repo.find_doing_tasks(inst.id)], "串行推进")
