"""issues/257 子流程定向 T0（仅内存仓，不起服务、不连 DB、不改共享 flows）。"""
from copy import deepcopy
from datetime import datetime
import json

import pytest

from jeeflow import EngineImpl, JeeflowFacade, MemoryRepository
from jeeflow.engine import (
    KEY_AUTO_ID,
    KEY_SUBMIT_TYPE,
    _can_rejected,
    apply_initial_tasks,
)
from jeeflow.extensions import EventType
from jeeflow.model import (
    ERR_NOT_FOUND_PROCESS_DEFINE,
    SUB_PROCESS_TYPES,
    InstanceState,
    ProcessDefine,
    ProcessTask,
    SubProcessModel,
    TaskState,
    TaskType,
    UserInfo,
    parse_flow_model,
)
from jeeflow.spi import IDGenerator, UserProvider


class _IDGen(IDGenerator):
    def __init__(self):
        self.n = 0

    def next_id(self) -> int:
        self.n += 1
        return self.n


class _UserProv(UserProvider):
    async def get_user(self, user_id: str):
        return UserInfo(userId=user_id, realName=user_id)


def _setup():
    repo = MemoryRepository()
    eng = EngineImpl(repo, _UserProv(), _IDGen())
    events = []
    eng.add_event_listener(events.append)
    return eng, repo, events


def _node(node_id, node_type, props=None, text=None):
    return {
        "id": node_id,
        "type": node_type,
        "x": 0,
        "y": 0,
        "properties": props or {},
        "text": {"value": text or node_id},
    }


def _edge(edge_id, source, target):
    return {"id": edge_id, "sourceNodeId": source, "targetNodeId": target, "properties": {}}


def _flow(name, nodes, edges):
    return {"name": name, "displayName": name, "type": "approval", "nodes": nodes, "edges": edges}


def _chain(name, middle):
    nodes = [_node("start", "snaker:start")]
    edges = []
    prev = "start"
    for node_id, node_type, props in middle:
        nodes.append(_node(node_id, node_type, props))
        edges.append(_edge(f"e-{prev}-{node_id}", prev, node_id))
        prev = node_id
    nodes.append(_node("end", "snaker:end"))
    edges.append(_edge(f"e-{prev}-end", prev, "end"))
    return _flow(name, nodes, edges)


def _task(node_id, assignee, **props):
    return (node_id, "snaker:task", {"assignee": assignee, **props})


def _sub(node_id, process_name=None, **props):
    if process_name is not None:
        props["processName"] = process_name
    return (node_id, "snaker:subProcess", props)


def _add_define(repo, name, content, version=1, state=1):
    define = ProcessDefine(
        name=name,
        displayName=name,
        type="approval",
        state=state,
        version=version,
        content=json.dumps(content, ensure_ascii=False),
    )
    repo.add_define(define)
    return define


async def _start_and_execute(eng, repo, define, operator="applicant", args=None):
    facade = JeeflowFacade(eng, repo)
    payload = {"processDefineId": define.id, "operator": operator, **(args or {})}
    result = await facade.flow("processInstance/startAndExecute", payload)
    assert result["code"] == 0, result
    return await repo.find_instance_by_id(int(result["data"]["processInstanceId"]))


async def _only_doing(repo, instance_id):
    doing = await repo.find_doing_tasks(instance_id)
    assert len(doing) == 1, [(t.id, t.taskName, t.taskState) for t in doing]
    return doing[0]


async def _only_child(repo, parent_id):
    children = [inst for inst in repo._instances.values() if inst.parentId == parent_id]
    assert len(children) == 1, [(i.id, i.defineId) for i in children]
    return await repo.find_instance_by_id(children[0].id)


async def _done(repo, instance_id, task_name):
    tasks = await repo.find_done_tasks(instance_id, [task_name])
    assert len(tasks) == 1, [(t.id, t.taskName, t.taskState) for t in tasks]
    return tasks[0]


def _events(events, event_type, instance_id):
    return [evt for evt in events
            if evt.type == event_type and evt.instanceId == int(instance_id)]


async def _task_start_count(repo, events, instance_id, task_name):
    count = 0
    for evt in _events(events, EventType.PROCESS_TASK_START, instance_id):
        task = await repo.find_task_by_id(evt.taskId)
        if task is not None and task.taskName == task_name:
            count += 1
    return count


async def _complete(eng, task, operator, submit_type=1, args=None):
    payload = {KEY_SUBMIT_TYPE: submit_type, **(args or {})}
    return await eng.execute_process_task(task.id, operator, payload)


@pytest.mark.asyncio
async def test_parent_child_closes_loop_and_child_apply_is_auto_executed_once():
    eng, repo, events = _setup()
    child_def = _add_define(repo, "child257", _chain("child257", [
        _task("apply", "applicant"), _task("childReview", "leader"),
    ]))
    parent_def = _add_define(repo, "parent257", _chain("parent257", [
        _task("apply", "applicant"), _sub("sub257", "child257", form="__wrong__"),
        _task("parentAfter", "manager"),
    ]))

    parent = await eng.start_process_instance_by_id(parent_def.id, "applicant", {})
    parent_apply = await _only_doing(repo, parent.id)
    # 父动作的产出列表不得混入子 start/子 apply 已自行持久化并发码 3 的任务。
    created = await eng.execute_process_task(parent_apply.id, KEY_AUTO_ID, {KEY_SUBMIT_TYPE: 0})
    assert created == [], [t.taskName for t in created]

    child = await _only_child(repo, parent.id)
    assert child.parentId == parent.id
    assert child.parentNodeName == "sub257"
    assert child.defineId == child_def.id
    assert child.operator == "applicant"
    assert child.state == InstanceState.DOING
    child_apply = await _done(repo, child.id, "apply")
    assert child_apply.actorId == KEY_AUTO_ID
    assert "applicant" in (await repo.find_task_actors(child_apply.id))
    assert (await _only_doing(repo, child.id)).taskName == "childReview"
    assert await repo.find_doing_tasks(parent.id) == []
    assert await _task_start_count(repo, events, child.id, "apply") == 1
    assert await _task_start_count(repo, events, child.id, "childReview") == 1
    assert len(_events(events, EventType.PROCESS_INSTANCE_START, child.id)) == 1

    child_review = await _only_doing(repo, child.id)
    returned = await _complete(eng, child_review, "leader")
    assert [t.taskName for t in returned] == ["parentAfter"]
    assert (await repo.find_instance_by_id(child.id)).state == InstanceState.DONE
    parent_after = await _only_doing(repo, parent.id)
    assert parent_after.taskName == "parentAfter"
    assert parent_after.processInstanceId == parent.id
    assert (await repo.find_instance_by_id(parent.id)).state == InstanceState.DOING
    assert await _task_start_count(repo, events, parent.id, "parentAfter") == 1

    await _complete(eng, parent_after, "manager")
    assert (await repo.find_instance_by_id(parent.id)).state == InstanceState.DONE
    assert len(await repo.find_history_tasks(child.id)) == 2
    assert len(await repo.find_history_tasks(parent.id)) == 2
    for instance_id in (parent.id, child.id):
        assert len(_events(events, EventType.PROCESS_INSTANCE_START, instance_id)) == 1
        assert len(_events(events, EventType.PROCESS_INSTANCE_END, instance_id)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("node_type,node_id,props", [
    ("snaker:subProcess", "sub", {"processName": "child257", "name": "wrong", "form": "__wrong__"}),
    ("snaker:subprocess", "sub", {"processName": "child257"}),
    ("snaker:wfSubProcess", "sub", {"processName": "child257"}),
    ("snaker:subProcess", "sub", {"name": "child257", "form": "__wrong__"}),
    ("snaker:subProcess", "child257", {"subProcessName": "wrong", "form": "__wrong__"}),
])
async def test_registered_aliases_and_process_name_fallbacks(node_type, node_id, props):
    assert node_type in SUB_PROCESS_TYPES
    eng, repo, _events = _setup()
    child_def = _add_define(repo, "child257", _chain("child257", [
        _task("apply", "applicant"), _task("review", "leader"),
    ]))
    parent_def = _add_define(repo, "parent257", _chain("parent257", [
        _task("apply", "applicant"), (node_id, node_type, props),
    ]))
    parent = await _start_and_execute(eng, repo, parent_def)
    child = await _only_child(repo, parent.id)
    assert child.defineId == child_def.id
    assert child.parentNodeName == node_id
    assert (await _only_doing(repo, child.id)).taskName == "review"


def test_sub_process_model_keeps_process_name_version_and_form():
    raw = _flow("model257", [
        _node("start", "snaker:start"),
        _node("sub", "snaker:subProcess", {
            "processName": "target", "name": "wrong", "version": "2", "form": "__wrong__",
        }),
        _node("end", "snaker:end"),
    ], [
        _edge("e1", "start", "sub"), _edge("e2", "sub", "end"),
    ])
    model = parse_flow_model(raw)
    sub = model.nodes[1]
    assert isinstance(sub, SubProcessModel)
    assert sub.processName == "target"
    assert sub.version == 2
    assert sub.form == "__wrong__"


@pytest.mark.asyncio
async def test_child_without_apply_self_handles_first_task():
    eng, repo, events = _setup()
    _add_define(repo, "childNoApply257", _chain("childNoApply257", [
        _task("first257", "leader"), _task("second257", "manager"),
    ]))
    parent_def = _add_define(repo, "parentNoApply257", _chain("parentNoApply257", [
        _task("apply", "applicant"), _sub("subNoApply257", "childNoApply257", version=1),
    ]))
    parent = await _start_and_execute(eng, repo, parent_def)
    child = await _only_child(repo, parent.id)
    assert (await _only_doing(repo, child.id)).taskName == "second257"
    first = await _done(repo, child.id, "first257")
    assert first.actorId == KEY_AUTO_ID
    assert "applicant" in (await repo.find_task_actors(first.id))
    assert await _task_start_count(repo, events, child.id, "first257") == 1


@pytest.mark.asyncio
async def test_missing_definition_has_code_20010002_and_zero_memory_writes():
    eng, repo, events = _setup()
    parent_def = _add_define(repo, "parentMissing257", _chain("parentMissing257", [
        _task("apply", "applicant"),
        _sub("subMissing257", "missing257", version=1),
    ]))
    parent = await eng.start_process_instance_by_id(parent_def.id, "applicant", {})
    apply = await _only_doing(repo, parent.id)
    tasks_before = deepcopy(await repo.find_history_tasks(parent.id))
    instances_before = deepcopy(repo._instances)
    events_before = list(events)

    with pytest.raises(ValueError) as caught:
        await apply_initial_tasks(eng, repo, parent.id, "applicant", {})
    assert caught.value.code == ERR_NOT_FOUND_PROCESS_DEFINE == 20010002
    assert str(caught.value) == "子流程missing257定义不存在"

    assert await repo.find_history_tasks(parent.id) == tasks_before
    assert repo._instances == instances_before
    assert events == events_before
    current = await repo.find_task_by_id(apply.id)
    assert current.taskState == TaskState.DOING
    assert (await repo.find_instance_by_id(parent.id)).state == InstanceState.DOING


@pytest.mark.asyncio
async def test_missing_definition_facade_keeps_contract_message():
    eng, repo, _events = _setup()
    parent_def = _add_define(repo, "parentMissingFacade257", _chain("parentMissingFacade257", [
        _task("apply", "applicant"), _sub("subMissingFacade257", "missing257"),
    ]))
    facade = JeeflowFacade(eng, repo)
    result = await facade.flow("processInstance/startAndExecute", {
        "processDefineId": parent_def.id, "operator": "applicant",
    })
    # 内部异常码是 20010002；Python 门面统一失败信封仍为 99999999，msg 逐字透契约文案。
    assert result["code"] == 99999999
    assert result["msg"] == "子流程missing257定义不存在"
    assert len([inst for inst in repo._instances.values() if inst.parentId is not None]) == 0


@pytest.mark.asyncio
async def test_version_selection_latest_exact_and_no_state_filter():
    eng, repo, _events = _setup()
    # 先建高版本，再建更高 id 的低版本：缺省必须按 version DESC，不能误用 id DESC。
    v3_old = _add_define(repo, "versioned257", _chain("versioned257", [
        _task("apply", "applicant"), _task("reviewV3Old", "leader"),
    ]), version=3)
    v3_latest = _add_define(repo, "versioned257", _chain("versioned257", [
        _task("apply", "applicant"), _task("reviewV3Latest", "leader"),
    ]), version=3)
    v2 = _add_define(repo, "versioned257", _chain("versioned257", [
        _task("apply", "applicant"), _task("reviewV2", "leader"),
    ]), version=2)
    disabled = _add_define(repo, "disabled257", _chain("disabled257", [
        _task("apply", "applicant"), _task("disabledReview", "leader"),
    ]), version=1, state=0)

    latest_parent = _add_define(repo, "parentLatest257", _chain("parentLatest257", [
        _task("apply", "applicant"), _sub("subLatest257", "versioned257", form="__wrong__"),
    ]))
    latest = await _start_and_execute(eng, repo, latest_parent)
    latest_child = await _only_child(repo, latest.id)
    assert latest_child.defineId == v3_latest.id == max(v3_old.id, v3_latest.id)
    assert (await _only_doing(repo, latest_child.id)).taskName == "reviewV3Latest"

    exact_parent = _add_define(repo, "parentExact257", _chain("parentExact257", [
        _task("apply", "applicant"), _sub("subExact257", "versioned257", version=2),
    ]))
    exact = await _start_and_execute(eng, repo, exact_parent)
    assert (await _only_child(repo, exact.id)).defineId == v2.id

    disabled_parent = _add_define(repo, "parentDisabled257", _chain("parentDisabled257", [
        _task("apply", "applicant"), _sub("subDisabled257", "disabled257"),
    ]))
    disabled_instance = await _start_and_execute(eng, repo, disabled_parent)
    assert (await _only_child(repo, disabled_instance.id)).defineId == disabled.id


@pytest.mark.asyncio
@pytest.mark.parametrize("operator,expected", [
    (KEY_AUTO_ID, "applicant"),
    ("manager", "manager"),
])
async def test_child_args_strip_parent_controls_and_operator_fallback(operator, expected):
    eng, repo, _events = _setup()
    _add_define(repo, "childArgs257", _chain("childArgs257", [
        _task("apply", "applicant"), _task("review", "leader"),
    ]))
    parent_def = _add_define(repo, "parentArgs257", _chain("parentArgs257", [
        _task("apply", "applicant"), _sub("subArgs257", "childArgs257"),
    ]))
    parent = await eng.start_process_instance_by_id(parent_def.id, "applicant", {
        "f_business": "keep", "f_nextNodeOperator": "wrong-next",
    })
    apply = await _only_doing(repo, parent.id)
    if operator != KEY_AUTO_ID:
        await repo.add_task_actor(apply.id, [operator])
    await eng.execute_process_task(apply.id, operator, {
        KEY_SUBMIT_TYPE: 2,
        "tf_approvalComment": "parent-only",
        "tf_nextNodeOperator": "wrong-next",
        "f_nextNodeOperator": "wrong-next",
        "_branchCtx": [{"fork": "parent"}],
        "_reapplyContext": {"mode": "RETURN_TO_SOURCE"},
    })
    child = await _only_child(repo, parent.id)
    assert child.operator == expected
    assert child.variables["f_business"] == "keep"
    assert child.variables[KEY_SUBMIT_TYPE] == 0  # 子自身 apply 的提交类型，不是父的 2
    for key in ("tf_approvalComment", "tf_nextNodeOperator", "f_nextNodeOperator",
                "_branchCtx", "_reapplyContext"):
        assert key not in child.variables, key


def test_can_rejected_skips_subprocess_and_countersign_parent():
    sub_flow = parse_flow_model(_chain("guard257", [
        _task("apply", "applicant"), _sub("subGuard257", "child257"),
        _task("after257", "manager"),
    ]))
    assert not _can_rejected(sub_flow, "after257", "apply")

    cs_flow = parse_flow_model(_chain("guardCs257", [
        _task("apply", "applicant"), _task("cs", "leader", performType=1),
        _task("after257", "manager"),
    ]))
    assert not _can_rejected(cs_flow, "after257", "cs")
    assert _can_rejected(cs_flow, "after257", "apply")


@pytest.mark.asyncio
async def test_child_reject_resumes_parent_once_and_events_are_not_duplicated():
    eng, repo, events = _setup()
    _add_define(repo, "childReject257", _chain("childReject257", [
        _task("apply", "applicant"), _task("childReview", "leader"),
    ]))
    parent_def = _add_define(repo, "parentReject257", _chain("parentReject257", [
        _task("apply", "applicant"), _sub("subReject257", "childReject257"),
        _task("parentAfter", "manager"),
    ]))
    parent = await _start_and_execute(eng, repo, parent_def)
    child = await _only_child(repo, parent.id)
    review = await _only_doing(repo, child.id)
    await eng.execute_and_jump_to_end(review.id, "leader", {})
    assert (await repo.find_instance_by_id(child.id)).state == InstanceState.REJECT
    assert (await _only_doing(repo, parent.id)).taskName == "parentAfter"
    await _complete(eng, await _only_doing(repo, parent.id), "manager")
    assert (await repo.find_instance_by_id(parent.id)).state == InstanceState.DONE
    for instance_id in (parent.id, child.id):
        assert len(_events(events, EventType.PROCESS_INSTANCE_START, instance_id)) == 1
        assert len(_events(events, EventType.PROCESS_INSTANCE_END, instance_id)) == 1
    assert await _task_start_count(repo, events, parent.id, "parentAfter") == 1


async def _manual_doing_task(eng, repo, instance_id, task_name, task_type, actor):
    task = ProcessTask(
        id=eng._next_id(),
        processInstanceId=instance_id,
        taskName=task_name,
        displayName=task_name,
        taskType=int(task_type),
        actorIds=[actor],
        createTime=datetime.now(),
        updateTime=datetime.now(),
    )
    task.variables["isFirstTaskNode"] = False
    await repo.save_task(task)
    return task


@pytest.mark.asyncio
async def test_child_end_rejects_unfinished_major_task():
    eng, repo, events = _setup()
    _add_define(repo, "childGuard257", _chain("childGuard257", [
        _task("apply", "applicant"), _task("childReview", "leader"),
    ]))
    parent_def = _add_define(repo, "parentGuard257", _chain("parentGuard257", [
        _task("apply", "applicant"), _sub("subGuard257", "childGuard257"),
    ]))
    parent = await _start_and_execute(eng, repo, parent_def)
    child = await _only_child(repo, parent.id)
    other = await _manual_doing_task(eng, repo, child.id, "otherMajor", TaskType.MAJOR, "other")
    review = next(t for t in await repo.find_doing_tasks(child.id) if t.taskName == "childReview")
    with pytest.raises(ValueError, match=r"^存在未完成的主办任务,请确认\.$"):
        await _complete(eng, review, "leader")
    assert (await repo.find_task_by_id(other.id)).taskState == TaskState.DOING
    assert (await repo.find_instance_by_id(child.id)).state == InstanceState.DOING
    assert _events(events, EventType.PROCESS_INSTANCE_END, child.id) == []


@pytest.mark.asyncio
async def test_child_end_auto_completes_secondary_tasks():
    eng, repo, events = _setup()
    _add_define(repo, "childAidant257", _chain("childAidant257", [
        _task("apply", "applicant"), _task("childReview", "leader"),
    ]))
    parent_def = _add_define(repo, "parentAidant257", _chain("parentAidant257", [
        _task("apply", "applicant"), _sub("subAidant257", "childAidant257"),
        _task("parentAfter", "manager"),
    ]))
    parent = await _start_and_execute(eng, repo, parent_def)
    child = await _only_child(repo, parent.id)
    aidant = await _manual_doing_task(eng, repo, child.id, "assist", TaskType.SECONDARY, "helper")
    review = next(t for t in await repo.find_doing_tasks(child.id) if t.taskName == "childReview")
    await _complete(eng, review, "leader")
    finished = await repo.find_task_by_id(aidant.id)
    assert finished.taskState == TaskState.DONE
    assert finished.actorId == KEY_AUTO_ID
    assert (await repo.find_instance_by_id(child.id)).state == InstanceState.DONE
    assert (await _only_doing(repo, parent.id)).taskName == "parentAfter"
    assert sorted(evt.taskName for evt in _events(events, EventType.TASK_COMPLETE, child.id)) == [
        "apply", "assist", "childReview",
    ]
    assert len(_events(events, EventType.PROCESS_INSTANCE_END, child.id)) == 1


@pytest.mark.asyncio
async def test_regression_plain_flow_start_and_execute_is_unchanged():
    eng, repo, events = _setup()
    define = _add_define(repo, "plain257", _chain("plain257", [
        _task("apply", "applicant"), _task("review", "leader"),
    ]))
    inst = await _start_and_execute(eng, repo, define)
    assert (await _only_doing(repo, inst.id)).taskName == "review"
    assert (await _done(repo, inst.id, "apply")).actorId == KEY_AUTO_ID
    await _complete(eng, await _only_doing(repo, inst.id), "leader")
    assert (await repo.find_instance_by_id(inst.id)).state == InstanceState.DONE
    assert len(_events(events, EventType.PROCESS_INSTANCE_START, inst.id)) == 1
    assert len(_events(events, EventType.PROCESS_INSTANCE_END, inst.id)) == 1
