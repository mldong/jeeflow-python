"""任务时限动作 T0（spec 04 · 任务时限动作；工作流时限方案 §8 的引擎侧判据，Python 腿）。

与 go `engine/timelimit_test.go`、java `TimeLimitActionTest` 同构：
建行（autoExecute/reminderTime）、旁路覆盖（主建单/串行会签补建）、不继承档（协办）、
失效联动（办结/实例终态取消）、完成回调事实范围、可注入时钟、实例级排他锁调用面。
"""
import json
import os
import sys
from datetime import datetime

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository                       # noqa: E402
from jeeflow.model import ProcessDefine, SubmitType                     # noqa: E402
from jeeflow.spi import IDGenerator, TimeAction                          # noqa: E402

APPLY = int(SubmitType.APPLY)
AGREE = int(SubmitType.AGREE)
REJECT = int(SubmitType.REJECT)

_seq = 0


class _IDGen(IDGenerator):
    def __init__(self):
        self.n = 700000

    def next_id(self) -> int:
        self.n += 1
        return self.n


class _UserProv:
    async def get_user(self, uid):
        from jeeflow.model import UserInfo
        return UserInfo(userId=uid, realName="用户" + str(uid))


class _Expr:
    async def eval(self, expr, vars):
        return False


class _Sink:
    def __init__(self):
        self.actions = []
        self.cancelled_tasks = []
        self.cancelled_instances = []

    async def insert_time_action(self, action: TimeAction):
        self.actions.append(action)

    async def cancel_task_time_actions(self, task_id):
        self.cancelled_tasks.append(task_id)

    async def cancel_instance_time_actions(self, instance_id):
        self.cancelled_instances.append(instance_id)

    def of_type(self, typ):
        return [a for a in self.actions if a.type == typ]


class _LockRepo(MemoryRepository):
    def __init__(self):
        super().__init__()
        self.locked = []

    async def lock_instance(self, instance_id):
        self.locked.append(instance_id)


def _node(nid, ntype, props, text):
    return {"id": nid, "type": ntype, "x": 0, "y": 0, "properties": props, "text": {"value": text}}


def _edge(eid, src, dst):
    return {"id": eid, "type": "snaker:transition", "sourceNodeId": src,
            "targetNodeId": dst, "properties": {}}


def flow(review_props):
    global _seq
    _seq += 1
    r = {"form": "f", "taskType": 0, "performType": 0, "assignee": "userA"}
    r.update(review_props or {})
    return {"name": f"tl-{_seq}", "displayName": "时限夹具", "type": "approval",
            "nodes": [
                _node("start", "snaker:start", {}, "开始"),
                _node("apply", "snaker:task", {"form": "af", "taskType": 0, "performType": 0,
                                               "assignee": "applicant"}, "申请"),
                _node("review", "snaker:task", r, "审批"),
                _node("end", "snaker:end", {}, "结束"),
            ],
            "edges": [_edge("e1", "start", "apply"), _edge("e2", "apply", "review"),
                      _edge("e3", "review", "end")]}


class Harness:
    def __init__(self, review_props, now=datetime(2026, 11, 19, 9, 0, 0)):
        self.now = now
        self.repo = _LockRepo()
        self.sink = _Sink()
        self.eng = EngineImpl(self.repo, _UserProv(), _IDGen(), _Expr(),
                              time_action_sink=self.sink, clock=lambda: self.now)
        d = ProcessDefine(name=f"tl-{abs(hash(str(review_props)))}", displayName="时限夹具",
                          type="approval", state=1, version=1,
                          content=json.dumps(flow(review_props), ensure_ascii=False))
        self.repo.add_define(d)
        self.def_id = d.id

    async def start_review(self):
        inst = await self.eng.start_process_instance_by_id(self.def_id, "applicant", {})
        for t in await self.repo.find_doing_tasks(inst.id):
            if t.taskName == "apply":
                await self.eng.execute_process_task(t.id, "applicant", {"submitType": APPLY})
        inst = await self.repo.find_instance_by_id(inst.id)
        rows = [t for t in await self.repo.find_doing_tasks(inst.id) if t.taskName == "review"]
        return inst, rows


def _ms(t: datetime) -> int:
    return int(t.timestamp() * 1000)


@pytest.mark.asyncio
async def test_auto_and_reminder_on_create():
    h = Harness({"expireTime": "2h", "autoExecute": "Y", "reminderTime": "30m"})
    inst, rows = await h.start_review()
    assert len(rows) == 1
    review = rows[0]
    autos, reminds = h.sink.of_type("AUTO_COMPLETE"), h.sink.of_type("REMIND")
    assert len(autos) == 1 and len(reminds) == 1
    assert autos[0].idempotencyKey == f"wf:auto:{review.id}:0"
    assert reminds[0].idempotencyKey == f"wf:remind:{review.id}:1"
    assert reminds[0].occurrenceNo == 1
    assert autos[0].taskId == review.id and autos[0].instanceId == inst.id
    assert reminds[0].dueAt == _ms(h.now.replace(hour=9, minute=30))
    assert autos[0].dueAt == _ms(h.now.replace(hour=11, minute=0))


@pytest.mark.asyncio
async def test_not_configured_creates_no_action():
    h = Harness({})
    await h.start_review()
    assert h.sink.actions == []


@pytest.mark.asyncio
async def test_auto_execute_only_exact_Y():
    for v in ("N", "n", "y", "true"):
        h = Harness({"expireTime": "2h", "autoExecute": v})
        await h.start_review()
        assert h.sink.of_type("AUTO_COMPLETE") == [], f"autoExecute={v} 不得开启"


@pytest.mark.asyncio
async def test_reminder_hhmm_and_next_day():
    h = Harness({"reminderTime": "12:00"})
    await h.start_review()
    assert h.sink.of_type("REMIND")[0].dueAt == _ms(h.now.replace(hour=12, minute=0))
    h2 = Harness({"reminderTime": "08:00"})
    await h2.start_review()
    want = h2.now.replace(day=20, hour=8, minute=0)
    assert h2.sink.of_type("REMIND")[0].dueAt == _ms(want), "已过创建时刻应取次日"


@pytest.mark.asyncio
async def test_invalid_reminder_does_not_break_flow():
    h = Harness({"reminderTime": "2.5h"})
    inst, rows = await h.start_review()
    assert rows, "流程不得因误配中断"
    assert h.sink.of_type("REMIND") == []


@pytest.mark.asyncio
async def test_parallel_countersign_every_member():
    h = Harness({"assignee": "userA,userB", "performType": 1, "countersignType": "PARALLEL",
                 "reminderTime": "1h"})
    inst, rows = await h.start_review()
    assert len(rows) == 2
    reminds = h.sink.of_type("REMIND")
    assert {a.taskId for a in reminds} == {r.id for r in rows}


@pytest.mark.asyncio
async def test_sequential_advance_gets_action():
    h = Harness({"assignee": "userA,userB", "performType": 1, "countersignType": "SEQUENTIAL",
                 "expireTime": "2h", "autoExecute": "Y"})
    inst, rows = await h.start_review()
    first = rows[0]
    assert any(a.taskId == first.id for a in h.sink.of_type("AUTO_COMPLETE"))
    await h.eng.execute_process_task(first.id, "userA", {"submitType": AGREE})
    after = await h.repo.find_instance_by_id(inst.id)
    nxt = [t for t in await h.repo.find_doing_tasks(after.id)
           if t.taskName == "review" and t.id != first.id]
    assert len(nxt) == 1, "串行会签应推进出下一位"
    assert any(a.taskId == nxt[0].id for a in h.sink.of_type("AUTO_COMPLETE")), \
        "推进出的下一位缺自动办理行（旁路覆盖）"
    assert first.id in h.sink.cancelled_tasks


@pytest.mark.asyncio
async def test_aidant_rows_inherit_no_action():
    h = Harness({"expireTime": "2h", "autoExecute": "Y", "reminderTime": "1h"})
    inst, rows = await h.start_review()
    before = len(h.sink.actions)
    await h.eng.aidant_task(rows[0].id, ["aid1"], "", False, "userA")
    assert len(h.sink.actions) == before, "协办行不得继承时限动作"


@pytest.mark.asyncio
async def test_callback_on_complete_only():
    h = Harness({"callback": "demoHandler", "reminderTime": "1h"})
    inst, rows = await h.start_review()
    review = rows[0]
    await h.eng.execute_process_task(review.id, "userA", {"submitType": AGREE})
    cbs = h.sink.of_type("CALLBACK")
    assert len(cbs) == 1
    assert cbs[0].idempotencyKey == f"wf:callback:{review.id}:0"
    assert review.id in h.sink.cancelled_tasks

    h2 = Harness({"callback": "demoHandler"})
    inst2, rows2 = await h2.start_review()
    await h2.eng.execute_and_jump_to_end(rows2[0].id, "userA", {"submitType": REJECT})
    assert h2.sink.of_type("CALLBACK") == [], "退回不得触发完成回调"


@pytest.mark.asyncio
async def test_instance_terminal_cancels_actions():
    h = Harness({"reminderTime": "1h"})
    inst, rows = await h.start_review()
    await h.eng.execute_process_task(rows[0].id, "userA", {"submitType": AGREE})
    assert inst.id in h.sink.cancelled_instances


@pytest.mark.asyncio
async def test_lock_instance_called_on_execute():
    h = Harness({})
    inst, rows = await h.start_review()
    await h.eng.execute_process_task(rows[0].id, "userA", {"submitType": AGREE})
    assert inst.id in h.repo.locked, "仓储实现 lock_instance 时办理必须取得实例级排他"
