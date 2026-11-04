"""「已办撤回（recall）」验收（issues/246，Python 腿）——移植 jeeflow-java 参考实现 90469a2 的
`RecallTest` 用例形状（Go 腿 engine/recall_test.go 同构）：复活三件套（原行留 FINISHED＋
tf_recallHistory、下游 DOING 废弃、同节点新行名册照旧）、窗口两档（宽口径放行／下游已办结拦
零写入）、五条逐字文案、后加签行连带废弃、并行分支全废弃、解析期禁配、实例非 DOING、
门面 action 信封、码 10 TASK_RECALL 事件。

夹具走内联 JSON（test_add_sign_after.py 同款纪律，**不碰共享 flows/ 目录**）；
断言一律落在读回的持久值（repo.find_doing_tasks / find_history_tasks / find_task_by_id）上。
"""
import json
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository                      # noqa: E402
from jeeflow import recall as RC                                      # noqa: E402
from jeeflow.facade import JeeflowFacade                             # noqa: E402
from jeeflow.model import (InstanceState, ProcessDefine, SubmitType,  # noqa: E402
                           UserInfo, parse_flow_model)
from jeeflow.spi import IDGenerator                                   # noqa: E402

APPLY = int(SubmitType.APPLY)
AGREE = int(SubmitType.AGREE)

_TIME_RE = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
_seq = 0


class _IDGen(IDGenerator):
    """顺序 ID：测试要拿 taskId 作血缘/账本断言，时间戳发生器会把断言变成噪声。"""

    def __init__(self):
        self.n = 800000

    def next_id(self) -> int:
        self.n += 1
        return self.n


class _UserProv:
    async def get_user(self, uid):
        return UserInfo(userId=uid, realName="用户" + str(uid), deptId="D01", deptName="测试部门")


class _Expr:
    async def eval(self, expr, vars):
        return False


def setup():
    repo = MemoryRepository()
    eng = EngineImpl(repo, _UserProv(), _IDGen(), _Expr())
    return eng, repo


def setup_facade():
    repo = MemoryRepository()
    eng = EngineImpl(repo, _UserProv(), _IDGen(), _Expr())
    return eng, repo, JeeflowFacade(eng, repo)


# ─── 夹具 ────────────────────────────────────────────────────────────────────

def _node(nid: str, ntype: str, props: dict, text: str) -> dict:
    return {"id": nid, "type": ntype, "x": 0, "y": 0, "properties": props, "text": {"value": text}}


def _edge(eid: str, src: str, dst: str) -> dict:
    return {"id": eid, "type": "snaker:transition", "sourceNodeId": src, "targetNodeId": dst,
            "properties": {}}


def chain(apply_props: dict, review_props: dict) -> dict:
    """start → apply → review → final → end（java RecallTest.chain 同形）。"""
    global _seq
    _seq += 1
    a = dict(apply_props) if apply_props else {"assignee": "applicant"}
    a.setdefault("assignee", "applicant")
    r = {"form": "form-r", "assignee": "userA", "taskType": 0, "performType": 0}
    r.update(review_props or {})
    return {
        "name": f"recall-{_seq}", "displayName": "已办撤回夹具", "type": "approval",
        "nodes": [
            _node("start", "snaker:start", {}, "开始"),
            _node("apply", "snaker:task", {"form": "apply-form", "taskType": 0, "performType": 0, **a},
                  "发起申请"),
            _node("review", "snaker:task", r, "审批"),
            _node("final", "snaker:task", {"form": "form-f", "assignee": "userF", "taskType": 0,
                                           "performType": 0}, "终审"),
            _node("end", "snaker:end", {}, "结束"),
        ],
        "edges": [_edge("e1", "start", "apply"), _edge("e2", "apply", "review"),
                  _edge("e3", "review", "final"), _edge("e4", "final", "end")],
    }


def chain_deep(review_props: dict) -> dict:
    """六节点链：start→apply→review→final→pay→end，供"下游已办结"窗口档（java chainDeep）。"""
    global _seq
    _seq += 1
    r = {"form": "form-r", "assignee": "userA", "taskType": 0, "performType": 0}
    r.update(review_props or {})
    return {
        "name": f"recalldeep-{_seq}", "displayName": "已办撤回深链", "type": "approval",
        "nodes": [
            _node("start", "snaker:start", {}, "开始"),
            _node("apply", "snaker:task", {"form": "apply-form", "taskType": 0,
                                           "performType": 0, "assignee": "applicant"}, "发起申请"),
            _node("review", "snaker:task", r, "审批"),
            _node("final", "snaker:task", {"form": "form-f", "assignee": "userF", "taskType": 0,
                                           "performType": 0}, "终审"),
            _node("pay", "snaker:task", {"form": "form-p", "assignee": "userP", "taskType": 0,
                                         "performType": 0}, "支付"),
            _node("end", "snaker:end", {}, "结束"),
        ],
        "edges": [_edge("e1", "start", "apply"), _edge("e2", "apply", "review"),
                  _edge("e3", "review", "final"), _edge("e4", "final", "pay"),
                  _edge("e5", "pay", "end")],
    }


def chain_fork(review_props: dict) -> dict:
    """并行双支：review 出两条边到 payA/payB，各自汇入 end；供"分支全废弃"档（java chainFork）。"""
    global _seq
    _seq += 1
    r = {"form": "form-r", "assignee": "userA", "taskType": 0, "performType": 0}
    r.update(review_props or {})
    return {
        "name": f"recallfork-{_seq}", "displayName": "已办撤回并行", "type": "approval",
        "nodes": [
            _node("start", "snaker:start", {}, "开始"),
            _node("apply", "snaker:task", {"form": "apply-form", "taskType": 0,
                                           "performType": 0, "assignee": "applicant"}, "发起申请"),
            _node("review", "snaker:task", r, "审批"),
            _node("payA", "snaker:task", {"form": "form-a", "assignee": "userPA", "taskType": 0,
                                          "performType": 0}, "支付甲"),
            _node("payB", "snaker:task", {"form": "form-b", "assignee": "userPB", "taskType": 0,
                                          "performType": 0}, "支付乙"),
            _node("end", "snaker:end", {}, "结束"),
        ],
        "edges": [_edge("e1", "start", "apply"), _edge("e2", "apply", "review"),
                  _edge("e3", "review", "payA"), _edge("e4", "review", "payB"),
                  _edge("e5", "payA", "end"), _edge("e6", "payB", "end")],
    }


RECALL_ON = {"allowRecall": True}


def register(repo, flow: dict) -> ProcessDefine:
    d = ProcessDefine(name=flow["name"], displayName=flow.get("displayName", ""),
                      type=flow.get("type", ""), state=1, version=1,
                      content=json.dumps(flow, ensure_ascii=False))
    repo.add_define(d)
    return d


async def start_to_review(review_props):
    """发起并办结申请节点，返回停在 review 的实例（元组带引擎与仓储，测试直接续用）。"""
    eng, repo = setup()
    d = register(repo, chain({"assignee": "applicant"}, review_props))
    inst = await eng.start_process_instance_by_id(d.id, "applicant", {})
    for t in await repo.find_doing_tasks(inst.id):
        await eng.execute_process_task(t.id, "applicant", {"submitType": APPLY})
    return eng, repo, await repo.find_instance_by_id(inst.id)


async def doing(repo, iid, name):
    for t in await repo.find_doing_tasks(iid):
        if t.taskName == name:
            return t
    return None


async def doing_count(repo, iid, name) -> int:
    return sum(1 for t in await repo.find_doing_tasks(iid) if t.taskName == name)


async def rows(repo, iid, name) -> int:
    return sum(1 for t in await repo.find_history_tasks(iid) if t.taskName == name)


async def finished_row(repo, iid, name):
    for t in await repo.find_history_tasks(iid):
        if t.taskName == name and t.is_finished():
            return t
    return None


async def any_row(repo, iid, name):
    """不按状态滤——撤回后的下游行是 ABANDONED(99)，不走 FINISHED（java anyRow 同形）。"""
    for t in await repo.find_history_tasks(iid):
        if t.taskName == name:
            return t
    return None


# ═══ 用例（照 java RecallTest 9 格＋码 10 事件格＋门面信封格） ═══════════════════

@pytest.mark.asyncio
async def test01_revives_abandons_downstream_and_allows_redo():
    """正向：办结→撤回→原行留账本、复活行名册照旧、下游废弃、重办后再推进（复活三件套）。"""
    eng, repo, inst = await start_to_review(RECALL_ON)
    iid = inst.id
    review = await doing(repo, iid, "review")
    await eng.execute_process_task(review.id, "userA", {"submitType": AGREE})
    old_review = await finished_row(repo, iid, "review")
    assert old_review is not None, "前置：review 应有一条 FINISHED 行"
    assert await doing(repo, iid, "final") is not None, "前置：final 应有 1 条待办"

    resurrected = await eng.recall_task(old_review.id, "userA")

    assert resurrected.taskName == "review" and resurrected.is_doing(), \
        f"复活行应为 review 且 DOING，实得 {resurrected.taskName}/{resurrected.taskState}"
    assert "userA" in (resurrected.actorIds or []), f"复活行名册应照原行，实得 {resurrected.actorIds}"
    assert resurrected.parentTaskId == old_review.parentTaskId, "复活行 parent 应＝原行 parent"
    # 下游废弃
    final_row = await any_row(repo, iid, "final")
    assert final_row is not None and int(final_row.taskState) == 99, \
        f"下游 final 行应废弃（99），实得 {final_row.taskState}"
    assert await doing(repo, iid, "final") is None, "下游不应残留待办"
    # 原行不改写＋账本指向复活行
    reloaded = await repo.find_task_by_id(old_review.id)
    assert reloaded.is_finished(), "原 FINISHED 行不得被改写"
    ledger = RC.recall_history_of(reloaded)
    assert len(ledger) == 1, f"原行应有 1 条 tf_recallHistory，实得 {len(ledger)}"
    assert ledger[0]["operator"] == "userA"
    assert ledger[0]["recalledTaskId"] == resurrected.id, \
        f"账本 recalledTaskId 应指向复活行 {resurrected.id}，实得 {ledger[0]['recalledTaskId']}"
    assert _TIME_RE.match(str(ledger[0]["time"])), \
        f"账本时间须是全生态格式串，实得 {ledger[0]['time']}"
    # 实例仍进行中，重办后再推进
    inst_now = await repo.find_instance_by_id(iid)
    assert int(inst_now.state) == int(InstanceState.DOING), "实例应仍进行中（10）"
    await eng.execute_process_task(resurrected.id, "userA", {"submitType": AGREE})
    assert await doing(repo, iid, "final") is not None, "重办后应推进到 final"
    assert await rows(repo, iid, "final") == 2, "final 应共两行（废弃＋新办）"


@pytest.mark.asyncio
async def test02_blocked_when_downstream_finished_zero_write():
    """窗口：下游已办结 ⇒ 逐字文案报错且零写入。"""
    eng, repo = setup()
    d = register(repo, chain_deep({**RECALL_ON}))
    inst = await eng.start_process_instance_by_id(d.id, "applicant", {})
    for t in await repo.find_doing_tasks(inst.id):
        await eng.execute_process_task(t.id, "applicant", {"submitType": APPLY})
    iid = inst.id
    review = await doing(repo, iid, "review")
    await eng.execute_process_task(review.id, "userA", {"submitType": AGREE})
    final_task = await doing(repo, iid, "final")
    await eng.execute_process_task(final_task.id, "userF", {"submitType": AGREE})
    old_review = await finished_row(repo, iid, "review")
    assert old_review is not None and await doing(repo, iid, "pay") is not None, \
        "前置：review 已办结、pay 应有待办"

    with pytest.raises(ValueError) as ei:
        await eng.recall_task(old_review.id, "userA")
    assert str(ei.value) == "任务[review]的后续节点已办理，不能撤回", str(ei.value)
    # 零写入：无账本、无复活行、下游原样
    assert RC.recall_history_of(await repo.find_task_by_id(old_review.id)) == [], \
        "报错零写入：原行不得出现账本"
    f = await any_row(repo, iid, "final")
    assert f is not None and f.is_finished(), "报错零写入：final 行不得被改写"
    assert await doing(repo, iid, "pay") is not None, "报错零写入：pay 待办不得被废弃"
    assert await doing_count(repo, iid, "review") == 0, "报错零写入：review 不得出现复活行"


@pytest.mark.asyncio
async def test03_blocked_when_switch_off():
    """开关未开 ⇒ 逐字文案。"""
    eng, repo, inst = await start_to_review({})
    iid = inst.id
    review = await doing(repo, iid, "review")
    await eng.execute_process_task(review.id, "userA", {"submitType": AGREE})
    old_review = await finished_row(repo, iid, "review")
    with pytest.raises(ValueError) as ei:
        await eng.recall_task(old_review.id, "userA")
    assert str(ei.value) == "节点[review]未开启 allowRecall，不能撤回", str(ei.value)


@pytest.mark.asyncio
async def test04_permission_and_not_finished_guards():
    """非参与者拦、特权放行；进行中行拦。"""
    eng, repo, inst = await start_to_review(RECALL_ON)
    iid = inst.id
    review = await doing(repo, iid, "review")
    await eng.execute_process_task(review.id, "userA", {"submitType": AGREE})
    old_review = await finished_row(repo, iid, "review")
    # 非参与者
    with pytest.raises(ValueError) as ei:
        await eng.recall_task(old_review.id, "userZ")
    assert str(ei.value) == "无权限撤回该任务", str(ei.value)
    # flow.admin 特权放行
    resurrected = await eng.recall_task(old_review.id, "flow.admin")
    assert resurrected.is_doing(), "flow.admin 撤回后的复活行应为 DOING"
    # 进行中行拦（复活行还没办结）
    with pytest.raises(ValueError) as ei:
        await eng.recall_task(resurrected.id, "userA")
    assert str(ei.value) == "任务非已完成状态，不可撤回", str(ei.value)


@pytest.mark.asyncio
async def test05_blocked_when_instance_finished():
    """实例已走完 ⇒ 实例非进行中档。"""
    eng, repo, inst = await start_to_review(RECALL_ON)
    iid = inst.id
    review = await doing(repo, iid, "review")
    await eng.execute_process_task(review.id, "userA", {"submitType": AGREE})
    final_task = await doing(repo, iid, "final")
    await eng.execute_process_task(final_task.id, "userF", {"submitType": AGREE})
    inst_now = await repo.find_instance_by_id(iid)
    assert int(inst_now.state) == int(InstanceState.DONE), "前置：实例应已办结（20）"
    old_review = await finished_row(repo, iid, "review")
    with pytest.raises(ValueError) as ei:
        await eng.recall_task(old_review.id, "userA")
    assert str(ei.value) == "流程实例非进行中状态，不可撤回", str(ei.value)


@pytest.mark.asyncio
async def test06_abandons_add_sign_rows_too():
    """后加签行属下游 DOING：撤回连带废弃，不留孤儿待办；复活后可正常推进。"""
    eng, repo, inst = await start_to_review({**RECALL_ON, "allowAddSign": True})
    iid = inst.id
    review = await doing(repo, iid, "review")
    await eng.execute_process_task(review.id, "userA",
                                   {"submitType": AGREE, "tf_addSignOperators": "userX"})
    add_sign_row = await doing(repo, iid, "review")
    assert add_sign_row is not None and "userX" in (add_sign_row.actorIds or []), \
        "前置：加签行 userX 应在 review 节点续办"
    old_review = await finished_row(repo, iid, "review")

    await eng.recall_task(old_review.id, "userA")

    # 加签行废弃、复活行是节点上唯一 DOING
    row = await repo.find_task_by_id(add_sign_row.id)
    assert int(row.taskState) == 99, f"加签行应连带废弃（99），实得 {row.taskState}"
    assert await doing_count(repo, iid, "review") == 1, "review 应只剩复活行 1 条待办"
    # 复活行照常推进（无兄弟待办 ⇒ merged 出边）
    resurrected = await doing(repo, iid, "review")
    await eng.execute_process_task(resurrected.id, "userA", {"submitType": AGREE})
    assert await doing(repo, iid, "final") is not None, "复活行办结后应推进到 final"


@pytest.mark.asyncio
async def test07_abandons_both_fork_branches():
    """并行分支：撤回后两支全部废弃。"""
    eng, repo = setup()
    d = register(repo, chain_fork(RECALL_ON))
    inst = await eng.start_process_instance_by_id(d.id, "applicant", {})
    for t in await repo.find_doing_tasks(inst.id):
        await eng.execute_process_task(t.id, "applicant", {"submitType": APPLY})
    iid = inst.id
    review = await doing(repo, iid, "review")
    await eng.execute_process_task(review.id, "userA", {"submitType": AGREE})
    assert await doing(repo, iid, "payA") is not None and await doing(repo, iid, "payB") is not None, \
        "前置：payA/payB 两支应各有一条待办"
    old_review = await finished_row(repo, iid, "review")

    await eng.recall_task(old_review.id, "userA")

    for name in ("payA", "payB"):
        row = await any_row(repo, iid, name)
        assert row is not None and int(row.taskState) == 99, \
            f"{name} 行应废弃（99），实得 {row.taskState if row else None}"
    assert await doing_count(repo, iid, "review") == 1, "review 应只剩复活行"


def test08_parse_bans():
    """解析期禁配：会签节点/申请节点配 allowRecall 直接报错；非法布尔值同拦。"""
    with pytest.raises(ValueError) as ei:
        parse_flow_model(chain({"assignee": "applicant"},
                               {**RECALL_ON, "performType": 1, "countersignType": "PARALLEL"}))
    assert "会签节点" in str(ei.value) and "不允许配置 allowRecall" in str(ei.value), str(ei.value)
    with pytest.raises(ValueError) as ei:
        # 申请节点（start 直接后继）配 allowRecall ⇒ 解析期报错
        parse_flow_model(chain({"assignee": "applicant", **RECALL_ON}, {}))
    assert "首任务节点" in str(ei.value) and "allowRecall" in str(ei.value), str(ei.value)
    with pytest.raises(ValueError) as ei:
        parse_flow_model(chain({"assignee": "applicant"}, {"allowRecall": "yes"}))
    assert "allowRecall 配置非法" in str(ei.value), str(ei.value)


def test09_pinned_messages():
    """消息常量与 spec 06 逐字文案一致性自证（防常量被改后测试仍绿）。"""
    assert RC.MSG_WINDOW == "任务[{0}]的后续节点已办理，不能撤回"
    assert RC.MSG_SWITCH_OFF == "节点[{0}]未开启 allowRecall，不能撤回"
    assert RC.MSG_NOT_FINISHED == "任务非已完成状态，不可撤回"
    assert RC.MSG_NO_PERMISSION == "无权限撤回该任务"
    assert RC.MSG_INSTANCE_NOT_DOING == "流程实例非进行中状态，不可撤回"
    assert RC.MSG_COUNTERSIGN_FORBID == '会签节点[{0}]不允许配置 allowRecall：撤"谁的一票"语义不清'
    assert RC.MSG_FIRST_TASK_FORBID == ("首任务节点[{0}]不允许配置 allowRecall："
                                        "申请节点走实例撤回＋重新发起，不需要同实例复活")
    assert RC.PROP_ALLOW_RECALL == "allowRecall"
    assert RC.KEY_RECALL_HISTORY == "tf_recallHistory"


@pytest.mark.asyncio
async def test10_fires_task_recall_event():
    """撤回落库后 fire 码 10 TASK_RECALL（sourceId=旧行），复活行待办另走码 3；
    只发 10 不补发 5/6（撤回不是办结也不是拒绝）。"""
    eng, repo, inst = await start_to_review(RECALL_ON)
    iid = inst.id
    review = await doing(repo, iid, "review")
    await eng.execute_process_task(review.id, "userA", {"submitType": AGREE})
    old_review = await finished_row(repo, iid, "review")

    events = []
    eng.add_event_listener(lambda e: events.append(e))
    await eng.recall_task(old_review.id, "userA")

    recalls = [e for e in events if e.type.name == "TASK_RECALL"]
    starts = [e for e in events if e.type.name == "PROCESS_TASK_START"]
    completes = [e for e in events if e.type.name == "TASK_COMPLETE"]
    rejects = [e for e in events if e.type.name == "TASK_REJECT"]
    assert len(recalls) == 1, f"撤回应恰好 fire 一次码 10，实得 {len(recalls)}"
    assert recalls[0].taskId == old_review.id, \
        f"码 10 sourceId 应＝被撤旧行 {old_review.id}，实得 {recalls[0].taskId}"
    assert recalls[0].instanceId == iid and recalls[0].operator == "userA", \
        "码 10 载荷应为 instanceId/operator"
    d = recalls[0].data
    assert d.get("taskId") == old_review.id and d.get("operator") == "userA", \
        f"码 10 data 应带 taskId/operator，实得 {d}"
    assert not completes and not rejects, \
        f"撤回不得补发 5/6，实得 complete={len(completes)} reject={len(rejects)}"
    assert len(starts) == 1, f"复活行待办应走既有建单腿 fire 一次码 3，实得 {len(starts)}"


@pytest.mark.asyncio
async def test11_facade_recall_action_envelope():
    """门面 processTask/recall：成功信封 processTaskId(string)/taskState；
    operator 必填、主键缺失各归各报错。"""
    eng, repo, facade = setup_facade()
    d = register(repo, chain({"assignee": "applicant"}, RECALL_ON))
    inst = await eng.start_process_instance_by_id(d.id, "applicant", {})
    for t in await repo.find_doing_tasks(inst.id):
        await eng.execute_process_task(t.id, "applicant", {"submitType": APPLY})
    iid = inst.id
    review = await doing(repo, iid, "review")
    await eng.execute_process_task(review.id, "userA", {"submitType": AGREE})
    old_review = await finished_row(repo, iid, "review")

    resp = await facade.flow("processTask/recall",
                             {"processTaskId": str(old_review.id), "operator": "userA"})
    assert resp["code"] == 0, resp
    assert isinstance(resp["data"]["processTaskId"], str), "出口 id 应 string 化"
    assert resp["data"]["taskState"] == 10, f"复活行应为 DOING(10)，实得 {resp['data']}"
    # 复活行真在库里
    resurrected = await doing(repo, iid, "review")
    assert resurrected is not None and resurrected.id == int(resp["data"]["processTaskId"]), \
        "门面返回的应正是复活行 id"

    resp2 = await facade.flow("processTask/recall", {"processTaskId": str(old_review.id)})
    assert resp2["code"] == 99999999 and resp2["msg"] == "operator 必填", resp2
    resp3 = await facade.flow("processTask/recall", {"operator": "userA"})
    assert resp3["code"] == 99999999 and resp3["msg"] == "processTaskId 缺失", resp3
