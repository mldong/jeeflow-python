"""「后加签（可重复）」验收（Python 栈）——移植 jeeflow-java `AddSignAfterTest` 的 16 格语义。

覆盖面（与基准逐格同判据）：链式续办、一轮多选等同圈兄弟、加给已办过的人合法、
四类写入口报错且**零写入**、三条解析期硬校验、拒绝(2)/退回发起人(6) 废弃未完成加签、
回归（不开开关逐字不变）、并发只放行一次、**根因守卫**（``tf_`` 控制键不得进实例变量）、
共享夹具 ``flows/19-add-sign-after.json`` 必须真被本栈消费。

夹具纪律照基准：**内联 JSON**（{@link #chain}），只有 test16 读共享 flows/ ——
往共享目录加文件会把它提前拖进十仓同步（那是协调仓那条腿的事）。

契约依据：jeeflow-doc `spec/04-engine-ops.md`「后加签（可重复）」＋「流程变量约定」末注、
`spec/06-facade.md` §2.7 与文末失败 msg 表、`spec/02-flow-definition.md` §4.3。
"""
import asyncio
import json
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository                      # noqa: E402
from jeeflow import add_sign as A                                     # noqa: E402
from jeeflow.model import (InstanceState, ProcessDefine, SubmitType,  # noqa: E402
                           TaskState, UserInfo, parse_flow_model)
from jeeflow.spi import IDGenerator                                   # noqa: E402

import flows_resolver                                                 # noqa: E402

APPLY = int(SubmitType.APPLY)
AGREE = int(SubmitType.AGREE)
REJECT = int(SubmitType.REJECT)
RETURN = int(SubmitType.ROLLBACK_TO_OPERATOR)

_TIME_RE = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
_seq = 0


class _IDGen(IDGenerator):
    """顺序 ID：测试要拿 taskId 作血缘断言，时间戳发生器会把断言变成噪声。"""

    def __init__(self):
        self.n = 900000

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


# ─── 夹具 ────────────────────────────────────────────────────────────────────

def _node(nid: str, ntype: str, props: dict, text: str) -> dict:
    return {"id": nid, "type": ntype, "x": 0, "y": 0, "properties": props, "text": {"value": text}}


def _edge(eid: str, src: str, dst: str) -> dict:
    return {"id": eid, "type": "snaker:transition", "sourceNodeId": src, "targetNodeId": dst,
            "properties": {}}


def chain(apply_props: dict, review_props: dict) -> dict:
    """start → apply → review → final → end。

    两参数分别控制**申请节点**与**审批节点**的 properties（None ⇒ 用默认档），
    与 java 基准的 `chain(applyProps, reviewProps)` 同一形状；名字带序号，避免多次注册撞名。
    """
    global _seq
    _seq += 1
    a = dict(apply_props) if apply_props else {"assignee": "applicant"}
    a.setdefault("assignee", "applicant")
    r = {"form": "form-r", "assignee": "userA", "taskType": 0, "performType": 0}
    r.update(review_props or {})
    return {
        "name": f"addsign-{_seq}", "displayName": "后加签夹具", "type": "approval",
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


ADD_SIGN_ON = {"allowAddSign": True}


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
    """该节点的全部任务行（含已完成/废弃）——"建了几条行"的读数口径。"""
    return sum(1 for t in await repo.find_history_tasks(iid) if t.taskName == name)


def first_actor(task) -> str:
    """进行中任务的参与者：取参与者列表首位（``actor_id`` 列恒空是家族不变量，办结才写）。"""
    ids = task.actorIds or []
    return ids[0] if ids else ""


def agree(operators) -> dict:
    return {"submitType": AGREE, A.KEY_ADD_SIGN_OPERATORS: operators}


def plain(submit_type: int = AGREE) -> dict:
    return {"submitType": submit_type}


def round_of(task, node: str = "review"):
    return task.variables.get(f"{A.VAR_ADD_SIGN_ROUND}_{node}")


# ─── 正向 ────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test01_chain_then_stop():
    """链式续办 A→B→C→不点人即流出：血缘串成链、轮次递增、账本落在已完成行。"""
    eng, repo, inst = await start_to_review(ADD_SIGN_ON)
    iid = inst.id
    a = await doing(repo, iid, "review")
    assert a is not None

    await eng.execute_process_task(a.id, "userA", agree("userB"))
    b = await doing(repo, iid, "review")
    assert b is not None, "被加签人应在同一节点收到新待办"
    assert b.parentTaskId == a.id, "血缘 parent＝刚办结那条"
    assert round_of(b) == 1, f"轮次 1: {b.variables}"
    assert await doing(repo, iid, "final") is None, "令牌不得流出本节点"
    assert await doing_count(repo, iid, "review") == 1, "本节点只应有 1 条待办"

    hist_a = A.history_of(await repo.find_task_by_id(a.id))
    assert len(hist_a) == 1, hist_a
    entry = hist_a[0]
    assert entry["fromActor"] == "userA"
    assert entry["toActors"] == ["userB"]
    assert entry["round"] == 1
    assert entry["operator"] == "userA"
    assert _TIME_RE.match(str(entry["time"])), f"时间须是全生态格式串: {entry['time']}"

    await eng.execute_process_task(b.id, "userB", agree("userC"))
    c = await doing(repo, iid, "review")
    assert c is not None
    assert round_of(c) == 2, "轮次递增"
    assert c.parentTaskId == b.id

    # C 不再点人 → 停止条件成立，流出本节点
    await eng.execute_process_task(c.id, "userC", plain())
    assert await doing(repo, iid, "review") is None, "不点人后不得再有本节点待办"
    fin = await doing(repo, iid, "final")
    assert fin is not None, "应流转到下一审批节点"
    assert await rows(repo, iid, "review") == 3, "review 共三行"

    await eng.execute_process_task(fin.id, "userF", plain())
    done = await repo.find_instance_by_id(iid)
    assert int(done.state) == int(InstanceState.DONE), "实例办结"


@pytest.mark.asyncio
async def test02_multi_select_waits_siblings():
    """数组形同判据（spec 06 §2.11）：一次点两人 ⇒ 同圈建两行，最后一位办结且没点人才放行。"""
    eng, repo, inst = await start_to_review(ADD_SIGN_ON)
    iid = inst.id
    a = await doing(repo, iid, "review")

    await eng.execute_process_task(a.id, "userA", agree(["userB", "userC"]))
    assert await doing_count(repo, iid, "review") == 2, "同圈应建两行待办"
    assert await doing(repo, iid, "final") is None

    b = await doing(repo, iid, "review")
    await eng.execute_process_task(b.id, first_actor(b), plain())
    assert await doing(repo, iid, "final") is None, "兄弟待办未完成时不得放行"
    assert await doing_count(repo, iid, "review") == 1

    c = await doing(repo, iid, "review")
    await eng.execute_process_task(c.id, first_actor(c), plain())
    assert await doing(repo, iid, "final") is not None, "最后一位办结且没点人才放行"
    assert await doing_count(repo, iid, "final") == 1, "放行只能发生一次"


@pytest.mark.asyncio
async def test03_re_add_previously_handled_is_legal():
    """加给已办过的人合法（A→B→A 乒乓），轮次继续计数。"""
    eng, repo, inst = await start_to_review(ADD_SIGN_ON)
    iid = inst.id
    a = await doing(repo, iid, "review")
    await eng.execute_process_task(a.id, "userA", agree("userB"))
    b = await doing(repo, iid, "review")
    await eng.execute_process_task(b.id, "userB", agree("userA"))
    back = await doing(repo, iid, "review")
    assert "userA" in back.actorIds, "加给已办过的人应合法"
    assert round_of(back) == 2, f"{back.variables}"


# ─── 负向：写入口报错且零写入 ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test04_mutex_with_next_node_operator_rejected():
    """与 tf_nextNodeOperator 同传 ⇒ 报错，且任务行/实例零改动（判据理由＝参数生命周期，spec 06 §2.7）。"""
    eng, repo, inst = await start_to_review(ADD_SIGN_ON)
    iid = inst.id
    a = await doing(repo, iid, "review")
    before = await rows(repo, iid, "review")
    with pytest.raises(ValueError) as ei:
        await eng.execute_process_task(a.id, "userA",
                                       dict(agree("userB"), tf_nextNodeOperator="userF"))
    assert str(ei.value) == A.MSG_MUTEX, str(ei.value)
    assert (await repo.find_task_by_id(a.id)).is_doing(), "报错时任务行不得被办结"
    assert await rows(repo, iid, "review") == before, "不得新增行"
    assert int((await repo.find_instance_by_id(iid)).state) == int(InstanceState.DOING)


@pytest.mark.asyncio
async def test05_switch_off_rejected():
    """节点未开开关却传键 ⇒ 报错（不得静默忽略），零写入。"""
    eng, repo, inst = await start_to_review({})       # review 不开 allowAddSign
    iid = inst.id
    a = await doing(repo, iid, "review")
    with pytest.raises(ValueError) as ei:
        await eng.execute_process_task(a.id, "userA", agree("userB"))
    assert "未开启 allowAddSign" in str(ei.value), str(ei.value)
    assert (await repo.find_task_by_id(a.id)).is_doing()
    assert await doing(repo, iid, "final") is None


@pytest.mark.asyncio
async def test06_self_rejected():
    """加给自己 ⇒ 报错（死循环入口），零写入。"""
    eng, repo, inst = await start_to_review(ADD_SIGN_ON)
    iid = inst.id
    a = await doing(repo, iid, "review")
    with pytest.raises(ValueError) as ei:
        await eng.execute_process_task(a.id, "userA", agree("userA"))
    assert str(ei.value) == A.MSG_SELF, str(ei.value)
    assert (await repo.find_task_by_id(a.id)).is_doing()
    assert await doing_count(repo, iid, "review") == 1, "不得多建待办"


@pytest.mark.asyncio
async def test07_max_rounds_rejected():
    """超上限 ⇒ 报错不强制放行（放行＝伪造"审批完成"），零写入；第 1 轮那位不点人仍可正常流出。"""
    eng, repo, inst = await start_to_review({**ADD_SIGN_ON, "addSignMaxRounds": 1})
    iid = inst.id
    a = await doing(repo, iid, "review")
    await eng.execute_process_task(a.id, "userA", agree("userB"))      # 第 1 轮，合法
    b = await doing(repo, iid, "review")
    with pytest.raises(ValueError) as ei:
        await eng.execute_process_task(b.id, "userB", agree("userC"))  # 第 2 轮，越界
    assert "已达上限 1" in str(ei.value), str(ei.value)
    assert (await repo.find_task_by_id(b.id)).is_doing(), "越界那次不得办结任务行"
    assert await doing(repo, iid, "final") is None
    await eng.execute_process_task(b.id, "userB", plain())
    assert await doing(repo, iid, "final") is not None


# ─── 解析期硬校验 ─────────────────────────────────────────────────────────────

def _parse_fails(flow: dict, fragment: str):
    with pytest.raises(ValueError) as ei:
        parse_flow_model(flow)
    assert fragment in str(ei.value), str(ei.value)


def test08_countersign_node_rejected_at_parse():
    """会签节点配 allowAddSign ⇒ 解析期报错（两档推进闸会打架）。"""
    _parse_fails(chain({"assignee": "applicant"},
                       {**ADD_SIGN_ON, "performType": 1, "countersignType": "PARALLEL"}), "会签节点")


def test09_first_task_node_rejected_at_parse():
    """首任务节点（申请节点）配 allowAddSign ⇒ 解析期报错（发起/重提语义与加签路由打架）。"""
    _parse_fails(chain({"assignee": "applicant", **ADD_SIGN_ON}, ADD_SIGN_ON), "首任务节点")


def test10_invalid_config_values_rejected_at_parse():
    """非法值报配置错误，不得静默当缺省（纪律照 reapplyMode 先例）。"""
    _parse_fails(chain({}, {"allowAddSign": "yes"}), "allowAddSign 配置非法")
    _parse_fails(chain({}, {**ADD_SIGN_ON, "addSignMaxRounds": -1}), "不得为负")
    _parse_fails(chain({}, {**ADD_SIGN_ON, "addSignMaxRounds": "1.5"}), "必须是整数")


# ─── 废弃未完成加签任务 ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test11_reject_abandons_siblings():
    """不同意(2)：实例结束，本节点未完成的兄弟加签任务全部废弃，不留孤儿待办。"""
    eng, repo, inst = await start_to_review(ADD_SIGN_ON)
    iid = inst.id
    a = await doing(repo, iid, "review")
    await eng.execute_process_task(a.id, "userA", agree(["userB", "userC"]))
    assert await doing_count(repo, iid, "review") == 2

    b = await doing(repo, iid, "review")
    await eng.execute_and_jump_to_end(b.id, first_actor(b), plain(REJECT))

    assert int((await repo.find_instance_by_id(iid)).state) == 45, "实例应已结束"
    assert await doing_count(repo, iid, "review") == 0, "未完成的加签兄弟不得留待办"
    abandoned = [t for t in await repo.find_history_tasks(iid)
                 if t.taskName == "review" and t.taskState == TaskState.ABANDONED]
    assert len(abandoned) == 1, f"被废弃的那条应留废弃态: {len(abandoned)}"


@pytest.mark.asyncio
async def test12_return_to_applicant_abandons_siblings():
    """退回发起人(6)：申请待办生成，本节点未完成加签任务废弃。"""
    eng, repo, inst = await start_to_review(ADD_SIGN_ON)
    iid = inst.id
    a = await doing(repo, iid, "review")
    await eng.execute_process_task(a.id, "userA", agree(["userB", "userC"]))
    b = await doing(repo, iid, "review")
    await eng.execute_and_jump_to_first_task_node(b.id, first_actor(b), plain(RETURN))

    assert await doing(repo, iid, "apply") is not None, "应生成申请待办"
    assert await doing_count(repo, iid, "review") == 0, "本节点未完成加签任务应被废弃"
    assert await doing(repo, iid, "final") is None


# ─── 回归与并发 ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test13_plain_node_unchanged():
    """不开开关的节点行为逐字不变：同意即流出，且不因新判据受影响。"""
    eng, repo, inst = await start_to_review({})
    iid = inst.id
    a = await doing(repo, iid, "review")
    await eng.execute_process_task(a.id, "userA", plain())
    assert await doing(repo, iid, "final") is not None
    assert await rows(repo, iid, "review") == 1
    assert A.history_of(await repo.find_task_by_id(a.id)) == []


@pytest.mark.asyncio
async def test14_concurrent_finish_releases_once():
    """两个同圈加签任务并发办结：只放行一次（后续节点恰好一条待办，不重复创建）。

    这条同时钉住「坏样本」：把 merged 判据的第 2 档（本节点还有未完成待办）摘掉，
    第一个兄弟办结就会放行 ⇒ 后续节点出现两行，本格红。
    """
    eng, repo, inst = await start_to_review(ADD_SIGN_ON)
    iid = inst.id
    a = await doing(repo, iid, "review")
    await eng.execute_process_task(a.id, "userA", agree(["userB", "userC"]))
    siblings = [(t.id, first_actor(t)) for t in await repo.find_doing_tasks(iid) if t.taskName == "review"]
    assert len(siblings) == 2

    results = await asyncio.gather(*[eng.execute_process_task(tid, actor, plain())
                                     for tid, actor in siblings],
                                   return_exceptions=True)
    assert not [r for r in results if isinstance(r, Exception)], f"并发办结不应报错: {results}"
    assert await doing_count(repo, iid, "final") == 1, "后续节点只应有一条待办"
    assert await doing_count(repo, iid, "review") == 0


@pytest.mark.asyncio
async def test15_control_keys_never_enter_instance_variables():
    """根因守卫：控制类 ``tf_*`` 参数不得进实例变量（跨请求复活的病根），
    但任务行留痕必须完整——两者是同一枚判据的两面。"""
    eng, repo, inst = await start_to_review(ADD_SIGN_ON)
    iid = inst.id
    a = await doing(repo, iid, "review")
    await eng.execute_process_task(a.id, "userA",
                                   dict(agree("userB"), tf_approvalComment="请 B 复核"))

    instance_vars = (await repo.find_instance_by_id(iid)).variables
    assert A.KEY_ADD_SIGN_OPERATORS not in instance_vars, "tf_addSignOperators 不得进实例变量"
    assert "tf_approvalComment" not in instance_vars, "tf_approvalComment 不得进实例变量"
    for key in instance_vars:
        assert not key.startswith(A.TF_KEY_PREFIX), f"实例变量里不应有任何 tf_ 键: {key}"

    # 留痕仍在任务行上（修根因不得把审计信息一起改掉）
    row_vars = (await repo.find_task_by_id(a.id)).variables
    assert row_vars.get("tf_approvalComment") == "请 B 复核", row_vars
    assert row_vars.get(A.KEY_ADD_SIGN_OPERATORS) == "userB", row_vars
    assert len(A.history_of(await repo.find_task_by_id(a.id))) == 1


@pytest.mark.asyncio
async def test16_shared_fixture_chain_runs():
    """共享夹具 ``flows/19-add-sign-after.json`` 必须真被本栈消费。

    七语言仓 flows/ 是 java 编辑源的入库副本，各语言移植后加签时读的就是这一份——
    没有生产者核对过的夹具会在传播轮里各语言各自"改一份能跑的"。
    """
    eng, repo = setup()
    with open(os.path.join(flows_resolver.dir(), "19-add-sign-after.json"), "r", encoding="utf-8") as f:
        raw = json.loads(f.read())
    d = register(repo, raw)
    inst = await eng.start_process_instance_by_id(d.id, "applicant", {})
    iid = inst.id
    for t in await repo.find_doing_tasks(iid):
        await eng.execute_process_task(t.id, "applicant", plain(APPLY))

    a = await doing(repo, iid, "review")
    assert a is not None, "共享夹具的申请节点办结后应到审批节点"
    await eng.execute_process_task(a.id, "userA", agree("userB,userC"))
    assert await doing_count(repo, iid, "review") == 2, "逗号串两形应建两行"
    assert await doing(repo, iid, "final") is None

    # 先取快照再逐条办结：办结会改写仓储里的进行中集合，边遍历边办是并发修改
    siblings = [(t.id, first_actor(t)) for t in await repo.find_doing_tasks(iid) if t.taskName == "review"]
    for tid, actor in siblings:
        await eng.execute_process_task(tid, actor, plain())

    fin = await doing(repo, iid, "final")
    assert fin is not None, "同圈兄弟全部办结后应放行"
    assert await rows(repo, iid, "final") == 1
    await eng.execute_process_task(fin.id, "userF", plain())
    assert int((await repo.find_instance_by_id(iid)).state) == int(InstanceState.DONE)


# ─── 加签链上「退回上一步」(submitType=3) 禁 ───────────────────────────────────

@pytest.mark.asyncio
async def test17_rollback_on_chain_rejected():
    """后加签链上「退回上一步」(3) 被禁：当前行与血缘 parent 同节点 ⇒ 既有 canRejected 守卫不过。

    判据与 go/node/rust/c#/moon 五栈同一条（基准＝jeeflow-java `AddSignAfterTest` 那一格）：
    不走新错误码、不新增守卫，靠 `engine._rollback_to_parent` 里 `_can_rejected` 那句逐字文案拦住。
    ⚠️ 行读数按本栈真实形状写：**守卫排在办结之后**（issues/243，java 参考实现同形——
    `execute_and_jump_task` 先 `_prepare_execute_task`（内含 `inst.complete_task`）再
    `_rollback_to_parent` 判守卫；内存仓储无 `with_tx` ⇒ 无回滚），所以当前行 b 已是完成态、
    但绝不复活第三条同节点行；位序不在本轮改动范围，改判据来"好看"才是错的。
    """
    eng, repo, inst = await start_to_review(ADD_SIGN_ON)
    iid = inst.id
    a = await doing(repo, iid, "review")
    await eng.execute_process_task(a.id, "userA", agree("userB"))

    b = await doing(repo, iid, "review")
    assert b is not None and b.parentTaskId == a.id, "加签应在同节点新建 userB 那一行"
    assert first_actor(b) == "userB", f"新行办理人＝被加签人: {b.actorIds}"
    assert await doing(repo, iid, "final") is None

    with pytest.raises(ValueError) as ei:
        await eng.execute_and_jump_task(b.id, "userB", plain(int(SubmitType.ROLLBACK)))
    assert ("无法驳回至上一步处理，请确认上一步骤并非fork、join、suprocess以及会签任务"
            in str(ei.value)), str(ei.value)
    assert "2001000" not in str(ei.value), f"对外文案不带引擎内部码: {ei.value}"

    # 下游节点：既无待办也无历史行（守卫不过＝不建任何复活行）
    assert await doing(repo, iid, "final") is None, "不得流出下游节点待办"
    assert await rows(repo, iid, "final") == 0, "下游节点不得被建出行"
    assert await rows(repo, iid, "review") == 2, "review 只有 a/b 两行，不复活第三条"
    assert await doing_count(repo, iid, "review") == 0, "报错那次当前行已办结、一条复活待办都不建"
    assert int((await repo.find_instance_by_id(iid)).state) == int(InstanceState.DOING), \
        "实例不因这次被禁的退回而进终态"
