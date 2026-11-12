"""「动态协办（aidant）」验收（issues/253，Python 腿）——移植 jeeflow-java 参考实现
`AidantTest`（34 例）的用例形状（Go 腿 facade/aidant_253_test.go 同构）：

派生形状（task_type=1／parent＝主办行／同节点同名／performType=NORMAL／expire_time 同尺／
委托并入／码 3 既有腿）、推进闸（协办行办结不驱动，execute／jump／reject／退发起人四入口同闸）、
等待闸两档（waitCompletion=True 逐字文案＋零写入／缺省 False 随时推进＋离开节点清场记账）、
两跳账本存活、detail 投影形状、协办行能力边界（可加/摘人、可被转办、禁递归）、
负向六文案＋越权＋非 DOING、回归（会签票齐即推进不被协办挡／与征询并存两闸各数／撤回清场／
transfer·surrogate·addCandidate 不变）。

派生/闸门在引擎与门面——测试经 facade.flow 驱动（与 java 同层）；夹具内联 JSON，
不碰共享 flows/ 目录（与 test_dynamic_countersign.py／test_recall.py 同款纪律）。
既有测试期望值一字未改（唯一例外＝action 条数形状锚 7→8，本轮同批 +1，见
tests/test_action_permission.py）。
"""
import json
import os
import sys
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository                      # noqa: E402
from jeeflow import aidant as AD                                     # noqa: E402
from jeeflow import add_sign as A                                    # noqa: E402
from jeeflow import collect as C                                     # noqa: E402
from jeeflow.facade import JeeflowFacade                             # noqa: E402
from jeeflow.memory import MemoryExtRepository                       # noqa: E402
from jeeflow.model import (InstanceState, ProcessDefine, ProcessSurrogate,  # noqa: E402
                           SubmitType, TaskState, UserInfo)
from jeeflow.spi import IDGenerator                                   # noqa: E402
from jeeflow.action_permission import permission_codes                # noqa: E402

APPLY = int(SubmitType.APPLY)
AGREE = int(SubmitType.AGREE)
REJECT = int(SubmitType.REJECT)
JUMP = int(SubmitType.JUMP)
ROLLBACK = int(SubmitType.ROLLBACK)
TO_OPERATOR = int(SubmitType.ROLLBACK_TO_OPERATOR)

_seq = 0


class _IDGen(IDGenerator):
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


# ─── 夹具 ────────────────────────────────────────────────────────────────────

def _node(nid, ntype, props, text):
    return {"id": nid, "type": ntype, "x": 0, "y": 0, "properties": props, "text": {"value": text}}


def _edge(eid, src, dst):
    return {"id": eid, "type": "snaker:transition", "sourceNodeId": src, "targetNodeId": dst,
            "properties": {}}


def chain(review_props=None):
    """start → apply → review → final → end（java AidantTest.chain 同形）。"""
    global _seq
    _seq += 1
    r = {"form": "form-r", "assignee": "userA", "taskType": 0, "performType": 0}
    r.update(review_props or {})
    return {
        "name": f"aidant-{_seq}", "displayName": "动态协办夹具", "type": "approval",
        "nodes": [
            _node("start", "snaker:start", {}, "开始"),
            _node("apply", "snaker:task",
                  {"form": "apply-form", "taskType": 0, "performType": 0, "assignee": "applicant"},
                  "发起申请"),
            _node("review", "snaker:task", r, "审批"),
            _node("final", "snaker:task",
                  {"form": "form-f", "assignee": "userF", "taskType": 0, "performType": 0}, "终审"),
            _node("end", "snaker:end", {}, "结束"),
        ],
        "edges": [_edge("e1", "start", "apply"), _edge("e2", "apply", "review"),
                  _edge("e3", "review", "final"), _edge("e4", "final", "end")],
    }


def cs_chain():
    """并行会签链：start→apply→cs（userA,userB）→end（java countersignChain 同形）。"""
    global _seq
    _seq += 1
    return {
        "name": f"aidant-cs-{_seq}", "displayName": "协办×会签夹具", "type": "approval",
        "nodes": [
            _node("start", "snaker:start", {}, "开始"),
            _node("apply", "snaker:task",
                  {"form": "apply-form", "taskType": 0, "performType": 0, "assignee": "applicant"},
                  "发起申请"),
            _node("cs", "snaker:task",
                  {"form": "form-c", "taskType": 0, "performType": 1,
                   "assignee": "userA,userB", "countersignType": "PARALLEL"}, "会签审批"),
            _node("end", "snaker:end", {}, "结束"),
        ],
        "edges": [_edge("e1", "start", "apply"), _edge("e2", "apply", "cs"),
                  _edge("e3", "cs", "end")],
    }


def register(repo, flow):
    d = ProcessDefine(name=flow["name"], displayName=flow.get("displayName", ""),
                      type=flow.get("type", ""), state=1, version=1,
                      content=json.dumps(flow, ensure_ascii=False))
    repo.add_define(d)
    return d


class Fx:
    """一套协办验收夹具：引擎＋内存仓＋扩展仓＋门面＋事件码捕获＋实例 id。"""

    def __init__(self, eng, repo, ext, facade, iid, events):
        self.eng, self.repo, self.ext, self.facade = eng, repo, ext, facade
        self.iid, self.events = iid, events

    async def start(self, flow):
        d = register(self.repo, flow)
        inst = await self.eng.start_process_instance_by_id(d.id, "applicant", {})
        for t in await self.repo.find_doing_tasks(inst.id):
            await self.eng.execute_process_task(t.id, "applicant", {"submitType": APPLY})
        self.iid = inst.id
        return self

    # ── 驱动助手 ──

    async def aidant(self, host, operator, actor_ids=None, reason=None, wait=None):
        a = {"processTaskId": str(host.id), "operator": operator}
        if actor_ids is not None:
            a["actorIds"] = actor_ids
        if reason is not None:
            a["reason"] = reason
        if wait is not None:
            a["waitCompletion"] = wait
        return await self.facade.flow("processTask/aidant", a)

    async def exec(self, row, operator, submit_type, extra=None):
        a = {"processTaskId": str(row.id), "operator": operator, "submitType": submit_type}
        if extra:
            a.update(extra)
        return await self.facade.flow("processTask/execute", a)

    async def task(self, task_id):
        return await self.repo.find_task_by_id(task_id)

    async def all_tasks(self):
        return await self.repo.find_history_tasks(self.iid)

    async def rows(self, host):
        """协办行（经引擎判据读回的持久值），按 id 升序＝建单顺序。"""
        out = AD.aidant_rows(await self.all_tasks(), await self.task(host.id))
        return sorted(out, key=lambda t: t.id)

    async def ledger(self, host):
        return AD.history_of(await self.task(host.id))

    async def doing(self, name):
        for t in await self.repo.find_doing_tasks(self.iid):
            if t.taskName == name:
                return t
        return None

    async def doing_count(self, name):
        return sum(1 for t in await self.repo.find_doing_tasks(self.iid) if t.taskName == name)

    async def state(self):
        inst = await self.repo.find_instance_by_id(self.iid)
        return int(inst.state)

    async def detail_ext(self, task_id):
        r = await self.facade.flow("processTask/detail", {"id": str(task_id)})
        assert r["code"] == 0, r
        return (r["data"] or {}).get("ext") or {}


async def setup(review_props=None, flow=None):
    repo = MemoryRepository()
    ext = MemoryExtRepository()
    eng = EngineImpl(repo, _UserProv(), _IDGen(), _Expr())
    events = []
    eng.add_event_listener(lambda e: events.append(int(e.type)))
    facade = JeeflowFacade(eng, repo, ext)
    fx = Fx(eng, repo, ext, facade, 0, events)
    return await fx.start(flow if flow is not None else chain(review_props))


def ok(resp):
    assert resp["code"] == 0, resp


def msg(resp, want):
    assert resp["code"] == 99999999, f"应失败 code=99999999，实得 {resp}"
    assert resp["msg"] == want, f"文案应逐字 {want!r}，实得 {resp['msg']!r}"


def actors_of(v):
    return [str(x) for x in (v or [])]


# ═══ 正向：派生形状 ═══════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_derive_shapes_secondary_rows_and_keeps_host_doing():
    """派生：每人一行、task_type=1、parent＝主办行、同节点同名、performType=NORMAL、
    主办保留 DOING、账本六键、只走既有码 3。"""
    fx = await setup()
    host = await fx.doing("review")
    fx.events.clear()

    resp = await fx.aidant(host, "userA", ["userX", "userY"], "需两部门协办")
    ok(resp)
    assert resp["data"] is None, f"成功 data → null（规范 06），实得 {resp['data']}"

    host_row = await fx.task(host.id)
    assert host_row.is_doing(), "主办行应保留 DOING"
    assert host_row.actorIds == ["userA"], "主办行名册不动"
    assert not host_row.actorId, "严禁覆写任务 actor_id 列（转办那条 ⚠️ 同尺）"

    rows = await fx.rows(host)
    assert len(rows) == 2, "多人每人一行"
    got_actors = []
    for r in rows:
        assert r.is_doing()
        assert r.taskType == 1, f"task_type 应=1，实得 {r.taskType}"
        assert r.parentTaskId == host.id
        assert r.taskName == host.taskName and r.displayName == host.displayName
        assert r.performType == 0, "performType 应=NORMAL"
        assert r.variables.get(AD.KEY_AIDANT_REASON) == "需两部门协办"
        assert "tf_approvalComment" not in r.variables, "协办行不占 tf_approvalComment"
        assert not r.actorId, "进行中任务 actor_id 列恒无值"
        got_actors.append(r.actorIds[0])
        assert len(await fx.repo.find_task_actors(r.id)) == 1, "参与者表真落库"
    assert got_actors == ["userX", "userY"]

    lg = await fx.ledger(host)
    assert len(lg) == 1
    e0 = lg[0]
    assert list(e0.keys()) == ["round", "fromActor", "toActors", "reason", "time", "operator"], \
        f"账本六键锁定键序，实得 {list(e0.keys())}"
    assert e0["round"] == 1 and e0["fromActor"] == "userA" and e0["operator"] == "userA"
    assert e0["toActors"] == ["userX", "userY"]
    assert e0["reason"] == "需两部门协办"
    import re
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", e0["time"]), \
        f"time 一律 yyyy-MM-dd HH:mm:ss，实得 {e0['time']}"
    assert fx.events == [3, 3], f"只走既有码 3（逐协办行一次），实得 {fx.events}"


@pytest.mark.asyncio
async def test_reason_optional_falls_back_to_empty_string():
    fx = await setup()
    host = await fx.doing("review")
    ok(await fx.aidant(host, "userA", "userX"))
    assert (await fx.ledger(host))[0]["reason"] == ""
    assert AD.KEY_AIDANT_REASON not in (await fx.rows(host))[0].variables
    assert "tf_approvalComment" not in (await fx.task(host.id)).variables


@pytest.mark.asyncio
async def test_actor_ids_accepts_comma_string_and_array():
    fx = await setup()
    host = await fx.doing("review")
    ok(await fx.aidant(host, "userA", " userX , userY ,"))
    assert len(await fx.rows(host)) == 2


@pytest.mark.asyncio
async def test_expire_time_recomputed_and_surrogate_merged():
    fx = await setup({"expireTime": "2d"})
    await fx.ext.save_surrogate(ProcessSurrogate(
        operator="userX", surrogate="userS", processName="",
        startTime=datetime.now() - timedelta(hours=1),
        endTime=datetime.now() + timedelta(hours=1), enabled=1))
    host = await fx.doing("review")
    ok(await fx.aidant(host, "userA", "userX"))
    child = (await fx.rows(host))[0]
    assert child.expireTime is not None, "expire_time 按节点表达式同尺重算"
    assert child.expireTime > datetime.now() + timedelta(hours=47)
    assert "userX" in child.actorIds, "委托并入不摘原人"
    assert "userS" in child.actorIds, "委托照既有腿并入"
    assert len(await fx.repo.find_task_actors(child.id)) == 2, "落库参与者表也真带上"


# ═══ 正向：推进闸（四入口同闸） ═══════════════════════════════════════════════

@pytest.mark.asyncio
async def test_aidant_completion_does_not_drive():
    fx = await setup()
    host = await fx.doing("review")
    await fx.aidant(host, "userA", "userX")
    child = (await fx.rows(host))[0]
    fx.events.clear()

    created = await fx.eng.execute_process_task(child.id, "userX", {"submitType": AGREE})
    assert created == [], "协办办结不产生执行对象（无新任务）"
    assert (await fx.task(child.id)).is_finished()
    assert (await fx.task(host.id)).is_doing(), "主办行保留 DOING"
    assert await fx.doing("final") is None, "不跑输出变迁：下游节点无待办"
    assert await fx.state() == int(InstanceState.DOING)
    assert fx.events == [5], f"只走既有码 5，不新增事件码，实得 {fx.events}"


@pytest.mark.asyncio
async def test_aidant_jump_path_also_gated():
    """四入口同闸：JUMP／ROLLBACK（execute_and_jump_task）对协办行同样不驱动。"""
    fx = await setup()
    host = await fx.doing("review")
    await fx.aidant(host, "userA", ["userX", "userY"])
    rows = await fx.rows(host)

    ok(await fx.exec(rows[0], "userX", JUMP, {"taskName": "final"}))
    assert (await fx.task(rows[0].id)).is_finished()
    assert await fx.doing("final") is None, "跳转路同闸：不得凭空建出 final 待办"

    created = await fx.eng.execute_and_jump_task(rows[1].id, "userY",
                                                 {"submitType": ROLLBACK}, None)
    assert created == []
    assert (await fx.task(rows[1].id)).is_finished()
    assert (await fx.task(host.id)).is_doing(), "主办行仍在"
    assert await fx.doing_count("review") == 1


@pytest.mark.asyncio
async def test_aidant_reject_and_return_do_not_drive():
    fx = await setup()
    host = await fx.doing("review")
    await fx.aidant(host, "userA", ["userX", "userY"])
    rows = await fx.rows(host)

    ok(await fx.exec(rows[0], "userX", REJECT))
    assert await fx.state() == int(InstanceState.DOING), "拒绝不得结束整单"
    ok(await fx.exec(rows[1], "userY", TO_OPERATOR))
    assert await fx.state() == int(InstanceState.DOING)
    assert (await fx.task(host.id)).is_doing()
    assert await fx.doing("apply") is None


@pytest.mark.asyncio
async def test_host_completion_after_aidant_drives():
    fx = await setup()
    host = await fx.doing("review")
    await fx.aidant(host, "userA", "userX")
    await fx.exec((await fx.rows(host))[0], "userX", AGREE)
    ok(await fx.exec(host, "userA", AGREE))
    assert await fx.doing("final") is not None, "主办办结才推进"
    assert (await fx.task(host.id)).is_finished()


# ═══ 正向／负向：等待闸两档 ═══════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_wait_true_blocks_host_until_aidant_done():
    fx = await setup()
    host = await fx.doing("review")
    ok(await fx.aidant(host, "userA", ["userX", "userY"], None, True))
    rows = await fx.rows(host)
    ledger_before = len(await fx.ledger(host))

    msg(await fx.exec(host, "userA", AGREE), "节点[review]协办未办结（已办结 0/2），不能办理")
    # 零写入：主办仍 DOING、无下游待办、账本不增、协办行不动
    assert (await fx.task(host.id)).is_doing()
    assert await fx.doing("final") is None
    assert len(await fx.ledger(host)) == ledger_before
    for r in await fx.rows(host):
        assert r.is_doing()

    await fx.exec(rows[0], "userX", AGREE)
    msg(await fx.exec(host, "userA", AGREE), "节点[review]协办未办结（已办结 1/2），不能办理")

    await fx.exec(rows[1], "userY", AGREE)
    ok(await fx.exec(host, "userA", AGREE))
    assert await fx.doing("final") is not None


@pytest.mark.asyncio
async def test_wait_gate_blocks_every_submit_type():
    fx = await setup()
    host = await fx.doing("review")
    await fx.aidant(host, "userA", "userX", None, True)
    for st in (AGREE, REJECT, TO_OPERATOR):
        msg(await fx.exec(host, "userA", st), "节点[review]协办未办结（已办结 0/1），不能办理")
        assert (await fx.task(host.id)).is_doing()
    assert await fx.state() == int(InstanceState.DOING)


@pytest.mark.asyncio
async def test_wait_default_false_cleans_pending_on_leave():
    fx = await setup()
    host = await fx.doing("review")
    await fx.aidant(host, "userA", ["userX", "userY"])
    rows = await fx.rows(host)
    await fx.exec(rows[0], "userX", AGREE)

    ok(await fx.exec(host, "userA", AGREE))
    assert await fx.doing("final") is not None, "缺省 false 主办可随时推进"
    assert (await fx.task(rows[1].id)).taskState == TaskState.ABANDONED, "未办结协办行清场为废弃(99)"
    assert (await fx.task(rows[0].id)).taskState == TaskState.DONE, "已办结那条不改写"

    lg = await fx.ledger(host)
    assert len(lg) == 2, f"账本应 2 条（派生＋清场），实得 {len(lg)}"
    last = lg[1]
    assert last["abandoned"] is True
    assert last["toActors"] == ["userY"], "清场 toActors＝被废弃的人"
    assert last["round"] == 1, "清场不消耗轮次"
    assert await fx.doing_count("review") == 0, "不留孤儿待办"


@pytest.mark.asyncio
async def test_wait_completion_string_and_invalid_forms():
    fx = await setup()
    host = await fx.doing("review")
    ok(await fx.aidant(host, "userA", "userX", None, "true"))
    msg(await fx.exec(host, "userA", AGREE), "节点[review]协办未办结（已办结 0/1），不能办理")

    fx2 = await setup()
    h2 = await fx2.doing("review")
    ok(await fx2.aidant(h2, "userA", "userX", None, "不是布尔"))
    ok(await fx2.exec(h2, "userA", AGREE))
    assert await fx2.doing("final") is not None, "非法值按缺省 false，不新造文案"


@pytest.mark.asyncio
async def test_reject_and_return_to_operator_clean_aidant():
    fx = await setup()
    h1 = await fx.doing("review")
    await fx.aidant(h1, "userA", "userX")
    c1 = (await fx.rows(h1))[0]
    ok(await fx.exec(h1, "userA", REJECT))
    assert (await fx.task(c1.id)).taskState == TaskState.ABANDONED
    assert (await fx.ledger(h1))[1]["abandoned"] is True

    fx2 = await setup()
    h2 = await fx2.doing("review")
    await fx2.aidant(h2, "userA", "userX")
    c2 = (await fx2.rows(h2))[0]
    ok(await fx2.exec(h2, "userA", TO_OPERATOR))
    assert (await fx2.task(c2.id)).taskState == TaskState.ABANDONED
    assert await fx2.doing("apply") is not None, "退回发起人后申请节点复活"


# ═══ 正向：账本与投影 ═════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_two_rounds_ledger_survives_host_finish():
    fx = await setup()
    host = await fx.doing("review")
    await fx.aidant(host, "userA", "userX", "第一轮", False)
    await fx.exec((await fx.rows(host))[0], "userX", AGREE)
    await fx.aidant(host, "userA", "userY", "第二轮", False)

    lg = await fx.ledger(host)
    assert len(lg) == 2, "两跳账本＝2"
    assert lg[0]["round"] == 1 and lg[1]["round"] == 2
    assert lg[0]["reason"] == "第一轮" and lg[1]["reason"] == "第二轮"
    assert lg[0]["toActors"] == ["userX"] and lg[1]["toActors"] == ["userY"]

    await fx.exec((await fx.rows(host))[1], "userY", AGREE)
    ok(await fx.exec(host, "userA", AGREE))
    assert (await fx.task(host.id)).is_finished()
    assert len(await fx.ledger(host)) == 2, "办结后账本仍在"


@pytest.mark.asyncio
async def test_detail_projection_shape():
    fx = await setup()
    host = await fx.doing("review")
    await fx.aidant(host, "userA", ["userX", "userY"], "协办原因", True)
    rows = await fx.rows(host)

    p = (await fx.detail_ext(host.id)).get("aidant")
    assert p is not None, "主办行应输出 ext.aidant"
    assert list(p.keys()) == ["active", "round", "total", "submitted", "actors", "wait"], \
        f"投影六键锁定键序，实得 {list(p.keys())}"
    assert p["active"] is True and p["round"] == 1 and p["total"] == 2
    assert p["submitted"] == 0 and p["wait"] is True
    assert len(p["actors"]) == 2
    assert list(p["actors"][0].keys()) == ["actor", "taskId", "state"]
    assert p["actors"][0]["actor"] == "userX"
    assert str(p["actors"][0]["taskId"]) == str(rows[0].id)
    assert p["actors"][0]["state"] == int(TaskState.DOING)

    await fx.exec(rows[0], "userX", AGREE)
    p2 = (await fx.detail_ext(host.id))["aidant"]
    assert p2["submitted"] == 1 and p2["active"] is True

    fx2 = await setup()
    plain = await fx2.doing("review")
    assert (await fx2.detail_ext(plain.id)).get("aidant") is None, "无协办行不输出该投影"


# ═══ 负向：六文案＋越权＋非 DOING ═════════════════════════════════════════════

@pytest.mark.asyncio
async def test_negative_actor_ids_required():
    fx = await setup()
    host = await fx.doing("review")
    msg(await fx.aidant(host, "userA", None), "actorIds 必填")
    msg(await fx.aidant(host, "userA", []), "actorIds 必填")
    msg(await fx.aidant(host, "userA", "  ,  "), "actorIds 必填")
    assert len(await fx.rows(host)) == 0, "零写入"


@pytest.mark.asyncio
async def test_negative_operator_required():
    fx = await setup()
    host = await fx.doing("review")
    msg(await fx.aidant(host, None, "userX"), "operator 必填")
    msg(await fx.aidant(host, "   ", "userX"), "operator 必填")


@pytest.mark.asyncio
async def test_negative_task_not_found():
    fx = await setup()
    msg(await fx.facade.flow("processTask/aidant",
                             {"processTaskId": "999999", "operator": "userA", "actorIds": "userX"}),
        "任务不存在")


@pytest.mark.asyncio
async def test_negative_unauthorized_operator():
    fx = await setup()
    host = await fx.doing("review")
    msg(await fx.aidant(host, "userZ", "userX"), "无权限派生协办任务")
    assert len(await fx.rows(host)) == 0, "零写入"
    assert (await fx.task(host.id)).is_doing()


@pytest.mark.asyncio
async def test_roster_member_and_privileged_allowed():
    fx = await setup()
    host = await fx.doing("review")
    ok(await fx.facade.flow("processTask/addCandidate",
                            {"processTaskId": str(host.id), "actorIds": ["userB"]}))
    ok(await fx.aidant(host, "userB", "userX"))
    ok(await fx.aidant(host, "flow.auto", "userY"))
    ok(await fx.aidant(host, "flow.admin", "userZ"))
    assert len(await fx.rows(host)) == 3


@pytest.mark.asyncio
async def test_negative_host_not_doing():
    fx = await setup()
    host = await fx.doing("review")
    await fx.exec(host, "userA", AGREE)
    assert (await fx.task(host.id)).is_finished()
    msg(await fx.aidant(host, "userA", "userX"), "任务非进行中，不可派生协办")


@pytest.mark.asyncio
async def test_negative_duplicate_actor():
    fx = await setup()
    host = await fx.doing("review")
    await fx.aidant(host, "userA", "userX")
    msg(await fx.aidant(host, "userA", "userX"), "该人已有未办结的协办任务")
    assert len(await fx.rows(host)) == 1, "零写入：不重复建行"
    assert len(await fx.ledger(host)) == 1

    await fx.exec((await fx.rows(host))[0], "userX", AGREE)
    ok(await fx.aidant(host, "userA", "userX"))
    assert len(await fx.rows(host)) == 2, "已办结的那位可以再派生"


@pytest.mark.asyncio
async def test_negative_recursion_forbidden():
    fx = await setup()
    host = await fx.doing("review")
    await fx.aidant(host, "userA", "userX")
    child = (await fx.rows(host))[0]
    msg(await fx.aidant(child, "userX", "userY"), "协办任务不能再派生协办")
    assert len(await fx.rows(host)) == 1, "零写入"
    assert AD.aidant_rows(await fx.all_tasks(), await fx.task(child.id)) == []


# ═══ 协办行能力边界 ═══════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_aidant_row_supports_actor_writes_and_transfer():
    """协办行可加/摘参与人、可被转办（走既有 actor 写腿／transfer，不造特例）。"""
    fx = await setup()
    host = await fx.doing("review")
    await fx.aidant(host, "userA", "userX")
    cid = (await fx.rows(host))[0].id

    ok(await fx.facade.flow("processTask/surrogate",
                            {"processTaskId": str(cid), "actorIds": ["userK"]}))
    assert "userK" in await fx.repo.find_task_actors(cid)
    ok(await fx.facade.flow("processTask/addCandidate",
                            {"processTaskId": str(cid), "actorIds": ["userL"]}))
    assert "userL" in await fx.repo.find_task_actors(cid)
    ok(await fx.facade.flow("processTask/removeTaskActor",
                            {"processTaskId": str(cid), "operator": "userK", "actorIds": ["userK"]}))
    assert "userK" not in await fx.repo.find_task_actors(cid)

    ok(await fx.facade.flow("processTask/transfer", {
        "processTaskId": str(cid), "operator": "userX",
        "fromActor": "userX", "toActor": "userT", "reason": "协办转办"}))
    roster = await fx.repo.find_task_actors(cid)
    assert "userX" not in roster and "userT" in roster
    after = await fx.task(cid)
    assert after.id == cid and after.is_doing() and after.taskType == 1, "转办不新建行、不改 task_type"

    ok(await fx.exec(after, "userT", AGREE))
    assert (await fx.task(host.id)).is_doing(), "转办后新办理人办结仍不驱动流程"
    assert await fx.doing("final") is None


# ═══ 回归：会签合并 ═══════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_countersign_merge_not_blocked_by_aidant():
    """会签票齐即推进，不被协办行挡（合并判据显式排除 task_type=1）。"""
    fx = await setup(flow=cs_chain())
    members = await fx.repo.find_doing_tasks(fx.iid)
    assert len(members) == 2
    row_a = next(t for t in members if "userA" in t.actorIds)
    row_b = next(t for t in members if "userB" in t.actorIds)

    ok(await fx.aidant(row_a, "userA", "userX"))
    child = AD.aidant_rows(await fx.all_tasks(), await fx.task(row_a.id))[0]
    assert child.is_doing() and child.taskName == "cs"

    ok(await fx.exec(row_a, "userA", AGREE))
    ok(await fx.exec(row_b, "userB", AGREE))
    assert await fx.state() == int(InstanceState.DONE), "票齐即推进：实例办结"
    assert (await fx.task(child.id)).taskState == TaskState.ABANDONED, "协办行随离开节点清场"
    assert len(await fx.repo.find_doing_tasks(fx.iid)) == 0


@pytest.mark.asyncio
async def test_aidant_on_countersign_does_not_advance_early():
    fx = await setup(flow=cs_chain())
    members = await fx.repo.find_doing_tasks(fx.iid)
    row_a = next(t for t in members if "userA" in t.actorIds)
    await fx.aidant(row_a, "userA", "userX")
    child = AD.aidant_rows(await fx.all_tasks(), await fx.task(row_a.id))[0]
    await fx.exec(child, "userX", AGREE)
    assert await fx.state() == int(InstanceState.DOING), "协办办结不推进"
    assert await fx.doing_count("cs") == 2


# ═══ 回归：与征询并存（两闸各数各的行） ═══════════════════════════════════════

@pytest.mark.asyncio
async def test_coexist_with_collect_two_gates_count_own_rows():
    fx = await setup({"dynamicCountersign": True})
    host = await fx.doing("review")
    ok(await fx.exec(host, "userA", AGREE, {A.KEY_ADD_SIGN_OPERATORS: ["userC"]}))
    ok(await fx.aidant(host, "userA", "userX", None, True))

    host_row = await fx.task(host.id)
    all_rows = await fx.all_tasks()
    assert C.member_counts(all_rows, host_row)[0] == 1, "征询收齐闸只数成员行"
    assert AD.count_aidant(all_rows, host_row)[0] == 1, "协办等待闸只数协办行"
    assert len(C.member_rows(all_rows, host_row)) == 1
    assert len(AD.aidant_rows(all_rows, host_row)) == 1
    aidant_row = AD.aidant_rows(all_rows, host_row)[0]
    assert not C.is_member_row(all_rows, aidant_row), "协办行不是征询成员行"

    # 协办先办结 ⇒ 征询闸仍拦（各数各的行），文案是征询那一族
    await fx.exec(aidant_row, "userX", AGREE)
    msg(await fx.exec(host_row, "userA", AGREE),
        "节点[review]动态会签意见未收齐（已提交 0/1），不能办理")

    member = C.member_rows(await fx.all_tasks(), await fx.task(host.id))[0]
    ok(await fx.exec(member, "userC", AGREE))
    ok(await fx.exec(await fx.task(host.id), "userA", AGREE))
    assert await fx.doing("final") is not None


@pytest.mark.asyncio
async def test_collect_done_but_aidant_pending_reports_aidant_message():
    """反向：征询已收齐、协办未办结 ⇒ 报协办那一族文案（证明两闸不串味）。"""
    fx = await setup({"dynamicCountersign": True})
    host = await fx.doing("review")
    await fx.exec(host, "userA", AGREE, {A.KEY_ADD_SIGN_OPERATORS: ["userC"]})
    await fx.aidant(host, "userA", "userX", None, True)
    member = C.member_rows(await fx.all_tasks(), await fx.task(host.id))[0]
    await fx.exec(member, "userC", AGREE)
    msg(await fx.exec(await fx.task(host.id), "userA", AGREE),
        "节点[review]协办未办结（已办结 0/1），不能办理")


@pytest.mark.asyncio
async def test_collect_node_wait_false_not_blocked_by_aidant():
    fx = await setup({"dynamicCountersign": True})
    host = await fx.doing("review")
    await fx.exec(host, "userA", AGREE, {A.KEY_ADD_SIGN_OPERATORS: ["userC"]})
    await fx.aidant(host, "userA", "userX", None, False)
    member = C.member_rows(await fx.all_tasks(), await fx.task(host.id))[0]
    await fx.exec(member, "userC", AGREE)
    ok(await fx.exec(await fx.task(host.id), "userA", AGREE))
    assert await fx.doing("final") is not None, "缺省不等协办：收齐即推进"
    aidant_row = AD.aidant_rows(await fx.all_tasks(), await fx.task(host.id))[0]
    assert (await fx.task(aidant_row.id)).taskState == TaskState.ABANDONED


# ═══ 回归：撤回清场 ═══════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_recall_cleans_aidant_rows():
    """已办撤回：下游 DOING 协办行一并废弃（血缘自然覆盖），复活行是唯一活待办。"""
    fx = await setup({"allowRecall": True})
    host = await fx.doing("review")
    ok(await fx.exec(host, "userA", AGREE))
    final_row = await fx.doing("final")
    assert final_row is not None
    ok(await fx.aidant(final_row, "userF", "userX"))
    child = AD.aidant_rows(await fx.all_tasks(), await fx.task(final_row.id))[0]
    assert child.is_doing()

    resurrected = await fx.eng.recall_task(host.id, "userA")
    assert resurrected.is_doing() and resurrected.taskName == "review"
    assert (await fx.task(child.id)).taskState == TaskState.ABANDONED, "下游协办行废弃"
    assert (await fx.task(final_row.id)).taskState == TaskState.ABANDONED, "下游主办行废弃"
    assert len(await fx.repo.find_doing_tasks(fx.iid)) == 1, "唯一活待办"


@pytest.mark.asyncio
async def test_instance_withdraw_cleans_aidant_rows():
    fx = await setup()
    host = await fx.doing("review")
    await fx.aidant(host, "userA", "userX")
    child = (await fx.rows(host))[0]
    ok(await fx.facade.flow("processInstance/withdraw",
                            {"id": str(fx.iid), "operator": "applicant"}))
    assert (await fx.task(child.id)).taskState == TaskState.WITHDRAW
    assert len(await fx.repo.find_doing_tasks(fx.iid)) == 0


# ═══ 回归：既有 actor 写腿不变 ═══════════════════════════════════════════════

@pytest.mark.asyncio
async def test_transfer_surrogate_add_candidate_unchanged():
    fx = await setup()
    host = await fx.doing("review")
    hid = host.id
    await fx.aidant(host, "userA", "userX")

    ok(await fx.facade.flow("processTask/surrogate",
                            {"processTaskId": str(hid), "actorIds": ["userB"]}))
    assert await fx.repo.find_task_actors(hid) == ["userA", "userB"], "surrogate 只追加原人保留"
    ok(await fx.facade.flow("processTask/addCandidate",
                            {"processTaskId": str(hid), "actorIds": ["userC"]}))
    assert await fx.repo.find_task_actors(hid) == ["userA", "userB", "userC"]

    ok(await fx.facade.flow("processTask/transfer", {
        "processTaskId": str(hid), "operator": "userA",
        "fromActor": "userA", "toActor": "userD", "reason": "转办原因"}))
    roster = await fx.repo.find_task_actors(hid)
    assert "userA" not in roster and "userD" in roster
    after = await fx.task(hid)
    assert after.id == hid, "转办不新建任务行"
    assert int(after.variables.get("submitType")) == 7, "转办留痕 submitType=7"
    assert after.variables.get("tf_transferHistory") is not None
    assert not after.actorId, "转办不写 actor_id 列"
    rows = await fx.rows(after)
    assert len(rows) == 1 and rows[0].is_doing(), "协办行不受主办转办影响"
    ok(await fx.exec(after, "userD", AGREE))
    assert await fx.doing("final") is not None, "新办理人办结照常推进"


def test_permission_code_rides_execute():
    """权限码复用 wf:processTask:execute（OR 规则，照 246/247 B 案惯例，不建按钮种子）。"""
    assert permission_codes("processTask/aidant") == ["wf:processTask:execute"]
    # 哨兵：没进表的 action 仍走默认派生
    assert permission_codes("processTask/transfer") == ["wf:processTask:transfer"]


@pytest.mark.asyncio
async def test_node_level_secondary_still_banned():
    """节点级 taskType=1 仍解析期禁配（issues/252 不因本轮运行期派生而放宽）。"""
    fx = await setup()
    flow = chain({"taskType": 1})
    d = register(fx.repo, flow)
    with pytest.raises(Exception) as ei:
        await fx.eng.start_process_instance_by_id(d.id, "applicant", {})
    assert "不允许配置协办任务类型" in str(ei.value), str(ei.value)