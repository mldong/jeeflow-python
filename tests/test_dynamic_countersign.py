"""「动态会签（征询模式 dynamicCountersign）」验收（issues/247，Python 腿）——移植
jeeflow-java 参考实现 ffe2749 的 `DynamicCountersignTest` 用例形状（Go 腿
facade/collect_247_test.go 同构）：发起不办结主办行＋响应 marker、成员行三戳
（parent/轮次/reason）、账本含 reason、闸门逐字文案挡全部 submitType、成员行禁拉人、
重复/给自己/上限三拦、撤回接口恢复闸门且已交意见保留、多轮征询跨轮计数、
detail 投影 active 翻转、三条解析期硬校验＋非法布尔值、征询档推进闸
（成员办结不流转、主办拍板才流转）、撤回动态会签非参与者文案、废弃腿扩双开关。

发起/闸门/撤回在门面分流——测试经 facade.flow 驱动（与 java 同层）；夹具内联 JSON，
不碰共享 flows/ 目录。
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository                      # noqa: E402
from jeeflow import add_sign as A                                     # noqa: E402
from jeeflow import collect as C                                      # noqa: E402
from jeeflow.facade import JeeflowFacade                             # noqa: E402
from jeeflow.model import (InstanceState, ProcessDefine, SubmitType,  # noqa: E402
                           UserInfo, parse_flow_model)
from jeeflow.spi import IDGenerator                                   # noqa: E402

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
        return UserInfo(userId=uid, realName="用户" + str(uid), deptId="D01", deptName="测试部门")


class _Expr:
    async def eval(self, expr, vars):
        return False


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
    """start → apply → review → final → end（java DynamicCountersignTest.chain 同形）。"""
    global _seq
    _seq += 1
    a = dict(apply_props) if apply_props else {"assignee": "applicant"}
    a.setdefault("assignee", "applicant")
    r = {"form": "form-r", "assignee": "userA", "taskType": 0, "performType": 0}
    r.update(review_props or {})
    return {
        "name": f"collect-{_seq}", "displayName": "动态会签夹具", "type": "approval",
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


COLLECT_ON = {"dynamicCountersign": True}


def register(repo, flow: dict) -> ProcessDefine:
    d = ProcessDefine(name=flow["name"], displayName=flow.get("displayName", ""),
                      type=flow.get("type", ""), state=1, version=1,
                      content=json.dumps(flow, ensure_ascii=False))
    repo.add_define(d)
    return d


async def setup_to_review(review_props):
    """部署夹具并走发起＋自动办结申请节点，返回停在 review 的（引擎、仓储、门面、实例）。"""
    eng, repo, facade = setup_facade()
    d = register(repo, chain({"assignee": "applicant"}, review_props))
    inst = await eng.start_process_instance_by_id(d.id, "applicant", {})
    for t in await repo.find_doing_tasks(inst.id):
        await eng.execute_process_task(t.id, "applicant", {"submitType": APPLY})
    return eng, repo, facade, await repo.find_instance_by_id(inst.id)


async def doing(repo, iid, name):
    for t in await repo.find_doing_tasks(iid):
        if t.taskName == name:
            return t
    return None


async def doing_count(repo, iid, name) -> int:
    return sum(1 for t in await repo.find_doing_tasks(iid) if t.taskName == name)


async def member_rows(repo, iid, host) -> list:
    """经仓储读回成员行（parent＝主办行且同节点同名；断言只认读回的持久值）。"""
    return [t for t in await repo.find_history_tasks(iid)
            if t.parentTaskId == host.id and t.taskName == host.taskName]


def round_of(task, node: str = "review"):
    """成员行变量里的轮次（键名逐字钉住 addSignRound_{节点名}）。"""
    return (task.variables or {}).get(f"{A.VAR_ADD_SIGN_ROUND}_{node}")


async def ledger(repo, host_id) -> list:
    """主办行账本（tf_addSignHistory）。"""
    row = await repo.find_task_by_id(host_id)
    v = (row.variables or {}).get(A.KEY_ADD_SIGN_HISTORY)
    return v if isinstance(v, list) else []


async def collect(facade, host, operator, operators, reason=""):
    """经门面发起动态会签（带 tf_addSignOperators 的 AGREE 提交）。"""
    a = {"processTaskId": str(host.id), "operator": operator, "submitType": AGREE,
         A.KEY_ADD_SIGN_OPERATORS: operators}
    if reason:
        a[C.KEY_ADD_SIGN_REASON] = reason
    return await facade.flow("processTask/execute", a)


async def exec_task(facade, task_id, operator, submit_type, extra=None):
    a = {"processTaskId": str(task_id), "operator": operator, "submitType": submit_type}
    if extra:
        a.update(extra)
    return await facade.flow("processTask/execute", a)


# ═══ 用例（照 java DynamicCountersignTest 8 格＋go 补格） ══════════════════════

@pytest.mark.asyncio
async def test01_submit_keeps_host_doing_and_stamps_member_rows():
    """发起：不办结主办行、成员行三戳齐、账本含 reason、响应 marker。"""
    eng, repo, facade, inst = await setup_to_review(COLLECT_ON)
    iid = inst.id
    host = await doing(repo, iid, "review")
    resp = await collect(facade, host, "userA", ["userX", "userY"], "涉及地规划需两部门意见")
    assert resp["code"] == 0, resp
    dc = resp["data"]["dynamicCountersign"]
    assert dc["round"] == 1 and dc["total"] == 2 and dc["submitted"] == 0, \
        f"marker 应为 round=1 total=2 submitted=0，实得 {dc}"
    # 主办行仍 DOING（读回的持久值）
    host_row = await repo.find_task_by_id(host.id)
    assert host_row.is_doing(), f"主办行不得被办结，实得 {host_row.taskState}"
    # 成员行三戳
    members = await member_rows(repo, iid, host)
    assert len(members) == 2, f"应建两条成员行，实得 {len(members)}"
    for m in members:
        assert m.is_doing(), f"成员行应 DOING，实得 {m.taskState}"
        assert m.parentTaskId == host.id, f"成员行 parent 应＝主办行 {host.id}，实得 {m.parentTaskId}"
        assert round_of(m) == 1, f"成员行轮次应为 1，实得 {m.variables}"
        assert m.variables.get(C.KEY_ADD_SIGN_REASON) == "涉及地规划需两部门意见", \
            f"成员行应带 reason，实得 {m.variables}"
    # 账本含 reason、只一条
    lg = await ledger(repo, host.id)
    assert len(lg) == 1, f"主办行应有 1 条账本，实得 {len(lg)}"
    assert lg[0]["reason"] == "涉及地规划需两部门意见"
    assert lg[0]["fromActor"] == "userA"


@pytest.mark.asyncio
async def test02_gate_blocks_all_submit_types_until_collected():
    """闸门：未收齐时主办按任意 submitType 办结 ⇒ 逐字文案报错且零写入。"""
    eng, repo, facade, inst = await setup_to_review(COLLECT_ON)
    iid = inst.id
    host = await doing(repo, iid, "review")
    await collect(facade, host, "userA", ["userX", "userY"])
    for submit_type in (AGREE, REJECT):
        resp = await exec_task(facade, host.id, "userA", submit_type)
        assert resp["code"] == 99999999, f"submitType={submit_type} 应被闸门拦下: {resp}"
        assert resp["msg"] == "节点[review]动态会签意见未收齐（已提交 0/2），不能办理", \
            f"闸门文案必须逐字，实得 {resp['msg']}"
    # 零写入：主办仍 DOING、成员行未被办结、行数不增
    host_row = await repo.find_task_by_id(host.id)
    assert host_row.is_doing(), "报错零写入：主办行不得被办结"
    assert await doing_count(repo, iid, "review") == 3, \
        f"review 应为主办＋两成员共 3 条 DOING，实得 {await doing_count(repo, iid, 'review')}"


@pytest.mark.asyncio
async def test03_host_proceeds_after_all_submitted_with_projection():
    """收齐后主办拍板推进；征询档推进闸：成员办结不流转；投影 active 随提交翻转为 false。"""
    eng, repo, facade, inst = await setup_to_review(COLLECT_ON)
    iid = inst.id
    host = await doing(repo, iid, "review")
    await collect(facade, host, "userA", ["userX", "userY"])
    members = await member_rows(repo, iid, host)
    # 逐个提交意见（意见落各自行的 tf_approvalComment）——成员办结不得流转
    for i, m in enumerate(members):
        resp = await exec_task(facade, m.id, m.actorIds[0], AGREE,
                               {"tf_approvalComment": f"意见{i + 1}"})
        assert resp["code"] == 0, f"成员办结应成功: {resp}"
        assert await doing(repo, iid, "final") is None, "成员办结不得流转（主办还没拍板）"
    # 投影：submitted=2、active=false
    detail = await facade.flow("processTask/detail", {"id": str(host.id)})
    ext = detail["data"]["ext"]
    dc = ext.get("dynamicCountersign")
    assert dc is not None, "detail 投影应有 ext.dynamicCountersign"
    assert dc["active"] is False, f"投影 active 应为 false，实得 {dc}"
    assert dc["total"] == 2 and dc["submitted"] == 2, f"投影应为 total=2 submitted=2，实得 {dc}"
    assert len(dc["actors"]) == 2, f"投影 actors 应有 2 条，实得 {dc['actors']}"
    # 主办拍板 → 推进
    resp = await exec_task(facade, host.id, "userA", AGREE)
    assert resp["code"] == 0, f"主办拍板应成功: {resp}"
    assert await doing(repo, iid, "final") is not None, "收齐后主办拍板应推进到 final"


@pytest.mark.asyncio
async def test04_member_pull_duplicate_and_self_guards():
    """成员行携带拉人参数 ⇒ 报错；重复拉同一人 ⇒ 报错；加给自己 ⇒ 报错。"""
    eng, repo, facade, inst = await setup_to_review(COLLECT_ON)
    iid = inst.id
    host = await doing(repo, iid, "review")
    await collect(facade, host, "userA", ["userX"])
    members = await member_rows(repo, iid, host)
    # 成员行拉人
    resp = await exec_task(facade, members[0].id, "userX", AGREE,
                           {A.KEY_ADD_SIGN_OPERATORS: "userZ"})
    assert resp["msg"] == "动态会签成员任务不能发起加签", f"成员拉人必须逐字报错，实得 {resp['msg']}"
    # 重复拉 pending 中的人
    resp2 = await collect(facade, host, "userA", ["userX"])
    assert resp2["msg"] == "操作人[userX]已在节点[review]的动态会签中", \
        f"重复拉人必须逐字报错，实得 {resp2['msg']}"
    # 加给自己
    resp3 = await collect(facade, host, "userA", ["userA"])
    assert resp3["msg"] == "不能把任务加签给自己", f"加给自己必须逐字报错，实得 {resp3['msg']}"


@pytest.mark.asyncio
async def test05_revoke_restores_gate_and_keeps_submitted_opinions():
    """撤回：未答废弃、已交保留、账本记撤回、闸门恢复；二次撤回归其逐字文案。"""
    eng, repo, facade, inst = await setup_to_review(COLLECT_ON)
    iid = inst.id
    host = await doing(repo, iid, "review")
    await collect(facade, host, "userA", ["userX", "userY"])
    members = await member_rows(repo, iid, host)
    x_row = next(m for m in members if "userX" in (m.actorIds or []))
    y_row = next(m for m in members if m.id != x_row.id)
    resp0 = await exec_task(facade, x_row.id, "userX", AGREE, {"tf_approvalComment": "甲部门意见"})
    assert resp0["code"] == 0, f"userX 交意见: {resp0}"

    resp = await facade.flow("processTask/revokeDynamicCountersign",
                             {"processTaskId": str(host.id), "operator": "userA",
                              "reason": "部门选错重新拉"})
    assert resp["code"] == 0, f"撤回应成功: {resp}"
    assert resp["data"]["revoked"] == 1, f"应只撤未答的 1 条，实得 {resp['data']}"
    # 已交保留（FINISHED）、未答废弃（99）
    row_x = await repo.find_task_by_id(x_row.id)
    assert row_x.is_finished(), f"已交意见行应保留，实得 {row_x.taskState}"
    row_y = await repo.find_task_by_id(y_row.id)
    assert int(row_y.taskState) == 99, f"未答成员行应废弃（99），实得 {row_y.taskState}"
    # 账本两条（发起＋撤回，撤回条带 revoked 标记）
    lg = await ledger(repo, host.id)
    assert len(lg) == 2, f"账本应有 2 条，实得 {len(lg)}"
    assert lg[1]["revoked"] is True, f"撤回条应带 revoked=true，实得 {lg[1]}"
    assert lg[1]["reason"] == "部门选错重新拉", f"撤回条应带 reason，实得 {lg[1]}"
    # 闸门恢复：主办可直接拍板推进
    go = await exec_task(facade, host.id, "userA", AGREE)
    assert go["code"] == 0, f"撤回后闸门应恢复: {go}"
    assert await doing(repo, iid, "final") is not None, "撤回后主办拍板应推进"
    # 撤回已无待答 → 逐字文案
    again = await facade.flow("processTask/revokeDynamicCountersign",
                              {"processTaskId": str(host.id), "operator": "userA"})
    assert again["msg"] == "节点[review]没有进行中的动态会签", \
        f"无待答撤回必须逐字报错，实得 {again['msg']}"


@pytest.mark.asyncio
async def test06_multi_round_collect_counts_across_rounds():
    """多轮征询：跨轮意见互不覆盖，轮次递增，收齐判据跨轮计数。"""
    eng, repo, facade, inst = await setup_to_review(COLLECT_ON)
    iid = inst.id
    host = await doing(repo, iid, "review")
    r1 = await collect(facade, host, "userA", ["userX"])
    assert r1["data"]["dynamicCountersign"]["round"] == 1, f"第一轮应为 1: {r1}"
    x_row = (await member_rows(repo, iid, host))[0]
    resp = await exec_task(facade, x_row.id, "userX", AGREE)
    assert resp["code"] == 0, f"userX 交意见: {resp}"
    # 第二轮（闸门已开时仍可再拉）
    r2 = await collect(facade, host, "userA", ["userY"])
    assert r2["data"]["dynamicCountersign"]["round"] == 2, f"第二轮应为 2: {r2}"
    # 跨轮收齐判据：x 已交、y 未交 ⇒ 1/2
    resp = await exec_task(facade, host.id, "userA", AGREE)
    assert resp["msg"] == "节点[review]动态会签意见未收齐（已提交 1/2），不能办理", \
        f"跨轮计数文案必须逐字，实得 {resp['msg']}"
    # 剩余未答成员行逐个办结
    for m in await member_rows(repo, iid, host):
        if not m.is_doing():
            continue  # 第 1 轮已提交的行不再重复办结
        resp = await exec_task(facade, m.id, m.actorIds[0], AGREE)
        assert resp["code"] == 0, f"成员办结: {resp}"
    go = await exec_task(facade, host.id, "userA", AGREE)
    assert go["code"] == 0, f"收齐后拍板: {go}"
    assert await doing(repo, iid, "final") is not None, "收齐后应推进到 final"


def test07_parse_bans_mutex_countersign_and_apply_nodes():
    """三条解析期硬校验：双开／会签节点／申请节点＋非法布尔值。"""
    with pytest.raises(ValueError) as ei:
        parse_flow_model(chain({"assignee": "applicant"},
                               {**COLLECT_ON, "allowAddSign": True}))
    assert "不允许同时配置 allowAddSign 与 dynamicCountersign" in str(ei.value), str(ei.value)
    with pytest.raises(ValueError) as ei:
        parse_flow_model(chain({"assignee": "applicant"},
                               {**COLLECT_ON, "performType": 1, "countersignType": "PARALLEL"}))
    assert "会签节点[review]不允许配置 dynamicCountersign" in str(ei.value), str(ei.value)
    with pytest.raises(ValueError) as ei:
        # 申请节点（start 直接后继）配 dynamicCountersign ⇒ 解析期报错
        parse_flow_model(chain({"assignee": "applicant", **COLLECT_ON}, {}))
    assert "申请节点[apply]不允许配置 dynamicCountersign" in str(ei.value), str(ei.value)
    with pytest.raises(ValueError) as ei:
        parse_flow_model(chain({"assignee": "applicant"}, {"dynamicCountersign": "yes"}))
    assert "dynamicCountersign 配置非法" in str(ei.value), str(ei.value)


def test08_pinned_messages():
    """消息常量与 spec 06 逐字文案一致性自证（防常量被改后测试仍绿）。"""
    assert C.MSG_PENDING == "节点[{0}]动态会签意见未收齐（已提交 {1}/{2}），不能办理"
    assert C.MSG_NO_PENDING == "节点[{0}]没有进行中的动态会签"
    assert C.MSG_NO_PERMISSION_REVOKE == "无权限撤回该动态会签"
    assert C.MSG_MEMBER_PULL == "动态会签成员任务不能发起加签"
    assert C.MSG_DUPLICATE == "操作人[{0}]已在节点[{1}]的动态会签中"
    assert C.MSG_MUTEX_WITH_ADD_SIGN == ("节点[{0}]不允许同时配置 allowAddSign 与 dynamicCountersign："
                                         "两档共用 tf_addSignOperators，行为靠节点属性分流，"
                                         "双开＝行为歧义")
    assert C.MSG_COUNTERSIGN_FORBID == ("会签节点[{0}]不允许配置 dynamicCountersign："
                                        "会签池推进语义与征询挂起语义打架")
    assert C.MSG_APPLY_FORBID == ("申请节点[{0}]不允许配置 dynamicCountersign："
                                  "申请节点那手走实例撤回＋重新发起，不需要征询挂起")
    assert C.PROP_DYNAMIC_COUNTERSIGN == "dynamicCountersign"
    assert C.KEY_ADD_SIGN_REASON == "tf_addSignReason"


@pytest.mark.asyncio
async def test09_revoke_permission_and_round_kept():
    """revokeDynamicCountersign 非参与者 ⇒ 逐字「无权限撤回该动态会签」；
    撤回后轮次不消耗（下一轮 round 续接，不被撤回重置）。"""
    eng, repo, facade, inst = await setup_to_review(COLLECT_ON)
    iid = inst.id
    host = await doing(repo, iid, "review")
    await collect(facade, host, "userA", ["userX"])
    resp = await facade.flow("processTask/revokeDynamicCountersign",
                             {"processTaskId": str(host.id), "operator": "userZ"})
    assert resp["msg"] == "无权限撤回该动态会签", f"非参与者撤回必须逐字报错，实得 {resp['msg']}"
    # 撤回后（userA 撤、待答清零）再拉：轮次续接为 2（撤回不消耗）
    rv = await facade.flow("processTask/revokeDynamicCountersign",
                           {"processTaskId": str(host.id), "operator": "userA"})
    assert rv["code"] == 0, f"userA 撤回应成功: {rv}"
    r2 = await collect(facade, host, "userA", ["userY"])
    assert r2["data"]["dynamicCountersign"]["round"] == 2, \
        f"撤回后轮次不消耗，应续接为 2: {r2}"


@pytest.mark.asyncio
async def test10_engine_reject_abandons_pending_members():
    """废弃腿条件扩双开关（spec 04 语义边界表）：直连引擎拒绝腿（不经门面闸门）废弃
    本节点未完成的征询成员行，不留孤儿待办；已交意见行（FINISHED）保留。"""
    eng, repo, facade, inst = await setup_to_review(COLLECT_ON)
    iid = inst.id
    host = await doing(repo, iid, "review")
    await collect(facade, host, "userA", ["userX", "userY"])
    members = await member_rows(repo, iid, host)
    x_row = members[0]
    await exec_task(facade, x_row.id, x_row.actorIds[0], AGREE)
    y_row = next(m for m in members if m.id != x_row.id)

    # 直连引擎拒绝（java executeAndJumpToEnd → abandonDoingOnNode 同判据）
    await eng.execute_and_jump_to_end(host.id, "userA", {"submitType": REJECT})
    row_y = await repo.find_task_by_id(y_row.id)
    assert int(row_y.taskState) == 99, f"未答成员行应废弃（99），实得 {row_y.taskState}"
    row_x = await repo.find_task_by_id(x_row.id)
    assert row_x.is_finished(), f"已交意见行应保留，实得 {row_x.taskState}"
    inst_now = await repo.find_instance_by_id(iid)
    assert int(inst_now.state) == int(InstanceState.REJECT), "实例应拒绝（45）"


@pytest.mark.asyncio
async def test11_member_row_needs_same_name_not_parent_only():
    """成员行识别＝parent＋同节点同名（CollectSupport 的承重判据）：主办拍板推进后，
    下一节点行（final）的 parent 同样是主办行 id——只认 parent 会把下游行误当成员
    （投影 total 虚高、撤回腿误废弃）。本格钉住：投影只数同名行、撤回腿不误伤下游。"""
    eng, repo, facade, inst = await setup_to_review(COLLECT_ON)
    iid = inst.id
    host = await doing(repo, iid, "review")
    await collect(facade, host, "userA", ["userX"])
    x_row = (await member_rows(repo, iid, host))[0]
    await exec_task(facade, x_row.id, "userX", AGREE)
    resp = await exec_task(facade, host.id, "userA", AGREE)
    assert resp["code"] == 0, f"主办拍板: {resp}"
    final_row = await doing(repo, iid, "final")
    assert final_row is not None and final_row.parentTaskId == host.id, \
        "前置：下游 final 行的 parent 应＝主办行 id（建单不变量，正是会误判的形状）"
    # 投影只数同名成员行：total=1（若只认 parent 会把 final 行也算进 total ⇒ 2）
    detail = await facade.flow("processTask/detail", {"id": str(host.id)})
    dc = detail["data"]["ext"].get("dynamicCountersign")
    assert dc is not None and dc["total"] == 1 and dc["submitted"] == 1, \
        f"投影必须只数同节点同名成员行，实得 {dc}"
    # 撤回腿不误伤：无待答 ⇒ 逐字文案（若误认下游行也不会在这里炸，但 pending 必须仍为空）
    again = await facade.flow("processTask/revokeDynamicCountersign",
                              {"processTaskId": str(host.id), "operator": "userA"})
    assert again["msg"] == "节点[review]没有进行中的动态会签", \
        f"实得 {again['msg']}"
    row_final = await repo.find_task_by_id(final_row.id)
    assert row_final.is_doing(), "撤回腿不得误废弃下游行"
