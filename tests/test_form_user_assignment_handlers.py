"""「表单指定用户所属部门经理／分管领导」两个内置处理器（Python 栈）。

契约＝执行卡 `docs/2026-11-22-form-user-assignment-handler-handoff.md` §3（八语言同形）；
行为基准＝jeeflow-go `v1.8.67`（本轮样章）；场景来源＝mldong-fastapi-wf `fe9233d`（HR 代员工提交）。
夹具＝内联流程 JSON（不占共享 flows 的号，见执行报告 P2）。

覆盖：正向四档并集、HR 两段链路、负向七档（含 SPI 未注册）、f_ 优先级、只取第一个 id、
现存 handler 回归、注册名可解析。
"""
import asyncio
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow.builtin import (                                        # noqa: E402
    HANDLER_FORM_USER_DEPT_LEADER, HANDLER_FORM_USER_DEPT_MAIN_LEADER,
    FormUserDeptLeaderAssignmentHandler, FormUserDeptMainLeaderAssignmentHandler,
    DeptLeaderAssignmentHandler, _first_form_user_id, _find_form_user_field_value,
    register_builtin_assignments)
from jeeflow.engine import EngineImpl                                # noqa: E402
from jeeflow.extensions import EngineExtensions, HandlerRegistry     # noqa: E402
from jeeflow.metadata import HandlerRegistry as MetaRegistry         # noqa: E402
from jeeflow.model import FlowNode, ProcessInstance, UserInfo        # noqa: E402

FQ_LEADER = "com.mldong.jeeflow.interceptor.impl.FormUserDeptLeaderAssignmentHandler"
FQ_MAIN = "com.mldong.jeeflow.interceptor.impl.FormUserDeptMainLeaderAssignmentHandler"


class _UserProv:
    """emp→D01、nodept→无部门、其余查无此人。"""

    async def get_user(self, user_id):
        if user_id == "emp":
            return UserInfo(userId="emp", realName="员工", deptId="D01")
        if user_id == "nodept":
            return UserInfo(userId="nodept", realName="无部门", deptId=None)
        if user_id == "other":
            return UserInfo(userId="other", realName="别的部门的", deptId="D02")
        return None


class _OrgProv:
    async def find_dept_leaders(self, dept_id):
        return ["leader1", "leader2"] if dept_id == "D01" else []

    async def find_dept_main_leaders(self, dept_id):
        return ["boss1"] if dept_id == "D01" else []

    async def find_by_role(self, role_code):
        return []


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


async def _actors(node_id, variables, main=False, user_prov=True, org_prov=True):
    h = (FormUserDeptMainLeaderAssignmentHandler if main else FormUserDeptLeaderAssignmentHandler)(
        _UserProv() if user_prov else None, _OrgProv() if org_prov else None)
    node = FlowNode(id=node_id, type="snaker:task")
    inst = ProcessInstance(id=1, defineId=1, operator="hr", variables=dict(variables))
    return await h.assign(node, inst, "hr")


# ─── 正向：四档并集，顺序即优先级 ────────────────────────────────────────────────

POSITIVE = [
    ("f_ 主通道", "manager", {"f_manager": "emp"}),
    ("裸名回落", "manager", {"manager": "emp"}),
    ("# 右半（字段名本身带 f_）", "manager#f_userId", {"f_userId": "emp"}),
    ("# 右半的 f_ 变体", "manager#userId", {"f_userId": "emp"}),
    ("_NN 去后缀的 f_ 形", "manager_01", {"f_manager": "emp"}),
    ("_NN 去后缀的裸名形", "manager_01", {"manager": "emp"}),
]


@pytest.mark.parametrize("label,node_id,variables", POSITIVE, ids=[p[0] for p in POSITIVE])
def test_positive_resolves_form_user_to_dept_leaders(label, node_id, variables):
    assert _run(_actors(node_id, variables)) == ["leader1", "leader2"], label


def test_positive_main_leader_branch():
    assert _run(_actors("manager#f_userId", {"f_userId": "emp"}, main=True)) == ["boss1"]


def test_positive_multi_leader_order_preserved():
    """经理多人按 SPI 配置顺序（不去重打乱）。"""
    assert _run(_actors("manager", {"f_manager": "emp"})) == ["leader1", "leader2"]


# ─── HR 代员工提交：引擎级两段链路 ───────────────────────────────────────────────

def _hr_flow():
    def task(nid, handler, y):
        return {"id": nid, "type": "snaker:task", "x": 480, "y": y, "text": {"value": nid},
                "properties": {"assignee": "", "taskType": 0, "performType": 0,
                               "assignmentHandler": handler}}
    return {
        "name": "fu-hr-proxy", "displayName": "HR代员工提交（表单指定用户）", "type": "approval",
        "nodes": [
            {"id": "start", "type": "snaker:start", "properties": {}, "text": {"value": "开始"}},
            {"id": "apply", "type": "snaker:task", "properties": {"assignee": "applicant", "performType": 0},
             "text": {"value": "发起申请"}},
            task("manager#f_userId", FQ_LEADER, 500),
            task("mainLeader#f_userId", FQ_MAIN, 650),
            {"id": "end", "type": "snaker:end", "properties": {}, "text": {"value": "结束"}},
        ],
        "edges": [
            {"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
            {"id": "e1", "sourceNodeId": "apply", "targetNodeId": "manager#f_userId", "properties": {}},
            {"id": "e2", "sourceNodeId": "manager#f_userId", "targetNodeId": "mainLeader#f_userId",
             "properties": {}},
            {"id": "e3", "sourceNodeId": "mainLeader#f_userId", "targetNodeId": "end", "properties": {}},
        ],
    }


@pytest.mark.asyncio
async def test_hr_proxy_flow_actors_land_on_form_user_dept_leaders():
    repo, eng, reg = await _engine(_hr_flow())
    inst = await eng.start_process_instance_by_id(reg["def_id"], "hr", {"f_userId": "emp"})
    apply_task = (await repo.find_doing_tasks(inst.id))[0]
    assert apply_task.taskName == "apply", apply_task
    await eng.execute_process_task(apply_task.id, "hr")

    doing = await repo.find_doing_tasks(inst.id)
    assert [t.taskName for t in doing] == ["manager#f_userId"], doing
    assert sorted(doing[0].actorIds) == ["leader1", "leader2"], doing[0].actorIds

    await eng.execute_process_task(doing[0].id, "leader1")
    doing = await repo.find_doing_tasks(inst.id)
    assert [t.taskName for t in doing] == ["mainLeader#f_userId"], doing
    assert doing[0].actorIds == ["boss1"], doing[0].actorIds


async def _engine(flow):
    from jeeflow.memory import MemoryRepository
    from jeeflow.model import ProcessDefine

    class _Gen:
        def __init__(self):
            self.n = 910000

        def next_id(self):
            self.n += 1
            return self.n

    repo = MemoryRepository()
    reg = HandlerRegistry()
    register_builtin_assignments(reg, _UserProv(), _OrgProv())
    eng = EngineImpl(repo, _UserProv(), _Gen(), None)
    eng.set_extensions(EngineExtensions(registry=reg))
    d = ProcessDefine(name=flow["name"], displayName=flow["displayName"], type=flow["type"], state=1,
                      content=json.dumps(flow, ensure_ascii=False))
    repo.add_define(d)
    return repo, eng, {"def_id": d.id}


# ─── 负向：一律空列表、不抛异常、实例不受影响 ────────────────────────────────────

NEGATIVE = [
    ("表单没给字段", "manager#f_userId", {}),
    ("值为空串", "manager#f_userId", {"f_userId": ""}),
    ("值为纯空白", "manager#f_userId", {"f_userId": "   "}),
    ("列表全空", "manager#f_userId", {"f_userId": ["", "  "]}),
    ("指向查无此人", "manager#f_userId", {"f_userId": "ghost"}),
    ("指向无部门用户", "manager#f_userId", {"f_userId": "nodept"}),
    ("指向别的部门（该部门无领导）", "manager#f_userId", {"f_userId": "other"}),
]


@pytest.mark.parametrize("label,node_id,variables", NEGATIVE, ids=[n[0] for n in NEGATIVE])
def test_negative_returns_empty_without_raising(label, node_id, variables):
    assert _run(_actors(node_id, variables)) == [], label
    assert _run(_actors(node_id, variables, main=True)) == [], label


def test_negative_without_spi_or_node():
    """SPI 未注册／node·inst 为空都不报错（与现存 handler 同形）。"""
    assert _run(_actors("manager", {"f_manager": "emp"}, user_prov=False)) == []
    assert _run(_actors("manager", {"f_manager": "emp"}, org_prov=False)) == []
    h = FormUserDeptLeaderAssignmentHandler(_UserProv(), _OrgProv())
    assert _run(h.assign(None, ProcessInstance(), "hr")) == []
    assert _run(h.assign(FlowNode(id="manager"), None, "hr")) == []


@pytest.mark.asyncio
async def test_negative_empty_actors_do_not_break_instance():
    """字段缺失 ⇒ 两节点参与者为空但实例仍进行中（不因空参与者打断）。"""
    repo, eng, reg = await _engine(_hr_flow())
    inst = await eng.start_process_instance_by_id(reg["def_id"], "hr", {})
    apply_task = (await repo.find_doing_tasks(inst.id))[0]
    await eng.execute_process_task(apply_task.id, "hr")
    doing = await repo.find_doing_tasks(inst.id)
    assert [t.taskName for t in doing] == ["manager#f_userId"], doing
    assert doing[0].actorIds == [], doing[0].actorIds
    fresh = await repo.find_instance_by_id(inst.id)
    assert str(fresh.state) in ("10", "InstanceState.DOING"), fresh.state


# ─── 优先级与取值细节 ────────────────────────────────────────────────────────────

def test_prefers_f_prefix_over_bare_name():
    assert _find_form_user_field_value({"f_x": "emp", "x": "other"}, "x") == "emp"
    assert _run(_actors("x", {"f_x": "emp", "x": "other"})) == ["leader1", "leader2"]


def test_hash_tier_prefers_bare_field_then_f_variant():
    """第 3 档先裸名再 f_：`manager#userId` 同时有 `userId` 与 `f_userId` ⇒ 取 `userId`。"""
    assert _find_form_user_field_value({"userId": "emp", "f_userId": "other"}, "manager#userId") == "emp"


def test_number_suffix_tier_prefers_f_variant():
    """第 4 档先 f_ 再裸名（与参照实现相反，jeeflow 变量存 f_）：`manager_01` ⇒ 先 `f_manager`。"""
    assert _find_form_user_field_value({"f_manager": "emp", "manager": "other"}, "manager_01") == "emp"


def test_takes_first_non_empty_id_only():
    assert _first_form_user_id("a,b") == "a"
    assert _first_form_user_id(["", "b", "c"]) == "b"
    assert _first_form_user_id("  ,  ,d") == "d"
    assert _first_form_user_id(0) == "0"          # "0" 是合法 id，不被真值判据吃掉
    assert _first_form_user_id("0") == "0"
    assert _first_form_user_id(["", " "]) == ""
    assert _first_form_user_id(None) == ""


def test_zero_id_still_resolves_leader():
    """id 为 "0" 时照常取人（回归 L2-36 那条"语言假值吃掉正常 id"的病灶形状）。"""
    class _ZeroUser:
        async def get_user(self, uid):
            return UserInfo(userId=uid, realName="零号", deptId="D01") if uid == "0" else None

    h = FormUserDeptLeaderAssignmentHandler(_ZeroUser(), _OrgProv())
    got = _run(h.assign(FlowNode(id="manager"), ProcessInstance(variables={"f_manager": "0"}), "hr"))
    assert got == ["leader1", "leader2"], got


# ─── 回归：现存 handler 的行为没被并集查找渗进去 ─────────────────────────────────

def test_existing_dept_leader_still_uses_operator():
    """DeptLeaderAssignmentHandler 读的是操作人；表单里塞别的 id 不该改变它。"""
    h = DeptLeaderAssignmentHandler(_UserProv(), _OrgProv())
    inst = ProcessInstance(id=1, defineId=1, operator="emp", variables={"manager": "other"})
    assert _run(h.assign(FlowNode(id="manager"), inst, "emp")) == ["leader1", "leader2"]
    # 操作人无部门 ⇒ 空（改前改后同形，说明本轮没把并集查找塞进老类）
    inst2 = ProcessInstance(id=2, defineId=1, operator="nodept", variables={"manager": "emp"})
    assert _run(h.assign(FlowNode(id="manager"), inst2, "nodept")) == []


def test_form_field_handler_unchanged_by_union_lookup():
    """现存 FormFieldAssigneeHandler 仍只做三档（f_→裸名→_NN 裸名），第 3 档不认 f_ 变体。"""
    from jeeflow.builtin import FormFieldAssigneeHandler
    h = FormFieldAssigneeHandler()
    inst = ProcessInstance(id=1, defineId=1, operator="hr", variables={"f_userId": "emp"})
    assert _run(h.assign(FlowNode(id="manager#userId"), inst, "hr")) == []


def test_form_field_number_suffix_tier():
    """issues/255：老类第三档＝`_NN` 去后缀后查**裸名 base**，正／负各钉一枚。

    本轮之前这条在 Java/Python/Node/PHP/C# 是活的，只有 Go 判据写死、Rust／MoonBit 没有——
    八语言补齐后由这条测试钉住形状：base 只查裸名，`f_<base>` 是新类扩面。
    """
    from jeeflow.builtin import FormFieldAssigneeHandler
    h = FormFieldAssigneeHandler()

    def actors(node_id, variables):
        inst = ProcessInstance(id=1, defineId=1, operator="hr", variables=variables)
        return _run(h.assign(FlowNode(id=node_id), inst, "hr"))

    assert actors("task_01", {"task": "userA,userB"}) == ["userA", "userB"]
    # 惰性量词的最早可切点：a_b_01 的 base 是 a_b，不是 a
    assert actors("a_b_01", {"a": "u_x", "a_b": "u_c"}) == ["u_c"]
    # 优先级：f_<id> → 裸名 → base 裸名
    assert actors("task_01", {"f_task_01": "u_1", "task_01": "u_2", "task": "u_3"}) == ["u_1"]
    assert actors("task_01", {"task_01": "u_2", "task": "u_3"}) == ["u_2"]
    # 反向：尾巴非纯数字、base 为空、base 只有 f_ 形 ⇒ 三档全落空
    assert actors("task_01x", {"task": "u_3"}) == []
    assert actors("_01", {"": "u_3"}) == []
    assert actors("task_02", {"f_task": "u_3"}) == []


# ─── 注册名／元数据 ──────────────────────────────────────────────────────────────

def test_registration_names_are_top_level_fqns():
    assert HANDLER_FORM_USER_DEPT_LEADER == FQ_LEADER
    assert HANDLER_FORM_USER_DEPT_MAIN_LEADER == FQ_MAIN
    reg = HandlerRegistry()
    register_builtin_assignments(reg, _UserProv(), _OrgProv())
    assert isinstance(reg.resolve_assignment(FQ_LEADER), FormUserDeptLeaderAssignmentHandler)
    assert isinstance(reg.resolve_assignment(FQ_MAIN), FormUserDeptMainLeaderAssignmentHandler)


def test_metadata_exposes_two_new_display_names():
    metas = MetaRegistry().list_handlers("AssignmentHandler")
    assert len(metas) == 11
    form_user = {m.className: (m.displayName, m.order) for m in metas[7:9]}
    assert form_user[FQ_LEADER] == ("表单指定用户所属部门经理", 70)
    assert form_user[FQ_MAIN] == ("表单指定用户所属部门分管领导", 80)
