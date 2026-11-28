"""issues/258：表单指定 1–N 个部门的负责人／分管领导处理器。

覆盖四档字段查找、多部门与保序去重、数组不二次拆、非法/0/空值、两档 SPI、
失败不抛、运行时注册、元数据与内存引擎接线；不启动服务或占用端口。
"""
import json

import pytest

from jeeflow import (
    HANDLER_FORM_DEPT_LEADER, HANDLER_FORM_DEPT_MAIN_LEADER,
    FormDeptLeaderAssignmentHandler, FormDeptMainLeaderAssignmentHandler,
)
from jeeflow.builtin import register_builtin_assignments
from jeeflow.engine import EngineImpl
from jeeflow.extensions import EngineExtensions, HandlerRegistry
from jeeflow.metadata import HandlerRegistry as MetaRegistry
from jeeflow.memory import MemoryRepository
from jeeflow.model import FlowNode, ProcessDefine, ProcessInstance, UserInfo

FQ_LEADER = "com.mldong.jeeflow.interceptor.impl.FormDeptLeaderAssignmentHandler"
FQ_MAIN = "com.mldong.jeeflow.interceptor.impl.FormDeptMainLeaderAssignmentHandler"

LEADERS = {
    "deptA": ["leaderA", "shared"],
    "deptB": ["leaderB", "shared"],
    "deptC": ["leaderC1", "leaderC2"],
    "0": ["zeroLeader"],
}
MAIN_LEADERS = {
    "deptA": ["mainA"],
    "deptB": ["mainB"],
    "deptC": ["mainC"],
    "0": ["zeroMain"],
}


class _OrgProv:
    def __init__(self, leaders=None, main_leaders=None, fail=()):
        self.leaders = LEADERS if leaders is None else leaders
        self.main_leaders = MAIN_LEADERS if main_leaders is None else main_leaders
        self.fail = set(fail)
        self.calls = []

    async def find_dept_leaders(self, dept_id):
        self.calls.append(("leaders", dept_id))
        if dept_id in self.fail:
            raise RuntimeError("leader lookup failed")
        return self.leaders.get(dept_id)

    async def find_dept_main_leaders(self, dept_id):
        self.calls.append(("main", dept_id))
        if dept_id in self.fail:
            raise RuntimeError("main leader lookup failed")
        return self.main_leaders.get(dept_id)

    async def find_by_role(self, role_code):
        return []


class _UserProv:
    async def get_user(self, user_id):
        return UserInfo(userId=user_id, realName=user_id, deptId="deptA") if user_id else None


async def _actors(variables, node_id="depts", main=False, org_prov=None, with_org=True):
    prov = org_prov if org_prov is not None else (_OrgProv() if with_org else None)
    cls = FormDeptMainLeaderAssignmentHandler if main else FormDeptLeaderAssignmentHandler
    handler = cls(None, prov)
    node = FlowNode(id=node_id, type="snaker:task")
    inst = ProcessInstance(id=1, defineId=1, operator="clerk", variables=variables)
    return await handler.assign(node, inst, "clerk")


# ─── 四档字段查找：顺序即优先级，键存在即命中 ────────────────────────────────

FIELD_CASES = [
    ("f_ 节点键优先", "depts", {"f_depts": "deptA", "depts": "deptB"}, "deptA"),
    ("裸名回落", "depts", {"depts": "deptB"}, "deptB"),
    ("# 取最后一段裸名优先", "manager#old#deptIds",
     {"deptIds": "deptA", "f_deptIds": "deptB", "old": "deptC"}, "deptA"),
    ("# 裸名缺失回落 f_", "manager#deptIds", {"f_deptIds": "deptB"}, "deptB"),
    ("_NN 去后缀先 f_", "depts_01", {"f_depts": "deptA", "depts": "deptB"}, "deptA"),
    ("_NN 去后缀裸名回落", "depts_01", {"depts": "deptA"}, "deptA"),
    ("_NN 惰性 base", "a_b_01", {"a_b": "deptA", "a": "deptB"}, "deptA"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("label,node_id,variables,dept_id", FIELD_CASES, ids=[c[0] for c in FIELD_CASES])
async def test_four_tier_field_lookup(label, node_id, variables, dept_id):
    org = _OrgProv()
    assert await _actors(variables, node_id, org_prov=org) == LEADERS[dept_id], label
    assert org.calls == [("leaders", dept_id)], label


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, "", "   ", [], [None, " "]])
async def test_existing_empty_key_does_not_fall_back(value):
    org = _OrgProv()
    assert await _actors({"f_depts": value, "depts": "deptB"}, org_prov=org) == []
    assert org.calls == []


# ─── 部门列表解析、合并顺序与去重 ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_comma_string_resolves_all_departments_in_order():
    org = _OrgProv()
    assert await _actors({"depts": " deptA, , deptB, deptA "}, org_prov=org) == [
        "leaderA", "shared", "leaderB",
    ]
    assert org.calls == [("leaders", "deptA"), ("leaders", "deptB")]


@pytest.mark.asyncio
async def test_array_elements_are_not_resplit_and_empty_items_are_dropped():
    org = _OrgProv()
    assert await _actors({"depts": ["deptA", "deptB", "", None, " deptA ", 0]}, org_prov=org) == [
        "leaderA", "shared", "leaderB", "zeroLeader",
    ]
    assert org.calls == [
        ("leaders", "deptA"), ("leaders", "deptB"), ("leaders", "0"),
    ]


@pytest.mark.asyncio
async def test_array_element_with_comma_is_one_department_id():
    org = _OrgProv()
    assert await _actors({"depts": ["deptA,deptB"]}, org_prov=org) == []
    assert org.calls == [("leaders", "deptA,deptB")]


@pytest.mark.asyncio
async def test_duplicate_departments_are_queried_once():
    org = _OrgProv()
    assert await _actors({"depts": "deptB,deptA,deptB, deptA"}, org_prov=org) == [
        "leaderB", "shared", "leaderA",
    ]
    assert org.calls == [("leaders", "deptB"), ("leaders", "deptA")]


@pytest.mark.asyncio
async def test_final_user_ids_are_trimmed_and_deduped_in_spi_order():
    org = _OrgProv(leaders={
        "deptA": [" leaderA ", "shared", None, "", "leaderA"],
        "deptB": ["shared", " leaderB ", "leaderB"],
    })
    assert await _actors({"depts": "deptA,deptB"}, org_prov=org) == [
        "leaderA", "shared", "leaderB",
    ]


@pytest.mark.asyncio
async def test_invalid_or_leaderless_department_does_not_block_other_departments():
    org = _OrgProv()
    assert await _actors({"depts": "ghost,deptA,empty,deptB"}, org_prov=org) == [
        "leaderA", "shared", "leaderB",
    ]
    assert org.calls == [
        ("leaders", "ghost"), ("leaders", "deptA"),
        ("leaders", "empty"), ("leaders", "deptB"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [0, "0", " 0 ", [0, " 0 ", ""]])
async def test_zero_department_id_is_legal(value):
    org = _OrgProv()
    assert await _actors({"depts": value}, org_prov=org) == ["zeroLeader"]
    assert org.calls == [("leaders", "0")]


@pytest.mark.asyncio
async def test_main_leader_variant_uses_only_main_spi():
    org = _OrgProv()
    assert await _actors({"depts": "deptA,deptB"}, main=True, org_prov=org) == ["mainA", "mainB"]
    assert org.calls == [("main", "deptA"), ("main", "deptB")]


@pytest.mark.asyncio
async def test_multiple_leaders_keep_spi_order():
    assert await _actors({"depts": "deptC"}) == ["leaderC1", "leaderC2"]


# ─── 失败语义：空结果且不抛 ────────────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, "", " , ", [], ["", None, " "], {}])
async def test_empty_or_non_list_values_return_empty_without_spi_calls(value):
    org = _OrgProv()
    assert await _actors({"depts": value}, org_prov=org) == []
    assert org.calls == []


@pytest.mark.asyncio
async def test_missing_field_or_empty_node_returns_empty():
    org = _OrgProv()
    assert await _actors({}, org_prov=org) == []
    assert await _actors({"f_": "deptA"}, node_id="", org_prov=org) == []
    assert org.calls == []


@pytest.mark.asyncio
async def test_missing_org_spi_returns_empty_for_both_variants():
    assert await _actors({"depts": "deptA"}, with_org=False) == []
    assert await _actors({"depts": "deptA"}, main=True, with_org=False) == []


@pytest.mark.asyncio
async def test_no_node_or_no_instance_returns_empty_for_both_variants():
    org = _OrgProv()
    inst = ProcessInstance(variables={"depts": "deptA"})
    node = FlowNode(id="depts", type="snaker:task")
    for cls in (FormDeptLeaderAssignmentHandler, FormDeptMainLeaderAssignmentHandler):
        handler = cls(None, org)
        assert await handler.assign(None, inst, "clerk") == []
        assert await handler.assign(node, None, "clerk") == []
    assert org.calls == []


@pytest.mark.asyncio
async def test_spi_failure_skips_failed_department_and_continues():
    org = _OrgProv(fail={"deptA"})
    assert await _actors({"depts": "deptA,deptB"}, org_prov=org) == ["leaderB", "shared"]
    assert org.calls == [("leaders", "deptA"), ("leaders", "deptB")]


@pytest.mark.asyncio
async def test_all_spi_failures_return_empty_without_raising():
    org = _OrgProv(fail={"deptA", "deptB"})
    assert await _actors({"depts": "deptA,deptB"}, org_prov=org) == []
    assert await _actors({"depts": "deptA,deptB"}, main=True, org_prov=org) == []


@pytest.mark.asyncio
async def test_department_handlers_do_not_use_user_provider():
    class _UnusedUserProv:
        async def get_user(self, user_id):
            raise AssertionError("部门 ID 不得当用户 ID 查询")

    handler = FormDeptLeaderAssignmentHandler(_UnusedUserProv(), _OrgProv())
    inst = ProcessInstance(variables={"depts": "deptA"})
    assert await handler.assign(FlowNode(id="depts"), inst, "clerk") == ["leaderA", "shared"]


# ─── 注册、元数据与内存引擎接线 ────────────────────────────────────────────

def test_registration_names_and_runtime_registry():
    assert HANDLER_FORM_DEPT_LEADER == FQ_LEADER
    assert HANDLER_FORM_DEPT_MAIN_LEADER == FQ_MAIN
    registry = HandlerRegistry()
    register_builtin_assignments(registry, None, _OrgProv())
    assert isinstance(registry.resolve_assignment(FQ_LEADER), FormDeptLeaderAssignmentHandler)
    assert isinstance(registry.resolve_assignment(FQ_MAIN), FormDeptMainLeaderAssignmentHandler)


def test_metadata_count_names_display_names_and_orders():
    metas = MetaRegistry().list_handlers("AssignmentHandler")
    assert len(metas) == 13
    assert [(m.className, m.displayName, m.order) for m in metas[10:12]] == [
        (FQ_LEADER, "表单指定部门负责人", 90),
        (FQ_MAIN, "表单指定部门分管领导", 100),
    ]


@pytest.mark.asyncio
async def test_memory_engine_resolves_fqcn_and_assigns_multi_department_leaders():
    class _Gen:
        def __init__(self):
            self.n = 258000

        def next_id(self):
            self.n += 1
            return self.n

    def task(node_id, handler, y):
        return {
            "id": node_id, "type": "snaker:task", "x": 480, "y": y,
            "text": {"value": node_id},
            "properties": {"assignee": "", "taskType": 0, "performType": 0,
                           "assignmentHandler": handler},
        }

    flow = {
        "name": "form-dept-leaders", "displayName": "表单指定部门领导", "type": "approval",
        "nodes": [
            {"id": "start", "type": "snaker:start", "properties": {}, "text": {"value": "开始"}},
            {"id": "apply", "type": "snaker:task",
             "properties": {"assignee": "applicant", "performType": 0}, "text": {"value": "发起申请"}},
            task("handle#deptIds", FQ_LEADER, 500),
            {"id": "end", "type": "snaker:end", "properties": {}, "text": {"value": "结束"}},
        ],
        "edges": [
            {"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
            {"id": "e1", "sourceNodeId": "apply", "targetNodeId": "handle#deptIds", "properties": {}},
            {"id": "e2", "sourceNodeId": "handle#deptIds", "targetNodeId": "end", "properties": {}},
        ],
    }
    repo = MemoryRepository()
    registry = HandlerRegistry()
    register_builtin_assignments(registry, _UserProv(), _OrgProv())
    engine = EngineImpl(repo, _UserProv(), _Gen(), None)
    engine.set_extensions(EngineExtensions(registry=registry))
    define = ProcessDefine(name=flow["name"], displayName=flow["displayName"], type=flow["type"],
                           state=1, content=json.dumps(flow, ensure_ascii=False))
    repo.add_define(define)

    inst = await engine.start_process_instance_by_id(define.id, "clerk", {"deptIds": "deptA,deptB"})
    apply_task = (await repo.find_doing_tasks(inst.id))[0]
    await engine.execute_process_task(apply_task.id, "clerk")
    doing = await repo.find_doing_tasks(inst.id)
    assert [t.taskName for t in doing] == ["handle#deptIds"]
    assert doing[0].actorIds == ["leaderA", "shared", "leaderB"]
