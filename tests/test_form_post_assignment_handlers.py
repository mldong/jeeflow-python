"""issues/259：任务节点关联岗位与表单指定岗位处理器（Python 栈）。

覆盖四档字段查找、null/数组/0、岗位与 userId 保序去重、单岗位异常隔离、
TaskPost、SPI/包出口、内置注册、元数据递增、旧 handler 回归与内存引擎接线；
不启动服务、不访问数据库、不占用共享 flows 编号。
"""
import inspect
import json

import pytest

from jeeflow import (
    HANDLER_FORM_POST_ASSIGNMENT, HANDLER_TASK_POST_ASSIGNEE,
    FormPostAssignmentHandler, PostUserProvider, TaskPostAssigneeHandler,
)
from jeeflow.builtin import (
    FormFieldAssigneeHandler, TaskRoleAssigneeHandler, register_builtin_assignments,
)
from jeeflow.engine import EngineImpl
from jeeflow.extensions import EngineExtensions, HandlerRegistry
from jeeflow.metadata import HandlerRegistry as MetaRegistry
from jeeflow.memory import MemoryRepository
from jeeflow.model import FlowNode, ProcessDefine, ProcessInstance, UserInfo

FQ_TASK_POST = "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$TaskPostAssigneeHandler"
FQ_FORM_POST = "com.mldong.jeeflow.interceptor.impl.FormPostAssignmentHandler"

POST_MEMBERS = {
    "postA": ["userA", "shared"],
    "postB": ["userB", "shared"],
    "postC": ["userC1", "userC2"],
    "0": ["zeroUser"],
    "empty": [],
}


class _PostProv(PostUserProvider):
    def __init__(self, members=None, fail=()):
        self.members = POST_MEMBERS if members is None else members
        self.fail = set(fail)
        self.calls = []

    async def find_by_post(self, post_code):
        self.calls.append(post_code)
        if post_code in self.fail:
            raise RuntimeError("post lookup failed")
        return self.members.get(post_code)


class _UserProv:
    async def get_user(self, user_id):
        return UserInfo(userId=user_id, realName=user_id, deptId="deptA")


async def _actors(variables, node_id="posts", post_prov=None, with_post=True):
    prov = post_prov if post_prov is not None else (_PostProv() if with_post else None)
    handler = FormPostAssignmentHandler(prov)
    node = FlowNode(id=node_id, type="snaker:task")
    inst = ProcessInstance(id=1, defineId=1, operator="clerk", variables=variables)
    return await handler.assign(node, inst, "clerk")


async def _task_actors(node_id="postA", post_prov=None, with_post=True):
    prov = post_prov if post_prov is not None else (_PostProv() if with_post else None)
    handler = TaskPostAssigneeHandler(prov)
    node = None if node_id is None else FlowNode(id=node_id, type="snaker:task")
    inst = ProcessInstance(id=1, defineId=1, operator="clerk",
                           variables={"f_postA": "postB", "postA": "postC"})
    return await handler.assign(node, inst, "clerk")


# ─── SPI 与包出口 ─────────────────────────────────────────────────────────────

def test_post_user_provider_is_optional_async_spi_and_exported():
    assert inspect.iscoroutinefunction(PostUserProvider.find_by_post)
    assert inspect.isabstract(PostUserProvider)
    assert isinstance(_PostProv(), PostUserProvider)


# ─── 四档字段查找：顺序即优先级，键存在即命中 ────────────────────────────────

FIELD_CASES = [
    ("f_ 节点键优先", "posts", {"f_posts": "postA", "posts": "postB"}, "postA"),
    ("裸名回落", "posts", {"posts": "postB"}, "postB"),
    ("# 取最后一段裸名优先", "manager#old#postCodes",
     {"postCodes": "postA", "f_postCodes": "postB", "old": "postC"}, "postA"),
    ("# 裸名缺失回落 f_", "manager#postCodes", {"f_postCodes": "postB"}, "postB"),
    ("_NN 去后缀先 f_", "posts_01", {"f_posts": "postA", "posts": "postB"}, "postA"),
    ("_NN 去后缀裸名回落", "posts_01", {"posts": "postA"}, "postA"),
    ("_NN 惰性 base", "a_b_01", {"a_b": "postA", "a": "postB"}, "postA"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("label,node_id,variables,post_code", FIELD_CASES, ids=[c[0] for c in FIELD_CASES])
async def test_four_tier_field_lookup(label, node_id, variables, post_code):
    post = _PostProv()
    assert await _actors(variables, node_id, post_prov=post) == POST_MEMBERS[post_code], label
    assert post.calls == [post_code], label


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, "", "   ", [], [None, " "], {}])
async def test_existing_empty_key_does_not_fall_back(value):
    post = _PostProv()
    assert await _actors({"f_posts": value, "posts": "postB"}, post_prov=post) == []
    assert post.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("node_id,variables", [
    ("_01", {"f_": "postA"}),
    ("posts_01x", {"posts": "postA"}),
])
async def test_lazy_suffix_regex_rejects_empty_base_and_non_numeric_tail(node_id, variables):
    post = _PostProv()
    assert await _actors(variables, node_id, post_prov=post) == []
    assert post.calls == []


# ─── 岗位编码解析、查询顺序与去重 ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_comma_string_resolves_all_posts_in_order():
    post = _PostProv()
    assert await _actors({"posts": " postA, , postB, postA "}, post_prov=post) == [
        "userA", "shared", "userB",
    ]
    assert post.calls == ["postA", "postB"]


@pytest.mark.asyncio
async def test_array_elements_are_not_resplit_and_empty_items_are_dropped():
    post = _PostProv()
    assert await _actors({"posts": ["postA", "postB", "", None, " postA ", 0]},
                         post_prov=post) == ["userA", "shared", "userB", "zeroUser"]
    assert post.calls == ["postA", "postB", "0"]


@pytest.mark.asyncio
async def test_tuple_collection_elements_are_not_resplit():
    post = _PostProv()
    assert await _actors({"posts": ("postB", "postA")}, post_prov=post) == [
        "userB", "shared", "userA",
    ]
    assert post.calls == ["postB", "postA"]


@pytest.mark.asyncio
async def test_array_element_with_comma_is_one_post_code():
    post = _PostProv()
    assert await _actors({"posts": ["postA,postB"]}, post_prov=post) == []
    assert post.calls == ["postA,postB"]


@pytest.mark.asyncio
async def test_duplicate_posts_are_queried_once_in_first_seen_order():
    post = _PostProv()
    assert await _actors({"posts": "postB,postA,postB, postA"}, post_prov=post) == [
        "userB", "shared", "userA",
    ]
    assert post.calls == ["postB", "postA"]


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [0, "0", " 0 ", [0, " 0 ", ""], (0,)])
async def test_zero_post_code_is_legal(value):
    post = _PostProv()
    assert await _actors({"posts": value}, post_prov=post) == ["zeroUser"]
    assert post.calls == ["0"]


@pytest.mark.asyncio
async def test_final_user_ids_are_trimmed_and_deduped_in_spi_order():
    post = _PostProv(members={
        "postA": [" userA ", "shared", None, "", "userA", 0, " 0 "],
        "postB": ["shared", " userB ", "userB"],
    })
    assert await _actors({"posts": "postA,postB"}, post_prov=post) == [
        "userA", "shared", "0", "userB",
    ]


@pytest.mark.asyncio
async def test_provider_bare_string_is_one_user_id_not_characters():
    post = _PostProv(members={"postA": " userA "})
    assert await _actors({"posts": "postA"}, post_prov=post) == ["userA"]


# ─── 失败语义：空结果且不抛 ──────────────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, "", " , ", [], ["", None, " "], {}])
async def test_empty_or_non_list_values_return_empty_without_spi_calls(value):
    post = _PostProv()
    assert await _actors({"posts": value}, post_prov=post) == []
    assert post.calls == []


@pytest.mark.asyncio
async def test_missing_field_or_empty_node_returns_empty():
    post = _PostProv()
    assert await _actors({}, post_prov=post) == []
    assert await _actors({"f_": "postA"}, node_id="", post_prov=post) == []
    assert post.calls == []


@pytest.mark.asyncio
async def test_no_node_or_no_instance_returns_empty():
    post = _PostProv()
    handler = FormPostAssignmentHandler(post)
    assert await handler.assign(None, ProcessInstance(variables={"posts": "postA"}), "clerk") == []
    assert await handler.assign(FlowNode(id="posts"), None, "clerk") == []
    assert post.calls == []


@pytest.mark.asyncio
async def test_missing_post_spi_returns_empty():
    assert await _actors({"posts": "postA"}, with_post=False) == []
    assert await _task_actors("postA", with_post=False) == []


@pytest.mark.asyncio
async def test_spi_failure_skips_failed_post_and_continues():
    post = _PostProv(fail={"postA"})
    assert await _actors({"posts": "postA,postB"}, post_prov=post) == ["userB", "shared"]
    assert post.calls == ["postA", "postB"]


@pytest.mark.asyncio
async def test_all_spi_failures_return_empty_without_raising():
    post = _PostProv(fail={"postA", "postB"})
    assert await _actors({"posts": "postA,postB"}, post_prov=post) == []
    assert post.calls == ["postA", "postB"]


@pytest.mark.asyncio
async def test_missing_or_memberless_post_does_not_block_other_posts():
    post = _PostProv()
    assert await _actors({"posts": "ghost,postA,empty,postB"}, post_prov=post) == [
        "userA", "shared", "userB",
    ]
    assert post.calls == ["ghost", "postA", "empty", "postB"]


# ─── TaskPost：节点唯一编码即岗位编码，保留 Provider 顺序 ────────────────────

@pytest.mark.asyncio
async def test_task_post_uses_node_code_and_keeps_spi_order():
    post = _PostProv()
    assert await _task_actors("postC", post_prov=post) == ["userC1", "userC2"]
    assert post.calls == ["postC"]


@pytest.mark.asyncio
async def test_task_post_does_not_dedupe_or_sort_provider_members():
    post = _PostProv(members={"postA": ["userB", "userA", "userB"]})
    assert await _task_actors("postA", post_prov=post) == ["userB", "userA", "userB"]


@pytest.mark.asyncio
@pytest.mark.parametrize("node_id", [None, ""])
async def test_task_post_missing_or_empty_node_is_empty(node_id):
    post = _PostProv()
    assert await _task_actors(node_id, post_prov=post) == []
    assert post.calls == []


@pytest.mark.asyncio
async def test_task_post_missing_or_memberless_post_is_empty():
    post = _PostProv()
    assert await _task_actors("ghost", post_prov=post) == []
    assert await _task_actors("empty", post_prov=post) == []
    assert post.calls == ["ghost", "empty"]


@pytest.mark.asyncio
async def test_task_post_spi_failure_is_empty_without_raising():
    post = _PostProv(fail={"postA"})
    assert await _task_actors("postA", post_prov=post) == []
    assert post.calls == ["postA"]


@pytest.mark.asyncio
async def test_task_post_zero_code_is_legal():
    post = _PostProv()
    assert await _task_actors("0", post_prov=post) == ["zeroUser"]
    assert post.calls == ["0"]


# ─── 注册、元数据与旧 handler 回归 ───────────────────────────────────────────

def test_registration_names_runtime_registry_and_package_exports():
    assert HANDLER_TASK_POST_ASSIGNEE == FQ_TASK_POST
    assert HANDLER_FORM_POST_ASSIGNMENT == FQ_FORM_POST
    registry = HandlerRegistry()
    register_builtin_assignments(registry, post_prov=_PostProv())
    assert isinstance(registry.resolve_assignment(FQ_TASK_POST), TaskPostAssigneeHandler)
    assert isinstance(registry.resolve_assignment(FQ_FORM_POST), FormPostAssignmentHandler)


def test_metadata_count_names_display_names_and_orders():
    metas = MetaRegistry().list_handlers("AssignmentHandler")
    assert len(metas) == 13
    assert [(m.className, m.displayName, m.order) for m in metas[7:8]] == [
        (FQ_TASK_POST, "根据任务节点唯一编码关联岗位分配参与者", 65),
    ]
    assert [(m.className, m.displayName, m.order) for m in metas[12:13]] == [
        (FQ_FORM_POST, "表单指定岗位分配参与者", 110),
    ]
    assert "$" in FQ_TASK_POST and "$" not in FQ_FORM_POST


@pytest.mark.asyncio
async def test_existing_task_role_still_uses_org_spi_not_post_spi():
    class _OrgProv:
        async def find_by_role(self, role_code):
            return ["roleUser"]

    class _UnusedPostProv:
        async def find_by_post(self, post_code):
            raise AssertionError("TaskRole 不得查询岗位 SPI")

    handler = TaskRoleAssigneeHandler(_OrgProv())
    assert await handler.assign(FlowNode(id="postA"), ProcessInstance(), "clerk") == ["roleUser"]


@pytest.mark.asyncio
async def test_existing_form_field_handler_keeps_old_three_tier_lookup():
    handler = FormFieldAssigneeHandler()
    inst = ProcessInstance(variables={"f_postCodes": "userA"})
    assert await handler.assign(FlowNode(id="manager#postCodes"), inst, "clerk") == []
    inst = ProcessInstance(variables={"task": "userA,userB"})
    assert await handler.assign(FlowNode(id="task_01"), inst, "clerk") == ["userA", "userB"]


# ─── 内存引擎接线：两个 FQCN 都能实际取到岗位成员 ────────────────────────────

@pytest.mark.asyncio
async def test_memory_engine_resolves_task_and_form_post_fqcns():
    class _Gen:
        def __init__(self):
            self.n = 259000

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
        "name": "post-assignments", "displayName": "岗位参与者", "type": "approval",
        "nodes": [
            {"id": "start", "type": "snaker:start", "properties": {}, "text": {"value": "开始"}},
            {"id": "apply", "type": "snaker:task",
             "properties": {"assignee": "applicant", "performType": 0}, "text": {"value": "发起申请"}},
            task("archive_clerk", FQ_TASK_POST, 400),
            task("handle#postCodes", FQ_FORM_POST, 550),
            {"id": "end", "type": "snaker:end", "properties": {}, "text": {"value": "结束"}},
        ],
        "edges": [
            {"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
            {"id": "e1", "sourceNodeId": "apply", "targetNodeId": "archive_clerk", "properties": {}},
            {"id": "e2", "sourceNodeId": "archive_clerk", "targetNodeId": "handle#postCodes",
             "properties": {}},
            {"id": "e3", "sourceNodeId": "handle#postCodes", "targetNodeId": "end", "properties": {}},
        ],
    }
    post = _PostProv(members={"archive_clerk": ["archiver"], **POST_MEMBERS})
    repo = MemoryRepository()
    registry = HandlerRegistry()
    register_builtin_assignments(registry, _UserProv(), post_prov=post)
    engine = EngineImpl(repo, _UserProv(), _Gen(), None)
    engine.set_extensions(EngineExtensions(registry=registry))
    define = ProcessDefine(name=flow["name"], displayName=flow["displayName"], type=flow["type"],
                           state=1, content=json.dumps(flow, ensure_ascii=False))
    repo.add_define(define)

    inst = await engine.start_process_instance_by_id(define.id, "clerk", {"postCodes": "postA,postB"})
    apply_task = (await repo.find_doing_tasks(inst.id))[0]
    await engine.execute_process_task(apply_task.id, "clerk")
    doing = await repo.find_doing_tasks(inst.id)
    assert [t.taskName for t in doing] == ["archive_clerk"]
    assert doing[0].actorIds == ["archiver"]

    await engine.execute_process_task(doing[0].id, "archiver")
    doing = await repo.find_doing_tasks(inst.id)
    assert [t.taskName for t in doing] == ["handle#postCodes"]
    assert doing[0].actorIds == ["userA", "shared", "userB"]
    assert post.calls == ["archive_clerk", "postA", "postB"]
