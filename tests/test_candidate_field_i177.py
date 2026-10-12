"""issues/177 R11 · 门面 candidatePage「field 子对象」候选兜底——旧设计器把 candidateUsers /
candidateGroups 写进 properties.field，门面 _next_task_candidates 只认顶层 ⇒ 候选恒空、
回落 user_search 钩子（未配置直接报错）。修后顶层优先、field 兜底（形状对齐 issues/163
会签条件附带读形）。夹具＝内联四节点流（start→apply→review→end）。
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow.engine import EngineImpl                                   # noqa: E402
from jeeflow.facade import JeeflowFacade                                # noqa: E402
from jeeflow.memory import MemoryExtRepository, MemoryRepository        # noqa: E402
from jeeflow.spi import IDGenerator                                     # noqa: E402


class _Gen(IDGenerator):
    def __init__(self):
        self.n = 700000

    def next_id(self) -> int:
        self.n += 1
        return self.n


class _OrgProv:
    async def find_dept_leaders(self, dept_id): return []
    async def find_dept_main_leaders(self, dept_id): return []

    async def find_by_role(self, role_code):
        return ["finA", "finB"] if role_code == "finance" else []


def _i177_flow(top_level_users=None):
    props = {"width": 100, "height": 50, "form": "review-form", "assignee": "leader",
             "field": {"candidateUsers": "userA,userB", "candidateGroups": "finance"}}
    if top_level_users:
        props["candidateUsers"] = top_level_users
    return {
        "name": "i177-candidate-field",
        "displayName": "i177候选字段",
        "type": "approval",
        "nodes": [
            {"id": "start", "type": "snaker:start", "properties": {}, "text": {"value": "开始"}},
            {"id": "apply", "type": "snaker:task",
             "properties": {"assignee": "applicant", "performType": 0}, "text": {"value": "发起"}},
            {"id": "review", "type": "snaker:task", "properties": props, "text": {"value": "审核"}},
            {"id": "end", "type": "snaker:end", "properties": {}, "text": {"value": "结束"}},
        ],
        "edges": [
            {"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
            {"id": "e1", "sourceNodeId": "apply", "targetNodeId": "review", "properties": {}},
            {"id": "e2", "sourceNodeId": "review", "targetNodeId": "end", "properties": {}},
        ],
    }


async def _candidates(top_level_users=None):
    repo = MemoryRepository()
    eng = EngineImpl(repo, None, _Gen(), None)   # 无宿主 SPI ⇒ 内置默认件
    facade = JeeflowFacade(eng, repo, MemoryExtRepository(), org_prov=_OrgProv())
    r = await facade.flow("processDefine/deploy",
                          {"content": json.dumps(_i177_flow(top_level_users), ensure_ascii=False)})
    assert r["code"] == 0, r
    def_id = int(r["data"]["processDefineId"])
    # 直接启动（不自动完成 apply）→ apply 任务 → candidatePage 查 review 候选
    inst = await eng.start_process_instance_by_id(def_id, "user1")
    doing = await repo.find_doing_tasks(inst.id)
    assert len(doing) == 1 and doing[0].taskName == "apply", doing
    r = await facade.flow("processTask/candidatePage", {"processTaskId": doing[0].id})
    assert r["code"] == 0, r
    return sorted(x["userId"] for x in r["data"]["rows"])


@pytest.mark.asyncio
async def test_i177_field_nested_candidates_visible():
    """① field 内两键 ⇒ 照读 4 候选（改前恒 0 ⇒ 未配钩子直接报错）。"""
    got = await _candidates()
    assert got == ["finA", "finB", "userA", "userB"], \
        f"field 内两键应读出 4 候选（改前恒 0）: {got}"


@pytest.mark.asyncio
async def test_i177_top_level_wins():
    """② 顶层优先：顶层 candidateUsers 压过 field 内版本（candidateGroups 腿独立兜底）。"""
    got = await _candidates(top_level_users="userX")
    assert got == ["finA", "finB", "userX"], f"顶层 candidateUsers 应压过 field 内版本、groups 仍兜底: {got}"
