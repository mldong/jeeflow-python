"""issues/178 R12 · 门面 candidatePage 候选映射链（findById→getUser→兜底）＋行键 id 三级回落。
形状照 java JeeflowFacade.java:945-1012；三档对齐 issues/178 §4.1：
  档① 仅 findById（命中）：行 id/realName/userId/deptName 取 findById 值，且压过 getUser；
     档①b findById 无 id 键（有 userId）：id 走 src.userId 回落；异步 fn 版同判；
  档② 无 findById 有 getUser：realName/deptName 取 getUser，id 走 src.userId 回落；
  档③ 双无：兜底 id==actor、realName==id（回归档，改前行为）。
修前红样：恒兜底 {c,c,c}（realName 丢真名）；夹具＝内联四节点流（start→apply→review→end）。
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow.engine import EngineImpl                                   # noqa: E402
from jeeflow.facade import JeeflowFacade                                # noqa: E402
from jeeflow.memory import MemoryExtRepository, MemoryRepository        # noqa: E402
from jeeflow.model import UserInfo                                      # noqa: E402
from jeeflow.spi import IDGenerator                                     # noqa: E402


class _Gen(IDGenerator):
    def __init__(self):
        self.n = 800000

    def next_id(self) -> int:
        self.n += 1
        return self.n


class _UserProv:
    """第二级桩：realName 带「用户」前缀，便于与第一级真名区分（优先级反证）。"""

    def __init__(self, mapping=None):
        self.calls = []
        self.mapping = mapping or {"u100": UserInfo(userId="u100", realName="用户u100", deptName="测试部门")}

    async def get_user(self, user_id):
        self.calls.append(user_id)
        return self.mapping.get(user_id)


class _MissUserProv:
    async def get_user(self, user_id):
        return None


def _i178_flow():
    return {
        "name": "i178-cand-chain",
        "displayName": "i178候选链",
        "type": "approval",
        "nodes": [
            {"id": "start", "type": "snaker:start", "properties": {}, "text": {"value": "开始"}},
            {"id": "apply", "type": "snaker:task",
             "properties": {"assignee": "applicant", "performType": 0}, "text": {"value": "发起"}},
            {"id": "review", "type": "snaker:task",
             "properties": {"assignee": "leader", "candidateUsers": "u100", "performType": 0},
             "text": {"value": "审核"}},
            {"id": "end", "type": "snaker:end", "properties": {}, "text": {"value": "结束"}},
        ],
        "edges": [
            {"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
            {"id": "e1", "sourceNodeId": "apply", "targetNodeId": "review", "properties": {}},
            {"id": "e2", "sourceNodeId": "review", "targetNodeId": "end", "properties": {}},
        ],
    }


async def _first_row(user_prov=None):
    repo = MemoryRepository()
    eng = EngineImpl(repo, user_prov, _Gen(), None)
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    r = await facade.flow("processDefine/deploy",
                          {"content": json.dumps(_i178_flow(), ensure_ascii=False)})
    assert r["code"] == 0, r
    def_id = int(r["data"]["processDefineId"])
    inst = await eng.start_process_instance_by_id(def_id, "user1")
    doing = await repo.find_doing_tasks(inst.id)
    assert len(doing) == 1 and doing[0].taskName == "apply", doing
    return facade, doing[0].id


async def _candidate_row(facade, task_id):
    r = await facade.flow("processTask/candidatePage", {"processTaskId": task_id})
    assert r["code"] == 0, r
    rows = r["data"]["rows"]
    assert len(rows) == 1, rows
    return rows[0]


@pytest.mark.asyncio
async def test_i178_find_by_id_first_wins():
    """档① 首级命中且压过 getUser（第二级桩真名「用户u100」不得出现）。"""
    facade, tid = await _first_row(user_prov=_UserProv())
    facade.set_user_search_by_id(lambda uid: {
        "id": "U100", "userId": "U100", "realName": "张三", "deptName": "研发部"})
    row = await _candidate_row(facade, tid)
    assert row == {"id": "U100", "realName": "张三", "userId": "U100", "deptName": "研发部"}, row


@pytest.mark.asyncio
async def test_i178_id_fallback_within_src():
    """档①b findById 无 id 键：id 走 src.userId 回落（异步 fn 形态）。"""
    facade, tid = await _first_row(user_prov=_UserProv())

    async def by_id(uid):
        return {"userId": "U200", "realName": "李四"}

    facade.set_user_search_by_id(by_id)
    row = await _candidate_row(facade, tid)
    assert row == {"id": "U200", "realName": "李四", "userId": "U200"}, row


@pytest.mark.asyncio
async def test_i178_get_user_second_level():
    """档② 无 findById：realName/deptName 取 getUser，id 走 src.userId 回落。"""
    up = _UserProv()
    facade, tid = await _first_row(user_prov=up)
    row = await _candidate_row(facade, tid)
    assert row == {"id": "u100", "realName": "用户u100", "userId": "u100", "deptName": "测试部门"}, row
    assert "u100" in up.calls, up.calls


@pytest.mark.asyncio
async def test_i178_fallback_both_miss():
    """档③ 两级都未命中：兜底 id==actor、realName==id、userId==actor（改前行为）。"""
    facade, tid = await _first_row(user_prov=_MissUserProv())
    row = await _candidate_row(facade, tid)
    assert row == {"id": "u100", "realName": "u100", "userId": "u100"}, row
