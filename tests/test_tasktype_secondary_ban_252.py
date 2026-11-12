"""节点级 taskType=1（协办）解析期禁配（issues/252，规范 02 §4.4）——Python 腿。

正向：折成 SECONDARY 的全部输入形（1／"1"／"协办"／"SECONDARY"，大小写不敏感——
本栈无既有 taskType 容错表，判定照 is_countersign_node 同尺）解析期报错、逐字文案、
零写入；回归：0／"Major"／缺省／未知值 "Aidant" 仍解析成功且建单回落主办 taskType=0
（issues/245 口径不变，不扩成 fail-fast）；custom 记录节点历史行形状不变（缺省 0）。

夹具内联 JSON，不碰共享 flows/ 目录（同 test_dynamic_countersign.py 纪律）。
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository                          # noqa: E402
from jeeflow.model import (MSG_TASK_TYPE_SECONDARY_FORBID, ProcessDefine,  # noqa: E402
                           SubmitType, TaskState, UserInfo, parse_flow_model)
from jeeflow.spi import IDGenerator                                       # noqa: E402

APPLY = int(SubmitType.APPLY)
MSG = "节点[review]不允许配置协办任务类型（taskType=1）"
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


def setup_engine():
    repo = MemoryRepository()
    return EngineImpl(repo, _UserProv(), _IDGen(), _Expr()), repo


# ─── 夹具 ────────────────────────────────────────────────────────────────────

def _node(nid: str, ntype: str, props: dict, text: str) -> dict:
    return {"id": nid, "type": ntype, "x": 0, "y": 0, "properties": props, "text": {"value": text}}


def _edge(eid: str, src: str, dst: str) -> dict:
    return {"id": eid, "type": "snaker:transition", "sourceNodeId": src, "targetNodeId": dst,
            "properties": {}}


def chain(review_props: dict) -> dict:
    """start → apply → review → final → end；review_props 控制 review 节点 taskType 等（空 dict＝缺省键）。"""
    global _seq
    _seq += 1
    r = {"form": "form-r", "assignee": "userA", "performType": 0}
    r.update(review_props or {})
    return {
        "name": f"tt252-{_seq}", "displayName": "协办禁配夹具", "type": "approval",
        "nodes": [
            _node("start", "snaker:start", {}, "开始"),
            _node("apply", "snaker:task",
                  {"form": "apply-form", "taskType": 0, "performType": 0, "assignee": "applicant"}, "发起申请"),
            _node("review", "snaker:task", r, "审批"),
            _node("final", "snaker:task",
                  {"form": "form-f", "assignee": "userF", "taskType": 0, "performType": 0}, "终审"),
            _node("end", "snaker:end", {}, "结束"),
        ],
        "edges": [_edge("e1", "start", "apply"), _edge("e2", "apply", "review"),
                  _edge("e3", "review", "final"), _edge("e4", "final", "end")],
    }


def custom_chain() -> dict:
    """start → apply → c1(custom，带 taskType:2 属性) → end：记录类节点不读 taskType，历史行形状不变。"""
    global _seq
    _seq += 1
    return {
        "name": f"tt252-custom-{_seq}", "displayName": "custom 回归夹具", "type": "approval",
        "nodes": [
            _node("start", "snaker:start", {}, "开始"),
            _node("apply", "snaker:task",
                  {"form": "apply-form", "taskType": 0, "performType": 0, "assignee": "applicant"}, "发起申请"),
            _node("c1", "snaker:custom", {"taskType": 2}, "记录"),
            _node("end", "snaker:end", {}, "结束"),
        ],
        "edges": [_edge("e1", "start", "apply"), _edge("e2", "apply", "c1"), _edge("e3", "c1", "end")],
    }


def register(repo, flow: dict) -> ProcessDefine:
    d = ProcessDefine(name=flow["name"], displayName=flow.get("displayName", ""),
                      type=flow.get("type", ""), state=1, version=1,
                      content=json.dumps(flow, ensure_ascii=False))
    repo.add_define(d)
    return d


async def start_to_review(eng, repo, flow: dict):
    """部署夹具并走发起＋办结申请节点，返回停在 review 的实例。"""
    d = register(repo, flow)
    inst = await eng.start_process_instance_by_id(d.id, "applicant", {})
    for t in await repo.find_doing_tasks(inst.id):
        await eng.execute_process_task(t.id, "applicant", {"submitType": APPLY})
    return await repo.find_instance_by_id(inst.id)


# ─── 正向 ────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw", [1, "1", "协办", "SECONDARY", "secondary"])
def test01_secondary_forms_rejected_at_parse(raw):
    """格 1：折成协办的全部输入形 ⇒ 解析期报错，逐字文案。"""
    with pytest.raises(ValueError) as ei:
        parse_flow_model(chain({"taskType": raw}))
    assert str(ei.value) == MSG, str(ei.value)


async def test02_start_fails_with_zero_writes():
    """格 2：发起腿报错且零写入——实例不得进仓（解析先于聚合根创建）。"""
    eng, repo = setup_engine()
    d = register(repo, chain({"taskType": 1}))
    with pytest.raises(ValueError) as ei:
        await eng.start_process_instance_by_id(d.id, "applicant", {})
    assert MSG in str(ei.value), str(ei.value)
    assert not await repo.has_instances_by_define_id(d.id), "解析报错必须零写入"


# ─── 回归 ────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw", [0, "0", "Major", "MAJOR", "主办", "Aidant", None])
async def test03_major_and_unknown_forms_fallback_unchanged(raw):
    """格 3：0／Major／缺省／未知值 Aidant 仍解析成功且建单回落主办（issues/245 口径）。"""
    props = {} if raw is None else {"taskType": raw}
    flow = chain(props)
    parse_flow_model(flow)  # 解析层：不得报错
    eng, repo = setup_engine()
    inst = await start_to_review(eng, repo, flow)
    review = [t for t in await repo.find_doing_tasks(inst.id) if t.taskName == "review"]
    assert len(review) == 1, f"taskType={raw!r} 应停在 review"
    assert review[0].taskType == 0, "未知值维持静默回落主办（issues/245 口径）"


async def test04_custom_history_row_unchanged():
    """格 4：custom 记录节点历史行不变——taskType:2 属性不触发禁配，DONE 行照落（本栈形状缺省 0）。"""
    flow = custom_chain()
    parse_flow_model(flow)
    eng, repo = setup_engine()
    d = register(repo, flow)
    inst = await eng.start_process_instance_by_id(d.id, "applicant", {})
    for t in await repo.find_doing_tasks(inst.id):
        await eng.execute_process_task(t.id, "applicant", {"submitType": APPLY})
    rows = [t for t in await repo.find_history_tasks(inst.id) if t.taskName == "c1"]
    assert len(rows) == 1, "custom 记录行必须照落"
    assert rows[0].taskState == TaskState.DONE
    assert rows[0].taskType == 0, "本栈历史行 taskType 形状不变（缺省 0）"
    assert not await repo.find_doing_tasks(inst.id), "令牌应续流到 end"


def test05_pinned_message():
    """格 5：消息常量逐字自证（防常量被改后测试仍绿）。"""
    assert MSG_TASK_TYPE_SECONDARY_FORBID == "节点[{0}]不允许配置协办任务类型（taskType=1）"