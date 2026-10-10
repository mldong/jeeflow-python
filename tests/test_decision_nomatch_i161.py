"""issues/161 · python 决策节点「零命中 ⇒ 显式报错（NOT_FOUND_NEXT_NODE 同档）」——对齐 java。

改前形状（HEAD `bca2da9`）：两条回退臂之后还有一条"无条件取 edges[0]"的蒙臂——同一份坏定义
（两 expr 全 false / 变量缺失）python 蒙第一支走、java 显式报错，属跨栈行为分叉。本案 A
（owner 2026-10-10 拍）＝删蒙臂对齐 java：无任何命中（含零出边）抛
``decision节点无法确定下一步执行路线``——文案逐字取 java ``WfErrEnum.NOT_FOUND_NEXT_NODE``
（`DecisionModel.exec` 尾部 `!found` 那支）。

判据档：
① amount=500（对 `amount>1000`／`amount<0` 两 expr 全 false）⇒ 门面出口 code=99999999 且 msg
   与 java 逐字同文；实例停在 DOING、零后续待办。活栈 java 对照读数同形状（探针 v2：
   任务已办结、实例 state=10、manager/director 无待办）。
② 对照档 amount=5000 ⇒ e3 为真照常走 task2——证明抛错只在"零命中"档，没把正常分支打断。
③ 缺 amount（两 expr 同样全 false）⇒ 同档报错（"变量没填"是现实里最主要的坏定义形状）。

夹具＝**内联**三节点流程（start → apply → task1(leader) → decision 两 expr 边 → task2/task3 → end，
与活栈探针 `probe_live2.py` 的 DEC_FLOW 逐字同形）；不新增共享 flows/ 夹具——共享夹具编辑源在
jeeflow-java，本轮不动跨仓夹具面。
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow.engine import EngineImpl                                   # noqa: E402
from jeeflow.facade import JeeflowFacade                                # noqa: E402
from jeeflow.memory import MemoryExtRepository, MemoryRepository        # noqa: E402
from jeeflow.spi import IDGenerator                                     # noqa: E402

NOT_FOUND_NEXT_NODE_MSG = "decision节点无法确定下一步执行路线"

DEC_FLOW = {
    "name": "i161-decision-nomatch",
    "displayName": "决策无命中(161夹具)",
    "type": "approval",
    "nodes": [
        {"id": "start", "type": "snaker:start", "x": 100, "y": 200,
         "properties": {"width": 50, "height": 50}, "text": {"value": "开始"}},
        {"id": "apply", "type": "snaker:task", "x": 200, "y": 200,
         "properties": {"width": 100, "height": 50, "form": "apply-form",
                        "assignee": "applicant", "taskType": 0, "performType": 0},
         "text": {"value": "发起申请"}},
        {"id": "task1", "type": "snaker:task", "x": 300, "y": 200,
         "properties": {"width": 100, "height": 50, "form": "expense-form",
                        "assignee": "leader", "taskType": 0, "performType": 0},
         "text": {"value": "填写报销单"}},
        {"id": "decision1", "type": "snaker:decision", "x": 500, "y": 200,
         "properties": {"width": 80, "height": 50, "expr": "", "handleClass": ""},
         "text": {"value": "金额判断"}},
        {"id": "task2", "type": "snaker:task", "x": 650, "y": 100,
         "properties": {"width": 100, "height": 50, "form": "expense-form",
                        "assignee": "manager", "taskType": 0, "performType": 0},
         "text": {"value": "经理审批"}},
        {"id": "task3", "type": "snaker:task", "x": 650, "y": 300,
         "properties": {"width": 100, "height": 50, "form": "expense-form",
                        "assignee": "director", "taskType": 0, "performType": 0},
         "text": {"value": "总监审批"}},
        {"id": "end", "type": "snaker:end", "x": 850, "y": 200,
         "properties": {"width": 50, "height": 50}, "text": {"value": "结束"}},
    ],
    "edges": [
        {"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
        {"id": "e_apply_1", "sourceNodeId": "apply", "targetNodeId": "task1", "properties": {}},
        {"id": "e2", "sourceNodeId": "task1", "targetNodeId": "decision1", "properties": {}},
        {"id": "e3", "sourceNodeId": "decision1", "targetNodeId": "task2",
         "properties": {"expr": "amount > 1000"}, "text": {"value": "金额>1000"}},
        {"id": "e4", "sourceNodeId": "decision1", "targetNodeId": "task3",
         "properties": {"expr": "amount < 0"}, "text": {"value": "金额<0"}},
        {"id": "e5", "sourceNodeId": "task2", "targetNodeId": "end", "properties": {}},
        {"id": "e6", "sourceNodeId": "task3", "targetNodeId": "end", "properties": {}},
    ],
}


class _Gen(IDGenerator):
    def __init__(self):
        self.n = 500000

    def next_id(self) -> int:
        self.n += 1
        return self.n


def _facade_and_repo():
    repo = MemoryRepository()
    eng = EngineImpl(repo, None, _Gen(), None)   # 无宿主 SPI ⇒ 走内置默认求值器（1.8.41 起）
    return JeeflowFacade(eng, repo, MemoryExtRepository()), repo


async def _start_leader_form(facade, repo, amount=None):
    """发起并让 leader 办掉 task1，停在 decision1 前——返回 (iid, leader 执行响应)。"""
    import json
    r = await facade.flow("processDefine/deploy",
                          {"content": json.dumps(DEC_FLOW, ensure_ascii=False)})
    assert r["code"] == 0, r
    payload = {"processDefineId": r["data"]["processDefineId"], "operator": "user1"}
    if amount is not None:
        payload["amount"] = amount
    r = await facade.flow("processInstance/startAndExecute", payload)
    assert r["code"] == 0, r
    iid = int(r["data"]["processInstanceId"])
    doing = await repo.find_doing_tasks(iid)
    task1 = next((t for t in doing if t.taskName == "task1"), None)
    assert task1 is not None, f"发起后 task1 应进行中，实得 {[t.taskName for t in doing]}"
    r = await facade.flow("processTask/execute",
                          {"processTaskId": task1.id, "operator": "leader", "submitType": 1})
    return iid, r


@pytest.mark.asyncio
@pytest.mark.parametrize("amount,label", [(500, "amount=500"), (None, "缺 amount")])
async def test_i161_no_match_raises(amount, label):
    """①③ 两 expr 全 false ⇒ 门面出口 99999999＋WfErrEnum 逐字文案；实例停在 DOING、零待办。"""
    facade, repo = _facade_and_repo()
    iid, r = await _start_leader_form(facade, repo, amount)

    assert r["code"] == 99999999, f"零命中必须显式失败（{label}）: {r}"
    assert r["msg"] == NOT_FOUND_NEXT_NODE_MSG, \
        f"文案应逐字取 java WfErrEnum.NOT_FOUND_NEXT_NODE: {r['msg']!r}"

    inst = await repo.find_instance_by_id(iid)
    assert int(inst.state) == 10, f"实例应停在 DOING（java 活栈读数同形状）: {inst.state}"
    doing = await repo.find_doing_tasks(iid)
    assert not doing, f"零命中后不得有后续待办（manager/director 都不该收到）: {[t.taskName for t in doing]}"
    # 任务本身已办结（对齐 java 活栈读数：报错发生在任务行 FINISHED 落库之后）
    task1 = next(t for t in inst.tasks if t.taskName == "task1")
    assert int(task1.taskState) == 20, f"leader 的任务应已办结（java 同形状）: {task1.taskState}"


@pytest.mark.asyncio
async def test_i161_truthy_edge_still_flows():
    """② 对照档：amount=5000 ⇒ e3(amount>1000) 为真，照常走 task2——抛错只在零命中档。"""
    facade, repo = _facade_and_repo()
    iid, r = await _start_leader_form(facade, repo, 5000)

    assert r["code"] == 0, f"有命中分支不得报错: {r}"
    doing = await repo.find_doing_tasks(iid)
    assert [t.taskName for t in doing] == ["task2"], \
        f"amount=5000 应走 e3 落在 task2: {[t.taskName for t in doing]}"
    assert int((await repo.find_instance_by_id(iid)).state) == 10, "实例应停在 task2 上（DOING）"
