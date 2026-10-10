"""issues/163 · python 并行会签「放行表达式真求值」——对齐 java（CountersignHandler）＋ rust gate_vars。

改前形状：`countersignCompletionCondition` 只被拿去比 `ONE_VOTE_VETO` 一档，非否决值**从不交
求值器** ⇒ `#nrOfCompletedInstances>=2` 被完全忽略，放行只由"是否还有 DOING"决定（三人会签要
三人全办完才推进，与配置阈值语义相反）。本案（owner 2026-10-10 拍"修"）＝cond 交给同一条求值
通道（`expr_eval_or_default()`），门控计数（total/activate/completed，**裸名**）并进求值原料，
照 rust `gate_vars` 那一步；`ONE_VOTE_VETO` 判定顺序保持原样。

判据档：
① 三人并行 + `#nrOfCompletedInstances>=2`：第 1 票后**仍停留**（state=10、还剩 2 条待办）；
   第 2 票后**立即放行**（state=20、userC 待办被废弃）——改前红在"第 2 票后仍停留"。
② 对照档（守过度合并）：三人并行**不带 cond**：两票后仍停留，第三票才放行。
③ `ONE_VOTE_VETO` 原样：userA 以 submitType=20 办理 ⇒ 立即合并（B/C 待办废弃）。
④ 嵌套读形（附带对齐）：cond 写在设计器实际落笔处 `properties.field` 子对象里（共享夹具 07
   就是这个形状），引擎"顶层优先、嵌套兜底"读取 ⇒ 与 ① 同判（第 2 票放行）。

夹具＝**内联**三节点流程（start → apply → 三人并行会签 task1 → end，与活栈探针 probe_live2.py
的 CS_FLOW 同形）；不新增共享 flows/ 夹具（编辑源在 jeeflow-java，本轮不动跨仓夹具面）。
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
        self.n = 600000

    def next_id(self) -> int:
        self.n += 1
        return self.n


def _cs_flow(name, cond, nested=False):
    props = {"width": 120, "height": 60, "form": "cs-form",
             "assignee": "userA,userB,userC", "taskType": 0, "performType": 1,
             "countersignType": "PARALLEL",
             "field": {"candidateUsers": "userA,userB,userC"}}
    if cond is not None:
        if nested:  # 设计器实际落笔处：cond 藏在 properties.field 子对象里
            props["field"]["countersignCompletionCondition"] = cond
        else:
            props["countersignCompletionCondition"] = cond
    return {
        "name": name,
        "displayName": "三人并行会签(163夹具)",
        "type": "approval",
        "nodes": [
            {"id": "start", "type": "snaker:start", "x": 100, "y": 200,
             "properties": {"width": 50, "height": 50}, "text": {"value": "开始"}},
            {"id": "apply", "type": "snaker:task", "x": 200, "y": 200,
             "properties": {"width": 100, "height": 50, "form": "apply-form",
                            "assignee": "applicant", "taskType": 0, "performType": 0},
             "text": {"value": "发起申请"}},
            {"id": "task1", "type": "snaker:task", "x": 350, "y": 200,
             "properties": props, "text": {"value": "会签审批"}},
            {"id": "end", "type": "snaker:end", "x": 600, "y": 200,
             "properties": {"width": 50, "height": 50}, "text": {"value": "结束"}},
        ],
        "edges": [
            {"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
            {"id": "e_apply_1", "sourceNodeId": "apply", "targetNodeId": "task1", "properties": {}},
            {"id": "e2", "sourceNodeId": "task1", "targetNodeId": "end", "properties": {}},
        ],
    }


def _facade_and_repo():
    repo = MemoryRepository()
    eng = EngineImpl(repo, None, _Gen(), None)   # 无宿主 SPI ⇒ 内置默认件（`#var` 裸名查得动）
    return JeeflowFacade(eng, repo, MemoryExtRepository()), repo


async def _start_cs(facade, repo, cond=None, name="i163-cs", nested=False):
    r = await facade.flow("processDefine/deploy",
                          {"content": json.dumps(_cs_flow(name, cond, nested=nested), ensure_ascii=False)})
    assert r["code"] == 0, r
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": r["data"]["processDefineId"], "operator": "user1"})
    assert r["code"] == 0, r
    iid = int(r["data"]["processInstanceId"])
    doing = await repo.find_doing_tasks(iid)
    assert len(doing) == 3, f"三人并行会签应有 3 条成员待办: {[(t.taskName, t.actorIds) for t in doing]}"
    return iid


async def _vote(facade, repo, iid, who, submit_type=1):
    doing = await repo.find_doing_tasks(iid)
    task = next((t for t in doing if who in (t.actorIds or [])), None)
    assert task is not None, f"{who} 应有待办可办: {[(t.id, t.actorIds) for t in doing]}"
    r = await facade.flow("processTask/execute",
                          {"processTaskId": task.id, "operator": who, "submitType": submit_type})
    return r, task


@pytest.mark.asyncio
async def test_i163_threshold_merges_at_two_of_three():
    """① 核心格：`#nrOfCompletedInstances>=2` ⇒ 第 2 票立即放行，第 3 位待办废弃。"""
    facade, repo = _facade_and_repo()
    iid = await _start_cs(facade, repo, "#nrOfCompletedInstances>=2")

    r1, _ = await _vote(facade, repo, iid, "userA")
    assert r1["code"] == 0, r1
    inst = await repo.find_instance_by_id(iid)
    assert int(inst.state) == 10, f"第 1 票后阈值未达，仍应停留: state={inst.state}"
    assert len(await repo.find_doing_tasks(iid)) == 2, "第 1 票后应还剩 2 条待办"

    r2, _ = await _vote(facade, repo, iid, "userB")
    assert r2["code"] == 0, r2
    inst = await repo.find_instance_by_id(iid)
    assert int(inst.state) == 20, \
        f"第 2 票达到 #nrOfCompletedInstances>=2，应放行办结（改前此格红：仍停留）: state={inst.state}"
    doing = await repo.find_doing_tasks(iid)
    assert not doing, f"放行后剩余成员待办应废弃: {[t.actorIds for t in doing]}"
    userC = next(t for t in inst.tasks if t.taskName == "task1" and "userC" in (t.actorIds or []))
    assert int(userC.taskState) == 99, f"userC 的成员行应被废弃（ABANDONED=99）: {userC.taskState}"


@pytest.mark.asyncio
async def test_i163_no_cond_keeps_all_finished_rule():
    """② 对照档：不带 cond ⇒ 两票仍停留、第三票才放行（"全部完成"语义不许被本案改掉）。"""
    facade, repo = _facade_and_repo()
    iid = await _start_cs(facade, repo, None, name="i163-cs-nocond")

    await _vote(facade, repo, iid, "userA")
    await _vote(facade, repo, iid, "userB")
    inst = await repo.find_instance_by_id(iid)
    assert int(inst.state) == 10, f"无 cond 时两票后必须仍停留: state={inst.state}"
    assert len(await repo.find_doing_tasks(iid)) == 1, "无 cond 时两票后应还剩 1 条"

    await _vote(facade, repo, iid, "userC")
    inst = await repo.find_instance_by_id(iid)
    assert int(inst.state) == 20, f"第三票（全部完成）才放行: state={inst.state}"


@pytest.mark.asyncio
async def test_i163_one_vote_veto_order_unchanged():
    """③ ONE_VOTE_VETO 原样：任一成员 submitType=20 ⇒ 立即合并、其余待办废弃。"""
    facade, repo = _facade_and_repo()
    iid = await _start_cs(facade, repo, "ONE_VOTE_VETO", name="i163-cs-veto")

    r, _ = await _vote(facade, repo, iid, "userA", submit_type=20)
    assert r["code"] == 0, r
    inst = await repo.find_instance_by_id(iid)
    assert int(inst.state) == 20, f"一票否决应立即合并: state={inst.state}"
    doing = await repo.find_doing_tasks(iid)
    assert not doing, f"否决后 B/C 待办应废弃: {[t.actorIds for t in doing]}"


@pytest.mark.asyncio
async def test_i163_nested_field_read_form():
    """④ 附带读形对齐：cond 藏在 properties.field 子对象（设计器实际落笔处）⇒ 同 ① 判。"""
    facade, repo = _facade_and_repo()
    iid = await _start_cs(facade, repo, "#nrOfCompletedInstances>=2",
                          name="i163-cs-nested", nested=True)

    await _vote(facade, repo, iid, "userA")
    inst = await repo.find_instance_by_id(iid)
    assert int(inst.state) == 10, f"第 1 票后应停留: state={inst.state}"

    await _vote(facade, repo, iid, "userB")
    inst = await repo.find_instance_by_id(iid)
    assert int(inst.state) == 20, \
        f"嵌套形 cond 也应被读到并在第 2 票放行（改前整条 cond 被忽略）: state={inst.state}"
    doing = await repo.find_doing_tasks(iid)
    assert not doing, f"放行后剩余成员待办应废弃: {[t.actorIds for t in doing]}"
