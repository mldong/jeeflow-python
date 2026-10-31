"""任务行 ext 的回退契约（issues/82-3 / 121 P1）× 直返保留键（spec 04）。

既有 spec_test.py::test_list_row_time_format_and_instance_ext 只断言 ext / instanceExt 是 dict，
回退有没有真发生它照不出——本轮 `_branchCtx` 写进每条任务变量后，八栈都是这么漏过去的
（只有 php 的 PDO 仓有内容级断言才报红）。两腿：

① 任务变量只剩引擎控制键 ⇒ 任务行 ext 回退实例变量、内部键不透出；
② 直返申请待办 ⇒ ext = 实例变量 + `_reapplyContext`（前端「重新提交后返回：X」的取数口），
   且合回不得污染同一响应里的 instanceExt。
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository                      # noqa: E402
from jeeflow.facade import JeeflowFacade                              # noqa: E402
from jeeflow.memory import MemoryExtRepository                        # noqa: E402
from jeeflow.model import UserInfo                                    # noqa: E402
from jeeflow.reapply import VAR_BRANCH_CTX, VAR_REAPPLY_CTX           # noqa: E402
from jeeflow.spi import ExpressionEvaluator, IDGenerator, UserProvider  # noqa: E402

import flows_resolver                                                 # noqa: E402

FLOW_DIR = flows_resolver.dir()


class _UserProv(UserProvider):
    async def get_user(self, user_id: str):
        return UserInfo(userId=user_id, realName=f"用户{user_id}", deptId="D01",
                        deptName="测试部门", postId="P01", postName="测试岗位")


class _IDGen(IDGenerator):
    def __init__(self):
        self.n = 0

    def next_id(self) -> int:
        self.n += 1
        return self.n


class _Expr(ExpressionEvaluator):
    async def eval(self, expr: str, vars: dict):
        amt = vars.get("amount")
        if amt is None:
            return False
        if expr == "amount > 1000":
            return float(amt) > 1000
        if expr == "amount <= 1000":
            return float(amt) <= 1000
        return False


def _setup():
    repo = MemoryRepository()
    return EngineImpl(repo, _UserProv(), _IDGen(), _Expr()), repo


async def _deploy_start(facade, fixture, operator):
    with open(os.path.join(FLOW_DIR, fixture), encoding="utf-8") as f:
        content = f.read()
    r = await facade.flow("processDefine/deploy", {"content": content})
    assert r["code"] == 0, r
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": r["data"]["processDefineId"], "operator": operator})
    assert r["code"] == 0, r
    return r


@pytest.mark.asyncio
async def test_task_row_ext_falls_back_with_reapply_reserved_keys():
    eng, repo = _setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())

    # ① 普通任务行：变量只剩 isFirstTaskNode + _branchCtx ⇒ ext 回退实例变量
    await _deploy_start(facade, "01-simple.json", "zhangsan")
    r = await facade.flow("processTask/todoList", {"operator": "leader"})
    assert r["code"] == 0, r
    assert r["data"]["rows"], r
    row = r["data"]["rows"][0]
    ext, inst_ext = row["ext"], row["instanceExt"]
    assert inst_ext, f"前置：instanceExt 必须非空，否则下面的回退断言恒真空 → {row}"
    assert VAR_BRANCH_CTX not in ext, f"内部键 {VAR_BRANCH_CTX} 不得从任务行 ext 透出（spec 04）：{ext}"
    assert VAR_BRANCH_CTX not in inst_ext, f"内部键 {VAR_BRANCH_CTX} 不得写进实例变量：{inst_ext}"
    assert ext == inst_ext, f"任务变量只剩控制键 ⇒ ext 应回退实例变量（82-3 / 121 P1）：{ext} vs {inst_ext}"

    # ①b 任务办结后：执行上下文不得把两枚保留键写回实例变量（go mergeExecIntoInstance 同款挡法）
    r = await facade.flow("processTask/execute",
                          {"processTaskId": row["id"], "operator": "leader", "submitType": 1})
    assert r["code"] == 0, r
    r = await facade.flow("processInstance/page", {"operator": "zhangsan"})
    assert r["code"] == 0 and r["data"]["rows"], r
    i_ext = r["data"]["rows"][0]["ext"]
    assert VAR_BRANCH_CTX not in i_ext, f"办结后内部键 {VAR_BRANCH_CTX} 不得进实例变量：{i_ext}"
    assert VAR_REAPPLY_CTX not in i_ext, f"办结后内部键 {VAR_REAPPLY_CTX} 不得进实例变量：{i_ext}"

    # ② 直返申请待办：ext = 实例变量 + _reapplyContext
    await _deploy_start(facade, "14-return-to-source.json", "applicant")
    r = await facade.flow("processTask/todoList", {"operator": "userA"})
    assert r["code"] == 0 and r["data"]["rows"], r
    task_a = r["data"]["rows"][0]
    r = await facade.flow("processTask/execute",
                          {"processTaskId": task_a["id"], "operator": "userA", "submitType": 6})
    assert r["code"] == 0, r

    r = await facade.flow("processTask/todoList", {"operator": "applicant", "pageSize": 100})
    assert r["code"] == 0, r
    marked = [x for x in r["data"]["rows"] if VAR_REAPPLY_CTX in (x.get("ext") or {})]
    assert marked, f"直返申请待办的 ext 应带 {VAR_REAPPLY_CTX}（前端提示的取数口）：{r['data']['rows']}"
    app = marked[0]
    a_ext, a_inst = app["ext"], app["instanceExt"]
    for k in a_inst:
        assert k in a_ext, f"回退仍要发生：实例变量键 {k} 应在 ext 上 → ext={a_ext} instExt={a_inst}"
    assert VAR_BRANCH_CTX not in a_ext, f"内部键 {VAR_BRANCH_CTX} 不得从 ext 透出：{a_ext}"
    assert a_ext[VAR_REAPPLY_CTX].get("sourceNode") == "taskA", a_ext[VAR_REAPPLY_CTX]
    assert VAR_REAPPLY_CTX not in a_inst, f"合回不得污染同一响应里的 instanceExt 出口：{a_inst}"
