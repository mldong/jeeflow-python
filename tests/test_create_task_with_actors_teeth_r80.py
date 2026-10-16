"""audit8 R80 python 半边：`_create_task_with_actors` 四条建单腿（登记腿＋落库腿）的有牙判据。

案面出处＝`notes/R92.md` 末节与交接卡 §1 那条"⚠ 等价变异残留"：R92 的 11 档判据走的是公开执行族，
而 `_create_task_with_actors` 自 issues/121 P2 起**零生产调用者**（只被 spec_test 的 i137b 两枚直调件喂），
所以把那四条 `_append_new_task` 登记腿（本轮现读 `engine.py:758/765/772/780`，R92 那轮记的
`756/763/770/778` 已漂两行）逐条摘掉后 11 档**全绿＝等价**。
本文件补的就是这一格，判据形状与 go 段一同构（`jeeflow-go/engine/r80_savenewtask_teeth_test.go`）：

1. **前置读数**——不注入时该支建的行数＝预期写次数，且**执行族产出列表逐行收到**（登记腿由此有牙）；
2. **原样透出**——本栈写失败靠异常自然上抛（函数里没有判错分支），⇒ 用 `pytest.raises` 认**那一枚异常本身**，
   换成别的错也算没牙；
3. **抛错即停**——仓储调用次数停在第 k 次、产出列表只登记前 k-1 行（专治"吞掉错误继续跑完"）。

四档＝函数里的四个分支：PARALLEL 逐人（两行）／SEQUENTIAL 首位（一行）／未知 countersignType 兜底
（合成档，仓库 16 份定义喂不到 default 支）／普通建单（多参与者也只一行）。
夹具内联（不取共享 flows/）：本轮要钉的是**写次与登记次读数**，共享夹具的参与者数被别轮改动会带走判据。
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository
from jeeflow.model import ProcessDefine, parse_flow_model
from jeeflow.spi import ExpressionEvaluator, IDGenerator, UserProvider

from jeeflow.engine import _NEW_TASKS


class _UserProv(UserProvider):
    async def get_user(self, user_id: str):
        return None


class _IDGen(IDGenerator):
    def __init__(self):
        self.n = 0

    def next_id(self) -> int:
        self.n += 1
        return self.n


class _EvalFalse(ExpressionEvaluator):
    async def eval(self, expr: str, vars: dict):
        return False


class _R80SaveError(RuntimeError):
    """注入用：仓储写失败的**唯一**指纹（判据认的就是这一枚，不是"任何异常"）。"""


class _R80Repo:
    """包一层内存仓：只挡第 fail_on 次 `save_task`（0＝不挡），其余属性原样透传。"""

    def __init__(self, inner, fail_on=0):
        self._inner = inner
        self.fail_on = fail_on
        self.calls = 0

    async def save_task(self, task):
        self.calls += 1
        if self.fail_on and self.calls == self.fail_on:
            raise _R80SaveError("r80: save_task 注入失败")
        return await self._inner.save_task(task)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def _flow_json(ct: str, assignee: str, name: str) -> str:
    props = ({"assignee": assignee, "taskType": 0, "performType": "1", "countersignType": ct}
             if ct else {"assignee": assignee, "taskType": 0, "performType": "0"})
    return json.dumps({
        "name": name, "displayName": "R80 建单腿判据", "type": "approval",
        "nodes": [
            {"id": "start", "type": "snaker:start", "properties": {}, "text": {"value": "开始"}},
            {"id": "t1", "type": "snaker:task", "properties": props, "text": {"value": "建单"}},
            {"id": "end", "type": "snaker:end", "properties": {}, "text": {"value": "结束"}},
        ],
        "edges": [
            {"id": "e0", "sourceNodeId": "start", "targetNodeId": "t1", "properties": {}},
            {"id": "e1", "sourceNodeId": "t1", "targetNodeId": "end", "properties": {}},
        ],
    }, ensure_ascii=False)


async def _harness(ct: str, assignee: str, name: str, fail_on: int = 0):
    """返回 (engine, 装饰仓, 定义号, t1 节点对象)——节点对象与主路径拿到的是同一形状。"""
    inner = MemoryRepository()
    repo = _R80Repo(inner, fail_on)
    eng = EngineImpl(repo, _UserProv(), _IDGen(), _EvalFalse())
    content = _flow_json(ct, assignee, name)
    def_ = ProcessDefine(name=name, displayName=name, type="test", state=1, content=content)
    repo.add_define(def_)
    node = next(n for n in parse_flow_model(json.loads(content)).nodes if n.id == "t1")
    return eng, repo, def_, node


async def _direct(eng, node, inst, actors, pname):
    """直调建单函数，并把本次执行族的产出槽接出来（公开方法入口才自动 set，直调需自备）。"""
    created: list = []
    token = _NEW_TASKS.set(created)
    before = {t.id for t in await eng.repo.find_doing_tasks(inst.id)}
    try:
        await eng._create_task_with_actors(node, inst, "boss1", {}, list(actors),
                                           process_name=pname, parent_id=0, is_first=True)
    finally:
        _NEW_TASKS.reset(token)
    new_rows = [t for t in await eng.repo.find_doing_tasks(inst.id) if t.id not in before]
    return created, new_rows


# 四档：分支 → (countersignType, 参与者, 该支正常走完应写几行)
# 行号＝本轮现读（R92 那轮记的 756/763/770/778 已随后续改动漂了两行）；括号里给的是**登记腿**行号
_BRANCHES = [
    ("PARALLEL 逐人支（登记腿 :758／写腿 :757，两行两次写）", "PARALLEL", ["a1", "a2"], 2),
    ("SEQUENTIAL 首位支（:765／:764，只建首位一行）", "SEQUENTIAL", ["a1", "a2", "a3"], 1),
    ("未知 countersignType 兜底支（:772／:771，合成档）", "BOGUS-TYPE", ["a1", "a2"], 2),
    ("普通建单支（:780／:779，多参与者也只承载一行）", "", ["a1", "a2"], 1),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("tag,ct,actors,writes", _BRANCHES, ids=[b[0] for b in _BRANCHES])
async def test_r80四支建单腿落库并逐行登记(tag, ct, actors, writes):
    """对照组：该支写满 expected 行，且**每一行都进了执行族产出列表**（登记腿有牙的那一条）。"""
    pname = "r80-%s" % (ct or "plain")
    eng, repo, def_, node = await _harness(ct, ",".join(actors), pname)
    inst = await eng.start_process_instance_by_id(def_.id, "boss1")
    repo.calls = 0  # 发起腿的写次不计入本判据（本文件只管直调这四支）

    created, new_rows = await _direct(eng, node, inst, actors, pname)

    assert repo.calls == writes, "%s：前置读数 save_task 应 %d 次，实读 %d（该支形状变了，判据要跟着改）" % (
        tag, writes, repo.calls)
    assert len(new_rows) == writes, "%s：应建满 %d 行，实读 %d 行" % (tag, writes, len(new_rows))
    assert len(created) == writes, (
        "%s：执行族产出列表应收到 %d 行，实读 %d 行 ⇒ 该支的 `_append_new_task` 登记腿没被执行"
        % (tag, writes, len(created)))
    assert sorted(str(t.id) for t in created) == sorted(str(t.id) for t in new_rows), (
        "%s：产出列表与仓储里新建的行必须一一对应" % tag)


@pytest.mark.asyncio
@pytest.mark.parametrize("tag,ct,actors,writes", _BRANCHES, ids=[b[0] for b in _BRANCHES])
async def test_r80四支写失败原样上抛且立即停(tag, ct, actors, writes):
    """注入档：逐次挡在第 k 个写点 ⇒ 必须把那枚异常原样抛出，且停在第 k 行之前。"""
    for k in range(1, writes + 1):
        pname = "r80-fail-%s-%d" % (ct or "plain", k)
        eng, repo, def_, node = await _harness(ct, ",".join(actors), pname)
        inst = await eng.start_process_instance_by_id(def_.id, "boss1")
        # 注入臂在发起腿走完之后才合闸：发起腿也走同一枚 save_task，先合闸打中的是它（本轮判据只管四支直调）
        repo.calls = 0
        repo.fail_on = k

        created: list = []
        token = _NEW_TASKS.set(created)
        before = {t.id for t in await eng.repo.find_doing_tasks(inst.id)}
        try:
            with pytest.raises(_R80SaveError):  # 认这一枚异常本身（换成别的错＝另一种没牙）
                await eng._create_task_with_actors(node, inst, "boss1", {}, list(actors),
                                                   process_name=pname, parent_id=0, is_first=True)
        finally:
            _NEW_TASKS.reset(token)
        new_rows = [t for t in await eng.repo.find_doing_tasks(inst.id) if t.id not in before]

        assert repo.calls == k, "%s：第 %d 次注入后不得再写（实读 %d 次）" % (tag, k, repo.calls)
        assert len(new_rows) == k - 1, "%s：第 %d 次注入后应停在 %d 行，实读 %d 行" % (
            tag, k, k - 1, len(new_rows))
        assert len(created) == k - 1, (
            "%s：失败那行不得进产出列表（登记发生在写成功之后），实读 %d 行" % (tag, len(created)))
