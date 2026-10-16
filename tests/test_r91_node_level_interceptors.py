"""audit8 R91（issues/234 §4.2／spec02 §3）：节点级 pre/postInterceptors 优先序、回落与键形。

基准＝java ``NodeModel.java:49-59``（节点串空才取 ``getProcessModel().getPre/PostInterceptors()``，
两档不叠加），条文＝spec02 §3「节点级拦截器的优先序、回落与键形」。
本栈修前形状＝只读流程根 ``postInterceptors`` 一个键、节点 properties 从不被问、
根级 ``preInterceptors`` 声明层零命中、缓存按 defineId 单键。

反空转纪律：每档先证明"拦截器真被调到"（calls 非空且看到目标节点），再断形状。
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository
from jeeflow.engine import KEY_SUBMIT_TYPE
from jeeflow.extensions import EngineExtensions, FlowInterceptor
from jeeflow.model import ProcessDefine, UserInfo
from jeeflow.spi import ExpressionEvaluator, IDGenerator, UserProvider


class _Recorder(FlowInterceptor):
    def __init__(self, tag, sink, order=0):
        self.tag, self.sink, self._order = tag, sink, order

    @property
    def order(self): return self._order

    async def pre_handle(self, node, instance) -> bool:
        self.sink.append({"phase": f"pre:{self.tag}", "node": node.id})
        return True

    async def post_handle(self, node, instance) -> None:
        self.sink.append({"phase": f"post:{self.tag}", "node": node.id})


class _UserProv(UserProvider):
    async def get_user(self, user_id: str):
        return UserInfo(userId=user_id, realName="用户" + user_id, deptId="D01")


class _IDGen(IDGenerator):
    def __init__(self): self.n = 0
    def next_id(self) -> int:
        self.n += 1
        return self.n


class _EvalFalse(ExpressionEvaluator):
    async def eval(self, expr: str, vars: dict):
        return False


def mk(registry):
    repo = MemoryRepository()
    eng = EngineImpl(repo, _UserProv(), _IDGen(), _EvalFalse())
    eng.set_extensions(EngineExtensions(interceptors=[], interceptor_registry=registry))
    return eng, repo


def define_with(name, root_post="", root_pre="", node_post="", node_pre=""):
    nodes = [
        {"id": "start", "type": "snaker:start", "properties": {}, "text": {"value": "开始"}},
        {"id": "apply", "type": "snaker:task",
         "properties": {"assignee": "user1",
                        **({"postInterceptors": node_post} if node_post else {}),
                        **({"preInterceptors": node_pre} if node_pre else {})},
         "text": {"value": "申请"}},
        {"id": "finish", "type": "snaker:end", "properties": {}, "text": {"value": "结束"}},
    ]
    content = json.dumps({
        "name": name, "displayName": name, "type": "approval",
        "relTableName": "biz_r91", "persistMode": "SYNC",
        "postInterceptors": root_post, "preInterceptors": root_pre,
        "nodes": nodes,
        "edges": [{"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
                  {"id": "e1", "sourceNodeId": "apply", "targetNodeId": "finish", "properties": {}}],
    }, ensure_ascii=False)
    d = ProcessDefine(name=name, displayName=name, type="approval", state=1, version=1, content=content)
    return d, nodes


def patch_node(d, node_id, key, value):
    """把某节点的 properties 补一条拦截器声明（夹具要挂到"真被推进的节点"上才不空转）。"""
    raw = json.loads(d.content)
    for n in raw["nodes"]:
        if n["id"] == node_id:
            n.setdefault("properties", {})[key] = value
    d.content = json.dumps(raw, ensure_ascii=False)
    return d


async def run_flow(eng, repo, d, operator="user1"):
    inst = await eng.start_process_instance_by_id(d.id, operator, {"f_title": "R91 单"})
    doing = await repo.find_doing_tasks(inst.id)
    assert doing, "前置：应有一条进行中任务（否则判据全部空转）"
    await repo.add_task_actor(doing[0].id, [operator])
    await eng.execute_process_task(doing[0].id, operator, {KEY_SUBMIT_TYPE: 1})


@pytest.mark.asyncio
async def test_r91_01_node_declaration_wins_no_stacking():
    """① 节点声明优先：节点与流程根同时声明 ⇒ 只跑节点声明那一条（两档不叠加）"""
    calls = []
    eng, repo = mk({"node": _Recorder("node", calls), "root": _Recorder("root", calls)})
    d, _ = define_with("r91-priority", root_post="root", node_post="node")
    repo.add_define(d)
    await run_flow(eng, repo, d)
    assert any(c["phase"] == "post:node" and c["node"] == "apply" for c in calls), \
        f"前置失效：节点级拦截器没被调到（calls={calls}）"
    assert not any(c["phase"] == "post:root" and c["node"] == "apply" for c in calls), \
        f"apply 上节点声明与根声明叠加了：{calls}"


@pytest.mark.asyncio
async def test_r91_02_falls_back_to_process_root():
    """② 缺省回落流程根：节点不声明 ⇒ 走流程根那一档（回归既有形状）"""
    calls = []
    eng, repo = mk({"root": _Recorder("root", calls)})
    d, _ = define_with("r91-fallback-root", root_post="root")
    repo.add_define(d)
    await run_flow(eng, repo, d)
    assert any(c["phase"] == "post:root" and c["node"] == "apply" for c in calls), \
        f"节点不声明时没回落到流程根那一档：{calls}"


@pytest.mark.asyncio
async def test_r91_03_root_pre_interceptors_are_read():
    """③ 根级 preInterceptors 也要读（修前零命中＝本栈旧形状只读根级 post）"""
    calls = []
    eng, repo = mk({"rootpre": _Recorder("rootpre", calls)})
    d, _ = define_with("r91-root-pre", root_pre="rootpre")
    repo.add_define(d)
    await run_flow(eng, repo, d)
    # 本栈 pre 相位只在被推进的节点上触发（此处为 finish）⇒ 只断"根级 pre 被解析并执行"
    assert any(c["phase"] == "pre:rootpre" for c in calls), \
        f"根级 preInterceptors 仍不被解析（修前即此形状）：{calls}"


@pytest.mark.asyncio
async def test_r91_04_node_pre_uses_same_priority():
    """④ 节点级 pre 也走同一优先序（节点声明 pre ⇒ 只跑它）"""
    calls = []
    eng, repo = mk({"nodepre": _Recorder("nodepre", calls), "rootpre": _Recorder("rootpre", calls)})
    d, _ = define_with("r91-node-pre", root_pre="rootpre")
    patch_node(d, "finish", "preInterceptors", "nodepre")
    repo.add_define(d)
    await run_flow(eng, repo, d)
    assert any(c["phase"] == "pre:nodepre" and c["node"] == "finish" for c in calls), \
        f"节点级 pre 没触发：{calls}"
    assert not any(c["phase"] == "pre:rootpre" and c["node"] == "finish" for c in calls), \
        f"finish 上节点 pre 与根 pre 叠加了：{calls}"


@pytest.mark.asyncio
async def test_r91_05_unregistered_declaration_raises():
    """⑤ 声明未注册 ⇒ 显式报错（节点档与根档同纪律，不静默忽略）"""
    eng, repo = mk({})
    d, _ = define_with("r91-ghost", node_post="com.mldong.jeeflow.ghost.GhostInterceptor")
    repo.add_define(d)
    with pytest.raises(ValueError, match="未注册"):
        await run_flow(eng, repo, d)


@pytest.mark.asyncio
async def test_r91_06_cache_key_includes_node_and_phase():
    """⑥ 缓存键含节点与档位：两节点声明不同拦截器 ⇒ 各自独立命中（防按 defineId 单键串档）"""
    calls = []
    eng, repo = mk({"a": _Recorder("A", calls), "b": _Recorder("B", calls)})
    d, _ = define_with("r91-cache", node_post="a")
    raw = json.loads(d.content)
    raw["nodes"].append({"id": "apply2", "type": "snaker:task",
                         "properties": {"assignee": "user1", "postInterceptors": "b"},
                         "text": {"value": "二级"}})
    raw["edges"] = [e for e in raw["edges"] if e["id"] != "e1"]
    raw["edges"].append({"id": "e2", "sourceNodeId": "apply", "targetNodeId": "apply2",
                         "edgeType": "approved", "properties": {}})
    raw["edges"].append({"id": "e3", "sourceNodeId": "apply2", "targetNodeId": "finish", "properties": {}})
    d.content = json.dumps(raw, ensure_ascii=False)
    repo.add_define(d)
    inst = await eng.start_process_instance_by_id(d.id, "user1", {"f_title": "两节点"})
    doing = await repo.find_doing_tasks(inst.id)
    await repo.add_task_actor(doing[0].id, ["user1"])
    await eng.execute_process_task(doing[0].id, "user1", {KEY_SUBMIT_TYPE: 1})
    doing = await repo.find_doing_tasks(inst.id)
    assert doing, "前置失效：第二节点没建出来（缓存档判据会空转）"
    await repo.add_task_actor(doing[0].id, ["user1"])
    await eng.execute_process_task(doing[0].id, "user1", {KEY_SUBMIT_TYPE: 1})
    assert any(c["phase"] == "post:A" and c["node"] == "apply" for c in calls), f"第一节点未走 a：{calls}"
    assert any(c["phase"] == "post:B" and c["node"] == "apply2" for c in calls), \
        f"第二节点未走 b（缓存按 defineId 串档）：{calls}"
