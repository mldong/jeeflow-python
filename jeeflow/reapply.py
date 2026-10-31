"""「退回修改、重提直返」——分支归属与直返上下文（Python 栈）。

状态落点与 Java/Go 参考实现一致：零 DDL，全部寄存在任务变量保留键里
（spec 04-engine-ops「退回修改、重提直返」）。本模块只放纯函数与引擎侧的薄适配，
避免把细节散进 engine.py。
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import Any, Optional

from .model import (
    FlowModel, FlowNode, TYPE_FORK, TYPE_JOIN, TYPE_TASK, TYPE_CUSTOM,
    TaskState,
)

PROP_REAPPLY_MODE = "reapplyMode"
VAR_BRANCH_CTX = "_branchCtx"
VAR_REAPPLY_CTX = "_reapplyContext"

NORMAL = "NORMAL"
RETURN_TO_SOURCE = "RETURN_TO_SOURCE"

_LOCKS: dict[tuple[int, int], asyncio.Lock] = {}


def instance_lock(engine: Any, instance_id: int) -> asyncio.Lock:
    """同实例串行化（进程内）。多进程部署需仓储侧提供跨进程锁。"""
    key = (id(engine), instance_id or 0)
    lock = _LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _LOCKS[key] = lock
    return lock


def parse_mode(node: FlowNode) -> str:
    """缺省/空串⇒NORMAL；非法值必须报配置错误，不得静默当缺省。"""
    raw = str((node.properties or {}).get(PROP_REAPPLY_MODE, "") or "").strip()
    if raw in ("", NORMAL):
        return NORMAL
    if raw == RETURN_TO_SOURCE:
        return RETURN_TO_SOURCE
    raise ValueError(f"节点[{node.id}]的 reapplyMode 配置非法: {raw}")


def strip_reserved(args: Optional[dict]) -> dict:
    out = dict(args or {})
    out.pop(VAR_BRANCH_CTX, None)
    out.pop(VAR_REAPPLY_CTX, None)
    return out


# ─── 模型静态分析 ────────────────────────────────────────────────────────────

def _incoming(flow: FlowModel, node_id: str) -> list[str]:
    return [e.sourceNodeId for e in flow.edges if e.targetNodeId == node_id]


def _outgoing(flow: FlowModel, node_id: str) -> list[str]:
    return [e.targetNodeId for e in flow.edges if e.sourceNodeId == node_id]


def _backward_task_ids(flow: FlowModel, node_id: str, stop_fork: str = "") -> set[str]:
    """沿入边回溯到指定分叉之间的任务集合；可穿过嵌套的内层 fork，遇到 stop_fork 即止。"""
    out: set[str] = set()
    stack = _incoming(flow, node_id)
    visited: set[str] = set()
    by_id = {n.id: n for n in flow.nodes}
    while stack:
        cur = stack.pop()
        if not cur or cur in visited:
            continue
        visited.add(cur)
        n = by_id.get(cur)
        if n is None:
            continue
        if n.type == TYPE_FORK and cur == stop_fork:
            continue
        if n.type in (TYPE_TASK, TYPE_CUSTOM):
            out.add(cur)
        stack.extend(_incoming(flow, cur))
    return out


def _fork_for_join(flow: FlowModel, join_id: str) -> str:
    """汇合点对应的分叉：取「最近后支配汇合点 == 本 join」的 fork，多候选取最外层。

    不能取"回溯遇到的第一个 fork"——嵌套并行时外层汇合点沿内层分支回溯会先撞到内层 fork。
    """
    best = ""
    best_depth = -1
    for n in flow.nodes:
        if n.type != TYPE_FORK:
            continue
        if _nearest_post_dominating_join(flow, n.id) != join_id:
            continue
        d = _max_distance_to_join(flow, n.id, join_id)
        if d > best_depth:
            best_depth = d
            best = n.id
    return best


def _nearest_post_dominating_join(flow: FlowModel, fork_id: str) -> str:
    best = ""
    best_max = None
    for n in flow.nodes:
        if n.type != TYPE_JOIN:
            continue
        if not _post_dominates(flow, fork_id, n.id):
            continue
        d = _max_distance_to_join(flow, fork_id, n.id)
        if d >= 0 and (best_max is None or d < best_max):
            best_max = d
            best = n.id
    return best


def _post_dominates(flow: FlowModel, fork_id: str, join_id: str) -> bool:
    """屏蔽该汇合点后，fork 的任一分支仍能走到末端即不成立。"""
    stack = _outgoing(flow, fork_id)
    visited: set[str] = set()
    while stack:
        cur = stack.pop()
        if not cur or cur == join_id or cur in visited:
            continue
        visited.add(cur)
        outs = _outgoing(flow, cur)
        if not outs:
            return False
        stack.extend(outs)
    return True


def _max_distance_to_join(flow: FlowModel, fork_id: str, join_id: str) -> int:
    frontier = list(dict.fromkeys(_outgoing(flow, fork_id)))
    visited = set(frontier)
    d = 1
    found = -1
    while frontier:
        nxt: list[str] = []
        for cur in frontier:
            if cur == join_id:
                found = d
            for t in _outgoing(flow, cur):
                if t not in visited:
                    visited.add(t)
                    nxt.append(t)
        frontier = nxt
        d += 1
    return found


def static_scopes(flow: FlowModel, node_id: str) -> list[dict[str, Any]]:
    """节点所在的全部分支范围（外层→内层）。"""
    hits: list[tuple[int, dict[str, Any]]] = []
    for n in flow.nodes:
        if n.type != TYPE_JOIN:
            continue
        fork = _fork_for_join(flow, n.id)
        if not fork:
            continue
        inside = _backward_task_ids(flow, n.id, fork)
        if node_id not in inside:
            continue
        hits.append((len(inside), {"fork": fork, "join": n.id, "round": 0, "root": 0}))
    hits.sort(key=lambda x: -x[0])
    return [h[1] for h in hits]


def build_scopes(flow: FlowModel, node_id: str, parent_vars: Optional[dict],
                 parent_id: int, self_id: int) -> list[dict[str, Any]]:
    statics = static_scopes(flow, node_id)
    parent_scopes = scopes_of(parent_vars)
    out = []
    for s in statics:
        inherited = next((p for p in parent_scopes
                          if p.get("fork") == s["fork"] and p.get("join") == s["join"]), None)
        if inherited:
            out.append({"fork": s["fork"], "join": s["join"],
                        "round": int(inherited.get("round") or 0),
                        "root": int(inherited.get("root") or 0)})
        else:
            out.append({"fork": s["fork"], "join": s["join"], "round": parent_id, "root": self_id})
    return out


# ─── 变量读写 ────────────────────────────────────────────────────────────────

def scopes_of(vars_: Optional[dict]) -> list[dict[str, Any]]:
    raw = (vars_ or {}).get(VAR_BRANCH_CTX)
    if not isinstance(raw, list):
        return []
    out = []
    for item in raw:
        if isinstance(item, dict):
            out.append({"fork": str(item.get("fork", "")), "join": str(item.get("join", "")),
                        "round": int(item.get("round") or 0), "root": int(item.get("root") or 0)})
    return out


def reapply_of(vars_: Optional[dict]) -> Optional[dict[str, Any]]:
    raw = (vars_ or {}).get(VAR_REAPPLY_CTX)
    return raw if isinstance(raw, dict) and raw else None


def branch_of(rc: Optional[dict]) -> list[dict[str, Any]]:
    if not rc:
        return []
    return scopes_of({VAR_BRANCH_CTX: rc.get("branch")})


def find_scope(scopes: list[dict[str, Any]], join_id: str) -> Optional[dict[str, Any]]:
    return next((s for s in scopes if s.get("join") == join_id), None)


def new_context(source_task_id: int, source_node: str, branch: list[dict[str, Any]]) -> dict[str, Any]:
    return {"mode": RETURN_TO_SOURCE, "sourceTaskId": str(source_task_id),
            "sourceNode": source_node, "branch": branch or []}


# ─── 引擎侧适配 ──────────────────────────────────────────────────────────────

async def attach_created(engine: Any, flow: FlowModel, inst: Any, created: list[Any],
                         node_id: str, parent_id: int) -> None:
    """建单钩子：给本次新任务补分支归属（父任务在聚合内按 ID 查）。

    建单已在钩子之前 save_task 落库（仓储存的是快照），因此这里必须 update_task 写回，
    否则 _branchCtx 只存在于本次内存对象，跨请求读到空值会回落旧汇合判据。
    """
    if not created:
        return
    parent = next((t for t in getattr(inst, "tasks", []) if t.id == parent_id), None)
    for nt in created:
        nt.variables[VAR_BRANCH_CTX] = build_scopes(
            flow, node_id, parent.variables if parent else {}, parent_id, nt.id)
        await engine.repo.update_task(nt)


async def merge_ready(engine: Any, flow: FlowModel, inst: Any, join_id: str, arriving_id: int) -> bool:
    arriving = next((t for t in getattr(inst, "tasks", []) if t.id == arriving_id), None)
    scope = find_scope(scopes_of(arriving.variables), join_id) if arriving else None
    if scope is None:
        return len(await engine.repo.find_doing_tasks(inst.id)) == 0
    roots: dict[int, bool] = {}
    for t in getattr(inst, "tasks", []):
        s = find_scope(scopes_of(t.variables), join_id)
        in_round = s is not None and s["fork"] == scope["fork"] and s["round"] == scope["round"]
        if in_round:
            roots.setdefault(s["root"], False)
        if t.taskState != TaskState.DOING:
            continue
        if in_round:
            roots[s["root"]] = True
        for b in branch_of(reapply_of(t.variables)):
            if (b.get("join") == join_id and b.get("fork") == scope["fork"]
                    and b.get("round") == scope["round"]):
                roots[b["root"]] = True
    if not roots:
        return len(await engine.repo.find_doing_tasks(inst.id)) == 0
    return not any(roots.values())


async def create_task_at(engine: Any, flow: FlowModel, node: FlowNode, inst: Any, operator: str,
                         vars_: dict, parent_id: int, scopes: list[dict[str, Any]],
                         rc: Optional[dict]) -> Any:
    actors = await engine._resolve_actors(node, inst, operator, vars_)
    from .engine import _apply_expire_time, _append_new_task
    nt = inst.create_task(engine._next_id(), node.id, (node.text or {}).get("value", ""),
                          actors[0] if actors else "", operator,
                          (node.properties or {}).get("form", ""), datetime.now(),
                          parent_id, engine._is_first_task_node(flow, node))
    if len(actors) > 1:
        nt.actorIds = actors
    _apply_expire_time(nt, (node.properties or {}).get("expireTime"), inst.variables)
    nt.variables[VAR_BRANCH_CTX] = scopes or []
    if rc:
        nt.variables[VAR_REAPPLY_CTX] = rc
    await engine._apply_surrogate(nt, await engine._surrogate_process_name(flow, inst))
    await engine.repo.save_task(nt)
    _append_new_task(nt)
    await engine._fire_task_start(inst, nt, node.id, operator)
    return nt


async def _load_flow(engine: Any, inst: Any) -> FlowModel:
    from .engine import parse_flow_model
    def_ = await engine.repo.find_define_by_id(inst.defineId)
    return parse_flow_model(json.loads(def_.content))


async def execute_reapply(engine: Any, task: Any, operator: str, args: dict) -> Any:
    """submitType=5 且申请待办带直返上下文：校验来源后在来源节点直接建新待办。"""
    st = args.get("submitType")
    if st is not None and int(st) != 5:
        raise ValueError("带重提直返标记的申请待办只接受重新提交(submitType=5)")
    inst = await engine.repo.find_instance_by_id(task.processInstanceId)
    if inst is None:
        raise ValueError("流程实例不存在")
    flow = await _load_flow(engine, inst)
    rc = reapply_of(task.variables)
    source_id = int(str((rc or {}).get("sourceTaskId", "0")))
    source = next((t for t in inst.tasks if t.id == source_id), None)
    if source is None or source.taskState != TaskState.DONE or source.processInstanceId != inst.id:
        raise ValueError("重提直返来源任务无效或未完成")
    source_node = next((n for n in flow.nodes if n.id == source.taskName), None)
    if source_node is None or source_node.type != TYPE_TASK or parse_mode(source_node) != RETURN_TO_SOURCE:
        raise ValueError("重提直返来源节点配置已变更，拒绝执行")
    apply_node = next((n for n in flow.nodes if n.id == task.taskName), None)
    if apply_node is None or not engine._is_first_task_node(flow, apply_node):
        raise ValueError("重提直返必须从申请待办发起")
    _, inst2, _, vars_ = await engine._prepare_execute_task(task.id, operator, args)
    # 遗留的下一节点指定人参数不得影响本次建单（历史记录不删）
    from .engine import KEY_NEXT_NODE_OPERATOR
    vars_.pop(KEY_NEXT_NODE_OPERATOR, None)
    await create_task_at(engine, flow, source_node, inst2, operator, vars_, task.id,
                         branch_of(rc), None)
    return await engine.repo.find_instance_by_id(inst2.id)
