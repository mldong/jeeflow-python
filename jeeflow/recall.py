"""已办撤回（allowRecall，issues/246）——节点开关解析、窗口判据、账本追加（Python 栈）。

契约依据：jeeflow-doc `docs/spec/02-flow-definition.md` §4.4（开关与两条解析期硬校验）、
`docs/spec/04-engine-ops.md`「已办撤回」整节（宽口径窗口／动作三件／承诺边界）、
`docs/spec/06-facade.md` §processTask/recall（判据位序与五条逐字失败文案）、
`docs/spec/11-events.md` 码 10 TASK_RECALL。

形状基准＝jeeflow-java 参考实现 90469a2 的 `core/RecallSupport.java`（纯函数家族）＋
`core/JeeflowEngineImpl.recallTask`（判据位序＋落库序）＋ Go 腿 `engine/recall.go`
（3ed6a85，结构与判据的第二工作样本）。移植时保留三条承重纪律：

1. **判据全部排在任何状态变更之前**（报错即零写入）——内存仓储无回滚，
   先废弃再判错就会留下不可恢复的半撤状态；与后加签写入口/退回守卫同一规矩。
   位序照 java：行存在且已办结 → 参与者/特权 → 实例 DOING → 节点开关 → 窗口。
2. **参与人判断不走只认 DOING 的 isAllowed 族**——目标行是 FINISHED，按名册
   （``actorIds``）判断；``flow.auto``/``flow.admin`` 特权放行（与转办同口径）。
3. **原 FINISHED 行一律不改写**——jeeflow 无独立历史表，任务行即审批历史，
   翻回 DOING＝改写已出证记录；唯一追加痕迹是落在旧行变量上的
   ``tf_recallHistory`` 追加账本（``tf_`` 前缀使退回复活腿天然剔掉它）。

窗口取**宽口径**（owner 2026-11-03 拍）：从该行沿 ``task_parent_id`` 血缘产出的
全部下游行中**存在任何 FINISHED ⇒ 拦**；否则仍 DOING 的下游行全部废弃
（后加签/征询成员行、并行分支多支自然覆盖；已废弃(99)/已撤回(30) 行既不拦也不重复废弃）。

本模块**不依赖 engine**（引擎侧对象一律鸭子类型传入）；与 ``model`` 的关系照
``add_sign`` 同款：本模块顶层 import model 的常量，``model.parse_flow_model``
在函数体内延迟 import 本模块做解析期校验，顶层互指会成环。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

# 与 add_sign 同款的方向性依赖（见 add_sign.py 文件头注释）：本模块可在顶层 import model。
from .model import TYPE_START, TYPE_TASK
from . import add_sign as _as

# ─── 键名（逐字对齐 Java FlowConst / RecallSupport）──────────────────────────

#: 节点属性键：已办撤回开关（properties 平铺层，缺省 false）
PROP_ALLOW_RECALL = "allowRecall"

#: 追加式账本键：每条撤回 append ``{operator, time, recalledTaskId}``，落在**被撤回的
#: 旧行**变量上（原行不改写＝历史即审批记录，账本是唯一追加痕迹）；recalledTaskId
#: 指向复活新行（从历史记录可一步跳到唯一活待办）。``tf_`` 前缀使
#: ``engine._lineage_vars``（退回上一步的复活血缘腿）天然剔掉它 ⇒ 不污染新待办。
KEY_RECALL_HISTORY = "tf_recallHistory"

# ─── 逐字判据文案（跨栈统一；规范 06 §processTask/recall 失败文案表）──────────

MSG_WINDOW = "任务[{0}]的后续节点已办理，不能撤回"
MSG_SWITCH_OFF = "节点[{0}]未开启 allowRecall，不能撤回"
MSG_NOT_FINISHED = "任务非已完成状态，不可撤回"
MSG_NO_PERMISSION = "无权限撤回该任务"
MSG_INSTANCE_NOT_DOING = "流程实例非进行中状态，不可撤回"

# ─── 解析期硬校验文案（规范 02 §4.4，两条禁配逐字）────────────────────────────

MSG_COUNTERSIGN_FORBID = '会签节点[{0}]不允许配置 allowRecall：撤"谁的一票"语义不清'
MSG_FIRST_TASK_FORBID = ("首任务节点[{0}]不允许配置 allowRecall："
                         "申请节点走实例撤回＋重新发起，不需要同实例复活")

#: 全生态时间格式（规范 §2.4），与 tf_addSignHistory / tf_transferHistory 同尺
_TIME_LAYOUT = "%Y-%m-%d %H:%M:%S"


# ─── 节点属性解析 ─────────────────────────────────────────────────────────────

def node_enabled(node: Any) -> bool:
    """节点是**任务节点**且开了 ``allowRecall``（属性缺省 false；非法值按解析期同一判据抛）。

    非法值的解析期拦截在 :func:`validate_definition`（模型解析入口统一跑）；
    运行期再判一次属第二道（值不可能非法——能走到这里的流程都已过解析校验），
    与 ``add_sign.node_allow`` 同形状。
    """
    if node is None or getattr(node, "type", "") != TYPE_TASK:
        return False
    props = getattr(node, "properties", None) or {}
    return _as.parse_allow(props.get(PROP_ALLOW_RECALL),
                           getattr(node, "id", ""), PROP_ALLOW_RECALL)


# ─── 解析期硬校验（规范 02 §4.4）──────────────────────────────────────────────

def validate_definition(nodes: list, edges: list) -> None:
    """流程定义解析期的已办撤回校验，两批判据**按此顺序**跑：

    1. **逐节点值校验**（``allowRecall`` 必须是布尔档）——非法值当场抛配置错误，
       不得静默落回缺省（纪律照 ``allowAddSign`` 先例）；
    2. **两条拓扑硬校验**：会签节点（performType=1）与首任务节点（start 直接后继）配了
       ``allowRecall`` ⇒ 抛。会签节点撤"谁的一票"语义不清；申请节点走实例撤回＋
       重新发起（既有通道），不需要同实例复活。

    位序照 java ``ModelParser.parse``（后加签两判 → 已办撤回两判 → 动态会签三判）；
    首任务节点判定排在值校验之后（拓扑关系建好才谈得上"是不是 start 直接后继"）。
    只对 ``snaker:task`` 档动手；非任务节点带这个键属设计器冗余属性，不拦（java 亦然）。
    """
    task_nodes = [n for n in (nodes or []) if getattr(n, "type", "") == TYPE_TASK]
    for n in task_nodes:
        _as.parse_allow((n.properties or {}).get(PROP_ALLOW_RECALL), n.id, PROP_ALLOW_RECALL)
    starts = {getattr(n, "id", "") for n in (nodes or []) if getattr(n, "type", "") == TYPE_START}
    first_task = {e.targetNodeId for e in (edges or [])
                  if getattr(e, "sourceNodeId", "") in starts}
    for n in task_nodes:
        props = n.properties or {}
        if not _as.parse_allow(props.get(PROP_ALLOW_RECALL), n.id, PROP_ALLOW_RECALL):
            continue
        if _as.is_countersign_node(n):
            raise ValueError(MSG_COUNTERSIGN_FORBID.format(n.id))
        if n.id in first_task:
            raise ValueError(MSG_FIRST_TASK_FORBID.format(n.id))


# ─── 窗口判据（纯函数，判据前置＝报错即零写入的前半）──────────────────────────

def collect_descendants(tasks: list, ancestor_id: int) -> list:
    """按 ``task_parent_id`` 血缘收集 target 的全部后代行（BFS）。

    血缘在建单不变量里必写（发起 execution 无当前任务 ⇒ 0＝无父），故
    "有后代"与"无后代"都由数据现状决定——与 java ``RecallSupport.collectDescendants``
    同一条栈式扫描（pop 顺序不改变集合内容）。
    """
    result: list = []
    frontier = [ancestor_id]
    while frontier:
        parent = frontier.pop()
        for t in tasks:
            if t.parentTaskId == parent:
                result.append(t)
                frontier.append(t.id)
    return result


def validate_window(tasks: list, target: Any) -> list:
    """窗口判据（宽口径）：后代行中**存在任何 FINISHED** ⇒ 抛逐字文案；
    否则返回其中仍 DOING 的行（供废弃——后加签/征询成员行、并行分支多支都自然覆盖；
    已废弃(99)/已撤回(30) 行既不拦也不重复废弃）。"""
    doing: list = []
    for t in collect_descendants(tasks, target.id):
        if t.is_finished():
            raise ValueError(MSG_WINDOW.format(target.taskName))
        if t.is_doing():
            doing.append(t)
    return doing


# ─── 账本（只追加不覆盖，落被撤回的旧行）──────────────────────────────────────

def append_recall_history(target: Any, operator: str, resurrected_task_id: int) -> None:
    """账本追加（只追加不覆盖），落**被撤回的旧行**变量；``recalledTaskId`` 指向
    复活的新行（从历史记录可一步跳到唯一活待办）。

    ⚠️ 调用方随后必须把这一行**再落一次库**——本栈两仓存的是快照
    （java 那边是共享引用，改了就看得见），不补这一刀账本就丢
    （与 ``add_sign.append_history`` 同一条纪律）。"""
    if target is None:
        return
    vars_ = target.variables
    if vars_ is None:
        return
    existing = vars_.get(KEY_RECALL_HISTORY)
    history = existing if isinstance(existing, list) else []
    history.append({
        "operator": operator,
        "time": datetime.now().strftime(_TIME_LAYOUT),
        "recalledTaskId": resurrected_task_id,
    })
    vars_[KEY_RECALL_HISTORY] = history


def recall_history_of(task: Any) -> list[dict[str, Any]]:
    """读账本（测试与投影用）；无该键／脏值 ⇒ 空表。"""
    if task is None:
        return []
    v = (getattr(task, "variables", None) or {}).get(KEY_RECALL_HISTORY)
    return v if isinstance(v, list) else []
