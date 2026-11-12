"""动态会签（征询模式 dynamicCountersign，issues/247）——成员行识别、闸门计数、
投影、轮次与账本的纯函数家族＋解析期禁配（Python 栈）。

契约依据：jeeflow-doc `docs/spec/02-flow-definition.md` §4.4（开关与三条解析期硬校验）、
`docs/spec/04-engine-ops.md`「动态会签（征询模式）」整节（提交分流／成员行建单规则／
撤回接口／语义边界）、`docs/spec/06-facade.md` §2.7（协议键与投影）与
§processTask/revokeDynamicCountersign。本能力不新增节点类型、不新增 submitType 档、
无 DDL（成员行即普通任务行）、不新增事件码（成员行建/结走既有码 3/5）、
成员意见无表决语义。

形状基准＝jeeflow-java 参考实现 ffe2749 的 `core/CollectSupport.java`（纯函数家族）＋
`facade/JeeflowFacade` 的 tryDynamicCountersign/gateDynamicCountersign/taskRevokeCountersign
（go 腿 `engine/collect.go`＋`facade/collect.go` 同构——java 放门面的分流/闸门/撤回
照旧放门面，见 ``facade.py`` 那三条腿；发起腿是门面分流而不是引擎执行闸，
因为"带人提交不办结主办行"必须发生在 complete_task 之前且不进 execute 参数空间语义）。

语义一句话：主办提交带 ``tf_addSignOperators`` 时**不办结主办行**，按人建成员行收意见；
全部成员提交后闸门打开，主办再按普通办理（同意/不同意）决定走向。与后加签的本质差异：
后加签主办在拉人时即办结、接力自动推进；征询模式主办挂起、**拍板权始终在主办**。

# 成员行识别（Java 实测踩过的坑，别简化）

成员行＝parent 指向主办行**且同节点同名**的行（成员不能再拉人 ⇒ 无更深层级）。
**同名过滤不可省**：主办办结推进后，下一节点行的 parent 同样是主办行 id（建单不变量），
只认 parent 会把下游行误当成员——撤回腿会误废弃它们。

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

# ─── 键名（逐字对齐 Java FlowConst / CollectSupport）──────────────────────────

#: 节点属性键：动态会签（征询模式）开关（properties 平铺层，缺省 false）
PROP_DYNAMIC_COUNTERSIGN = "dynamicCountersign"

#: 协议键（选填，字符串）：发起该轮会签的原因说明，随账本条目落库并盖到每条成员行
#: （单行出证自足，不回查主办行）；选填不写键。与后加签共用 ``tf_addSignOperators``，
#: reason 这一枚是征询模式自己的（spec 06 §2.7）。
KEY_ADD_SIGN_REASON = "tf_addSignReason"

# ─── 逐字判据文案（跨栈统一；规范 06 文末失败文案表）──────────────────────────

MSG_PENDING = "节点[{0}]动态会签意见未收齐（已提交 {1}/{2}），不能办理"
MSG_DUPLICATE = "操作人[{0}]已在节点[{1}]的动态会签中"
MSG_MEMBER_PULL = "动态会签成员任务不能发起加签"
MSG_NO_PENDING = "节点[{0}]没有进行中的动态会签"
MSG_NO_PERMISSION_REVOKE = "无权限撤回该动态会签"

# ─── 解析期硬校验文案（规范 02 §4.4，三条禁配逐字）────────────────────────────

MSG_MUTEX_WITH_ADD_SIGN = ("节点[{0}]不允许同时配置 allowAddSign 与 dynamicCountersign："
                           "两档共用 tf_addSignOperators，行为靠节点属性分流，双开＝行为歧义")
MSG_COUNTERSIGN_FORBID = "会签节点[{0}]不允许配置 dynamicCountersign：会签池推进语义与征询挂起语义打架"
MSG_APPLY_FORBID = ("申请节点[{0}]不允许配置 dynamicCountersign："
                    "申请节点那手走实例撤回＋重新发起，不需要征询挂起")

#: 全生态时间格式（规范 §2.4），与 tf_addSignHistory / tf_recallHistory 同尺
_TIME_LAYOUT = "%Y-%m-%d %H:%M:%S"


# ─── 成员行识别（纯函数）──────────────────────────────────────────────────────

def node_enabled(node: Any) -> bool:
    """节点是**任务节点**且开了 ``dynamicCountersign``（缺省 false；非法值按解析期同一判据抛）。

    执行闸（``engine`` 征询档推进闸）与门面分流/闸门腿都用这一枚判"是不是征询节点"。
    """
    if node is None or getattr(node, "type", "") != TYPE_TASK:
        return False
    props = getattr(node, "properties", None) or {}
    return _as.parse_allow(props.get(PROP_DYNAMIC_COUNTERSIGN),
                           getattr(node, "id", ""), PROP_DYNAMIC_COUNTERSIGN)


def member_rows(tasks: list, host: Any) -> list:
    """成员行＝parent 指向主办行**且同节点同名**的行（成员不能再拉人 ⇒ 无更深层级）。

    issues/253：协办行（task_type=1）也满足 parent＋同名，但它属另一族——征询收齐闸只数
    征询成员行（task_type=0），两闸各数各的（规范 04「与征询允许同节点并存」）。
    """
    out: list = []
    if host is None:
        return out
    from . import aidant as _ad
    for t in tasks:
        if (host.id and t.parentTaskId == host.id and t.taskName == host.taskName
                and not _ad.is_aidant_row(t)):
            out.append(t)
    return out


def is_member_row(tasks: list, row: Any) -> bool:
    """成员行判定：parent 指向**同节点同名**行（成员行禁带拉人参数的判据）。

    issues/253：协办行（task_type=1）不是征询成员行——两族靠 task_type 分辨。
    """
    if row is None or not row.parentTaskId:
        return False
    from . import aidant as _ad
    if _ad.is_aidant_row(row):
        return False
    for t in tasks:
        if t.id == row.parentTaskId and t.taskName == row.taskName:
            return True
    return False


def pending_members(tasks: list, host: Any) -> list:
    """未答成员行（DOING），撤回接口的废弃集合。"""
    return [t for t in member_rows(tasks, host) if t.is_doing()]


def member_counts(tasks: list, host: Any) -> tuple[int, int]:
    """闸门原料：在册成员行总数与已提交（FINISHED）数——已废弃(99) 行（撤回腿清场的）
    不算在册。返回 ``(total, submitted)``。"""
    total = submitted = 0
    for t in member_rows(tasks, host):
        if int(t.taskState) == 99:
            continue
        total += 1
        if t.is_finished():
            submitted += 1
    return total, submitted


def build_projection(tasks: list, host: Any) -> Optional[dict[str, Any]]:
    """投影（规范 06 §2.7）：征询主办行 ``ext.dynamicCountersign``
    ＝``{active, round, total, submitted, actors:[{actor, taskId, state}]}``；
    无成员行 ⇒ ``None``。从成员行现算，跳过 99 废弃行（撤回清场的行不进投影）。"""
    members = member_rows(tasks, host)
    if not members:
        return None
    total = submitted = 0
    actors: list[dict[str, Any]] = []
    for m in members:
        if int(m.taskState) == 99:
            continue
        total += 1
        if m.is_finished():
            submitted += 1
        actors.append({
            "actor": m.actorIds[0] if m.actorIds else None,
            "taskId": m.id,
            "state": int(m.taskState),
        })
    return {
        "active": submitted < total,
        "round": last_round(host),
        "total": total,
        "submitted": submitted,
        "actors": actors,
    }


# ─── 轮次与账本 ───────────────────────────────────────────────────────────────

def last_round(host: Any) -> int:
    """主办行当前轮次＝账本条数（每次拉人追加一条＝轮次推进；撤回不消耗）。

    读的是主办行自己的 ``tf_addSignHistory`` 末条 ``round``——账本落**主办行**
    （征询模式下主办行未办结，行变量随行活到办结后），与后加签 ``round_of``
    读成员行轮次戳的写法对称。
    """
    history = _as.history_of(host)
    if not history:
        return 0
    r = (history[-1] or {}).get("round") if isinstance(history[-1], dict) else None
    try:
        return int(r)
    except (TypeError, ValueError):
        return 0


def append_collect_history(host: Any, operator: str, to_actors: list,
                           round_no: int, reason: str) -> None:
    """发起会签账本追加（只追加不覆盖），落**主办行**；每条含 reason（选填，空串不写键）。

    ⚠️ 调用方随后必须把主办行**再落一次库**（快照仓储纪律，与 ``add_sign.append_history``
    同一条；不补这一刀账本就丢）。"""
    entry = _base_entry(operator, to_actors, round_no)
    if reason:
        entry["reason"] = reason
    _append(host, entry)


def append_revoke_history(host: Any, operator: str, revoked_actors: list,
                          round_no: int, reason: str) -> None:
    """撤回会签账本追加：``revoked=True`` 标记本条是撤回（未答废弃），toActors＝被撤的人；
    轮次不消耗（本条不推进轮次，round 记当轮）。"""
    entry = _base_entry(operator, [], round_no)
    entry["toActors"] = list(revoked_actors)
    entry["revoked"] = True
    if reason:
        entry["reason"] = reason
    _append(host, entry)


def _base_entry(operator: str, to_actors: list, round_no: int) -> dict[str, Any]:
    """账本条目五键固定 camelCase（round/fromActor/toActors/time/operator），
    形状照 ``tf_addSignHistory`` 既有条目（后加签 ``append_history`` 同族）。"""
    return {
        "round": round_no,
        "fromActor": operator,
        "toActors": list(to_actors),
        "time": datetime.now().strftime(_TIME_LAYOUT),
        "operator": operator,
    }


def _append(host: Any, entry: dict[str, Any]) -> None:
    if host is None:
        return
    vars_ = host.variables
    if vars_ is None:
        return
    existing = vars_.get(_as.KEY_ADD_SIGN_HISTORY)
    history = existing if isinstance(existing, list) else []
    history.append(entry)
    vars_[_as.KEY_ADD_SIGN_HISTORY] = history


# ─── 解析期硬校验（规范 02 §4.4）──────────────────────────────────────────────

def validate_definition(nodes: list, edges: list) -> None:
    """流程定义解析期的动态会签校验，两批判据**按此顺序**跑：

    1. **逐节点值校验**（``dynamicCountersign`` 必须是布尔档）——非法值当场抛配置错误；
    2. **三条拓扑/互斥硬校验**（位序照 java ``ModelParser.parse``）：与 ``allowAddSign``
       双开（两模式共用 ``tf_addSignOperators``，行为靠节点属性分流，双开＝行为歧义）、
       会签节点禁配（会签池推进语义与征询挂起语义打架）、申请节点（start 直接后继）禁配
       （申请节点那手走实例撤回＋重新发起，不需要征询挂起）。
    """
    task_nodes = [n for n in (nodes or []) if getattr(n, "type", "") == TYPE_TASK]
    for n in task_nodes:
        _as.parse_allow((n.properties or {}).get(PROP_DYNAMIC_COUNTERSIGN),
                        n.id, PROP_DYNAMIC_COUNTERSIGN)
    starts = {getattr(n, "id", "") for n in (nodes or []) if getattr(n, "type", "") == TYPE_START}
    first_task = {e.targetNodeId for e in (edges or [])
                  if getattr(e, "sourceNodeId", "") in starts}
    for n in task_nodes:
        props = n.properties or {}
        if not _as.parse_allow(props.get(PROP_DYNAMIC_COUNTERSIGN),
                               n.id, PROP_DYNAMIC_COUNTERSIGN):
            continue
        if _as.parse_allow(props.get(_as.PROP_ALLOW_ADD_SIGN), n.id):
            raise ValueError(MSG_MUTEX_WITH_ADD_SIGN.format(n.id))
        if _as.is_countersign_node(n):
            raise ValueError(MSG_COUNTERSIGN_FORBID.format(n.id))
        if n.id in first_task:
            raise ValueError(MSG_APPLY_FORBID.format(n.id))
