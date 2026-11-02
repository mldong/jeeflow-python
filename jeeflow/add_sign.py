"""后加签（可重复）——节点开关解析、写入口校验、轮次与账本读写（Python 栈）。

契约依据：jeeflow-doc `docs/spec/02-flow-definition.md` §4.3（两开关与两条解析期硬校验）、
`docs/spec/04-engine-ops.md`「后加签（可重复）」整节（三档推进闸／merged 判据／建单七条／边界表）、
`docs/spec/06-facade.md` §2.7（协议键与互斥论证）与文末失败 msg 表（四条逐字）。

形状基准＝jeeflow-java `core/AddSignSupport.java` ＋ `handler/impl/AddSignHandler.java`，
移植时保留三条承重纪律：

1. **四判据（互斥／开关未开／加给自己／超上限）排在任何状态变更之前**——内存仓储无回滚，
   先 `complete_task` 再判错就会留下一条已完成却无人能改回的任务行；
   所以校验挂在 `EngineImpl._prepare_execute_task` 的 `inst.complete_task(...)` **之前**，
   而不是放进建单腿（与 java 把 `validateSubmission` 放在 `prepareExecution` 同理）。
2. **加签人只认当次请求**（`operators(args)`，args 是本次提交参数）——**不得**从合并后的执行变量
   或实例变量里取：`tf_addSignOperators` 一旦随实例变量跨请求复活，后续办结会凭空多建一条本节点待办。
   根因由 `ProcessInstance.complete_task` 那腿修掉（实例变量只收数据类键），本条是第二道，不替代根因。
3. **账本只追加不覆盖**，且落在**已完成的任务行**变量上；`tf_` 前缀使 `engine._lineage_vars`
   （退回上一步的复活血缘腿）天然剔掉它 ⇒ 不污染新待办，正是审计想要的形状。

本模块**不依赖 engine**（引擎侧对象一律鸭子类型传入），因此 model 层的解析期校验也能复用它；
循环导入由 `model.parse_flow_model` 里的函数级延迟 import 收口。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

# ⚠️ 拓扑常量的方向性依赖：本模块**可以**在顶层 import model（model 只在自己函数体里
# 延迟 import 本模块，见 model.parse_flow_model 末尾那一刀），反过来若把 model 的顶层
# 依赖加进 model 就会成环——加 model 顶层 import 前先改这里的写法。
from .model import TF_KEY_PREFIX, TYPE_START, TYPE_TASK
from .spi import normalize_actors, normalize_actor_value

# ─── 键名（逐字对齐 Java FlowConst）────────────────────────────────────────────

#: 节点属性键：后加签开关（properties 平铺层，缺省 false）
PROP_ALLOW_ADD_SIGN = "allowAddSign"
#: 节点属性键：加签轮次上限（0＝不限，缺省 0）
PROP_ADD_SIGN_MAX_ROUNDS = "addSignMaxRounds"

#: 本次提交指定的**本节点**后继办理人（逗号串与数组两形同判据）
KEY_ADD_SIGN_OPERATORS = "tf_addSignOperators"
#: 追加式账本（落已完成任务行变量，形状照 tf_transferHistory 那族）
KEY_ADD_SIGN_HISTORY = "tf_addSignHistory"
#: 轮次号键前缀（拼 `_{节点名}`）。不复用会签的 loopCounter——后者是退回复活时被剔的会签簿记
VAR_ADD_SIGN_ROUND = "addSignRound"

#: 下一节点处理人（与 tf_addSignOperators 互斥）；与 engine.KEY_NEXT_NODE_OPERATOR 同值
KEY_NEXT_NODE_OPERATOR = "tf_nextNodeOperator"

# ─── 逐字判据文案（跨栈统一；规范 06 文末失败 msg 表）──────────────────────────

MSG_MUTEX = "tf_addSignOperators 与 tf_nextNodeOperator 互斥，不能同时提交"
MSG_SWITCH_OFF = "节点[{0}]未开启 allowAddSign，不接受 tf_addSignOperators"
MSG_SELF = "不能把任务加签给自己"
MSG_MAX_ROUNDS = "节点[{0}]的加签轮次已达上限 {1}，本次提交被拒绝"
MSG_ALLOW_ILLEGAL = "节点[{0}]的 {1} 配置非法: {2}"
MSG_ROUNDS_NOT_INT = "节点[{0}]的 {1} 配置非法: {2}（必须是整数）"
MSG_ROUNDS_NOT_INT_ARG = "节点[{0}]的 {1} 配置非法: {2}（必须是整数，0 表示不限）"
MSG_ROUNDS_NEGATIVE = "节点[{0}]的 {1} 配置非法: {2}（不得为负，0 表示不限）"
MSG_COUNTERSIGN_FORBID = ("会签节点[{0}]不允许配置 allowAddSign："
                          "会签池内加人与会签之后加人是两种语义，推进闸会打架")
MSG_FIRST_TASK_FORBID = ("首任务节点[{0}]不允许配置 allowAddSign："
                         "申请节点的办理是发起/重提语义，与加签路由打架")

#: 全生态时间格式（规范 §2.4），与 tf_transferHistory 同尺
_TIME_LAYOUT = "%Y-%m-%d %H:%M:%S"


# ─── 节点属性解析（缺省不静默：非法值报配置错误）──────────────────────────────

def parse_allow(value: Any, node_name: str) -> bool:
    """解析 ``allowAddSign``：缺省 false；非布尔报配置错误，不得静默当缺省。

    档位与 java 逐字同：``None``／空串／纯空白 ⇒ False；布尔原样；串 ``"true"``/``"FALSE"``
    （大小写不敏感，trim 后比）⇒ 真/假；**其余一律抛**——``1``/``"yes"`` 这类"看着像真"的值
    静默落回缺省，就是配置者以为开关开了、实际没有的那类排障地狱。
    """
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    v = str(value).strip()
    if v == "":
        return False
    if v.lower() == "true":
        return True
    if v.lower() == "false":
        return False
    raise ValueError(MSG_ALLOW_ILLEGAL.format(node_name, PROP_ALLOW_ADD_SIGN, v))


def parse_max_rounds(value: Any, node_name: str) -> int:
    """解析 ``addSignMaxRounds``：缺省 0＝不限；非整数或负数报配置错误。

    ``bool`` 在 python 里是 ``int`` 的子类，java 那边 ``Boolean`` 却**不是** ``Number``——
    故这里显式把 bool 摘出去走字符串臂（``True`` ⇒ ``"True"`` ⇒ 非整数 ⇒ 抛），与 java 同答案；
    不摘就会把 ``"allowAddSignMaxRounds": true`` 读成上限 1，属跨栈分叉。
    小数档同理两家都抛（java ``doubleValue() != intValue()``／本栈 ``int(value) != value``），
    而 ``2.0`` 这类"整数值的小数"两家都收（java 那个判据就是这么写的）。
    """
    if value is None:
        return 0
    if isinstance(value, bool):
        v = str(value).strip()
        if v == "":
            return 0
        raise ValueError(MSG_ROUNDS_NOT_INT_ARG.format(node_name, PROP_ADD_SIGN_MAX_ROUNDS, v))
    if isinstance(value, (int, float)):
        n = int(value)
        if float(value) != n:
            raise ValueError(MSG_ROUNDS_NOT_INT.format(node_name, PROP_ADD_SIGN_MAX_ROUNDS, value))
        if n < 0:
            raise ValueError(MSG_ROUNDS_NEGATIVE.format(node_name, PROP_ADD_SIGN_MAX_ROUNDS, n))
        return n
    v = str(value).strip()
    if v == "":
        return 0
    try:
        n = int(v)
    except ValueError:
        raise ValueError(MSG_ROUNDS_NOT_INT_ARG.format(node_name, PROP_ADD_SIGN_MAX_ROUNDS, v)) from None
    if n < 0:
        raise ValueError(MSG_ROUNDS_NEGATIVE.format(node_name, PROP_ADD_SIGN_MAX_ROUNDS, n))
    return n


def node_allow(node: Any) -> bool:
    """节点是否开了后加签（属性缺省 false；非法值按解析期同一判据抛）。"""
    return parse_allow((getattr(node, "properties", None) or {}).get(PROP_ALLOW_ADD_SIGN)
                       if node is not None else None,
                       getattr(node, "id", "") if node is not None else "")


def node_max_rounds(node: Any) -> int:
    """节点的加签轮次上限（缺省 0＝不限；非法值同档抛）。"""
    return parse_max_rounds((getattr(node, "properties", None) or {}).get(PROP_ADD_SIGN_MAX_ROUNDS)
                            if node is not None else None,
                            getattr(node, "id", "") if node is not None else "")


def is_countersign_node(node: Any) -> bool:
    """是否会签档（``performType`` 容错解析，与 java ``ProcessTaskPerformTypeEnum.codeOf`` 同档：
    ``1``／``"ALL"``／``"COUNTERSIGN"``（大小写不敏感）算会签，其余算普通）。

    存在的理由有两处：解析期硬校验（会签节点不得配 allowAddSign）与执行闸的档位顺序
    （java ``TaskModel.exec`` 是「会签档优先，else-if allowAddSign」，两档天然互斥）。
    """
    props = (getattr(node, "properties", None) or {}) if node is not None else {}
    raw = props.get("performType", 0)
    if raw is None:
        return False
    s = str(raw).strip().upper()
    if s in ("ALL", "COUNTERSIGN"):
        return True
    try:
        return int(str(raw).strip()) == 1
    except (TypeError, ValueError):
        return False


# ─── 解析期硬校验（规范 02 §4.3）──────────────────────────────────────────────

def validate_definition(nodes: list, edges: list) -> None:
    """流程定义解析期的后加签校验，两批判据**按此顺序**跑：

    1. **逐节点值校验**（``allowAddSign`` 必须是布尔档、``addSignMaxRounds`` 必须是非负整数档）
       ——非法值当场抛配置错误，不得静默落回缺省（纪律照 ``reapplyMode`` 先例）；
    2. **两条拓扑硬校验**：会签节点（performType=1）与首任务节点（start 直接后继）配了
       ``allowAddSign`` ⇒ 抛。静默跑下去会让配置者以为开关生效，实际两档推进闸各裁各的。

    首任务节点判定排在值校验之后（与 java 同序：引用/拓扑关系建好才谈得上"是不是 start 直接后继"）。
    只对 ``snaker:task`` 档动手；非任务节点带这两个键属设计器冗余属性，不拦（java 亦然）。
    """
    task_nodes = [n for n in (nodes or []) if getattr(n, "type", "") == TYPE_TASK]
    for n in task_nodes:
        props = n.properties or {}
        parse_allow(props.get(PROP_ALLOW_ADD_SIGN), n.id)
        parse_max_rounds(props.get(PROP_ADD_SIGN_MAX_ROUNDS), n.id)
    starts = {getattr(n, "id", "") for n in (nodes or []) if getattr(n, "type", "") == TYPE_START}
    first_task = {e.targetNodeId for e in (edges or [])
                  if getattr(e, "sourceNodeId", "") in starts}
    for n in task_nodes:
        props = n.properties or {}
        if not parse_allow(props.get(PROP_ALLOW_ADD_SIGN), n.id):
            continue
        if is_countersign_node(n):
            raise ValueError(MSG_COUNTERSIGN_FORBID.format(n.id))
        if n.id in first_task:
            raise ValueError(MSG_FIRST_TASK_FORBID.format(n.id))


# ─── 写入口校验（顺序不可反，且必须排在状态变更之前）──────────────────────────

def operators(args: Optional[dict]) -> list[str]:
    """本次提交指定的加签人（逗号串与数组两形共用 ``spi.normalize_actors`` 那一枚归一单点：
    逐元素 str＋trim、空串/纯空白/``None`` 丢弃、顺序保持、同次折叠）。
    **没点人返回空表**＝正常流转档（空串与空数组都算没传，不算"加给空集合"）。"""
    if not args:
        return []
    return normalize_actors(args.get(KEY_ADD_SIGN_OPERATORS))


def validate_submission(node: Any, node_name: str, task: Any,
                        args: Optional[dict], operator: str) -> None:
    """写入口四判据（互斥／开关未开／加给自己／超上限）。**必须在改动任何状态之前调用**。

    互斥这条的理由不是语义矛盾（一个管本节点再来一轮、一个管流出后交给谁，逻辑可共存），
    而是**参数生命周期**：``tf_*`` 键只在当次请求的执行参数与已完成任务行变量里可读，
    加签使令牌停留本节点、流出发生在后续请求，届时先传的 ``tf_nextNodeOperator`` 已不可读
    ——同传时它是死参数。静默择一＝用户选的"下一节点处理人"无声消失，报错是把它变响话。

    :param node: 当前行的节点模型（查不到传 ``None`` ⇒ 按"未开开关"处理）
    :param node_name: 报错文案用的节点名（取任务行的 ``taskName``，与 java 同）
    :param task: 即将办结的那条任务行（读它的轮次判上限；此刻**尚未**改状态）
    """
    if args is None:
        return
    ops = operators(args)
    next_op = args.get(KEY_NEXT_NODE_OPERATOR)
    next_present = next_op is not None and str(next_op).strip() != ""
    if ops and next_present:
        raise ValueError(MSG_MUTEX)
    if not ops:
        # 没点人＝正常流转档，其余判据不涉及（也不要求节点开开关）
        return
    if node is None or getattr(node, "type", "") != TYPE_TASK or not node_allow(node):
        raise ValueError(MSG_SWITCH_OFF.format(node_name))
    me = normalize_actor_value(operator)
    for actor in ops:
        if me != "" and me == actor:
            raise ValueError(MSG_SELF)
    max_rounds = node_max_rounds(node)
    if max_rounds > 0 and round_of(task, node_name) + 1 > max_rounds:
        # 超上限＝报错，**不强制放行**（放行＝伪造"审批完成"）
        raise ValueError(MSG_MAX_ROUNDS.format(node_name, max_rounds))


# ─── 轮次 / 账本 / 本节点待办 ─────────────────────────────────────────────────

def round_of(task: Any, node_name: str) -> int:
    """读本任务行的加签轮次：首圈（普通建单产生的那条）没有该键 ⇒ 0。"""
    if task is None:
        return 0
    v = (getattr(task, "variables", None) or {}).get(VAR_ADD_SIGN_ROUND + "_" + node_name)
    try:
        return int(v) if v is not None else 0
    except (TypeError, ValueError):
        return 0


def append_history(completed: Any, operator: str, to_actors: list[str], round_no: int) -> None:
    """账本追加（只追加不覆盖），落在**已完成的那条任务行**变量上。

    五键固定 ``{round, fromActor, toActors, time, operator}``，``time`` 为
    ``"yyyy-MM-dd HH:mm:ss"`` 串（规范 §2.4 全生态格式，账本跨栈同形）。
    ⚠️ 调用方随后必须把这一行**再落一次库**（本栈内存/SQL 仓都存的是快照，
    java 那边是共享引用——形状差异见本轮报告的"与 java 不一致处"）。
    """
    if completed is None:
        return
    vars_ = completed.variables
    if vars_ is None:
        return
    existing = vars_.get(KEY_ADD_SIGN_HISTORY)
    history = existing if isinstance(existing, list) else []
    history.append({
        "round": round_no,
        "fromActor": operator,
        "toActors": list(to_actors),
        "time": datetime.now().strftime(_TIME_LAYOUT),
        "operator": operator,
    })
    vars_[KEY_ADD_SIGN_HISTORY] = history


def history_of(task: Any) -> list[dict[str, Any]]:
    """读账本（测试与投影用）；无该键／脏值 ⇒ 空表。"""
    if task is None:
        return []
    v = (getattr(task, "variables", None) or {}).get(KEY_ADD_SIGN_HISTORY)
    return v if isinstance(v, list) else []


def has_doing_on_node(inst: Any, node_name: str) -> bool:
    """本节点是否还有未完成任务（刚办结那条此刻已是 DONE，不计入——由调用方保证已同步进聚合）。"""
    if inst is None or not node_name:
        return False
    return any(node_name == t.taskName and t.is_doing() for t in (getattr(inst, "tasks", None) or []))


def node_allows(node: Any) -> bool:
    """执行闸的档位判据：节点是**任务节点**且开了 ``allowAddSign``。

    会签档的排除**不在这里**（由调用方按 java 的档位顺序裁：会签优先、else-if 加签），
    本函数只回答"这个节点开了开关没有"。"""
    if node is None or getattr(node, "type", "") != TYPE_TASK:
        return False
    return node_allow(node)
