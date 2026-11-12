"""动态协办（aidant，issues/253）——协办行识别、等待闸计数、投影、轮次与账本的纯函数家族
（Python 栈）。

契约依据：jeeflow-doc `docs/spec/04-engine-ops.md`「动态协办（aidant）」整节（派生规则／
推进闸与等待闸／协办行能力与边界／留痕与投影）、`docs/spec/06-facade.md` §2.7 协议键与
§processTask/aidant、文末跨栈统一失败文案表「协办六行」。本能力**无节点开关**
（节点级 ``taskType=1`` 解析期禁配，issues/252）、不新增节点类型、不新增 submitType 档、
无 DDL（协办行即普通任务行）、不新增事件码（协办行建/结走既有码 3/5）。

形状基准＝jeeflow-java 参考实现（issues/253 实施轮工作树现读）的 `core/AidantSupport.java`
（纯函数家族）＋ `core/JeeflowEngineImpl.aidantTask`／`completeAidantWithoutDrive`／
`prepareExecution` 里的 `gateWait` ＋ `model/TaskModel.abandonAidantOnLeave` ＋
`handler/impl/CountersignHandler`（会签合并显式排除协办行）；
`jeeflow-go/engine/aidant.go` 与 `jeeflow-node/src/aidant.ts` 是同一条移植腿的另两份样本。

语义一句话：主办把"这活"交给别人干——按人派生 ``task_type=1`` 的协办子行，**主办行原样保留
DOING**；协办行办结不驱动流程，节点推进仍由主办行办结触发。与征询的本质差异：征询收的是
"意见"且**强制等收齐**；协办交的是"活"，**等不等由派生参数 waitCompletion 决定**（缺省 false＝不等）。

# 与征询的分辨判据＝task_type（别简化）

两族子行都是 parent 指向主办行＋同节点同名：协办行 ``task_type=1``、征询成员行 ``task_type=0``。
等待闸只数协办行、征询收齐闸只数成员行，各数各的（spec 04「与征询允许同节点并存」）。
**同名过滤不可省**：主办办结推进后，下一节点行的 parent 同样是主办行 id（建单不变量），
只认 parent 会把下游行误当协办行——清场腿会误废弃它们。

本模块**不依赖 engine**（引擎侧对象一律鸭子类型传入），与 ``collect``／``add_sign`` 同款纪律。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

# ─── 键名（逐字对齐 Java FlowConst / AidantSupport）──────────────────────────

#: 动态协办账本：{round, fromActor, toActors, reason, time, operator} 只追加，落**主办行**变量
#: （形状照 tf_addSignHistory，键序锁定、time 为 §2.4 字符串）。离开节点清场未办结协办行时
#: 追加一条带 abandoned=True 的条目（照征询撤回的 revoked）。
KEY_AIDANT_HISTORY = "tf_aidantHistory"

#: 派生原因（选填，字符串）：盖每条协办行（单行出证自足）；**不占 tf_approvalComment**
#: （协办不是审批意见，与转办末跳文案不同族）。
KEY_AIDANT_REASON = "tf_aidantReason"

#: 等待闸开关：派生时把 waitCompletion 盖在**主办行**变量上，缺省 False＝主办不等协办办结
#: 即可推进。tf_ 前缀使实例合并自动剥键（不传播，与 tf_allowRecall 同通道）；
#: 投影 ext.aidant.wait 同源。
KEY_AIDANT_WAIT = "tf_aidantWait"

# ─── 逐字判据文案（跨栈统一；规范 06 文末失败文案表「协办六行」＋等待闸那句）──────

MSG_NO_ACTORS = "actorIds 必填"
MSG_NO_PERMISSION = "无权限派生协办任务"
MSG_NOT_DOING = "任务非进行中，不可派生协办"
MSG_DUPLICATE = "该人已有未办结的协办任务"
MSG_NO_RECURSION = "协办任务不能再派生协办"
MSG_TASK_NOT_FOUND = "任务不存在"
MSG_WAIT_PENDING = "节点[{0}]协办未办结（已办结 {1}/{2}），不能办理"

#: 全生态时间格式（规范 §2.4），与 tf_addSignHistory / tf_recallHistory 同尺
_TIME_LAYOUT = "%Y-%m-%d %H:%M:%S"

#: 协办档 task_type 值（SECONDARY）——两族子行唯一的分辨判据
TASK_TYPE_SECONDARY = 1


# ─── 协办行识别（纯函数）──────────────────────────────────────────────────────

def is_aidant_row(row: Any) -> bool:
    """协办行判定＝``task_type=1``（SECONDARY）。这是两族子行唯一的分辨判据。"""
    return row is not None and int(getattr(row, "taskType", 0) or 0) == TASK_TYPE_SECONDARY


def aidant_rows(tasks: list, host: Any) -> list:
    """协办行＝parent 指向主办行**且同节点同名且 task_type=1** 的行。

    同名过滤不可省（照 ``collect.member_rows`` 的理由）：主办办结推进后，下一节点行的 parent
    同样是主办行 id（建单不变量），只认 parent 会把下游行误当协办行。``task_type`` 过滤同样
    不可省：征询成员行也满足 parent＋同名，混进来会让等待闸把"意见未收齐"误报成"协办未办结"。
    """
    out: list = []
    if host is None or not getattr(host, "id", 0):
        return out
    for t in tasks or []:
        if (is_aidant_row(t) and t.parentTaskId == host.id
                and t.taskName == host.taskName):
            out.append(t)
    return out


def count_aidant(tasks: list, host: Any) -> tuple[int, int]:
    """等待闸原料：在册协办行总数与已办结（FINISHED）数——已废弃(99) 行（离开节点清场的）
    不算在册（spec 04「计数跳过已废弃(99) 行」）。返回 ``(total, submitted)``。"""
    total = submitted = 0
    for t in aidant_rows(tasks, host):
        if int(t.taskState) == 99:
            continue
        total += 1
        if t.is_finished():
            submitted += 1
    return total, submitted


def pending_aidant(tasks: list, host: Any) -> list:
    """未办结协办行（DOING）——等待闸的判据集合，也是离开节点时的清场集合。"""
    return [t for t in aidant_rows(tasks, host) if t.is_doing()]


def wait_of(host: Any) -> bool:
    """等待闸开关（``waitCompletion``）：派生时盖在**主办行**变量上，缺省 False＝不等。
    多轮派生时后一次的值生效（投影 ``ext.aidant.wait`` 同源）。

    非法值按缺省 False（照 java ``Boolean.parseBoolean``：只认 "true"，大小写不敏感）——
    契约只定义了 bool 档，不为非法值新造文案（协办六行之外不加第七行）。
    """
    if host is None:
        return False
    v = (getattr(host, "variables", None) or {}).get(KEY_AIDANT_WAIT)
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() == "true"


def gate_wait(tasks: list, host: Any) -> None:
    """等待闸（spec 04「推进闸与等待闸」）：``waitCompletion=True`` 时主办行办结前判
    "不存在 DOING 协办行"，未齐 ⇒ 逐字文案报错、零写入。协办行自身不受本闸约束
    （它办结本来就不驱动流程）；``waitCompletion=False``（缺省）原样放行。

    必须由引擎排在 ``complete_task`` **之前**——内存仓储无回滚，判据后置时"报错"那一次
    调用结束后主办待办已经消失（与后加签/退回守卫同一规矩）。
    """
    if host is None or is_aidant_row(host) or not wait_of(host):
        return
    if not pending_aidant(tasks, host):
        return
    total, submitted = count_aidant(tasks, host)
    raise ValueError(MSG_WAIT_PENDING.format(host.taskName, submitted, total))


def filter_out_aidant(tasks: list) -> list:
    """从行集合里剔掉协办行（task_type=1）——会签合并计数与"本节点还有兄弟待办"判据共用
    这一枚（spec 04「不计入会签合并计数」＋「缺省 false 主办可随时推进」）。"""
    return [t for t in (tasks or []) if not is_aidant_row(t)]


# ─── 轮次与账本 ───────────────────────────────────────────────────────────────

def history_of(task: Any) -> list:
    """读账本（测试与投影用）；无该键／脏值 ⇒ 空表。"""
    if task is None:
        return []
    v = (getattr(task, "variables", None) or {}).get(KEY_AIDANT_HISTORY)
    return v if isinstance(v, list) else []


def last_round(host: Any) -> int:
    """主办行当前轮次＝账本末条 ``round``（每次派生追加一条＝轮次推进；**清场记账不消耗轮次**）。"""
    history = history_of(host)
    for entry in reversed(history):
        if not isinstance(entry, dict):
            continue
        try:
            n = int(entry.get("round"))
        except (TypeError, ValueError):
            continue
        if n > 0:
            return n
    return 0


def append_aidant_history(host: Any, operator: str, to_actors: list,
                          round_no: int, reason: str) -> None:
    """派生账本追加（只追加不覆盖），落**主办行**变量；六键
    ``{round, fromActor, toActors, reason, time, operator}``（spec 06 §2.7），
    ``reason`` 空串兜底、``time`` 一律 §2.4 字符串。

    主办行在派生时不办结，``complete_task`` 的变量合并是并入而非整体替换，故账本能活到
    主办办结之后（与转办/征询账本同一条存活理由）。
    ⚠️ 调用方随后必须把主办行**再落一次库**（快照仓储纪律，与 ``collect.append_collect_history``
    同一条；不补这一刀账本就丢）。
    """
    _append(host, _base_entry(operator, to_actors, round_no, reason))


def append_abandon_history(host: Any, operator: str, abandoned_actors: list,
                           round_no: int) -> None:
    """清场账本追加：``abandoned=True`` 标记本条是"离开节点废弃"（形状照征询撤回条目的
    ``revoked=True``），六键原序不动，``toActors``＝被废弃的人。"""
    entry = _base_entry(operator, [], round_no, "")
    entry["toActors"] = list(abandoned_actors)
    entry["abandoned"] = True
    _append(host, entry)


def _base_entry(operator: str, to_actors: list, round_no: int, reason: str) -> dict[str, Any]:
    """账本条目六键固定 camelCase（round/fromActor/toActors/reason/time/operator），
    形状照 ``tf_addSignHistory`` 既有条目（征询 ``_base_entry`` 同族，多 reason 一枚）。"""
    return {
        "round": round_no,
        "fromActor": operator,
        "toActors": list(to_actors),
        "reason": reason if reason is not None else "",
        "time": datetime.now().strftime(_TIME_LAYOUT),
        "operator": operator,
    }


def _append(host: Any, entry: dict[str, Any]) -> None:
    if host is None:
        return
    vars_ = host.variables
    if vars_ is None:
        return
    existing = vars_.get(KEY_AIDANT_HISTORY)
    history = existing if isinstance(existing, list) else []
    history.append(entry)
    vars_[KEY_AIDANT_HISTORY] = history


# ─── 投影 ─────────────────────────────────────────────────────────────────────

def build_projection(tasks: list, host: Any) -> Optional[dict[str, Any]]:
    """投影（spec 06 §2.7）：主办行 ``ext.aidant``
    ＝``{active, round, total, submitted, actors:[{actor, taskId, state}], wait}``；
    无协办行 ⇒ ``None``（门面那侧"不写键"）。从 ``task_parent_id``＋``task_type`` **现算**、
    不落新存储，跳过 99 废弃行（清场的行不进投影，与征询投影同口径）。"""
    rows = aidant_rows(tasks, host)
    if not rows:
        return None
    total = submitted = 0
    actors: list[dict[str, Any]] = []
    for t in rows:
        if int(t.taskState) == 99:
            continue
        total += 1
        if t.is_finished():
            submitted += 1
        actors.append({
            "actor": t.actorIds[0] if t.actorIds else None,
            "taskId": t.id,
            "state": int(t.taskState),
        })
    return {
        "active": submitted < total,
        "round": last_round(host),
        "total": total,
        "submitted": submitted,
        "actors": actors,
        "wait": wait_of(host),
    }