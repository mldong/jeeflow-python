"""域类型——对标 Java domain + model 包"""
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Optional

from .surrogate import surrogate_enabled_on, to_datetime

# ─── LogicFlow JSON Types ──────────────────────────────────────────────────────

@dataclass
class FlowModel:
    name: str = ""
    displayName: str = ""
    type: str = ""
    nodes: list["FlowNode"] = field(default_factory=list)
    edges: list["FlowEdge"] = field(default_factory=list)

@dataclass
class FlowNode:
    id: str = ""
    type: str = ""
    x: float = 0
    y: float = 0
    properties: dict[str, Any] = field(default_factory=dict)
    text: dict[str, str] = field(default_factory=dict)

@dataclass
class FlowEdge:
    id: str = ""
    sourceNodeId: str = ""
    targetNodeId: str = ""
    properties: dict[str, Any] = field(default_factory=dict)
    text: Optional[dict[str, str]] = None

# ─── Node Type Constants ───────────────────────────────────────────────────────

TYPE_START    = "snaker:start"
TYPE_END      = "snaker:end"
TYPE_TASK     = "snaker:task"
TYPE_DECISION = "snaker:decision"
TYPE_FORK     = "snaker:fork"
TYPE_JOIN     = "snaker:join"
TYPE_CUSTOM   = "snaker:custom"

# 类型表全集（spec/02「类型键的三条义务」第 2 条 · issues/141 G4 立的判据本体）。
#
# 为什么要**具名成一个集合**，而不是让执行腿 `if/elif` 的 `else` 直接兜住：
# `snaker:start` 也是表里的一档，但它在执行链上是**入口**（引擎从 start 的出边起步，start 自身
# 正常不会被 `_execute_node` 走到）。用"没人认领"当未知判据，会把"令牌真走到 start 上"这种
# 拓扑病误报成"类型不在表里"——两个病得分别可诊断。义务 2 要的是**串 ∉ 表** 那一判。
#
# ⚠️ 表里**没有** `snaker:subProcess`／`snaker:subprocess`：owner 2026-10-01 二拍「子流程暂不进
# 契约面」（spec/02 义务 3 段），六栈不补这一档。设计器画出子流程节点时，本栈就靠下面那条
# 未知档日志把它**显式暴露**出来——那条裁定唯一的可诊断面就是这条日志（见 engine._execute_node）。
KNOWN_NODE_TYPES = frozenset({TYPE_START, TYPE_END, TYPE_TASK, TYPE_DECISION, TYPE_FORK,
                              TYPE_JOIN, TYPE_CUSTOM})

# ─── Domain Types ──────────────────────────────────────────────────────────────

class InstanceState(IntEnum):
    DOING     = 10
    DONE      = 20
    WITHDRAW  = 30
    INTERRUPT = 40
    REJECT    = 45
    PENDING   = 50
    ABANDON   = 99

class TaskState(IntEnum):
    DOING     = 10
    DONE      = 20
    WITHDRAW  = 30
    INTERRUPT = 40
    PENDING   = 50
    ABANDONED = 99

# ─── 引擎内部错误（对齐 Java enums/WfErrEnum 的 code + message 形状）─────────────
#
# 本栈既有形状＝抛 ValueError(固定中文文案)，**内部码只进注释/文档不进 msg**（issues/121 口径）：
# 门面 flow() 捕获后出 {"code": 99999999, "msg": 这句原文}，不拼码、不加前缀。
# 参照 20010007 / 20010008（engine._rollback_to_parent 的 NO_LINEAGE / GUARD 两句）。
#
# 20010009 WITHDRAW_INSTANCE_NOT_DOING —— 撤回的实例状态守卫（issues/134 案 A），
# 落点见下方 ProcessInstance.withdraw。
ERR_WITHDRAW_INSTANCE_NOT_DOING = "流程实例非进行中，无法撤回"

# ─── 字典枚举（v1.4.0，对齐 Java enums，值与 boot3 字典一致） ────────────────

class DefineState(IntEnum):
    """流程定义状态（wf_process_define_state）"""
    DISABLE = 0
    ENABLE  = 1

class SubmitType(IntEnum):
    """流程提交类型（wf_process_submit_type）"""
    APPLY                = 0
    AGREE                = 1
    REJECT               = 2
    ROLLBACK             = 3
    JUMP                 = 4
    RE_APPLY             = 5
    ROLLBACK_TO_OPERATOR = 6
    TRANSFER             = 7   # issues/115：转办（processTask/transfer 留痕，不走 execute）
    COUNTERSIGN_DISAGREE = 20

class TaskType(IntEnum):
    """任务类型（wf_process_task_type）"""
    MAJOR     = 0
    SECONDARY = 1
    RECORD    = 2

class PerformType(IntEnum):
    """任务参与方式（wf_process_task_perform_type）"""
    NORMAL     = 0
    COUNTERSIGN = 1

class CountersignType(IntEnum):
    """会签类型（wf_countersign_type）"""
    PARALLEL   = 0
    SEQUENTIAL = 1

@dataclass
class ProcessDefine:
    id: int = 0
    name: str = ""
    displayName: str = ""
    type: str = ""
    state: int = 1
    content: str = ""
    version: int = 1
    createTime: Any = None
    createUser: str = ""
    updateTime: Any = None
    updateUser: str = ""

@dataclass
class ProcessInstance:
    id: int = 0
    defineId: int = 0
    state: InstanceState = InstanceState.DOING
    operator: str = ""
    parentId: Optional[int] = None
    parentNodeName: str = ""
    businessNo: str = ""
    expireTime: Any = None
    variables: dict[str, Any] = field(default_factory=dict)
    tasks: list["ProcessTask"] = field(default_factory=list, repr=False)
    createTime: Any = None
    createUser: str = ""
    updateTime: Any = None
    updateUser: str = ""

    # ── 聚合根行为（对标 Java domain/ProcessInstance）──

    def complete_task(self, task: "ProcessTask", operator: str, vars_: dict, now) -> None:
        """完成任务（子实体状态转换 + 实例变量合并）"""
        task.finish(operator, vars_, now)
        self.variables = vars_
        self.updateTime = now
        self.updateUser = operator

    def abandon_task(self, task: "ProcessTask", now) -> None:
        """废弃单个任务"""
        task.abandon(now)
        self.updateTime = now

    def abandon_all_doing(self, now) -> list["ProcessTask"]:
        """废弃所有进行中任务，返回被废弃列表"""
        abandoned = []
        for t in self.tasks:
            if t.is_doing():
                t.abandon(now)
                abandoned.append(t)
        self.updateTime = now
        return abandoned

    def finish(self, now) -> None:
        """流程完成"""
        self.state = InstanceState.DONE
        self.updateTime = now

    def withdraw(self, now) -> None:
        """撤回流程（issues/53 E25：withdraw 用 Withdraw(30)，与 reject 区分）

        issues/134 案 A：撤回只允许**进行中(10)** 的实例。实例不是 10（已完成 20 / 已撤回 30 /
        强行终止 40 / 已拒绝 45 / 挂起 50 / 已废弃 99）⇒ 抛内部码 20010009，**一行都不改、不落库**，
        否则已办结实例会被静默改写成 30（改历史、且用户看不到任何报错）。
        守卫排在调用方的任务行循环之前（门面已把本调用上提），任务行层面那句
        "已完成(20)/已终止(40) 行不改写"的既有保护保持原样。

        Raises:
            ValueError: 20010009 实例非进行中（出口 99999999 + 固定文案，文案不含内部码）
        """
        if self.state != InstanceState.DOING:
            raise ValueError(ERR_WITHDRAW_INSTANCE_NOT_DOING)
        self.state = InstanceState.WITHDRAW
        self.updateTime = now

    def reject(self, now) -> None:
        """驳回流程"""
        self.state = InstanceState.REJECT
        self.updateTime = now

    def add_variable(self, vars_: dict) -> None:
        """追加变量"""
        self.variables.update(vars_)

    def get_doing_tasks(self) -> list["ProcessTask"]:
        return [t for t in self.tasks if t.is_doing()]

    def get_done_tasks(self) -> list["ProcessTask"]:
        return [t for t in self.tasks if t.is_finished()]

    def is_all_tasks_finished(self) -> bool:
        return not any(t.is_doing() for t in self.tasks)

    def create_task(self, task_id: int, task_name: str, display_name: str, actor: str,
                    operator: str, form_key: str, now,
                    parent_task_id: int, is_first_task_node: bool,
                    perform_type: int = 0) -> "ProcessTask":
        """创建任务（子实体工厂）——perform_type：0 普通 / 1 会签（issues/52 E24 落库对齐 Java）

        建单不变量（issues/121 P1）：必写 parentTaskId（发起 execution 无当前任务⇒0）
        与行级 isFirstTaskNode。二者无默认值，漏传即 TypeError，不留静默路径。

        ``actor=""`` ⇒ **空参与者集合**（issues/142 §5.3 / spec 02 §6.1 硬结论 1：任务类零参与者
        照样建单但行上不挂人）。这里不收空串是刻意的：``[""]`` 会往 ``wf_process_task_actor``
        灌一条空 ``actor_id`` 归属值，正是 issues/142 B 表点名那族垃圾形状（五种空值形态之一），
        而"零参与者"要的是**一条 actor 行都不写**。"""
        task = ProcessTask(id=task_id, processInstanceId=self.id,
                           taskName=task_name, displayName=display_name,
                           taskState=TaskState.DOING, actorIds=[actor] if actor else [],
                           formKey=form_key, performType=perform_type,
                           parentTaskId=parent_task_id,
                           createTime=now, updateTime=now,
                           createUser=operator, updateUser=operator)
        task.variables["isFirstTaskNode"] = is_first_task_node
        self.tasks.append(task)
        return task

    def create_history_task(self, task_id: int, task_name: str, display_name: str,
                            operator: str, now, parent_task_id: int,
                            is_first_task_node: bool) -> "ProcessTask":
        """创建**历史/已完成**任务行（记录类节点专用，issues/141 G9 · spec 02-flow-definition.md §6.1）。

        形状基准＝jeeflow-java ``ProcessInstance.createHistoryTask(CustomModel, operator, ...)``
        （domain/ProcessInstance.java:425）：``ProcessTask.create(...)`` ＋ ``setTaskState(FINISHED)``
        ——即 **task_state=20**、参与者＝当前操作人（留痕主体，**不是待办**）、无 form、
        无 expireTime、会签字段 0；建单不变量（parentTaskId ＋ 行级 isFirstTaskNode）同样适用
        （issues/121 P1，java 那边是同一句注释）。

        ⚠️ 这一支存在的理由（owner 2026-09-29 裁定，原话「这个得根据任务类型来，自定义类型这种
        记录类的，不会有参与人，是正常行为」）：记录类节点**没有参与者是正常形态**，既不许按
        任务类建 DOING 行，也不许"兜底把行挂给当前操作人"伪造一条他不该收到的待办，更不许
        直接跳过节点丢留痕。正确形状只有这里这一种：落一条 DONE 行、令牌继续流转。
        任务类节点仍走 ``create_task``（DOING）。"""
        task = ProcessTask(id=task_id, processInstanceId=self.id,
                           taskName=task_name, displayName=display_name,
                           taskState=TaskState.DONE,
                           actorIds=[operator] if operator else [], actorId=operator,
                           formKey="", performType=0, parentTaskId=parent_task_id,
                           finishTime=now, createTime=now, updateTime=now,
                           createUser=operator, updateUser=operator)
        task.variables["isFirstTaskNode"] = is_first_task_node
        self.tasks.append(task)
        return task


@dataclass
class ProcessTask:
    id: int = 0
    processInstanceId: int = 0
    taskName: str = ""
    displayName: str = ""
    taskType: int = 0
    performType: int = 0
    taskState: TaskState = TaskState.DOING
    actorId: str = ""
    actorIds: list[str] = field(default_factory=list)
    finishTime: Any = None
    expireTime: Any = None
    formKey: str = ""
    parentTaskId: Optional[int] = None
    variables: dict[str, Any] = field(default_factory=dict)
    createTime: Any = None
    createUser: str = ""
    updateTime: Any = None
    updateUser: str = ""

    # ── 子实体行为（对标 Java domain/ProcessTask）──

    def finish(self, operator: str, vars_: dict, now) -> None:
        """完成任务"""
        self.taskState = TaskState.DONE
        self.actorId = operator
        self.finishTime = now
        self.updateTime = now
        self.updateUser = operator
        self.variables = vars_

    def abandon(self, now) -> None:
        """废弃任务"""
        self.taskState = TaskState.ABANDONED
        self.updateTime = now

    def withdraw(self, now) -> None:
        """随实例撤回任务（区别于 abandon：撤回是发起人主动收回，废弃是引擎清理）"""
        self.taskState = TaskState.WITHDRAW
        self.updateTime = now

    def is_doing(self) -> bool:
        return self.taskState == TaskState.DOING

    def is_finished(self) -> bool:
        return self.taskState == TaskState.DONE

    def is_allowed(self, operator: str) -> bool:
        """操作人是否有权限处理"""
        return operator in self.actorIds

@dataclass
class ProcessDesign:
    """流程设计（v1.1.0，wf_process_design）——设计器保存的设计稿元信息"""
    id: int = 0
    name: str = ""
    displayName: str = ""
    type: str = "approval"
    icon: str = ""
    isDeployed: int = 0
    remark: str = ""
    createTime: Any = None
    createUser: str = ""
    updateTime: Any = None
    updateUser: str = ""

@dataclass
class ProcessDesignHis:
    """流程设计历史（v1.1.0，wf_process_design_his）——每次保存的 content 快照"""
    id: int = 0
    processDesignId: int = 0
    content: str = ""
    createTime: Any = None
    createUser: str = ""

@dataclass
class ProcessSurrogate:
    """流程委托代理（v1.1.0，wf_process_surrogate）——授权人把待办委托给代理人

    生效规则见 ``is_effective``（四判据）：``enabled`` 严格只认 1、被委托人非空且非授权人本人、
    时间窗覆盖判定时刻（起止为空 = 该侧不限）；``processName`` 为空 = 全部流程。
    多条并存时由仓储按主键 id **取最新一条再交本方法裁决**
    （规范 06 §4.5 条款 1.4；不得"先滤生效再取最新"，见 issues/123）。
    """
    id: int = 0
    processName: str = ""
    operator: str = ""
    surrogate: str = ""
    startTime: Any = None
    endTime: Any = None
    enabled: int = 1
    createTime: Any = None
    createUser: str = ""
    updateTime: Any = None
    updateUser: str = ""

    def is_effective(self, operator: Optional[str], at: Any = None) -> bool:
        """四判据（规范 06 §4.5 条款 3/4 ＋ issues/123 §1）：本条委托此刻对该授权人是否生效。

        调用方必须先按 id 选出「该作用域内最新的一条」再问本方法——本方法只裁决单条，不做多条择优。
        SQL 仓与内存仓必须走同一份判据（08-compliance 用例 27 要求双仓同答案）。

        :param operator: 授权人（判自委托：被委托人等于授权人 ⇒ 不新增、不重复）
        :param at: 判定时刻；传 ``None`` 表示不做窗口比较（引擎建单路径恒有值）。
            时刻基准与本栈写入侧同一把尺子（``datetime.now()`` 宿主本地时区的 naive 时间，
            规范 06 §4.5 条款 5 / issues/120），不得拿 UTC 去比库里的本地时间戳。
        """
        if not surrogate_enabled_on(self.enabled):        # 只认 1；0 / 2 / None / 脏值一律不生效
            return False
        agent = (self.surrogate or "").strip()
        if not agent:                                     # 被委托人为空/纯空白 ⇒ 不生效
            return False
        if operator is not None and agent == operator:     # 自委托：不新增、不重复
            return False
        time = to_datetime(at)
        if time is None:                                  # 不传判定时刻 ⇒ 不比窗
            return True
        start, end = to_datetime(self.startTime), to_datetime(self.endTime)
        if start is not None and start > time:            # 未到窗
            return False
        return end is None or end >= time                 # 起止为 NULL = 该侧不限

@dataclass
class UserInfo:
    userId: str = ""
    realName: str = ""
    deptId: Optional[str] = None
    deptName: Optional[str] = None
    postId: Optional[str] = None
    postName: Optional[str] = None


@dataclass
class CcInstanceRow:
    """抄送实例行数据（ccList 分页，v1.3.0，对齐 Java InstanceRow）"""
    id: int = 0
    parentId: Optional[int] = None
    defineId: int = 0
    state: InstanceState = InstanceState.DOING
    parentNodeName: str = ""
    businessNo: str = ""
    operator: str = ""
    expireTime: Any = None
    variables: dict = field(default_factory=dict)
    createTime: Any = None
    createUser: str = ""
    updateTime: Any = None
    updateUser: str = ""
    defineName: str = ""
    defineDisplayName: str = ""
    defineVersion: int = 0


# ─── JSON Parsing ────────────────────────────────────────────────────────────────

def _pick(d: dict, *keys: str) -> dict:
    """从 dict 中只取特定 key"""
    return {k: v for k, v in d.items() if k in keys}


_KNOWN_MODEL = {"name", "displayName", "type", "nodes", "edges"}
_KNOWN_NODE = {"id", "type", "x", "y", "properties", "text"}
_KNOWN_EDGE = {"id", "sourceNodeId", "targetNodeId", "properties", "text"}


def parse_flow_model(raw: dict) -> FlowModel:
    """从 JSON dict 解析 FlowModel，过滤未知字段"""
    nodes = [_parse_node(n) for n in raw.get("nodes", [])]
    edges = [_parse_edge(e) for e in raw.get("edges", [])]
    return FlowModel(
        name=raw.get("name", ""),
        displayName=raw.get("displayName", ""),
        type=raw.get("type", ""),
        nodes=nodes,
        edges=edges,
    )


def _parse_node(raw: dict) -> FlowNode:
    return FlowNode(
        id=raw.get("id", ""),
        type=raw.get("type", ""),
        x=float(raw.get("x", 0)),
        y=float(raw.get("y", 0)),
        properties=raw.get("properties", {}),
        text=raw.get("text", {}),
    )


def _parse_edge(raw: dict) -> FlowEdge:
    return FlowEdge(
        id=raw.get("id", ""),
        sourceNodeId=raw.get("sourceNodeId", ""),
        targetNodeId=raw.get("targetNodeId", ""),
        properties=raw.get("properties", {}),
        text=raw.get("text"),
    )


# ─── 核心表分页行数据（v1.5.0，对齐 Java DefineRow/InstanceRow/TaskRow） ─────

@dataclass
class DefineRow:
    """流程定义行数据（page_defines 分页）"""
    id: int = 0
    name: str = ""
    displayName: str = ""
    type: str = ""
    state: int = 1
    version: int = 1
    createTime: Any = None
    createUser: str = ""
    updateTime: Any = None
    updateUser: str = ""


@dataclass
class InstanceRow:
    """流程实例行数据（page_instances 分页）"""
    id: int = 0
    parentId: Optional[int] = None
    defineId: int = 0
    state: InstanceState = InstanceState.DOING
    parentNodeName: str = ""
    businessNo: str = ""
    operator: str = ""
    expireTime: Any = None
    variables: dict = field(default_factory=dict)
    createTime: Any = None
    createUser: str = ""
    updateTime: Any = None
    updateUser: str = ""
    defineName: str = ""
    defineDisplayName: str = ""
    defineVersion: int = 0


@dataclass
class TaskRow:
    """任务行数据（page_todo_tasks / page_done_tasks 分页）"""
    id: int = 0
    processInstanceId: int = 0
    taskName: str = ""
    displayName: str = ""
    taskType: int = 0
    performType: int = 0
    taskState: TaskState = TaskState.DOING
    operator: str = ""
    finishTime: Any = None
    expireTime: Any = None
    formKey: str = ""
    taskParentId: Optional[int] = None
    variables: dict = field(default_factory=dict)
    createTime: Any = None
    createUser: str = ""
    updateTime: Any = None
    updateUser: str = ""
    processDefineName: str = ""
    processDefineDisplayName: str = ""
    defineVersion: int = 0
    instanceVariable: str = ""
    instanceCreateTime: Any = None


# ─── 统计查询 DTO（v1.8.25，issues/103） ─────────────────────────────────────

@dataclass
class InstanceStatsRow:
    """实例统计查询行（query_instances_for_stats）"""
    defineId: int = 0
    state: int = 0
    operator: str = ""
    createTime: Any = None


@dataclass
class TaskStatsRow:
    """任务统计查询行（query_tasks_for_stats）"""
    operator: str = ""
    displayName: str = ""
    performType: int = 0
    createTime: Any = None
    finishTime: Any = None
    expireTime: Any = None
