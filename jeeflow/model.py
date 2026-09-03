"""域类型——对标 Java domain + model 包"""
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Optional

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
        """撤回流程（issues/53 E25：withdraw 用 Withdraw(30)，与 reject 区分）"""
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
                    operator: str, form_key: str, now, perform_type: int = 0) -> "ProcessTask":
        """创建任务（子实体工厂）——perform_type：0 普通 / 1 会签（issues/52 E24 落库对齐 Java）"""
        task = ProcessTask(id=task_id, processInstanceId=self.id,
                           taskName=task_name, displayName=display_name,
                           taskState=TaskState.DOING, actorIds=[actor],
                           formKey=form_key, performType=perform_type,
                           createTime=now, updateTime=now,
                           createUser=operator, updateUser=operator)
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
    """流程委托代理（v1.1.0，wf_process_surrogate）——授权人把待办委托给代理人"""
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
