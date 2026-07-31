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
    DOING  = 10
    DONE   = 20
    REJECT = 45

class TaskState(IntEnum):
    DOING     = 10
    DONE      = 20
    ABANDONED = 99

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

@dataclass
class UserInfo:
    userId: str = ""
    realName: str = ""
    deptId: Optional[str] = None
    deptName: Optional[str] = None
    postId: Optional[str] = None
    postName: Optional[str] = None


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
