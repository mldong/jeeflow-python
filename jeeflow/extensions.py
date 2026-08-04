"""扩展体系——拦截器、事件、HandlerRegistry"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Union, Awaitable


class EventType(Enum):
    PROCESS_START = "PROCESS_START"
    PROCESS_FINISH = "PROCESS_FINISH"
    PROCESS_REJECT = "PROCESS_REJECT"
    TASK_CREATE = "TASK_CREATE"
    TASK_COMPLETE = "TASK_COMPLETE"


@dataclass
class ProcessEvent:
    type: EventType
    instanceId: int = 0
    taskId: int = 0
    taskName: str = ""
    operator: str = ""


# ─── Interceptor ─────────────────────────────────────────────────────────────────

class FlowInterceptor(ABC):
    """流程拦截器"""
    @abstractmethod
    async def pre_handle(self, node, instance) -> bool: ...
    @abstractmethod
    async def post_handle(self, node, instance) -> None: ...
    @property
    def order(self) -> int: return 0


# ─── Assignment / Decision / Event Handler ───────────────────────────────────────

AssignmentHandler = Callable[[str, Any, Any], Union[list[str], Awaitable[list[str]]]]
"""assignmentHandler(hint: str, node, inst) -> list[str]"""

DecisionHandler = Callable[[str, Any, Any, dict], Union[str, Awaitable[str]]]
"""decisionHandler(hint: str, node, inst, vars) -> str (next node id)"""


class IAssignmentHandler(ABC):
    """可注册的参与者处理器（Registry 用）"""
    @abstractmethod
    async def assign(self, node, instance, operator: str) -> list[str]:
        """返回参与者列表（operator: 当前任务操作人，issues/16 对齐 Java Execution.getOperator）"""
        ...


class IDecisionHandler(ABC):
    """可注册的决策处理器（Registry 用）"""
    @abstractmethod
    async def decide(self, node, instance, vars: dict) -> str: ...


# ─── HandlerRegistry ─────────────────────────────────────────────────────────────

class HandlerRegistry:
    """仿 Spring IoC：按名称注册/解析处理器"""

    def __init__(self):
        self._assignments: dict[str, IAssignmentHandler] = {}
        self._decisions: dict[str, IDecisionHandler] = {}

    def register_assignment(self, name: str, handler: IAssignmentHandler):
        self._assignments[name] = handler

    def register_decision(self, name: str, handler: IDecisionHandler):
        self._decisions[name] = handler

    def resolve_assignment(self, name: str) -> Optional[IAssignmentHandler]:
        return self._assignments.get(name)

    def resolve_decision(self, name: str) -> Optional[IDecisionHandler]:
        return self._decisions.get(name)


# ─── EngineExtensions ────────────────────────────────────────────────────────────

@dataclass
class EngineExtensions:
    interceptors: list[FlowInterceptor] = field(default_factory=list)
    # 定义级拦截器注册表（issue 34）：名字 → 实例；流程定义顶层 postInterceptors 按名解析
    interceptor_registry: dict[str, FlowInterceptor] = field(default_factory=dict)
    assignment_handler: Optional[AssignmentHandler] = None
    decision_handler: Optional[DecisionHandler] = None
    event_listener: Optional[Callable[[ProcessEvent], Awaitable[None]]] = None
    registry: Optional[HandlerRegistry] = None
