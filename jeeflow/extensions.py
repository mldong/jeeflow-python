"""扩展体系——拦截器、事件、HandlerRegistry、委托代理运行期应用（issues/116）"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Union, Awaitable

from .surrogate import ExtRepositorySurrogateApplier, SurrogateApplier


class EventType(Enum):
    PROCESS_START = "PROCESS_START"
    PROCESS_FINISH = "PROCESS_FINISH"
    PROCESS_REJECT = "PROCESS_REJECT"
    TASK_CREATE = "TASK_CREATE"
    TASK_COMPLETE = "TASK_COMPLETE"
    # issues/102：抄送知会（对齐 Java CC_CREATE / Go EventCCCreate / Node CcCreate / PHP CC_CREATE）
    CC_CREATE = "CC_CREATE"


@dataclass
class ProcessEvent:
    type: EventType
    instanceId: int = 0
    taskId: int = 0
    taskName: str = ""
    operator: str = ""
    # 抄送人 id 直传事件体，监听器免反查 cc 表（issues/102；对齐 Java ccActorId / Go CcActorID）
    ccActorId: str = ""


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
    # ── 委托代理运行期自动生效（issues/116 批次 D：引擎内置、默认开启、可显式关闭）──
    # 委托查询数据源（可选 ProcessExtRepository）；None → 建任务时静默跳过，不得抛错打断建单
    ext_repository: Optional[Any] = None
    # 关闭路①（配置开关）：False → 引擎不应用委托，回到"仅台账"行为
    surrogate_enabled: bool = True
    # 关闭路②（注册空实现 NullSurrogateApplier）/ 自定义数据源：None → 用内置实现查 ext_repository
    surrogate_applier: Optional[SurrogateApplier] = None

    def resolve_surrogate_applier(self) -> Optional[SurrogateApplier]:
        """解析建任务时生效的委托应用器（issues/116 06 §4.5 条款 3/4）：

        - 开关关闭（``surrogate_enabled=False``）→ ``None``（静默跳过）
        - 显式注册实现（含 ``NullSurrogateApplier`` 空实现）→ 用它
        - 未注册但已接入扩展仓储 → 内置 ``ExtRepositorySurrogateApplier``
        - 未接入扩展仓储 → ``None``（缺仓储属正常部署形态，静默跳过）
        """
        if not self.surrogate_enabled:
            return None
        if self.surrogate_applier is not None:
            return self.surrogate_applier
        if self.ext_repository is None:
            return None
        return ExtRepositorySurrogateApplier(self.ext_repository)
