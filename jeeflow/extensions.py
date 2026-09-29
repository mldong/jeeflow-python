"""扩展体系——拦截器、事件、HandlerRegistry、委托代理运行期应用（issues/116）"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Callable, Optional, Union, Awaitable

from .surrogate import ExtRepositorySurrogateApplier, SurrogateApplier


# ─── 事件码表（spec 11-events §11.3 · A 套整型）───────────────────────────────────

class EventType(IntEnum):
    """流程事件码——**规范名是权威，整型码值只是本栈内部附带数值**（spec 11-events §11.3）。

    issues/132：本栈原为字符串名（C 套 ``CC_CREATE="CC_CREATE"``），现整型化到 A 套 1..9，
    与 Java/PHP/Rust/MoonBit/C# 同码。集成层跨语言判据**一律用规范名**
    （``evt.type is EventType.CC_CREATE`` / ``evt.name``），严禁拿数字码当判据。

    码 5..9 是本轮新增（spec §11.2 原则 2「码粗、载荷细」：号段只随**事实类别**增长，
    拒绝/跳转/退发起人共用 ``TASK_REJECT`` 靠载荷 ``submitType`` 分）。
    **一个号一旦发出去不许改语义、不许复用**；10+ 预留（超时催办等，本轮不发）。
    """

    PROCESS_INSTANCE_START = 1   # 实例发起成功（sourceId＝instanceId）
    PROCESS_INSTANCE_END = 2     # 实例进入终态（办结/拒绝共用，靠载荷 state 分）
    PROCESS_TASK_START = 3       # 新待办生成（含会签逐人、回退复活行）
    CC_CREATE = 4                # 新增一条抄送记录（逐抄送人 fire 一次）
    TASK_COMPLETE = 5            # 任务被办掉（同意/跳转/会签办理）
    TASK_REJECT = 6              # 任务被退回/拒绝（含退发起人、软拒绝、跳转回退）
    TASK_TRANSFER = 7            # 转办发生
    TASK_WITHDRAW = 8            # 撤回发生（实例进入 30）
    INSTANCE_TERMINATED = 9      # 实例被终止（40）

    # ── 旧名兼容别名（issues/132 C 套迁移；同码即同物，别名成员与规范名指向同一个枚举成员）──
    # 旧 PROCESS_START→1 / PROCESS_FINISH·PROCESS_REJECT→2（spec §11.6：办结与拒绝合并成
    # 「实例进入终态」一支，靠载荷 state 分）/ TASK_CREATE→3。别名不参与 list(EventType)。
    PROCESS_START = 1
    PROCESS_FINISH = 2
    PROCESS_REJECT = 2
    TASK_CREATE = 3


#: 走 task 维度（sourceId＝taskId）的码（spec §11.3「sourceId 指向」列）
_TASK_SCOPED_CODES = frozenset({3, 5, 6, 7})


@dataclass
class ProcessEvent:
    """事件体——载荷键一律 camelCase（spec 11-events §11.3）。

    ``type`` 之外的字段是**直传载荷的承载位**：老字段（instanceId/taskId/taskName/operator/
    ccActorId）保持原位不动（位置参数构造不破），新增字段一律排在其后。
    ``data`` 属性按码值给出「必备键」视图，监听器无需再猜各栈形状。
    """
    type: EventType
    instanceId: int = 0
    taskId: int = 0
    taskName: str = ""
    operator: str = ""
    # 抄送人 id 直传事件体，监听器免反查 cc 表（issues/102；对齐 Java ccActorId / Go CcActorID）
    ccActorId: str = ""
    # ── spec §11.3 载荷扩展（本轮新增）──────────────────────────────────────────
    defineId: int = 0
    #: 码 2：实例**落库后**的状态整数（20/30/40/45/50/99）
    state: Optional[int] = None
    #: 码 5/6：本次动作的 submitType（拒绝/跳转/退发起人靠它分，见 §11.2 原则 2）
    submitType: Optional[int] = None
    #: 码 3：该待办的参与者列表
    actors: list[str] = field(default_factory=list)
    #: 码 7：转办双方
    fromActor: str = ""
    toActor: str = ""
    #: 码 9：终止原因
    reason: str = ""

    @property
    def name(self) -> str:
        """规范名（跨语言判据用它，不用 code）"""
        return self.type.name

    @property
    def code(self) -> int:
        """A 套整型码值（本栈内部附带数值）"""
        return int(self.type)

    @property
    def sourceId(self) -> int:
        """spec §11.3「sourceId 指向」列：任务类事件指 taskId，其余指 instanceId"""
        return self.taskId if self.code in _TASK_SCOPED_CODES else self.instanceId

    @property
    def data(self) -> dict[str, Any]:
        """按码值给出直传载荷（camelCase；必备键恒在，额外键允许缺省即不给）"""
        d: dict[str, Any] = {"instanceId": self.instanceId}
        code = self.code
        if code == 1:
            d["operator"] = self.operator
            if self.defineId:
                d["defineId"] = self.defineId
        elif code == 2:
            d["state"] = self.state
        elif code == 3:
            d["taskId"] = self.taskId
            d["actors"] = list(self.actors)
        elif code == 4:
            d["ccActorId"] = self.ccActorId
        elif code in (5, 6):
            d["taskId"] = self.taskId
            d["operator"] = self.operator
            d["submitType"] = self.submitType
        elif code == 7:
            d["taskId"] = self.taskId
            d["fromActor"] = self.fromActor
            d["toActor"] = self.toActor
            d["operator"] = self.operator
        elif code == 8:
            d["operator"] = self.operator
        elif code == 9:
            d["operator"] = self.operator
            d["reason"] = self.reason
        if code in (3, 5, 6, 7) and self.taskName:
            d["taskName"] = self.taskName
        return d


# ─── 事件监听器（spec 11-events §11.5 订阅形状）───────────────────────────────────

#: 事件监听器：收 ProcessEvent，同步或异步皆可（spec §11.5「注册顺序＝回调顺序」）
ProcessEventListener = Callable[[ProcessEvent], Union[None, Awaitable[None]]]


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
    # ── 订阅形状（spec 11-events §11.5）：监听器**列表**是基线，一次 fire 送达到全部监听器 ──
    # issues/132 收口前本栈只有 event_listener 单回调（后注册覆盖前注册，三壳只能各自塞一个
    # 自派发回调）。现 event_listeners 为权威容器；event_listener 保留为**旧形状兼容位**：
    # 解析时排在列表最前（视作"更早注册"的那一支），同一 callable 挂两处只调一次。
    event_listeners: list[ProcessEventListener] = field(default_factory=list)
    event_listener: Optional[ProcessEventListener] = None
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

    # ── 监听器注册/解析（spec 11-events §11.5）──────────────────────────────────

    def add_event_listener(self, listener: ProcessEventListener) -> "EngineExtensions":
        """追加一个监听器（**不覆盖**已注册的任何一个）。None 忽略；重复注册同一 callable 无效。"""
        if listener is None:
            return self
        if listener not in self.event_listeners:
            self.event_listeners.append(listener)
        return self

    def resolve_event_listeners(self) -> list[ProcessEventListener]:
        """fire 时生效的监听器序列（注册顺序＝回调顺序，spec §11.5「一次 fire 送达全部监听器」）。

        旧形状兼容：``event_listener`` 单回调排在最前（视作更早注册的那一支），
        与 ``event_listeners`` 里的同一个 callable 只算一次，不会重复消费。
        """
        out: list[ProcessEventListener] = []
        if self.event_listener is not None:
            out.append(self.event_listener)
        for listener in self.event_listeners:
            if listener is not None and listener not in out:
                out.append(listener)
        return out
