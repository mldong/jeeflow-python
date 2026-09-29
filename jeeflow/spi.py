"""SPI 接口——对标 SPEC.md §6"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass
class QueryCondition:
    """查询条件（issues/05-5：m_ 前缀参数解析产物，对齐 Java PageQuery.Condition）"""
    column: str
    operator: str
    value: Any
from .model import (ProcessDefine, ProcessInstance, ProcessTask, ProcessDesign, ProcessDesignHis, ProcessSurrogate, UserInfo, CcInstanceRow, DefineRow, InstanceRow, TaskRow, InstanceStatsRow, TaskStatsRow)

class ProcessRepository(ABC):
    @abstractmethod
    async def find_define_by_id(self, id: int) -> Optional[ProcessDefine]: ...
    @abstractmethod
    async def find_define_by_name(self, name: str) -> Optional[ProcessDefine]:
        """按流程编码查最新一条定义（v1.1.0，Facade deploy 版本管理用）"""
        ...
    # 定义写操作（v1.0.1，集成反馈①）：保存/更新/启停/删除流程定义
    @abstractmethod
    async def save_define(self, define: ProcessDefine) -> None: ...
    @abstractmethod
    async def update_define(self, define: ProcessDefine) -> None: ...
    @abstractmethod
    async def update_define_state(self, define_id: int, state: int) -> None: ...
    @abstractmethod
    async def remove_define(self, define_id: int) -> None: ...
    @abstractmethod
    async def find_instance_by_id(self, id: int) -> Optional[ProcessInstance]: ...
    @abstractmethod
    async def save_instance(self, inst: ProcessInstance) -> None: ...
    @abstractmethod
    async def update_instance(self, inst: ProcessInstance) -> None: ...
    @abstractmethod
    async def find_task_by_id(self, task_id: int) -> Optional[ProcessTask]: ...
    @abstractmethod
    async def save_task(self, task: ProcessTask) -> None: ...
    @abstractmethod
    async def update_task(self, task: ProcessTask) -> None: ...
    @abstractmethod
    async def find_doing_tasks(self, instance_id: int, task_names: Optional[list[str]] = None) -> list[ProcessTask]: ...
    @abstractmethod
    async def find_done_tasks(self, instance_id: int, task_names: Optional[list[str]] = None) -> list[ProcessTask]: ...
    @abstractmethod
    async def find_history_tasks(self, instance_id: int) -> list[ProcessTask]: ...
    @abstractmethod
    async def find_task_actors(self, task_id: int) -> list[str]: ...
    @abstractmethod
    async def add_task_actor(self, task_id: int, actors: list[str]) -> None: ...
    @abstractmethod
    async def remove_task_actor(self, task_id: int, actors: list[str]) -> None: ...
    @abstractmethod
    async def create_cc_instance(self, instance_id: int, creator: str, *actor_ids: str) -> None:
        """落 cc 行（**写侧判重＝幂等空操作**，issues/141 G2 · spec 06-facade.md §4）。

        同一 `(instance_id, actor_id)` **已存在 cc 行时直接跳过**：①不新增行 ②不重置未读状态
        （`state` 保持原值）③不更新原行时间（`create_time`/`update_time` 逐字不变）。
        判重放在**写侧**而不是查询侧——`page_cc_instances` 不引入 `DISTINCT`、历史重复行也不清理
        （owner 2026-09-29 拍：接受既成事实，写侧判重只保证今后不再新增）。
        建 cc 的三条入口（发起 `f_ccActors`／办理 `tf_ccActors`／手动 `createCCInstance`）都经由
        引擎的 `handle_cc_actors` 漏斗，那里调的是 `create_cc_instance_if_absent`
        ——需要"实际新建了谁"拿去 fire `CC_CREATE`（码 4）。"""
        ...
    @abstractmethod
    async def update_cc_status(self, instance_id: int, actor_id: str) -> None: ...

    async def find_cc_actor_ids(self, instance_id: int) -> list[str]:
        """某实例**已存在**的 cc 行 actor id（issues/141 G2 写侧判重的读侧）。

        default 返回空集＝**不判重**：未覆写的第三方仓储维持旧行为（全量建行、全量 fire），
        SPI 源码兼容不破。jeeflow 自带的两仓（SQL 仓 `JdbcRepository` / 内存仓
        `MemoryRepository`）**必须**覆写——否则「同一栈 SQL 仓与内存仓两个答案」
        （issues/117 场景 27 那把尺子）在写侧重演一遍。
        """
        return []

    async def create_cc_instance_if_absent(self, instance_id: int, creator: str,
                                           actor_ids: list[str]) -> list[str]:
        """写侧幂等建 cc 行，返回**实际新建**的 actor 子集（issues/141 G2 · spec 06 §4）。

        判据：`actor_ids` 里已在该实例有 cc 行的跳过、同一次调用内的重复也折叠（顺序与入参一致），
        剩下的子集交给 `create_cc_instance` 落库。

        为什么返回子集而不是 None：spec 11-events §11.2 原则 1「码值表达发生了什么事实」
        ⇒ 没发生"创建"就**不得** fire `CC_CREATE`（码 4）。三条入口一律拿这个子集去 fire，
        **子集为空整支不发**（不空转，也不照旧按原始请求全量 fire）。

        未覆写 `find_cc_actor_ids` 的第三方仓储走本 default ⇒ 与旧
        `create_cc_instance(全量)` 逐字一致（子集＝入参去重后全量），不静默改变既有集成方行为。
        """
        existing = set(await self.find_cc_actor_ids(instance_id) or [])
        fresh: list[str] = []
        for actor_id in actor_ids or []:
            if actor_id is None or actor_id in existing or actor_id in fresh:
                continue
            fresh.append(actor_id)
        if fresh:
            await self.create_cc_instance(instance_id, creator, *fresh)
        return fresh

    @abstractmethod
    async def page_cc_instances(self, page_num: int = 1, page_size: int = 10,
                                actor_id: Optional[str] = None,
                                conditions: Optional[list[QueryCondition]] = None) -> tuple[list[CcInstanceRow], int]:
        """我的抄送分页（v1.3.0，对齐 Java pageCcInstances）：按抄送人 actor_id 过滤实例列表。

        **归属条件必填**（issues/141 G1 · spec 06-facade.md §2.5「抄送分页同一条尺子」）：
        查询必须带归属列 `cc.actor_id` 的**有效**条件——`actor_id` 入参，或 `conditions` 里
        某一条件 `column == "cc.actor_id"`；有效＝值非 `None`、`str` 型 strip 后非空、集合非空。
        **没有有效归属条件时返回空页**（`[], 0`），严禁退化成"这条条件不加"而放出全部实例。
        这条义务同时钉在 SQL 仓与内存仓上：**同一份数据两仓必须给同一个答案**（issues/117 场景 27）。
        门面 `processInstance/ccList` 恒挂 `cc.actor_id EQ operator`，这里防的是绕过门面
        直连仓储的调用方（与下一版门面的漏挂）。
        """
        ...

    # ── 统计查询（v1.8.25，issues/103） ──

    @abstractmethod
    async def query_instances_for_stats(self, state_in: list[int], order_by: str = "create_time",
                                        start: Optional[datetime] = None,
                                        end: Optional[datetime] = None) -> list[InstanceStatsRow]:
        """统计用实例查询：按 state IN + create_time 范围"""
        ...

    @abstractmethod
    async def query_tasks_for_stats(self, task_state: Optional[int] = None,
                                    start: Optional[datetime] = None,
                                    end: Optional[datetime] = None) -> list[TaskStatsRow]:
        """统计用任务查询：按 task_state + finish_time 范围"""
        ...

    @abstractmethod
    async def stats_pending_and_overdue_count(self) -> tuple[int, int]:
        """待办数 + 超期数（task_state=10）"""
        ...

    @abstractmethod
    async def stats_completed_task_aggregate(self) -> tuple[int, int, int, int]:
        """已完成任务聚合：(total, countersign, on_time, on_time_denom)"""
        ...

    @abstractmethod
    async def stats_avg_completed_duration_seconds(self, start: Optional[datetime] = None,
                                                   end: Optional[datetime] = None) -> int:
        """已完成实例平均耗时（秒）"""
        ...

    @abstractmethod
    async def stats_define_group(self, start: Optional[datetime] = None,
                                 end: Optional[datetime] = None,
                                 limit: int = 10) -> list[dict]:
        """按流程定义分组（join define，含 avgDurationSeconds）"""
        ...

    @abstractmethod
    async def stats_stuck_node_group(self, limit: int = 10) -> list[dict]:
        """卡点节点分组（task_state=10，实时快照）"""
        ...

    @abstractmethod
    async def stats_stuck_approver_group(self, limit: int = 10) -> list[dict]:
        """卡点审批人分组（task_actor join task_state=10，实时快照）"""
        ...

    @abstractmethod
    async def stats_completed_instance_durations(self, start: Optional[datetime] = None,
                                                 end: Optional[datetime] = None) -> list[int]:
        """已完成实例耗时列表（秒），用于 durationBucket 分组"""
        ...

    # ── 核心表分页（v1.5.0，对齐 Java pageDefines/pageInstances/pageTodoTasks/pageDoneTasks）──

    @abstractmethod
    async def page_defines(self, page_num: int = 1, page_size: int = 10,
                           conditions: Optional[list[QueryCondition]] = None) -> tuple[list[DefineRow], int]:
        """流程定义分页"""
        ...
    @abstractmethod
    async def page_instances(self, page_num: int = 1, page_size: int = 10,
                             operator: Optional[str] = None,
                             conditions: Optional[list[QueryCondition]] = None) -> tuple[list[InstanceRow], int]:
        """我发起的流程实例分页（operator 过滤）"""
        ...
    @abstractmethod
    async def page_todo_tasks(self, page_num: int = 1, page_size: int = 10,
                              actor_id: Optional[str] = None,
                              conditions: Optional[list[QueryCondition]] = None) -> tuple[list[TaskRow], int]:
        """我的待办分页（actor_id 过滤，仅进行中任务）"""
        ...
    @abstractmethod
    async def page_done_tasks(self, page_num: int = 1, page_size: int = 10,
                              operator: Optional[str] = None,
                              conditions: Optional[list[QueryCondition]] = None) -> tuple[list[TaskRow], int]:
        """我的已办分页（operator 过滤，非进行中任务）"""
        ...

class UserProvider(ABC):
    @abstractmethod
    async def get_user(self, user_id: str) -> Optional[UserInfo]: ...

class OrgUserProvider(ABC):
    """组织维度用户提供者（issues/16）——部门领导 / 部门分管领导 / 角色成员。

    通用业务语义，业务方只实现数据接口，不写 AssignmentHandler。
    """

    @abstractmethod
    async def find_dept_leaders(self, dept_id: str) -> list[str]:
        """部门领导（deptId → 领导 userId 列表）"""
        ...

    @abstractmethod
    async def find_dept_main_leaders(self, dept_id: str) -> list[str]:
        """部门分管领导（deptId → 分管领导 userId 列表）"""
        ...

    @abstractmethod
    async def find_by_role(self, role_code: str) -> list[str]:
        """按角色取人（roleCode → userId 列表）"""
        ...

class IDGenerator(ABC):
    @abstractmethod
    def next_id(self) -> int: ...

class ExpressionEvaluator(ABC):
    @abstractmethod
    async def eval(self, expr: str, vars: dict[str, Any]) -> Any: ...

class ProcessExtRepository(ABC):
    """扩展仓储 SPI（v1.1.0，可选）——流程设计 / 设计历史 / 委托代理

    引擎核心不依赖本接口；门面（Facade）与委托参考实现使用。
    """

    # ── 流程设计（wf_process_design） ──
    @abstractmethod
    async def find_design_by_id(self, id: int) -> Optional[ProcessDesign]: ...
    @abstractmethod
    async def save_design(self, d: ProcessDesign) -> None: ...
    @abstractmethod
    async def update_design(self, d: ProcessDesign) -> None: ...
    @abstractmethod
    async def remove_design(self, id: int) -> None: ...
    @abstractmethod
    async def page_designs(self, page_num: int = 1, page_size: int = 10,
                           filters: Optional[dict] = None,
                           conditions: Optional[list[QueryCondition]] = None) -> tuple[list[ProcessDesign], int]: ...

    # ── 设计历史（wf_process_design_his） ──
    @abstractmethod
    async def save_design_his(self, his: ProcessDesignHis) -> None: ...
    @abstractmethod
    async def list_design_his(self, design_id: int) -> list[ProcessDesignHis]: ...

    # ── 委托代理（wf_process_surrogate） ──
    @abstractmethod
    async def find_surrogate_by_id(self, id: int) -> Optional[ProcessSurrogate]: ...
    @abstractmethod
    async def save_surrogate(self, s: ProcessSurrogate) -> None: ...
    @abstractmethod
    async def update_surrogate(self, s: ProcessSurrogate) -> None: ...
    @abstractmethod
    async def remove_surrogate(self, id: int) -> None: ...
    @abstractmethod
    async def page_surrogates(self, page_num: int = 1, page_size: int = 10,
                              filters: Optional[dict] = None,
                              conditions: Optional[list[QueryCondition]] = None) -> tuple[list[ProcessSurrogate], int]: ...

    # GetSurrogate 查询指定时间生效中的委托：各作用域（processName 精确优先，空值全流程兜底）
    # 先按 id 取**最新一条**，再交 ProcessSurrogate.is_effective 裁决这一条（06 §4.5 条款 1.4；
    # 不得"先按 enabled/时间窗/自委托过滤、再从剩下的取最新"——见 issues/123）
    # 判据④ enabled **只认整数 1**（issues/130 案 A，对齐 Java Integer.valueOf(1).equals(enabled)）：
    # '1' / 1.0 / True 这类等价写法与 0 / 2 / 脏值 / None 一律不生效。整数列被驱动回读成字符串
    # 要在**实现侧**装行处先还原（内置 SQL 仓走 surrogate.hydrate_enabled，只认规范整数串），
    # 引擎读侧不再做宽松转换；自定义 SPI 仓储传非整数即按停用。
    @abstractmethod
    async def get_surrogate(self, operator: str, process_name: str, at=None) -> Optional[ProcessSurrogate]: ...
