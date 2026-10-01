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


def normalize_actors(raw) -> list[str]:
    """**归属值归一的唯一判据点**（issues/141 G10「空不创建行」spec 06-facade.md §2.10 ＋
    issues/142 B 批 spec 06-facade.md §2.11「归属值写侧归一」）——
    逐元素 ``str``＋trim，**空串与纯空白丢弃**，同一次调用内的重复折叠（顺序保持）。

    形状基准＝jeeflow-java ``StringUtils.normalizeCcActors``（commit ``5fbd5ac``），
    §2.11 把它逐字搬到任务侧。本仓把它放在 **spi 层**（与 ``ProcessRepository`` 同模块）而不是
    engine 层，理由和 java 放在 ``StringUtils`` 一样：这条判据要同时被**漏斗**
    （``engine.parse_cc_actors``／门面 ``addCandidate``/``surrogate``/``transfer``／
    引擎 ``_resolve_actors`` 的 nextNodeOperator 两支）和**写侧**（``create_cc_instance_if_absent``
    default、两仓 ``create_cc_instance``、两仓 ``add_task_actor``）吃到，而仓储实现不该反向依赖
    引擎模块——只修漏斗时，绕过门面/引擎直连仓储的调用方照样能把空归属值灌进 ``actor_id``
    （issues/129 那族"空 operator 读全库"的病根）。

    **改名而非另立**（§2.11 收尾那句「复用 §2.10 已落地的那一枚单点……不要再抄第二份」）：
    本函数原名 ``normalize_cc_actors``，只挂在抄送一支时任务侧的三条腿各自另长了一把尺子
    （python 实测：数组腿 ``[str(x) for x in v]`` 不 trim、不丢空、``None`` 串化成 ``"None"``）。
    ``normalize_cc_actors`` 保留为**同一个对象的别名**，cc 支继续走这一枚。

    判据要点：
    - **两形同判据在这一点上完成**：``raw`` 是逗号串时在这里拆成元素（与数组同一处过 trim/丢空/折叠，
      两形不可能分叉）；``list``/``tuple`` 逐元素；标量按单个值；``None``/空集合 ⇒ ``[]``；
    - **落库与比较一律取 trim 后的值**：``" 123 "`` 与 ``"123"`` 是同一个人，不 trim 就会与
      §4 的写侧判重错开，同一人落两行；
    - **``"0"`` 这类"看起来像空"的正常 id 不得丢掉**——只按 ``str(x).strip() == ""`` 判，
      **严禁 ``if not x`` 这种语言自带假值判据**（它会吃掉 ``'0'``；反向哨兵见
      tests/spec_test.py 的 §2.10／§2.11 两段）。
    """
    out: list[str] = []
    if raw is None:
        return out
    if isinstance(raw, str):
        items: Any = raw.split(",")        # 逗号串腿：拆串与 trim/丢空/折叠同一枚尺子
    elif isinstance(raw, (list, tuple)):
        items = raw                        # 数组腿
    else:
        items = [raw]                      # 标量（含数字 id）按单个归属值
    for item in items:
        if item is None:
            continue
        actor = str(item).strip()
        # 判空只用 strip 后是否为空串；``if not actor`` 会把 '0' 当空吃掉（spec §2.11 要求④）
        if actor == "" or actor in out:
            continue
        out.append(actor)
    return out


# §2.11 收尾要求复用同一枚单点：cc 支的旧名保留为**别名**（同一个函数对象，不是第二份判据）。
normalize_cc_actors = normalize_actors


def normalize_actor_value(value) -> str:
    """**单个**归属值归一（§2.11 写点表里 transfer 的 ``fromActor``/``toActor``、
    ``updateCCStatus`` 的 ``operator`` 这类标量档）：``str``＋trim，空/纯空白/``None`` ⇒ ``""``。

    判据不在这里另立——内部调 ``normalize_actors``（``"0"``/int ``0`` 都归一成 ``"0"`` 保住，
    绝不被假值判据折成"没填"）。调用方判空一律写 ``== ""``，不写 ``if not x``。
    """
    out = normalize_actors([value])
    return out[0] if out else ""


def actor_delete_forms(raw) -> list[str]:
    """归属值**删除腿**展开（issues/137 §3-6 · spec 06 §processTask/removeTaskActor 语义 6，
    owner 2026-10-02 拍「两形并集」）：把待删列表展开成 ``DELETE ... IN (...)`` 真正要绑的值——
    **空值一律丢弃，非空值同时保留「原值」与「trim 值」两形**（按字面去重、保序）。

    为什么必须两形、只取一头各有一种假成功（1.8.36 之前八栈正好分成这两派，没有一处两全）：

    - 只取 **trim 值**（php/csharp/rust/moon 四栈八处的旧形状）⇒ 门面按语义 6 交出的历史脏行
      原值 ``" 9101 "`` 被削成 ``9101``，真库（MySQL NO PAD 排序规则）下那一行删不掉，
      门面却报成功——被摘的人待办还在；
    - 只取 **原值**（go/node/python/java 四栈九处的旧形状，本栈两仓此前即裸传）⇒ 第三方绕过
      门面直连仓储传 ``" 8601 "`` 时删不掉写侧归一后落库的规范行 ``8601``（issues/142 §9.2
      那一路）；且空值照喂 ``DELETE``，会把历史 ``actor_id=''`` 脏行批量误删
      （那是替脏数据做掉唯一痕迹）。

    两形并集同时满足两侧：脏行按原值命中、规范行按 trim 形命中。按 §2.11 归一口径
    ``" 9101 "`` 与 ``9101`` 本就是**同一个人**，两行都删掉才是"摘掉这个人"的正确结果，
    不构成误删。

    判据本体**复用既有归一单点** ``normalize_actors``（trim／判空／``None`` 不串化都在那一枚里，
    本函数只加"原值也进集合"这一层，**不抄第二份 trim/判空代码**——spec §2.11 尾注明令）：
    判空一律 ``strip() == ""``，``"0"`` 是合法 id 必须留下，且 ``"0"`` 与 ``"00"`` 是两个人
    （**严禁 ``if not x`` 这种语言自带假值判据**）。去重按**字面**做，不按"strip 后相同"折叠
    原值形：``" 9101 "`` 与 ``"  9101  "`` 是两种不同的原值形，都要保留。

    :param raw: 待删归属值（``list``/``tuple``/逗号串/标量，与 ``normalize_actors`` 同形），
        元素可为 ``None``（丢弃，**不得**串化成 ``"None"``）
    :return: 展开后的删除值列表（保序、按字面去重、无空值）；入参为 ``None`` 或全为空值时
        返回**空列表**——调用方（仓储删除侧）据此**早退，一条 ``DELETE`` 都不发**
        （不得退化成"清空该任务全部参与者"）
    """
    out: list[str] = []
    if raw is None:
        return out
    if isinstance(raw, str):
        items: Any = raw.split(",")        # 逗号串腿：与 normalize_actors 同一套入参形状分派
    elif isinstance(raw, (list, tuple)):
        items = raw                        # 数组腿
    else:
        items = [raw]                      # 标量（含数字 id）按单个归属值
    for item in items:
        if item is None:
            continue
        # trim 与判空的判据本体交给既有单点（丢空即 ①"空值一律丢弃，不喂 DELETE"），不另抄
        trimmed = normalize_actors([item])
        if not trimmed:
            continue                       # None/空串/纯空白 ⇒ 丢弃
        original = item if isinstance(item, str) else str(item)
        if original not in out:
            out.append(original)           # ② 原值形：保住修复前落下的未 trim 历史脏行
        if trimmed[0] not in out:
            out.append(trimmed[0])         # ② trim 形：保住写侧归一后落库的规范行
    return out


def require_present_id(value, label: str = "processTaskId"):
    """主键类参数**另判一档**（spec 06 §2.11 末段）：``processTaskId`` 缺失/空串/``0`` 必须响亮报错。

    与"归属值为空 ⇒ 丢弃"是两件事：归属值可有可无，主键没有就是调用方写错了，静默接受会把脏数据
    钉进表里（现读 php ``JeeflowFacade.php:546`` 不校验 taskId、``addTaskActor('', …)`` 照跑，属反面）。
    错误沿用本栈既有的 ``ValueError`` → 门面 ``{code:99999999, msg}`` 信封，不新造错误码/文案。
    """
    if value is None:
        raise ValueError(f"{label} 缺失或非法: {value!r}")
    text = str(value).strip()
    if text == "" or text == "0":
        raise ValueError(f"{label} 缺失或非法: {value!r}")
    return value


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
    async def add_task_actor(self, task_id: int, actors: list[str]) -> None:
        """追加任务参与者（**只追加不清空**，issues/03 语义）。

        **归属值写侧归一**（issues/142 B 批 · spec 06-facade.md §2.11「归属值写侧归一」）——
        义务与 ``create_cc_instance`` 上那条**逐字同源**，只是换到 ``wf_process_task_actor.actor_id``
        这张表上（``actor_id`` 是 §2.5 口径表里的归属列，空串/``"  "``/``"None"`` 落进去就是
        issues/129 那族"空归属值读全库"的进水口）：

        ① 入参先过 ``normalize_actors``（**与 cc 支同一枚单点**，不许另抄一份）——逐元素
        ``str``＋trim，空串/纯空白/``None`` **一律丢弃**，同一次调用内的重复折叠；
        逗号串与数组**两形同判据**（``"a, ,b"`` 与 ``["a", "", "b"]`` 必须得到同一个答案）；
        ② **落库与判重一律取 trim 后的值**——``" 123 "`` 与 ``"123"`` 是同一个人，不 trim 就会
        把判重打穿成同一人两行；
        ③ 丢完为空 ⇒ **不写任何行**（与"空 actors"同形，不报错也不落脏值）；
        ④ 反向哨兵：``"0"`` 这类"看起来像空"的正常 id **不得**被丢掉，判空一律
        ``str(x).strip() == ""``，**严禁 ``if not x``** 这种语言自带假值判据；
        ⑤ **主键另判一档**：``task_id`` 缺失/空串/``0`` 必须**响亮报错**（本栈 ``ValueError``），
        不得拿 ``''``/``0`` 当 id 落库——归属值可有可无，主键没有就是调用方写错了。

        这条义务要钉在**实现方**而不只钉在门面/引擎漏斗里：绕过门面直连仓储的调用方（集成层、
        第三方仓储消费者）同样不得把空归属值灌进 ``actor_id``。本仓两仓（SQL 仓
        ``JdbcRepository`` / 内存仓 ``MemoryRepository``）同判据——**两仓分叉就是 issues/117
        场景 27 那把尺子**（rust 实测：sqlx 仓盲插、同栈内存仓判重，两个答案）。"""
        ...
    @abstractmethod
    async def remove_task_actor(self, task_id: int, actors: list[str]) -> None:
        """摘除任务参与者（**删除腿**，spec 06 §processTask/removeTaskActor 语义 6 ＋ §2.11
        写点表末行 · owner 2026-10-02 拍「两形并集」）。

        实现方义务是**三件事**（判据本体＝本模块 ``actor_delete_forms`` 那一枚，两仓与第三方
        实现一律走它，不许另抄）：

        ① **空值一律丢弃、不喂 ``DELETE``**——``None``／``""``／纯空白都不进 ``IN``，否则历史
        ``actor_id=''`` 脏行会被批量误删（那是替脏数据做掉唯一痕迹）；
        ② **非空值同时以「原值」与「trim 值」两形进 ``IN``**（按字面去重、保序；两形相同则只
        一份）——只取 trim 形删不掉修复前落下的未 trim 历史脏行 ``" 9101 "``（真库 NO PAD
        排序规则下门面报成功而人没被摘），只取原值则绕过门面直连仓储传 ``" 8601 "`` 时删不掉
        写侧归一后落库的规范行 ``8601``（issues/142 §9.2）；
        ③ 并集为空 ⇒ **早退，一条 ``DELETE`` 都不发**（不得退化成"清空该任务全部参与者"）。

        ⚠️ **与写侧义务 ``add_task_actor`` 不同、别照抄**：写侧是"落库与比较一律取 trim 后的
        值"（归一后只落一行），删除腿却必须**多带一份原值**——两形并集才两头都删得掉。
        判空一律 ``str(x).strip() == ""``，**严禁 ``if not x``**（``"0"`` 是合法 id，且 ``"0"``
        与 ``"00"`` 是两个人）。内存仓与 SQL 仓**同一条判据、同一个答案**（issues/117 场景 27）。"""
        ...
    @abstractmethod
    async def create_cc_instance(self, instance_id: int, creator: str, *actor_ids: str) -> None:
        """落 cc 行（**写侧判重＝幂等空操作**，issues/141 G2 · spec 06-facade.md §4）。

        同一 `(instance_id, actor_id)` **已存在 cc 行时直接跳过**：①不新增行 ②不重置未读状态
        （`state` 保持原值）③不更新原行时间（`create_time`/`update_time` 逐字不变）。
        判重放在**写侧**而不是查询侧——`page_cc_instances` 不引入 `DISTINCT`、历史重复行也不清理
        （owner 2026-09-29 拍：接受既成事实，写侧判重只保证今后不再新增）。
        建 cc 的三条入口（发起 `f_ccActors`／办理 `tf_ccActors`／手动 `createCCInstance`）都经由
        引擎的 `handle_cc_actors` 漏斗，那里调的是 `create_cc_instance_if_absent`
        ——需要"实际新建了谁"拿去 fire `CC_CREATE`（码 4）。

        **空不创建行**（issues/141 G10 · spec 06-facade.md §2.10）：入参里的**空串、纯空白、
        `None` 一律丢弃**，落库值取 **trim 后的串**。这条义务要钉在**实现方**而不只钉在引擎漏斗里
        ——绕过 `handle_cc_actors`/门面直连仓储的调用方（集成层、第三方仓储消费者）同样不得把空
        归属值灌进 `actor_id`，那正是 issues/129 那族"空 operator 读全库"的病根；不 trim 则
        `" 123 "` 与 `"123"` 会被判成两个人，把上面那条写侧判重打穿成同一人两行。两仓
        （SQL 仓 `JdbcRepository` / 内存仓 `MemoryRepository`）同判据，第三方实现按本 docstring 自守。"""
        ...
    @abstractmethod
    async def update_cc_status(self, instance_id: int, actor_id: str) -> None:
        """抄送置已读（``processInstance/updateCCStatus``）。

        **入参归一后再比**（issues/142 B 批 · spec 06-facade.md §2.11 写点表第 4 行）：
        ``actor_id`` 先过 ``normalize_actors``/``normalize_actor_value``（同一枚单点）再与库里的
        ``cc.actor_id`` 比较——① 不 trim 则 ``" lisi "`` 判成另一个人，已读打不上；
        ② **空/纯空白/``None`` 的 operator 是 no-op**，不得退化成"这条条件不加"而把 ``state=1``
        批量打到历史 ``actor_id=''`` 的脏行上（issues/129 那族"空归属值读全库"的写侧对偶）。
        判空一律 ``== ""``，严禁 ``if not x``（``"0"`` 是正常 id，必须照样能置已读）。
        两仓同判据。"""
        ...

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

        **空不创建行**（issues/141 G10 · spec 06 §2.10）：入参先过 ``normalize_cc_actors``——
        空串/纯空白/`None` 丢弃，比较与返回的子集一律取 **trim 后的值**（`" 123 "` 与 `"123"`
        是同一个人，也才和上面那条判重咬合）。丢完为空 ⇒ 子集空 ⇒ 不建行、不 fire 码 4。

        为什么返回子集而不是 None：spec 11-events §11.2 原则 1「码值表达发生了什么事实」
        ⇒ 没发生"创建"就**不得** fire `CC_CREATE`（码 4）。三条入口一律拿这个子集去 fire，
        **子集为空整支不发**（不空转，也不照旧按原始请求全量 fire）。

        未覆写 `find_cc_actor_ids` 的第三方仓储走本 default ⇒ 与旧
        `create_cc_instance(全量)` 逐字一致（子集＝入参**归一**后全量），不静默改变既有集成方行为
        ——G10 的归一腿是唯一被加进来的判据，旧行为里"空值也建行"那一档按裁定作废。
        """
        existing = set(await self.find_cc_actor_ids(instance_id) or [])
        fresh: list[str] = []
        # issues/141 G10「空不创建行」：先过归一腿——空串/纯空白/None 丢弃，值取 trim 后的串
        # （" 123 " 与 "123" 是同一个人，也才与下面的判重咬合）。判据落在这一层而不只落在引擎
        # 漏斗：绕过 handle_cc_actors 直连仓储的调用方同样建不出空行。
        for actor_id in normalize_cc_actors(actor_ids):
            if actor_id in existing or actor_id in fresh:
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
