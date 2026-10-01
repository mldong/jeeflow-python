"""内存仓储——测试用"""
from copy import deepcopy
from datetime import datetime
from typing import Optional
from .model import (ProcessDefine, ProcessInstance, ProcessTask, TaskState, InstanceState,
                    CcInstanceRow, DefineRow, InstanceRow, TaskRow,
                    ProcessDesign, ProcessDesignHis, ProcessSurrogate,
                    InstanceStatsRow, TaskStatsRow)
from .spi import ProcessRepository, ProcessExtRepository
from .spi import normalize_actors, normalize_cc_actors, require_present_id, actor_delete_forms
from .surrogate import hydrate_enabled, to_datetime   # 判据本身已收口到 ProcessSurrogate.is_effective（issues/123）

class CcRow(str):
    """内存仓的 cc 行（issues/141 G2）——形状对齐 `wf_process_cc_instance`：actor id ＋ 未读状态
    （0 未读 / 1 已读）＋ 建行时间与更新时间。

    ⚠️ 为什么是 `str` **子类**而不是 dataclass：判重的②③档（不重置未读、不刷原行时间）要求
    内存仓也带 state/时间，而本仓既有测试直读 `repo._cc[iid]` 并与 `["alice","bob"]` 比较
    （如 `test_cc_leg_untouched_start_and_manual_paths`）——行模型不许改既有断言。
    `str` 子类两头都满足：值本身就是 actor id（`==`/`in`/`in rows` 一律按字符串判），
    另挂三个可变字段给②③档取证。对齐 Java 内存仓的同名 `CcRow`（那边的既有读法走
    `ccActorsForTest`，所以它可以直接做成普通类）。
    """

    __slots__ = ("state", "create_time", "update_time")

    def __new__(cls, actor_id: str, *, state: int = 0, create_time=None, update_time=None):
        row = super().__new__(cls, actor_id)
        row.state = state
        row.create_time = create_time
        row.update_time = update_time
        return row

    def __repr__(self):
        return f"CcRow({str(self)!r}, state={self.state})"


class MemoryRepository(ProcessRepository):
    def __init__(self):
        self._defines: dict[int, ProcessDefine] = {}
        self._instances: dict[int, ProcessInstance] = {}
        self._tasks: dict[int, ProcessTask] = {}
        self._actors: dict[int, list[str]] = {}
        self._cc: dict[int, list[CcRow]] = {}
        self._seq = 1

    def add_define(self, d: ProcessDefine):
        d.id = d.id or self._seq; self._seq += 1
        self._defines[d.id] = d

    async def find_define_by_id(self, id): return deepcopy(self._defines.get(id))

    async def find_define_by_name(self, name):
        """按流程编码查最新一条定义（id 倒序取首条，v1.1.0）"""
        best = None
        for d in self._defines.values():
            if d.name == name and (best is None or d.id > best.id):
                best = d
        return deepcopy(best) if best else None

    # ── 定义写操作（v1.0.1，对齐 SPI）──

    async def save_define(self, d: ProcessDefine):
        d.id = d.id or self._seq; self._seq += 1
        self._defines[d.id] = d
    async def update_define(self, d: ProcessDefine):
        self._defines[d.id] = d
    async def update_define_state(self, define_id: int, state: int):
        if define_id in self._defines:
            self._defines[define_id].state = state
    async def remove_define(self, define_id: int):
        self._defines.pop(define_id, None)

    async def find_instance_by_id(self, id):
        inst = self._instances.get(id)
        if not inst: return None
        cp = deepcopy(inst)
        cp.tasks = [deepcopy(t) for t in self._tasks.values() if t.processInstanceId == id]
        for t in cp.tasks: t.actorIds = self._actors.get(t.id, t.actorIds)
        return cp
    async def save_instance(self, inst: ProcessInstance):
        inst.id = inst.id or self._seq; self._seq += 1
        self._instances[inst.id] = deepcopy(inst)
    async def update_instance(self, inst: ProcessInstance):
        self._instances[inst.id] = deepcopy(inst)
        # v1.0.1：级联保存聚合根内任务状态变更
        for t in inst.tasks:
            if t.id:
                self._tasks[t.id] = deepcopy(t)
                if t.actorIds: self._actors[t.id] = list(t.actorIds)
    async def find_task_by_id(self, task_id):
        t = self._tasks.get(task_id)
        if not t: return None
        cp = deepcopy(t); cp.actorIds = self._actors.get(task_id, cp.actorIds)
        return cp
    async def save_task(self, task: ProcessTask):
        task.id = task.id or self._seq; self._seq += 1
        self._tasks[task.id] = deepcopy(task)
        if task.actorIds: self._actors[task.id] = list(task.actorIds)
    async def update_task(self, task: ProcessTask):
        self._tasks[task.id] = deepcopy(task)
        if task.actorIds: self._actors[task.id] = list(task.actorIds)
    async def find_doing_tasks(self, instance_id, task_names=None):
        result = []
        for t in self._tasks.values():
            if t.processInstanceId == instance_id and t.taskState == TaskState.DOING:
                if task_names and t.taskName not in task_names: continue
                cp = deepcopy(t); cp.actorIds = self._actors.get(t.id, t.actorIds)
                result.append(cp)
        return result
    async def find_done_tasks(self, instance_id, task_names=None):
        return [deepcopy(t) for t in self._tasks.values() if t.processInstanceId == instance_id and t.taskState == TaskState.DONE]
    async def find_history_tasks(self, instance_id):
        return [deepcopy(t) for t in self._tasks.values() if t.processInstanceId == instance_id]
    async def find_task_actors(self, task_id): return list(self._actors.get(task_id, []))
    async def add_task_actor(self, task_id, actors):
        """内存仓写侧兜底（issues/142 B 批 · spec 06 §2.11 要求①「两层都挡」）：
        判据与 SQL 仓 ``JdbcRepository.add_task_actor`` **逐字同一条**（两仓分叉＝issues/117
        场景 27；rust 实测就是 sqlx 仓盲插、同栈内存仓判重的两个答案），复用 §2.10 落地的
        同一枚单点 ``spi.normalize_actors``。旧形状只判重不判空、不 trim ⇒
        ``""``/``"  "``/``None`` 全放行。"""
        # 主键另判一档：task_id 缺失/空串/0 响亮报错，不拿 ''/0 当 key 落库（§2.11 末段）
        task_id = require_present_id(task_id)
        # 归属值：trim／空串·纯空白·None 丢弃／同次调用折叠；"0" 不得丢（要求④）
        actors = normalize_actors(actors)
        if not actors:
            return
        existing = self._actors.get(task_id, [])
        for a in actors:
            # 比较取 trim 后的值（要求②）：入参已归一 ⇒ " 123 " 与 "123" 是同一个人，只一行
            if a not in existing: existing.append(a)
        self._actors[task_id] = existing
    async def remove_task_actor(self, task_id, actors):
        """内存仓删除腿（issues/137 §3-6 · spec 06 §processTask/removeTaskActor 语义 6 ＋ §2.11
        写点表末行「两形并集」）：判据与 SQL 仓 ``JdbcRepository.remove_task_actor``
        **逐字同一条**（两仓分叉＝issues/117 场景 27），复用同一枚单点
        ``spi.actor_delete_forms``。旧形状 ``remove = set(actors)`` 是**裸传**：既不产出
        trim 形（第三方绕过门面直连仓储传 ``" 8601 "`` 时删不掉写侧归一后的规范行 ``8601``，
        issues/142 §9.2），也不丢空值（``""`` 入参会把历史 ``actor_id=''`` 脏行删掉——
        那是替脏数据做掉唯一痕迹）。

        ⚠️ 与写侧 ``add_task_actor`` 的义务**不同、别照抄**：写侧取 trim 后的值落库，
        删除腿必须「原值 ∪ trim 值」两形并集——只取一头各有一种假成功（见单点 docstring）。"""
        # ①空值一律丢弃 ＋ ②非空值「原值 ∪ trim 值」两形进比较集（判据本体在 actor_delete_forms）
        forms = actor_delete_forms(actors)
        if not forms:
            return  # ③并集为空 ⇒ 早退，一条"删除"都不发生（不得退化成清空该任务全部参与者）
        rows = self._actors.get(task_id)
        if rows is None:
            return  # 任务不存在 ⇒ 零操作不抛异常（连空条目都不建）
        remove = set(forms)
        self._actors[task_id] = [a for a in rows if a not in remove]
    async def create_cc_instance(self, instance_id: int, creator: str, *actor_ids: str):
        # issues/141 G2 写侧判重＝幂等空操作（spec 06 §4），与 JdbcRepository.create_cc_instance
        # 同一条判据：同一 (实例, 被抄送人) 已有 cc 行 ⇒ 跳过——①不新增行 ②不重置未读（state 保持
        # 原值）③不更新原行时间（create_time/update_time 逐字不变）。判重在写侧，查询侧不引入去重。
        #
        # issues/141 G10「空不创建行」（spec 06 §2.10）：入参先过 normalize_cc_actors——
        # 空串/纯空白/None 一律丢弃，落库值取 **trim 后的串**（" 123 " 与 "123" 是同一个人，
        # 不 trim 就会把上面 G2 的写侧判重打穿成同一人两行）。这一层是**绕过引擎漏斗直连仓储**
        # 的兜底：漏斗那侧 parse_cc_actors 已经在归一，摘掉仓储这一层也照样建不出空行。
        # 与 JdbcRepository.create_cc_instance 同判据——两仓分叉＝issues/117 场景 27。
        rows = self._cc.setdefault(instance_id, [])
        for actor_id in normalize_cc_actors(actor_ids):
            if actor_id in rows:
                continue
            now = datetime.now()
            rows.append(CcRow(actor_id, create_time=now, update_time=now))

    async def find_cc_actor_ids(self, instance_id: int) -> list[str]:
        """某实例已有的 cc 行 actor id（issues/141 G2 写侧判重的读侧，覆写 SPI default）。"""
        return [str(row) for row in self._cc.get(instance_id, [])]

    async def update_cc_status(self, instance_id: int, actor_id: str):
        # 已读：state 0→1 ＋ 刷 update_time（对齐 wf_process_cc_instance.state 语义与 SQL 仓那句
        # UPDATE）。issues/141 G2 之前这里是 `pass`——内存仓的 cc 行不带 state/时间，
        # "重复抄送不重置未读 / 不刷原行时间"两档根本照不出来，只能空转断言。
        #
        # issues/142 B 批（spec 06 §2.11 写点表第 4 行）：**入参归一后再比**——不 trim 则
        # " lisi " 判成另一个人（已读打不上）；空/纯空白/None ⇒ **整个 no-op**，不得退化成
        # "这条条件不加"而把历史 actor_id='' 的脏行批量打勾（与 SQL 仓同判据；
        # 判空只用 == ""，"0" 是正常 id）。
        normalized = normalize_actors([actor_id])
        if not normalized:
            return
        for row in self._cc.get(instance_id, []):
            if row == normalized[0]:
                row.state = 1
                row.update_time = datetime.now()

    def cc_rows_for_test(self, instance_id: int) -> list["CcRow"]:
        """测试访问器：读回某实例的 cc **行**（issues/141 G2 的②③档要看未读状态与原行时间，
        只看 actor id 集合照不出"重复抄送把 state 抹回未读 / 把时间刷成 now"这两种假修）。"""
        return list(self._cc.get(instance_id, []))

    async def page_cc_instances(self, page_num: int = 1, page_size: int = 10, actor_id: Optional[str] = None,
                                conditions=None):
        """我的抄送分页（v1.3.0）：按抄送人 actor_id 过滤，join 实例 + 定义。

        **归属条件必填**（issues/141 G1 · spec 06 §2.5）：`cc.actor_id` 没有有效条件（`actor_id`
        入参与 `cc.actor_id` 条件两形都算）⇒ 返回**空页**，判据与 `JdbcRepository.page_cc_instances`
        逐字同一条。本仓的旧形状是 `_ownership_blank` 只收空串，`actor_id=None` 被当成"本次不带
        归属过滤"⇒ 放出全部"有 cc 行的实例"；SQL 仓同一档却因恒绑 `cc.actor_id = ?`（None ⇒ `= NULL`
        恒不命中）返 0 行——同一份数据两仓两个答案（issues/117 场景 27 那把尺子；spec 06 §2.5 点名的
        反面教材是 php PDO 仓"不带条件放全部实例"，本栈两仓各错一头）。
        """
        if not _has_cc_ownership(actor_id, conditions):
            return [], 0  # issues/141 G1：缺有效归属条件 ⇒ 空页（G1 前这里是"不过滤＝读全部有 cc 行的实例"）
        rows = []
        for inst_id, actors in self._cc.items():
            if actor_id and actor_id not in actors:
                continue
            inst = self._instances.get(inst_id)
            if not inst:
                continue
            row = CcInstanceRow(
                id=inst.id, parentId=inst.parentId, defineId=inst.defineId, state=inst.state,
                parentNodeName=inst.parentNodeName, businessNo=inst.businessNo, operator=inst.operator,
                expireTime=inst.expireTime, variables=deepcopy(inst.variables),
                createTime=inst.createTime, createUser=inst.createUser,
                updateTime=inst.updateTime, updateUser=inst.updateUser)
            defn = self._defines.get(inst.defineId)
            if defn:
                row.defineName = defn.name
                row.defineDisplayName = defn.displayName
                row.defineVersion = defn.version
            fields = _pick_fields(row, _INSTANCE_FIELDS)
            fields["cc.actor_id"] = actors
            if _match_conditions(conditions, fields):
                rows.append(row)
        total = len(rows)
        start = (page_num - 1) * page_size
        return rows[start:start + page_size], total

    def all_defines(self): return list(self._defines.values())
    def all_instances(self): return list(self._instances.values())
    def all_tasks(self):
        return [deepcopy(t) for t in self._tasks.values()]


    # ── 核心表分页（v1.5.0）──

    async def page_defines(self, page_num: int = 1, page_size: int = 10, conditions=None):
        rows = []
        for d in self._defines.values():
            row = DefineRow(id=d.id, name=d.name, displayName=d.displayName, type=d.type,
                            state=d.state, version=d.version, createTime=d.createTime,
                            createUser=d.createUser, updateTime=d.updateTime, updateUser=d.updateUser)
            if _match_conditions(conditions, _pick_fields(row, _DEFINE_FIELDS)):
                rows.append(row)
        return self._slice(rows, page_num, page_size)

    async def page_instances(self, page_num: int = 1, page_size: int = 10, operator: Optional[str] = None,
                             conditions=None):
        if _ownership_blank(operator):
            return [], 0  # issues/129：t.operator 空串 ⇒ 空页（原先 `if operator and` 把空串折成"读全库"）
        rows = []
        for inst in self._instances.values():
            if operator and inst.operator != operator:
                continue
            row = InstanceRow(
                id=inst.id, parentId=inst.parentId, defineId=inst.defineId, state=inst.state,
                parentNodeName=inst.parentNodeName, businessNo=inst.businessNo, operator=inst.operator,
                expireTime=inst.expireTime, variables=deepcopy(inst.variables),
                createTime=inst.createTime, createUser=inst.createUser,
                updateTime=inst.updateTime, updateUser=inst.updateUser)
            defn = self._defines.get(inst.defineId)
            if defn:
                row.defineName = defn.name
                row.defineDisplayName = defn.displayName
                row.defineVersion = defn.version
            if _match_conditions(conditions, _pick_fields(row, _INSTANCE_FIELDS)):
                rows.append(row)
        return self._slice(rows, page_num, page_size)

    async def page_todo_tasks(self, page_num: int = 1, page_size: int = 10, actor_id: Optional[str] = None,
                              conditions=None):
        if _ownership_blank(actor_id):
            return [], 0  # issues/129：pta.actor_id 空串 ⇒ 空页
        rows = []
        for t in self._tasks.values():
            if t.taskState != TaskState.DOING:
                continue
            if actor_id and actor_id not in self._actors.get(t.id, []):
                continue
            row = self._task_row(t)
            fields = _pick_fields(row, _TASK_FIELDS)
            fields["pta.actor_id"] = self._actors.get(t.id, [])
            if _match_conditions(conditions, fields):
                rows.append(row)
        return self._slice(rows, page_num, page_size)

    async def page_done_tasks(self, page_num: int = 1, page_size: int = 10, operator: Optional[str] = None,
                              conditions=None):
        if _ownership_blank(operator):
            return [], 0  # issues/129：t.operator 空串 ⇒ 空页
        rows = []
        for t in self._tasks.values():
            if t.taskState == TaskState.DOING:
                continue
            if operator and t.actorId != operator:
                continue
            row = self._task_row(t)
            if _match_conditions(conditions, _pick_fields(row, _TASK_FIELDS)):
                rows.append(row)
        return self._slice(rows, page_num, page_size)

    def _task_row(self, t: ProcessTask) -> TaskRow:
        row = TaskRow(
            id=t.id, processInstanceId=t.processInstanceId, taskName=t.taskName,
            displayName=t.displayName, taskType=t.taskType, performType=t.performType,
            taskState=t.taskState, operator=t.actorId, finishTime=t.finishTime,
            expireTime=t.expireTime, formKey=t.formKey, taskParentId=t.parentTaskId,
            variables=deepcopy(t.variables), createTime=t.createTime, createUser=t.createUser,
            updateTime=t.updateTime, updateUser=t.updateUser)
        inst = self._instances.get(t.processInstanceId)
        if inst:
            row.instanceCreateTime = inst.createTime
            defn = self._defines.get(inst.defineId)
            if defn:
                row.processDefineName = defn.name
                row.processDefineDisplayName = defn.displayName
                row.defineVersion = defn.version
        return row

    @staticmethod
    def _slice(rows, page_num, page_size):
        total = len(rows)
        start = (page_num - 1) * page_size
        return rows[start:start + page_size], total

    # ── 统计查询（v1.8.25，issues/103） ──

    @staticmethod
    def _to_dt(v) -> Optional[datetime]:
        if v is None:
            return None
        if isinstance(v, datetime):
            return v
        if isinstance(v, str):
            s = v.replace("T", " ")[:19]
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    return datetime.strptime(s, fmt)
                except ValueError:
                    continue
        return None

    async def query_instances_for_stats(self, state_in: list[int] | None, order_by: str = "create_time",
                                        start=None, end=None) -> list:
        sd = self._to_dt(start)
        ed = self._to_dt(end)
        rows = []
        for inst in self._instances.values():
            sv = int(inst.state)
            # state_in 空 = 无 state 过滤（对齐内置线：仅 overview 六计数用 stateIn）
            if state_in and sv not in state_in:
                continue
            ct = self._to_dt(inst.createTime)
            if sd and ct and ct < sd:
                continue
            if ed and ct and ct > ed:
                continue
            rows.append(InstanceStatsRow(
                defineId=inst.defineId, state=sv,
                operator=inst.operator or "", createTime=inst.createTime))
        return rows

    async def query_tasks_for_stats(self, task_state=None, start=None, end=None) -> list:
        sd = self._to_dt(start)
        ed = self._to_dt(end)
        rows = []
        for t in self._tasks.values():
            if task_state is not None and int(t.taskState) != task_state:
                continue
            ft = self._to_dt(t.finishTime)
            if sd and ft and ft < sd:
                continue
            if ed and ft and ft > ed:
                continue
            rows.append(TaskStatsRow(
                operator=t.actorId or "", displayName=t.displayName or "",
                performType=t.performType or 0,
                createTime=t.createTime, finishTime=t.finishTime, expireTime=t.expireTime))
        return rows

    async def stats_pending_and_overdue_count(self) -> tuple:
        now = datetime.now()
        pending = 0
        overdue = 0
        for t in self._tasks.values():
            if int(t.taskState) != int(TaskState.DOING):
                continue
            pending += 1
            exp = self._to_dt(t.expireTime)
            if exp and exp < now:
                overdue += 1
        return pending, overdue

    async def stats_completed_task_aggregate(self) -> tuple:
        total = 0
        countersign = 0
        on_time = 0
        on_time_denom = 0
        for t in self._tasks.values():
            if int(t.taskState) != int(TaskState.DONE):
                continue
            total += 1
            if t.performType == 1:
                countersign += 1
            ft = self._to_dt(t.finishTime)
            exp = self._to_dt(t.expireTime)
            if exp is not None:
                on_time_denom += 1
                if ft and ft <= exp:
                    on_time += 1
        return total, countersign, on_time, on_time_denom

    async def stats_avg_completed_duration_seconds(self, start=None, end=None) -> int:
        sd = self._to_dt(start)
        ed = self._to_dt(end)
        total_sec = 0
        count = 0
        for inst in self._instances.values():
            if int(inst.state) != int(InstanceState.DONE):
                continue
            ct = self._to_dt(inst.createTime)
            if sd and ct and ct < sd:
                continue
            if ed and ct and ct > ed:
                continue
            max_ft = None
            for t in self._tasks.values():
                if t.processInstanceId != inst.id:
                    continue
                ft = self._to_dt(t.finishTime)
                if ft and (max_ft is None or ft > max_ft):
                    max_ft = ft
            if max_ft and ct:
                total_sec += int((max_ft - ct).total_seconds())
                count += 1
        return total_sec // count if count > 0 else 0

    async def stats_define_group(self, start=None, end=None, limit=10) -> list:
        sd = self._to_dt(start)
        ed = self._to_dt(end)
        grouped: dict[int, dict] = {}
        for inst in self._instances.values():
            ct = self._to_dt(inst.createTime)
            if sd and ct and ct < sd:
                continue
            if ed and ct and ct > ed:
                continue
            did = inst.defineId
            if did not in grouped:
                defn = self._defines.get(did)
                grouped[did] = {"key": defn.name if defn else "", "label": defn.displayName if defn else None,
                                "count": 0, "totalDur": 0, "durCount": 0}
            g = grouped[did]
            g["count"] += 1
            if int(inst.state) == int(InstanceState.DONE):
                max_ft = None
                for t in self._tasks.values():
                    if t.processInstanceId != inst.id:
                        continue
                    ft = self._to_dt(t.finishTime)
                    if ft and (max_ft is None or ft > max_ft):
                        max_ft = ft
                if max_ft and ct:
                    g["totalDur"] += int((max_ft - ct).total_seconds())
                    g["durCount"] += 1
        entries = sorted(grouped.values(), key=lambda x: x["count"], reverse=True)[:limit]
        return [{"key": e["key"], "label": e["label"], "count": e["count"],
                 "avgDurationSeconds": (e["totalDur"] // e["durCount"] if e["durCount"] > 0 else None)}
                for e in entries]

    async def stats_stuck_node_group(self, limit=10) -> list:
        grouped: dict[str, int] = {}
        for t in self._tasks.values():
            if int(t.taskState) != int(TaskState.DOING):
                continue
            dn = t.displayName
            if not dn:
                continue
            grouped[dn] = grouped.get(dn, 0) + 1
        entries = sorted(grouped.items(), key=lambda x: x[1], reverse=True)[:limit]
        return [{"key": k, "count": c} for k, c in entries]

    async def stats_stuck_approver_group(self, limit=10) -> list:
        grouped: dict[str, int] = {}
        for t in self._tasks.values():
            if int(t.taskState) != int(TaskState.DOING):
                continue
            actors = self._actors.get(t.id, [])
            for aid in actors:
                if aid:
                    grouped[aid] = grouped.get(aid, 0) + 1
        entries = sorted(grouped.items(), key=lambda x: x[1], reverse=True)[:limit]
        return [{"key": k, "count": c} for k, c in entries]

    async def stats_completed_instance_durations(self, start=None, end=None) -> list:
        sd = self._to_dt(start)
        ed = self._to_dt(end)
        durations = []
        for inst in self._instances.values():
            if int(inst.state) != int(InstanceState.DONE):
                continue
            ct = self._to_dt(inst.createTime)
            if sd and ct and ct < sd:
                continue
            if ed and ct and ct > ed:
                continue
            max_ft = None
            for t in self._tasks.values():
                if t.processInstanceId != inst.id:
                    continue
                ft = self._to_dt(t.finishTime)
                if ft and (max_ft is None or ft > max_ft):
                    max_ft = ft
            if max_ft and ct:
                durations.append(int((max_ft - ct).total_seconds()))
        return durations

# ═══ 条件匹配基建（issues/05-5，对齐 JDBC 白名单语义） ═══

# 行字段映射（列名 → 行属性，白名单列均可匹配）
_TASK_FIELDS = {
    "t.id": "id", "t.task_name": "taskName", "t.display_name": "displayName",
    "t.task_type": "taskType", "t.perform_type": "performType", "t.task_state": "taskState",
    "t.operator": "operator", "t.form_key": "formKey", "t.create_time": "createTime",
    "t.finish_time": "finishTime", "t.expire_time": "expireTime",
    "t.process_instance_id": "processInstanceId", "t.task_parent_id": "taskParentId",
    "pd.name": "processDefineName", "pd.display_name": "processDefineDisplayName",
    "pd.version": "defineVersion",
}

_INSTANCE_FIELDS = {
    "t.id": "id", "t.parent_id": "parentId", "t.process_define_id": "defineId",
    "t.state": "state", "t.parent_node_name": "parentNodeName", "t.business_no": "businessNo",
    "t.operator": "operator", "t.expire_time": "expireTime", "t.create_time": "createTime",
    "pd.name": "defineName", "pd.display_name": "defineDisplayName", "pd.version": "defineVersion",
}

_DEFINE_FIELDS = {
    "t.id": "id", "t.name": "name", "t.display_name": "displayName", "t.type": "type",
    "t.state": "state", "t.version": "version", "t.create_time": "createTime",
    "t.update_time": "updateTime",
}

_DESIGN_FIELDS = {
    "t.id": "id", "t.name": "name", "t.display_name": "displayName", "t.type": "type",
    "t.is_deployed": "isDeployed", "t.remark": "remark",
    "t.create_time": "createTime", "t.update_time": "updateTime",
}

_SURROGATE_FIELDS = {
    "t.id": "id", "t.process_name": "processName", "t.operator": "operator",
    "t.surrogate": "surrogate", "t.enabled": "enabled",
    "t.start_time": "startTime", "t.end_time": "endTime",
    "t.create_time": "createTime", "t.update_time": "updateTime",
}


def _pick_fields(row, field_map: dict) -> dict:
    return {col: getattr(row, key, None) for col, key in field_map.items()}


def _eq_value(v, expect) -> bool:
    if isinstance(v, (list, tuple, set)):
        return expect in v
    return str(v) == str(expect)


def _ownership_blank(v) -> bool:
    """归属谓词拿到空串（含全空白）——issues/129 案 A 第二层的判据。

    None 不算空：内存仓储这些分页方法的 `operator/actor_id` 是**专用入参**，
    None 是既有 SPI 语义"本次不带归属过滤"，把 None 也判成空会让不带该参的既有调用整体变空页。
    只有"显式传了个空串"才是本 issue 的病灶（门面第一层已归一化，这一层防绕过门面的调用方）。

    ⚠️ 本判据只管 `page_instances`/`page_todo_tasks`/`page_done_tasks` 三张列表；
    **抄送分页不吃这条**——issues/141 G1（spec 06 §2.5）把 `page_cc_instances` 的尺子延长成
    "归属条件必填、None 也算没填"，见 `_has_cc_ownership`（G9 那条"要不要把必填推广到
    其余三张"正等 owner 拍，本轮不推广）。
    """
    return v is not None and isinstance(v, str) and not v.strip()


def _effective_ownership(val) -> bool:
    """归属条件的**有效值**判据（issues/141 G1）：值非 None、`str` 型 strip 后非空、集合非空。

    与 `repository/base.py::_effective_ownership` 同名同判据（两仓逐字一条，判据分叉＝
    issues/117 场景 27 立过法的"同一栈两个仓储两个答案"）。空串档与 issues/129 的
    `OWNERSHIP_COLUMNS`/`_ownership_blank` 同一口径，这一格多收的是"整条条件没给"与"空集合"。
    """
    if val is None:
        return False
    if isinstance(val, str) and not val.strip():
        return False
    if isinstance(val, (list, tuple, set, dict)) and len(val) == 0:
        return False
    return True


def _has_cc_ownership(actor_id, conditions) -> bool:
    """抄送分页有没有**有效**归属条件（issues/141 G1 · spec 06 §2.5）。

    本栈的归属落点有两形（Java 只有 conditions 一形）：专用入参 `actor_id`，以及
    `conditions` 里 `column == "cc.actor_id"` 的那条。任一形给了有效值就算"条件齐了"；
    两形都没给 ⇒ 调用方要求的就是空页。
    """
    if _effective_ownership(actor_id):
        return True
    for cond in conditions or []:
        if getattr(cond, "column", None) == "cc.actor_id" and _effective_ownership(cond.value):
            return True
    return False


def _match_conditions(conditions, fields: dict) -> bool:
    """条件全匹配（操作符对齐 JDBC buildWhere；列不在字段中则跳过）"""
    for c in conditions or []:
        v = fields.get(c.column)
        expect = c.value
        if v is None or expect is None:
            continue
        op = c.operator.upper()
        if op == "EQ":
            if not _eq_value(v, expect):
                return False
        elif op == "NE":
            if _eq_value(v, expect):
                return False
        elif op == "LIKE":
            if str(expect) not in str(v):
                return False
        elif op == "LLIKE":
            if not str(v).endswith(str(expect)):
                return False
        elif op == "RLIKE":
            if not str(v).startswith(str(expect)):
                return False
        elif op == "GT":
            if not (v > expect):
                return False
        elif op == "GE":
            if not (v >= expect):
                return False
        elif op == "LT":
            if not (v < expect):
                return False
        elif op == "LE":
            if not (v <= expect):
                return False
        elif op == "IN":
            # IN 值应为列表；标量列判断"列值在列表内"（对齐 Java/Go：列表才过滤，否则放行）
            if isinstance(expect, (list, tuple)) and str(v) not in [str(x) for x in expect]:
                return False
        elif op == "NIN":
            if isinstance(expect, (list, tuple)) and str(v) in [str(x) for x in expect]:
                return False
    return True


class MemoryExtRepository(ProcessExtRepository):
    """扩展仓储内存实现（v1.1.0，测试/演示用）"""

    def __init__(self):
        self._designs: dict[int, ProcessDesign] = {}
        self._designHis: dict[int, list[ProcessDesignHis]] = {}
        self._surrogates: dict[int, ProcessSurrogate] = {}
        self._seq = 1

    # ── 流程设计 ──

    async def find_design_by_id(self, id): return deepcopy(self._designs.get(id))

    async def save_design(self, d: ProcessDesign):
        d.id = d.id or self._seq; self._seq += 1
        now = datetime.now()
        d.createTime = d.createTime or now
        d.updateTime = d.updateTime or now
        self._designs[d.id] = deepcopy(d)

    async def update_design(self, d: ProcessDesign):
        d.updateTime = datetime.now()
        self._designs[d.id] = deepcopy(d)

    async def remove_design(self, id: int):
        self._designs.pop(id, None)
        self._designHis.pop(id, None)

    async def page_designs(self, page_num=1, page_size=10, filters=None, conditions=None):
        rows = [d for d in self._designs.values()
                if _match_conditions(conditions, _pick_fields(d, _DESIGN_FIELDS))]
        return rows, len(rows)

    # ── 设计历史 ──

    async def save_design_his(self, his: ProcessDesignHis):
        his.id = his.id or self._seq; self._seq += 1
        his.createTime = his.createTime or datetime.now()
        self._designHis.setdefault(his.processDesignId, []).insert(0, deepcopy(his))

    async def list_design_his(self, design_id: int):
        return [deepcopy(h) for h in self._designHis.get(design_id, [])]

    # ── 委托代理 ──

    async def find_surrogate_by_id(self, id): return deepcopy(self._surrogates.get(id))

    async def save_surrogate(self, s: ProcessSurrogate):
        s.id = s.id or self._seq; self._seq += 1
        now = datetime.now()
        s.createTime = s.createTime or now
        s.updateTime = s.updateTime or now
        # 显式 enabled=0 是合法值（停用委托）；缺省由门面处理（对齐 Java/Go，issues/82-7）
        # ── issues/130 遗留分叉收口（owner 2026-09-29 拍：**统一到 node 侧**）──────────────
        # wf_process_surrogate.enabled 建模的是 **INT 列**（tests/schema/schema-mysql.sql），
        # 本台账就是那张列的替身：把文本 '1' 直写进 INT 列，落进去的就是数值 1——SQL 仓那一步
        # 由数据库做，内存仓没人做，于是绕过门面的仓储直写会把 '1' 原样留在台账里，被读侧
        # 严判据（只认整数 1）判废 ⇒ 同一条 '1' 两栈两答案（node 归一、python 不归一）。
        # ⚠️ 只还原**规范整数串**（`hydrate_enabled`：'1'→1、'0'→0；'abc' / '1.0' / ' 1' / '01' /
        # True / 1.0 一律原样留着交判据停用），**不是**把接受集合放宽回去——判据④本体
        # （`surrogate_enabled_on`）一个字没动，仍只认整数 1；node 的 memory-ext.saveSurrogate
        # 是同一形状（其脏值矩阵走 updateSurrogate 那一条不归一的整行覆盖路径）。
        # ⚠️ update_surrogate **故意不归一**（与本方法有意不对称，对齐 node updateSurrogate 与
        # PHP InMemoryProcessExtRepository::updateSurrogate）：那是"调用方给的原始值直接显形"
        # 的唯一出口，脏值矩阵钉的就是这一档。
        s.enabled = hydrate_enabled(s.enabled)
        self._surrogates[s.id] = deepcopy(s)

    async def update_surrogate(self, s: ProcessSurrogate):
        s.updateTime = datetime.now()
        self._surrogates[s.id] = deepcopy(s)

    async def remove_surrogate(self, id: int):
        self._surrogates.pop(id, None)

    async def page_surrogates(self, page_num=1, page_size=10, filters=None, conditions=None):
        rows = []
        for s in self._surrogates.values():
            ok = True
            for col, val in (filters or {}).items():
                if val is None or val == "":
                    continue
                key = "processName" if col == "process_name" else col
                if str(getattr(s, key, "")) != str(val):
                    ok = False
                    break
            if ok and _match_conditions(conditions, _pick_fields(s, _SURROGATE_FIELDS)):
                rows.append(deepcopy(s))
        return rows, len(rows)

    async def get_surrogate(self, operator: str, process_name: str, at=None):
        """生效委托查询——与 SQL 仓 ``JdbcProcessExtRepository.get_surrogate`` **同形同答案**
        （issues/116 批次 D / 06 §4.5 条款 6：同栈两仓对同一份数据结论不同即缺陷）。

        顺序是契约的一部分（06 §4.5 条款 1.4 / issues/123）：
        **先在指定流程作用域内按 id 取最新一条**，交 ``ProcessSurrogate.is_effective`` 裁决这一条；
        该作用域判否（或没有记录）才看"全流程"作用域（各自取自己作用域里最新的一条）。
        反过来写（先按判据②③④过滤、再从剩下的取 id 最大）等价于"上一条窗内委托把用户后续
        改停用 / 改到未来 / 改成自委托的设置永久盖掉"⇒ 委托永久生效，正是 issues/123 的成因。

        四判据本身见 ``ProcessSurrogate.is_effective``（判据① 作用域在本方法这一层）。

        ⚠️ 本仓**不做** issues/130 案 A 的**读侧**类型还原（``surrogate.hydrate_enabled`` 挂在
        内置 SQL 仓装行处那一趟，读侧还原的是"驱动把 INT 列回读成字符串"这个边界事实）：
        内存仓没有驱动，读回的就是台账里存着的东西。它补的是**写侧**那一步——
        ``save_surrogate`` 把规范整数串 ``'1'`` 落成整数 1（本台账建模的就是 INT 列，
        SQL 那侧由数据库做，owner 2026-09-29 拍"统一到 node 侧 memory-ext.saveSurrogate"）；
        ``update_surrogate`` 有意不归一，是"台账里就是调用方给的原始值"的显形出口
        （issues/130 §2，脏值矩阵走这一路）。**判据④本身始终只认整数 1**，两档边界都不放宽它。
        """
        if operator is None:
            return None
        at = to_datetime(at) or datetime.now()    # 引擎钟：与写入侧同一把尺子（条款 5 / issues/120）
        exact = self._newest_surrogate(
            operator, lambda s: bool(process_name) and (s.processName or "") == process_name)
        if exact is not None and exact.is_effective(operator, at):
            return deepcopy(exact)
        # 精确作用域判否 ≠ 判否即止：全流程作用域的最新一条仍要单独裁决（条款 1.4 尾注）
        global_ = self._newest_surrogate(operator, lambda s: not (s.processName or ""))
        return deepcopy(global_) if global_ is not None and global_.is_effective(operator, at) else None

    def _newest_surrogate(self, operator: str, scope_match) -> Optional[ProcessSurrogate]:
        """取该授权人在指定作用域内 **id 最大的一条**（对齐 SQL 侧 ``ORDER BY id DESC LIMIT 1``）；
        只择优、不带任何生效判据过滤（判据在 ``is_effective`` 里）。该作用域无记录返回 ``None``。"""
        best = None
        for s in self._surrogates.values():
            if s.operator != operator or not scope_match(s):
                continue
            if best is None or (s.id or 0) > (best.id or 0):
                best = s
        return best

    # ── 核心表分页（v1.5.0）──

    async def page_defines(self, page_num: int = 1, page_size: int = 10, conditions=None):
        rows = []
        for d in self._defines.values():
            row = DefineRow(id=d.id, name=d.name, displayName=d.displayName, type=d.type,
                            state=d.state, version=d.version, createTime=d.createTime,
                            createUser=d.createUser, updateTime=d.updateTime, updateUser=d.updateUser)
            if _match_conditions(conditions, _pick_fields(row, _DEFINE_FIELDS)):
                rows.append(row)
        return self._slice(rows, page_num, page_size)

    async def page_instances(self, page_num: int = 1, page_size: int = 10, operator: Optional[str] = None,
                             conditions=None):
        if _ownership_blank(operator):
            return [], 0  # issues/129：t.operator 空串 ⇒ 空页（原先 `if operator and` 把空串折成"读全库"）
        rows = []
        for inst in self._instances.values():
            if operator and inst.operator != operator:
                continue
            row = InstanceRow(
                id=inst.id, parentId=inst.parentId, defineId=inst.defineId, state=inst.state,
                parentNodeName=inst.parentNodeName, businessNo=inst.businessNo, operator=inst.operator,
                expireTime=inst.expireTime, variables=deepcopy(inst.variables),
                createTime=inst.createTime, createUser=inst.createUser,
                updateTime=inst.updateTime, updateUser=inst.updateUser)
            defn = self._defines.get(inst.defineId)
            if defn:
                row.defineName = defn.name
                row.defineDisplayName = defn.displayName
                row.defineVersion = defn.version
            if _match_conditions(conditions, _pick_fields(row, _INSTANCE_FIELDS)):
                rows.append(row)
        return self._slice(rows, page_num, page_size)

    async def page_todo_tasks(self, page_num: int = 1, page_size: int = 10, actor_id: Optional[str] = None,
                              conditions=None):
        if _ownership_blank(actor_id):
            return [], 0  # issues/129：pta.actor_id 空串 ⇒ 空页
        rows = []
        for t in self._tasks.values():
            if t.taskState != TaskState.DOING:
                continue
            if actor_id and actor_id not in self._actors.get(t.id, []):
                continue
            row = self._task_row(t)
            fields = _pick_fields(row, _TASK_FIELDS)
            fields["pta.actor_id"] = self._actors.get(t.id, [])
            if _match_conditions(conditions, fields):
                rows.append(row)
        return self._slice(rows, page_num, page_size)

    async def page_done_tasks(self, page_num: int = 1, page_size: int = 10, operator: Optional[str] = None,
                              conditions=None):
        if _ownership_blank(operator):
            return [], 0  # issues/129：t.operator 空串 ⇒ 空页
        rows = []
        for t in self._tasks.values():
            if t.taskState == TaskState.DOING:
                continue
            if operator and t.actorId != operator:
                continue
            row = self._task_row(t)
            if _match_conditions(conditions, _pick_fields(row, _TASK_FIELDS)):
                rows.append(row)
        return self._slice(rows, page_num, page_size)

    def _task_row(self, t: ProcessTask) -> TaskRow:
        row = TaskRow(
            id=t.id, processInstanceId=t.processInstanceId, taskName=t.taskName,
            displayName=t.displayName, taskType=t.taskType, performType=t.performType,
            taskState=t.taskState, operator=t.actorId, finishTime=t.finishTime,
            expireTime=t.expireTime, formKey=t.formKey, taskParentId=t.parentTaskId,
            variables=deepcopy(t.variables), createTime=t.createTime, createUser=t.createUser,
            updateTime=t.updateTime, updateUser=t.updateUser)
        inst = self._instances.get(t.processInstanceId)
        if inst:
            row.instanceCreateTime = inst.createTime
            defn = self._defines.get(inst.defineId)
            if defn:
                row.processDefineName = defn.name
                row.processDefineDisplayName = defn.displayName
                row.defineVersion = defn.version
        return row

    @staticmethod
    def _slice(rows, page_num, page_size):
        total = len(rows)
        start = (page_num - 1) * page_size
        return rows[start:start + page_size], total
