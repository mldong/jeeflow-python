"""内存仓储——测试用"""
from copy import deepcopy
from datetime import datetime
from typing import Optional
from .model import (ProcessDefine, ProcessInstance, ProcessTask, TaskState, InstanceState,
                    CcInstanceRow, DefineRow, InstanceRow, TaskRow,
                    ProcessDesign, ProcessDesignHis, ProcessSurrogate,
                    InstanceStatsRow, TaskStatsRow)
from .spi import ProcessRepository, ProcessExtRepository

class MemoryRepository(ProcessRepository):
    def __init__(self):
        self._defines: dict[int, ProcessDefine] = {}
        self._instances: dict[int, ProcessInstance] = {}
        self._tasks: dict[int, ProcessTask] = {}
        self._actors: dict[int, list[str]] = {}
        self._cc: dict[int, list[str]] = {}
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
        existing = self._actors.get(task_id, [])
        for a in actors:
            if a not in existing: existing.append(a)
        self._actors[task_id] = existing
    async def remove_task_actor(self, task_id, actors):
        remove = set(actors)
        self._actors[task_id] = [a for a in self._actors.get(task_id, []) if a not in remove]
    async def create_cc_instance(self, instance_id: int, creator: str, *actor_ids: str):
        self._cc[instance_id] = list(dict.fromkeys([*self._cc.get(instance_id, []), *actor_ids]))
    async def update_cc_status(self, instance_id: int, actor_id: str): pass
    async def page_cc_instances(self, page_num: int = 1, page_size: int = 10, actor_id: Optional[str] = None,
                                conditions=None):
        """我的抄送分页（v1.3.0）：按抄送人 actor_id 过滤，join 实例 + 定义"""
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
        at = at or datetime.now()
        fallback = None
        for s in self._surrogates.values():
            if s.operator != operator or s.enabled != 1:
                continue
            if s.startTime and s.startTime > at:
                continue
            if s.endTime and s.endTime < at:
                continue
            if s.processName == process_name and process_name:
                return deepcopy(s)
            if (not s.processName or s.processName == process_name) and fallback is None:
                fallback = s
        return deepcopy(fallback) if fallback else None

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
        at = at or datetime.now()
        fallback = None
        for s in self._surrogates.values():
            if s.operator != operator or s.enabled != 1:
                continue
            if s.startTime and s.startTime > at:
                continue
            if s.endTime and s.endTime < at:
                continue
            if s.processName == process_name and process_name:
                return deepcopy(s)
            if (not s.processName or s.processName == process_name) and fallback is None:
                fallback = s
        return deepcopy(fallback) if fallback else None

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
