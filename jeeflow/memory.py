"""内存仓储——测试用"""
from copy import deepcopy
from datetime import datetime
from typing import Optional
from .model import (ProcessDefine, ProcessInstance, ProcessTask, TaskState, CcInstanceRow, DefineRow, InstanceRow, TaskRow,
                    ProcessDesign, ProcessDesignHis, ProcessSurrogate)
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
    async def page_cc_instances(self, page_num: int = 1, page_size: int = 10, actor_id: Optional[str] = None):
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
            rows.append(row)
        total = len(rows)
        start = (page_num - 1) * page_size
        return rows[start:start + page_size], total

    def all_defines(self): return list(self._defines.values())
    def all_instances(self): return list(self._instances.values())
    def all_tasks(self):
        return [deepcopy(t) for t in self._tasks.values()]


    # ── 核心表分页（v1.5.0）──

    async def page_defines(self, page_num: int = 1, page_size: int = 10):
        rows = [DefineRow(id=d.id, name=d.name, displayName=d.displayName, type=d.type,
                          state=d.state, version=d.version, createTime=d.createTime,
                          createUser=d.createUser, updateTime=d.updateTime, updateUser=d.updateUser)
                for d in self._defines.values()]
        return self._slice(rows, page_num, page_size)

    async def page_instances(self, page_num: int = 1, page_size: int = 10, operator: Optional[str] = None):
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
            rows.append(row)
        return self._slice(rows, page_num, page_size)

    async def page_todo_tasks(self, page_num: int = 1, page_size: int = 10, actor_id: Optional[str] = None):
        rows = []
        for t in self._tasks.values():
            if t.taskState != TaskState.DOING:
                continue
            if actor_id and actor_id not in self._actors.get(t.id, []):
                continue
            rows.append(self._task_row(t))
        return self._slice(rows, page_num, page_size)

    async def page_done_tasks(self, page_num: int = 1, page_size: int = 10, operator: Optional[str] = None):
        rows = []
        for t in self._tasks.values():
            if t.taskState == TaskState.DOING:
                continue
            if operator and t.actorId != operator:
                continue
            rows.append(self._task_row(t))
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

    async def page_designs(self, page_num=1, page_size=10, filters=None):
        rows = [deepcopy(d) for d in self._designs.values()]
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
        s.enabled = s.enabled or 1
        self._surrogates[s.id] = deepcopy(s)

    async def update_surrogate(self, s: ProcessSurrogate):
        s.updateTime = datetime.now()
        self._surrogates[s.id] = deepcopy(s)

    async def remove_surrogate(self, id: int):
        self._surrogates.pop(id, None)

    async def page_surrogates(self, page_num=1, page_size=10, filters=None):
        rows = [deepcopy(s) for s in self._surrogates.values()]
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

    async def page_designs(self, page_num=1, page_size=10, filters=None):
        rows = [deepcopy(d) for d in self._designs.values()]
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
        s.enabled = s.enabled or 1
        self._surrogates[s.id] = deepcopy(s)

    async def update_surrogate(self, s: ProcessSurrogate):
        s.updateTime = datetime.now()
        self._surrogates[s.id] = deepcopy(s)

    async def remove_surrogate(self, id: int):
        self._surrogates.pop(id, None)

    async def page_surrogates(self, page_num=1, page_size=10, filters=None):
        rows = [deepcopy(s) for s in self._surrogates.values()]
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

    async def page_defines(self, page_num: int = 1, page_size: int = 10):
        rows = [DefineRow(id=d.id, name=d.name, displayName=d.displayName, type=d.type,
                          state=d.state, version=d.version, createTime=d.createTime,
                          createUser=d.createUser, updateTime=d.updateTime, updateUser=d.updateUser)
                for d in self._defines.values()]
        return self._slice(rows, page_num, page_size)

    async def page_instances(self, page_num: int = 1, page_size: int = 10, operator: Optional[str] = None):
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
            rows.append(row)
        return self._slice(rows, page_num, page_size)

    async def page_todo_tasks(self, page_num: int = 1, page_size: int = 10, actor_id: Optional[str] = None):
        rows = []
        for t in self._tasks.values():
            if t.taskState != TaskState.DOING:
                continue
            if actor_id and actor_id not in self._actors.get(t.id, []):
                continue
            rows.append(self._task_row(t))
        return self._slice(rows, page_num, page_size)

    async def page_done_tasks(self, page_num: int = 1, page_size: int = 10, operator: Optional[str] = None):
        rows = []
        for t in self._tasks.values():
            if t.taskState == TaskState.DOING:
                continue
            if operator and t.actorId != operator:
                continue
            rows.append(self._task_row(t))
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
