"""内存仓储——测试用"""
from copy import deepcopy
from .model import ProcessDefine, ProcessInstance, ProcessTask, TaskState
from .spi import ProcessRepository

class MemoryRepository(ProcessRepository):
    def __init__(self):
        self._defines: dict[int, ProcessDefine] = {}
        self._instances: dict[int, ProcessInstance] = {}
        self._tasks: dict[int, ProcessTask] = {}
        self._actors: dict[int, list[str]] = {}
        self._seq = 1

    def add_define(self, d: ProcessDefine):
        d.id = d.id or self._seq; self._seq += 1
        self._defines[d.id] = d

    async def find_define_by_id(self, id): return deepcopy(self._defines.get(id))
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
    async def create_cc_instance(self, *args): pass
    async def update_cc_status(self, *args): pass

    def all_defines(self): return list(self._defines.values())
    def all_instances(self): return list(self._instances.values())
    def all_tasks(self):
        return [deepcopy(t) for t in self._tasks.values()]
