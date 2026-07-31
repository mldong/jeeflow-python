"""jeeflow FastAPI demo —— boot2 接口规范对齐"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import Optional
from datetime import datetime

from jeeflow import EngineImpl, MemoryRepository, EventType, ProcessEvent
from jeeflow.model import InstanceState, TaskState, ProcessDefine, ProcessInstance, ProcessTask, UserInfo, parse_flow_model
from jeeflow.spi import IDGenerator, ExpressionEvaluator

# ─── Setup ───────────────────────────────────────────────────────────────────────

FLOWS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "jeeflow-java",
                         "jeeflow-core", "src", "test", "resources", "flows")

class SnowflakeIDGen(IDGenerator):
    """简化雪花 ID"""
    def __init__(self): self._epoch = 1700000000000; self._seq = 0
    def next_id(self) -> int:
        import time
        ts = int(time.time() * 1000) - self._epoch
        self._seq = (self._seq + 1) & 0xFFF
        return (ts << 10) | self._seq

class SimpleUserProvider:
    async def get_user(self, uid: str) -> Optional[UserInfo]:
        return UserInfo(userId=uid, realName=uid, deptId="D01", deptName="部门", postId="P01", postName="岗位")

class SimpleExprEvaluator(ExpressionEvaluator):
    """简易 SpEL 表达式：支持 amount >/>=/</<=/== number"""
    async def eval(self, expr: str, vars: dict):
        import re
        m = re.match(r"^\s*(\w+)\s*(>=|<=|!=|==|>|<)\s*(\d+(?:\.\d+)?)\s*$", expr)
        if not m:
            return False
        key, op, val = m.group(1), m.group(2), float(m.group(3))
        actual = vars.get(key)
        if actual is None:
            return False
        actual = float(actual)
        if op == ">": return actual > val
        if op == ">=": return actual >= val
        if op == "<": return actual < val
        if op == "<=": return actual <= val
        if op == "==": return actual == val
        if op == "!=": return actual != val
        return False

repo = MemoryRepository()
idgen = SnowflakeIDGen()
user_prov = SimpleUserProvider()
engine = EngineImpl(repo, user_prov, idgen, SimpleExprEvaluator())

# 预加载流程定义
for fname in sorted(os.listdir(FLOWS_DIR)):
    if fname.endswith(".json"):
        with open(os.path.join(FLOWS_DIR, fname), "r", encoding="utf-8") as f:
            raw = json.loads(f.read())
        d = ProcessDefine(
            name=raw.get("name", fname),
            displayName=raw.get("displayName", fname),
            type=raw.get("type", ""),
            state=1,
            content=json.dumps(raw, ensure_ascii=False),
        )
        repo.add_define(d)

app = FastAPI(title="jeeflow demo", version="0.1.0")
# CORS——允许 jeeflow-ui (localhost:5173) 跨域直连
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ─── Helpers（boot2 CommonResult：code=0 成功 / 99999999 失败，字段 code/msg/data）──

def _ok(data=None):
    return {"code": 0, "msg": "成功", "data": data}

def _err(msg: str, code=99999999):
    return JSONResponse({"code": code, "msg": msg}, status_code=200)

def _page(rows, page_num=1, page_size=999):
    return {"pageNum": page_num, "pageSize": page_size, "rows": rows, "recordCount": len(rows), "totalPage": 1}

def _fmt_time(t):
    return t.strftime("%Y-%m-%d %H:%M:%S") if t else None

def _inst_vo(inst: ProcessInstance, def_: ProcessDefine = None) -> dict:
    """ProcessInstanceVO：Entity 字段 + displayName + jsonObject + activeTaskList"""
    vo = {
        "id": inst.id, "parentId": inst.parentId, "processDefineId": inst.defineId,
        "state": inst.state, "parentNodeName": inst.parentNodeName,
        "businessNo": inst.businessNo, "operator": inst.operator,
        "expireTime": _fmt_time(inst.expireTime), "variable": json.dumps(inst.variables, ensure_ascii=False),
        "createTime": _fmt_time(inst.createTime), "createUser": inst.createUser,
        "updateTime": _fmt_time(inst.updateTime), "updateUser": inst.updateUser,
    }
    if def_:
        vo["displayName"] = def_.displayName
        vo["name"] = def_.name
        vo["version"] = def_.version
        if def_.content:
            vo["jsonObject"] = json.loads(def_.content)
    vo["activeTaskList"] = [_task_vo(t) for t in inst.tasks if t.taskState == TaskState.DOING]
    return vo

def _task_vo(t: ProcessTask, inst: ProcessInstance = None, def_: ProcessDefine = None) -> dict:
    """ProcessTaskVO：Entity 字段 + 展示字段"""
    vo = {
        "id": t.id, "processInstanceId": t.processInstanceId,
        "taskName": t.taskName, "displayName": t.displayName,
        "taskType": t.taskType, "performType": t.performType,
        "taskState": t.taskState, "operator": t.actorId,
        "finishTime": _fmt_time(t.finishTime), "expireTime": _fmt_time(t.expireTime),
        "formKey": t.formKey, "taskParentId": t.parentTaskId,
        "variable": json.dumps(t.variables, ensure_ascii=False),
        "createTime": _fmt_time(t.createTime), "createUser": t.createUser,
        "updateTime": _fmt_time(t.updateTime), "updateUser": t.updateUser,
    }
    if inst and def_:
        vo["processDefineName"] = def_.name
        vo["processDefineDisplayName"] = def_.displayName
        vo["instanceCreateTime"] = _fmt_time(inst.createTime)
    vo["taskActorIdList"] = list(t.actorIds)
    return vo

def _load_graph(define_id) -> Optional[dict]:
    d = repo._defines.get(define_id)
    return json.loads(d.content) if d and d.content else None

# boot2 submitType 枚举
APPLY, AGREE, REJECT, ROLLBACK, JUMP, RE_APPLY = 0, 1, 2, 3, 4, 5
ROLLBACK_TO_OPERATOR, COUNTERSIGN_DISAGREE = 6, 20

# ─── 流程定义 ────────────────────────────────────────────────────────────────────

@app.post("/wf/processDefine/page")
async def define_page(request: Request):
    body = await request.json() if request.method == "POST" else {}
    rows = []
    for d in repo.all_defines():
        rows.append({"id": d.id, "name": d.name, "displayName": d.displayName,
                     "type": d.type, "state": d.state, "version": d.version,
                     "createTime": _fmt_time(d.createTime), "updateTime": _fmt_time(d.updateTime)})
    return _ok(_page(rows))

@app.post("/wf/processDefine/detail")
async def define_detail(request: Request):
    body = await request.json()
    id = int(body.get("id", 0))
    d = await repo.find_define_by_id(id)
    if not d: return _err("流程定义不存在")
    graph = json.loads(d.content) if d.content else None
    return _ok({"id": d.id, "name": d.name, "displayName": d.displayName, "type": d.type,
                "state": d.state, "version": d.version, "jsonObject": graph})

@app.post("/wf/processDefine/startAndExecute")
async def define_start_and_execute(request: Request):
    """启动流程实例（boot2 主入口）"""
    return await _start_and_execute_body(request)

# ─── 流程实例 ────────────────────────────────────────────────────────────────────

@app.post("/wf/processInstance/startAndExecute")
async def instance_start_and_execute(request: Request):
    return await _start_and_execute_body(request)

async def _start_and_execute_body(request: Request):
    body = await request.json()
    define_id = int(body.get("processDefineId", 0))
    operator = str(body.get("operator", "user1"))
    args = dict(body)
    args.pop("processDefineId", None)
    inst = await engine.start_process_instance_by_id(define_id, operator, args)
    # boot2 startAndExecute：自动完成申请节点
    doing = await repo.find_doing_tasks(inst.id)
    for task in doing:
        await repo.add_task_actor(task.id, [operator])
        await engine.execute_process_task(task.id, operator, {**args, "submitType": APPLY})
    return _ok()

@app.post("/wf/processInstance/page")
async def instance_page(request: Request):
    body = await request.json()
    user_id = str(body.get("operator", body.get("userId", "user1")))
    rows = []
    for i in repo.all_instances():
        if i.createUser != user_id: continue
        d = await repo.find_define_by_id(i.defineId)
        rows.append(_inst_vo(i, d))
    rows.sort(key=lambda x: x["id"], reverse=True)
    return _ok(_page(rows))

@app.post("/wf/processInstance/detail")
async def instance_detail(request: Request):
    body = await request.json()
    id = int(body.get("id", 0))
    inst = await repo.find_instance_by_id(id)
    if not inst: return _err("实例不存在")
    d = await repo.find_define_by_id(inst.defineId)
    return _ok(_inst_vo(inst, d))

@app.post("/wf/processInstance/highLight")
async def instance_highlight(request: Request):
    """高亮数据（独立端点）"""
    body = await request.json()
    id = int(body.get("id", 0))
    inst = await repo.find_instance_by_id(id)
    if not inst: return _err("实例不存在")
    d = await repo.find_define_by_id(inst.defineId)
    finished = set(); active = set()
    for t in inst.tasks:
        if t.taskState == TaskState.DONE: finished.add(t.taskName)
        if t.taskState == TaskState.DOING: active.add(t.taskName)
    finished_edges = []
    graph = _load_graph(inst.defineId)
    if graph:
        for e in graph.get("edges", []):
            if e.get("sourceNodeId") in finished and e.get("targetNodeId") in finished:
                finished_edges.append(e.get("id"))
    return _ok({"historyNodeNames": list(finished), "historyEdgeNames": finished_edges,
                "activeNodeNames": list(active)})

@app.post("/wf/processInstance/approvalRecord")
async def instance_approval_record(request: Request):
    """审批记录（独立端点）"""
    body = await request.json()
    id = int(body.get("id", 0))
    inst = await repo.find_instance_by_id(id)
    if not inst: return _err("实例不存在")
    d = await repo.find_define_by_id(inst.defineId)
    records = [_task_vo(t, inst, d) for t in sorted(inst.tasks, key=lambda x: x.id)]
    return _ok(records)

# ─── 流程任务 ────────────────────────────────────────────────────────────────────

@app.post("/wf/processTask/todoList")
async def todo_list(request: Request):
    body = await request.json()
    user_id = str(body.get("userId", body.get("operator", "user1")))
    rows = []
    for t in repo.all_tasks():
        actors = repo._actors.get(t.id, [])
        if t.taskState != TaskState.DOING or user_id not in actors:
            continue
        inst = await repo.find_instance_by_id(t.processInstanceId)
        d = await repo.find_define_by_id(inst.defineId) if inst else None
        rows.append(_task_vo(t, inst, d))
    rows.sort(key=lambda x: x["id"], reverse=True)
    return _ok(_page(rows))

@app.post("/wf/processTask/doneList")
async def done_list(request: Request):
    body = await request.json()
    user_id = str(body.get("userId", body.get("operator", "user1")))
    rows = []
    for t in repo.all_tasks():
        actors = repo._actors.get(t.id, [])
        if t.taskState != TaskState.DONE or (user_id not in actors and t.actorId != user_id):
            continue
        inst = await repo.find_instance_by_id(t.processInstanceId)
        d = await repo.find_define_by_id(inst.defineId) if inst else None
        rows.append(_task_vo(t, inst, d))
    rows.sort(key=lambda x: x["id"], reverse=True)
    return _ok(_page(rows))

@app.post("/wf/processTask/execute")
async def execute_task(request: Request):
    """处理待办（boot2 submitType 全枚举）"""
    body = await request.json()
    task_id = int(body.get("processTaskId", 0))
    operator = str(body.get("operator", "user1"))
    submit_type = int(body.get("submitType", AGREE))
    args = dict(body)
    args.pop("processTaskId", None)
    try:
        if submit_type == REJECT:                       # 2 拒绝 → 跳结束（实例→45）
            await engine.execute_and_jump_to_end(task_id, operator, args)
        elif submit_type == ROLLBACK:                   # 3 退回上一步（回溯上一任务节点）
            await _rollback_to_prev(task_id, operator, args)
        elif submit_type == JUMP:                       # 4 跳指定节点
            await engine.execute_and_jump_task(task_id, operator, args, args.get("taskName"))
        elif submit_type == ROLLBACK_TO_OPERATOR:       # 6 退回发起人（第一个任务节点）
            await engine.execute_and_jump_to_first_task_node(task_id, operator, args)
        elif submit_type == COUNTERSIGN_DISAGREE:       # 20 会签不同意
            args["countersignDisagreeFlag"] = 1
            await engine.execute_process_task(task_id, operator, args)
        else:                                           # 0/1/5 及默认 → 执行
            await engine.execute_process_task(task_id, operator, args)
        return _ok()
    except Exception as e:
        return _err(str(e))

async def _rollback_to_prev(task_id: int, operator: str, args: dict):
    """退回上一步：找到当前任务节点的上一个任务节点并跳转"""
    task = await repo.find_task_by_id(task_id)
    inst = await repo.find_instance_by_id(task.processInstanceId)
    graph = _load_graph(inst.defineId)
    if not graph: return await engine.execute_and_jump_to_end(task_id, operator, args)
    # 找当前节点
    prev = None
    for e in graph.get("edges", []):
        if e.get("targetNodeId") == task.taskName:
            prev = e.get("sourceNodeId")
            break
    # 沿 prev 回溯到任务节点
    target = prev
    seen = set()
    while target:
        if target in seen: break
        seen.add(target)
        node = next((n for n in graph["nodes"] if n["id"] == target), None)
        if not node: break
        if node["type"] in ("snaker:task", "snaker:custom"):
            break
        # 非任务节点继续向上找
        found = None
        for e in graph.get("edges", []):
            if e.get("targetNodeId") == target:
                found = e.get("sourceNodeId"); break
        target = found
    if target:
        await engine.execute_and_jump_task(task_id, operator, args, target)
    else:
        await engine.execute_and_jump_to_end(task_id, operator, args)

@app.post("/wf/processTask/jumpAbleTaskNameList")
async def jump_able_task_name_list(request: Request):
    """可跳转的任务节点名称"""
    body = await request.json()
    instance_id = int(body.get("processInstanceId", 0))
    done = await repo.find_done_tasks(instance_id)
    seen = set()
    rows = []
    for t in done:
        if t.taskName not in seen:
            seen.add(t.taskName)
            rows.append({"label": t.displayName, "value": t.taskName})
    return _ok(rows)

# ─── 仪表盘统计（UI 用，非 boot2 端点）───────────────────────────────────────────

@app.get("/api/stats")
async def api_stats(userId: str = "user1"):
    tasks = [t for t in repo.all_tasks() if t.taskState == TaskState.DOING and userId in repo._actors.get(t.id, [])]
    insts = [i for i in repo.all_instances() if i.createUser == userId]
    return _ok({"todoCount": len(tasks), "myInstanceCount": len(insts)})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("demo.main:app", host="0.0.0.0", port=8100, reload=True)
