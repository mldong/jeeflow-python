"""jeeflow FastAPI demo"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
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
        print(f"  loaded: {d.id} {d.displayName}")

app = FastAPI(title="jeeflow demo", version="0.1.0")
# CORS——允许 jeeflow-ui (localhost:5173) 跨域直连
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ─── Helpers ─────────────────────────────────────────────────────────────────────

def _ok(data=None):
    return {"code": 200, "message": "成功", "data": data}

def _err(msg: str, code=500):
    return JSONResponse({"code": code, "message": msg}, status_code=200)

def _page(rows, page_num=1, page_size=999):
    return {"pageNum": page_num, "pageSize": page_size, "rows": rows, "recordCount": len(rows), "totalPage": 1}

# boot2 submitType: 0=APPLY, 1=AGREE, 2=REJECT
APPLY, AGREE, REJECT = 0, 1, 2

# ─── API (boot2 兼容) ────────────────────────────────────────────────────────────

@app.get("/api/stats")
async def api_stats(userId: str = "user1"):
    tasks = [t for t in repo.all_tasks() if t.taskState == TaskState.DOING and userId in repo._actors.get(t.id, [])]
    insts = [i for i in repo.all_instances() if i.createUser == userId]
    return _ok({"todoCount": len(tasks), "myInstanceCount": len(insts)})

# 流程定义列表（boot2: POST /wf/processDefine/page）
@app.post("/wf/processDefine/page")
async def define_page(request: Request):
    body = await request.json() if request.method == "POST" else {}
    rows = []
    for d in repo.all_defines():
        rows.append({"id": d.id, "name": d.name, "displayName": d.displayName, "type": d.type, "state": d.state, "version": d.version})
    return _ok(_page(rows))

# 流程定义详情（boot2: POST /wf/processDefine/detail）
@app.post("/wf/processDefine/detail")
async def define_detail(request: Request):
    body = await request.json()
    id = int(body.get("id", 0))
    d = await repo.find_define_by_id(id)
    if not d: return _err("流程定义不存在")
    graph = json.loads(d.content) if d.content else None
    return _ok({"id": d.id, "name": d.name, "displayName": d.displayName, "type": d.type,
                "state": d.state, "version": d.version, "graphData": graph})

@app.get("/api/process-define/list")
async def api_define_list():
    return _ok([{"id": d.id, "name": d.name, "displayName": d.displayName, "type": d.type} for d in repo.all_defines()])

@app.get("/api/process-define/{id}")
async def api_define_detail(id: int):
    d = await repo.find_define_by_id(id)
    if not d: return _err("流程定义不存在")
    graph = json.loads(d.content) if d.content else None
    return _ok({"id": d.id, "name": d.name, "displayName": d.displayName, "type": d.type, "graphData": graph})

# 启动并自动执行第一个任务（boot2: POST /wf/processInstance/startAndExecute）
@app.post("/wf/processInstance/startAndExecute")
async def start_and_execute(request: Request):
    body = await request.json()
    define_id = int(body.get("processDefineId", 0))
    operator = str(body.get("operator", "user1"))
    args = dict(body)
    args.pop("processDefineId", None)

    # 1. 启动流程——"applicant"解析为 operator
    inst = await engine.start_process_instance_by_id(define_id, operator, args)

    # 2. 获取当前待办，自动执行（boot2 契约：发起申请节点自动完成）
    doing = await repo.find_doing_tasks(inst.id)
    for task in doing:
        await repo.add_task_actor(task.id, [operator])
        task_args = {**args, "submitType": APPLY}
        await engine.execute_process_task(task.id, operator, task_args)

    return _ok({"processInstanceId": str(inst.id)})

# 流程实例列表（boot2: POST /wf/processInstance/page）
@app.post("/wf/processInstance/page")
async def instance_page(request: Request):
    body = await request.json()
    user_id = str(body.get("operator", body.get("userId", "user1")))
    rows = []
    for i in repo.all_instances():
        if i.createUser != user_id: continue
        d = await repo.find_define_by_id(i.defineId)
        rows.append({
            "id": i.id, "processDefineId": i.defineId, "state": i.state,
            "operator": i.operator, "businessNo": i.businessNo,
            "createTime": str(i.createTime),
            "processDefineDisplayName": d.displayName if d else "",
        })
    rows.sort(key=lambda x: x["id"], reverse=True)
    return _ok(_page(rows))

# 流程实例详情（boot2: POST /wf/processInstance/detail）
@app.post("/wf/processInstance/detail")
async def instance_detail(request: Request):
    body = await request.json()
    id = int(body.get("id", 0))
    inst = await repo.find_instance_by_id(id)
    if not inst: return _err("实例不存在")
    d = await repo.find_define_by_id(inst.defineId)

    records = []
    finished_nodes = set()
    active_nodes = set()
    for t in repo.all_tasks():
        if t.processInstanceId != id: continue
        records.append({
            "id": t.id, "taskName": t.taskName, "displayName": t.displayName,
            "taskState": t.taskState, "operator": t.actorId,
            "createTime": str(t.createTime), "finishTime": str(t.finishTime),
        })
        if t.taskState == TaskState.DONE: finished_nodes.add(t.taskName)
        if t.taskState == TaskState.DOING: active_nodes.add(t.taskName)

    graph_data = None
    finished_edges = []
    if d and d.content:
        try:
            graph_data = json.loads(d.content)
            for e in graph_data.get("edges", []):
                if e.get("sourceNodeId") in finished_nodes and e.get("targetNodeId") in finished_nodes:
                    finished_edges.append(e.get("id"))
        except: pass

    return _ok({
        "id": str(inst.id), "state": inst.state, "operator": inst.operator,
        "businessNo": inst.businessNo, "createTime": str(inst.createTime),
        "defineName": d.displayName if d else "",
        "graphData": graph_data,
        "approvalRecords": records,
        "highLight": {
            "historyNodeNames": list(finished_nodes),
            "historyEdgeNames": finished_edges,
            "activeNodeNames": list(active_nodes),
        },
    })

# 待办列表（boot2: POST /wf/processTask/todoList）
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
        rows.append({
            "id": t.id, "processInstanceId": t.processInstanceId,
            "taskName": t.taskName, "displayName": t.displayName,
            "taskState": t.taskState, "formKey": t.formKey,
            "createTime": str(t.createTime),
            "processDefineDisplayName": d.displayName if d else "",
        })
    rows.sort(key=lambda x: x["id"], reverse=True)
    return _ok(_page(rows))

# 执行任务（boot2: POST /wf/processTask/execute）
@app.post("/wf/processTask/execute")
async def execute_task(request: Request):
    body = await request.json()
    task_id = int(body.get("processTaskId", 0))
    operator = str(body.get("operator", "user1"))
    submit_type = int(body.get("submitType", AGREE))

    try:
        if submit_type == REJECT:
            # boot2 驳回：跳回 "apply" 节点，由 applicant 解析为发起人
            await engine.execute_and_jump_task(task_id, operator, target_task_name="apply")
        else:
            await engine.execute_process_task(task_id, operator, body)
        return _ok({"message": "处理成功"})
    except Exception as e:
        return _err(str(e))

# 已审批列表（boot2: POST /wf/processTask/doneList）
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
        rows.append({
            "id": t.id, "processInstanceId": t.processInstanceId,
            "taskName": t.taskName, "displayName": t.displayName,
            "taskState": t.taskState, "createTime": str(t.createTime),
            "finishTime": str(t.finishTime),
            "processDefineDisplayName": d.displayName if d else "",
        })
    rows.sort(key=lambda x: x["id"], reverse=True)
    return _ok(_page(rows))


# ─── Static ──────────────────────────────────────────────────────────────────────

WEB_DIR = os.path.join(os.path.dirname(__file__), "web")
os.makedirs(WEB_DIR, exist_ok=True)

# mount static if web dir has content
if os.path.isdir(WEB_DIR) and os.listdir(WEB_DIR):
    app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("demo.main:app", host="0.0.0.0", port=8100, reload=True)
