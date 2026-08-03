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

from jeeflow import EngineImpl, MemoryRepository, EventType, ProcessEvent, JeeflowFacade
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
facade = JeeflowFacade(engine, repo, None)  # demo 未接入扩展仓储

def load_seed():
    """预加载流程定义（种子）——/api/reset 重置后复用"""
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


load_seed()

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

@app.post("/wf/{action:path}")
async def wf_flow(action: str, request: Request):
    """单入口门面转发（v1.5.0）：/wf/{action}，action 多段（如 processDefine/page）"""
    body = await request.json() if await request.body() else {}
    return await facade.flow(action, body)

@app.post("/api/reset")
async def api_reset():
    """一键重置演示数据（issues/11）：清空内存库（实例/任务/抄送/参与者）+ 重载种子流程定义"""
    repo._defines.clear()
    repo._instances.clear()
    repo._tasks.clear()
    repo._actors.clear()
    repo._cc.clear()
    repo._seq = 1
    load_seed()
    return _ok()


@app.get("/api/stats")
async def api_stats(userId: str = "user1"):
    tasks = [t for t in repo.all_tasks() if t.taskState == TaskState.DOING and userId in repo._actors.get(t.id, [])]
    insts = [i for i in repo.all_instances() if i.createUser == userId]
    return _ok({"todoCount": len(tasks), "myInstanceCount": len(insts)})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("demo.main:app", host="0.0.0.0", port=8100, reload=True)
