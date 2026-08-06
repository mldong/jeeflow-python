# FastAPI 集成

`jeeflow-python` 内置 FastAPI 演示模块（`demo/main.py`），可作为集成参考：路由、CORS、依赖注入的完整范例。

## 最小集成

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from jeeflow import EngineImpl, MemoryRepository

app = FastAPI(title="my-jeeflow")
# CORS——允许前端跨域直连（生产改为指定域名）
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

repo = MemoryRepository()          # 生产替换为你的仓储实现（见 SPI 指南）
engine = EngineImpl(repo, user_prov, idgen, expr_eval)
```

## REST 端点

demo 提供了完整的 mldong 框架兼容端点（`/wf/processDefine/*`、`/wf/processInstance/*`、`/wf/processTask/*`、`/api/stats`），直接复制或按需裁剪：

```python
@app.post("/wf/processDefine/startAndExecute")
async def start_and_execute(request: Request):
    body = await request.json()
    inst = await engine.start_process_instance_by_id(
        int(body["processDefineId"]), body["operator"], body)
    # startAndExecute 契约：自动完成申请节点
    for task in await repo.find_doing_tasks(inst.id):
        await repo.add_task_actor(task.id, [body["operator"]])
        await engine.execute_process_task(task.id, body["operator"], {"submitType": 0})
    return {"code": 0, "msg": "成功", "data": None}
```

端点清单与响应结构（code=0/msg、submitType 全枚举）见[统一门面接口文档](../../spec/06-facade)。

## 启动

```bash
uvicorn demo.main:app --host 0.0.0.0 --port 8100 --reload   # 开发
uvicorn demo.main:app --host 0.0.0.0 --port 8100 --workers 4  # 生产
```

> 完整示例：`demo/main.py`（含 CORS、VO 转换、submitType 全枚举 switch、highLight/approvalRecord 独立端点）。
