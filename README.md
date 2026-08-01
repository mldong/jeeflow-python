# jeeflow · Python

轻量级异步工作流引擎 — Python 实现，对齐 [jeeflow SPEC](https://jeeflow-doc.mldong.com/spec/)。

## 快速开始

```bash
pip install jeeflow

# 启动 demo
cd demo
python main.py
# → http://localhost:8100
```

## 项目结构

```
jeeflow/
├── model.py        # 域类型 + LogicFlow JSON 解析
├── spi.py          # SPI 接口（ProcessRepository / UserProvider / IDGenerator / ExpressionEvaluator）
├── engine.py       # 引擎核心（EngineImpl）
├── extensions.py   # 扩展体系（拦截器 / 事件 / HandlerRegistry）
├── memory.py       # 内存仓储（MemoryRepository）
├── repository/     # JDBC 多库实现（base 共享核心 + mysql/postgres 适配器）
├── __init__.py
tests/
├── spec_test.py    # 10 项 SPEC 合规测试
demo/
├── main.py         # FastAPI 演示站
├── web/
│   └── index.html  # 前端仪表盘
pyproject.toml      # 项目配置
```

## 引擎使用

```python
from jeeflow import EngineImpl, MemoryRepository
from jeeflow.model import parse_flow_model
import json

repo = MemoryRepository()
engine = EngineImpl(repo)

# 加载流程定义（LogicFlow JSON）
with open("flow.json") as f:
    flow = parse_flow_model(json.load(f))

# 启动流程
inst = await engine.start_process_instance_by_id(define_id, "user1", {"amount": 3000})

# 完成任务
inst = await engine.execute_process_task(task_id, "user1")

# 驳回
inst = await engine.execute_and_jump_to_end(task_id, "user1")

# 跳转
inst = await engine.execute_and_jump_task(task_id, "user1", target_task_name="previous")
```

## 扩展

```python
from jeeflow import EngineExtensions, FlowInterceptor, HandlerRegistry, EventType

class MyInterceptor(FlowInterceptor):
    async def pre_handle(self, node, inst): return True
    async def post_handle(self, node, inst): pass
    @property
    def order(self): return 10

async def on_event(evt):
    print(f"[{evt.type.value}] instance={evt.instanceId}")

engine.set_extensions(EngineExtensions(
    interceptors=[MyInterceptor()],
    event_listener=on_event,
    registry=HandlerRegistry(),
))
```

## SPI 接口

| 接口 | 说明 | 必须 |
|------|------|------|
| `ProcessRepository` | 流程数据持久化 | ✅ |
| `UserProvider` | 用户信息获取 | 可选 |
| `IDGenerator` | ID 生成（默认时间戳） | 可选 |
| `ExpressionEvaluator` | 决策表达式求值 | 可选 |

## 运行测试

```bash
pip install pytest pytest-asyncio
pytest tests/ -v
# 10 passed
```

## 与其他语言版本对比

| 特性 | Java | Go | Node.js | Python |
|------|------|-----|---------|--------|
| 引擎核心 | ✅ | ✅ | ✅ | ✅ |
| 拦截器 | ✅ | ✅ | ✅ | ✅ |
| 事件监听 | ✅ | ✅ | ✅ | ✅ |
| HandlerRegistry | ✅ | ✅ | ✅ | ✅ |
| 会签 | ✅ | ✅ | ✅ | ✅ |
| 决策表达式 | ✅ | ✅ | ✅ | ✅ |
| Fork/Join | ✅ | ✅ | ✅ | ✅ |
| 驳回/跳转 | ✅ | ✅ | ✅ | ✅ |
| Demo + 前端 | ✅ | ✅ | ✅ | ✅ |
| SPEC 合规测试 | ✅ | ✅ | ✅ | ✅ |

## 许可

Apache 2.0
