# 快速开始（SDK 集成）

> 把 jeeflow-python 作为依赖集成到你的项目。演示站（FastAPI 应用）见 [演示站（Demo）](./demo.md)。

## 安装

```bash
# ⚠️ 尚未发布到 PyPI——源码方式使用：
git clone https://github.com/mldong/jeeflow-python
# 你的项目里直接引用源码目录（或后续发布后 pip install jeeflow）
```

引擎核心零第三方依赖（纯异步标准库），只需 Python 3.10+。

## 最小示例（内存模式，5 行跑起来）

不依赖任何数据库，适合学习、测试：

```python
import asyncio, json
from jeeflow import EngineImpl, MemoryRepository
from jeeflow.model import ProcessDefine

async def main():
    repo = MemoryRepository()
    # 1. 注册流程定义（LogicFlow JSON，见流程定义格式）
    repo.add_define(ProcessDefine(
        name="simple", displayName="简单审批", type="approval", state=1,
        content=json.dumps({...flow_json...}, ensure_ascii=False),
    ))
    # 2. 初始化引擎（仓储必传，其余 SPI 可选）
    engine = EngineImpl(repo)
    # 3. 启动流程（startAndExecute 契约：调用方自动完成申请节点）
    inst = await engine.start_process_instance_by_id(1, "user1", {})
    for task in await repo.find_doing_tasks(inst.id):
        await repo.add_task_actor(task.id, ["user1"])
        await engine.execute_process_task(task.id, "user1", {"submitType": 0})
    print(inst.state)  # 10 进行中

asyncio.run(main())
```

## 下一步

- [引擎 API](./engine-api.md) —— `EngineImpl` 全部方法
- [流程定义格式](./flow-definition.md) —— LogicFlow JSON
- [SPI 实现指南](./spi-guide.md) —— 接入自己的数据库/用户体系
