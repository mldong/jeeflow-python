# jeeflow-python 文档

> jeeflow 引擎的 **Python 实现**——对齐 Java 参考实现的行为语义。本文档面向 Python 开发者，内容也聚合到[文档站语言指南](../../)。

## 快速开始

| 文档 | 内容 |
|------|------|
| [SDK 集成（快速开始）](./getting-started.md) | 安装、最小示例（内存模式 5 行跑起来） |
| [演示站（Demo）](./index.md) | 启动演示站（:8100）、快速验证、测试、生产部署 |
| [引擎 API](./engine-api.md) | `EngineImpl` 核心方法（异步风格） |
| [流程定义格式](./flow-definition.md) | LogicFlow JSON 结构、节点类型、加载 |
| [SPI 实现指南](./spi-guide.md) | `ProcessRepository` / `UserProvider` 等 SPI |
| [FastAPI 集成](./fastapi.md) | FastAPI 应用接入（路由/CORS/端点） |

## 相关

- 引擎规范（唯一事实来源）：[SPEC](../../spec/)
- 设计原理 / 通用指南：[jeeflow-doc](../../)
