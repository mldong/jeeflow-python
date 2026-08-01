# Python 快速开始

> jeeflow 引擎的 **Python 实现**——对齐 Java 参考实现的行为语义。本文面向 Python 开发者：安装、启动演示站、跑测试、生产部署。

## 环境要求

- Python 3.10+
- 依赖：`fastapi`、`uvicorn`（引擎核心零第三方依赖，纯异步标准库）

## 启动演示站（:8100）

```bash
pip install fastapi uvicorn
python demo/main.py
# → http://localhost:8100（uvicorn 热重载）
```

> 演示站从 `jeeflow-java` 的共享流程 JSON 加载 10 个示例流程。对接 jeeflow-ui（:5173）时右上角切到 `🐍 Python :8100`；接口规范见 [文档站 REST API 指南](https://jeeflow-doc.mldong.com/guides/03-api)。

## 快速验证

```bash
B=http://localhost:8100
curl -s -X POST $B/wf/processDefine/page -H "Content-Type: application/json" -d '{}'   # → {"code":0,"msg":"成功",...}
curl -s -X POST $B/wf/processDefine/startAndExecute -H "Content-Type: application/json" -d '{"processDefineId":9,"operator":"user1","amount":500}'
```

完整验证矩阵（同意/拒绝/退回发起人/highLight/approvalRecord）见文档站通用指南。

## 运行测试

```bash
python tests/spec_test.py   # 引擎合规测试 10 项
python tests/e2e_test.py    # 接口端到端测试 34 项
```

## 生产部署

```bash
uvicorn demo.main:app --host 0.0.0.0 --port 8100 --workers 4
```

生产接入：实现 `ProcessRepository` SPI（内存/DB 随意），映射 [SPEC §2](https://jeeflow-doc.mldong.com/spec/) 的 5 张表。
