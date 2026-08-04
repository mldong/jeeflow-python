# 业务数据入库（persist 组件）

> issues/18 · 1.6.2 起随主包发布（`jeeflow.persist` 模块）

`persist.py`：**引擎无关的动态表写入组件 + 工作流入库适配拦截器**。
规范契约见文档站《09 · 业务数据通用入库》；本页是 Python 语言视角。

## 引入

```python
from jeeflow.persist import JdbcDynamicTableWriter, PersistPostInterceptor
```

## 动态表写入（引擎无关）

```python
conn = sqlite3.connect("biz.db")  # 或 pymysql / psycopg2 连接
writer = JdbcDynamicTableWriter(conn)  # sqlite3 连接自动识别方言，其余走 information_schema

# ① 列过滤
kept = writer.filter_columns("biz_leave", ["title", "ghost_col"])
# ② 幂等检查
ok = writer.exists("biz_leave", "process_instance_id", inst_id)
# ③ 系统字段填充 + 参数化插入
data = {"title": "年假申请"}
writer.fill_system_fields(data, True)
writer.insert("biz_leave", data)
```

安全：`sys_` 前缀表拒绝写入；非法字符表名拒绝；值走参数化占位符。
列匹配（1.6.4）：默认宽松——驼峰表单字段 ↔ 下划线表列（`companyName` → `company_name`），严格模式可配置。

## 流程入库拦截器（流程结束同意自动落表）

```python
writer = JdbcDynamicTableWriter(conn)
ic = PersistPostInterceptor(writer=writer, loader=repo.find_define_by_id)  # loader 透传 findDefineById
eng.set_extensions(EngineExtensions(interceptors=[ic]))
```

- 拦截器挂在引擎全局 Extensions；内部按「结束节点 + 实例 DONE + submitType=AGREE」过滤，
  仅对流程定义顶层声明了 `relTableName`（缺省回落流程 name）的流程生效
- 语义：实例 `f_` 字段（去前缀）+ 流程上下文（`process_instance_id`/`apply_user_id`/`apply_dept_id`）
  + 系统字段写入业务表；`process_instance_id` 幂等（先查后插）+ 同链内存标记（1.6.3，共享 instance.variables，不落库）；用户列默认取 operator（1.6.3）；表不存在显性报错（ValueError，
  配置错误快速失败）；不同意/退回不入库
- 引擎对齐（1.6.2）：任务完成后结束节点统一走 `_execute_node`，拦截器在流程结束时完整触发

## 测试

```bash
python -m pytest tests/test_persist.py -q   # 9 用例：writer 4 + 拦截器集成 5（SQLite 内存库全链路）
```
