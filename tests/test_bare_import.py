"""裸装回归：不安装可选依赖（aiomysql/asyncpg）时 `import jeeflow` 必须成功。

> 背景（1.8.28 修复）：mysql.py 曾顶层 `import aiomysql`，dev 环境永远装着
> dev extras 所以测试全绿，用户裸装后第一行 import 即 ModuleNotFoundError。
> 本测试在子进程里把可选依赖顶成 ImportError（sys.modules 置 None 即可让
> `import` 语句抛 ImportError），再断言核心入口与两个适配器类照常可导入
> （对齐 postgres.py 先例：类型标注惰性化，适配器类本身不依赖三方包）。
"""
import os
import subprocess
import sys

BLOCK_CODE = """
import sys
sys.modules["aiomysql"] = None
sys.modules["asyncpg"] = None
import jeeflow
from jeeflow import EngineImpl, MemoryRepository, JdbcRepository, TsIDGenerator
assert isinstance(jeeflow.MySqlAdapter, type), "aiomysql 缺失时 MySqlAdapter 类也应可导入"
assert isinstance(jeeflow.PostgresAdapter, type), "asyncpg 缺失时 PostgresAdapter 类也应可导入"
print("bare-import ok")
"""


def test_import_without_optional_deps():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    r = subprocess.run([sys.executable, "-c", BLOCK_CODE], cwd=root,
                       capture_output=True, text=True)
    assert r.returncode == 0, f"裸装 import 炸：{r.stderr}"
    assert "bare-import ok" in r.stdout
