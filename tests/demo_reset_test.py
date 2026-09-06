"""demo /api/reset 契约测试（issues/11）：清空业务数据并重载种子。

T003 起 reset 会复跑业务种子 driver（引擎真实启动 16 进行中 + 9 已完成 + 8 委托），
断言从「清空」改为「回到矩阵规模」。
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "demo"))

from main import repo, ext_repo, api_reset  # noqa: E402


async def test_reset_clears_and_reseeds():
    # 造点数据：一个实例 + 一个任务 + 一条委托
    before_defines = len(repo._defines)
    assert before_defines > 0
    repo._instances[999999] = object()
    repo._tasks[999999] = object()

    await api_reset()

    # T003：reset 后 = 完整业务种子矩阵（16 进行中 + 9 已完成），非空库
    assert len(repo._instances) == 25, f"reset 后实例应回到矩阵规模 25，实际 {len(repo._instances)}"
    assert len(repo._tasks) > 0, "reset 后任务应随业务种子重建"
    assert len(repo._actors) > 0, "reset 后参与者应随业务种子重建"
    assert len(repo._cc) > 0, "reset 后抄送应随业务种子重建"
    assert len(ext_repo._surrogates) == 8, f"reset 后委托应为 8 条，实际 {len(ext_repo._surrogates)}"
    assert len(repo._defines) == before_defines, "种子定义应重载"

    # 落点：16 state=10 + 9 state=20
    states = [inst.state for inst in repo._instances.values()]
    assert states.count(10) == 16, f"进行中应 16 条，实际 {states.count(10)}"
    assert states.count(20) == 9, f"已完成应 9 条，实际 {states.count(20)}"
