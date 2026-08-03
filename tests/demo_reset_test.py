"""demo /api/reset 契约测试（issues/11）：清空实例/任务并重载种子"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "demo"))

from main import repo, api_reset  # noqa: E402


async def test_reset_clears_and_reseeds():
    # 造点数据：一个实例 + 一个任务
    before_defines = len(repo._defines)
    assert before_defines > 0
    repo._instances[1] = object()
    repo._tasks[1] = object()

    await api_reset()

    assert len(repo._instances) == 0, "实例应清空"
    assert len(repo._tasks) == 0, "任务应清空"
    assert len(repo._actors) == 0, "参与者应清空"
    assert len(repo._cc) == 0, "抄送应清空"
    assert len(repo._defines) == before_defines, "种子定义应重载"
