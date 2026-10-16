"""audit8 R77 读法一（python 腿）：可选「提交后腿」通道。

契约下限（owner 10-16 拍，spec12 §12.3.1／issues/234 §4.3）＝"监听器看到的世界＝已提交"由
**监听器侧**承担：引擎派发点不动（145-3 钉着"监听器随事件拿到动作事务连接"），要提交后语义就自己
经 ``EngineImpl.run_after_commit`` 声明 ⇒ COMMIT 后执行、回滚整批丢弃；无活动事务 ⇒ 立即执行。
php 侧 R25（issues/191）同形，java 参考实现＝事务内 fire＋监听器自取 afterCommit。

三档各打一件事：
  ① 仓储没这条通道（内存仓）⇒ 立即执行（行为逐字不变）；
  ② 载体登记成功 ⇒ 事务内不跑、提交后恰好一次，且**跑在事务上下文已解绑之后**
     （反查不得踩在已 COMMIT 的连接上——这是把 fire 挪到提交后最容易做错的一格）；
  ③ 回滚腿 ⇒ 整批丢弃（修前无通道＝这一腿只能在事务内跑，回滚了也已经跑过）。
②③ 走 ``JdbcRepository.with_tx`` 本体（只替掉 adapter/连接），不绕过生产代码路径。
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository
from jeeflow.repository.base import JdbcRepository, current_tx_conn


class _FakeConn:
    def __init__(self, log): self.log = log
    async def begin(self): self.log.append("begin")
    async def commit(self): self.log.append("commit")
    async def rollback(self): self.log.append("rollback")


class _FakeAdapter:
    placeholder = "?"

    def __init__(self, log): self.log = log
    async def acquire(self): return _FakeConn(self.log)
    async def release(self, conn): self.log.append("release")


def _repo():
    return JdbcRepository(_FakeAdapter([]))


def _engine(repo):
    return EngineImpl(repo, None, None, None)


@pytest.mark.asyncio
async def test_r77_01_immediate_without_channel():
    """① 内存仓没有 after_commit ⇒ 立即执行"""
    engine = _engine(MemoryRepository())
    ran = []
    await engine.run_after_commit(lambda: ran.append(1))
    assert ran == [1], "无通道／无事务时必须立即执行（「落库后」与「提交后」等价）"


@pytest.mark.asyncio
async def test_r77_02_runs_after_commit_outside_tx_context():
    """② 提交后腿在 COMMIT 之后、且事务上下文已解绑时执行（恰好一次）"""
    repo = _repo()
    engine = _engine(repo)
    ran = []
    seen_tx = []

    async def body():
        await engine.run_after_commit(lambda: seen_tx.append(current_tx_conn()))
        await engine.run_after_commit(lambda: ran.append("after"))
        assert ran == [], "提交前就跑了提交后腿"
        assert seen_tx == [], "提交前就跑了提交后腿（ctx 探针）"

    await repo.with_tx(body)
    assert ran == ["after"], f"提交后腿应恰好执行一次，实得 {ran}"
    assert seen_tx == [None], f"提交后腿不得再拿事务连接（反查须走池连接），实得 {seen_tx}"


@pytest.mark.asyncio
async def test_r77_03_rollback_discards_all():
    """③ 回滚 ⇒ 提交后腿整批丢弃（修前无通道＝这一腿只能在事务内跑，那时回滚也已经跑过了）"""
    repo = _repo()
    engine = _engine(repo)
    ran = []
    with pytest.raises(RuntimeError):
        async def body():
            await engine.run_after_commit(lambda: ran.append("should-not-run"))
            raise RuntimeError("injected: R77 回滚探针")
        await repo.with_tx(body)
    assert ran == [], f"回滚场景提交后腿一条都不该执行，实得 {ran}"


@pytest.mark.asyncio
async def test_r77_04_nested_joins_outermost_scope():
    """④ 嵌套 with_tx 并入最外层 ⇒ 登记归最外层那张表，内层提前返回不触发执行"""
    repo = _repo()
    engine = _engine(repo)
    ran = []

    async def inner():
        await engine.run_after_commit(lambda: ran.append("inner"))

    async def outer():
        await repo.with_tx(inner)   # join 支：不建第二张表
        assert ran == [], "内层退出不得触发提交后腿"

    await repo.with_tx(outer)
    assert ran == ["inner"], f"提交后腿应归最外层并在其 COMMIT 后执行，实得 {ran}"


@pytest.mark.asyncio
async def test_r77_05_callback_error_does_not_break_main_flow():
    """⑤ 登记腿自家异常只打印、不传播（提交已成功，不能因通知失败把动作改成失败）"""
    repo = _repo()
    engine = _engine(repo)
    ran = []

    async def body():
        await engine.run_after_commit(_boom)
        await engine.run_after_commit(lambda: ran.append("second"))

    await repo.with_tx(body)
    assert ran == ["second"], f"单条回调炸掉不得中断后续腿，实得 {ran}"


def _boom():
    raise ValueError("injected: afterCommit 回调炸")
