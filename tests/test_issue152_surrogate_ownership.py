"""issues/152 ②③ · Python 栈腿：委托台账分页的**归属不变式**与授权人三档归一。

条文依据＝jeeflow-doc `spec/06-facade.md` §2.5 口径表（本轮新增 `processSurrogate/page` 一行）
＋ §4.5「processSurrogate/page 归属不变式」（门面注入 / 仓储第二层 / 双仓同答案）
＋ §4.5「新建时授权人」⚠️ 段（三档入参一律归一，不得落空串）。
判据基准＝jeeflow-java `SurrogateAutoApplyTest` 的 issues/152 七格。

本文件覆盖 §7 的档 4（save 三档 operator）与档 5（page 归属两层）。档 1/2/3（作用域优先级、
择优顺序、自委托过滤）由 `tests/surrparity.py` 的四判据夹具与
`spec_test.py::test_surrogate_query_shuffled_id_parity_*` 在 issues/123 轮钉死，本案不动那条语义。

**为什么档 4 的正面判据落在 `ext.get_surrogate` 上**：引擎建单那一刻并入代理人走的就是这条读通道
（`SurrogateInterceptor` → `get_surrogate(actor, processName)`），空串行的症状正是"这行永远读不到"。
比返回码更能证"死行"，且不依赖某个特定流程的参与者是谁。
"""
import os
import sys

import pytest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow.facade import JeeflowFacade
from jeeflow.memory import MemoryExtRepository
from jeeflow.repository.ext import JdbcProcessExtRepository
from jeeflow.spi import QueryCondition

from tests.spec_test import setup, _deploy, _start  # 复用既有夹具，不另建一套基建


def _win(days_before: int = 1, days_after: int = 1) -> tuple[str, str]:
    now = datetime.now()
    return ((now - timedelta(days=days_before)).strftime("%Y-%m-%d %H:%M:%S"),
            (now + timedelta(days=days_after)).strftime("%Y-%m-%d %H:%M:%S"))


def _fresh():
    eng, repo = setup()
    ext = MemoryExtRepository()
    return eng, repo, ext, JeeflowFacade(eng, repo, ext)


# ═══ 档 4（issues/152 ③）：save 的 operator 三档一律归一，不落空串死行 ═══

@pytest.mark.asyncio
async def test_i152_save_operator_three_tiers_land_on_user1_and_are_not_dead_rows():
    for label, operator in (("缺键", None), ("显式空串", ""), ("全空白", "   ")):
        eng, repo, ext, facade = _fresh()
        start, end = _win()
        body = {"surrogate": "d4Agent", "processName": "simple",
                "startTime": start, "endTime": end}
        if operator is not None:
            body["operator"] = operator
        r = await facade.flow("processSurrogate/save", body)
        assert r["code"] == 0, (label, r)
        detail = await facade.flow("processSurrogate/detail", {"id": r["data"]["id"]})
        assert detail["data"]["operator"] == "user1", \
            f"{label}：三档必须走 §2.5 归一落 user1，不得落空串（空串＝死行），实得 {detail['data']['operator']!r}"
        # 正面判据：该行必须能被归一后的授权人读到（建单那一刻用的就是这条通道）
        hit = await ext.get_surrogate("user1", "simple")
        assert hit is not None and hit.surrogate == "d4Agent", \
            f"{label}：该行读不到就是死行（台账看得见、待办永不并人）"
        # 反证：换个人读不到 ⇒ 上面那条不是恒真
        assert await ext.get_surrogate("other-op", "simple") is None, \
            f"{label}：归属判据被放宽成不过滤了"


@pytest.mark.asyncio
async def test_i152_explicit_operator_wins_and_blank_update_keeps_original():
    """阳性对照：归一不是把三档焊死成 user1；update 的空白档保留**原授权人**。"""
    eng, repo, ext, facade = _fresh()
    start, end = _win()
    save = await facade.flow("processSurrogate/save", {
        "operator": "opExplicit", "surrogate": "d4Keep", "processName": "simple",
        "startTime": start, "endTime": end})
    assert save["code"] == 0, save
    sid = save["data"]["id"]
    detail = await facade.flow("processSurrogate/detail", {"id": sid})
    assert detail["data"]["operator"] == "opExplicit", "显式非空 operator 仍以显式值为授权人"

    upd = await facade.flow("processSurrogate/update", {
        "id": sid, "operator": "   ", "processName": "simple", "surrogate": "d4Keep", "enabled": 1})
    assert upd["code"] == 0, upd
    detail = await facade.flow("processSurrogate/detail", {"id": sid})
    assert detail["data"]["operator"] == "opExplicit", \
        "update 的空白档既不清空也不改成缺省 user1（spec 06 §processSurrogate/update）"


# ═══ 档 5（issues/152 ②）：page 归属不变式，门面第一层 + 仓储第二层 ═══

@pytest.mark.asyncio
async def test_i152_page_ownership_only_own_rows_through_facade():
    eng, repo, ext, facade = _fresh()
    start, end = _win()
    for op, agent in (("user1", "agMine"), ("op-others", "agX"), ("op-others", "agY")):
        r = await facade.flow("processSurrogate/save", {
            "operator": op, "surrogate": agent, "processName": "simple",
            "startTime": start, "endTime": end})
        assert r["code"] == 0, r

    mine = await facade.flow("processSurrogate/page", {"operator": "op-others", "pageSize": 50})
    assert mine["data"]["recordCount"] == 2, mine["data"]
    assert {row["operator"] for row in mine["data"]["rows"]} == {"op-others"}, \
        f"带 operator 只见自己授出的行（逐行核对归属列，不是恰好条数对）: {mine['data']['rows']}"

    # 不带 operator ⇒ §2.5 归一到 user1，只出 user1 的那条，绝不全库三条
    none = await facade.flow("processSurrogate/page", {"pageSize": 50})
    assert none["data"]["recordCount"] == 1, \
        f"缺 operator 不是「不过滤」，是归一到缺省 user1: {none['data']}"
    assert {row["operator"] for row in none["data"]["rows"]} == {"user1"}

    # 空串／全空白与缺键同档（都是归一，不是"这条不加"）
    for blank in ("", "   "):
        got = await facade.flow("processSurrogate/page", {"operator": blank, "pageSize": 50})
        assert got["data"]["recordCount"] == 1, f"空白 operator 档应同缺键（实得 {got['data']}）"


@pytest.mark.asyncio
async def test_i152_repo_missing_or_blank_ownership_is_empty_page_both_repos():
    """第二层：绕过门面直调仓储——空值档与**缺失档**都判空页，两仓同答案（条款 6）。"""
    eng, repo, ext, facade = _fresh()
    start, end = _win()
    await ext.save_surrogate(_surr("op-mine", "agMine", "simple", start, end))

    # 内存仓：阳性对照先证判据不是恒真
    ok_rows, ok_total = await ext.page_surrogates(
        1, 50, conditions=[QueryCondition(column="t.operator", operator="EQ", value="op-mine")])
    assert ok_total == 1 and ok_rows[0].operator == "op-mine", (ok_total, ok_rows)
    for blank in ("", "   ", None):
        rows, total = await ext.page_surrogates(
            1, 50, conditions=[QueryCondition(column="t.operator", operator="EQ", value=blank)])
        assert (rows, total) == ([], 0), f"内存仓空值档不得退化成全库: {blank!r} → {total}"
    rows, total = await ext.page_surrogates(1, 50)
    assert (rows, total) == ([], 0), "内存仓归属条件整条没给同样空页（§2.5 缺失档与空值档同判）"

    # SQL 仓：T0 不连库，判据打在拼装层（与内存仓同一函数形状）
    probe = JdbcProcessExtRepository.__new__(JdbcProcessExtRepository)
    eff = JdbcProcessExtRepository._surrogate_ownership_effective
    assert eff(None, [QueryCondition(column="t.operator", operator="EQ", value="op-mine")]) is True
    for blank in ("", "   ", None):
        assert eff(None, [QueryCondition(column="t.operator", operator="EQ", value=blank)]) is False, \
            f"SQL 仓空值档判据与内存仓分叉: {blank!r}"
    assert eff(None, []) is False, "SQL 仓缺失档判据与内存仓分叉"
    assert eff({"operator": ""}, None) is False, "Filters 那一形（本栈两形）同样收"


def _surr(operator, agent, process_name, start, end):
    from jeeflow.model import ProcessSurrogate
    return ProcessSurrogate(operator=operator, surrogate=agent, processName=process_name,
                            startTime=start, endTime=end, enabled=1)
