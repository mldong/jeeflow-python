"""委托查询「四判据 + 条款 1.4 取 id 最大」的**共用数据集与期望表**（issues/116 批次 D）。

契约依据：`jeeflow-doc/docs/spec/06-facade.md` §4.5「运行期语义」第 5 条（四判据）+ 第 6 条
（**内存仓与 SQL 仓两条路径都要满足**，同栈两仓给出不同结论即缺陷）+ 条款 1.4（多条命中取 id 最大）。

调用方：
  - 内存仓侧：`tests/spec_test.py::test_surrogate_query_shuffled_id_parity_memory`
  - 真机 SQL 仓侧：`tests/jdbc_test.py` 第 ⑯ 节

**数据与期望只写在本文件这一处**——两侧各跑一遍并比对同一份期望，避免"两边各造一份断言"
导致的漂移（一侧改了期望、另一侧悄悄不跟）。

⚠️ **夹具的 id 序是刻意打乱的**（条款 1.4 的判别力所在）：多条命中那一组里，插入顺序给的
id 偏移是 `+2 → +3 → +1`，即"期望命中的最大 id 行"**既不是插入首条、也不是插入末条**。于是三种
写法的结论互相可分：
  - 取遍历首条              → `exact-first`（红）
  - 取插入末条 / 边扫边覆盖  → `exact-last`（红）
  - 取 id 最大              → `exact-max`（绿，期望）
兜底分支（`zs2-*` 组）同样打乱，且把期望行做成 `process_name IS NULL`，一并钉住
"兜底不得只写 process_name = ''"。判别力不是靠人眼看注释，由 `verify_problems()` **独立重算**
四判据 + 取最大规则后核对：①期望与数据自洽、②多命中组的期望行落在插入序中间。

⚠️ **已知事实（Java/Go 实测踩到，写在这里免得误以为 SQL 侧也在起判别力）**：SQL 侧
`WHERE operator = ? ORDER BY id DESC LIMIT 1` 本来就按主键序回行，**插入序在结果里根本不出现**，
打乱与否答案都一样 ⇒ "打乱 id 序"在 **SQL 侧没有判别力**；真正钉住条款 1.4 的是 SQL 里的
`ORDER BY id DESC` 排序子句本身（去掉它就退化成"取物理首行"）。内存侧（dict 按插入序遍历）
才是打乱序起作用的地方。两侧仍各跑一遍，并对**同一份答案**负责。

时间用「相对查询时刻的偏移（小时）」表达，用例不随挂钟过期；两侧共用同一 `now`。
脏值 `enabled="abc"` 只在内存侧可存（SQL 列是 INT），属**写入侧**判据，由
`spec_test.py::test_facade_surrogate_save_dirty_enabled_is_off` 与 jdbc ⑮.4 各自覆盖，不进本夹具。

⚠️ **已核对、刻意不进夹具的一处两侧差异**（160 实测 `wf_process_surrogate` 排序规则是
`utf8mb4_general_ci`：大小写不敏感 + PAD SPACE 尾空格不敏感，`SELECT 'a'='a '` 返回 1）：
SQL 侧 `operator = ?` / `process_name = ?` 对**尾随空格**与**大小写**比内存侧宽松。
- 不属契约第 5 条的四判据，且方向是"SQL 多匹配"，不会把该生效的判成不生效；
- 引擎侧自本轮起传出的流程名**已 trim**（条款 1.1 单点）⇒ 从建单路径根本构造不出
  "带尾空格的名字"，该差异不可达；
- 且它依赖服务器的列排序规则（MySQL 8 默认 `utf8mb4_0900_ai_ci` 是 NO PAD，行为就不一样），
  把它写进夹具等于把某台机器的建表参数固化成契约。
故此处只留说明，不补断言；真要收口应在建表 SQL 层面统一列排序规则（跨栈议题，建议挂账）。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Optional

from jeeflow.model import ProcessSurrogate

# 全部操作人/代理人的命名空间后缀——两仓用同一批人，SQL 侧按本轮显式 id 清理
OP_PREFIX = "py116"
FLOW = "leave116"           # 用例主流程名
OTHER_FLOW = "other116"     # 与 FLOW 不同的流程名（兜底 / 不串人用例用）
MEM_BASE_ID = 900000        # 内存侧 id 基准（SQL 侧传自己的段基准，id 偏移两边完全相同）


def op(base: str) -> str:
    """授权人（operator）"""
    return f"{base}-{OP_PREFIX}"


def name(base: str) -> str:
    """代理人（surrogate）/ 流程名"""
    return f"{base}-{OP_PREFIX}"


@dataclass
class Row:
    """一条委托台账行。真实 id = base_id + id_off（id_off 刻意非单调，见模块注释）。"""
    id_off: int
    operator: str
    surrogate: str
    process_name: Optional[str] = FLOW   # None = 存 NULL（与 '' 同属"全流程兜底"逻辑类）
    start_off: Optional[float] = None    # 小时偏移；None = 该侧不限
    end_off: Optional[float] = None
    enabled: Optional[int] = 1


@dataclass
class Case:
    """一次 get_surrogate 查询 + 期望命中的 surrogate（"" = 期望不命中）"""
    what: str
    operator: str
    process_name: str
    query_off: Optional[float]           # 查询时刻 = now + query_off 小时
    want: str


# ─── 共用数据集 ─────────────────────────────────────────────────────────────────

def rows() -> list[Row]:
    w, e = -1.0, 1.0   # 窗口内：start = now-1h，end = now+1h
    return [
        # ── 判据① + 条款 1.4：同一授权人**三条同时精确命中且生效**，插入序 id +2 → +3 → +1 ──
        Row(2, op("zs"), name("exact-first"), FLOW, w, e, 1),
        Row(3, op("zs"), name("exact-max"), FLOW, w, e, 1),   # 期望（最大，居插入序中间）
        Row(1, op("zs"), name("exact-last"), FLOW, w, e, 1),
        # 兜底行：a2/a3 的期望。id 必须**小于**下面的 other-flow 行，才能钉住
        # "非空 processName 的行不得充当兜底"（兜底若不判空就会答成 other-flow）
        Row(5, op("zs"), name("all-flow"), "", w, e, 1),
        Row(7, op("zs"), name("other-flow"), OTHER_FLOW, w, e, 1),
        # 停用行 / 自委托行 id 比生效行更大：实现若把"enabled 非 1"折叠成启用、或漏了
        # 自委托过滤，a1/d5/c4 就会答成 disabled / zs 自己。
        # 它们被判据③④先滤掉 ⇒ **不属**"多条同时命中"集合，不参与判别力自证（见 verify_problems）
        Row(6, op("zs"), name("disabled"), FLOW, w, e, 0),
        Row(8, op("zs"), op("zs"), FLOW, w, e, 1),
        # 与"自委托"形似而不同：被委托人恰好叫 self-py116（字符串 ≠ zs-py116）
        Row(9, op("zs"), name("self"), name("self"), w, e, 1),

        # ── 兜底分支自己也要按 id 取最大（精确分支答对 ≠ 兜底分支答对：两分支各查一次）──
        # 三条空 processName 生效行，插入序 +42 → +43 → +41，期望 +43（最大，且它是 NULL，
        # 兜底若只写 process_name = '' 就会答成 +42）
        Row(42, op("zs2"), name("all2-first"), "", w, e, 1),
        Row(43, op("zs2"), name("all2-max"), None, w, e, 1),   # 期望（最大 = NULL 行）
        Row(41, op("zs2"), name("all2-last"), "", w, e, 1),

        # ── 判据② 时间窗（每条独立授权人：答错不会被别的判据掩盖）──
        Row(10, op("solo-window"), name("agent"), FLOW, -10.0, -9.0, 1),
        Row(11, op("solo-future"), name("agent"), FLOW, 9.0, None, 1),
        Row(12, op("solo-openend"), name("agent"), FLOW, -9.0, -1.0, 1),
        Row(13, op("solo-halfopen"), name("agent"), FLOW, -9.0, None, 1),
        Row(14, op("solo-nowindow"), name("agent"), FLOW, None, None, 1),

        # ── 判据④ enabled 只认 1 ──
        Row(15, op("solo-disabled"), name("agent"), FLOW, w, e, 0),
        Row(16, op("solo-dirty"), name("agent"), FLOW, w, e, 2),
        Row(17, op("solo-null"), name("agent"), FLOW, w, e, None),
        Row(18, op("solo-on"), name("agent"), FLOW, w, e, 1),

        # ── 判据③ 自委托过滤（精确 / 全流程兜底两条路径各一次）──
        Row(20, op("solo-self-exact"), op("solo-self-exact"), FLOW, w, e, 1),
        Row(21, op("solo-self-all"), op("solo-self-all"), "", w, e, 1),
        Row(22, op("solo-self-all"), op("solo-self-all"), None, w, e, 1),

        # ── 判据① 补：process_name 为 NULL 的单条全流程兜底（无干扰）──
        Row(23, op("solo-nullflow"), name("agent"), None, None, None, 1),

        # ── 不串人 ──
        Row(30, op("someone-else"), name("agent"), FLOW, None, None, 1),
    ]


# ─── 共用期望表 ─────────────────────────────────────────────────────────────────

def cases() -> list[Case]:
    return [
        # 判据① 精确优先 / 兜底 / 条款 1.4 取 id 最大
        Case("a1 多条精确命中取 id 最大（插入序 +2→+3→+1：非遍历首条、非插入末条）",
             op("zs"), FLOW, None, name("exact-max")),
        Case("a2 未精确命中 → 全流程兜底（id 更大的非空 processName 行不得充当兜底）",
             op("zs"), "unmatched116", None, name("all-flow")),
        Case("a3 查询传空流程名 = 只走兜底", op("zs"), "", None, name("all-flow")),
        Case("a4 process_name=NULL 也属全流程兜底", op("solo-nullflow"), "any116", None, name("agent")),
        Case("a5 兜底分支多条命中同样取 id 最大（期望行 process_name 为 NULL）",
             op("zs2"), FLOW, None, name("all2-max")),
        # 判据② 时间窗
        Case("b1 窗口已过期", op("solo-window"), FLOW, None, ""),
        Case("b2 窗口未开始（结束侧 NULL）", op("solo-future"), FLOW, None, ""),
        Case("b3 已过窗", op("solo-openend"), FLOW, None, ""),
        Case("b4 单侧 NULL = 该侧不限（正向）", op("solo-halfopen"), FLOW, None, name("agent")),
        Case("b5 双侧 NULL = 不限（正向）", op("solo-nowindow"), FLOW, None, name("agent")),
        Case("b6 未来窗口按未来时刻查询命中", op("solo-future"), FLOW, 10.0, name("agent")),
        # 判据③ 自委托过滤
        Case("c1 精确路径自委托不生效", op("solo-self-exact"), FLOW, None, ""),
        Case("c2 兜底路径自委托同样不生效（含 NULL processName 行）", op("solo-self-all"), "any116", None, ""),
        Case("c3 委托给同名用户 ≠ 自委托", op("zs"), name("self"), None, name("self")),
        Case("c4 自委托行 id 更大时仍取生效的 id 最大行", op("zs"), FLOW, None, name("exact-max")),
        # 判据④ enabled 只认 1
        Case("d1 enabled=0 停用", op("solo-disabled"), FLOW, None, ""),
        Case("d2 enabled=2 非 1 值不得当启用", op("solo-dirty"), FLOW, None, ""),
        Case("d3 enabled=NULL 不得当启用", op("solo-null"), FLOW, None, ""),
        Case("d4 enabled=1 生效", op("solo-on"), FLOW, None, name("agent")),
        Case("d5 停用行 id 更大时仍取生效的 id 最大行", op("zs"), FLOW, None, name("exact-max")),
        # 边界：无委托 / 他人委托不串人
        Case("e1 无委托授权人", op("nobody"), FLOW, None, ""),
        Case("e2 他人委托不串到别的流程名上", op("someone-else"), OTHER_FLOW, None, ""),
        # 正向对照：证明"他人委托"那行确实落库且生效——否则 e2 这类负向期望会因为
        # "根本没数据"而空转通过（issues/113 教训：断言要落在真落库的值上）
        Case("e3 正向对照：他人自己的委托确实生效", op("someone-else"), FLOW, None, name("agent")),
    ]


# ─── 判别力自证（独立重算判据，不复用被测实现）────────────────────────────────

def _qualifies(r: Row, case: Case) -> bool:
    """四判据的**独立**重算（刻意不复用 jeeflow.surrogate 的谓词，否则自证与实现同源共错）：
    授权人匹配 + 非自委托 + enabled 严格 == 1 + 时间窗（偏移比较，与挂钟无关）。"""
    at = case.query_off or 0.0
    if r.operator != case.operator or r.surrogate == case.operator:
        return False
    if r.enabled != 1:
        return False
    if r.start_off is not None and r.start_off > at:
        return False
    if r.end_off is not None and r.end_off < at:
        return False
    return True


def hit_pool(case: Case) -> list[Row]:
    """该查询按契约应"同时命中"的候选行（插入序）：先精确、空则兜底——与仓储两分支同形状"""
    data = rows()
    cands = [r for r in data if _qualifies(r, case)]
    if case.process_name:
        exact = [r for r in cands if r.process_name == case.process_name]
        if exact:
            return exact
    return [r for r in cands if not r.process_name]   # '' 与 NULL 同属兜底


def verify_problems() -> list[str]:
    """自证夹具：期望表与数据集自洽 + 多命中组的期望行落在插入序**中间**。
    返回问题列表（空 = 夹具合格）；两侧都先跑它，比的是同一份自证。"""
    problems: list[str] = []
    for c in cases():
        pool = hit_pool(c)
        got = max(pool, key=lambda r: r.id_off) if pool else None
        if c.want == "":
            if pool:
                problems.append(f"{c.what}：期望不命中，但数据里有 {len(pool)} 条同时生效的候选"
                                f"（{[r.surrogate for r in pool]}）——期望表与数据不自洽")
            continue
        if not pool:
            problems.append(f"{c.what}：期望命中 {c.want!r}，但数据里没有任何生效候选行"
                            f"——负向断言会因'根本没数据'空转通过")
            continue
        if got.surrogate != c.want:
            problems.append(f"{c.what}：按条款 1.4「取 id 最大」应命中 {got.surrogate!r}"
                            f"(id_off={got.id_off})，期望表却写了 {c.want!r}——期望与规则不自洽")
            continue
        if len(pool) >= 2:
            idx = [r.id_off for r in pool].index(max(r.id_off for r in pool))
            if idx in (0, len(pool) - 1):
                offs = [r.id_off for r in pool]
                problems.append(
                    f"夹具失去判别力：{c.what} 有 {len(pool)} 条同时命中（插入序 id {offs}），"
                    f"期望那条(id_off={max(offs)})却落在插入序第 {idx + 1} 位（首位/末位）——"
                    f"『取遍历首条』『取插入末条』的错实现会跟着这份夹具一起绿；"
                    f"须把 id 序打乱到期望行既非首条也非末条")
    return problems


# ─── 共跑入口 ───────────────────────────────────────────────────────────────────

def to_surrogate(row: Row, base_id: int, now: datetime) -> ProcessSurrogate:
    """Row → ProcessSurrogate：两侧共用同一映射，**唯一差异是 base_id**（SQL 侧用自己的段）"""
    return ProcessSurrogate(
        id=base_id + row.id_off,          # id_off 决定"打乱序"，base 只负责不撞号
        processName=row.process_name,
        operator=row.operator,
        surrogate=row.surrogate,
        startTime=None if row.start_off is None else now + timedelta(hours=row.start_off),
        endTime=None if row.end_off is None else now + timedelta(hours=row.end_off),
        enabled=row.enabled,
        createUser="t", updateUser="t")


async def run_parity(ext, now: datetime, base_id: int,
                     report: Callable[[str, bool, str], Any]) -> list[int]:
    """同一份数据 + 同一份期望跑一仓：夹具自证 → 落库 → 逐行读回自证 → 逐条期望对拍。
    ``report(desc, ok, detail)`` 由调用方给（pytest 侧收集失败后 assert；jdbc 侧用 check 计数）。
    返回本轮落库的 id 列表，供 SQL 侧按 id 精确清理。"""
    # 0) 夹具自证：先确认这份数据真有判别力（打乱序 + 期望与规则自洽）
    problems = verify_problems()
    report("夹具自证：期望表与数据集自洽，且多命中组 id 序确已打乱（期望行非插入首条/末条）",
           not problems, "；".join(problems))

    # 1) 落库（两侧走同一映射，只有 base_id 不同）
    data = rows()
    ids = []
    for r in data:
        s = to_surrogate(r, base_id, now)
        await ext.save_surrogate(s)
        ids.append(s.id)

    # 2) 种子自证：逐行按显式 id 读回，确认全部真落库。缺这一步时"未命中"类期望会因为
    #    数据根本没进去而空转通过（issues/113 教训）
    missing = []
    for r in data:
        back = await ext.find_surrogate_by_id(base_id + r.id_off)
        if back is None or back.surrogate != r.surrogate or back.operator != r.operator:
            missing.append(f"id={base_id + r.id_off} 读回 {back and back.surrogate}")
    report("种子自证：全部委托行按显式 id 读回一致", not missing, "；".join(missing[:3]))

    # 3) 期望对拍（判据①②③④ + 条款 1.4，两侧同一份期望）
    for c in cases():
        at = now + timedelta(hours=c.query_off) if c.query_off is not None else now
        hit = await ext.get_surrogate(c.operator, c.process_name, at)
        got = (hit.surrogate or "") if hit is not None else ""
        report(c.what, got == c.want, f"want={c.want!r} got={got!r}")
    return ids
