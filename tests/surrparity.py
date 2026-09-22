"""委托查询「四判据 + 条款 1.4 取最新一条再裁决」的**共用数据集与期望表**
（issues/116 批次 D + issues/123「委托自动生效四判据」读回侧修复）。

契约依据：`jeeflow-doc/docs/spec/06-facade.md` §4.5「运行期语义」第 5 条（四判据）+ 第 6 条
（**内存仓与 SQL 仓两条路径都要满足**，同栈两仓给出不同结论即缺陷）+ 条款 1.4
（**各作用域先按 id 取最新一条，再由四判据裁决这一条**）。

调用方：
  - 内存仓侧：`tests/spec_test.py::test_surrogate_query_shuffled_id_parity_memory`
  - 真机 SQL 仓侧：`tests/jdbc_test.py` 第 ⑯ 节

**数据与期望只写在本文件这一处**——两侧各跑一遍并比对同一份期望，避免"两边各造一份断言"
导致的漂移（一侧改了期望、另一侧悄悄不跟）。

⚠️ **取数顺序本身就是契约（issues/123 的成因）**：本夹具里那几组 `veto=True` 的用例刻意造出
「同一作用域内旧记录有效 + 新记录不生效」的形状——它是"先滤生效再取最新"与"先取最新再裁决"
**唯一会分叉**的形状。前者会把那条旧的有效委托复活（⇒ 用户此后改停用 / 改到未来 / 改成自委托
全都不算数），后者判否不并。`verify_problems()` 因此**独立重算两种顺序的答案**并钉住：
① 期望表 == 新顺序（条款 1.4）的算法结果；② 每个 veto 用例在旧顺序下必须给出**不同**的答案
（否则该格已失去判别力——把实现改回旧形状也不会红）。

⚠️ **夹具的 id 序是刻意打乱的**（a1/a5 那两格"多条同时有效取哪条"的判别力所在）：插入顺序给的
id 偏移是 `+2 → +3 → +1`，即"期望命中的最新一条（id 最大）"**既不是插入首条、也不是插入末条**。于是三种
写法的结论互相可分：
  - 取遍历首条              → `exact-first`（红）
  - 取插入末条 / 边扫边覆盖  → `exact-last`（红）
  - 取 id 最大              → `exact-max`（绿，期望）
兜底分支（`zs2-*` 组）同样打乱，且把期望行做成 `process_name IS NULL`，一并钉住
"兜底不得只写 process_name = ''"。判别力不是靠人眼看注释，由 `verify_problems()` **独立重算**
四判据 + 取最新规则后核对：①期望与数据自洽、②多命中组的期望行落在插入序中间、
③veto 组在新旧两种顺序下确实分叉。

⚠️ **已知事实（Java/Go 实测踩到，写在这里免得误以为 SQL 侧也在起判别力）**：SQL 侧
`WHERE operator = ? ORDER BY id DESC LIMIT 1` 本来就按主键序回行，**插入序在结果里根本不出现**，
打乱与否答案都一样 ⇒ "打乱 id 序"在 **SQL 侧没有判别力**；真正钉住条款 1.4 的是 SQL 里的
`ORDER BY id DESC` 排序子句本身（去掉它就退化成"取物理首行"）以及**判据谓词不得下推进 SQL**
（带上 `enabled = 1` / 时间窗 / `surrogate <> ?` 就等于"先滤后取"，veto 组会当场复活旧记录）。
内存侧（dict 按插入序遍历）才是打乱序起作用的地方。两侧仍各跑一遍，并对**同一份答案**负责。

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
    """一条委托台账行。真实 id = base_id + id_off（id_off 决定"谁是最新一条"，见模块注释）。"""
    id_off: int
    operator: str
    surrogate: str
    process_name: Optional[str] = FLOW   # None = 存 NULL（与 '' 同属"全流程兜底"逻辑类）
    start_off: Optional[float] = None    # 小时偏移；None = 该侧不限
    end_off: Optional[float] = None
    enabled: Optional[int] = 1


@dataclass
class Case:
    """一次 get_surrogate 查询 + 期望命中的 surrogate（"" = 期望不命中）。
    ``veto=True`` ⇒ 本格是「最新一条不生效，压过更旧的窗内有效记录」的判别用例
    （issues/123；把实现改回"先滤后取"的旧形状时，红的就是这些格）。"""
    what: str
    operator: str
    process_name: str
    query_off: Optional[float]           # 查询时刻 = now + query_off 小时
    want: str
    veto: bool = False


# ─── 共用数据集 ─────────────────────────────────────────────────────────────────

def rows() -> list[Row]:
    w, e = -1.0, 1.0   # 窗口内：start = now-1h，end = now+1h
    return [
        # ── 判据① + 条款 1.4：同一授权人**三条同时精确命中且生效**，插入序 id +2 → +3 → +1 ──
        Row(2, op("zs"), name("exact-first"), FLOW, w, e, 1),
        Row(3, op("zs"), name("exact-max"), FLOW, w, e, 1),   # 期望（最新一条，居插入序中间）
        Row(1, op("zs"), name("exact-last"), FLOW, w, e, 1),
        # 兜底行：a2/a3 的期望。id 必须**小于**下面的 other-flow 行，才能钉住
        # "非空 processName 的行不得充当兜底"（兜底若不判空就会答成 other-flow）
        Row(5, op("zs"), name("all-flow"), "", w, e, 1),
        Row(7, op("zs"), name("other-flow"), OTHER_FLOW, w, e, 1),
        # 与"自委托"形似而不同：被委托人恰好叫 self-py116（字符串 ≠ zs-py116）
        Row(9, op("zs"), name("self"), name("self"), w, e, 1),
        # ⚠️ 原先压在 FLOW 作用域里的"停用行 id+6 / 自委托行 id+8"已迁到下面的 veto 组：条款 1.4
        #   的新顺序下它们就是该作用域的"最新一条"，会把 a1 整格裁决成不生效（正是本轮要修的语义）；
        #   判据③④自身的判别力改由 solo-* 单条组 + veto 组承担，一条没少。

        # ── 兜底分支自己也要按 id 取最新（精确分支答对 ≠ 兜底分支答对：两分支各查一次）──
        # 三条空 processName 生效行，插入序 +42 → +43 → +41，期望 +43（最大，且它是 NULL，
        # 兜底若只写 process_name = '' 就会答成 +42）
        Row(42, op("zs2"), name("all2-first"), "", w, e, 1),
        Row(43, op("zs2"), name("all2-max"), None, w, e, 1),   # 期望（最大 = NULL 行）
        Row(41, op("zs2"), name("all2-last"), "", w, e, 1),

        # ── 条款 1.4 / issues/123：同作用域「旧的有效 + 新的不生效」——由最新一条裁决 ──
        # 每组：先插一条窗内 + enabled=1 的有效旧行（id 较小），再插一条更"新"的不生效行（id 较大）。
        # 期望一律 **不命中**：旧的那条不得复活。四种不生效原因各一组（"窗外"再拆未到/已过两形），
        # 另加"精确作用域判否后仍看全流程作用域最新一条"的兜底组与"数据真在"的正向对照组。
        Row(101, op("v-future"), name("old-valid"), FLOW, w, e, 1),
        Row(102, op("v-future"), name("new-invalid"), FLOW, 9.0, None, 1),      # 最新一条：未到窗
        Row(111, op("v-past"), name("old-valid"), FLOW, w, e, 1),
        Row(112, op("v-past"), name("new-invalid"), FLOW, -10.0, -9.0, 1),      # 最新一条：已过窗
        Row(121, op("v-off"), name("old-valid"), FLOW, w, e, 1),
        Row(122, op("v-off"), name("new-invalid"), FLOW, w, e, 0),              # 最新一条：enabled=0
        Row(131, op("v-dirty"), name("old-valid"), FLOW, w, e, 1),
        Row(132, op("v-dirty"), name("new-invalid"), FLOW, w, e, 2),            # 最新一条：脏值 2
        Row(141, op("v-self"), name("old-valid"), FLOW, w, e, 1),
        Row(142, op("v-self"), op("v-self"), FLOW, w, e, 1),                    # 最新一条：自委托
        # 精确判否 ≠ 判否即止：全流程作用域的最新一条仍要单独裁决，且旧的有效精确行不得复活
        Row(151, op("v-fb"), name("old-exact"), FLOW, w, e, 1),
        Row(152, op("v-fb"), name("new-exact-off"), FLOW, w, e, 0),
        Row(153, op("v-fb"), name("all-fallback"), "", w, e, 1),                # 期望命中这一条

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
        # 判据① 精确优先 / 兜底 / 条款 1.4 取最新一条
        Case("a1 多条精确命中取 id 最大（插入序 +2→+3→+1：非遍历首条、非插入末条）",
             op("zs"), FLOW, None, name("exact-max")),
        Case("a2 未精确命中 → 全流程兜底（id 更大的非空 processName 行不得充当兜底）",
             op("zs"), "unmatched116", None, name("all-flow")),
        Case("a3 查询传空流程名 = 只走兜底", op("zs"), "", None, name("all-flow")),
        Case("a4 process_name=NULL 也属全流程兜底", op("solo-nullflow"), "any116", None, name("agent")),
        Case("a5 兜底分支多条命中同样取 id 最大（期望行 process_name 为 NULL）",
             op("zs2"), FLOW, None, name("all2-max")),

        # 条款 1.4 / issues/123：最新一条不生效 ⇒ 压过更旧的窗内有效记录（改回旧形状这几格必红）
        Case("v1 最新一条未到窗 → 不得复活旧的窗内有效记录", op("v-future"), FLOW, None, "", veto=True),
        Case("v2 最新一条已过窗 → 不得复活旧的窗内有效记录", op("v-past"), FLOW, None, "", veto=True),
        Case("v3 最新一条 enabled=0 → 不得复活旧的窗内有效记录", op("v-off"), FLOW, None, "", veto=True),
        Case("v4 最新一条 enabled=2 脏值（契约：只认 1）→ 不得复活旧的窗内有效记录",
             op("v-dirty"), FLOW, None, "", veto=True),
        Case("v5 最新一条自委托 → 不得复活旧的窗内有效记录", op("v-self"), FLOW, None, "", veto=True),
        Case("v6 精确作用域最新一条判否 → 仍看全流程作用域最新一条（条款 1.4 尾注，Java 同名用例形状）",
             op("v-fb"), FLOW, None, name("all-fallback"), veto=True),
        Case("v7 正向对照：v1 那组到了未来时刻最新一条自己进窗即生效（证明'没命中'不是因为数据没进去）",
             op("v-future"), FLOW, 10.0, name("new-invalid")),

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

        # 判据④ enabled 只认 1
        Case("d1 enabled=0 停用", op("solo-disabled"), FLOW, None, ""),
        Case("d2 enabled=2 非 1 值不得当启用", op("solo-dirty"), FLOW, None, ""),
        Case("d3 enabled=NULL 不得当启用", op("solo-null"), FLOW, None, ""),
        Case("d4 enabled=1 生效", op("solo-on"), FLOW, None, name("agent")),

        # 边界：无委托 / 他人委托不串人
        Case("e1 无委托授权人", op("nobody"), FLOW, None, ""),
        Case("e2 他人委托不串到别的流程名上", op("someone-else"), OTHER_FLOW, None, ""),
        # 正向对照：证明"他人委托"那行确实落库且生效——否则 e2 这类负向期望会因为
        # "根本没数据"而空转通过（issues/113 教训：断言要落在真落库的值上）
        Case("e3 正向对照：他人自己的委托确实生效", op("someone-else"), FLOW, None, name("agent")),
    ]


# ─── 判别力自证（独立重算判据与取数顺序，不复用被测实现）──────────────────────

def _at_off(case: Case) -> float:
    return case.query_off if case.query_off is not None else 0.0


def _criteria_pass(r: Row, case: Case) -> bool:
    """四判据的**独立**重算（刻意不复用 ProcessSurrogate.is_effective / jeeflow.surrogate 的谓词，
    否则自证与实现同源共错）：授权人匹配 + 被委托人非空且非本人 + enabled 严格 == 1 +
    时间窗（偏移比较，与挂钟无关；起止 None = 该侧不限）。"""
    if r.operator != case.operator:
        return False
    if r.enabled != 1:
        return False
    if not (r.surrogate or "").strip() or r.surrogate.strip() == case.operator:
        return False
    at = _at_off(case)
    if r.start_off is not None and r.start_off > at:
        return False
    if r.end_off is not None and r.end_off < at:
        return False
    return True


def _scope(rs: list[Row], case: Case, want_global: bool) -> list[Row]:
    """该授权人在**一个作用域**内的全部委托行（保持插入序）。
    精确作用域 = processName 与查询名完全相同；全流程作用域 = processName 为空（''/NULL）。"""
    mine = [r for r in rs if r.operator == case.operator]
    if want_global:
        return [r for r in mine if not r.process_name]      # '' 与 NULL 同属兜底
    if not case.process_name:
        return []
    return [r for r in mine if r.process_name == case.process_name]


def _newest(rs: list[Row]) -> Optional[Row]:
    """id 最大（= 最新）的一条；空作用域返回 None"""
    return max(rs, key=lambda r: r.id_off) if rs else None


def contract_answer(case: Case, data: Optional[list[Row]] = None) -> str:
    """条款 1.4 的**正确**顺序：各作用域先取最新一条 → 交四判据裁决这一条；
    精确作用域判否后仍看全流程作用域的最新一条。返回期望 surrogate（"" = 不命中）。"""
    rs = rows() if data is None else data
    exact = _newest(_scope(rs, case, False))
    if exact is not None and _criteria_pass(exact, case):
        return exact.surrogate
    glob = _newest(_scope(rs, case, True))
    return glob.surrogate if glob is not None and _criteria_pass(glob, case) else ""


def legacy_answer(case: Case, data: Optional[list[Row]] = None) -> str:
    """被钉死的**错**顺序（issues/123 的成因）：先按判据把不生效的滤掉，再从剩下的取 id 最大。
    本函数只用于 `verify_problems()` 证明 veto 组确有判别力，不参与被测代码。"""
    rs = rows() if data is None else data
    cands = [r for r in rs if _criteria_pass(r, case)]
    exact = _newest([r for r in cands
                     if case.process_name and r.process_name == case.process_name])
    if exact is not None:
        return exact.surrogate
    glob = _newest([r for r in cands if not r.process_name])
    return glob.surrogate if glob is not None else ""


def verify_problems() -> list[str]:
    """自证夹具：期望表与数据集自洽 + 多命中组的期望行落在插入序**中间** +
    veto 组在新旧两种取数顺序下确实分叉。
    返回问题列表（空 = 夹具合格）；两侧都先跑它，比的是同一份自证。"""
    problems: list[str] = []
    data = rows()
    for c in cases():
        want_new = contract_answer(c, data)
        if c.want != want_new:
            problems.append(f"{c.what}：按条款 1.4「取最新一条再裁决」应命中 {want_new!r}，"
                            f"期望表却写了 {c.want!r}——期望与规则不自洽")
            continue
        if c.veto:
            want_old = legacy_answer(c, data)
            if want_old == c.want:
                problems.append(
                    f"夹具失去判别力：{c.what} 标了 veto，但「先滤生效再取最新」的旧顺序"
                    f"也给出同一答案（{want_old!r}）——把实现改回 issues/123 的旧形状它照样绿；"
                    f"须在该作用域内补一条更旧的窗内有效记录，或把无效那条的 id 抬到最大")
        # "多条同时有效取哪条"这一格另需插入序打乱：期望行既非首条也非末条，
        # 否则"取遍历首条""取插入末条"的错实现会跟着一起绿
        scope = _scope(data, c, False) or _scope(data, c, True)
        valid = [r for r in scope if _criteria_pass(r, c)]
        if c.want != "" and len(valid) >= 2:
            offs = [r.id_off for r in valid]
            idx = offs.index(max(offs))
            if idx in (0, len(valid) - 1):
                problems.append(
                    f"夹具失去判别力：{c.what} 的作用域里有 {len(valid)} 条同时生效"
                    f"（插入序 id {offs}），期望那条(id_off={max(offs)})却落在插入序第 {idx + 1} 位"
                    f"（首位/末位）——须把 id 序打乱到期望行既非首条也非末条")
    return problems


# ─── 共跑入口 ───────────────────────────────────────────────────────────────────

def to_surrogate(row: Row, base_id: int, now: datetime) -> ProcessSurrogate:
    """Row → ProcessSurrogate：两侧共用同一映射，**唯一差异是 base_id**（SQL 侧用自己的段）"""
    return ProcessSurrogate(
        id=base_id + row.id_off,          # id_off 决定"谁是最新一条"，base 只负责不撞号
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
    # 0) 夹具自证：先确认这份数据真有判别力（打乱序 + 期望与规则自洽 + veto 组确有分叉）
    problems = verify_problems()
    report("夹具自证：期望表与数据集自洽，多命中组 id 序确已打乱，veto 组在新旧顺序下答案不同",
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

    # 3) 期望对拍（判据①②③④ + 条款 1.4 的取数顺序，两侧同一份期望）
    for c in cases():
        at = now + timedelta(hours=c.query_off) if c.query_off is not None else now
        hit = await ext.get_surrogate(c.operator, c.process_name, at)
        got = (hit.surrogate or "") if hit is not None else ""
        report(c.what, got == c.want, f"want={c.want!r} got={got!r}")
    return ids
