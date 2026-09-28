"""委托代理**运行期自动生效**（issues/116 批次 D）——引擎内置、默认开启。

契约依据：`docs/spec/06-facade.md` §4.5「运行期语义」六条 + `docs/spec/05-spi.md`
「SurrogateInterceptor（委托生效，内置实现）」+ `docs/spec/08-compliance.md` 用例 26/27。

四条要点（各栈必须同形状，本模块是 Python 栈的落点）：

1. **并入参与者集合本身**：建任务时把命中的代理人追加进 ``ProcessTask.actorIds``，
   随后由 ``repo.save_task`` 随任务一起落到 ``wf_process_task_actor``。
   ⚠️ 不走"事后 ``add_task_actor`` 补写"——Java 首版 `SurrogateInterceptor` 正是在
   taskId 分配前补写，打在空 id 上**静默无效**（06 §4.5 条款 2 的 ⚠️）。
2. **授权人保留**：只追加、只去重，不摘原人（与 ``processTask/surrogate`` 加签同语义，任一可办）。
3. **默认开启、可显式关闭**：
   - 配置开关：``EngineExtensions(surrogate_enabled=False)``
   - 注册空实现：``EngineExtensions(surrogate_applier=NullSurrogateApplier())``
   关闭后回到"仅台账"行为（委托记录照存照查，建任务不再应用）。
4. **未配置扩展仓储时静默跳过**：``EngineExtensions.ext_repository is None`` → 不查、不抛，
   建单流程零影响（缺仓储属正常部署形态）。

另含**委托查询四判据**的 Python 侧共用谓词（``surrogate_enabled_on`` / ``to_datetime``），
由 ``model.ProcessSurrogate#is_effective`` 这一条裁决函数复用——四判据只此一处，
内存仓与 SQL 仓都取「本作用域最新一条 → 交 is_effective 裁决」同一形状，
必须对同一份数据给出同一结论（06 §4.5 条款 6；顺序本身是条款 1.4 的硬约束，见 issues/123）。

判据④自 issues/130 案 A 起**只认整数 1**（``'1'`` / ``1.0`` / ``True`` 等等价写法一律停用）。
整数列被驱动回读成字符串属**边界事实**，由 ``hydrate_enabled`` 在内置 SQL 仓储装行处还原后再交判据，
判据本身不为此放宽；内存仓无驱动，故不还原（Python 对象类型即列值类型）。
"""
from __future__ import annotations

import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional

__all__ = [
    "SurrogateApplier",
    "ExtRepositorySurrogateApplier",
    "NullSurrogateApplier",
    "hydrate_enabled",
    "surrogate_enabled_on",
    "surrogate_is_effective",
    "to_datetime",
]

# 时间文本格式（06 §4.5 契约格式 yyyy-MM-dd HH:mm:ss，另兼容 ISO T 与纯日期，对齐门面 _parse_surrogate_time）
_TIME_LAYOUTS = ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d")

# 规范十进制整数串（issues/130 案 A 读侧边界还原的唯一接受形状）：无空格、无前导零、无小数点、无正号
_CANONICAL_INT = re.compile(r"^-?(0|[1-9]\d*)$")


# ─── 四判据共用谓词（内存仓 / SQL 仓同答案的 Python 侧保证）────────────────────

def to_datetime(value: Any) -> Optional[datetime]:
    """时间窗边界归一：``datetime`` 原样；文本按契约格式解析；
    ``None`` / 空串 / 不可解析 → ``None``（判据②：**该侧不限**）。"""
    if value is None or isinstance(value, datetime):
        return value
    s = str(value).strip()
    if not s:
        return None
    for fmt in _TIME_LAYOUTS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def surrogate_enabled_on(value: Any) -> bool:
    """判据④：``enabled`` **只认整数 1**，其余一律停用（issues/130 案 A，owner 2026-09-28
    「我习惯用 1 和 0，和 java 版保持一致」）。

    严形状对齐 Java 参考实现 ``ProcessSurrogate#isEffective`` 的
    ``Integer.valueOf(1).equals(enabled)``：类型必须是整数**且**值必须是 1。
    故 ``"1"`` / ``1.0`` / ``True`` / ``2`` / ``-1`` / ``"x"`` / ``None`` / ``""`` 全判**停用**
    （``True`` 在 Python 里 ``== 1`` 恒成立，须显式排除 ``bool``）。

    ⚠️ 本判据**刻意不"等价"于** SQL 侧 ``enabled = 1`` 的隐式转换——MySQL 会把 ``'1'`` / ``1.0``
    折成 1 判启用，两条判据的宽严并不一致；那句"与隐式转换等价"正是 issues/130 的病灶来源
    （php / python / node 三栈由它漂成宽，java / go / rust / moon / csharp 五栈一直是严）。
    脏值默认方向各栈必须一致（05-spi：不可解析脏值 → 停用是正确方向，回落 1 启用是相反默认）。

    ⚠️ 收窄只影响**自定义 SPI 仓储**直传非整数 ``enabled`` 的路径（issues/130 §2）：
    内置 SQL 仓储该列是 INT 列（``tests/schema/schema-mysql.sql``: ``enabled INT NULL DEFAULT 1``），
    门面写入侧 ``processSurrogate/save`` 另有 ``_to_int`` 归一（显式脏值落 0、键缺失落契约默认 1），
    两条路八栈同结论，L2-17/L2-18 不受影响。
    ⚠️ **整数列被读回成字符串**（驱动边界事实，见下）**不在这里放行**——那是边界的活，
    见 hydrate_enabled；判据本身不吃串。
    """
    return isinstance(value, int) and not isinstance(value, bool) and value == 1


def hydrate_enabled(value: Any) -> Any:
    """**驱动边界**还原：把整数列被读回成字符串的形态换回 ``int``，再交 surrogate_enabled_on
    裁决（issues/130 案 A 的读侧另一半；owner 2026-09-28 定案第 2 条）。

    为什么需要：``enabled`` 在现网是 INT 列，而"宿主语言拿到的到底是 ``1`` 还是 ``'1'``"是
    **驱动/接入层**决定的，不由引擎负责：PHP 同栈已由 PDO 实证（缓冲查询
    ``ATTR_EMULATE_PREPARES`` / ``ATTR_STRINGIFY_FETCHES`` 把数值列一律回读成字符串 ``'1'``，
    见 jeeflow-php ``cf93d8f``）；Python 侧 ``aiomysql`` / ``asyncpg`` 默认在客户端按列类型转换
    （给 ``1``），但**并不保证**——文本协议经代理/网关把列类型报成 VAR_STRING、ORM 列类型漂移
    （SQLite 无类型/TEXT 声明的列、遗留 VARCHAR 台账）、以及业务方自定义 SPI 仓储自己拼行，
    都会交出 ``'1'``。判据④自案 A 起只认整数，缺这一层还原时这类宿主的委托会
    **整体静默判废且零告警**（Java ``rs.getInt``、Go ``Scan(&int)``、C# ``GetFieldValue<int>``
    干的是同一件事：类型还原属于边界，不属于判据）。

    ⚠️ 这**不是**把接受集合放宽回去：只认**规范整数串** ``-?(0|[1-9]\\d*)``（无空格、无前导零、
    无小数点、无正号），``'1.0'`` / ``' 1'`` / ``'01'`` / ``'1abc'`` / ``'abc'`` / ``''`` 以及
    ``True`` / ``1.0`` 一律原样返回，由判据④判停用。特别地 ``'2'`` 会被还原成整数 ``2``——
    还原只补类型，值不是 1 照样停用，可见"还原"与"判宽"是两回事。
    ⚠️ 调用点只允许在**内置 SQL 仓储装行处**（``repository/ext.JdbcProcessExtRepository._map_surrogate``）；
    内存仓 ``MemoryExtRepository`` **刻意不还原**——它没有驱动，Python 对象类型就是列值本身，
    在此还原等于伪造 INT 列的类型事实，也就废掉了 issues/130 §2 那条"SPI 直传脏值即停用"
    的判别力（``tests/spec_test.py`` 的脏值矩阵正钉在这上面）。业务方自定义 SPI 仓储读出非整数
    属 §2 的分叉源，按案 A 由实现侧自行还原（``hydrate_enabled`` 即为此导出）。
    """
    if isinstance(value, str) and _CANONICAL_INT.match(value):
        return int(value)
    return value


def surrogate_is_effective(surrogate_row: Any, operator: str, at: Any = None) -> bool:
    """**单条裁决**（06 §4.5 条款 5 四判据 + issues/123）：这一行委托此刻对该授权人生效吗。

    ⚠️ 调用方必须**先**按主键 id 选出「该授权人在该流程作用域内的最新一条」再问本函数
    （条款 1.4 + issues/123）——本函数只裁决单条，不做多条择优，也**不回落**。
    反过来写（先用判据把记录滤掉，剩下的才取最新）等于"历史上留过一条窗内 ``enabled=1``
    的记录就永久生效"，用户随后新建的窗外 / ``enabled=0`` / 脏值 / 自委托记录全都判不动它
    ——issues/123 里 13 栈 L2-17/L2-18 全红的病灶。

    判据本体只有 **``ProcessSurrogate.is_effective``** 一处（对齐 Java 参考实现
    jeeflow-java `6feeae6` 的 ``ProcessSurrogate#isEffective``）；本函数是它的 None 安全包装，
    供拿不到行对象 / 传入可能为 None 的调用方使用。⚠️ 不要再在这里另写一份四判据——
    两份判据各自漂移正是 issues/116 §5 / 123 抓过的病灶（内存仓与 SQL 仓必须同答案，条款 6）。

    :param surrogate_row: ``ProcessSurrogate`` 或 None（该作用域内没有记录）
    :param operator: 授权人（判自委托：被委托人等于授权人 ⇒ 不新增、不重复）
    :param at: 判定时刻；``None``（或不可解析的文本）= 不做窗口比较。
               注：两仓的 ``get_surrogate`` 都把"调用方没给时刻"解析成"当前时间"
               （Python 栈既有入参语义），所以这里的 None 分支只在直接传行裁决时可达。
    """
    if surrogate_row is None:
        return False
    return surrogate_row.is_effective(operator, at)


# ─── 委托应用扩展点 ─────────────────────────────────────────────────────────────

class SurrogateApplier(ABC):
    """委托应用扩展点：给定任务的参与者集合，返回**并入代理人后**的参与者集合。

    引擎在"参与者解析完成后、落库前"调用（06 §4.5 条款 1）。
    返回值中的原有元素顺序与内容不得变动（授权人保留，判据②）。
    """

    @abstractmethod
    async def expand(self, actors: list[str], process_name: str, task: Any = None) -> list[str]:
        """actors: 已解析的任务参与者；process_name: 委托查询流程名，取值口径与 trim 由引擎侧
        `EngineImpl._surrogate_process_name` 单点保证（模型 name 优先、trim 后判空、
        未带回落 wf_process_define.name，spec 06 §4.5 条款 1.1）；task: 待落库任务对象（只读上下文）"""
        ...


class NullSurrogateApplier(SurrogateApplier):
    """空实现——显式关闭的第二条路（注册后引擎不再应用委托，回到"仅台账"）。"""

    async def expand(self, actors: list[str], process_name: str, task: Any = None) -> list[str]:
        return list(actors or [])


class ExtRepositorySurrogateApplier(SurrogateApplier):
    """内置默认实现：逐个参与者查 ``IProcessExtRepository.get_surrogate``
    （判据①②③④ 由仓储保证：各作用域先取 id 最新一条，再交 ``ProcessSurrogate#is_effective`` 裁决）。

    命中即把代理人（``surrogate``）追加到参与者集合尾部，已存在则去重跳过；
    参与者按**快照**遍历，代理人自身不再级联委托（一单一查，避免 A→B→C 连锁）。
    """

    def __init__(self, ext_repository: Any):
        self._ext = ext_repository

    async def expand(self, actors: list[str], process_name: str, task: Any = None) -> list[str]:
        result: list[str] = [a for a in (actors or [])]
        if not result:
            return result
        # 判定时刻 = 引擎钟（naive 本地时间，与本栈写 create_time 同一把尺子，06 §4.5 条款 5 /
        # issues/120）：窗口比较不得另起 UTC 钟，否则与库里的本地时间戳差整小时数错判窗内窗外。
        now = datetime.now()
        # 入参 process_name 已由引擎侧单点 trim（条款 1.1）；此处**故意不再二次 trim**——
        # 否则引擎回归（把未 trim 的名字传下来）会被这里掩盖，用例捕获不到真实入参。
        pname = process_name or ""
        for actor in list(result):  # 快照：本轮追加的代理人不再触发查询
            if not actor:
                continue
            hit = await self._ext.get_surrogate(actor, pname, now)
            agent = str(getattr(hit, "surrogate", "") or "").strip()
            if agent and agent not in result:
                result.append(agent)
        return result
