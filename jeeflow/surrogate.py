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
供内存仓与门面复用——内存仓与 SQL 仓必须对同一份数据给出同一结论（06 §4.5 条款 6）。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional

__all__ = [
    "SurrogateApplier",
    "ExtRepositorySurrogateApplier",
    "NullSurrogateApplier",
    "surrogate_enabled_on",
    "to_datetime",
]

# 时间文本格式（06 §4.5 契约格式 yyyy-MM-dd HH:mm:ss，另兼容 ISO T 与纯日期，对齐门面 _parse_surrogate_time）
_TIME_LAYOUTS = ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d")


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
    """判据④：``enabled`` **只有 1 生效**；脏值不得当启用。

    - ``None`` / ``0`` / 其它整数（含 2）→ 停用；
    - 不可解析为整数的脏值（``"abc"`` / ``""``）→ 停用（各栈脏值默认方向必须一致，
      05-spi 明列 PHP ``(int)'abc'``→0 停用为正确方向、C# 回落 1 启用为相反默认）；
    - 等价写法 ``"1"`` / ``1.0`` / ``True`` 按启用处理（对齐 SQL 侧 ``enabled = 1`` 的隐式转换）。
    """
    if value is None:
        return False
    try:
        return int(float(value)) == 1
    except (TypeError, ValueError):
        return False


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
    """内置默认实现：逐个参与者查 ``IProcessExtRepository.get_surrogate``（判据①②③④ 由仓储保证）。

    命中即把代理人（``surrogate``）追加到参与者集合尾部，已存在则去重跳过；
    参与者按**快照**遍历，代理人自身不再级联委托（一单一查，避免 A→B→C 连锁）。
    """

    def __init__(self, ext_repository: Any):
        self._ext = ext_repository

    async def expand(self, actors: list[str], process_name: str, task: Any = None) -> list[str]:
        result: list[str] = [a for a in (actors or [])]
        if not result:
            return result
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
