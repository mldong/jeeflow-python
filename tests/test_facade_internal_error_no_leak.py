"""门面内部异常出口不得泄漏内部原文（issues/137 §3-1 · spec 06-facade.md §2.12 · python 腿）。

判据形状照 java 参考实现 ``JeeflowFacade.isForeignDetail(type, message, cause, trace)`` ＋ 测试
``FacadeInternalErrorNoLeakTest``：文案判据抽成**纯函数**（``jeeflow.facade.is_foreign_detail``，
四个入参全是已抽好的值，不碰异常对象、不产生副作用），副作用（记日志）单独一格验，
门面出口则走**真实调用路径**（不是只测纯函数）。

本栈的关键前提（普查见 jeeflow-hub/docs/批三-3-1-落地记录-2026-10-02.md §1.5 python 行，
本轮已逐条复核）：``jeeflow/*.py`` 里**零** ``class *Error/*Exception`` 定义，引擎的契约文案
全部是裸 ``raise ValueError(...)``（102 处）＋ ``raise NotImplementedError``（1 处，``meta.py:100``）
⇒ 判别式**第 2 条（契约异常族）在本栈无处落**，判据重心在 1/3/4/5 条；
且第 4 条**绝不能**把 ``ValueError`` 整族判内部——那等于把整个契约面静默改写成固定文案，
正是 java 侧「约十处裸 RuntimeException/ISE/IAE 携带中文契约文案」那个教训的本栈放大版
（java 是约十处，本栈是 102 处）。所以 ``ValueError`` 只按两条例外收：
CPython 内置解析文案模板（python 没有 ``NumberFormatException`` 这个独立类型）＋
非契约载体的子类（``UnicodeError`` / ``json.JSONDecodeError``）。

两侧都要有牙：
  **负向**＝内部原文（运行时／解析器／驱动／集成方 provider）不得进 ``msg``，只能进日志与 cause；
  **正向／回归**＝引擎自己写的契约文案必须**逐字**留在 ``msg``——判据过宽就是静默改契约面，
  而 java 侧这类文案一处测试都没钉（spec §2.12 明文点出），所以这一组在本文件里占一半篇幅。
"""
import json
import logging
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository
from jeeflow import facade as facade_mod
from jeeflow.facade import (INTERNAL_FAILURE_MSG, JeeflowFacade, foreign_detail_of,
                            is_foreign_detail, runtime_internal_detail, thrown_inside_engine)
from jeeflow.memory import MemoryExtRepository
from jeeflow.model import InstanceState, ProcessDefine, ProcessInstance, UserInfo
from jeeflow.spi import ExpressionEvaluator, IDGenerator, UserProvider

# ─── 逐字常量（**硬编码字面量**，不引用被测常量：常量本身写错也要红）────────────────────

#: 八栈逐字同一串（owner 2026-10-02 第 3 问拍 A）。硬编码，不从 facade 导入。
_FIXED = "流程处理失败"

#: issues/139 的解析腿契约文案（本文件里当"正向"用：它是引擎写的，必须逐字透出）
_PARSE_MSG = "读取流程定义 JSON 失败"

#: 出口 msg 一律不得出现的内部细节（异常类名／解析器文本／堆栈／驱动原文／文件路径）
_LEAK_MARKERS = ["Error", "Exception", "Traceback", 'File "', "object has no attribute",
                 "could not convert", "invalid literal", "Expecting", "line 1", "column",
                 "char ", "sqlite3", "aiomysql", "asyncpg", "12345", "1045", "Access denied",
                 "handler-boom", "SENTINEL", "jeeflow/", ".py"]

# ─── 帧归属夹具（第 5 条纯函数格用）────────────────────────────────────────────────────

_ENGINE_DIR = os.path.dirname(os.path.abspath(facade_mod.__file__))
_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))

#: 栈顶帧在引擎主包内（逐模块各取一个真实文件，覆盖 facade/engine/model/spi/persist/meta/memory/repo）
_F_FACADE = (os.path.join(_ENGINE_DIR, "facade.py"),)
_F_ENGINE = (os.path.join(_ENGINE_DIR, "engine.py"),)
_F_MODEL = (os.path.join(_ENGINE_DIR, "model.py"),)
_F_SPI = (os.path.join(_ENGINE_DIR, "spi.py"),)
_F_PERSIST = (os.path.join(_ENGINE_DIR, "persist.py"),)
_F_META = (os.path.join(_ENGINE_DIR, "meta.py"),)
_F_REPO_BASE = (os.path.join(_ENGINE_DIR, "repository", "base.py"),)
_F_MEMORY = (os.path.join(_ENGINE_DIR, "memory.py"),)

#: 栈顶帧在引擎包外（集成方 provider / 测试桩 / 标准库 / 第三方驱动）
_F_TESTS = (os.path.join(_TESTS_DIR, "test_facade_internal_error_no_leak.py"),)
_F_STDLIB = (os.path.join(os.path.dirname(os.path.abspath(json.__file__)), "decoder.py"),)
_F_DRIVER = (os.path.join(os.path.dirname(os.path.abspath(sqlite3.__file__)), "dbapi2.py"),)


# ─── SPI 桩（全部定义在本文件 ⇒ 抛出点在引擎包外，就是"集成方 provider"那一档）─────────

class _UserProv(UserProvider):
    async def get_user(self, user_id: str):
        return UserInfo(userId=user_id, realName=f"用户{user_id}", deptId="D01",
                        deptName="测试部门", postId="P01", postName="测试岗位")


class _IdGen(IDGenerator):
    def __init__(self):
        self.n = 0

    def next_id(self) -> int:
        self.n += 1
        return self.n


class _Expr(ExpressionEvaluator):
    async def eval(self, expr: str, vars: dict):
        return False


class _ThrowingRepo(MemoryRepository):
    """内存仓 ＋ 按方法名注入异常（java 侧 ``Proxy.newProxyInstance`` 那个夹具的本栈对偶）。

    本类定义在 ``tests/`` ⇒ 注入异常的抛出点在**引擎包外**，正好是 spec §2.12 第 5 条
    点名的"集成方 provider／测试桩"那一档。
    """

    def __init__(self, to_throw=None):
        super().__init__()
        self.to_throw = dict(to_throw or {})

    def _boom(self, name):
        exc = self.to_throw.get(name)
        if exc is not None:
            raise exc

    async def page_instances(self, *a, **kw):
        self._boom("page_instances")
        return await super().page_instances(*a, **kw)

    async def page_defines(self, *a, **kw):
        self._boom("page_defines")
        return await super().page_defines(*a, **kw)

    async def find_define_by_id(self, *a, **kw):
        self._boom("find_define_by_id")
        return await super().find_define_by_id(*a, **kw)

    async def find_instance_by_id(self, *a, **kw):
        self._boom("find_instance_by_id")
        return await super().find_instance_by_id(*a, **kw)


class _ThrowingMetaReader:
    """bizData 腿的 meta_reader 桩（issue 30 注入式）——集成方实现，抛出点在引擎包外"""

    def __init__(self, exc):
        self._exc = exc

    def read_by_process_instance(self, table_name, process_instance_id):
        raise self._exc


class _ProviderBoom(Exception):
    """集成方自定义异常类型（``__module__`` 既不是 ``builtins`` 也不在 ``jeeflow`` 下）"""


# ─── 夹具 ──────────────────────────────────────────────────────────────────────────────

#: ``_facade(ext=...)`` 的哨兵：区分"没传 ⇒ 建一枚新的内存扩展仓"与"显式传 None ⇒ 不配扩展仓"
_DEFAULT_EXT = object()


@pytest.fixture
def facade_log():
    """挂一枚 handler 到门面自己的 logger（java 侧 ``FACADE_LOG.addHandler(CAPTURE_HANDLER)`` 的对偶）。

    不用 pytest ``caplog``：caplog 走 root 传播、抓到的是全套件的记录；这里要精确断言
    "门面这一条 ERROR 记录带着**原异常对象**"（cause 分离的日志那一半）。
    """
    log = logging.getLogger("jeeflow.facade")
    captured = []

    class _Capture(logging.Handler):
        def emit(self, record):
            captured.append(record)

    handler = _Capture()
    old_level = log.level
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    try:
        yield captured
    finally:
        log.removeHandler(handler)
        log.setLevel(old_level)


def _facade(repo=None, ext=_DEFAULT_EXT, meta_reader=None):
    repo = repo if repo is not None else MemoryRepository()
    ext = MemoryExtRepository() if ext is _DEFAULT_EXT else ext
    eng = EngineImpl(repo, _UserProv(), _IdGen(), _Expr())
    f = JeeflowFacade(eng, repo, ext)
    if meta_reader is not None:
        f.set_meta_reader(meta_reader)
    return f, repo


def _repo_throwing(exc, method="page_instances"):
    return _ThrowingRepo({method: exc})


_PAGE_ARGS = {"operator": "user1", "pageNum": 1, "pageSize": 10}


def _errors(records):
    return [rec for rec in records if rec.levelno >= logging.ERROR]


def _assert_fixed_envelope(r, action, boom, records, *, also_markers=()):
    """失败信封四件（照 java ``npeFromEnginePathKeepsInternalsOutOfMsg`` 的三段 ＋ data 那条）：
    ① code 固定 99999999；② msg **逐字**等于固定文案；③ 逐项禁泄漏（含 data）；
    ④ 原文连同栈进了日志，且日志里拿到的是**原异常对象本身**。
    """
    assert r["code"] == 99999999, f"{action} 必须报失败，不得被吞成成功: {r}"
    assert r["msg"] == _FIXED, (
        f"{action} 出口 msg 要逐字等于 {_FIXED!r}（内部原文不得外透），实得 {r['msg']!r}")
    leaked = [m for m in (*_LEAK_MARKERS, *also_markers) if m in str(r["msg"])]
    assert not leaked, f"{action} msg 泄漏内部细节 {leaked}: {r['msg']!r}"
    assert r.get("data") is None, f"原文也不得从 data 那一侧漏: {r}"

    errs = _errors(records)
    assert len(errs) == 1, f"期望门面记且只记一条 ERROR（原文进日志那一半），实得 {len(errs)} 条"
    rec = errs[0]
    assert rec.name == "jeeflow.facade", f"要记在门面自己的 logger 上: {rec.name}"
    assert rec.exc_info is not None and rec.exc_info[1] is boom, \
        f"日志里要拿到原异常对象（java assertSame(npe, rec.getThrown()) 的对偶）: {rec.exc_info}"
    assert action in rec.getMessage(), f"日志要指出是哪个 action: {rec.getMessage()}"


# ═══ A 组 · 纯函数格：五条判据各自可测（文案判据与副作用分离）═══════════════════════════

def test_fixed_msg_is_verbatim_and_cross_stack_identical():
    """固定文案逐字（八栈同一串）：字面量 ＋ 常量 ＋ UTF-8 字节长三处都钉，改措辞当场红"""
    assert INTERNAL_FAILURE_MSG == _FIXED
    assert INTERNAL_FAILURE_MSG == "流程处理失败"
    assert len(INTERNAL_FAILURE_MSG.encode("utf-8")) == 18, "六个汉字 = 18 字节（跨栈比对用）"
    assert INTERNAL_FAILURE_MSG != _PARSE_MSG, "两句固定文案是两件事，不得合并成一句"


def test_criterion_1_blank_message_is_internal():
    """第 1 条：没有可用 message ⇒ 内部（java 对偶 ``message == null``；本栈对偶是空串）"""
    for message in (None, "", "   ", "\n\t "):
        assert is_foreign_detail(ValueError, message, None, _F_FACADE) is True, repr(message)
    # 活的无参异常同判：`raise NotImplementedError` 的 str() 就是 ''（meta.py:100 那个形状）
    try:
        raise NotImplementedError
    except NotImplementedError as e:
        assert str(e) == "", "前提：无参异常 str() 为空串"
        assert foreign_detail_of(e) is True


def test_criterion_3_bare_wrapper_is_internal():
    """第 3 条：裸包装（message 恰等于 ``str(cause)``）⇒ 内部——引擎没写过这段文案，只是搬运。

    python 两种搬运写法都认：``raise X(str(e)) from e`` 与 ``raise X(e)``；
    附带认 java ``String.valueOf(cause)`` 那个带类型名前缀的形状（``"ValueError: 原文"``）。
    **栈顶帧钉在引擎包内**，第 5 条帮不上忙 ⇒ 命中的必是第 3 条。
    """
    inner = sqlite3.OperationalError("内部驱动细节 12345")
    # 形状 a：raise ValueError(str(inner)) from inner
    assert is_foreign_detail(ValueError, str(inner), inner, _F_FACADE) is True
    # 形状 b：java `String.valueOf(cause)` 的对偶——带 **cause 的类型名**前缀
    assert is_foreign_detail(ValueError, f"{type(inner).__name__}: {inner}", inner, _F_FACADE) is True
    assert is_foreign_detail(ValueError, "OperationalError: 内部驱动细节 12345", inner, _F_FACADE) is True
    # 反证：message 不是搬运（引擎自己写了一句）⇒ 不判内部
    assert is_foreign_detail(ValueError, "流程实例不存在", inner, _F_FACADE) is False
    # 反证：cause 文案为空时不得把任何 message 都判成裸包装
    assert is_foreign_detail(ValueError, "流程实例不存在", ValueError(""), _F_FACADE) is False


def test_criterion_4_runtime_type_family_is_internal():
    """第 4 条 a 腿：运行时／解析器／IO 类型族 ⇒ 内部。**栈顶帧故意放在引擎包内**，
    这样命中的必是第 4 条而不是第 5 条——两条判据要各自可测。"""
    cases = [
        (TypeError, "'NoneType' object is not subscriptable"),           # java NPE 对偶
        (AttributeError, "'NoneType' object has no attribute 'lower'"),   # java NPE／反射对偶
        (KeyError, "'f_amount'"),
        (IndexError, "list index out of range"),                          # java IndexOutOfBounds
        (NameError, "name 'foo' is not defined"),
        (ZeroDivisionError, "division by zero"),                          # java ArithmeticException
        (OverflowError, "math range error"),
        (RecursionError, "maximum recursion depth exceeded"),             # java StackOverflowError
        (ImportError, "No module named 'aiomysql'"),                      # java LinkageError
        (ModuleNotFoundError, "No module named 'asyncpg'"),
        (OSError, "[Errno 2] No such file or directory: '/x'"),           # java IOException
        (ConnectionRefusedError, "[Errno 111] Connection refused"),
        (TimeoutError, "timed out"),
        (SyntaxError, "invalid syntax"),
        (AssertionError, "assert failed"),
        (StopAsyncIteration, "x"),
        (MemoryError, "x"),                                               # java VirtualMachineError
        (RuntimeError, "Event loop is closed"),
        (UnicodeDecodeError, "x"),                                        # ⚠ ValueError 子类
        (json.JSONDecodeError, "x"),                                      # ⚠ ValueError 子类
    ]
    for exc_type, message in cases:
        assert is_foreign_detail(exc_type, message, None, _F_FACADE) is True, \
            f"{exc_type.__name__} 属运行时族，必须判内部"


def test_criterion_4_numeric_parse_templates():
    """第 4 条 b 腿：数字／时间解析族。

    python **没有** ``NumberFormatException`` 这个独立类型——``float('x')`` / ``int('x')`` 抛的是
    裸 ``ValueError``，与 102 处契约文案同一个类型 ⇒ 只能按 CPython 内置文案模板识别。
    模板不写死在断言里，而是**当场把内置函数调炸**取真实文案（换 CPython 版本漂了当场红）。
    """
    from datetime import datetime

    def _grab(fn):
        try:
            fn()
        except ValueError as e:
            return str(e)
        raise AssertionError("前提没了：这个调用本该抛 ValueError")

    real_messages = [
        _grab(lambda: float("x")),                           # could not convert string to float: 'x'
        _grab(lambda: int("x")),                             # invalid literal for int() with base 10
        _grab(lambda: int("x", 16)),                         # ... with base 16
        _grab(lambda: complex("x")),                         # complex() arg is a malformed string
        _grab(lambda: bytes.fromhex("zz")),                  # non-hexadecimal number found in fromhex()
        _grab(lambda: datetime.fromisoformat("zz")),         # Invalid isoformat string: 'zz'
        _grab(lambda: datetime.strptime("zz", "%Y-%m-%d")),  # time data 'zz' does not match format
        _grab(lambda: datetime.strptime("2026-10-02zz", "%Y-%m-%d")),  # unconverted data remains
    ]
    assert len(set(real_messages)) == len(real_messages), f"前提：八条模板文案互不相同 {real_messages}"
    for message in real_messages:
        # 栈顶帧在引擎包内 ⇒ 第 5 条放行，命中的必须是第 4 条 b 腿
        assert is_foreign_detail(ValueError, message, None, _F_FACADE) is True, \
            f"CPython 解析原文属内部信息，判据漂了: {message!r}"
        assert runtime_internal_detail(ValueError, message) is True, message


def test_criterion_4_never_misfires_on_engine_contract_value_error():
    """第 4 条的**反闸**（本组最重要的一格）：引擎拿 ``ValueError`` 携带的契约文案一条都不许误伤。

    语料是本轮 grep ``jeeflow/`` 全量 ``raise ValueError`` 的真实文案（102 处逐模块取样，
    含带插值的那几种形状）。任何一条被判内部＝契约面被静默改写（spec §2.12 明文警告的坑）。
    """
    corpus = [
        # facade.py
        (_F_FACADE, "id 缺失或非法"),
        (_F_FACADE, "流程定义不存在"),
        (_F_FACADE, "流程实例不存在"),
        (_F_FACADE, "流程设计不存在"),
        (_F_FACADE, "流程设计没有内容，无法发布"),
        (_F_FACADE, "流程定义缺少 name"),
        (_F_FACADE, "processDefineId 缺失或非法"),
        (_F_FACADE, "opType/state 缺失或非法"),
        (_F_FACADE, "operator 必填"),
        (_F_FACADE, "无权限撤回该流程实例"),
        (_F_FACADE, "processTaskId 缺失"),
        (_F_FACADE, "processInstanceId 缺失"),
        (_F_FACADE, "processInstanceId/actorIds 缺失"),
        (_F_FACADE, "processTaskId/actorIds 缺失"),
        (_F_FACADE, "任务不存在"),
        (_F_FACADE, "未配置 user_search（用户搜索钩子）"),
        (_F_FACADE, "未配置 ProcessExtRepository（扩展仓储）"),
        (_F_FACADE, "fromActor 必填"),
        (_F_FACADE, "toActor 必填"),
        (_F_FACADE, "无权限转办该任务"),
        (_F_FACADE, "任务非进行中，不可转办"),
        (_F_FACADE, "原办理人不是该任务参与人"),
        (_F_FACADE, "目标人已是该任务参与人"),
        (_F_FACADE, "无权限摘除该任务参与人"),
        (_F_FACADE, "任务非进行中，不可摘除参与人"),
        (_F_FACADE, "至少需保留一名参与人"),
        (_F_FACADE, "委托记录不存在"),
        (_F_FACADE, "流程定义未配置 relTableName"),
        (_F_FACADE, "业务数据读取器未注册（facade.set_meta_reader(MetaTableReader(...))，需引入 jeeflow.meta）"),
        (_F_FACADE, "流程定义不存在: 请假流程"),
        (_F_FACADE, "id 2.0843205438341243e+18 超出 float64 精确范围（2^53），请以字符串传递"),
        (_F_FACADE, "不支持的 granularity: bogus"),
        (_F_FACADE, "trend 缺少必填参数：start/end/granularity"),
        (_F_FACADE, "不支持的 dimension: bogus"),
        (_F_FACADE, "content 缺失"),
        (_F_FACADE, "未知 action: processDefine/bogus"),
        (_F_FACADE, _PARSE_MSG),                        # issues/139 解析腿：契约文案，必须逐字透出
        # engine.py
        (_F_ENGINE, "ExpressionEvaluator 未配置"),
        (_F_ENGINE, "define not found: 2084320543834124290"),
        (_F_ENGINE, "no start node"),
        (_F_ENGINE, "根据节点名称[no-such-node]无法找到节点模型"),
        (_F_ENGINE, "task not found: 88"),
        (_F_ENGINE, "task not doing"),
        (_F_ENGINE, "operator hacker not allowed"),
        (_F_ENGINE, "instance not found"),
        (_F_ENGINE, "postInterceptors 声明的拦截器未注册: com.example.NoSuch"),
        # model.py
        (_F_MODEL, "流程实例非进行中，无法撤回"),
        # spi.py
        (_F_SPI, "processTaskId 缺失或非法: ''"),
        (_F_SPI, "processTaskId 缺失或非法: 0"),
        # persist.py / meta.py（英文技术文案同样是引擎写的，不属"内部原文"那一档）
        (_F_PERSIST, "persist: table 'biz_leave' not found"),
        (_F_PERSIST, "persist: no matching columns for biz_leave"),
        (_F_PERSIST, "persist: update biz_leave requires where column"),
        (_F_PERSIST, "persist: table name is empty"),
        (_F_PERSIST, "persist: table 'sys_user' with sys_ prefix is not allowed"),
        (_F_PERSIST, "persist: table 'a b' contains illegal characters"),
        (_F_META, "persist: parent primary key missing, cannot insert sub table f_x"),
        (_F_MEMORY, "内存仓里抛的契约文案同样按引擎包内判"),
        (_F_REPO_BASE, "仓储基类里抛的契约文案同样按引擎包内判"),
    ]
    for frames, message in corpus:
        assert is_foreign_detail(ValueError, message, None, frames) is False, \
            f"引擎契约文案被误判成内部（契约面会被静默改写）: {message!r}"
        assert runtime_internal_detail(ValueError, message) is False, message


def test_criterion_4_driver_and_third_party_types_are_internal():
    """第 4 条 c 腿：驱动／第三方类型族 ⇒ 内部。

    ``aiomysql`` / ``asyncpg`` / ``pymysql`` / ``sqlite3`` 的 ``Error`` 全都只 ``extends Exception``、
    类型名上零共性，而核心包**零依赖**（``pyproject.toml dependencies = []``）不能 import 它们
    ⇒ 按「类型不是 ``builtins`` 定义的」判。
    **栈顶帧故意钉在 ``jeeflow/repository/base.py``**：``base.py:224`` 的
    ``except BaseException: rollback; raise`` 会把驱动异常在引擎包内裸重抛，栈顶帧因此落在
    base.py ⇒ 第 5 条会**误判成引擎写的**，只有 c 腿能兜住。这一格就是钉这条腿的。
    """
    driver_text = "(1045, \"Access denied for user 'root'@'10.0.0.7' (using password: YES)\")"
    for exc_type in (sqlite3.Error, sqlite3.OperationalError, sqlite3.ProgrammingError,
                     sqlite3.IntegrityError, sqlite3.DatabaseError):
        assert is_foreign_detail(exc_type, driver_text, None, _F_REPO_BASE) is True, exc_type.__name__
        assert runtime_internal_detail(exc_type, "任何文案") is True, exc_type.__name__
        assert thrown_inside_engine(_F_REPO_BASE) is True, "前提：这一格必须由 c 腿兜，不是第 5 条"
    # 集成方自定义异常类型（__module__ = 本测试模块）同判
    assert is_foreign_detail(_ProviderBoom, "handler-boom", None, _F_REPO_BASE) is True
    assert runtime_internal_detail(_ProviderBoom, "handler-boom") is True
    # 标准库里的非 ValueError 类型（decimal / asyncio 一族）同判
    import decimal
    assert runtime_internal_detail(decimal.InvalidOperation, "转换失败") is True


def test_criterion_5_frame_attribution():
    """第 5 条：抛出点不在引擎主包 ⇒ 内部（纯函数，只吃文件名序列；下标 0 = 最内层帧）"""
    assert thrown_inside_engine(_F_FACADE) is True
    assert thrown_inside_engine(_F_ENGINE) is True
    assert thrown_inside_engine(_F_REPO_BASE) is True          # 子包 repository/ 也算引擎主包
    assert thrown_inside_engine(_F_MEMORY) is True
    assert thrown_inside_engine(_F_TESTS) is False             # 测试桩
    assert thrown_inside_engine(_F_STDLIB) is False            # 标准库
    assert thrown_inside_engine(_F_DRIVER) is False            # 第三方驱动
    assert thrown_inside_engine(()) is False                   # 无栈（java：trace == null）
    assert thrown_inside_engine(None) is False
    # 包**兄弟**目录不算包内（`jeeflow-x/…` 不得被 `startswith('jeeflow')` 那种写法误收）
    _parent = os.path.dirname(_ENGINE_DIR)
    assert thrown_inside_engine((os.path.join(_parent, "jeeflow-x", "a.py"),)) is False
    assert thrown_inside_engine((os.path.join(_parent, "demo", "main.py"),)) is False
    assert thrown_inside_engine((os.path.join(_parent, "build", "lib", "jeeflow", "facade.py"),)) is False

    # 同一条文案，只换帧归属 ⇒ 结论翻转（证明第 5 条真的在起作用，不是被别的判据遮住了）
    msg = "第三方 provider 自己写的原文"
    assert is_foreign_detail(Exception, msg, None, _F_TESTS) is True
    assert is_foreign_detail(Exception, msg, None, _F_STDLIB) is True
    assert is_foreign_detail(Exception, msg, None, _F_FACADE) is False


def test_foreign_detail_of_extracts_innermost_frame_first():
    """抽取层：``traceback.extract_tb`` 是外层→内层，要反转成 java ``getStackTrace()`` 的内层→外层序，
    于是 ``trace[0]`` 在两边都指最内层帧。这一格钉住反转没写反。"""
    import traceback as _tb

    def _inner():
        raise ValueError("第三方 provider 自己写的原文")

    def _outer():
        _inner()

    try:
        _outer()
    except ValueError as e:
        # 抛出点在本测试文件（引擎包外）⇒ 第 5 条判内部
        assert foreign_detail_of(e) is True
        frames = tuple(f.filename for f in reversed(_tb.extract_tb(e.__traceback__)))
        assert frames[0].endswith("test_facade_internal_error_no_leak.py"), \
            f"frames[0] 必须是最内层帧（本文件的 _inner），实得 {frames}"
        assert frames[-1].endswith("test_facade_internal_error_no_leak.py")
        assert is_foreign_detail(ValueError, str(e), None, frames) is True
        # 反向对照：同一异常，把栈顶帧换成引擎内 ⇒ 判据翻转（证明真的在看帧）
        assert is_foreign_detail(ValueError, str(e), None, (_F_FACADE[0], *frames)) is False


def test_foreign_detail_of_cause_extraction():
    """cause 抽取：``__cause__``（``raise … from e``）优先，回落 ``__context__``（隐式链）。
    java 只有一个 ``getCause()``，python 这两条都对应"下层原文"。
    两格都把栈顶帧钉在引擎包内，确保命中的是第 3 条而不是第 5 条。"""
    inner = sqlite3.OperationalError("内部驱动细节 12345")

    # 显式链：raise ValueError(str(inner)) from inner
    try:
        try:
            raise inner
        except sqlite3.OperationalError as e:
            raise ValueError(str(e)) from e
    except ValueError as e:
        assert e.__cause__ is inner
        assert foreign_detail_of(e) is True
        assert is_foreign_detail(ValueError, str(e), e.__cause__, _F_FACADE) is True

    # 隐式链：不写 from ⇒ 只有 __context__
    try:
        try:
            raise sqlite3.OperationalError("隐式链原文 67890")
        except sqlite3.OperationalError as e:
            raise Exception(str(e))
    except Exception as e:
        assert e.__cause__ is None and e.__context__ is not None, "前提：隐式链只有 __context__"
        assert foreign_detail_of(e) is True
        assert is_foreign_detail(Exception, str(e), e.__context__, _F_FACADE) is True

    # 反证：有 cause 但 message 是引擎自己写的 ⇒ 不判内部（issues/139 解析腿就是这个形状）。
    # 走真实调用路径：`_parse_define_content` 在 jeeflow/facade.py 里 raise，栈顶帧天然在引擎包内。
    try:
        JeeflowFacade._parse_define_content('{"nodes":[')
    except ValueError as e:
        assert isinstance(e.__cause__, json.JSONDecodeError), repr(e.__cause__)
        assert str(e) == _PARSE_MSG
        assert foreign_detail_of(e) is False, "引擎写的契约文案（带 cause）必须逐字透出"


def test_engine_module_never_defines_its_own_exception_type():
    """本栈前提的**回归钉**（普查结论 §1.5 python 行）：``jeeflow/`` 里零 ``class *Error/*Exception``
    定义 ⇒ 判别式第 2 条无处落，判据重心必须在 1/3/4/5。

    哪天有人给引擎加了契约异常类型，这一格会红——那时第 2 条才需要落地（并且要同步改
    ``is_foreign_detail`` 的空档），而不是让第 4 条的 ``ValueError`` 例外悄悄失效。
    """
    import ast

    offenders = []
    carriers = {}
    for root, dirs, files in os.walk(_ENGINE_DIR):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for name in files:
            if not name.endswith(".py"):
                continue
            path = os.path.join(root, name)
            with open(path, encoding="utf-8") as f:
                tree = ast.parse(f.read(), filename=path)
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    bases = [ast.unparse(b) for b in node.bases]
                    if any(b.split(".")[-1].endswith(("Error", "Exception")) for b in bases):
                        offenders.append(f"{os.path.relpath(path, _ENGINE_DIR)}:{node.lineno} {node.name}")
                elif isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call) \
                        and isinstance(node.exc.func, ast.Name):
                    carriers[node.exc.func.id] = carriers.get(node.exc.func.id, 0) + 1
    assert not offenders, (
        f"jeeflow/ 里出现了自定义异常类型 {offenders}——判别式第 2 条（契约异常族）需要落地了，"
        f"请同步改 is_foreign_detail 的空档并复核第 4 条的 ValueError 例外")
    assert set(carriers) <= {"ValueError", "NotImplementedError"}, \
        f"引擎的契约文案载体类型变了（普查基线是 ValueError×102 ＋ NotImplementedError×1）: {carriers}"
    assert carriers.get("ValueError", 0) >= 100, \
        f"ValueError 载体数量大幅变化 ⇒ 第 4 条例外的普查前提要重新核: {carriers}"


# ═══ B 组 · 门面出口格：走真实调用路径（照 java FacadeInternalErrorNoLeakTest）═══════════

async def test_attribute_error_from_repo_keeps_internals_out_of_msg(facade_log):
    """java NPE 对偶（``AttributeError``/``TypeError``）：原文只进日志，出口逐字固定文案"""
    boom = AttributeError("'NoneType' object has no attribute 'getRaw'")
    facade, _ = _facade(_repo_throwing(boom))

    r = await facade.flow("processInstance/page", dict(_PAGE_ARGS))

    _assert_fixed_envelope(r, "processInstance/page", boom, facade_log,
                           also_markers=["NoneType", "getRaw"])


async def test_type_error_from_repo_keeps_internals_out_of_msg(facade_log):
    boom = TypeError("unsupported operand type(s) for +: 'int' and 'str'")
    facade, _ = _facade(_repo_throwing(boom, "page_defines"))

    r = await facade.flow("processDefine/page", dict(_PAGE_ARGS))

    _assert_fixed_envelope(r, "processDefine/page", boom, facade_log,
                           also_markers=["unsupported operand"])


async def test_key_error_from_repo_keeps_internals_out_of_msg(facade_log):
    """``KeyError`` 的 ``str()`` 是带引号的 repr（``"'f_amount'"``）——改前会原样出成 msg"""
    boom = KeyError("f_amount")
    assert str(boom) == "'f_amount'"
    facade, _ = _facade(_repo_throwing(boom))

    r = await facade.flow("processInstance/page", dict(_PAGE_ARGS))

    _assert_fixed_envelope(r, "processInstance/page", boom, facade_log, also_markers=["f_amount"])


async def test_numeric_parse_value_error_keeps_internals_out_of_msg(facade_log):
    """137 案 java 侧实测出口 ``msg=For input string: "x"`` 的本栈对偶。

    这里走注入的仓储腿（抛出点在包外 ⇒ 第 4 条 b 腿与第 5 条同时命中）；
    **第 4 条 b 腿单独可测**的那一半在 A 组 ``test_criterion_4_numeric_parse_templates``
    （栈顶帧钉在引擎包内，第 5 条帮不上忙）。引擎自己的数字解析点已全部就地 guard
    （``facade._to_int`` / ``engine.py`` 四处 ``except (TypeError, ValueError)`` /
    ``surrogate.to_datetime``），所以真实世界里这一族主要从集成方 provider 上来。
    """
    try:
        float("x")
    except ValueError as e:
        boom = e
    assert str(boom) == "could not convert string to float: 'x'", str(boom)
    facade, _ = _facade(_repo_throwing(boom))

    r = await facade.flow("processInstance/page", dict(_PAGE_ARGS))

    _assert_fixed_envelope(r, "processInstance/page", boom, facade_log,
                           also_markers=["could not convert string to float"])


async def test_json_decode_error_keeps_internals_out_of_msg(facade_log):
    """JSON 解析器原文（``json.JSONDecodeError`` 是 ``ValueError`` 子类 ⇒ 只能按子类收，
    绝不能按 ``ValueError`` 收）"""
    try:
        json.loads('{"nodes":[SENTINEL_泄漏面_137]}')
    except json.JSONDecodeError as e:
        boom = e
    facade, _ = _facade(_repo_throwing(boom, "find_define_by_id"))

    r = await facade.flow("processDefine/detail", {"id": 7, "operator": "user1"})

    _assert_fixed_envelope(r, "processDefine/detail", boom, facade_log,
                           also_markers=["Expecting", "SENTINEL"])


async def test_bare_wrapper_keeps_cause_text_out_of_msg(facade_log):
    """java ``bareWrapperKeepsCauseTextOutOfMsg`` 的对偶：``raise X(str(e)) from e`` 只是搬运下层
    原文，引擎没写过它 ⇒ 出口固定文案；但 cause 那一半（``__cause__``）要能在日志里拿到。"""
    inner = sqlite3.OperationalError("内部驱动细节 12345")
    boom = ValueError(str(inner))
    boom.__cause__ = inner
    facade, _ = _facade(_repo_throwing(boom))

    r = await facade.flow("processInstance/page", dict(_PAGE_ARGS))

    _assert_fixed_envelope(r, "processInstance/page", boom, facade_log, also_markers=["内部驱动细节"])
    rec = _errors(facade_log)[0]
    assert rec.exc_info[1].__cause__ is inner, "原文要留在错误对象的 __cause__ 上，从日志拿得到"
    assert "12345" in str(rec.exc_info[1].__cause__), "cause 里的原文不得被整个丢掉"


async def test_driver_error_via_bizdata_leg_keeps_internals_out_of_msg(facade_log):
    """覆盖面第 ② 处的另一条腿：``processInstance/bizData`` 的 meta_reader（集成方注入）抛驱动异常。

    真实形状＝``MetaTableReader`` 底下 sqlite3/aiomysql 报连接或 SQL 错，原文带主机名、账号、
    口令提示——正是 spec §2.12 点名不得外透的那一类。
    """
    boom = sqlite3.OperationalError(
        "(1045, \"Access denied for user 'root'@'10.0.0.7' (using password: YES)\")")
    facade, repo = _facade(meta_reader=_ThrowingMetaReader(boom))
    define = ProcessDefine(name="biz137", displayName="业务137", type="approval", state=1,
                           content=json.dumps({"name": "biz137", "relTableName": "biz_leave"}))
    repo.add_define(define)
    inst = ProcessInstance(defineId=define.id, state=InstanceState.DOING, operator="user1")
    await repo.save_instance(inst)

    r = await facade.flow("processInstance/bizData",
                          {"processInstanceId": inst.id, "operator": "user1"})

    _assert_fixed_envelope(r, "processInstance/bizData", boom, facade_log,
                           also_markers=["Access denied", "10.0.0.7", "using password"])


async def test_third_party_provider_exception_keeps_internals_out_of_msg(facade_log):
    """集成方自定义异常类型 ＋ 自定义文案（第 4 条 c 腿 ＋ 第 5 条同时命中）"""
    boom = _ProviderBoom("provider 内部实现细节 SENTINEL-137")
    facade, _ = _facade(_repo_throwing(boom))

    r = await facade.flow("processInstance/page", dict(_PAGE_ARGS))

    _assert_fixed_envelope(r, "processInstance/page", boom, facade_log,
                           also_markers=["provider 内部实现细节"])


async def test_blank_message_exception_gets_fixed_text_not_empty_msg(facade_log):
    """改前这一格出口是 ``msg=""``（空信封，前端 toast 一片空白）：``str(NotImplementedError())``
    就是空串。第 1 条把它收成固定文案。"""
    boom = NotImplementedError()
    assert str(boom) == ""
    facade, _ = _facade(_repo_throwing(boom))

    r = await facade.flow("processInstance/page", dict(_PAGE_ARGS))

    assert r["code"] == 99999999, r
    assert r["msg"] == _FIXED, f"空 message 不得原样出成空信封: {r['msg']!r}"
    assert _errors(facade_log), "原文（含栈）要进日志"


async def test_unknown_action_still_reports_action_name():
    """``未知 action: …`` 是引擎自己写的契约文案（门面直接 return，不经异常腿）⇒ 保持原样。
    这一格是防"顺手把整条出口都换成固定文案"（判据收窄的极端形）。"""
    facade, _ = _facade()
    r = await facade.flow("processDefine/bogus", {})
    assert r["code"] == 99999999, r
    assert r["msg"] == "未知 action: processDefine/bogus", r["msg"]


# ── 正向／回归：引擎契约文案逐字透出，没被收窄（spec §2.12 明文警告的那一面）──────────

async def test_engine_contract_text_still_passes_through_verbatim(facade_log):
    """六个模块各取真实腿，断言 msg **逐字**等于引擎写的那句，且门面一条 ERROR 都不记
    （java ``engineIllegalStateOutsideContractTypeStillPassesThrough`` 的 ``CAPTURED.isEmpty()`` 对偶）。
    """
    facade, repo = _facade(ext=None)

    cases = [
        # (action, args, 期望逐字文案)
        ("processDefine/detail", {}, "id 缺失或非法"),
        ("processDefine/detail", {"id": 424242}, "流程定义不存在"),
        ("processDesign/page", dict(_PAGE_ARGS), "未配置 ProcessExtRepository（扩展仓储）"),
        ("processInstance/stats/trend", {"granularity": "bogus"}, "不支持的 granularity: bogus"),
        ("processInstance/withdraw", {"id": 1, "operator": ""}, "operator 必填"),
        ("processTask/addCandidate", {"processTaskId": 1}, "processTaskId/actorIds 缺失"),
        ("processInstance/startAndExecute",
         {"processDefineId": 2084320543834124288.0, "operator": "user1"},
         "id 2.0843205438341243e+18 超出 float64 精确范围（2^53），请以字符串传递"),
    ]
    for action, args, want in cases:
        r = await facade.flow(action, dict(args))
        assert r["code"] == 99999999, (action, r)
        assert r["msg"] == want, f"{action} 契约文案要逐字相等，实得 {r['msg']!r} want {want!r}"

    # engine.py 腿：文案里带调用方传进来的原始雪花 id（既有测试 spec_test.py:1544 钉的就是这条）
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": "2084320543834124290", "operator": "user1"})
    assert r["code"] == 99999999, r
    assert r["msg"] == "define not found: 2084320543834124290", r["msg"]

    # model.py 腿：聚合根里的状态守卫（issues/134 案 A，内部码 20010009 不进 msg）
    done = ProcessInstance(defineId=1, state=InstanceState.DONE, operator="zhangsan")
    await repo.save_instance(done)
    r = await facade.flow("processInstance/withdraw", {"id": done.id, "operator": "zhangsan"})
    assert r["code"] == 99999999, r
    assert r["msg"] == "流程实例非进行中，无法撤回", r["msg"]
    assert "2001000" not in r["msg"], f"内部码不进 msg（issues/121 口径）: {r['msg']!r}"

    # spi.py 腿：仓储写侧的主键档守卫（绕过门面直连仓储的调用方）
    with pytest.raises(ValueError) as ei:
        await repo.add_task_actor("", ["a"])
    assert str(ei.value) == "processTaskId 缺失或非法: ''", str(ei.value)
    assert is_foreign_detail(type(ei.value), str(ei.value), None, _F_SPI) is False, \
        "spi.py 的契约文案不得被判内部"

    assert not _errors(facade_log), \
        f"引擎自己写的文案不该被记成内部异常日志（记了就说明判据已经过宽）: {[r.getMessage() for r in _errors(facade_log)]}"


async def test_parse_leg_contract_text_passes_through_verbatim(facade_log):
    """覆盖面第 ② 处（bizData／JSON 解析族）**已到位**的证据：deploy 腿的坏 JSON
    ⇒ 出口 msg 逐字是 ``读取流程定义 JSON 失败``（引擎写的契约文案），既不是解析器原文、
    也**不是**本轮那句 ``流程处理失败``——两句固定文案是两件事，收窄成一句就是改契约面。

    改前（顶层 ``str(e)``）这条腿本来也绿：``_parse_define_content`` 在 issues/139 那轮已把
    cause 分离（契约文案进 message、原文进 ``raise ... from e`` 的 ``__cause__``），
    ``str(e)`` 拿到的正是契约文案。本格钉的是"§3-1 的判别式没把它改坏"。
    """
    facade, _ = _facade()
    for leg, args in (
        ("processDefine/deploy", {"content": '{"name":"bad137","nodes":[', "operator": "zhangsan"}),
        ("processDefine/deploy", {"content": '{"nodes":[SENTINEL_泄漏面_137]}', "operator": "zhangsan"}),
        ("processDefine/deploy", {"content": "", "operator": "zhangsan"}),
    ):
        r = await facade.flow(leg, dict(args))
        assert r["code"] == 99999999, (leg, args, r)
        assert r["msg"] == _PARSE_MSG, (
            f"{leg} 解析腿契约文案要逐字相等（不得被本轮固定文案吃掉、更不得漏解析器原文），"
            f"实得 {r['msg']!r}")
        leaked = [m for m in _LEAK_MARKERS if m in r["msg"]]
        assert not leaked, f"{leg} msg 泄漏内部细节 {leaked}: {r['msg']!r}"

    # 解析腿走的是"引擎契约文案"那一档 ⇒ 门面不该记 ERROR（记了就说明被判成内部了）
    assert not _errors(facade_log), [r.getMessage() for r in _errors(facade_log)]


async def test_parse_leg_keeps_original_exception_as_cause():
    """cause 分离的另一半（``raise ... from e`` ⇒ ``__cause__``）：解析器原文留在错误对象上，
    排查侧拿得到，只是不跨出口。与既有 ``test_i139_parse_fail_keeps_original_exception_as_cause``
    同判据，本文件自带一份好让 §3-1 的覆盖面主张自洽（不改动既有测试）。"""
    with pytest.raises(ValueError) as ei:
        JeeflowFacade._parse_define_content('{"name":"bad137","nodes":[')
    assert str(ei.value) == _PARSE_MSG, str(ei.value)
    assert isinstance(ei.value.__cause__, json.JSONDecodeError), repr(ei.value.__cause__)
    # 判别式对这条的结论：引擎契约文案 ⇒ False（逐字透出）
    assert is_foreign_detail(type(ei.value), str(ei.value), ei.value.__cause__, _F_FACADE) is False
    # 但一旦有人把 cause 原文搬进 message，第 3 条（裸包装）与第 4 条（JSONDecodeError 在族里）
    # 会双双把它挡成固定文案——这就是"构造层不放原文"与"出口层判别式"两条路的互补
    moved_text = str(ei.value.__cause__)
    assert is_foreign_detail(ValueError, moved_text, ei.value.__cause__, _F_FACADE) is True
    assert is_foreign_detail(json.JSONDecodeError, moved_text, None, _F_FACADE) is True


async def test_parse_leg_three_legs_share_the_same_verbatim_msg(facade_log):
    """三条腿共用单一解析点（对齐 java 三条腿同走 ``ModelParser.parse``）：
    ``processDefine/redeploy`` 与 ``processDesign/redeploy`` 与 ``deploy`` 同句逐字。"""
    facade, _ = _facade()
    r0 = await facade.flow("processDesign/save", {"name": "bad137d", "displayName": "坏稿137",
                                                  "content": '{"name":"bad137d","nodes":[',
                                                  "operator": "zhangsan"})
    assert r0["code"] == 0, f"前置：save 不校验内容合法性，坏 JSON 也该入库回 id: {r0}"
    design_id = int(r0["data"]["id"])

    r = await facade.flow("processDesign/redeploy", {"id": design_id, "operator": "zhangsan"})
    assert r["code"] == 99999999 and r["msg"] == _PARSE_MSG, r

    good = json.dumps({"name": "ok137", "displayName": "正常137", "type": "approval",
                       "nodes": [], "edges": []})
    r_ok = await facade.flow("processDefine/deploy", {"content": good, "operator": "zhangsan"})
    assert r_ok["code"] == 0 and r_ok["msg"] == "成功", f"正向对照不得被新分支拦掉: {r_ok}"
    define_id = int(r_ok["data"]["processDefineId"])

    r = await facade.flow("processDefine/redeploy",
                          {"processDefineId": define_id, "content": '{"name":"x","nodes":[',
                           "operator": "zhangsan"})
    assert r["code"] == 99999999 and r["msg"] == _PARSE_MSG, r
    untouched = await facade._repo.find_define_by_id(define_id)
    assert untouched.content == good, "被拒的 redeploy 不得改写既有定义内容"
    assert not _errors(facade_log), [r.getMessage() for r in _errors(facade_log)]
