"""统一门面（v1.1.0）——"接口即 POST + JSON body"风格的单入口

集成方只实现一个转发端点：把 body JSON 转成 dict 传入 flow()，
所有流程能力按 action（boot2/boot3 端点短名）路由。返回统一结构
{code, msg, data}（code=0 成功 / 99999999 失败）。

操作人约定：门面不感知登录态，args["operator"] 显式传入。
"""
from __future__ import annotations

import dataclasses
import inspect
import asyncio
import json
import logging
import os
import re
import traceback
from datetime import datetime, timedelta
from typing import Any, Optional

from .engine import (Engine, KEY_ADMIN_ID, KEY_AUTO_ID, KEY_CC_ACTORS, KEY_CC_ACTORS_START,
                     KEY_NEXT_NODE_OPERATOR,
                     KEY_PROCESS_START_NEXT_NODE_OPERATOR, KEY_SUBMIT_TYPE)
from .extensions import EventType, ProcessEvent
from .model import ProcessDefine, ProcessDesign, ProcessDesignHis, ProcessSurrogate, TaskState, InstanceState
from .spi import (ProcessExtRepository, ProcessRepository, QueryCondition,
                  normalize_actors, normalize_actor_value)

# submitType 枚举（对齐 boot3）

# submitType 枚举（对齐 boot3）
SUBMIT_APPLY = 0
SUBMIT_AGREE = 1
SUBMIT_REJECT = 2
SUBMIT_ROLLBACK = 3
SUBMIT_JUMP = 4
SUBMIT_ROLLBACK_TO_OPERATOR = 6
SUBMIT_TRANSFER = 7  # issues/115：转办留痕（不走 execute，由 processTask/transfer 写）
SUBMIT_COUNTERSIGN_DISAGREE = 20

# 抄送人入参键（对齐 Java FlowConst.CC_ACTORS_START / CC_ACTORS）：
# 发起腿 f_ccActors、办理腿 tf_ccActors。**两条腿都在引擎里落 cc 行并 fire CC_CREATE**
# （spec 11-events §11.7／issues/127：基准＝Java JeeflowEngineImpl.handleCcActors 在 runInTx 内；
# 办理腿的覆盖面按 §11.7 边界 2 只有 executeProcessTask 一条，见 _processTask_execute 的注释）。
# 门面只把键随 args 透传给引擎，手动腿 createCCInstance 也调同一个 engine.handle_cc_actors——
# 门面不再持有第二份实现（本轮从门面搬走的就是这个）。常量以引擎侧为单一来源，此处保留原名导出。
CC_ACTORS_START = KEY_CC_ACTORS_START
CC_ACTORS = KEY_CC_ACTORS

# issues/139（八栈同批）：流程定义 content 解析失败的**对外** msg 是逐字固定文案，
# 基准＝Java 参考实现 jeeflow-core/parser/ModelParser.java:47
# `throw new RuntimeException("读取流程定义 JSON 失败", e)`——原始异常只作 cause 挂在错误对象上，
# 一律不进 msg（门面顶层 `flow()` 出 msg 时只透**引擎自己写的**文案，见下方 §2.12 判别式；
# 解析器原文属内部信息，拼进 message 就是泄漏）。
# deploy / processDefine/redeploy / processDesign/redeploy 三条腿共用这一句，对齐 Java 三条腿
# 同走一个 ModelParser.parse 的形状。
MSG_READ_DEFINE_JSON_FAIL = "读取流程定义 JSON 失败"


# ─── issues/137 §3-1 · 门面内部异常出口（spec 06-facade.md §2.12）───────────────────────
#
# 门面顶层 `flow()` 是所有内部异常的共性通道。旧形状在 `except Exception as e` 里直接
# `self._error(str(e))`，于是运行时异常、解析器、DB 驱动、集成方 provider 写的原文一路进用户面
# （java 侧 137 案实测出口 `msg=For input string: "x"`，本栈对偶是 `could not convert string
# to float: 'x'`、`'NoneType' object has no attribute 'x'`、`Expecting value: line 1 column 1`、
# `aiomysql` 的连接原文一类）。
# 现在：判据是「这段文案是谁写的」——引擎自己写的中文契约文案照旧**逐字**透出（八栈＋十三个
# 集成壳＋前端 toast 都按原文对齐，收窄就是静默改契约面），外来/内部原文一律换成
# `INTERNAL_FAILURE_MSG`，原文连同栈只进日志与错误对象的 cause。

#: 内部异常对外只说这一句（固定文案，八栈逐字同一串，不许改措辞——owner 2026-10-02 第 3 问拍 A）
INTERNAL_FAILURE_MSG = "流程处理失败"

#: 门面自己的 logger，对齐 java `Logger.getLogger(JeeflowFacade.class.getName())`。
#: 原文只进这里（`exc_info=` 带完整异常对象＋栈＋它自己的 `__cause__`），不进 msg。
_log = logging.getLogger(__name__)

#: 第 5 条「抛出点在不在引擎主包」的归属基准（＝本文件所在的 `jeeflow/` 目录）
_ENGINE_DIR = os.path.dirname(os.path.abspath(__file__))

#: 第 4 条的「运行时／解析器／IO 类型族」——java `JeeflowFacade.jvmInternal` 的本栈等价件。
#: ⚠️ **`ValueError` 本身不在这一族**：本栈 102 处契约文案全是裸 `raise ValueError('中文')`
#: （普查见 docs/批三-3-1-落地记录-2026-10-02.md §1.5 与本轮报告），把它整族判内部＝把整个
#: 契约面静默改写成固定文案。ValueError 只按「CPython 内置解析文案模板」（下面那个正则）
#: 与「非契约载体的子类」（UnicodeError / json.JSONDecodeError）两条例外收。
_RUNTIME_INTERNAL_TYPES = (
    TypeError,          # java NullPointerException / ClassCastException 的对偶（None 上取属性、错类型下标）
    AttributeError,     # 同上：`'NoneType' object has no attribute 'x'`（java 反射族的对偶也在这）
    NameError,          # 含 UnboundLocalError
    LookupError,        # 含 KeyError / IndexError（java IndexOutOfBoundsException）
    ArithmeticError,    # java ArithmeticException（含 ZeroDivisionError / OverflowError）
    RuntimeError,       # 含 RecursionError（java StackOverflowError）/ NotImplementedError / asyncio 内部错
    MemoryError,        # java VirtualMachineError 的近似对偶
    ImportError,        # java LinkageError（含 ModuleNotFoundError）
    OSError,            # java IOException（IOError 是它的别名；含 ConnectionError / TimeoutError）
    SyntaxError,        # 含 IndentationError / TabError（eval / compile 腿）
    AssertionError,
    StopIteration, StopAsyncIteration,
    UnicodeError,       # ⚠ ValueError 的子类，但引擎不拿它当契约载体 ⇒ 解码/编码原文属内部
    json.JSONDecodeError,  # ⚠ 同上：解析器原文。issues/139 已在解析腿挡掉，这里是兜底
)

#: python **没有** `NumberFormatException` 这种独立类型：数字／时间解析腿抛的是裸 `ValueError`，
#: 与 102 处契约文案同一个类型 ⇒ 这一族只能按 CPython 内置文案模板识别（java 第 4 条
#: `NumberFormatException` 那一格的本栈等价件）。模板**不写死**：由
#: tests/test_facade_internal_error_no_leak.py::test_criterion_4_numeric_parse_templates
#: 当场把内置函数调炸取真实文案来钉（换 CPython 版本漂了当场红）。
_FOREIGN_VALUE_ERROR_RE = re.compile(
    r"\A(?:could not convert string to float"        # float('x')
    r"|invalid literal for int\(\) with base \d+"    # int('x') / int('x', 16)
    r"|complex\(\) arg is a malformed string"        # complex('x')
    r"|non-hexadecimal number found in fromhex\(\)"  # bytes.fromhex('zz')
    r"|Invalid isoformat string"                     # datetime.fromisoformat('zz')
    r"|time data .* does not match format"           # datetime.strptime('zz', fmt)
    r"|unconverted data remains"                     # strptime 尾部残余
    r")"
)


def runtime_internal_detail(exc_type, message: Optional[str]) -> bool:
    """判别式第 4 条：异常类型属**运行时／解析器／IO／驱动自己抛的族** ⇒ 内部。

    三条腿（任一命中即内部）：
      a. `_RUNTIME_INTERNAL_TYPES` 里的内置类型族（含 `ValueError` 的两个非契约子类）；
      b. `ValueError` ＋ CPython 内置数字/时间解析文案模板（python 无 `NumberFormatException`）；
      c. **类型不是 `builtins` 定义的** ⇒ 驱动／第三方类型族。
         `aiomysql` / `asyncpg` / `pymysql` / `psycopg` / `sqlite3` 的 `Error` 全都只
         `extends Exception`、类型名上零共性，而核心包**零依赖**（`pyproject.toml`
         `dependencies = []`）不能 import 它们来 isinstance ⇒ 只能按定义模块判。
         前提已普查：引擎自己的契约文案 100% 是 `builtins` 的 `ValueError`（102 处）
         与 `NotImplementedError`（1 处，`meta.py:100`），零第三方类型 ⇒ c 不会误伤契约面。
         c 还兜住 `repository/base.py:224` 那种「驱动异常在引擎包内被裸 `raise` 重抛」的腿：
         重抛会把 `base.py` 压成栈顶帧，第 5 条按帧归属会**误判成引擎写的**。

    纯函数：只吃「类型 ＋ 文案」两个值，不碰异常对象、不产生副作用。
    """
    if not isinstance(exc_type, type):
        return False
    if issubclass(exc_type, _RUNTIME_INTERNAL_TYPES):
        return True
    if message and issubclass(exc_type, ValueError) and _FOREIGN_VALUE_ERROR_RE.match(message):
        return True
    module = getattr(exc_type, "__module__", "") or ""
    return module != "builtins" and not module.startswith("jeeflow")


def thrown_inside_engine(trace) -> bool:
    """第 5 条的归属判据：栈顶帧（最内层）是否落在引擎主包目录 `jeeflow/` 里。

    ``trace`` 的形状与 java ``StackTraceElement[]`` 对齐——**下标 0 = 最内层帧**，元素是文件名
    （java 那边取 ``trace[0].getClassName()``，这边取 ``trace[0]`` 的路径）。

    java 还要额外排除 ``com.mldong.jeeflow.test.``（测试桩抛的不算引擎契约文案）；python 的
    测试目录 ``tests/`` 本就在包外，天然落到「不在引擎包」⇒ 无需第二条排除。
    ``jeeflow/memory.py``（内存仓，演示/测试夹具）虽在包内，但普查显示它**零 raise**，
    不构成"夹具文案冒充引擎契约文案"的口子。

    纯函数：只吃文件名序列。
    """
    if not trace:
        return False
    try:
        rel = os.path.relpath(os.path.abspath(str(trace[0])), _ENGINE_DIR)
    except (TypeError, ValueError):   # Windows 跨盘符时 relpath 抛 ValueError ⇒ 判不出归属，按"不在包内"
        return False
    return rel != os.pardir and not rel.startswith(os.pardir + os.sep)


def is_foreign_detail(exc_type, message, cause, trace) -> bool:
    """「这段文案能不能原样进 `msg`」——判别式五条，逐条同 spec 06-facade.md §2.12（顺序即优先级）。

    返回 ``True`` ⇒ 属内部信息 ⇒ 出口只给 :data:`INTERNAL_FAILURE_MSG`。
    java 参考实现＝``JeeflowFacade.isForeignDetail(type, message, cause, trace)``，函数名按本栈
    snake_case 直译（``isForeignDetail`` ↔ ``is_foreign_detail``，八栈可 grep 互查）。

    **四个入参全是已经抽好的值**（类型／文案／cause／栈帧文件名），不碰异常对象也不产生副作用
    ⇒ 「文案判据」与「记日志那一半副作用」可以各自单测（spec §2.12 判据形状要求）。
    从活异常对象抽四元组的那一层是 :func:`foreign_detail_of`，里面**零判据逻辑**。

    五条：
      1. 没有可用 message ⇒ 内部（兜底只会吐类型名或空串；java 对偶＝``message == null``）；
      2. 属契约异常族 ⇒ 逐字透出（本条返回 False）。**本栈这一条无处落**：``jeeflow/*.py``
         里零 ``class *Error/*Exception`` 定义（普查见 docs/批三-3-1 §1.5 python 行），引擎契约
         文案全是裸 ``raise ValueError('中文')`` ⇒ 判据重心在 1/3/4/5，此处只留空档以便八栈逐条比对；
      3. 裸包装：message 恰等于 ``str(cause)``（``raise X(str(e)) from e`` / ``raise X(e)``）⇒ 内部。
         附带认 java ``String.valueOf(cause)`` 那个形状（``"<cause 的类型名>: 原文"``，
         如 ``"OperationalError: (1045, ...)"``），因为那是同一件事的另一种写法；
      4. 运行时／解析器／IO／驱动类型族 ⇒ 内部（见 :func:`runtime_internal_detail`）；
      5. 抛出点不在引擎主包（标准库、集成方 provider、测试桩）⇒ 内部
         （见 :func:`thrown_inside_engine`）。
    """
    # ① 没有可用 message
    if message is None or not str(message).strip():
        return True
    # ② 契约异常族——本栈无此类型（见 docstring），留空档
    # ③ 裸包装
    if cause is not None:
        cause_text = str(cause)
        if cause_text and message in (cause_text, f"{type(cause).__name__}: {cause_text}"):
            return True
    # ④ 运行时／解析器／驱动类型族
    if runtime_internal_detail(exc_type, str(message)):
        return True
    # ⑤ 抛出点不在引擎主包
    return not thrown_inside_engine(trace)


def foreign_detail_of(exc: BaseException) -> bool:
    """把活异常对象拆成四元组喂给 :func:`is_foreign_detail`（**抽取层，零判据逻辑**）。

    - ``message``：``str(exc)``（java ``e.getMessage()`` 的对偶；``args`` 为空时得 ``''``，
      由第 1 条判内部——`raise NotImplementedError` 这类无参异常正是这个形状）；
    - ``cause``：``__cause__``（``raise ... from e``）优先，回落 ``__context__``（隐式链）。
      java 只有一个 ``getCause()``，python 这两条都对应"下层原文"；
    - ``trace``：``traceback.extract_tb`` 是**外层→内层**，反转成 java ``getStackTrace()``
      的**内层→外层**序，于是 ``trace[0]`` 在两边都指最内层帧。
      ⚠️ C 层内置函数（``float()`` / ``json.loads`` 一类）**不压栈帧**，栈顶帧是调用它的引擎
      文件 ⇒ 第 5 条对"引擎代码里调内置解析器"这一族无效，必须靠第 4 条的文案模板兜。
    """
    cause = exc.__cause__ if exc.__cause__ is not None else exc.__context__
    tb = exc.__traceback__
    frames = tuple(f.filename for f in reversed(traceback.extract_tb(tb))) if tb is not None else ()
    return is_foreign_detail(type(exc), str(exc), cause, frames)


class JeeflowFacade:
    """统一门面——flow(action, args) -> dict"""

    def __init__(self, engine: Engine, repo: ProcessRepository,
                 ext_repo: Optional[ProcessExtRepository] = None,
                 user_search: Optional[callable] = None,
                 org_prov: Optional["OrgUserProvider"] = None):
        self._engine = engine
        self._repo = repo
        self._ext = ext_repo
        self._user_search = user_search  # 可空：candidatePage 用户分页搜索依赖
        self._org_prov = org_prov  # 可空：candidatePage candidateGroups 角色取人（v1.6.0）
        self._meta_reader = None  # 可空：bizData 业务数据读取器（issue 30，注入式）
        # issues/116 批次 D：委托代理自动生效是**引擎内置、默认开启**能力，数据源就是门面的扩展仓储。
        # 集成方只要给门面传了 ext_repo 即零配置生效（对齐内置版白拿体验）；未传则引擎侧静默跳过。
        attach = getattr(engine, "attach_ext_repository", None)
        if attach is not None and ext_repo is not None:
            attach(ext_repo)

    def set_meta_reader(self, reader) -> "JeeflowFacade":
        """注入业务数据读取器（issue 30）：需有 read_by_process_instance(table_name, process_instance_id)"""
        self._meta_reader = reader
        return self

    def set_user_search(self, fn: callable) -> "JeeflowFacade":
        """注入用户搜索钩子：fn(query: dict) -> (rows: list[dict], total: int)"""
        self._user_search = fn
        return self

    def set_org_provider(self, org_prov: "OrgUserProvider") -> "JeeflowFacade":
        """注入组织用户提供者（candidatePage candidateGroups 角色取人）"""
        self._org_prov = org_prov
        return self

    async def flow(self, action: str, args: Optional[dict] = None) -> dict:
        args = args or {}
        try:
            handler = getattr(self, "_" + action.replace("/", "_"), None)
            if handler is None:
                return self._error(f"未知 action: {action}")
            data = await handler(args)
            # issues/38 E9 出口统一：id 类字段转 string（对齐 Node 全程 string / Java 全局
            # ToStringSerializer）——前端 JS number 无法承载雪花 id（>2^53）
            return self._ok(_stringify_ids(data))
        except Exception as e:
            # issues/137 §3-1（spec 06-facade.md §2.12）：判别规则与理由见 is_foreign_detail——
            # 引擎自己写的中文契约文案照旧**逐字**透出（八栈＋十三个集成壳＋前端 toast 都按原文
            # 对齐，在这条上收窄就是静默改契约面），只把运行时／解析器／驱动／集成方 provider
            # 写的原文换成固定文案；原文连同栈只进日志（`exc_info=` ⇒ cause 分离的"日志"那一半，
            # 与 issues/139「内部细节不进 msg」同一条尺子）。
            if foreign_detail_of(e):
                _log.error("[jeeflow] action 执行失败: action=%s", action, exc_info=e)
                return self._error(INTERNAL_FAILURE_MSG)
            return self._error(str(e))

    # ── 流程定义 / 实例 ─────────────────────────────────────────────────────

    async def _processDefine_page(self, args: dict) -> dict:
        """流程定义分页（v1.5.0 补齐）"""
        page_num = self._to_int(args.get("pageNum")) or 1
        page_size = self._to_int(args.get("pageSize")) or 10
        rows, total = await self._repo.page_defines(page_num, page_size, self._parse_m_query(args))
        return self._page_data([self._define_row_to_dict(r) for r in rows], total, page_num, page_size)

    async def _processDefine_detail(self, args: dict) -> dict:
        """流程定义详情（v1.5.0 补齐）"""
        define_id = self._to_int(args.get("id"))
        if not define_id:
            raise ValueError("id 缺失或非法")
        def_ = await self._repo.find_define_by_id(define_id)
        if not def_:
            raise ValueError("流程定义不存在")
        return {"id": def_.id, "name": def_.name, "displayName": def_.displayName,
                "type": def_.type, "state": def_.state, "version": def_.version,
                "jsonObject": self._parse_graph(def_.content)}

    async def _processDefine_startAndExecute(self, args: dict) -> dict:
        return await self._startAndExecute(args)

    async def _processInstance_page(self, args: dict) -> dict:
        """我发起的流程实例分页（operator 过滤，v1.5.0 补齐）"""
        page_num = self._to_int(args.get("pageNum")) or 1
        page_size = self._to_int(args.get("pageSize")) or 10
        operator = self._operator_arg(args)
        rows, total = await self._repo.page_instances(page_num, page_size, operator, self._parse_m_query(args))
        return self._page_data([self._instance_row_to_dict(r) for r in rows], total, page_num, page_size)

    async def _processInstance_detail(self, args: dict) -> dict:
        """流程实例详情（含任务列表，v1.5.0 补齐）"""
        instance_id = self._to_int(args.get("id"))
        if not instance_id:
            raise ValueError("id 缺失或非法")
        inst = await self._repo.find_instance_by_id(instance_id)
        if not inst:
            raise ValueError("流程实例不存在")
        graph = await self._instance_json_object(inst)
        first_task_id = self._first_task_node_id(graph)
        tasks, active_task_list = [], []
        for t in inst.tasks:
            vo = self._task_vo(t)
            ext = dict(t.variables or {})
            doing = t.taskState == TaskState.DOING
            # issues/121 P1：行上值优先（引擎建单时写入，历史行同样有效），缺键（存量行）才回退现算
            row_first = (t.variables or {}).get("isFirstTaskNode")
            ext["isFirstTaskNode"] = bool(row_first) if row_first is not None                 else (doing and t.taskName == first_task_id)
            vo["ext"] = ext
            tasks.append(vo)
            if doing:
                active_task_list.append(vo)
        data = {
            "id": inst.id, "parentId": inst.parentId, "processDefineId": inst.defineId,
            "state": inst.state, "parentNodeName": inst.parentNodeName,
            "businessNo": inst.businessNo, "operator": inst.operator,
            "ext": inst.variables or {},  # issues/124：变量唯一对外出口，空变量出 {} 而非 null
            "formData": self._form_data_of(inst.variables, "f_"),  # issues/15
            "createTime": inst.createTime, "createUser": inst.createUser,
            "jsonObject": graph,
            "tasks": tasks,
            "activeTaskList": active_task_list,
        }
        defn = await self._repo.find_define_by_id(inst.defineId)
        if defn:
            data["displayName"] = defn.displayName  # issues/15
            data["name"] = defn.name
            data["version"] = defn.version
        return data

    async def _processInstance_startAndExecute(self, args: dict) -> dict:
        return await self._startAndExecute(args)

    async def _startAndExecute(self, args: dict) -> dict:
        define_id = self._to_int(args.get("processDefineId"))
        if not define_id:
            raise ValueError("processDefineId 缺失或非法")
        operator = self._operator_arg(args)
        flow_args = {k: v for k, v in args.items() if k not in ("processDefineId", "operator")}
        inst = await self._engine.start_process_instance_by_id(define_id, operator, flow_args)
        # issues/56 E28 → issues/127：发起时抄送（f_ccActors）**不在这里**——键随 flow_args 进引擎，
        # 由 engine.handle_cc_actors 在实例行 insert 的同一次调用栈里落 cc 行并逐人 fire CC_CREATE
        # （spec §11.7；本轮把腿从门面搬进引擎，直连引擎 API 的调用方同样生效）。
        # startAndExecute：自动完成申请节点（assignee="applicant" → 发起人）
        doing = await self._repo.find_doing_tasks(inst.id)
        for task in doing:
            await self._repo.add_task_actor(task.id, [operator])
            flow_args["submitType"] = SUBMIT_APPLY
            # 对齐 boot3：f_nextNodeOperator（发起时预指派人）→ tf_nextNodeOperator（引擎执行参数）
            start_next_op = flow_args.get(KEY_PROCESS_START_NEXT_NODE_OPERATOR)
            if start_next_op:
                flow_args[KEY_NEXT_NODE_OPERATOR] = start_next_op
            await self._engine.execute_process_task(task.id, operator, flow_args)
        return {"processInstanceId": inst.id}

    async def _processDefine_deploy(self, args: dict) -> dict:
        return await self._deploy(args)

    async def _processDesign_deploy(self, args: dict) -> dict:
        ext = self._ext_repo()
        design_id = self._to_int(args.get("id"))
        design = await ext.find_design_by_id(design_id)
        if not design:
            raise ValueError("流程设计不存在")
        his_list = await ext.list_design_his(design_id)
        if not his_list:
            raise ValueError("流程设计没有内容，无法发布")
        define_id = await self._deploy({
            "content": his_list[0].content,
            "operator": args.get("operator", "system"),
        })
        design.isDeployed = 1
        design.updateUser = str(args.get("operator", "system"))
        await ext.update_design(design)
        return define_id

    async def _deploy(self, args: dict) -> dict:
        """deploy 版本管理（对齐 boot3）：按 name 查最新定义，存在 version+1 插新记录，否则从 0 起"""
        content = self._content(args)
        flow = self._parse_define_content(content)
        name = flow.get("name", "")
        if not name:
            raise ValueError("流程定义缺少 name")
        version = 0
        latest = await self._repo.find_define_by_name(name)
        if latest:
            version = (latest.version or 0) + 1
        operator = str(args.get("operator", "system"))
        def_ = ProcessDefine(name=name, displayName=flow.get("displayName", ""),
                             type=flow.get("type", "approval"), state=1,
                             content=content, version=version,
                             createUser=operator, updateUser=operator)
        await self._repo.save_define(def_)
        return {"processDefineId": def_.id}

    async def _processDefine_redeploy(self, args: dict) -> dict:
        define_id = self._to_int(args.get("processDefineId"))
        if not define_id:
            raise ValueError("processDefineId 缺失或非法")
        content = self._content(args)
        flow = self._parse_define_content(content)
        def_ = ProcessDefine(id=define_id, name=flow.get("name", ""),
                             displayName=flow.get("displayName", ""),
                             type=flow.get("type", "approval"),
                             content=content,
                             updateUser=str(args.get("operator", "system")))
        await self._repo.update_define(def_)
        return None

    async def _processDefine_remove(self, args: dict) -> dict:
        # issues/95：前端删除统一发 {ids}（此前 Python 唯一没做批量兼容的语言）
        for define_id in self._id_list(args):
            await self._repo.remove_define(define_id)
        return None

    async def _processDefine_upAndDown(self, args: dict) -> dict:
        # issues/54 E26：兼容 {ids, opType} 批量与 {id, state} 单条（对齐 Java issues/28）
        state = self._to_int(args.get("opType") if args.get("opType") is not None else args.get("state"))
        if state is None:
            raise ValueError("opType/state 缺失或非法")
        for define_id in self._id_list(args):
            await self._repo.update_define_state(define_id, state)
        return None

    async def _processInstance_withdraw(self, args: dict) -> dict:
        instance_id = self._to_int(args.get("id"))
        if not instance_id:
            raise ValueError("id 缺失或非法")
        # issues/114：operator 硬必填——严禁缺省回落 "user1" 等固定账号
        # （那会把撤回人静默记成别人，审计链失真且不报错）
        operator = str(args.get("operator") or "").strip()
        if not operator:
            raise ValueError("operator 必填")
        inst = await self._repo.find_instance_by_id(instance_id)
        if not inst:
            raise ValueError("流程实例不存在")
        # 撤回：全部 doing 任务置 WITHDRAW(30) + 实例置 30（v1.0.1：update_instance 级联落库）
        # 注意：find_instance_by_id 现水合 tasks（issues/110），此处仍按实例单独查 doing 任务撤回，
        # 且必须把聚合副本重置为仅被撤回项（见下方 inst.tasks = withdrawn），防级联回写多余任务
        # ——已完成(20)/已终止(40) 的任务行因此不被改写（契约 06 §processInstance/withdraw）
        doing = await self._repo.find_doing_tasks(instance_id)
        if not await self._can_withdraw(inst, operator, doing):
            raise ValueError("无权限撤回该流程实例")
        now = datetime.now()
        # issues/134 案 A：实例状态守卫在聚合根 withdraw 里（非 10 ⇒ 20010009），
        # 故这一句必须排在下面的任务行循环**之前**——被拒时任务行也不该被内存改写，
        # 更不该有机会落库（对齐 Java JeeflowFacade.withdraw 的 canWithdraw → inst.withdraw → updateInstance 序）
        inst.withdraw(now)  # issues/53 E25：撤回状态 Withdraw(30) 而非 Reject(45)
        withdrawn = []
        for t in doing:
            # issues/113：撤回写 WITHDRAW(30)，不用 ABANDONED(99)——99 是引擎废弃码
            # （会签一票否决 / abandon_all_doing 用它），混用会让撤回单与废弃单在任务表里塌成同值
            t.withdraw(now)
            # issues/114：进行中任务的 update_user 回写为真实撤回人（契约同段）
            t.updateUser = operator
            withdrawn.append(t)
        inst.updateUser = operator
        # 级联覆盖防护（issues/57 补正）：撤回副本同步回聚合（update_instance 级联覆盖防护）
        inst.tasks = withdrawn
        for t in withdrawn:
            await self._repo.update_task(t)
        await self._repo.update_instance(inst)
        # TASK_WITHDRAW（码 8）：实例 state 写 30 落库 + 被撤回任务行更新完成后 fire 一次
        # （spec §11.3：每轮撤回只 fire 一次，不逐任务）
        await self._engine.fire_event(ProcessEvent(EventType.TASK_WITHDRAW, instance_id,
                                                   operator=operator, state=int(inst.state)))
        return None

    async def _can_withdraw(self, inst, operator: str, doing: list) -> bool:
        """撤回归属判据（issues/114，命中任一即放行）：

        ① operator = 实例发起人；② operator 是该实例任一**进行中任务**的参与者；
        ③ operator ∈ {flow.auto, flow.admin}。

        ⚠️ 判据 ① **不可复用** ``Engine._is_allowed``：它只判"operator 在不在该任务
        actorIds" + auto/admin 放行，不查实例发起人（contract 06 §processInstance/withdraw）。
        本方法只沿用它的 ②③ 两支口径（KEY_AUTO_ID/KEY_ADMIN_ID 常量 + 子实体 actorIds 判定），
        发起人一支显式补齐。
        """
        op = operator.lower()
        if op == KEY_AUTO_ID or op == KEY_ADMIN_ID:
            return True
        if inst.operator and inst.operator == operator:
            return True
        for t in doing:
            # 参与者以仓储为准（find_task_actors），水合副本 actorIds 兜底
            if operator in (await self._repo.find_task_actors(t.id) or t.actorIds or []):
                return True
        return False

    # ── 流程任务 ─────────────────────────────────────────────────────────────

    async def _processTask_todoList(self, args: dict) -> dict:
        """我的待办分页（operator 作为待办人过滤，v1.5.0 补齐）"""
        page_num = self._to_int(args.get("pageNum")) or 1
        page_size = self._to_int(args.get("pageSize")) or 10
        actor_id = self._operator_arg(args)
        rows, total = await self._repo.page_todo_tasks(page_num, page_size, actor_id, self._parse_m_query(args))
        return self._page_data([self._task_row_to_dict(r) for r in rows], total, page_num, page_size)

    async def _processTask_doneList(self, args: dict) -> dict:
        """我的已办分页（operator 过滤，v1.5.0 补齐）"""
        page_num = self._to_int(args.get("pageNum")) or 1
        page_size = self._to_int(args.get("pageSize")) or 10
        operator = self._operator_arg(args)
        rows, total = await self._repo.page_done_tasks(page_num, page_size, operator, self._parse_m_query(args))
        return self._page_data([self._task_row_to_dict(r) for r in rows], total, page_num, page_size)

    async def _processTask_execute(self, args: dict) -> dict:
        task_id = self._to_int(args.get("processTaskId"))
        if not task_id:
            raise ValueError("processTaskId 缺失或非法")
        operator = self._operator_arg(args)
        submit_type = self._to_int(args.get("submitType")) or SUBMIT_AGREE
        flow_args = {k: v for k, v in args.items() if k not in ("processTaskId", "operator")}
        flow_args["submitType"] = submit_type
        # boot3 execute 分发（spec §11.2）
        if submit_type == SUBMIT_REJECT:
            inst = await self._engine.execute_and_jump_to_end(task_id, operator, flow_args)
        elif submit_type == SUBMIT_ROLLBACK:
            inst = await self._engine.execute_and_jump_task(task_id, operator, flow_args)
        elif submit_type == SUBMIT_JUMP:
            inst = await self._engine.execute_and_jump_task(task_id, operator, flow_args,
                                                             str(args.get("taskName", "")))
        elif submit_type == SUBMIT_ROLLBACK_TO_OPERATOR:
            inst = await self._engine.execute_and_jump_to_first_task_node(task_id, operator, flow_args)
        elif submit_type == SUBMIT_COUNTERSIGN_DISAGREE:
            flow_args["countersignDisagreeFlag"] = 1
            inst = await self._engine.execute_process_task(task_id, operator, flow_args)
        else:  # 0 APPLY / 1 AGREE / 5 重新提交
            inst = await self._engine.execute_process_task(task_id, operator, flow_args)
        # issues/127 办理时抄送（tf_ccActors）**不在这里**——键随 flow_args 进引擎，由
        # engine.handle_cc_actors 在任务更新（update_task）的同一次调用栈里落 cc 行、
        # 落库后逐人 fire CC_CREATE(4)（spec §11.7）。
        # ⚠️ 覆盖面按 §11.7 边界 2 只有 submitType=0/1/5/20 那两条走 ``execute_process_task`` 的腿：
        # 引擎把 cc 做成 ``_prepare_execute_task(on_task_updated=…)`` 钩子、只由该腿注入，
        # 上面 2/3/4/6 四档（reject/rollback/jump/退发起人）**不注入 ⇒ 带 tf_ccActors 也不建 cc、
        # 不发码 4**（与 java/go 基准同形）。门面不需要区分档位，也不在此处补发（§11.1 严禁补发）。
        # 此前本栈在 engine.* **返回之后**、无事务包裹地在门面建 cc 行 ⇒ 直连引擎 API 的调用方
        # 传 tf_ccActors 不建行，且与 go/node/java 三栈形状不一致（本轮搬掉）。
        return None

    # ── 流程设计（需扩展仓储） ───────────────────────────────────────────────

    async def _processDesign_page(self, args: dict) -> dict:
        # issues/50 E22：行转 dict（模型对象直接透传则出口 stringify 不生效，id 为数字）
        ext = self._ext_repo()
        page_num = self._to_int(args.get("pageNum")) or 1
        page_size = self._to_int(args.get("pageSize")) or 10
        rows, total = await ext.page_designs(page_num, page_size,
                                             conditions=self._parse_m_query(args))
        out = []
        for d in rows:
            out.append({"id": d.id, "name": d.name, "displayName": d.displayName, "type": d.type,
                        "icon": d.icon, "isDeployed": d.isDeployed, "remark": d.remark,
                        "createTime": self._fmt_time(d.createTime), "createUser": d.createUser,
                        "updateTime": self._fmt_time(d.updateTime), "updateUser": d.updateUser})
        return self._page_data(out, total, page_num, page_size)

    async def _processDesign_detail(self, args: dict) -> dict:
        ext = self._ext_repo()
        design_id = self._to_int(args.get("id"))
        if not design_id:
            raise ValueError("id 缺失或非法")
        design = await ext.find_design_by_id(design_id)
        if not design:
            raise ValueError("流程设计不存在")
        data = {
            "id": design.id, "name": design.name, "displayName": design.displayName,
            "type": design.type, "icon": design.icon, "isDeployed": design.isDeployed,
            "remark": design.remark,
        }
        his_list = await ext.list_design_his(design_id)
        json_object = None
        if his_list:
            try:
                json_object = json.loads(his_list[0].content)
            except Exception:
                pass
        # issues/07：jsonObject 缺失基本信息时从设计表补齐（对齐 boot3 ProcessDesignServiceImpl.findById）
        if not json_object or not isinstance(json_object, dict):
            json_object = {}
        if "name" not in json_object:
            json_object["name"] = design.name
        if "displayName" not in json_object:
            json_object["displayName"] = design.displayName
        if "type" not in json_object:
            json_object["type"] = design.type
        if "processDesignId" not in json_object:
            json_object["processDesignId"] = design.id
        data["jsonObject"] = json_object
        data["his"] = his_list
        return data

    async def _processDesign_save(self, args: dict) -> dict:
        ext = self._ext_repo()
        operator = self._operator_arg(args)
        design_id = self._to_int(args.get("id"))
        if not design_id:
            design = ProcessDesign(name=str(args.get("name", "")),
                                   displayName=str(args.get("displayName", "")),
                                   type=str(args.get("type", "approval")),
                                   icon=str(args.get("icon", "")),
                                   remark=str(args.get("remark", "")),
                                   isDeployed=0,
                                   createUser=operator, updateUser=operator)
            await ext.save_design(design)
        else:
            design = await ext.find_design_by_id(design_id)
            if not design:
                raise ValueError("流程设计不存在")
            if args.get("displayName") is not None:
                design.displayName = str(args["displayName"])
            if args.get("type") is not None:
                design.type = str(args["type"])
            if args.get("icon") is not None:
                design.icon = str(args["icon"])
            if args.get("remark") is not None:
                design.remark = str(args["remark"])
            design.updateUser = operator
            # 内容快照变更 → 置为未部署（对齐 boot3 updateDefine 语义，issues/08）
            if self._content(args, required=False):
                design.isDeployed = 0
            await ext.update_design(design)
        # 内容快照（设计稿内容存历史表）
        content = self._content(args, required=False)
        if content:
            await ext.save_design_his(ProcessDesignHis(processDesignId=design.id,
                                                       content=content, createUser=operator))
        return {"id": design.id}

    async def _processDesign_update(self, args: dict) -> dict:
        """修改流程设计基本信息（对齐 boot3 ProcessDesignController.update，不写设计稿快照）"""
        ext = self._ext_repo()
        design_id = self._to_int(args.get("id"))
        if not design_id:
            raise ValueError("id 缺失或非法")
        design = await ext.find_design_by_id(design_id)
        if not design:
            raise ValueError("流程设计不存在")
        if args.get("name") is not None:
            design.name = str(args["name"])
        if args.get("displayName") is not None:
            design.displayName = str(args["displayName"])
        if args.get("type") is not None:
            design.type = str(args["type"])
        if args.get("icon") is not None:
            design.icon = str(args["icon"])
        if args.get("remark") is not None:
            design.remark = str(args["remark"])
        design.updateUser = str(args.get("operator", "system"))
        await ext.update_design(design)
        return None

    async def _processDesign_updateDefine(self, args: dict) -> dict:
        """更新流程设计定义（设计稿保存，issues/08）：content 快照入库 + 同步基本信息 + 置未部署"""
        ext = self._ext_repo()
        design_id = self._to_int(args.get("processDesignId"))
        if not design_id:
            raise ValueError("processDesignId 缺失或非法")
        design = await ext.find_design_by_id(design_id)
        if not design:
            raise ValueError("流程设计不存在")
        content = self._content(args, required=False)
        if not content:
            raise ValueError("content 缺失")
        # 与最新一条相同则不重复入库（对齐 boot3 updateDefine）
        his_list = await ext.list_design_his(design_id)
        if not his_list or his_list[0].content != content:
            await ext.save_design_his(ProcessDesignHis(processDesignId=design_id,
                                                       content=content,
                                                       createUser=str(args.get("operator", "system"))))
        # 同步设计基本信息（jsonObject 里的 name/displayName/type）+ 内容变更 → 未部署
        import json as _json
        try:
            flow = _json.loads(content)
            if flow.get("name"):
                design.name = flow["name"]
            if flow.get("displayName"):
                design.displayName = flow["displayName"]
            if flow.get("type"):
                design.type = flow["type"]
        except Exception:
            pass
        design.isDeployed = 0
        design.updateUser = str(args.get("operator", "system"))
        await ext.update_design(design)
        return None

    async def _processDesign_redeploy(self, args: dict) -> dict:
        """重新部署流程定义（issues/08）：替换最新定义内容 + 置已部署（对齐 boot3 redeploy）"""
        ext = self._ext_repo()
        design_id = self._to_int(args.get("id"))
        if not design_id:
            raise ValueError("id 缺失或非法")
        design = await ext.find_design_by_id(design_id)
        if not design:
            raise ValueError("流程设计不存在")
        his_list = await ext.list_design_his(design_id)
        if not his_list:
            raise ValueError("流程设计没有内容，无法发布")
        content = his_list[0].content
        flow = self._parse_define_content(content)
        name = flow.get("name") or ""
        if not name:
            raise ValueError("流程定义缺少 name")
        # 按 name 取最新定义：有则替换内容（version 不变），无则新建（对齐 boot3 redeploy）
        last = await self._repo.find_define_by_name(name)
        if last is None:
            define_id = await self._deploy({"content": content,
                                            "operator": args.get("operator", "system")})
        else:
            last.name = name
            last.displayName = flow.get("displayName", "")
            last.type = flow.get("type", "")
            last.content = content
            last.updateUser = str(args.get("operator", "system"))
            await self._repo.update_define(last)
            define_id = last.id
        design.isDeployed = 1
        design.updateUser = str(args.get("operator", "system"))
        await ext.update_design(design)
        return {"processDefineId": define_id}

    async def _processDesign_remove(self, args: dict) -> dict:
        # issues/28：兼容 {ids} 批量（boot3 前端 IdsParam 惯例）与单 {id}
        ext = self._ext_repo()
        for design_id in self._id_list(args):
            await ext.remove_design(design_id)
        return None

    async def _processDesign_listByType(self, args: dict) -> dict:
        """按类型分组列出流程设计（issue 30，对齐 Java issues/28）——不依赖框架字典：
        设计全量 → 按 type 分组 → 组内每 name 取最新 define 的 {processDefineId, name,
        displayName, icon, remark, jsonObject}。"""
        ext = self._ext_repo()
        page_num = self._to_int(args.get("pageNum")) or 1
        page_size = self._to_int(args.get("pageSize")) or 10000
        rows, _total = await ext.page_designs(page_num, page_size, self._parse_m_query(args))
        # 每 name 最新 define（version 最大）
        def_rows, _ = await self._repo.page_defines(1, 10000, [])
        latest_by_name: dict = {}
        for r in def_rows:
            prev = latest_by_name.get(r.name)
            if prev is None or r.version > prev.version:
                latest_by_name[r.name] = r
        groups: dict = {}
        for d in rows:
            groups.setdefault(d.type or "", []).append({
                "processDesignId": d.id,
                "name": d.name,
                "displayName": d.displayName,
                "icon": getattr(d, "icon", None),
                "remark": getattr(d, "remark", None),
                "processDefineId": latest_by_name[d.name].id if d.name in latest_by_name else None,
                "processDefineState": latest_by_name[d.name].state if d.name in latest_by_name else None,
                "jsonObject": self._parse_graph((await ext.list_design_his(d.id))[0].content)
                              if await ext.list_design_his(d.id) else None,
            })
        return groups

    async def _processInstance_bizData(self, args: dict) -> dict:
        """按流程实例回显业务数据（issue 30，对齐 Java issues/28）——meta_reader 注入式，未注入清晰报错"""
        instance_id = self._to_int(args.get("processInstanceId") or args.get("id"))
        if not instance_id:
            raise ValueError("processInstanceId 缺失")
        inst = await self._repo.find_instance_by_id(instance_id)
        if not inst:
            raise ValueError("流程实例不存在")
        def_ = await self._repo.find_define_by_id(inst.defineId)
        if not def_:
            raise ValueError("流程定义不存在")
        table_name = self._rel_table_name(def_.content)
        if not table_name:
            raise ValueError("流程定义未配置 relTableName")
        if self._meta_reader is None:
            raise ValueError("业务数据读取器未注册（facade.set_meta_reader(MetaTableReader(...))，需引入 jeeflow.meta）")
        return self._meta_reader.read_by_process_instance(table_name, instance_id)

    @staticmethod
    def _rel_table_name(content) -> Optional[str]:
        """从流程定义 content 顶层解析 relTableName（缺省回落 name）"""
        try:
            if isinstance(content, bytes):
                content = content.decode("utf-8")
            meta = json.loads(str(content))
            table = str(meta.get("relTableName") or "").strip()
            if not table:
                table = str(meta.get("name") or "").strip()
            return table or None
        except Exception:
            return None

    # ── 委托代理（需扩展仓储） ───────────────────────────────────────────────

    async def _processSurrogate_page(self, args: dict) -> dict:
        ext = self._ext_repo()
        page_num = self._to_int(args.get("pageNum")) or 1
        page_size = self._to_int(args.get("pageSize")) or 10
        rows, total = await ext.page_surrogates(page_num, page_size,
                                                filters={"operator": str(args["operator"])}
                                                if args.get("operator") else None,
                                                conditions=self._parse_m_query(args))
        return self._page_data([self._surrogate_row_to_dict(s) for s in rows],
                               total, page_num, page_size)

    async def _processSurrogate_save(self, args: dict) -> dict:
        ext = self._ext_repo()
        operator = self._operator_arg(args)
        surrogate_id = self._to_int(args.get("id"))
        if not surrogate_id:
            surrogate = ProcessSurrogate(operator=operator,  # 授权人 = 操作人（新建必有）
                                         createUser=operator, updateUser=operator)
            self._apply_surrogate_fields(surrogate, args, operator)
            await ext.save_surrogate(surrogate)
        else:
            surrogate = await ext.find_surrogate_by_id(surrogate_id)
            if not surrogate:
                raise ValueError("委托记录不存在")
            self._apply_surrogate_fields(surrogate, args, operator)
            await ext.update_surrogate(surrogate)
        return {"id": surrogate.id}

    async def _processSurrogate_update(self, args: dict) -> dict:
        """委托更新（issues/77）：按 id 全字段更新，id 不存在/缺失报错"""
        ext = self._ext_repo()
        surrogate_id = self._to_int(args.get("id"))
        if not surrogate_id:
            raise ValueError("id 缺失或非法")
        surrogate = await ext.find_surrogate_by_id(surrogate_id)
        if not surrogate:
            raise ValueError("委托记录不存在")
        operator = self._operator_arg(args)
        self._apply_surrogate_fields(surrogate, args, operator)
        await ext.update_surrogate(surrogate)
        return {"id": surrogate.id}

    async def _processSurrogate_detail(self, args: dict) -> dict:
        """委托详情（issues/77）：按 id 查单条，返回行结构（时间格式化）"""
        surrogate_id = self._to_int(args.get("id"))
        if not surrogate_id:
            raise ValueError("id 缺失或非法")
        surrogate = await self._ext_repo().find_surrogate_by_id(surrogate_id)
        if not surrogate:
            raise ValueError("委托记录不存在")
        return self._surrogate_row_to_dict(surrogate)

    @staticmethod
    def _apply_surrogate_fields(s, args: dict, operator: str):
        """委托写入公共字段。授权人（operator）仅在显式传入时覆盖，避免 update
        时清空原授权人（前端编辑表单不带 operator；集成层注入时 operator=授权人，覆盖无害）"""
        s.processName = str(args.get("processName", ""))
        if "operator" in args:
            s.operator = str(args.get("operator"))
        s.surrogate = str(args.get("surrogate", ""))
        s.startTime = JeeflowFacade._parse_surrogate_time(args.get("startTime"))
        s.endTime = JeeflowFacade._parse_surrogate_time(args.get("endTime"))
        enabled = args.get("enabled", None)
        if enabled is None:
            s.enabled = 1  # 仅"键缺失"才吃契约默认 1（06 §4.5 save 参数表）；
            # 空串属脏值 → 走下面的 else 落 0（读写两侧口径见 06 §4.5 条款 5）
        else:
            parsed = JeeflowFacade._to_int(enabled)
            # 显式 0 不得被 or 1 吞掉（对齐 Java/Go toIntDef）；
            # 传了但解析不出整数的脏值 → **停用**（05-spi 判据④：脏值不得默认当启用，各栈方向一致）
            s.enabled = parsed if parsed is not None else 0
        s.updateUser = operator

    @staticmethod
    def _parse_surrogate_time(v):
        """解析委托时间入参：兼容 yyyy-MM-dd HH:mm:ss（前端 RangePicker/SPEC 契约）
        与 ISO T（issues/77）；无法解析返回 None"""
        if v is None:
            return None
        if isinstance(v, datetime):
            return v
        s = str(v).strip()
        if not s:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        return None

    def _surrogate_row_to_dict(self, s) -> dict:
        """委托行：时间格式化（issues/77，对齐 Java surrogateRowToMap / SPEC）"""
        return {"id": s.id, "processName": s.processName, "operator": s.operator,
                "surrogate": s.surrogate,
                "startTime": self._fmt_time(s.startTime), "endTime": self._fmt_time(s.endTime),
                "enabled": s.enabled,
                "createTime": self._fmt_time(s.createTime), "createUser": s.createUser,
                "updateTime": self._fmt_time(s.updateTime), "updateUser": s.updateUser}

    async def _processSurrogate_remove(self, args: dict) -> dict:
        # issues/95：前端「我的委托」行内/批量删除统一发 {ids}，与 define/design remove 同惯例
        ext = self._ext_repo()
        for surrogate_id in self._id_list(args):
            await ext.remove_surrogate(surrogate_id)
        return None

    # ── 视图端点（v1.2.0） ──────────────────────────────────────────────────

    async def _processDefine_getLastByName(self, args: dict) -> dict:
        name = str(args.get("processDefineName", ""))
        def_ = await self._repo.find_define_by_name(name)
        if not def_:
            raise ValueError(f"流程定义不存在: {name}")
        return {"id": def_.id, "name": def_.name, "displayName": def_.displayName,
                "type": def_.type, "state": def_.state, "version": def_.version}

    async def _processInstance_highLight(self, args: dict) -> dict:
        instance_id = self._to_int(args.get("id"))
        if not instance_id:
            raise ValueError("id 缺失或非法")
        inst = await self._repo.find_instance_by_id(instance_id)
        if not inst:
            raise ValueError("流程实例不存在")
        active, history, edges = [], [], []
        doing = await self._repo.find_doing_tasks(instance_id)
        for t in doing:
            if t.taskName not in active:
                active.append(t.taskName)
        his = await self._repo.find_history_tasks(instance_id)
        for t in his:
            if t.taskName not in active and t.taskName not in history:
                history.append(t.taskName)
        # 路径补全：start 沿边递归（遇活跃节点停止）
        node_progress = {}
        def_ = await self._repo.find_define_by_id(inst.defineId)
        if def_:
            try:
                flow = json.loads(def_.content)
                node_progress = await self._build_node_progress(flow, his)
                await self._collect_path(flow, "start", "", active, history, edges, set(),
                                         inst.variables, his)
            except Exception:
                pass
        return {"activeNodeNames": active, "historyNodeNames": history,
                "historyEdgeNames": edges, "nodeProgress": node_progress}

    async def _build_node_progress(self, flow: dict, tasks: list) -> dict:
        """节点成员进度（issue 41，对齐 boot3 highLight）：按任务状态 + 会签变量组装。
        会签节点带 type（PARALLEL/SEQUENTIAL）；done 按任务完成状态逐人标记，
        active = 进行中任务首位；动态参与人无静态成员不返回；name 缺省（前端降级显示 id）"""
        from .model import TaskState
        progress = {}
        for name in dict.fromkeys(t.taskName for t in tasks):
            ts = [t for t in tasks if t.taskName == name]
            vars_ = ts[0].variables or {}
            # 完整办理人列表：会签变量 operatorList_{node} 优先（顺序会签全量），否则任务 actorIds 并集
            members = vars_.get(f"operatorList_{name}")
            if not members:
                members = list(dict.fromkeys(a for t in ts for a in (t.actorIds or [])))
            if not members:
                continue  # 动态参与人：无静态成员，不返回
            done_set = {a for t in ts if t.taskState == TaskState.DONE for a in (t.actorIds or [])}
            active_actor = next((t.actorIds[0] for t in ts
                                 if t.taskState == TaskState.DOING and t.actorIds), None)
            # 会签判定：定义节点属性（引擎创建任务时 performType 未落任务表，取模型为准）
            node = next((n for n in flow.get("nodes", []) if n.get("id") == name), None)
            props = (node or {}).get("properties", {}) or {}
            cs_type = props.get("countersignType")
            is_cs = cs_type is not None or str(props.get("performType", "")).strip().upper() in ("1", "ALL", "COUNTERSIGN")
            # 姓名走 UserProvider SPI 解析（issue 43/E15）：asyncio.gather 并行批量，查不到缺省空串
            name_map = {}
            if self._engine.user_prov is not None:
                us = await asyncio.gather(*[self._engine.user_prov.get_user(uid) for uid in members],
                                          return_exceptions=True)
                for uid, u in zip(members, us):
                    if isinstance(u, Exception):
                        continue  # 单用户失败不影响其余
                    if u and u.realName:
                        name_map[uid] = u.realName
            members_out = []
            for uid in members:
                m = {"id": uid, "name": name_map.get(uid, "")}
                if uid in done_set:
                    m["done"] = True
                elif uid == active_actor:
                    m["active"] = True
                members_out.append(m)
            item = {"members": members_out}
            if is_cs and cs_type:
                item["type"] = cs_type
            progress[name] = item
        return progress

    async def _collect_path(self, flow: dict, node_id: str, edge_name: str,
                            active: list, history: list, edges: list, visited: set,
                            vars_: dict, history_tasks: list):
        if node_id in visited:
            return
        visited.add(node_id)
        if edge_name and edge_name not in edges:
            edges.append(edge_name)
        src = self._find_node(flow, node_id)
        for e in flow.get("edges", []):
            if e.get("sourceNodeId") != node_id:
                continue
            # 决策节点：输出边表达式求值过滤（对齐 boot3 recursionModel，issues/06）
            if src and src.get("type") == "snaker:decision":
                expr = (e.get("properties") or {}).get("expr")
                if expr and not await self._eval_decision_expr(flow, src, expr, vars_, history_tasks):
                    continue
            target = self._find_node(flow, e.get("targetNodeId"))
            if not target:
                continue
            tid = target.get("id")
            if tid not in active and tid not in history:
                history.append(tid)
            if tid in active:
                continue
            await self._collect_path(flow, tid, e.get("id"), active, history, edges, visited,
                                     vars_, history_tasks)

    async def _eval_decision_expr(self, flow: dict, decision: dict, expr: str,
                                  vars_: dict, history_tasks: list) -> bool:
        """决策输出边表达式求值（args = 实例变量 + 决策节点前置任务变量）"""
        import asyncio
        args = dict(vars_ or {})
        for e in flow.get("edges", []):
            if e.get("targetNodeId") == decision.get("id"):
                for t in history_tasks or []:
                    if t.taskName == e.get("sourceNodeId") and t.variables:
                        args.update(t.variables)
                    break
                break
        result = await self._engine.eval_expr(expr, args)
        if asyncio.iscoroutine(result):
            result = await result
        return bool(result)

    @staticmethod
    def _find_node(flow: dict, node_id):
        for n in flow.get("nodes", []):
            if n.get("id") == node_id:
                return n
        return None

    async def _processInstance_approvalRecord(self, args: dict) -> dict:
        instance_id = self._to_int(args.get("id"))
        if not instance_id:
            raise ValueError("id 缺失或非法")
        his = await self._repo.find_history_tasks(instance_id)
        return [{
            "taskName": t.taskName, "displayName": t.displayName,
            "taskType": int(t.taskType) if t.taskType is not None else None,
            "performType": int(t.performType) if t.performType is not None else None,
            "taskState": int(t.taskState) if t.taskState is not None else None,
            "operator": t.actorId, "finishTime": self._fmt_time(t.finishTime),
            "ext": t.variables,  # issues/15：前端读 ext.tf_approvalComment；issues/124 variable 原串出口下线
        } for t in his]

    async def _processInstance_getAssigneeTextData(self, args: dict) -> dict:
        instance_id = self._to_int(args.get("id"))
        if not instance_id:
            raise ValueError("id 缺失或非法")
        include_node_name = args.get("includeNodeName") is not False
        rows = []
        doing = await self._repo.find_doing_tasks(instance_id)
        for t in doing:
            actors = await self._repo.find_task_actors(t.id)
            for actor in actors:
                label = actor
                if include_node_name:
                    label = f"{t.displayName}:{actor}"
                rows.append({"label": label, "value": actor})
        return rows

    async def _processInstance_createCCInstance(self, args: dict) -> dict:
        instance_id = self._to_int(args.get("processInstanceId"))
        operator = self._operator_arg(args)
        actor_ids = self._to_actor_ids(args.get("actorIds"))
        if not instance_id or not actor_ids:
            raise ValueError("processInstanceId/actorIds 缺失")
        # 手动 CC 与发起/办理两条腿**同一个漏斗**（spec §11.2 原则 1：码值表达"发生了什么事实"，
        # 不表达"谁触发的"；§11.7 三条路径同判）。门面不再自己 create_cc_instance、不再自己
        # fire CC_CREATE —— 落库 + 逐人 fire 都在 engine.handle_cc_actors 那一处。
        #
        # issues/141 G10「空不创建行」（spec 06 §2.10）：判空**之前**先过归一单点
        # （`spi.normalize_actors`，与引擎腿 parse_cc_actors、任务侧 addCandidate/transfer 同一枚）
        # ——`actorIds=[""]`／`["  "]` 这类"非空但全是空元素"的形态，丢完为空 ⇒ 与上面那条
        # **"空 actorIds"同档**（沿用既有 ``processInstanceId/actorIds 缺失`` 文案，不新造错误码/文案），
        # 不再"报错没报、行也没建"。逗号串与数组两形在这一枚里同判据。
        await self._engine.handle_cc_actors(instance_id, operator, actor_ids)
        return None

    async def _processInstance_updateCCStatus(self, args: dict) -> dict:
        instance_id = self._to_int(args.get("processInstanceId"))
        if not instance_id:
            raise ValueError("processInstanceId 缺失或非法")
        # issues/142 B 批（spec 06 §2.11 写点表第 4 行）：operator **归一后再交给仓储比**——
        # 不 trim 则 " lisi " 判成另一个人（已读打不上）；空值档由仓储写侧那层兜成 no-op，
        # 免得把 state=1 批量打到历史 actor_id='' 的脏行上（issues/129 那族的写侧对偶）。
        # `_operator_arg` 的 demo 缺省（缺失/空串 ⇒ user1）是 issues/129/141 G1 钉过的行为，不动。
        operator = normalize_actor_value(self._operator_arg(args))
        await self._repo.update_cc_status(instance_id, operator)
        return None

    async def _processInstance_ccList(self, args: dict) -> dict:
        """我的抄送分页（v1.3.0）：operator 作为抄送人过滤"""
        page_num = self._to_int(args.get("pageNum")) or 1
        page_size = self._to_int(args.get("pageSize")) or 10
        actor_id = self._operator_arg(args)
        rows, total = await self._repo.page_cc_instances(page_num, page_size, actor_id, self._parse_m_query(args))
        return self._page_data([self._cc_row_to_dict(r) for r in rows], total, page_num, page_size)

    async def _processTask_detail(self, args: dict) -> dict:
        task_id = self._to_int(args.get("id"))
        operator = self._operator_arg(args)
        if not task_id:
            raise ValueError("id 缺失或非法")
        task = await self._repo.find_task_by_id(task_id)
        if not task:
            raise ValueError("任务不存在")
        actors = await self._repo.find_task_actors(task_id)
        # issues/82-5：任务级 ext.isFirstTaskNode（前端 detail.vue 双兜底 record.ext?.isFirstTaskNode）
        # 首个任务节点且 DOING → true，与 instance detail 的 activeTaskList 行语义一致
        t_ext = dict(task.variables or {})
        doing = task.taskState == TaskState.DOING
        # 先留住行上值再覆写出口（"缺键"这个事实一旦丢了就没法回退现算）
        t_row_first = t_ext.get("isFirstTaskNode")
        t_ext["isFirstTaskNode"] = bool(t_row_first) if t_row_first is not None else False
        vo = {
            "id": task.id, "processInstanceId": task.processInstanceId,
            "taskName": task.taskName, "displayName": task.displayName,
            "taskType": int(task.taskType) if task.taskType is not None else None,
            "performType": int(task.performType) if task.performType is not None else None,
            "taskState": int(task.taskState) if task.taskState is not None else None,
            "operator": task.actorId, "formKey": task.formKey,
            "taskActorIdList": actors, "executable": task.is_allowed(operator),
            "ext": t_ext,
            "taskFormData": self._form_data_of(task.variables, "tf_"),  # issues/15 + 对齐 java 顶层出口（issues/124 G4）
        }
        # taskModel：流程定义中对应节点
        inst = await self._repo.find_instance_by_id(task.processInstanceId)
        if inst:
            def_ = await self._repo.find_define_by_id(inst.defineId)
            if def_:
                vo["jsonObject"] = self._parse_graph(def_.content)  # issues/05
                if t_row_first is None:
                    # 存量行没有落库标记 ⇒ 回退现算（仅进行中口径）
                    t_ext["isFirstTaskNode"] = doing and task.taskName == self._first_task_node_id(
                        self._parse_graph(def_.content))
                try:
                    flow = json.loads(def_.content)
                    for n in flow.get("nodes", []):
                        if n.get("id") == task.taskName:
                            props = n.get("properties", {}) or {}
                            # issues/62：taskModel 补 form/ext（节点字段权限，对齐 boot2）
                            vo["taskModel"] = {"name": n.get("id"),
                                               "displayName": (n.get("text") or {}).get("value", ""),
                                               "type": n.get("type"),
                                               "form": props.get("form"),
                                               "ext": props.get("field")}
                            break
                except Exception:
                    pass
        return vo

    async def _processTask_jumpAbleTaskNameList(self, args: dict) -> dict:
        instance_id = self._to_int(args.get("processInstanceId"))
        if not instance_id:
            raise ValueError("processInstanceId 缺失或非法")
        done = await self._repo.find_done_tasks(instance_id)
        rows, seen = [], set()
        for t in done:
            if int(t.performType or 0) == 1:  # COUNTERSIGN
                continue
            if t.taskName not in seen:
                seen.add(t.taskName)
                rows.append({"label": t.displayName, "value": t.taskName})
        return rows

    async def _processTask_candidatePage(self, args: dict) -> dict:
        page_num = self._to_int(args.get("pageNum")) or 1
        page_size = self._to_int(args.get("pageSize")) or 10
        task_id = self._to_int(args.get("processTaskId")) or self._to_int(args.get("id"))
        if not task_id:
            raise ValueError("processTaskId 缺失")
        task = await self._repo.find_task_by_id(task_id)
        if not task:
            raise ValueError("任务不存在")
        inst = await self._repo.find_instance_by_id(task.processInstanceId)
        if not inst:
            raise ValueError("流程实例不存在")
        # 模型候选解析：后继任务节点的 candidateUsers 配置
        candidates = []
        def_ = await self._repo.find_define_by_id(inst.defineId)
        if def_:
            try:
                flow = json.loads(def_.content)
                candidates = await self._next_task_candidates(flow, task.taskName)
            except Exception:
                pass
        if candidates:
            # issues/80：行键对齐前端 UserSelect（valueField='id'）——补 id 键，保留 userId 兼容旧消费方
            rows = [{"id": c, "userId": c, "realName": c} for c in candidates]
            return self._page_data(rows, len(rows), page_num, page_size)
        # 无模型候选 → 用户分页搜索（依赖 user_search 钩子）
        if self._user_search is None:
            raise ValueError("未配置 user_search（用户搜索钩子）")
        result = self._user_search(args)
        if inspect.isawaitable(result):
            result = await result
        rows, total = result
        return self._page_data(rows, total, page_num, page_size)

    async def _next_task_candidates(self, flow: dict, task_name: str) -> list:
        result = []
        visited = set()

        async def collect(node: dict):
            v = (node.get("properties") or {}).get("candidateUsers", "")
            if v:
                for s in str(v).split(","):
                    s = s.strip()
                    if s and s not in result:
                        result.append(s)
            # candidateGroups：按角色取人（v1.6.0，对齐 boot4 GlobalCandidateHandler）
            g = (node.get("properties") or {}).get("candidateGroups", "")
            if g and self._org_prov is not None:
                for rc in str(g).split(","):
                    rc = rc.strip()
                    if not rc:
                        continue
                    ids = await self._org_prov.find_by_role(rc) or []
                    for uid in ids:
                        if uid and uid not in result:
                            result.append(uid)

        async def walk(node_id: str):
            if node_id in visited:
                return
            visited.add(node_id)
            for e in flow.get("edges", []):
                if e.get("sourceNodeId") != node_id:
                    continue
                target = self._find_node(flow, e.get("targetNodeId"))
                if not target:
                    continue
                if target.get("type") in ("snaker:task", "snaker:custom"):
                    await collect(target)
                    continue
                if target.get("type") in ("snaker:fork", "snaker:join", "snaker:decision"):
                    await walk(target.get("id"))

        await walk(task_name)
        return result

    async def _processTask_surrogate(self, args: dict) -> dict:
        return await self._taskAddActor(args)

    async def _processTask_addCandidate(self, args: dict) -> dict:
        return await self._taskAddActor(args)

    async def _taskAddActor(self, args: dict) -> dict:
        """``processTask/addCandidate`` 与 ``processTask/surrogate`` 同体（只追加不清空，issues/115）。

        issues/142 B 批（spec 06 §2.11）：``actorIds`` 走**与 cc 支同一枚**归一单点——逐元素
        trim、空串/纯空白/``None`` 丢弃、同次调用折叠，逗号串与数组**两形同判据**（旧形状数组腿
        ``[str(x) for x in v]`` 不 trim、不丢空、``None`` 串化成字符串 ``"None"`` 直接落进归属列）。
        丢完为空 ⇒ 与"空 actorIds"**同档报错**（既有 ``processTaskId/actorIds 缺失`` 信封，
        不新造码/文案）；``processTaskId`` 属**主键档**，缺失/空串/0 一律响亮报错，不拿 ``''``/``0`` 落库。
        仓储写侧（两仓 ``add_task_actor``）还有一层同样判据的兜底——绕过门面直连仓储的调用方
        同样灌不进空值（要求①「两层都挡」）。
        """
        task_id = self._to_int(args.get("processTaskId"))
        actor_ids = self._to_actor_ids(args.get("actorIds"))
        if not task_id or not actor_ids:
            raise ValueError("processTaskId/actorIds 缺失")
        await self._repo.add_task_actor(task_id, actor_ids)
        return None

    async def _processTask_transfer(self, args: dict) -> dict:
        """转办（issues/115，契约 06 §processTask/transfer）：摘原办理人 + 追加新参与人。

        与 surrogate/addCandidate（**只追加不清空**）是两回事：本 action 摘走 fromActor 在
        该任务的**那一行**参与者（会签节点转的是"自己那一票"，其余成员不受影响），
        待办从 A 的列表挪到 B 的列表；任务不新建（沿用同一 processTaskId），
        节点进度与高亮图不变。留痕三件（契约 06 §4「缺一不可」）：任务行 submitType=7 槽位 +
        追加式账本 tf_transferHistory + 末跳可读文案 tf_approvalComment。
        """
        task_id = self._to_int(args.get("processTaskId"))
        if not task_id:
            raise ValueError("processTaskId 缺失或非法")
        # issues/142 B 批（spec 06 §2.11 写点表第 2 行）：from/to/operator **归一后再用**，判据仍然
        # 只有 `spi.normalize_actor_value` 那一枚（内部即 §2.10 落地的 normalize_actors 单点）。
        # 旧形状 `str(args.get("fromActor") or "")` 用的是**语言自带假值判据**——int `0` 被折成
        # "没填"而报 `toActor 必填`，而 "0" 恰是要求④点名的反向哨兵（"看起来像空"的正常 id）。
        # trim 必须同时覆盖三处：① 权限比较（operator 与 fromActor 是同一个人）
        # ② 摘人/加人写进 wf_process_task_actor 的值 ③ 留痕（tf_transferTo／文案／tf_transferHistory）。
        from_actor = normalize_actor_value(args.get("fromActor"))
        to_actor = normalize_actor_value(args.get("toActor"))
        if from_actor == "":
            raise ValueError("fromActor 必填")
        if to_actor == "":
            raise ValueError("toActor 必填")
        operator = normalize_actor_value(args.get("operator"))
        if operator == "":
            raise ValueError("operator 必填")
        task = await self._repo.find_task_by_id(task_id)
        if not task:
            raise ValueError(f"task not found: {task_id}")
        # 归属：只能转自己那一条待办，系统代执行/超级管理员除外（对齐撤回口径）
        op = operator.lower()
        if operator != from_actor and op != KEY_AUTO_ID and op != KEY_ADMIN_ID:
            raise ValueError("无权限转办该任务")
        if task.taskState != TaskState.DOING:
            raise ValueError("任务非进行中，不可转办")
        actors = await self._repo.find_task_actors(task_id) or list(task.actorIds or [])
        if from_actor not in actors:
            raise ValueError("原办理人不是该任务参与人")
        if to_actor in actors:
            raise ValueError("目标人已是该任务参与人")
        # 摘原人 + 加新人：走仓储既有 remove/add（add 为去重追加，issues/03 语义不动）
        await self._repo.remove_task_actor(task_id, [from_actor])
        await self._repo.add_task_actor(task_id, [to_actor])
        # 留痕：任务不新建，审批记录即本任务行 → submitType=7 + tf_ 变量（契约 §4 之①槽位）
        reason = str(args.get("reason") or "").strip()
        now = datetime.now()
        vars_ = dict(task.variables or {})
        vars_[KEY_SUBMIT_TYPE] = SUBMIT_TRANSFER
        vars_["tf_transferTo"] = to_actor
        vars_["tf_transferReason"] = reason
        # 展示文案走本栈既有方式：approvalRecord 的 ext 即任务变量，前端读 ext.tf_approvalComment
        vars_["tf_approvalComment"] = (f"{from_actor} 转办给 {to_actor}（{reason}）" if reason
                                       else f"{from_actor} 转办给 {to_actor}")
        # 契约 §4 之②：追加式账本 tf_transferHistory，每跳 append 一条、只追加不覆盖。
        # 动机：审批记录的槽位就是任务行本身，B 办结时 submitType 被自己的办理参数覆盖——没有追加式账本，
        # 多跳转办只剩末跳、办结后转办事实整体消失（键名 camelCase 属跨栈契约键，勿改 snake_case）。
        history = vars_.get("tf_transferHistory")
        vars_["tf_transferHistory"] = [
            *(history if isinstance(history, list) else []),
            {"submitType": SUBMIT_TRANSFER, "fromActor": from_actor, "toActor": to_actor,
             "reason": reason, "time": self._fmt_time(now), "operator": operator}]
        task.variables = vars_
        # 契约 06 §transfer 留痕⚠️：**严禁覆写 actor_id 列**——进行中任务该列恒无值是既有不变量；
        # 写进被摘走的人，该单撤回/终止后（离开 DOING 但列值留着）会凭空出现在他从没办过的
        # 「我已办」列表（page_done_tasks 按 state <> DOING AND operator = ? 过滤）。
        # "办理人记谁"由 updateUser + tf_transferHistory[].operator 承载。
        # 同步聚合副本：内存仓 update_task 会按 actorIds 覆写参与者表，须与 remove/add 结果一致
        task.actorIds = [*(a for a in actors if a != from_actor), to_actor]
        task.updateTime = now
        task.updateUser = operator
        await self._repo.update_task(task)
        # TASK_TRANSFER（码 7）：任务参与者被替换并落库之后 fire（spec §11.3 触发时机列），
        # 载荷带 fromActor/toActor/operator —— 监听器免反查任务行即知"谁转给了谁"
        await self._engine.fire_event(ProcessEvent(EventType.TASK_TRANSFER,
                                                   task.processInstanceId, task.id, task.taskName,
                                                   operator, fromActor=from_actor, toActor=to_actor))
        return None

    async def _processTask_removeTaskActor(self, args: dict) -> dict:
        """摘除参与人（issues/115 残留 · 门面第 **47** 个 action，spec 06-facade.md
        §processTask/removeTaskActor）。SPI 侧 ``remove_task_actor`` 早就是必选方法、两仓都实现，
        本 action 补的只是"上门面"那一段（Java 基准 ``taskRemoveActor`` 同批）。

        三个兄弟 action 的分工先钉死，免得后来人把三条混用：
        ``_taskAddActor``（surrogate/addCandidate 同体）＝**只加**；``_processTask_transfer``＝
        **换人**（摘 A 并加 B，写 submitType=7 + tf_transferHistory 留痕 + fire 码 7）；
        本 action＝**只摘不加、零留痕**：删掉 ``actorIds`` 在本任务的参与者行，不新建任务、
        不写任何任务变量、不覆写任务 ``actorId``/``updateUser``/``updateTime``，也**不 fire 事件**——
        issues/132 §11.3 定稿的事件集里没有"摘人"这一码，码 7 ``TASK_TRANSFER`` 的语义是
        "参与者被替换"，只摘不加却发码 7 等于把没发生的转办写进事件流（要立法先开 issue）。

        守卫次序（spec 同节末尾钉死，逐栈一致，门禁按 msg 断言，不接受本栈自行重排）：
        ``operator 必填`` → ``processTaskId/actorIds 缺失`` → ``任务不存在`` → ``无权限摘除该任务参与人``
        → ``任务非进行中，不可摘除参与人`` → ``至少需保留一名参与人`` → 落库。
        ``operator`` 排在最前：参数全缺时若先报缺参数，鉴权缺口会被参数报错藏起来（严禁回落 user1）。

        ⚠️ ``任务不存在`` 用的是 spec 钉的逐字中文文案，**不沿用** ``_processTask_transfer`` 里那句
        历史形状 ``f"task not found: {task_id}"``——spec 同节已注明那是既有分叉、本轮不回改 transfer。
        """
        operator = normalize_actor_value(args.get("operator"))
        if operator == "":
            raise ValueError("operator 必填")
        # 缺参数档与兄弟 action ``_taskAddActor`` 复用同一枚判据、同一条逐字文案（spec 语义 8
        # 「同族同文案，不另造」）：``processTaskId`` 属主键档（缺失/空串/0 一律响亮报错，§2.11 末段），
        # ``actorIds`` 两形（数组/逗号串）过同一枚归一单点，归一后丢完为空 ⇒ 同一条文案。
        # 两条都不落库，空串元素也绝不会被喂进 DELETE ⇒ 历史 ``actor_id=''`` 脏行天然安全。
        task_id = self._to_int(args.get("processTaskId"))
        actor_ids = self._to_actor_ids(args.get("actorIds"))
        # 缺参数档收齐五种形状（spec 语义 8）：缺键 / 空串 / 纯空白 / 0 / 负数。
        # `not task_id` 管前者三者与 0，负数另判——`None < 0` 会 TypeError，靠 or 短路挡住，
        # 所以两个条件的顺序不能颠倒。拿 0 或负数当 id 去查/去落库，与没传 id 是同一种
        # 调用方错误，不得改口成「任务不存在」。
        if not task_id or task_id < 0 or not actor_ids:
            raise ValueError("processTaskId/actorIds 缺失")
        task = await self._repo.find_task_by_id(task_id)
        if not task:
            raise ValueError("任务不存在")
        # 归属判据同 transfer：被摘集合必须含操作人本人（入参已归一，比较才咬得上），或 operator 是
        # ``flow.auto``/``flow.admin`` 哨兵（大小写不敏感沿用本栈既有 ``operator.lower() == KEY_*`` 写法）。
        # transfer 能"摘 A 加 B"是因为 A 就是操作人本人；本 action 不得成为借道摘他人的口子。
        targets = set(actor_ids)
        op = operator.lower()
        if operator not in targets and op != KEY_AUTO_ID and op != KEY_ADMIN_ID:
            raise ValueError("无权限摘除该任务参与人")
        # 前置态：仅进行中（DOING=10）任务可摘人。已办结/废弃/撤回的历史参与人行是 approvalRecord 的
        # 取证依据（它读全状态任务行），摘它等于改写审批历史。
        if task.taskState != TaskState.DOING:
            raise ValueError("任务非进行中，不可摘除参与人")
        actors = await self._repo.find_task_actors(task_id) or list(task.actorIds or [])
        # 语义 6「匹配取归一值、DELETE 取行上的原值」（§2.11 硬要求②的**删除腿**）：库里的行可能是
        # 修复前落下的未 trim 原值 ``" leader "``，入参 ``"leader"`` 必须判成同一个人**并真删掉它**——
        # 所以匹配用归一形、喂给仓储的是那一行的**原值**。只拿归一值去 DELETE 会"判成同一人却一条没删"，
        # 门面报成功而被摘的人待办还在，是**假成功**。
        #
        # 语义 5「不得摘空」的下限按**能办单的人数**算（``remaining`` 只数归一后非空的行）：历史
        # ``actor_id=''``/纯空白脏行谁也办不了单，拿它撑住下限等于让"摘空"伪装成成功。
        # 判据是**集合差**（当前参与者 − 归一后入参），不是"入参条数"——否则 ``actorIds`` 里混进
        # 非参与者的 id 就能绕过这条下限。
        to_delete: list[str] = []
        remaining = 0
        for row in actors:
            normalized = normalize_actor_value(row)
            if normalized == "":
                continue  # 归一后为空的历史脏行：既不匹配任何入参，也不计入"一个人"
            if normalized in targets:
                to_delete.append(row)
            else:
                remaining += 1
        if to_delete and remaining == 0:
            raise ValueError("至少需保留一名参与人")
        # 语义 7「幂等」：一个都没命中 ⇒ 空操作、成功信封（前端双点/集成层重放第二次不再报错）。
        # 需要"人不在任务里就报错"请用 transfer（它有「原办理人不是该任务参与人」那档判据）。
        if to_delete:
            await self._repo.remove_task_actor(task_id, to_delete)
        return None

    async def _processTask_latest(self, args: dict) -> dict:
        instance_id = self._to_int(args.get("processInstanceId"))
        if not instance_id:
            raise ValueError("processInstanceId 缺失或非法")
        doing = await self._repo.find_doing_tasks(instance_id)
        if not doing:
            return None
        t = doing[0]
        return {"id": t.id, "taskName": t.taskName, "displayName": t.displayName,
                "taskState": int(t.taskState) if t.taskState is not None else None,
                "operator": t.actorId}

    def _operator_arg(self, args: dict) -> str:
        """归属/操作人入参归一化（issues/129 案 A · spec 06-facade.md:100 补句）。

        空串与**缺键同档**：传 ""（或全空白）视同未传，一并回落 demo 缺省 user1。
        修前是 `operator = str(args.get(… , "user1"))`——`get` 的缺省只在**键不存在**时生效，
        显式空串原样穿过落进归属谓词；内存仓储的 `if operator and ...` 又把空串折成
        "这次不过滤" ⇒ 我的列表读出全库（160 python demo 实测空串档 25 行 vs user1 档 4 行）。
        门面归一化是第一层，仓储的归属兜底是第二层（memory.py / repository/base.py），两层都要在。
        """
        raw = args.get("operator", None)
        if raw is None:
            return "user1"
        s = str(raw)
        return s if s.strip() else "user1"

    @staticmethod
    def _to_actor_ids(v) -> list:
        """归属值入参 → ``list[str]``（**门面各条腿共用**：addCandidate/surrogate、手动 createCCInstance）。

        issues/142 B 批（spec 06 §2.11）：判据**不在这里**，单点只有 ``spi.normalize_actors``
        （§2.10 已落地的那一枚，cc 支继续走同一个对象）——本函数是薄转发，**严禁再抄第二份**。
        旧形状 ``[str(x) for x in v]`` 数组腿不 trim、不丢空、``None`` 串化成字符串 ``"None"``，
        与逗号串腿（strip＋过滤）是两把尺子；改后两形同判据，数字元素 ``str()`` 后仍 trim，
        ``"0"`` 这类"看起来像空"的正常 id 照样保住（判空只用 ``strip() == ""``）。
        """
        return normalize_actors(v)

    # 旧私有名保留为别名（既有注释/集成壳引用过它）——指向**同一枚**判据，不是第二份。
    _to_str_list = _to_actor_ids

    # ── 工具 ─────────────────────────────────────────────────────────────────

    def _ext_repo(self) -> ProcessExtRepository:
        if self._ext is None:
            raise ValueError("未配置 ProcessExtRepository（扩展仓储）")
        return self._ext

    @staticmethod
    def _content(args: dict, required: bool = True) -> Optional[str]:
        content = args.get("content")
        if content is None:
            # issues/31：兼容 boot3 顶层 JSON（无 content 字段）——非保留字段序列化为内容快照
            copy = {k: v for k, v in args.items() if k not in ("processDesignId", "operator")}
            if not copy:
                if required:
                    raise ValueError("content 缺失")
                return None
            content = json.dumps(copy, ensure_ascii=False)
        if isinstance(content, (dict, list)):
            # content 为对象（前端直接传 JSON 对象）：序列化为 JSON 字符串
            return json.dumps(content, ensure_ascii=False)
        if isinstance(content, bytes):
            return content.decode("utf-8")
        return str(content)

    @staticmethod
    def _parse_define_content(content):
        """流程定义 content → JSON（issues/139：Java ModelParser.parse 的同位单一解析点）。

        解析失败对外只出逐字固定文案 ``MSG_READ_DEFINE_JSON_FAIL``，原始异常作 ``__cause__``
        留在错误对象上（``raise ... from e``）——门面顶层 ``flow()`` 出 msg 时只透**引擎自己写的**
        文案（issues/137 §3-1 判别式，见 ``is_foreign_detail``），解析器文本（异常类名/位置/
        内容片段）属内部信息，拼进 message 就是把它透给前端。

        本腿即 spec §2.12「覆盖面每栈至少两处」的第 ② 处（bizData／JSON 解析族）：
        ① 门面顶层 catch（``flow()`` 的 ``except Exception``）；② 这里。
        判据两侧都有牙：``ValueError(MSG_READ_DEFINE_JSON_FAIL)`` 抛出点在 ``jeeflow/facade.py``
        ⇒ 第 5 条判"引擎写的"、``ValueError`` 不在第 4 条类型族里 ⇒ **契约文案照旧逐字透出**；
        而 ``__cause__`` 上那个 ``json.JSONDecodeError`` 若被搬进 message，第 3 条（裸包装）
        与第 4 条（``json.JSONDecodeError`` 在族里）会双双把它挡成固定文案。
        """
        try:
            return json.loads(content)
        except Exception as e:
            raise ValueError(MSG_READ_DEFINE_JSON_FAIL) from e

    @staticmethod
    def _to_int(v) -> Optional[int]:
        if v is None:
            return None
        # issues/82 负向（对齐 Go TestSnowflakeIDPrecision / Node toId / Java toLong / issues/38 E9）：
        # 浮点型 id 超 2^53 说明精度已丢（json 解析 / 调用方 float 产物），必须显性报错，
        # 不能 int() 静默截断成错误 id。Python int 本任意精度不受限，仅 float 会丢精度。
        if isinstance(v, float) and abs(v) > 2 ** 53:
            raise ValueError(f"id {v} 超出 float64 精确范围（2^53），请以字符串传递")
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    def _id_list(self, args: dict) -> list:
        """删除/启停类 action 的批量主键：mldong IdsParam 惯例下 {ids} 数组优先，兼容单
        {id}；两者皆缺失、空数组或含非法值一律报错（issues/95，对齐 Java idListArgs）。"""
        ids = args.get("ids")
        if isinstance(ids, (list, tuple)):
            out = []
            for v in ids:
                i = self._to_int(v)
                if not i:
                    raise ValueError("id 缺失或非法")
                out.append(i)
            if not out:
                raise ValueError("id 缺失或非法")
            return out
        single = self._to_int(args.get("id"))
        if not single:
            raise ValueError("id 缺失或非法")
        return [single]

    def _parse_m_query(self, args: dict) -> list:
        """m_ 前缀查询参数解析（issues/05-5，对齐 Java JeeflowQueryParser）：
        m_EQ_taskName → t.task_name EQ；m_pd_LIKE_displayName → pd.display_name LIKE"""
        out = []
        for key, value in args.items():
            if not key.startswith("m_") or value is None or value == "":
                continue
            parts = key[2:].split("_")
            if len(parts) < 2:
                continue
            if len(parts) == 2:
                # 无别名 → 默认主表别名 t（对齐 Java，白名单列均带表别名）
                operator, column = parts[0], "t." + self._to_underscore(parts[1])
            else:
                operator, column = parts[1], parts[0] + "." + self._to_underscore(parts[2])
            out.append(QueryCondition(column=column, operator=operator.upper(), value=value))
        return out

    @staticmethod
    def _to_underscore(camel: str) -> str:
        out = []
        for c in camel:
            if c.isupper():
                out.append("_" + c.lower())
            else:
                out.append(c)
        return "".join(out)

    @staticmethod
    def _page_data(rows, total: int, page_num: int = 1, page_size: int = 10) -> dict:
        # issues/64：对齐 mldong 分页五键（Java pageResult / Go pageData）
        page_num = page_num or 1
        page_size = page_size or 10
        total_page = 0
        if total > 0 and page_size > 0:
            total_page = (total + page_size - 1) // page_size
        return {
            "pageNum": page_num,
            "pageSize": page_size,
            "recordCount": total,
            "totalPage": total_page,
            "rows": rows,
        }

    @staticmethod
    def _ok(data) -> dict:
        return {"code": 0, "msg": "成功", "data": data}

    @staticmethod
    def _error(msg: str) -> dict:
        return {"code": 99999999, "msg": msg}

    @staticmethod
    def _task_vo(t) -> dict:
        """任务 VO（instanceDetail 任务列表用，对齐 Java taskVo）"""
        return {
            "id": t.id, "processInstanceId": t.processInstanceId, "taskName": t.taskName,
            "displayName": t.displayName, "taskType": t.taskType, "performType": t.performType,
            "taskState": t.taskState, "operator": t.actorId, "finishTime": t.finishTime,
            "expireTime": t.expireTime, "formKey": t.formKey, "taskParentId": t.parentTaskId,
            "createTime": t.createTime, "createUser": t.createUser,
            "updateTime": t.updateTime, "updateUser": t.updateUser, "taskActorIdList": t.actorIds,
            "taskFormData": JeeflowFacade._form_data_of(t.variables, "tf_"),  # issues/15（_task_vo 无 self，走类名调用）
        }

    @staticmethod
    def _parse_graph(content) -> Optional[dict]:
        """定义 content 解析为 LogicFlow JSON（issues/05 jsonObject）"""
        import json as _json
        if not content:
            return None
        try:
            obj = _json.loads(content) if isinstance(content, str) else content
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None

    async def _instance_json_object(self, inst) -> Optional[dict]:
        """实例关联定义的 jsonObject"""
        def_ = await self._repo.find_define_by_id(inst.defineId)
        return self._parse_graph(def_.content) if def_ else None

    @staticmethod
    def _first_task_node_id(graph: Optional[dict]) -> Optional[str]:
        """流程 JSON 中第一个任务节点 id（issues/05-4 isFirstTaskNode 用）"""
        for n in (graph or {}).get("nodes", []):
            if isinstance(n, dict) and n.get("type") == "snaker:task":
                return n.get("id")
        return None

    # ── 统计（v1.8.25，issues/103） ──────────────────────────────────────────

    _DEFAULT_STATE_IN = [10, 20, 30, 40, 45, 50]
    _DEFAULT_STATS_LIMIT = 10
    _VALID_GRANULARITY = {"hour", "day", "week", "month"}
    _VALID_DIMENSION = {"state", "define", "category", "approver", "applicant",
                        "node", "stuckNode", "stuckApprover", "durationBucket"}

    async def _processInstance_stats_overview(self, args: dict) -> dict:
        start = self._parse_surrogate_time(args.get("start"))
        end = self._parse_surrogate_time(args.get("end"))
        state_in = args.get("stateIn") or self._DEFAULT_STATE_IN

        insts = await self._repo.query_instances_for_stats(state_in, "create_time", start, end)
        total = len(insts)
        in_progress = sum(1 for r in insts if r.state == 10)
        completed = sum(1 for r in insts if r.state == 20)
        withdrawn = sum(1 for r in insts if r.state == 30)
        rejected = sum(1 for r in insts if r.state == 45)
        suspended = sum(1 for r in insts if r.state == 50)

        now = datetime.now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        today_end = today_start + timedelta(days=1)
        # E：todayNew 恒按服务器当日、不过滤 state / 不受 stateIn 影响（对齐内置线 countTodayNew）
        today_insts = await self._repo.query_instances_for_stats(
            None, "create_time", today_start, today_end)
        today_new = len(today_insts)

        pending, overdue = await self._repo.stats_pending_and_overdue_count()
        avg_dur = await self._repo.stats_avg_completed_duration_seconds(start, end)

        cs_total, cs_count, on_time, on_time_denom = await self._repo.stats_completed_task_aggregate()
        countersign_rate = _stats_round4(cs_count / cs_total) if cs_total > 0 else 0.0
        on_time_rate = _stats_round4(on_time / on_time_denom) if on_time_denom > 0 else 0.0
        reject_rate = _stats_round4(rejected / max(1, completed + rejected))

        return {
            "total": total, "inProgress": in_progress, "completed": completed,
            "rejected": rejected, "withdrawn": withdrawn, "suspended": suspended,
            "todayNew": today_new, "avgDurationSeconds": avg_dur,
            "rejectRate": reject_rate, "pendingTaskCount": pending,
            "overdueTaskCount": overdue, "countersignRate": countersign_rate,
            "onTimeRate": on_time_rate,
        }

    async def _processInstance_stats_trend(self, args: dict) -> dict:
        granularity = str(args.get("granularity", ""))
        if granularity not in self._VALID_GRANULARITY:
            raise ValueError(f"不支持的 granularity: {granularity}")
        start = self._parse_surrogate_time(args.get("start"))
        end = self._parse_surrogate_time(args.get("end"))
        # C：start/end 必填（对齐内置线 20010012 缺参语义），不静默回退不限时间
        if start is None or end is None:
            raise ValueError("trend 缺少必填参数：start/end/granularity")

        # 实例侧无 state 过滤（对齐内置线 countInstanceStartedByBucket）
        insts = await self._repo.query_instances_for_stats(None, "create_time", start, end)
        done_tasks = await self._repo.query_tasks_for_stats(int(TaskState.DONE), start, end)

        buckets = _stats_enumerate_buckets(start, end, granularity)
        started_map: dict[str, int] = {}
        for r in insts:
            ct = self._parse_surrogate_time(r.createTime)
            if ct:
                bk = _stats_bucket_key(ct, granularity)
                started_map[bk] = started_map.get(bk, 0) + 1
        finished_map: dict[str, int] = {}
        for r in done_tasks:
            ft = self._parse_surrogate_time(r.finishTime)
            if ft:
                bk = _stats_bucket_key(ft, granularity)
                finished_map[bk] = finished_map.get(bk, 0) + 1

        series = []
        for b in buckets:
            series.append({"bucket": b, "started": started_map.get(b, 0),
                           "finished": finished_map.get(b, 0)})
        # A：data 本体为裸数组（去掉 {granularity, series} 包装，对齐契约 spec 06 §4.2 / 内置线）
        return series

    async def _processInstance_stats_group(self, args: dict) -> dict:
        dimension = str(args.get("dimension", ""))
        if dimension not in self._VALID_DIMENSION:
            raise ValueError(f"不支持的 dimension: {dimension}")
        start = self._parse_surrogate_time(args.get("start"))
        end = self._parse_surrogate_time(args.get("end"))
        limit = self._to_int(args.get("limit")) or self._DEFAULT_STATS_LIMIT

        if dimension == "define":
            raw = await self._repo.stats_define_group(start, end, limit)
            rows = [{"key": r["key"], "label": r.get("label"), "count": r["count"],
                     "avgDurationSeconds": r.get("avgDurationSeconds")} for r in raw]

        elif dimension == "state":
            insts = await self._repo.query_instances_for_stats(None, "create_time", start, end)  # 无 state 过滤（对齐内置线：仅 overview 用 stateIn）
            grouped: dict[str, int] = {}
            for r in insts:
                k = str(r.state)
                grouped[k] = grouped.get(k, 0) + 1
            entries = sorted(grouped.items(), key=lambda x: x[1], reverse=True)[:limit]
            rows = [{"key": k, "label": None, "count": c, "avgDurationSeconds": None} for k, c in entries]

        elif dimension == "category":
            insts = await self._repo.query_instances_for_stats(None, "create_time", start, end)  # 无 state 过滤（对齐内置线：仅 overview 用 stateIn）
            define_types: dict[int, str] = {}
            for r in insts:
                if r.defineId not in define_types:
                    defn = await self._repo.find_define_by_id(r.defineId)
                    define_types[r.defineId] = defn.type if defn else ""
            grouped2: dict[str, int] = {}
            for r in insts:
                tp = define_types.get(r.defineId, "")
                grouped2[tp] = grouped2.get(tp, 0) + 1
            entries2 = sorted(grouped2.items(), key=lambda x: x[1], reverse=True)[:limit]
            rows = [{"key": k, "label": None, "count": c, "avgDurationSeconds": None} for k, c in entries2]

        elif dimension == "approver":
            tasks = await self._repo.query_tasks_for_stats(int(TaskState.DONE), start, end)
            grouped3: dict[str, int] = {}
            for r in tasks:
                if not r.operator:
                    continue
                grouped3[r.operator] = grouped3.get(r.operator, 0) + 1
            entries3 = sorted(grouped3.items(), key=lambda x: x[1], reverse=True)[:limit]
            rows = [{"key": k, "label": None, "count": c, "avgDurationSeconds": None} for k, c in entries3]

        elif dimension == "applicant":
            insts = await self._repo.query_instances_for_stats(None, "create_time", start, end)  # 无 state 过滤（对齐内置线：仅 overview 用 stateIn）
            grouped4: dict[str, int] = {}
            for r in insts:
                if not r.operator:
                    continue
                grouped4[r.operator] = grouped4.get(r.operator, 0) + 1
            entries4 = sorted(grouped4.items(), key=lambda x: x[1], reverse=True)[:limit]
            rows = [{"key": k, "label": None, "count": c, "avgDurationSeconds": None} for k, c in entries4]

        elif dimension == "node":
            tasks = await self._repo.query_tasks_for_stats(int(TaskState.DONE), start, end)
            node_agg: dict[str, dict] = {}
            for r in tasks:
                if not r.displayName:
                    continue
                dur = 0
                ft = self._parse_surrogate_time(r.finishTime)
                ct = self._parse_surrogate_time(r.createTime)
                if ft and ct:
                    dur = int((ft - ct).total_seconds())
                agg = node_agg.get(r.displayName)
                if agg is None:
                    agg = {"count": 0, "totalDur": 0}
                    node_agg[r.displayName] = agg
                agg["count"] += 1
                agg["totalDur"] += dur
            entries5 = sorted(node_agg.items(), key=lambda x: x[1]["count"], reverse=True)[:limit]
            rows = []
            for name, agg in entries5:
                avg = int(round(agg["totalDur"] / agg["count"])) if agg["count"] > 0 else None
                rows.append({"key": name, "label": None, "count": agg["count"],
                             "avgDurationSeconds": avg})

        elif dimension == "stuckNode":
            raw5 = await self._repo.stats_stuck_node_group(limit)
            rows = [{"key": r["key"], "label": r.get("label"), "count": r["count"],
                     "avgDurationSeconds": r.get("avgDurationSeconds")} for r in raw5]

        elif dimension == "stuckApprover":
            raw6 = await self._repo.stats_stuck_approver_group(limit)
            rows = [{"key": r["key"], "label": r.get("label"), "count": r["count"],
                     "avgDurationSeconds": r.get("avgDurationSeconds")} for r in raw6]

        elif dimension == "durationBucket":
            durations = await self._repo.stats_completed_instance_durations(start, end)
            same_day = d1to3 = d3to7 = over7d = 0
            for dur in durations:
                if dur < 86400:
                    same_day += 1
                elif dur < 259200:
                    d1to3 += 1
                elif dur < 604800:
                    d3to7 += 1
                else:
                    over7d += 1
            keys = ["sameDay", "1to3d", "3to7d", "over7d"]
            counts = [same_day, d1to3, d3to7, over7d]
            rows = [{"key": keys[i], "label": None, "count": counts[i],
                     "avgDurationSeconds": None} for i in range(4)]
        else:
            rows = []

        # A：data 本体为裸数组（去掉 {dimension, rows} 包装，对齐契约 spec 06 §4.2 / 内置线）
        return rows

    # ═══ 行输出转换（issues/05-2 字段契约 + 05-3 时间格式）═══

    @staticmethod
    def _form_data_of(vars_: dict, prefix: str) -> dict:
        """issues/15：取 vars 中 prefix 前缀字段，输出「带前缀 + 去前缀副本」（对齐 boot3 getFormData）"""
        out = {}
        for k, v in (vars_ or {}).items():
            if k and k.startswith(prefix):
                out[k] = v
                out[k[len(prefix):]] = v
        return out

    @staticmethod
    def _fmt_time(t) -> Optional[str]:
        """时间格式化 yyyy-MM-dd HH:mm:ss"""
        if t is None:
            return None
        if isinstance(t, str):
            return t.replace("T", " ")[:19]
        return t.strftime("%Y-%m-%d %H:%M:%S")

    def _define_row_to_dict(self, r) -> dict:
        return {"id": r.id, "name": r.name, "displayName": r.displayName, "type": r.type,
                "state": r.state, "version": r.version,
                "createTime": self._fmt_time(r.createTime), "createUser": r.createUser,
                "updateTime": self._fmt_time(r.updateTime), "updateUser": r.updateUser}

    def _instance_row_to_dict(self, r) -> dict:
        return {"id": r.id, "parentId": r.parentId, "processDefineId": r.defineId,
                "state": int(r.state) if r.state is not None else None,
                "parentNodeName": r.parentNodeName, "businessNo": r.businessNo, "operator": r.operator,
                "expireTime": self._fmt_time(r.expireTime),
                "createTime": self._fmt_time(r.createTime), "createUser": r.createUser,
                "updateTime": self._fmt_time(r.updateTime), "updateUser": r.updateUser,
                "processDefineName": r.defineName, "processDefineDisplayName": r.defineDisplayName,
                "processDefineVersion": r.defineVersion,
                "ext": r.variables, "displayName": r.defineDisplayName, "version": r.defineVersion}

    def _cc_row_to_dict(self, r) -> dict:
        return self._instance_row_to_dict(r) if hasattr(r, "defineName") else {
            "id": r.id, "parentId": r.parentId, "processDefineId": r.defineId,
            "state": int(r.state) if r.state is not None else None,
            "parentNodeName": r.parentNodeName, "businessNo": r.businessNo, "operator": r.operator,
            "expireTime": self._fmt_time(r.expireTime),
            "createTime": self._fmt_time(r.createTime), "createUser": r.createUser,
            "updateTime": self._fmt_time(r.updateTime), "updateUser": r.updateUser,
            "processDefineName": r.defineName, "processDefineDisplayName": r.defineDisplayName,
            "processDefineVersion": r.defineVersion,
            "ext": r.variables, "displayName": r.defineDisplayName, "version": r.defineVersion}

    def _task_row_to_dict(self, r) -> dict:
        instance_ext = r.instanceVariable
        if isinstance(instance_ext, str):
            try:
                instance_ext = json.loads(instance_ext) if instance_ext else {}
            except Exception:
                instance_ext = {}
        # issues/121 P1：引擎建单必写的控制键不算「任务变量非空」，否则新建任务的 ext
        # 永远不再回退实例变量（issues/82-3 既有契约）。
        ext = r.variables or {}
        if not [k for k in ext if k != 'isFirstTaskNode']:
            ext = instance_ext
        return {"id": r.id, "processInstanceId": r.processInstanceId, "taskName": r.taskName,
                "displayName": r.displayName, "taskType": r.taskType, "performType": r.performType,
                "taskState": int(r.taskState) if r.taskState is not None else None,
                "operator": r.operator, "finishTime": self._fmt_time(r.finishTime),
                "expireTime": self._fmt_time(r.expireTime), "formKey": r.formKey,
                "taskParentId": r.taskParentId,
                "createTime": self._fmt_time(r.createTime), "createUser": r.createUser,
                "updateTime": self._fmt_time(r.updateTime), "updateUser": r.updateUser,
                "processDefineName": r.processDefineName,
                "processDefineDisplayName": r.processDefineDisplayName,
                "instanceCreateTime": self._fmt_time(r.instanceCreateTime),
                "ext": ext, "instanceExt": instance_ext, "version": r.defineVersion,
                "taskFormData": self._form_data_of(ext, "tf_")}  # issues/15


def _stats_round4(v: float) -> float:
    return round(v * 10000) / 10000


def _stats_week_key(t: datetime) -> str:
    iso = t.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def _stats_enumerate_buckets(start: Optional[datetime], end: Optional[datetime],
                             granularity: str) -> list[str]:
    now = datetime.now()
    s = start if start else now - timedelta(days=30)
    e = end if end else now

    buckets: list[str] = []
    if granularity == "hour":
        cursor = s.replace(minute=0, second=0, microsecond=0)
        while cursor <= e:
            buckets.append(cursor.strftime("%Y-%m-%d %H:00"))
            cursor += timedelta(hours=1)
    elif granularity == "day":
        cursor = s.replace(hour=0, minute=0, second=0, microsecond=0)
        end_day = e.replace(hour=0, minute=0, second=0, microsecond=0)
        while cursor <= end_day:
            buckets.append(cursor.strftime("%Y-%m-%d"))
            cursor += timedelta(days=1)
    elif granularity == "week":
        cursor = s.replace(hour=0, minute=0, second=0, microsecond=0)
        weekday = cursor.weekday()
        cursor -= timedelta(days=weekday)
        end_day = e.replace(hour=0, minute=0, second=0, microsecond=0)
        while cursor <= end_day:
            buckets.append(_stats_week_key(cursor))
            cursor += timedelta(days=7)
    elif granularity == "month":
        year, month = s.year, s.month
        end_year, end_month = e.year, e.month
        while (year, month) <= (end_year, end_month):
            buckets.append(f"{year}-{month:02d}")
            month += 1
            if month > 12:
                month = 1
                year += 1
    return buckets


def _stats_bucket_key(t: datetime, granularity: str) -> str:
    if granularity == "hour":
        return t.strftime("%Y-%m-%d %H:00")
    elif granularity == "day":
        return t.strftime("%Y-%m-%d")
    elif granularity == "week":
        return _stats_week_key(t)
    elif granularity == "month":
        return t.strftime("%Y-%m")
    return ""


def _is_id_key(k: str) -> bool:
    """id 类字段名判定（对齐 Java 实体 id 命名）：精确 'id' 或以 'Id' 结尾
    （processDefineId/processInstanceId/processTaskId/processDesignId/parentId/...）"""
    return k == "id" or k.endswith("Id")


def _stringify_ids(v):
    """出口 id 统一 string 化（issues/38 E9，对齐 Node 全程 string / Java 全局
    ToStringSerializer）——递归处理 dict/list；id 类字段的 int 值转 str，
    None 保持 None（parentId 无值不出 'None'），字符串直通。

    dataclass 分支（issues/76）：dataclass 实例 asdict 后递归，收口
    "嵌套 dataclass 列表整表外泄 int id" 的泄漏面（his 列表），
    对齐 Go stringifyIDs 处理 reflect.Struct（issues/58）。"""
    if isinstance(v, dict):
        return {k: (_stringify_ids(val) if not _is_id_key(k) else
                    (None if val is None else
                     (str(val) if isinstance(val, int) and not isinstance(val, bool) else val)))
                for k, val in v.items()}
    if isinstance(v, (list, tuple)):
        return [_stringify_ids(x) for x in v]
    if dataclasses.is_dataclass(v) and not isinstance(v, type):
        return _stringify_ids(dataclasses.asdict(v))
    return v
