"""引擎核心——对标 Java EngineImpl"""
import logging
import inspect
import json, re, time, random
from datetime import datetime, timedelta
from typing import Any, Callable, Optional
from .model import (
    FlowModel, FlowNode, FlowEdge,
    TYPE_START, TYPE_END, TYPE_TASK, TYPE_DECISION, TYPE_FORK, TYPE_JOIN, TYPE_CUSTOM,
    KNOWN_NODE_TYPES,
    ProcessInstance, ProcessTask, ProcessDefine,
    InstanceState, TaskState, SubmitType, PerformType,
    parse_flow_model,
)
from .spi import ProcessRepository, UserProvider, IDGenerator, ExpressionEvaluator
from .spi import normalize_actors
from .extensions import EngineExtensions, EventType, ProcessEvent

KEY_SUBMIT_TYPE   = "submitType"
KEY_BUSINESS_NO   = "BUSINESS_NO"
KEY_USER_ID       = "u_userId"
KEY_REAL_NAME     = "u_realName"
KEY_DEPT_ID       = "u_deptId"
KEY_DEPT_NAME     = "u_deptName"
KEY_POST_ID       = "u_postId"
KEY_POST_NAME     = "u_postName"
# v1.0.1：下一节点处理人（对齐 boot3 tf_nextNodeOperator）
KEY_NEXT_NODE_OPERATOR = "tf_nextNodeOperator"
# v1.6.0：流程启动时预指派人（对齐 boot3 f_nextNodeOperator）——startAndExecute 时转换为 tf_
KEY_PROCESS_START_NEXT_NODE_OPERATOR = "f_nextNodeOperator"
# v1.0.1：系统代执行 / 超级管理员（对齐 boot3 FlowConst）
KEY_AUTO_ID   = "flow.auto"
KEY_ADMIN_ID  = "flow.admin"
# issue 29：自动生成标题（对齐 boot3 FlowConst.AUTO_GEN_TITLE）
KEY_AUTO_GEN_TITLE = "autoGenTitle"
# ─── 抄送人入参键（spec 11-events §11.7／issues/127；逐字对齐 Java FlowConst.CC_ACTORS_START·CC_ACTORS）
# 发起腿 f_ccActors、办理腿 tf_ccActors——两条腿在**引擎侧**共用同一个 handle_cc_actors 漏斗，
# 门面只解析参数不再自己建 cc 行／自己 fire（三栈 java/go/node 同形状，见该方法 docstring）。
# 办理腿的**覆盖面**按 spec §11.7 边界 2 只有 executeProcessTask 一条（钩子参数见
# ``_prepare_execute_task`` 的 ``on_task_updated``，与 go :102-105 传钩子 / :206 传 nil 同形）。
KEY_CC_ACTORS_START = "f_ccActors"
KEY_CC_ACTORS = "tf_ccActors"

# ─── 记录类（自定义）节点返回值变量键（逐字对齐 Java FlowConst.CUSTOM_RETURN_VAL，issues/141 G9）
# 节点 properties 的 `val` 指定别的键名时用那个；`val` 缺省才回落到本键。
KEY_CUSTOM_RETURN_VAL = "custom_return_val"

# ─── 事件分档（spec 11-events §11.3 码 5/6 互斥判据）──────────────────────────────

#: 归入 TASK_REJECT（码 6）的 submitType 档：拒绝 / 退上一步 / 退发起人 / 会签软拒绝。
#: 其余（APPLY/AGREE/JUMP/RE_APPLY 与未传）归 TASK_COMPLETE（码 5）。「跳转回退」按
#: spec 应归 6，但本栈的 JUMP 档位不带前/后向信息，按 §11.3 的「跳转」列入 5（见本轮报告）。
_REJECT_SUBMIT_TYPES = frozenset({int(SubmitType.REJECT), int(SubmitType.ROLLBACK),
                                  int(SubmitType.ROLLBACK_TO_OPERATOR),
                                  int(SubmitType.COUNTERSIGN_DISAGREE)})


def _to_submit_type(value) -> Optional[int]:
    """submitType 归一化为整数；脏值/缺省按 None 处理（不得因脏值把办掉说成退回）"""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_cc_actors(value: Any) -> list[str]:
    """抄送人入参归一（判据对齐 Java ``JeeflowEngineImpl.handleCcActors`` 的
    String/Collection 两支 ＋ Go ``parseCcActors``/Node ``parseCcActors`` 的 trim/丢空/去重）：

    逗号串、``list``/``tuple``、单个标量都吃 → ``list[str]``；逐项 ``str``＋trim、丢空项、
    **按出现顺序去重**；``None``/空串/空集合 → ``[]``（零副作用）。

    **issues/141 G10「空不创建行」**（spec 06 §2.10）：判据单点在
    ``spi.normalize_actors``（旧名 ``normalize_cc_actors``，同一枚对象；java 的
    ``StringUtils.normalizeCcActors`` 同构），本函数保留为**薄转发**（既有 API 名不动）——
    形态适配（逗号串拆成元素）也已收进那一枚，两形共用同一条尺子，这里不再留第二支。
    ``""`` 拆出来的是**一个空元素**（python 与 java 同病），归一后为 ``[]`` ⇒ 调用方
    （``handle_cc_actors``）既不建 cc 行也不 fire 码 4；``"0"`` 这类正常 id **不是**空值，不得丢。
    漏斗只是**第一层**，写侧（两仓 ``create_cc_instance`` ＋ SPI default ``create_cc_instance_if_absent``）
    还各有一层兜底，绕过引擎/门面直连仓储的调用方同样灌不进空值。

    去重不是锦上添花：``create_cc_instance`` 逐行写、CC_CREATE 逐人 fire，二者粒度必须一一对应
    （spec §11.3 码 4「逐抄送人 fire 一次」）。同一人传两次在内存仓会被 ``dict.fromkeys`` 折成一行，
    事件却发两条 ⇒「一行两事件」破掉粒度；SQL 仓那侧更是直接双写 cc 行。
    """
    return normalize_actors(value)


class Engine:
    """引擎接口"""

    async def start_process_instance_by_id(self, define_id: int, operator: str, args: dict[str, Any] = None) -> ProcessInstance: ...
    async def execute_process_task(self, task_id: int, operator: str, args: dict[str, Any] = None) -> ProcessInstance: ...
    async def execute_and_jump_to_end(self, task_id: int, operator: str, args: dict[str, Any] = None) -> ProcessInstance: ...
    async def execute_and_jump_task(self, task_id: int, operator: str, args: dict[str, Any] = None, target_task_name: str = None) -> ProcessInstance: ...
    async def execute_and_jump_to_first_task_node(self, task_id: int, operator: str, args: dict[str, Any] = None) -> ProcessInstance: ...
    async def handle_cc_actors(self, instance_id: int, operator: str, cc_actors: Any) -> list[str]:
        """抄送唯一漏斗（spec §11.7）：门面手动 ``createCCInstance`` 与引擎发起/办理两条腿共用；
        自定义引擎实现若不提供，手动抄送腿就只剩"写行不 fire"，属 §11.2 原则 1 的缺支。

        自定义实现同样要守 issues/141 G2（spec 06 §4）：落库走
        ``repo.create_cc_instance_if_absent``，fire 只用它返回的**实际新建子集**，子集为空不发码 4。"""
        ...

class EngineImpl(Engine):
    def __init__(self, repo: ProcessRepository, user_prov: UserProvider = None,
                 id_gen: IDGenerator = None, expr_eval: ExpressionEvaluator = None):
        self.repo = repo
        self.user_prov = user_prov
        self.id_gen = id_gen
        self.expr_eval = expr_eval
        self.ext: Optional[EngineExtensions] = None
        self._ic_cache: dict = {}   # 定义级拦截器解析缓存（issue 34，按 defineId）
        # 定义行 name 缓存（spec 06 §4.5 条款 1.1 回落路径，按 defineId）——
        # 委托查询的流程名解析全栈唯一入口是 _surrogate_process_name，缓存只是它的取数底座
        self._define_name_cache: dict = {}

    def set_extensions(self, ext: EngineExtensions):
        # 已接入的扩展仓储跨 set_extensions 保留（门面先 attach、集成方后设扩展体的顺序不能丢能力）
        if self.ext is not None and ext.ext_repository is None:
            ext.ext_repository = self.ext.ext_repository
        self.ext = ext
        self._ic_cache.clear()

    def attach_ext_repository(self, ext_repo) -> "EngineImpl":
        """接入扩展仓储（委托自动生效的查询数据源，issues/116 批次 D）

        ``JeeflowFacade`` 构造时自动调用——集成方只要给门面传了 ``ext_repo``，
        委托就默认生效（零配置，对齐内置版白拿体验）。未接入（传 None / 从未调用）时
        建任务阶段静默跳过委托查询，不抛错。
        """
        if ext_repo is None:
            return self
        if self.ext is None:
            self.ext = EngineExtensions()
        self.ext.ext_repository = ext_repo
        return self

    async def eval_expr(self, expr: str, vars_: dict) -> Any:
        """表达式求值（v1.5.0，门面 highLight 决策分支过滤用）"""
        if self.expr_eval is None:
            raise ValueError("ExpressionEvaluator 未配置")
        return await self.expr_eval.eval(expr, vars_)

    # ─── 委托查询流程名解析（spec 06 §4.5 条款 1.1 全栈唯一实现）────────────────

    def _cache_define_name(self, define_id, define_name) -> None:
        """把**已经读到手**的定义行 name 记入回落缓存（起点处 def_ 已在手，
        回落路径因此零额外定义读——见 _surrogate_process_name）。"""
        if define_id is not None:
            self._define_name_cache[define_id] = str(define_name or "").strip()

    async def _surrogate_process_name(self, flow: FlowModel, inst: ProcessInstance) -> str:
        """委托查询用的流程名（契约 06 §4.5 条款 1.1）：**先 trim 再判空**。

        取流程模型 ``name``（迁移基线：内置版 SurrogateInterceptor 用
        ``execution.getProcessModel().getName()``，Java 参考实现与之同构）；
        模型未带（键缺失 / ``null`` / 空串 / **仅空白**）才回落 ``wf_process_define.name``。
        **返回给委托查询的值一律 trim 后**（``" 名 "`` 与 ``"名"`` 必须命中同一条委托）。

        为什么不是"假值判空"（此前本栈写法 ``flow.name or def_.name``）：模型 name 为纯空白时，
        假值判空会把空白串当流程名去查委托——精确查必不中、兜底查也拿不到该流程自己配的委托，
        只剩"全流程兜底"行能命中 ⇒ 用户视角＝委托静默失效；而 Java/PHP/Node/Go 同数据会回落
        定义行 name 并命中。同一份数据两栈不同答案，就是本轮堵掉的分叉。

        回落路径要读定义行（部分栈含 ``content`` BLOB），故按 defineId 缓存复用，**不得逐任务解析**
        （条款 1.1 尾注）。读定义行失败按「拿不到流程名」处理（只能命中全流程兜底委托），
        绝不打断建单（条款 4：委托是增强能力）。
        """
        name = str(getattr(flow, "name", "") or "").strip()
        if name:
            return name
        define_id = getattr(inst, "defineId", None)
        if define_id is None:
            return ""
        cached = self._define_name_cache.get(define_id)
        if cached is not None:
            return cached
        name = ""
        try:
            def_ = await self.repo.find_define_by_id(define_id)
            name = str(getattr(def_, "name", "") or "").strip()
        except Exception:  # noqa: BLE001 —— 回落读定义行失败不得打断建单
            logging.exception("[jeeflow] find_define_by_id(%s) failed, surrogate processName unresolved",
                              define_id)
        self._define_name_cache[define_id] = name
        return name

    # ─── 抄送腿（spec 11-events §11.7／issues/127）─────────────────────────────────

    async def handle_cc_actors(self, instance_id: int, operator: str, cc_actors: Any) -> list[str]:
        """抄送的**唯一漏斗**：归一化抄送人 → **先** ``create_cc_instance_if_absent`` 落 cc 行 →
        落库**后**逐**实际新建**的抄送人 fire ``CC_CREATE``(码 4)。

        形状基准＝jeeflow-java ``JeeflowEngineImpl.handleCcActors`` → ``ProcessPublisher.notifyCcCreate``
        （发起 ``f_ccActors`` 与办理 ``tf_ccActors`` 共用同一条腿），go ``EngineImpl.HandleCcActors``、
        node ``Engine.handleCcActors`` 同构。**三条路径都进这一个函数**（spec §11.2 原则 1
        「同一事实只发一次、路径不进事件名」）：

        - 发起 ``f_ccActors`` —— 本引擎 ``start_process_instance_by_id`` 内部调用；
        - 办理 ``tf_ccActors`` —— 只由 ``execute_process_task`` 经 ``_prepare_execute_task`` 的
          ``on_task_updated`` 钩子调用（与任务更新同一次调用栈）。**jump/reject 族不挂此钩子**
          ——spec §11.7 边界 2 明写覆盖面「只算 executeProcessTask 一条」，
          「``executeAndJumpTask`` / ``jumpToEnd`` / ``rollbackToOperator`` 这类跳转·回退 action
          带的 ``tf_ccActors`` 本轮不建 cc、不发 ``CC_CREATE``」，单栈自行放宽＝跨栈分叉；
        - 手动 ``processInstance/createCCInstance`` —— 门面调本方法（不再自己建行、不再自己 fire）。

        ⚠️ **契约位置**：cc 行的写入与事件都在**引擎执行路径内**，因此与同一次 ``start``/``execute``
        里的实例/任务写库处在**同一个事务作用域**——本栈的事务约定是
        ``JdbcProcessRepository.with_tx``（``contextvars`` 绑连接，spec 05 §7.4）：调用方把
        ``engine.start/execute`` 包进 ``with_tx`` 时，cc 行 insert 与任务/实例更新同连接同事务、
        一起提交一起回滚；未包时逐条 autocommit（与本栈其余写库点同档）。
        抄送腿若留在门面（本轮之前的形状），门面是在 ``engine.*`` **返回之后**才执行的 ⇒
        任何只包住引擎调用的事务都盖不到 cc 行，且**直连引擎 API 的调用方（不经门面）
        传 ``f_ccActors``/``tf_ccActors`` 根本不建 cc 行**——这正是本轮要搬掉的病灶。

        ``cc_actors`` 为 ``None``/空 ⇒ 零写入、零 fire、返回 ``[]``（纯增量：不带抄送的发起/办理
        行为与上一版逐字一致）。返回归一化后的**请求**抄送人列表（不是新建子集——调用方按
        "我请求抄给了谁"读，事件按"实际新建了谁"发，两件事各有各的形状）。

        **空不创建行**（issues/141 G10 · spec 06 §2.10）：``f_ccActors``/``tf_ccActors`` 给
        ``""``、``"   "``、``"a,"``、``["a", "", "  "]`` 这类形态时，``parse_cc_actors`` 先归一
        ——空串/纯空白/空元素全丢，**丢完为空就是 ``[]``** ⇒ 这一支既不建行也不 fire 码 4；
        逗号串与数组两形同判据（``" 123 "`` 与 ``"123"`` 归一后是同一个人，与下面的写侧判重咬合）。

        重复抄送同一个人（issues/141 G2 · spec 06 §4）＝数据面 no-op：不新增行、不重置未读、
        不刷原行时间，且**不发**码 4。判重落在仓储写侧（两仓同判据），事件收口落在这一个漏斗里，
        三条入口（``f_ccActors``／``tf_ccActors``／手动 ``createCCInstance``）共用同一条腿。
        """
        cc_list = parse_cc_actors(cc_actors)
        if not cc_list or not instance_id:
            return cc_list
        # issues/141 G2 写侧判重＝幂等空操作（spec 06 §4）：同一 (实例, 被抄送人) 已有 cc 行时跳过，
        # 拿回来的 created 是**实际新建的子集**（可能比 cc_list 短，甚至为空）。
        created = await self.repo.create_cc_instance_if_absent(instance_id, operator, cc_list)
        # 逐人 fire 的入参＝实际新建的子集，不是原始 cc_list（issues/141 G2 · spec §11.2 原则 1
        # 「码值表达发生了什么事实」）：重复抄送没发生"创建"⇒ 不发码 4；子集为空**整支不 fire**
        # （不空转，也不照旧全量 fire）。
        if created:
            await self._notify_cc_create(instance_id, created)
        return cc_list

    async def _notify_cc_create(self, instance_id: int, cc_list: list[str]) -> None:
        """CC_CREATE（码 4，issues/102）逐抄送人 fire，与 cc 行的逐行写一一对应
        （对齐 Java ``ProcessPublisher.notifyCcCreate``）。``ccActorId`` 直传事件体，监听器免反查 cc 表。
        接收人过滤（合法性/存在性）属集成层监听器职责，引擎只按 cc 行粒度 fire。
        ⚠️ 只在 cc 行落库**之后**调用（spec §11.2 原则 3）——本方法是 ``handle_cc_actors`` 的下游，
        不得脱离 cc 行落库单独调用。

        ⚠️ ``cc_list`` **入参一律是"实际新建的 actor 子集"**（issues/141 G2 · spec 06 §4）：
        调用点先走 ``repo.create_cc_instance_if_absent`` 拿子集，**子集为空整支不 fire**——
        §11.2 原则 1「码=事实」，重复抄送没发生"创建"就不该发码 4，严禁照旧按原始请求全量 fire。
        """
        for actor in cc_list:
            await self._fire_event(ProcessEvent(type=EventType.CC_CREATE, instanceId=instance_id,
                                                ccActorId=actor))

    # ─── Start ────────────────────────────────────────────────────────────────

    async def start_process_instance_by_id(self, define_id: int, operator: str, args: dict[str, Any] = None) -> ProcessInstance:
        def_ = await self.repo.find_define_by_id(define_id)
        if not def_: raise ValueError(f"define not found: {define_id}")
        flow = parse_flow_model(json.loads(def_.content))
        # 委托查询的流程名解析已收敛到 _surrogate_process_name 单点（spec 06 §4.5 条款 1.1）。
        # 此处只做一件事：把**刚读到手**的定义行 name 记入回落缓存，令模型未带 name 时的回落零额外读。
        self._cache_define_name(define_id, def_.name)
        vars_ = {**(args or {})}
        await self._add_user_info(operator, vars_)
        self._add_auto_gen_title(def_.displayName, vars_)
        inst = ProcessInstance(id=self._next_id(), defineId=define_id, operator=operator,
                               variables=vars_, createTime=datetime.now(), updateTime=datetime.now(),
                               createUser=operator, updateUser=operator,
                               businessNo=str(vars_.get(KEY_BUSINESS_NO, "")))
        # issues/137 A · 裁定 A（批二 §3-4）· **实例级 expire_time 的唯一写点**：
        # 取**流程定义顶层** expireTime 表达式（spec 02:21/55「流程期望完成时间」），非空才求值写入。
        # 基准＝Java JeeflowEngineImpl.java:93-96（``if (StringUtils.isNotEmpty(...)) instance.setExpireTime(
        # FlowUtil.processTime(expireTime, args))``）与 boot2 内置版 ProcessInstanceServiceImpl.java:157-160；
        # 本栈原形状是**这一句都没有** ⇒ wf_process_instance.expire_time 恒 NULL（本案病灶）。
        # 三条口径逐条落到这里：① 进列的是**求值结果（时刻）**不是表达式原串——原串在 STRICT_TRANS_TABLES
        # 的 MySQL 上是服务端硬错（rust/php 实测 1292/22007 Incorrect datetime value: '2h'）；
        # ② 变量源＝**发起参数**（上面 ``_add_user_info`` / ``_add_auto_gen_title`` 注入完的那份 ``vars_``，
        # 与 Java 就地改过的 ``args`` 同档；取成 caller 原始 args 会让被注入覆盖的键判错档）；
        # ③④ 没配／算不出都留 NULL，不兜底 now()——守卫与尺子都是既有的那一枚（``_apply_expire_time``
        # 包着的 ``process_time``，档位顺序与语义一字不改，不许新造第二把）。
        _apply_expire_time(inst, flow.expireTime, vars_)
        await self.repo.save_instance(inst)
        # PROCESS_INSTANCE_START（码 1）：实例行 insert 之后 fire（spec §11.3 触发时机列）
        await self._fire_event(ProcessEvent(EventType.PROCESS_INSTANCE_START, inst.id,
                                            defineId=define_id, operator=operator))
        # 发起腿抄送（issues/127／spec §11.7）：实例行 insert 之后、节点执行之前，与实例写库
        # 处在同一次调用栈（同事务作用域）建 cc 行并逐人 fire 码 4。
        # 位置对齐 Java startProcessInstanceById 的第 6→7 步（saveInstance → handleCcActors → start.execute）。
        await self.handle_cc_actors(inst.id, operator, (args or {}).get(KEY_CC_ACTORS_START))
        start_node = _find_by_type(flow, TYPE_START)
        if not start_node: raise ValueError("no start node")
        for node in _follow_edges(flow, start_node.id):
            await self._execute_node(flow, inst, node, operator, vars_)
        return await self.repo.find_instance_by_id(inst.id)

    # ─── Execute ──────────────────────────────────────────────────────────────

    async def execute_process_task(self, task_id: int, operator: str, args: dict[str, Any] = None) -> ProcessInstance:
        # issues/127 / spec §11.7 办理时抄送（tf_ccActors）：**覆盖面只有本条腿**——钩子挂在这里，
        # 不挂在共用的 ``_prepare_execute_task`` 里（形状基准＝go engine_impl.go:102-105 传钩子、
        # :206/:228/:265 三个 jump 入口传 nil；java 基准 JeeflowEngineImpl 的 handleCcActors 唯一
        # 调用点也在 executeProcessTask 的 runInTx 内）。以后新增办理入口默认不带 cc，不会漏收。
        args = args or {}
        task, inst, flow, vars_ = await self._prepare_execute_task(
            task_id, operator, args,
            on_task_updated=lambda instance_id: self.handle_cc_actors(
                instance_id, operator, args.get(KEY_CC_ACTORS)))
        now = datetime.now()
        cur_node = _find_node(flow, task.taskName)
        if cur_node:
            # 1.8.0：任务完成节点自身的后置拦截器（SYNC 同步演进——任务节点推进更新状态/字段）。
            # _create_task 不再触发（引擎语义修正），此处为完成任务节点的唯一触发点
            await self._fire_post(cur_node, inst)
            ct = cur_node.properties.get("countersignType", "")
            cs_cond = str(cur_node.properties.get("countersignCompletionCondition", "") or "").strip()
            # issues/91：会签一票否决仅当节点配置 ONE_VOTE_VETO（忽略大小写）时生效，
            # submitType=20 才跳过会签"未完成即停留"门控提前流转；否则为软拒绝——
            # 否决者任务正常完成、countersignDisagreeFlag=1 已记录为变量（供下游参考），
            # 流程不阻断（对齐 mldong 内置引擎 / Java CountersignHandler）
            try:
                cs_veto = ct != "" and cs_cond.upper() == "ONE_VOTE_VETO" and \
                    int(vars_.get(KEY_SUBMIT_TYPE, -1)) == int(SubmitType.COUNTERSIGN_DISAGREE)
            except (ValueError, TypeError):
                cs_veto = False
            if ct == "SEQUENTIAL" and not cs_veto:
                doing = await self.repo.find_doing_tasks(inst.id)
                if not doing:
                    actors, lc = _get_cs_state(vars_, cur_node.id)
                    if actors and lc + 1 < len(actors):
                        # 聚合根：创建串行会签下一步任务
                        nt = inst.create_task(self._next_id(), cur_node.id, cur_node.text.get("value", ""),
                                              actors[lc + 1], operator, cur_node.properties.get("form", ""), now,
                                              # 建单不变量：parent＝刚办结的那一位成员任务
                                              task.id, self._is_first_task_node(flow, cur_node), 1)
                        nt.variables |= {f"operatorList_{cur_node.id}": actors, f"loopCounter_{cur_node.id}": lc + 1,
                                        f"nrOfInstances_{cur_node.id}": len(actors)}
                        # 写点⑤「串行会签推进出的下一位成员」——这一支绕过 _create_tasks 直建任务行，
                        # 必须显式上同一把尺子。基准依据：boot2 的串行推进是**回调**
                        # createCountersignTask（ProcessTaskServiceImpl:485，内含 :524 那处到期写）
                        # ⇒ 推进出的第二、三位成员同样带到期时间；java 同批补在 CountersignHandler.
                        # createNextCountersignTask（commit cb541d4，入口 ProcessInstance.applyNodeExpireTime）。
                        # 变量源＝实例变量（与写点①②③同档，boot2 的 execution.getArgs()；
                        # 只有写点④回退新建用随行那份 hisVariable）。
                        _apply_expire_time(nt, (cur_node.properties or {}).get("expireTime"), inst.variables)
                        await self._apply_surrogate(nt, await self._surrogate_process_name(flow, inst))
                        await self.repo.save_task(nt)
                        # PROCESS_TASK_START（码 3）：顺序会签推进新任务落库后 fire（对齐 Java CreateTaskHandler）
                        await self._fire_task_start(inst, nt, cur_node.id, operator)
                        return await self.repo.find_instance_by_id(inst.id)
                else:
                    return await self.repo.find_instance_by_id(inst.id)
            if (ct in ("PARALLEL",) or ct.startswith("RATIO")) and not cs_veto:
                doing = await self.repo.find_doing_tasks(inst.id)
                if doing: return await self.repo.find_instance_by_id(inst.id)

            # issues/91：会签节点 merged 后（ONE_VOTE_VETO 否决 / 全部完成任一路径），
            # 废弃该节点剩余 DOING 任务（对齐内置引擎 abandonProcessTask）：
            # SEQUENTIAL 逐人创建天然 no-op；PARALLEL 全员预创建，否决时废弃其余成员
            # （刚完成者已 DONE 不会误伤）。逐条持久化并回写聚合副本（E25：防 update_instance 级联回写旧状态）
            if ct:
                remaining = await self.repo.find_doing_tasks(inst.id, [cur_node.id])
                for t in remaining:
                    t.abandon(now)
                    await self.repo.update_task(t)
                    _sync_task_to_aggregate(inst, t)

            for node in _follow_edges(flow, cur_node.id):
                # 统一走 _execute_node：结束节点也经节点执行链（拦截器/事件完整触发），
                # _execute_node 内部 TYPE_END 分支完成聚合根 finish + 事件发布
                await self._execute_node(flow, inst, node, operator, vars_, task.id)
        return await self.repo.find_instance_by_id(inst.id)

    # ─── Reject ───────────────────────────────────────────────────────────────

    async def execute_and_jump_to_end(self, task_id: int, operator: str, args: dict[str, Any] = None) -> ProcessInstance:
        # 门面 submitType=2 REJECT 唯一入口（对齐 Java executeAndJumpToEnd 语义）
        # spec §11.7 边界 2：jumpToEnd 档不带 cc 钩子（显式 None，同 go :206 传 nil）——
        # 这一档传 tf_ccActors 也零 cc 行、零码 4。
        _, inst, _, _ = await self._prepare_execute_task(
            task_id, operator, args, reject_as=int(SubmitType.REJECT), on_task_updated=None)
        inst.reject(datetime.now())
        await self.repo.update_instance(inst)
        # 实例进入终态＝PROCESS_INSTANCE_END（码 2），拒绝/办结合一号（spec §11.6：
        # 旧的 PROCESS_REJECT/PROCESS_FINISH 拆分以「实例终态＝2」为准，靠载荷 state 分）
        await self._fire_event(ProcessEvent(EventType.PROCESS_INSTANCE_END, inst.id,
                                            operator=operator, state=int(inst.state)))
        return await self.repo.find_instance_by_id(inst.id)

    # ─── Jump（ROLLBACK 空 target / JUMP 命名 target，boot2 executeAndJumpTask）──

    async def execute_and_jump_task(self, task_id: int, operator: str, args: dict[str, Any] = None,
                                     target_task_name: str = None) -> ProcessInstance:
        # 空 target＝血缘回退（spec §11.3 码 6 的"退回"族）；命名 target＝跳转（码 5 族）
        # spec §11.7 边界 2：ROLLBACK / JUMP 两档都不挂 cc 钩子（显式 None，同 go :228）
        task, inst, flow, vars_ = await self._prepare_execute_task(
            task_id, operator, args,
            reject_as=None if target_task_name else int(SubmitType.ROLLBACK),
            on_task_updated=None)
        if not target_task_name:
            # issues/121 P2：ROLLBACK 走血缘版——复活 parentTaskId 指的那条历史行，
            # 参与者＝该行办结人（首任务节点行取该行 u_userId）。无血缘/守卫不过显式报错，
            # 不再像拓扑版那样"什么都不做、实例保持 DOING 却零待办"。
            await self._rollback_to_parent(flow, inst, task, operator)
        else:
            # issues/79：对齐 Java——目标节点不存在显式报错（前端 JUMP 无效 taskName 不再静默空操作）
            target = _find_node(flow, target_task_name)
            if target is None:
                raise ValueError(f"根据节点名称[{target_task_name}]无法找到节点模型")
            # 对齐 Java isFirstTaskName：跳首任务节点（start 直接后继）assignee 强制为发起人
            if target.type == TYPE_TASK and self._is_first_task_node(flow, target):
                target.properties["assignee"] = inst.operator
            await self._execute_node(flow, inst, target, operator, vars_, task.id)
        return await self.repo.find_instance_by_id(inst.id)

    # ─── Jump To First Task（退回发起人，boot2 ROLLBACK_TO_OPERATOR=6）───────

    async def execute_and_jump_to_first_task_node(self, task_id: int, operator: str,
                                                   args: dict[str, Any] = None) -> ProcessInstance:
        # 退发起人＝spec §11.3 码 6 的"退回"族（载荷 submitType=6 分档，不另开号）
        # spec §11.7 边界 2：退发起人档不挂 cc 钩子（显式 None，同 go :265）
        _, inst, flow, vars_ = await self._prepare_execute_task(
            task_id, operator, args, reject_as=int(SubmitType.ROLLBACK_TO_OPERATOR),
            on_task_updated=None)
        # 找到第一个任务节点，强制参与者为发起人，重新执行
        start_node = _find_by_type(flow, TYPE_START)
        if start_node:
            for node in _follow_edges(flow, start_node.id):
                if node.type in (TYPE_TASK, TYPE_CUSTOM):
                    node.properties["assignee"] = inst.operator
                    await self._execute_node(flow, inst, node, operator, vars_, task_id)
                    break
        return await self.repo.find_instance_by_id(inst.id)

    # ─── Execute 公共序言（对齐 Java prepareExecution）────────────────────────

    async def _prepare_execute_task(self, task_id: int, operator: str, args: dict[str, Any] = None,
                                     reject_as: Optional[int] = None,
                                     on_task_updated: Optional[Callable[[int], Any]] = None):
        """执行公共序言（对齐 Java prepareExecution）：权限校验 → f_ 字段权限过滤 →
        完成任务（子实体状态转换 + 实例变量合并，经 update_instance 级联落库）→
        返回流程模型 + 合并后执行变量。Java jump 路径不废弃其余 DOING 任务
        （会签兄弟任务不受影响），此处保持一致。

        ``reject_as``：调用方本身就是「退回」族动作时显式声明其 submitType 档
        （REJECT=2 / ROLLBACK=3 / ROLLBACK_TO_OPERATOR=6）。有值 ⇒ 本次任务事件必发
        ``TASK_REJECT``（码 6），不依赖调用方有没有把 submitType 塞进 args。

        ``on_task_updated``：**任务行 update 落库之后、码 5/6 fire 之前**的钩子（协程，收 instance_id）。
        形状照 go ``prepareExecuteTask(ctx, …, onTaskUpdated)``——cc 这类「只对某一条办理腿成立」的
        副作用由**调用方注入**，而不是在公共序言里判断"我是哪条腿"，这样以后加入口不会漏。
        本栈唯一挂此钩子的是 ``execute_process_task``（办理抄送腿）；jump/reject 族一律不挂
        （spec §11.7 边界 2）。钩子留在公共序言的调用位＝与任务更新同一次调用栈（同事务作用域），
        满足 §11.7 边界 1。"""
        task, inst = await self._load_and_check(task_id, operator)
        # issues/26：办理提交的 f_ 字段按任务节点字段权限过滤（只读/隐藏不入变量）
        def_ = await self.repo.find_define_by_id(inst.defineId)
        flow = parse_flow_model(json.loads(def_.content))
        # 定义行 name 记入回落缓存（同 start 路径，条款 1.1 解析收敛在 _surrogate_process_name 单点）
        self._cache_define_name(inst.defineId, def_.name)
        args = _filter_field_by_perm(args or {}, _find_node(flow, task.taskName))
        # issues/97：捕获原始实例变量（start 注入的发起人 u_*）——操作人 u_* 只进执行上下文
        # 与任务行，不得整体写回实例（对齐 Java completeTask=putAll(args)，args 不含 u_*）。
        base_vars = inst.variables
        vars_ = {**base_vars, **task.variables, **args}
        await self._add_user_info(operator, vars_)
        now = datetime.now()
        # 聚合根：完成任务（子实体状态转换 + 实例变量合并）
        inst.complete_task(task, operator, vars_, now)
        await self.repo.update_task(task)
        # v1.0.1：update_instance 级联持久化依赖聚合内任务副本为最新状态，
        # complete_task 改的是外部任务对象，需同步回聚合根
        _sync_task_to_aggregate(inst, task)
        # 办理腿副作用钩子位（spec §11.7 边界 1「与任务更新同事务」）：位置取 Go
        # ``prepareExecuteTask`` 的那一刀（UpdateTask 之后、码 5/6 之前）。
        # ⚠️ 抄送**不在这里无条件发生**：cc 由 ``execute_process_task`` 注入的钩子带来，
        # jump/reject 族不注入 ⇒ 那些档位带 ``tf_ccActors`` 也零建行、零 fire 码 4
        # （spec §11.7 边界 2；上一版把漏斗挂在本序言内 ⇒ 五条办理腿全建 cc，属"单栈超集"
        # 跨栈分叉，本轮按 go/java 收窄）。
        if on_task_updated is not None:
            await on_task_updated(inst.id)
        # 任务落库后 fire（spec §11.3 码 5/6 互斥：同一动作走退回就不再 fire「办掉」；
        # 拒绝/退上一步/退发起人/会签软拒绝都归 TASK_REJECT，靠载荷 submitType 分档）。
        # submitType 只取**本次 args**——实例变量里可能残留上一步的 submitType（_merge_exec_into_instance
        # 会保留非 u_ 键），拿残留值判档会把一次普通"同意"错认成"退回"。
        submit_type = _to_submit_type((args or {}).get(KEY_SUBMIT_TYPE))
        if submit_type is None:
            submit_type = reject_as
        is_reject = reject_as is not None or submit_type in _REJECT_SUBMIT_TYPES
        evt_type = EventType.TASK_REJECT if is_reject else EventType.TASK_COMPLETE
        await self._fire_event(ProcessEvent(evt_type, inst.id, task.id, task.taskName, operator,
                                            submitType=submit_type))
        # issues/97：实例变量写回排除操作人 u_*，保留 start 注入的发起人 u_*（u_realName 恒为发起人）
        inst.variables = _merge_exec_into_instance(base_vars, vars_)
        await self.repo.update_instance(inst)
        return task, inst, flow, vars_

    async def _rollback_to_parent(self, flow: FlowModel, inst: ProcessInstance,
                                   task: ProcessTask, operator: str) -> ProcessTask:
        """退回上一步（血缘版，规范 04 · 退回上一步）：上一步来源＝当前行的 parentTaskId，
        复活那条历史行；不按模型入边拓扑推。对外 msg 用固定中文文案、不含引擎内部码（出口统一 99999999）。"""
        NO_LINEAGE = "上一步任务ID为空，无法驳回至上一步处理"
        GUARD = "无法驳回至上一步处理，请确认上一步骤并非fork、join、suprocess以及会签任务"
        parent_id = getattr(task, "parentTaskId", None)
        if not parent_id:
            raise ValueError(NO_LINEAGE)
        his = await self.repo.find_task_by_id(parent_id)
        if his is None:
            raise ValueError(NO_LINEAGE)
        prev = _find_node(flow, his.taskName)
        if prev is None or not _can_rejected(flow, task.taskName, prev.id):
            raise ValueError(GUARD)
        # 首任务节点那条由发起人提交 ⇒ 参与者取该行 u_userId；其余取该行办结人。
        # 老行没这个键 ⇒ 按 False 处理（宁可派给该行 actorId，也不用带"仅进行中"判定的现算值）。
        is_first = bool((his.variables or {}).get("isFirstTaskNode"))
        actor = his.actorId
        if is_first:
            actor = (his.variables or {}).get("u_userId") or inst.operator
        if not actor:
            raise ValueError(NO_LINEAGE)
        now = datetime.now()
        form = (prev.properties or {}).get("form", "")
        nt = inst.create_task(self._next_id(), prev.id, (prev.text or {}).get("value", ""),
                              actor, his.createUser, form, now,
                              his.parentTaskId if his.parentTaskId is not None else 0, is_first,
                              his.performType or 0)
        # 复活行只带数据类键（tf_*/csv_*/submitType/taskName/会签簿记都是上次提交的残留）
        nt.variables = _lineage_vars(his.variables)
        nt.variables["isFirstTaskNode"] = is_first
        # 写点④「退回/跳转新建」（Java rejectTask 本轮并入同一个 applyExpireTime）：到期时间按
        # **被回退掉的那个节点**（＝当前行所属节点 task.taskName，boot2 里的 current）的表达式重算，
        # **不是**复活行落地的那个节点（prev＝历史行所属节点；form 等数据类字段仍照 prev 走，
        # boot2 就是这个形状）。基准逐字：ProcessTaskServiceImpl.rejectTask :363
        # current = model.getNode(currentTask.getTaskName()) → :385 expireTime =
        # ((TaskModel)current).getExpireTime() → :387 setExpireTime(FlowUtil.processTime(expireTime,
        # hisVariable))（java/php/csharp/rust 同此）。变量源＝新建行随行那份（boot2 的 hisVariable），
        # **不是实例变量**——两档混了，"表达式是变量名"这一档就会跨栈给出不同答案。
        cur_node = _find_node(flow, task.taskName)
        _apply_expire_time(nt, (cur_node.properties or {}).get("expireTime") if cur_node else None,
                           nt.variables)
        await self._apply_surrogate(nt, await self._surrogate_process_name(flow, inst))
        await self.repo.save_task(nt)
        await self._fire_task_start(inst, nt, prev.id, operator)
        return nt

    def _is_first_task_node(self, flow: FlowModel, node: FlowNode) -> bool:
        """是否 start 直接后继任务节点（issues/79 对齐 Java FlowUtil.isFirstTaskName）"""
        start = _find_by_type(flow, TYPE_START)
        if start is None:
            return False
        return any(e.sourceNodeId == start.id and e.targetNodeId == node.id for e in flow.edges)

    def _rollback_actors(self, node: FlowNode, inst: ProcessInstance, operator: str, task: ProcessTask) -> list[str]:
        """ROLLBACK 新任务参与者：优先当前任务完成人（退回操作人，
        对齐 Java rejectTask singletonList(currentTask.getActorId())），
        其次按目标节点 assignee 解析"""
        if task.actorId:
            return [task.actorId]
        return self._sync_resolve_actors(node, inst, operator) or [operator]

    def _sync_resolve_actors(self, node: FlowNode, inst: ProcessInstance, operator: str) -> list[str]:
        """_resolve_actors 同步子集（ROLLBACK 场景：无 ext 注册表/处理器回调，
        仅 tf_nextNodeOperator / assignee token 解析，对齐 Java rejectTask 语义）"""
        # 退回腿与办理腿必须是**同一枚**归一单点（issues/142 §2 B 表：本栈两处数组臂逐字一样，
        # 是同一把第二尺子的两个副本；只修一处就是同栈内分叉）
        next_op = normalize_actors(inst.variables.get(KEY_NEXT_NODE_OPERATOR))
        if next_op:
            return next_op
        assignee = node.properties.get("assignee", "")
        if assignee:
            actors = []
            for a in assignee.split(","):
                token = a.strip()
                if not token: continue
                if "applicant" in token:
                    token = token.replace("applicant", inst.operator)
                if token in inst.variables:
                    val = inst.variables[token]
                    if isinstance(val, (list, tuple)):
                        actors.extend(str(x) for x in val)
                    else:
                        actors.append(str(val))
                else:
                    actors.append(token)
            return actors
        return []

    async def _create_task_with_actors(self, node: FlowNode, inst: ProcessInstance, operator: str,
                                        vars_: dict, actors: list[str], process_name: str = "",
                                        parent_id: int = 0, is_first: bool = False):
        """以显式参与者建任务（会签节点拆分为逐人任务，对齐 Java 会签创建语义）；
        建单前同样应用委托（issues/116：任何新任务都是"建单那一刻"）

        **当前零调用者，但承担契约形状义务（issues/137 B）**——121 P2 之后回退改走血缘版
        ``_rollback_to_parent``，本函数成了"显式参与者建单"这条形状的留档位。owner 拍"不删、
        补用例钉住"（口径：与主路径 ``_create_task`` 的建单产物逐维一致，见 spec_test
        ``test_i137b_create_task_with_actors_matches_main_path``）。
        ⚠️ 已知缺口（126 案 A 同源，rust 侧 ``reject_task`` 同条留档）：本函数**没有**到期写点①
        （不调 ``_apply_expire_time``）——将来要复活它，先补写点再把该维并进一致性断言。
        """
        if not actors: return
        ct = node.properties.get("countersignType", "")
        _pt = node.properties.get("performType", 0)
        try:
            perform_type = int(_pt)
        except (ValueError, TypeError):
            perform_type = 1 if str(_pt).strip().upper() in ("ALL", "COUNTERSIGN") else 0
        now = datetime.now()
        form = node.properties.get("form", "")
        if perform_type == 1 and ct:
            if ct in ("PARALLEL", ""):
                for a in actors:
                    nt = inst.create_task(self._next_id(), node.id, node.text.get("value", ""), a, operator, form, now, parent_id, is_first, 1)
                    await self._apply_surrogate(nt, process_name)
                    await self.repo.save_task(nt)
                    await self._fire_task_start(inst, nt, node.id, operator)
            elif ct == "SEQUENTIAL":
                nt = inst.create_task(self._next_id(), node.id, node.text.get("value", ""), actors[0], operator, form, now, parent_id, is_first, 1)
                nt.variables |= {f"operatorList_{node.id}": actors, f"loopCounter_{node.id}": 0, f"nrOfInstances_{node.id}": len(actors)}
                await self._apply_surrogate(nt, process_name)
                await self.repo.save_task(nt)
                await self._fire_task_start(inst, nt, node.id, operator)
            else:
                for a in actors:
                    nt = inst.create_task(self._next_id(), node.id, node.text.get("value", ""), a, operator, form, now, parent_id, is_first, 1)
                    await self._apply_surrogate(nt, process_name)
                    await self.repo.save_task(nt)
                    await self._fire_task_start(inst, nt, node.id, operator)
        else:
            nt = inst.create_task(self._next_id(), node.id, node.text.get("value", ""), actors[0], operator, form, now, parent_id, is_first)
            if len(actors) > 1:
                nt.actorIds = actors
            await self._apply_surrogate(nt, process_name)
            await self.repo.save_task(nt)
            await self._fire_task_start(inst, nt, node.id, operator)

    # ─── Helpers ──────────────────────────────────────────────────────────────

    async def _load_and_check(self, task_id: int, operator: str):
        task = await self.repo.find_task_by_id(task_id)
        if not task: raise ValueError(f"task not found: {task_id}")
        if task.taskState != TaskState.DOING: raise ValueError("task not doing")
        if not self._is_allowed(task, operator): raise ValueError(f"operator {operator} not allowed")
        inst = await self.repo.find_instance_by_id(task.processInstanceId)
        if not inst: raise ValueError("instance not found")
        return task, inst

    async def _execute_node(self, flow: FlowModel, inst: ProcessInstance, node: FlowNode, operator: str, vars_: dict,
                            parent_id: int = 0):
        # 任务创建（对齐 Java CreateTaskHandler：不触发节点拦截器——创建任务 ≠ 节点执行完成；
        # 任务完成的拦截器由 execute_process_task 显式触发，1.8.0 SYNC 同步演进）
        if node.type == TYPE_TASK:
            await self._create_task(node, inst, operator, vars_,
                                    await self._surrogate_process_name(flow, inst),
                                    parent_id, self._is_first_task_node(flow, node))
            return
        # 记录类节点（snaker:custom）：**不是**任务类，按节点类型分流（issues/141 G9 ·
        # spec 02 §6.1，owner 2026-09-29 裁定「自定义类型这种记录类的，不会有参与人，是正常行为」）
        # ——java 那边 CustomModel 与 TaskModel 是两个平行模型，custom 从不走 CreateTaskHandler。
        if node.type == TYPE_CUSTOM:
            await self._exec_custom_node(flow, inst, node, operator, vars_, parent_id)
            return
        if not await self._fire_pre(node, inst): return
        try:
            if node.type == TYPE_DECISION:
                await self._evaluate_decision(flow, inst, node, operator, vars_, parent_id)
            elif node.type == TYPE_FORK:
                for n in _follow_edges(flow, node.id): await self._execute_node(flow, inst, n, operator, vars_, parent_id)
            elif node.type == TYPE_JOIN:
                if not await self.repo.find_doing_tasks(inst.id):
                    for n in _follow_edges(flow, node.id): await self._execute_node(flow, inst, n, operator, vars_, parent_id)
            elif node.type == TYPE_END:
                # 对齐 Java EndProcessHandler：submitType=REJECT → reject，否则 finish
                submit_type = inst.variables.get(KEY_SUBMIT_TYPE)
                if submit_type is not None and int(submit_type) == int(SubmitType.REJECT):
                    inst.reject(datetime.now())
                else:
                    inst.finish(datetime.now())
                # issues/97：结束节点写回同样排除操作人 u_*（保留发起人 u_*，与 _prepare_execute_task 一致）
                inst.variables = _merge_exec_into_instance(inst.variables, vars_)
                await self.repo.update_instance(inst)
                # PROCESS_INSTANCE_END（码 2）：实例 state 落库之后 fire，载荷带落库后的 state
                # （办结 20 / 拒绝 45 共用一号，spec §11.6 收口旧的 Finish/Reject 拆分）
                await self._fire_event(ProcessEvent(EventType.PROCESS_INSTANCE_END, inst.id,
                                                    operator=operator, state=int(inst.state)))
            else:
                # issues/141 G4 义务 2（spec/02「类型键的三条义务」第 2 条）：**未知档不得静默丢节点**
                # ——类型不在表里时先记一条可诊断日志，再决定跳过。
                #
                # 落点为什么在执行腿而不是解析期：本栈 `parse_flow_model` **不按类型过滤节点**
                # （model.py 里那张表只是常量族，没有 java `ModelParser` 那种"查不到解析器就
                # continue"的解析期丢弃臂），节点一路留在模型里，真正"这个节点什么都不做、
                # 出边也没人走"的决定点就是这里的 if/elif 走完没人认领。⇒ 每次令牌落到该节点
                # 打一条；令牌停在原地不再有后续推进，所以不存在按请求刷屏
                # （反例：挂到 parse_flow_model 上，会跟着每一次发起与每一次办理各打一遍）
                #
                # **为什么必须带实得类型串原文**：spec/02 义务 3 段 owner 二拍「子流程暂不进契约
                # 面」，六栈不补 `snaker:subProcess` 档 ⇒ 设计器画出的子流程节点在本栈唯一的
                # 痕迹就是这条日志。少了 type= 那一半，"snaker:subProcess 被吞了"和"某个手写
                # 的 snaker:Task 拼错大小写被吞了"长得一模一样，那条裁定就没有可诊断面，
                # 等于没立法依据。nodeId 同理：没有它连是哪个节点都找不到。
                #
                # ⚠️ 只记日志，不改行为：跳过形状（不建行、不沿出边推进、令牌停住）已由 issues/143
                # 在 java/php/c# 收口，本栈本来就是"按 id 现查目标、查不到就停"那一派，行为已对。
                if node.type not in KNOWN_NODE_TYPES:
                    logging.warning(
                        "[jeeflow] 流程定义里的节点类型不在类型表里，该节点及其出边将被跳过"
                        "（不建行、不推进，令牌停在此处）: nodeId=%s, type=%s",
                        node.id, node.type)
        finally:
            await self._fire_post(node, inst)

    async def _evaluate_decision(self, flow, inst, node, operator, vars_, parent_id: int = 0):
        # 收集所有出边
        edges = [e for e in flow.edges if e.sourceNodeId == node.id]
        if not edges: return
        # 先尝试表达式求值
        if self.expr_eval:
            for edge in edges:
                expr = edge.properties.get("expr", "")
                if not expr: continue
                result = await self.expr_eval.eval(expr, vars_)
                if _is_truthy(result):
                    target = _find_node(flow, edge.targetNodeId)
                    if target: return await self._execute_node(flow, inst, target, operator, vars_, parent_id)
        # 回退：取第一条没有 expr 的边作为默认路径
        for edge in edges:
            expr = edge.properties.get("expr", "")
            if not expr:
                target = _find_node(flow, edge.targetNodeId)
                if target: return await self._execute_node(flow, inst, target, operator, vars_, parent_id)
        # 最后的回退：取第一条边
        if edges:
            target = _find_node(flow, edges[0].targetNodeId)
            if target: return await self._execute_node(flow, inst, target, operator, vars_, parent_id)

    async def _create_task(self, node: FlowNode, inst: ProcessInstance, operator: str, vars_: dict,
                           process_name: str = "", parent_id: int = 0, is_first: bool = False):
        actors = await self._resolve_actors(node, inst, operator, vars_)
        # issues/141 G5→G9→**硬结论 1**（spec 02 §6.1，owner 2026-09-30 拍；issues/142 §5.3 第 3 问）：
        # **任务类**节点参与者解析为空 ⇒ **照常建这一行 DOING、参与者集合为空**，
        # 不再 `fallback = operator or inst.operator` 兜底挂当前操作人。
        # 上一笔 ``f9f6ca8`` 只按 §6.1 拆干净了**记录类**那一腿，任务类这条腿当时留话"若也适用于
        # 任务类需 owner 另行拍板"——现在拍了：§6.1 硬结论 1 原文「"参与者为空 ⇒ 兜底挂给当前
        # 操作人"这种写法**八栈一律不许有**」不分节点类型。兜底的坏处是**伪造**一条当前操作人
        # 不该收到的待办（他既没被指派、也没申请过这一格，待办列表里却多一条，且他能真办掉）。
        # 旧形状另一半更糟：`if not fallback: return` —— 操作人为空时**既不建行也不推进**，
        # 实例停在 state=10 却零可办行，正是 §6.1 点名的死锁黑洞，一并收掉。
        # 新形状与 java `CreateTaskHandler.handle`（:38-63）**逐字同形**：resolveActors 返回空
        # list 也照样 `instance.createTask(...)` ＋ `execution.addTasks(tasks)`，行建出来、
        # 参与者列就是空集合（java `ProcessTask.create` :63 ⇒ `actorIds = new ArrayList<>()`）。
        # ⚠️ "零参与者"≠"参与者为空就不建单"——要的是**建行且不挂人**（go/node/rust/moon 四栈
        # 现在是"一行不建"那一侧，本栈不许跟过去）。
        # 谁也办不动是**设计如此**：``ProcessTask.is_allowed`` 对空集合恒 False，所以这一行
        # 是"看得见、办不动"的显性堵点（可由 addTaskActor/transfer 补人，或 flow.admin/
        # flow.auto 逃生口办理），比"静默消失＋实例卡死"可诊断得多。
        # 重入安全：本腿只建**一行**、不推进令牌（推进由 execute_process_task 驱动），
        # 零参与者行既过不了 JOIN 的"无 DOING 即前进"判据（它自己就是 DOING，反而卡住 JOIN，
        # 与 java 同形），也不会被会签 merged 分支反复唤醒 ⇒ 不存在自动重入环。
        # performType 容错解析（对齐 Java codeOf，issue 42）：int 优先；
        # 字符串 'ALL'/'COUNTERSIGN'（设计器面板格式，大小写不敏感）映射为会签；未知回落 0
        _pt = node.properties.get("performType", 0)
        try:
            perform_type = int(_pt)
        except (ValueError, TypeError):
            perform_type = 1 if str(_pt).strip().upper() in ("ALL", "COUNTERSIGN") else 0
        ct = node.properties.get("countersignType", "")
        now = datetime.now()
        form = node.properties.get("form", "")
        # issues/126 案 A：节点到期表达式（可能没配）；变量源＝实例变量（boot2 execution.getArgs()）
        expire_expr = node.properties.get("expireTime")
        if perform_type == 1 and ct:
            if ct == "PARALLEL":
                # ``actors or [""]``：零参与者会签同样**必须建一行**（§6.1 表第一行"任务类含会签"），
                # 循环吃空列表 ⇒ 一行不建 = §6.1 禁的死锁形状。这一档 java 是零行
                # （createCountersignTasks 的 for 循环吃空 list），见本轮报告"基准自身缺口"。
                for a in (actors or [""]):
                    nt = inst.create_task(self._next_id(), node.id, node.text.get("value", ""), a, operator, form, now, parent_id, is_first, 1)
                    # 写点③「并行会签全员」：每位成员各算一次（Java createCountersignTasks 并行循环）
                    _apply_expire_time(nt, expire_expr, inst.variables)
                    await self._apply_surrogate(nt, process_name)
                    await self.repo.save_task(nt)
                    # PROCESS_TASK_START（码 3）：任务落库后 fire（会签多任务逐个，对齐 Java CreateTaskHandler）
                    await self._fire_task_start(inst, nt, node.id, operator)
            elif ct == "SEQUENTIAL":
                # 零参与者时 primary 取 ""（⇒ create_task 落**空**参与者集合）：java 这一支是
                # `actorIds.get(0)`，空 list 直接 IndexOutOfBoundsException（基准自身缺口，报告单列），
                # 本栈按 §6.1"任务类零参与者必须建 DOING 行"建一行不挂人，不跟着炸。
                nt = inst.create_task(self._next_id(), node.id, node.text.get("value", ""), actors[0] if actors else "", operator, form, now, parent_id, is_first, 1)
                # 写点②「串行会签首位成员」（Java createCountersignTasks SEQUENTIAL 分支）。
                # 串行会签**推进**出的下一位成员是第五处写点（execute_process_task 的 SEQUENTIAL 分支
                # 里另有一次 _apply_expire_time）：基准侧 boot2 推进时回调 createCountersignTask
                # （ProcessTaskServiceImpl:485/:524）⇒ 首成员与推进出的成员都带到期，不是只有首位。
                _apply_expire_time(nt, expire_expr, inst.variables)
                nt.variables |= {f"operatorList_{node.id}": actors, f"loopCounter_{node.id}": 0, f"nrOfInstances_{node.id}": len(actors)}
                await self._apply_surrogate(nt, process_name)
                await self.repo.save_task(nt)
                await self._fire_task_start(inst, nt, node.id, operator)
            else:
                # 同 PARALLEL 档：零参与者也要落一行，不吃空列表
                for a in (actors or [""]):
                    nt = inst.create_task(self._next_id(), node.id, node.text.get("value", ""), a, operator, form, now, parent_id, is_first, 1)
                    # 未知/比例会签档（RATIO_* 等）＝ Java 的"非 SEQUENTIAL 一律并行全员"循环
                    _apply_expire_time(nt, expire_expr, inst.variables)
                    await self._apply_surrogate(nt, process_name)
                    await self.repo.save_task(nt)
                    await self._fire_task_start(inst, nt, node.id, operator)
        else:
            # 普通任务：一个任务承载全部参与者（对齐 boot3 createTask + addTaskActor，多参与者任一可办）
            # 零参与者档（硬结论 1）：primary 取 "" ⇒ create_task 落**空**参与者集合
            # （model.create_task 对 "" 不再写出 [""] 那种空串归属值，见该函数注释）。
            nt = inst.create_task(self._next_id(), node.id, node.text.get("value", ""), actors[0] if actors else "", operator, form, now, parent_id, is_first)
            # 写点①「普通建单」（Java createTask：本轮把占位 now() 换成按表达式真算）
            _apply_expire_time(nt, expire_expr, inst.variables)
            if len(actors) > 1:
                nt.actorIds = actors
            await self._apply_surrogate(nt, process_name)
            await self.repo.save_task(nt)
            await self._fire_task_start(inst, nt, node.id, operator)

    async def _exec_custom_node(self, flow: FlowModel, inst: ProcessInstance, node: FlowNode,
                                operator: str, vars_: dict, parent_id: int = 0) -> None:
        """记录类节点（``snaker:custom``／带 ``clazz`` 的自定义节点）执行腿
        ——issues/141 G9 · spec 02-flow-definition.md §6.1（owner 2026-09-29 裁定）。

        原话：「这个得根据任务类型来，自定义类型这种记录类的，不会有参与人，是正常行为。」
        ⇒ "参与者解析为空"按节点类型**分判**，本腿是记录类那一半：

        1. 执行 ``clazz``（按名解析处理器，见下）；
        2. 落一条**历史/已完成**行（``task_state=20``，``ProcessInstance.create_history_task``）；
        3. **令牌沿出边继续流转**（流程推进，不卡在这一格）。

        三件禁止的形状（spec §6.1 表列"本轮抓到过实例"），本腿逐条不犯：
        - ① 当任务类建 DOING 行 —— 不建；
        - ② **兜底把行挂给当前操作人伪造一条待办** —— 上一笔 commit ``ac8b557`` 为消
          ``demo_reset_test`` 长期红就是这么修的，本轮按裁定**撤回**；操作人只作为历史行的
          **留痕主体**（``actorIds=[operator]``，java ``createHistoryTask`` 同形），行状态是
          DONE ⇒ 谁也办不动，不在待办里出现；
        - ③ 直接跳过节点不建行（丢留痕）—— 上一版之前的旧形状，也不许回到那里。

        ``clazz`` 的解析形状：java 用反射按 FQCN 实例化（``CustomModel.exec``），python 没有
        JVM 类路径可解析（夹具 ``flows/08-custom-node.json`` 里就是 ``com.mldong...`` 这种
        Java 类名），故与 **C# 栈同策**：集成方 ``HandlerRegistry.register_custom(<clazz 原样串>,
        handler)`` 按名注册，引擎按名解析后调用，返回值非 ``None`` 时写进执行变量的 ``val``
        （缺省键 ``custom_return_val``，对齐 java ``FlowConst.CUSTOM_RETURN_VAL``）。
        ⚠️ **与 java/C# 的一处刻意的栈分歧（owner 2026-09-30 已拍，spec 02 §6.2 第 2 条）**：
        java 与 C# 在"clazz 不可解析"时**显式报错**（``自定义模型[class=...]实例化对象失败``），
        本栈**记 WARNING 日志后照常落历史行＋续流**——因为本栈根本没有"能不能反射到某个 Java 类"
        这件事可判，报错会让任何沿用共享夹具（``08-custom-node.json`` 的 JVM 类名）的流程在
        python 上必然失败，那是**栈限制**不是**语义缺陷**；而"建行＋继续推进"才是 §6.1 要钉的形状。
        §6.2 第 2 条同时把这一条定为本栈形状（原文「python 已经是这个形状（记 WARNING 后继续），
        保持」），java/c# 跟改；要 python 回到报错档需 owner 重新拍。
        ⇒ 附带要求已落实：**"未注册处理器"与"clazz 为空串/缺失"分两档日志**（两条都照常
        落历史行＋续流，只是文案分别可诊断）；处理器**自身执行失败**不在豁免内，照旧外抛。

        不发 ``PROCESS_TASK_START``（码 3）：码 3 表达"新待办产生"，本腿建行即已完成态，
        对齐 java——``persistTasks`` 只对 ``exec.getProcessTaskList()``（新建的 DOING 单）
        走 ``saveNewTask``→``notifyTaskStart``，``createHistoryTask`` 挂的是聚合根 tasks，
        由 ``updateInstance`` 级部落库，不进码 3 那支。"""
        clazz = str(node.properties.get("clazz", "") or "").strip()
        handler = None
        if clazz and self.ext is not None and self.ext.registry is not None:
            handler = self.ext.registry.resolve_custom(clazz)
        if handler is not None:
            # 处理器执行失败一律外抛（对齐 java CustomModel：方法调用失败 ⇒ RuntimeException）
            ret = handler.handle(node, inst, operator, vars_)
            if inspect.isawaitable(ret):
                ret = await ret
            var_key = str(node.properties.get("val", "") or "").strip() or KEY_CUSTOM_RETURN_VAL
            if ret is not None:
                vars_[var_key] = ret
        elif clazz:
            # 档 1「clazz 非空但未注册」：可诊断到**具体类名/处理器名**，集成方一眼看出是
            # 忘了 register_custom 还是名字写错（spec 02 §6.2 第 2 条要求这一档与档 2 分开）。
            logging.warning("[jeeflow] custom 节点 %s 的 clazz=%s 未注册处理器，跳过执行、"
                            "照常落历史行并续流（本栈按名注册：HandlerRegistry.register_custom"
                            "(clazz, handler)）", node.id, clazz)
        else:
            # 档 2「clazz 为空串／属性缺失」：这是**定义配错**（设计器里没填或填了空），
            # 与"填了名字但注册表里没有"是两种病，日志文案必须能分别诊断（§6.2 第 2 条）。
            # 上一版这里只有 `elif clazz:` 一档 ⇒ 空串 clazz 静默无日志，配置错误照不出来。
            logging.warning("[jeeflow] custom 节点 %s 未配置 clazz（属性缺失或空串），跳过执行、"
                            "照常落历史行并续流；流程定义请补 properties.clazz", node.id)
        now = datetime.now()
        ht = inst.create_history_task(self._next_id(), node.id, node.text.get("value", ""),
                                      operator, now, parent_id,
                                      self._is_first_task_node(flow, node))
        await self.repo.save_task(ht)
        # 令牌继续流转（java CustomModel 收尾那句 runOutTransition）
        for n in _follow_edges(flow, node.id):
            await self._execute_node(flow, inst, n, operator, vars_, parent_id)

    async def _apply_surrogate(self, task: ProcessTask, process_name: str) -> None:
        """委托代理自动生效（issues/116 批次 D，引擎内置默认开启）——
        **参与者解析完成后、落库前**把命中的代理人并入 ``task.actorIds``，
        由紧随其后的 ``save_task`` 随任务一起写进 ``wf_process_task_actor``。

        - 不走"事后 ``add_task_actor`` 补写"：Java 首版补写打在 taskId 分配前的空 id 上静默无效
          （06 §4.5 条款 2 ⚠️）。本实现挂在 save_task 之前，taskId 已由 ``create_task`` 分配，
          且代理人直接进落库的参与者集合。
        - 授权人保留（只追加去重，任一可办）。
        - 未接入扩展仓储 / 显式关闭（开关或空实现）→ 静默跳过（``resolve_surrogate_applier`` 返回 None）。
        - 查询异常只记录不外抛：委托是增强能力，不得打断建单（同 ``_fire_event`` 兜底口径）。
        """
        applier = self.ext.resolve_surrogate_applier() if self.ext is not None else None
        if applier is None:
            return
        base = list(task.actorIds) if task.actorIds else ([task.actorId] if task.actorId else [])
        if not base:
            return
        try:
            task.actorIds = await applier.expand(base, process_name, task)
        except Exception:  # noqa: BLE001 —— 建单不被委托查询打断
            logging.exception("[jeeflow] surrogate apply error, keep original actors: task=%s", task.id)

    async def _resolve_actors(self, node: FlowNode, inst: ProcessInstance, operator: str, vars_: dict) -> list[str]:
        # 1. 动态指定下一节点处理人优先（v1.0.1：对齐 boot3 tf_nextNodeOperator）
        #
        # 两形同判据（issues/142 B 批 · spec 06 §2.11 写点表第 3 行）：旧形状这里挂着**第二把尺子**
        # ——逗号串臂 trim＋丢空，而数组臂 `[str(a) for a in next_op]` 不 trim、不丢空、
        # `None` 串化成字符串 "None"（spec 点名的反面正是 java 的 String.valueOf(null)→"null"）。
        # 判据单点只有 `spi.normalize_actors` 那一枚（与 cc 支同一枚，不另抄）。
        # 全空档（[""]／"  "／None）归一成 [] ⇒ 与"没带这个参数"同形 ⇒ 回落下面的 assignee 解析，
        # 绝不拿空数组当有效指派往 actor_id 里灌空值。
        next_op = normalize_actors(vars_.get(KEY_NEXT_NODE_OPERATOR))
        if next_op:
            return next_op
        assignee = node.properties.get("assignee", "")
        if assignee:
            actors = []
            for a in assignee.split(","):
                token = a.strip()
                if not token: continue
                # mldong 契约特殊值：applicant → 流程发起人
                if "applicant" in token:
                    token = token.replace("applicant", inst.operator)
                # token 即变量 key：命中用值（集合展开）、未命中字面量（对齐 boot3 args.get(token, token)）
                if token in vars_:
                    val = vars_[token]
                    if isinstance(val, (list, tuple)):
                        actors.extend(str(x) for x in val)
                    else:
                        actors.append(str(val))
                else:
                    actors.append(token)
            return actors
        handler_name = node.properties.get("assignmentHandler", "")
        if handler_name and self.ext and self.ext.registry:
            h = self.ext.registry.resolve_assignment(handler_name)
            if h: return await h.assign(node, inst, operator)
        if self.ext and self.ext.assignment_handler:
            result = self.ext.assignment_handler(handler_name, node, inst)
            if hasattr(result, '__await__'): return await result
            return result
        return []

    def _is_allowed(self, task: ProcessTask, operator: str) -> bool:
        # v1.0.1：系统代执行（flow.auto）/超级管理员（flow.admin）放行（对齐 boot3 isAllowed）
        if operator and (operator.lower() == KEY_AUTO_ID or operator.lower() == KEY_ADMIN_ID):
            return True
        # 子实体：actorIds 权限判断
        return task.is_allowed(operator)

    async def _add_user_info(self, operator: str, vars_: dict):
        if not self.user_prov: return
        # v1.0.1：系统代执行（flow.auto）/超级管理员（flow.admin）非真实用户，跳过注入（对齐 boot3）
        if operator and (operator.lower() == KEY_AUTO_ID or operator.lower() == KEY_ADMIN_ID):
            return
        u = await self.user_prov.get_user(operator)
        if not u: return
        vars_[KEY_USER_ID] = u.userId
        if u.realName: vars_[KEY_REAL_NAME] = u.realName
        if u.deptId: vars_[KEY_DEPT_ID] = u.deptId
        if u.deptName: vars_[KEY_DEPT_NAME] = u.deptName
        if u.postId: vars_[KEY_POST_ID] = u.postId
        if u.postName: vars_[KEY_POST_NAME] = u.postName

    def _add_auto_gen_title(self, display_name: str, vars_: dict):
        """issue 29：自动生成标题（对齐 boot3 FlowUtil.addAutoGenTitle）"""
        real_name = vars_.get(KEY_REAL_NAME, "")
        title = f"{real_name}的{display_name}-{datetime.now().strftime('%Y-%m-%d %H:%M')}"
        vars_[KEY_AUTO_GEN_TITLE] = title

    def _next_id(self) -> int:
        if self.id_gen: return self.id_gen.next_id()
        return int(time.time() * 1000) + random.randint(0, 999)

    # ─── Extensions ───────────────────────────────────────────────────────────

    async def _fire_pre(self, node, inst) -> bool:
        if not self.ext: return True
        for ic in sorted(await self._resolve_interceptors(inst), key=lambda x: x.order):
            if not await ic.pre_handle(node, inst): return False
        return True

    async def _fire_post(self, node, inst):
        if not self.ext: return
        for ic in sorted(await self._resolve_interceptors(inst), key=lambda x: x.order, reverse=True):
            await ic.post_handle(node, inst)

    async def _resolve_interceptors(self, inst) -> list:
        """定义级拦截器解析（issue 34，对齐 Java 模型级 postInterceptors）：
        流程定义顶层 postInterceptors 声明 → 按名从 interceptor_registry 取（未声明该流程不触发）；
        未声明 → 回落引擎级列表（向后兼容现状）。结果按 defineId 缓存。
        issues/60：解析与校验分离——定义读取/JSON 解析失败回落引擎级（现状语义），
        声明中存在未注册名时抛 ValueError（不静默跳过），且错误不写缓存保证持续报错。"""
        if not self.ext:
            return []
        define_id = getattr(inst, "defineId", None)
        if define_id is None:
            return list(self.ext.interceptors)
        cached = self._ic_cache.get(define_id)
        if cached is not None:
            return cached
        ic_list = list(self.ext.interceptors)
        declared = None
        try:
            def_ = await self.repo.find_define_by_id(define_id)
            if def_ is not None:
                content = def_.content
                meta = json.loads(content) if isinstance(content, str) else json.loads(content.decode("utf-8"))
                declared = str(meta.get("postInterceptors") or "").strip()
        except Exception:
            pass
        if declared:
            ic_list = []
            for name in declared.split(","):
                name = name.strip()
                if not name:
                    continue
                if name not in (self.ext.interceptor_registry or {}):
                    raise ValueError(f"postInterceptors 声明的拦截器未注册: {name}")
                ic_list.append(self.ext.interceptor_registry[name])
        self._ic_cache[define_id] = ic_list
        return ic_list

    async def fire_event(self, evt: ProcessEvent):
        """公开事件发布入口（issues/102）：门面层的**转办/撤回**等非引擎执行链内的事实经此 fire。
        ⚠️ 抄送（CC_CREATE）**不走这里**——已由引擎侧 ``handle_cc_actors`` 漏斗在 cc 行落库后
        自行 fire（spec §11.7），门面不得再自己补发（§11.1 禁止态）。
        零监听器时安全返回（spec §11.5「无监听器」行）。"""
        await self._fire_event(evt)

    def add_event_listener(self, listener) -> "EngineImpl":
        """追加事件监听器（spec 11-events §11.5 列表基线）：**追加不覆盖**。

        旧形状 ``EngineExtensions(event_listener=...)`` 仍然有效（解析时排在最前），
        三壳升级到多监听器时可以逐壳迁移，不需要一次改完。
        ``ext`` 未建时补建一个空扩展体（零配置场景注册监听器不该报错）。
        """
        if self.ext is None:
            self.ext = EngineExtensions()
        self.ext.add_event_listener(listener)
        return self

    async def _fire_task_start(self, inst: ProcessInstance, task: ProcessTask,
                               node_id: str, operator: str) -> None:
        """PROCESS_TASK_START（码 3）：任务行落库后**逐任务** fire（spec §11.3 触发时机列）。

        每个调用点都紧随 ``save_task``（taskId 已分配、行已可见），监听器可按 taskId 反查；
        载荷必备键 instanceId/taskId/actors——actors 取落库那份参与者集合（含委托并入的代理人）。
        """
        actors = list(task.actorIds) if task.actorIds else ([task.actorId] if task.actorId else [])
        await self._fire_event(ProcessEvent(EventType.PROCESS_TASK_START, inst.id, task.id,
                                            node_id, operator, actors=actors))

    async def _fire_event(self, evt: ProcessEvent):
        """事件派发（spec 11-events §11.5）：一次 fire 送达**全部**已注册监听器，
        注册顺序＝回调顺序；**逐监听器** catch——单个监听器抛异常只记日志，
        ① 不回滚主流程，② 不中断后续监听器（issues/104 P2 八栈统一口径）。
        零监听器时直接返回（不得空指针）。"""
        if self.ext is None:
            return
        listeners = self.ext.resolve_event_listeners()
        for idx, listener in enumerate(listeners):
            try:
                result = listener(evt)
                if hasattr(result, '__await__'):
                    await result
            except Exception:  # noqa: BLE001 —— 引擎侧兜底：异常不外溢、不中断后续监听器
                logging.exception("[jeeflow] process event listener #%d error: type=%s code=%s",
                                  idx, evt.type.name, int(evt.type))

# ─── Pure Functions ─────────────────────────────────────────────────────────────

def _can_rejected(flow: FlowModel, current_id: str, parent_id: str) -> bool:
    """照 mldong-boot2 NodeModel.canRejected：自 current 的入边回溯，命中 parent 放行；
    入边来源是 fork/join/start 时**跳过该条入边、不再深入**（boot2 是 continue，不是穿越），
    其余来源递归。subprocess 在 boot2 里被注释掉，等同普通节点。"""
    for edge in flow.edges:
        if edge.targetNodeId != current_id:
            continue
        if edge.sourceNodeId == parent_id:
            return True
        src = _find_node(flow, edge.sourceNodeId)
        if src is None:
            continue
        if src.type in (TYPE_FORK, TYPE_JOIN, TYPE_START):
            continue
        if _can_rejected(flow, src.id, parent_id):
            return True
    return False


def _lineage_vars(src: dict) -> dict:
    """复活行的变量净化：剔控制类残留，保留 f_*/u_*/autoGenTitle/isFirstTaskNode。"""
    out = {}
    for k, v in (src or {}).items():
        if (k in ("submitType", "taskName")
                or k.startswith("tf_") or k.startswith("csv_")
                or k.startswith("loopCounter") or k.startswith("nrOfInstances")
                or k.startswith("operatorList")):
            continue
        out[k] = v
    return out

def _find_node(flow: FlowModel, id: str) -> Optional[FlowNode]:
    return next((n for n in flow.nodes if n.id == id), None)

def _find_by_type(flow: FlowModel, typ: str) -> Optional[FlowNode]:
    return next((n for n in flow.nodes if n.type == typ), None)

def _follow_edges(flow: FlowModel, source_id: str) -> list[FlowNode]:
    return [_find_node(flow, e.targetNodeId) for e in flow.edges if e.sourceNodeId == source_id and _find_node(flow, e.targetNodeId)]

def _sync_task_to_aggregate(inst: ProcessInstance, task: ProcessTask):
    """把外部任务对象的最新状态同步回聚合根任务副本
    （v1.0.1：update_instance 级联持久化依赖聚合内任务副本为最新状态）"""
    for i, t in enumerate(inst.tasks):
        if t.id == task.id:
            inst.tasks[i] = task
            return

def _merge_exec_into_instance(base: dict, exec_vars: dict) -> dict:
    """实例变量写回合并（issues/97 对齐 Java）：以 base（start 注入的发起人 u_*）为底，
    并入执行上下文中**非 u_*** 键（f_ 表单字段 / submitType 等流转数据）。
    add_user_info 生成的操作人 u_* 只属于当次执行上下文与任务行 ext，不整体写回实例——
    实例 u_realName 语义是「发起人」（与 autoGenTitle 一致），不随审批节点漂移。"""
    out = dict(base)
    for k, v in exec_vars.items():
        if k.startswith("u_"):
            continue
        out[k] = v
    return out

def _get_cs_state(vars_: dict, node_id: str):
    actors = vars_.get(f"operatorList_{node_id}")
    lc = int(vars_.get(f"loopCounter_{node_id}", 0))
    return actors, lc

def _is_truthy(v) -> bool:
    if isinstance(v, bool): return v
    if isinstance(v, str): return v not in ("", "false")
    if v is None: return False
    if isinstance(v, (int, float)): return v != 0
    return True


def _filter_field_by_perm(args: dict, node: Optional[FlowNode]) -> dict:
    """办理提交的 f_ 字段按任务节点 field 权限过滤（issues/26）——
    任务节点 properties.field 声明 PERMISSION_f_{全名}（前端约定，优先）或
    PERMISSION_{去前缀名}（兼容）的字段，值非 EDIT(2)（只读 1/隐藏 3 等）→ 剔除不入变量。
    键格式双兼容（issues/25），与 persist 拦截器 _is_editable 同契约。"""
    if not args or node is None or node.type not in (TYPE_TASK, TYPE_CUSTOM):
        return args
    field_perm = node.properties.get("field") if node.properties else None
    if not isinstance(field_perm, dict) or not field_perm:
        return args
    out = {}
    for k, v in args.items():
        if k.startswith("f_") and len(k) > 2:
            name = k[2:]
            perm = field_perm.get(f"PERMISSION_f_{name}")
            if perm is None:
                perm = field_perm.get(f"PERMISSION_{name}")
            if perm is not None and int(perm) != 2:
                continue  # 只读/隐藏：剔除（不入变量）
        out[k] = v
    return out


# ─── 到期时间求值（issues/126 案 A · 逐字对齐 Java FlowUtil.processTime）───────────

#: 绝对时刻格式（Java ``SimpleDateFormat("yyyy-MM-dd HH:mm:ss")``，只此一种）
_EXPIRE_LAYOUT = "%Y-%m-%d %H:%M:%S"
#: 相对档前缀必须是整数（Java ``Integer.parseInt`` 的接受面；按 issues/137 E 裁掉**两端空白**之后，
#: ``"1_0"`` 这类带下划线/小数的串仍不算）。
#: ``[+-]?`` 是**故意**留符号位的（issues/137 D 只裁负不裁加号）：负数在 :func:`process_time`
#: 里 ``int()`` **之后**判掉 ⇒ 落穿档 3，正则本身不收窄，免得把 '+' 裁成新的一处跨栈分叉。
#: ⚠️ 正则里**不塞** ``\s*``：空白由 :func:`process_time` 在切片上做 ``.strip()`` 处理，
#: 位置只在「判整数之前」，这样 137 D 的判负点（``offset >= 0``）不必跟着挪。
_INT_PREFIX = re.compile(r"[+-]?[0-9]+")
_UNIT_BY_SUFFIX = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}


def process_time(expr: Optional[str], args: Optional[dict]) -> Optional[datetime]:
    """解析节点配的「期待完成时间」表达式 —— **三档顺序不可换**（Java ``FlowUtil.processTime``，
    jeeflow-java ``d9e9397`` 同批的参考实现）。

    1. **变量档**：``args`` 里存在键名 == ``expr`` 原串的项 ⇒ 取该项的值：
       ``datetime`` → 该时刻；``int`` 毫秒时间戳 → 本地该时刻（Java ``new Date(long)`` + 系统默认时区）；
       ``str`` → 按 ``"yyyy-MM-dd HH:mm:ss"`` 解析，解析失败 → ``None``。
       值类型不认识（``list`` / ``dict`` / ``float`` / ``bool`` …）⇒ **落穿**到档 2/3，
       不是提前返回 ``None``（Java/C# 都是落穿，改成 return None 就是跨栈分叉）。
    2. **相对档**：``expr`` 以 ``s|m|h|d`` 结尾且前缀是**非负**整数 ⇒ 当前时间 + N 秒/分/时/天。
       ``d`` 走**日历加天**（Java ``Calendar.add(DAY_OF_MONTH)``），不乘 86400 秒
       ——本栈写 ``createTime`` 用的是 naive 本地钟，naive ``datetime + timedelta(days=n)``
       即名义（墙上时钟）加天，与 ``Calendar`` 同档。
       前缀为负（``-5h`` / ``-5d``）按**不合法**处理，与坏前缀一样落穿档 3 ⇒ ``None``
       （issues/137 D · owner 2026-10-01 拍"判非负"：放行负偏移＝建单即逾期）；带 ``'+'`` 的前缀
       照旧合法（只裁负不裁加号）。
       前缀**允许两端空白**（issues/137 E · owner 2026-10-01 拍"统一 trim"）：判整数**之前**裁掉
       ``expr[:-1]`` 的两端空白，裁完再走上那条非负判定 —— ``" 2h"`` / ``"2 h"``（空格落在前缀区内、
       末位仍是单位符）照样算得出。裁的边界**只到前缀**：``"2h "`` 的末位是空格、认不出单位 ⇒ 仍按误配
       落穿；``" 2.5h"`` 裁完仍是小数 ⇒ 仍落穿。变量档与绝对档的串本身**不 trim**。
    3. **绝对档**：把 ``expr`` 本身按 ``"yyyy-MM-dd HH:mm:ss"`` 解析 → 时刻；失败 → ``None``。

    **任何一档都不得返回 now()**：没配 / 解析不出 ⇒ ``None``（这一列留 NULL）。
    这条是本卡的红线——占位写法 ``expire = now`` 让"配了到期表达式的节点"建单即逾期，
    逾期统计因此全失真（issues/126 病灶形状）。

    ⚠️ 档 2「以 s/m/h/d 结尾但前缀不是整数」（``xh``／``2.5h``／``3hh``）**落穿到档 3、最终 None**，
    这是**八栈一致**口径（issues/137 C · 卡面 §1.5 的读法「以 s/m/h/d 结尾**且前缀是整数**」）：
    错配一个到期表达式不该让流程起不来，且 ``None`` 正是「解析失败」的既定方向。
    旧文那句「Java 在档 2 由 ``Integer.parseInt`` 抛 NumberFormatException **打断建单**、本栈是有意差异」
    **已过期**——java 参考实现已改成同款落穿（jeeflow-java commit ``6bdf41b``，随 1.8.36 发出）。
    """
    if expr is None:
        return None
    # ── 档 1：变量档（优先于相对档：args 里真有个键叫 "2h" 时取的是变量值，不是 now+2h）
    if isinstance(args, dict) and expr in args:
        value = args[expr]
        if isinstance(value, datetime):
            return value
        if isinstance(value, int) and not isinstance(value, bool):
            try:
                return datetime.fromtimestamp(value / 1000)
            except (OverflowError, OSError, ValueError):
                return None      # 超出可表示范围 = 解析失败 ⇒ NULL
        if isinstance(value, str):
            try:
                return datetime.strptime(value, _EXPIRE_LAYOUT)
            except ValueError:
                return None      # Java 此档同样直接 return null，**不落穿**
        # 其它类型 ⇒ 落穿
    if not expr:
        return None
    suffix = expr[-1]
    # issues/137 E（owner 2026-10-01 拍"统一 trim" · spec 04 §「相对档前缀允许两端空白」）：
    # **判整数之前**先裁掉前缀的两端空白，裁完才走 137 D 那条"非负"判定（基准＝jeeflow-java ``bf1f401``）。
    # 为什么要显式裁：各栈整数解析对空白的容忍度天然不同 —— go 在 ``Atoi`` 前 ``TrimSpace``、rust
    # ``.trim()``、.NET ``TryParse`` 与 python ``int()`` 默认就收前后空白，而 java ``Integer.parseInt(" 2")``
    # 偏偏抛 ⇒ 不裁就是"同一份流程定义在别家有到期时间、这一家没有"（到期表达式是设计器手填 / JSON
    # 搬运的字符串，夹一个空格是常态）。
    # 三条分界（本栈靠"只裁 ``expr[:-1]``、``suffix`` 原样"这一处形状天然成立）：
    #   ① ``" 2h"`` / ``"2 h"``：空格落在**前缀区内**、末位仍是单位符 ⇒ 裁完照样算出 now+2h；
    #   ② ``"2h "``：单位符后面还带空白 ⇒ ``suffix`` 是空格、认不出单位 ⇒ 按误配**落穿**到档 3 ⇒ None。
    #      裁的边界只到前缀：把整串去空白（``expr.strip()`` 再判末位）是另一件没立过法的事，不许顺手做；
    #   ③ ``" 2.5h"``：trim 之后仍是小数误配 ⇒ ``fullmatch`` 照样不收 ⇒ 仍落穿 —— trim ≠ "裁容错"。
    # 判负（137 D）位置不动、加了 trim 也照旧生效：``" -5h"`` 裁完是 ``-5`` ⇒ 下面 ``offset >= 0`` 拦下 ⇒ None。
    # ⚠️ 变量档（上面 ``expr in args``）与绝对档（下面 ``strptime(expr, ...)``）**都不 trim 串本身**：
    #    那是键名 / 时间串本身，裁它改的是另一件事（键名带空格就该取不到值、按既有落穿路径走）。
    # 正则 ``_INT_PREFIX`` 一个字不动（不往 pattern 里塞 ``\s*``）：fullmatch 与 ``int()`` 必须吃
    # **同一个**裁过的切片，否则"认得出但转不了"或反之，两处判据就分叉了。
    prefix = expr[:-1].strip()
    if suffix in _UNIT_BY_SUFFIX and _INT_PREFIX.fullmatch(prefix):
        offset = int(prefix)
        # issues/137 D（owner 2026-10-01 拍"判非负"）：**负数前缀同样算不合法** —— 不进档 2，
        # 沿本函数既有的落穿路径继续走档 3 ⇒ 仍解析不出即 ``None``（这一列留 NULL）。
        # 两条理由逐字对齐 java ``FlowUtil.parseIntOrNull``：
        #   1. **任何一档都不许退化成"取当前时间"**：放行 ``-5h`` 算出的是一个**过去**的时刻 ⇒
        #      新建的行当场就逾期，比"没配到期时间"更难发现，正是 issues/126 占位 ``now()``
        #      病灶的同型形状。
        #   2. **只裁负、不裁加号**：判负发生在 ``int()`` **之后**，``_INT_PREFIX`` 的 ``[+-]?``
        #      原样保留。各栈整数解析（python ``[+-]?``、node ``[-+]?\d+``、php ``[+-]?\d{1,18}``、
        #      go 的 ``Atoi``）都收 ``'+'``，把加号一并裁掉反而新造一处跨栈分叉——``+2h`` 照旧是
        #      合法的 now+7200s。
        # 判点覆盖面：四档 s/m/h/d 共用这一处前缀解析（单位靠 ``_UNIT_BY_SUFFIX`` 查同一张表、
        # 复用同一个 ``offset``，``d`` 档没有另开加天数分支）⇒ 这一判同时拦住四档。
        if offset >= 0:
            # 本栈写 createTime 用 naive 本地钟 ⇒ naive datetime + timedelta 是**名义（墙上时钟）加量**：
            # "d" 这一档即日历加天，与 Java Calendar.add(DAY_OF_MONTH) 同形，不是乘 86400 秒的瞬时加法
            return datetime.now() + timedelta(**{_UNIT_BY_SUFFIX[suffix]: offset})
    try:
        return datetime.strptime(expr, _EXPIRE_LAYOUT)
    except ValueError:
        return None


def _apply_expire_time(target: Any, expr: Any, args: Optional[dict]) -> None:
    """到期时间**唯一**写入口：任务行建单五写点 + 实例行发起写点共用同一把尺子
    （对齐 Java ``ProcessInstance.applyExpireTime`` 两个重载 +
    ``cb541d4`` 为绕过 createTask 直建行那一支开的公开入口 ``applyNodeExpireTime``）。

    - ``target`` 是带 ``expireTime`` 槽的行对象（``ProcessTask`` / ``ProcessInstance``）。
    - ``expr`` 任务级取节点属性 ``properties.expireTime``、实例级取**流程定义顶层**
      ``FlowModel.expireTime``（两处都是设计器 JSON 里的表达式原串，Java ``TaskParser`` 的
      ``EXPIRE_TIME_KEY`` 同名键）；非串按 Java ``getStr`` 归一为串。
    - **没配 ⇒ 这一列保持 NULL**（owner 2026-09-28 口径：不造默认值，不写 now()/''/0）。
      空白串（``"   "``）不算"没配"而是照 Java 交给 :func:`process_time` 求值 ⇒ 结果 ``None``。
    - ``args`` = 变量源两档：**建单四处＝实例变量**（普通建单 / 串行首位 / 并行全员 /
      串行推进出的下一位；boot2 的 ``execution.getArgs()``），
      **回退新建＝随行拷贝那份**（boot2 的 ``hisVariable``）。搞混这两档，
      "表达式是个变量名"这一格会跨栈给出不同答案。
      实例级写点用**发起参数**（已注入用户信息与 autoGenTitle 的那份 ``vars_``），
      基准＝Java ``JeeflowEngineImpl:93-96`` 就地改过的 ``args``。
    """
    if target is None or expr is None:
        return
    if not isinstance(expr, str):
        expr = str(expr)
    if not expr:
        return
    target.expireTime = process_time(expr, args if isinstance(args, dict) else {})
