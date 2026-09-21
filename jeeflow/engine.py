"""引擎核心——对标 Java EngineImpl"""
import logging
import json, time, random
from datetime import datetime
from typing import Any, Optional
from .model import (
    FlowModel, FlowNode, FlowEdge,
    TYPE_START, TYPE_END, TYPE_TASK, TYPE_DECISION, TYPE_FORK, TYPE_JOIN, TYPE_CUSTOM,
    ProcessInstance, ProcessTask, ProcessDefine,
    InstanceState, TaskState, SubmitType, PerformType,
    parse_flow_model,
)
from .spi import ProcessRepository, UserProvider, IDGenerator, ExpressionEvaluator
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

class Engine:
    """引擎接口"""

    async def start_process_instance_by_id(self, define_id: int, operator: str, args: dict[str, Any] = None) -> ProcessInstance: ...
    async def execute_process_task(self, task_id: int, operator: str, args: dict[str, Any] = None) -> ProcessInstance: ...
    async def execute_and_jump_to_end(self, task_id: int, operator: str, args: dict[str, Any] = None) -> ProcessInstance: ...
    async def execute_and_jump_task(self, task_id: int, operator: str, args: dict[str, Any] = None, target_task_name: str = None) -> ProcessInstance: ...
    async def execute_and_jump_to_first_task_node(self, task_id: int, operator: str, args: dict[str, Any] = None) -> ProcessInstance: ...

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
        await self.repo.save_instance(inst)
        await self._fire_event(ProcessEvent(type=EventType.PROCESS_START, instanceId=inst.id, operator=operator))
        start_node = _find_by_type(flow, TYPE_START)
        if not start_node: raise ValueError("no start node")
        for node in _follow_edges(flow, start_node.id):
            await self._execute_node(flow, inst, node, operator, vars_)
        return await self.repo.find_instance_by_id(inst.id)

    # ─── Execute ──────────────────────────────────────────────────────────────

    async def execute_process_task(self, task_id: int, operator: str, args: dict[str, Any] = None) -> ProcessInstance:
        task, inst, flow, vars_ = await self._prepare_execute_task(task_id, operator, args)
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
                                              actors[lc + 1], operator, cur_node.properties.get("form", ""), now, 1)
                        nt.variables = {f"operatorList_{cur_node.id}": actors, f"loopCounter_{cur_node.id}": lc + 1,
                                        f"nrOfInstances_{cur_node.id}": len(actors)}
                        await self._apply_surrogate(nt, await self._surrogate_process_name(flow, inst))
                        await self.repo.save_task(nt)
                        # TASK_CREATE：顺序会签推进新任务落库后 fire（对齐 Java CreateTaskHandler）
                        await self._fire_event(ProcessEvent(EventType.TASK_CREATE, inst.id, nt.id, cur_node.id, operator))
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
                await self._execute_node(flow, inst, node, operator, vars_)
        return await self.repo.find_instance_by_id(inst.id)

    # ─── Reject ───────────────────────────────────────────────────────────────

    async def execute_and_jump_to_end(self, task_id: int, operator: str, args: dict[str, Any] = None) -> ProcessInstance:
        _, inst, _, _ = await self._prepare_execute_task(task_id, operator, args)
        # 门面 submitType=2 REJECT 唯一入口（对齐 Java executeAndJumpToEnd 语义）
        inst.reject(datetime.now())
        await self.repo.update_instance(inst)
        await self._fire_event(ProcessEvent(EventType.PROCESS_REJECT, inst.id, task_id, operator=operator))
        return await self.repo.find_instance_by_id(inst.id)

    # ─── Jump（ROLLBACK 空 target / JUMP 命名 target，boot2 executeAndJumpTask）──

    async def execute_and_jump_task(self, task_id: int, operator: str, args: dict[str, Any] = None,
                                     target_task_name: str = None) -> ProcessInstance:
        task, inst, flow, vars_ = await self._prepare_execute_task(task_id, operator, args)
        if not target_task_name:
            # issues/79：ROLLBACK 对齐 Java rejectTask——退回上一任务节点（首条输入边 source），
            # 新任务 actor=当前任务完成人（退回操作人）；无上一任务节点则不产生新待办
            prev_name = self._previous_task_name(flow, task.taskName)
            if prev_name:
                prev = _find_node(flow, prev_name)
                if prev:
                    actors = self._rollback_actors(prev, inst, operator, task)
                    await self._create_task_with_actors(prev, inst, operator, vars_, actors,
                                                        await self._surrogate_process_name(flow, inst))
        else:
            # issues/79：对齐 Java——目标节点不存在显式报错（前端 JUMP 无效 taskName 不再静默空操作）
            target = _find_node(flow, target_task_name)
            if target is None:
                raise ValueError(f"根据节点名称[{target_task_name}]无法找到节点模型")
            # 对齐 Java isFirstTaskName：跳首任务节点（start 直接后继）assignee 强制为发起人
            if target.type == TYPE_TASK and self._is_first_task_node(flow, target):
                target.properties["assignee"] = inst.operator
            await self._execute_node(flow, inst, target, operator, vars_)
        return await self.repo.find_instance_by_id(inst.id)

    # ─── Jump To First Task（退回发起人，boot2 ROLLBACK_TO_OPERATOR=6）───────

    async def execute_and_jump_to_first_task_node(self, task_id: int, operator: str,
                                                   args: dict[str, Any] = None) -> ProcessInstance:
        _, inst, flow, vars_ = await self._prepare_execute_task(task_id, operator, args)
        # 找到第一个任务节点，强制参与者为发起人，重新执行
        start_node = _find_by_type(flow, TYPE_START)
        if start_node:
            for node in _follow_edges(flow, start_node.id):
                if node.type in (TYPE_TASK, TYPE_CUSTOM):
                    node.properties["assignee"] = inst.operator
                    await self._execute_node(flow, inst, node, operator, vars_)
                    break
        return await self.repo.find_instance_by_id(inst.id)

    # ─── Execute 公共序言（对齐 Java prepareExecution）────────────────────────

    async def _prepare_execute_task(self, task_id: int, operator: str, args: dict[str, Any]):
        """执行公共序言（对齐 Java prepareExecution）：权限校验 → f_ 字段权限过滤 →
        完成任务（子实体状态转换 + 实例变量合并，经 update_instance 级联落库）→
        返回流程模型 + 合并后执行变量。Java jump 路径不废弃其余 DOING 任务
        （会签兄弟任务不受影响），此处保持一致。"""
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
        await self._fire_event(ProcessEvent(EventType.TASK_COMPLETE, inst.id, task.id, task.taskName, operator))
        # issues/97：实例变量写回排除操作人 u_*，保留 start 注入的发起人 u_*（u_realName 恒为发起人）
        inst.variables = _merge_exec_into_instance(base_vars, vars_)
        await self.repo.update_instance(inst)
        return task, inst, flow, vars_

    def _previous_task_name(self, flow: FlowModel, task_name: str) -> str:
        """当前任务节点的首条输入边 source（issues/79 对齐 Java getPreviousTaskName）"""
        node = _find_node(flow, task_name)
        if node is None:
            return ""
        for edge in flow.edges:
            if edge.targetNodeId == node.id:
                src = _find_node(flow, edge.sourceNodeId)
                if src is not None and src.type in (TYPE_TASK, TYPE_CUSTOM):
                    return src.id
        return ""

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
        next_op = inst.variables.get(KEY_NEXT_NODE_OPERATOR)
        if next_op:
            if isinstance(next_op, str):
                return [a.strip() for a in next_op.split(",") if a.strip()]
            if isinstance(next_op, (list, tuple)):
                return [str(a) for a in next_op]
            return [str(next_op)]
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
                                        vars_: dict, actors: list[str], process_name: str = ""):
        """以显式参与者建任务（会签节点拆分为逐人任务，对齐 Java 会签创建语义）；
        建单前同样应用委托（issues/116：任何新任务都是"建单那一刻"）"""
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
                    nt = inst.create_task(self._next_id(), node.id, node.text.get("value", ""), a, operator, form, now, 1)
                    await self._apply_surrogate(nt, process_name)
                    await self.repo.save_task(nt)
                    await self._fire_event(ProcessEvent(EventType.TASK_CREATE, inst.id, nt.id, node.id, operator))
            elif ct == "SEQUENTIAL":
                nt = inst.create_task(self._next_id(), node.id, node.text.get("value", ""), actors[0], operator, form, now, 1)
                nt.variables = {f"operatorList_{node.id}": actors, f"loopCounter_{node.id}": 0, f"nrOfInstances_{node.id}": len(actors)}
                await self._apply_surrogate(nt, process_name)
                await self.repo.save_task(nt)
                await self._fire_event(ProcessEvent(EventType.TASK_CREATE, inst.id, nt.id, node.id, operator))
            else:
                for a in actors:
                    nt = inst.create_task(self._next_id(), node.id, node.text.get("value", ""), a, operator, form, now, 1)
                    await self._apply_surrogate(nt, process_name)
                    await self.repo.save_task(nt)
                    await self._fire_event(ProcessEvent(EventType.TASK_CREATE, inst.id, nt.id, node.id, operator))
        else:
            nt = inst.create_task(self._next_id(), node.id, node.text.get("value", ""), actors[0], operator, form, now)
            if len(actors) > 1:
                nt.actorIds = actors
            await self._apply_surrogate(nt, process_name)
            await self.repo.save_task(nt)
            await self._fire_event(ProcessEvent(EventType.TASK_CREATE, inst.id, nt.id, node.id, operator))

    # ─── Helpers ──────────────────────────────────────────────────────────────

    async def _load_and_check(self, task_id: int, operator: str):
        task = await self.repo.find_task_by_id(task_id)
        if not task: raise ValueError(f"task not found: {task_id}")
        if task.taskState != TaskState.DOING: raise ValueError("task not doing")
        if not self._is_allowed(task, operator): raise ValueError(f"operator {operator} not allowed")
        inst = await self.repo.find_instance_by_id(task.processInstanceId)
        if not inst: raise ValueError("instance not found")
        return task, inst

    async def _execute_node(self, flow: FlowModel, inst: ProcessInstance, node: FlowNode, operator: str, vars_: dict):
        # 任务创建（对齐 Java CreateTaskHandler：不触发节点拦截器——创建任务 ≠ 节点执行完成；
        # 任务完成的拦截器由 execute_process_task 显式触发，1.8.0 SYNC 同步演进）
        if node.type in (TYPE_TASK, TYPE_CUSTOM):
            await self._create_task(node, inst, operator, vars_,
                                    await self._surrogate_process_name(flow, inst))
            return
        if not await self._fire_pre(node, inst): return
        try:
            if node.type == TYPE_DECISION:
                await self._evaluate_decision(flow, inst, node, operator, vars_)
            elif node.type == TYPE_FORK:
                for n in _follow_edges(flow, node.id): await self._execute_node(flow, inst, n, operator, vars_)
            elif node.type == TYPE_JOIN:
                if not await self.repo.find_doing_tasks(inst.id):
                    for n in _follow_edges(flow, node.id): await self._execute_node(flow, inst, n, operator, vars_)
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
                await self._fire_event(ProcessEvent(EventType.PROCESS_FINISH, inst.id, operator=operator))
        finally:
            await self._fire_post(node, inst)

    async def _evaluate_decision(self, flow, inst, node, operator, vars_):
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
                    if target: return await self._execute_node(flow, inst, target, operator, vars_)
        # 回退：取第一条没有 expr 的边作为默认路径
        for edge in edges:
            expr = edge.properties.get("expr", "")
            if not expr:
                target = _find_node(flow, edge.targetNodeId)
                if target: return await self._execute_node(flow, inst, target, operator, vars_)
        # 最后的回退：取第一条边
        if edges:
            target = _find_node(flow, edges[0].targetNodeId)
            if target: return await self._execute_node(flow, inst, target, operator, vars_)

    async def _create_task(self, node: FlowNode, inst: ProcessInstance, operator: str, vars_: dict,
                           process_name: str = ""):
        actors = await self._resolve_actors(node, inst, operator, vars_)
        if not actors: return
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
        if perform_type == 1 and ct:
            if ct == "PARALLEL":
                for a in actors:
                    nt = inst.create_task(self._next_id(), node.id, node.text.get("value", ""), a, operator, form, now, 1)
                    await self._apply_surrogate(nt, process_name)
                    await self.repo.save_task(nt)
                    # TASK_CREATE：任务落库后 fire（会签多任务逐个，对齐 Java CreateTaskHandler）
                    await self._fire_event(ProcessEvent(EventType.TASK_CREATE, inst.id, nt.id, node.id, operator))
            elif ct == "SEQUENTIAL":
                nt = inst.create_task(self._next_id(), node.id, node.text.get("value", ""), actors[0], operator, form, now, 1)
                nt.variables = {f"operatorList_{node.id}": actors, f"loopCounter_{node.id}": 0, f"nrOfInstances_{node.id}": len(actors)}
                await self._apply_surrogate(nt, process_name)
                await self.repo.save_task(nt)
                await self._fire_event(ProcessEvent(EventType.TASK_CREATE, inst.id, nt.id, node.id, operator))
            else:
                for a in actors:
                    nt = inst.create_task(self._next_id(), node.id, node.text.get("value", ""), a, operator, form, now, 1)
                    await self._apply_surrogate(nt, process_name)
                    await self.repo.save_task(nt)
                    await self._fire_event(ProcessEvent(EventType.TASK_CREATE, inst.id, nt.id, node.id, operator))
        else:
            # 普通任务：一个任务承载全部参与者（对齐 boot3 createTask + addTaskActor，多参与者任一可办）
            nt = inst.create_task(self._next_id(), node.id, node.text.get("value", ""), actors[0], operator, form, now)
            if len(actors) > 1:
                nt.actorIds = actors
            await self._apply_surrogate(nt, process_name)
            await self.repo.save_task(nt)
            await self._fire_event(ProcessEvent(EventType.TASK_CREATE, inst.id, nt.id, node.id, operator))

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
        next_op = vars_.get(KEY_NEXT_NODE_OPERATOR)
        if next_op:
            if isinstance(next_op, str):
                return [a.strip() for a in next_op.split(",") if a.strip()]
            if isinstance(next_op, (list, tuple)):
                return [str(a) for a in next_op]
            return [str(next_op)]
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
        """公开事件发布入口（issues/102）：facade 层 CC 创建后逐抄送人 fire CC_CREATE；
        无监听器（ext/event_listener 为空）时零副作用，与上一版逐字节一致"""
        await self._fire_event(evt)

    async def _fire_event(self, evt: ProcessEvent):
        if self.ext and self.ext.event_listener:
            # 兜底语义（issues/104 P2 统一口径）：监听器异常只记录不传播——不得影响引擎主流程
            # （对齐 PHP per-listener catch；Python 为单回调形态，无"后续监听器"概念）
            try:
                result = self.ext.event_listener(evt)
                if hasattr(result, '__await__'):
                    await result
            except Exception:  # noqa: BLE001 —— 引擎侧兜底，异常不外溢
                logging.exception("[jeeflow] process event listener error: type=%s", evt.type)

# ─── Pure Functions ─────────────────────────────────────────────────────────────

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
