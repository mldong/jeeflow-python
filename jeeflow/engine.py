"""引擎核心——对标 Java EngineImpl"""
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

    def set_extensions(self, ext: EngineExtensions):
        self.ext = ext
        self._ic_cache.clear()

    async def eval_expr(self, expr: str, vars_: dict) -> Any:
        """表达式求值（v1.5.0，门面 highLight 决策分支过滤用）"""
        if self.expr_eval is None:
            raise ValueError("ExpressionEvaluator 未配置")
        return await self.expr_eval.eval(expr, vars_)

    # ─── Start ────────────────────────────────────────────────────────────────

    async def start_process_instance_by_id(self, define_id: int, operator: str, args: dict[str, Any] = None) -> ProcessInstance:
        def_ = await self.repo.find_define_by_id(define_id)
        if not def_: raise ValueError(f"define not found: {define_id}")
        flow = parse_flow_model(json.loads(def_.content))
        vars_ = {**(args or {})}
        await self._add_user_info(operator, vars_)
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
        task, inst = await self._load_and_check(task_id, operator)
        # issues/26：办理提交的 f_ 字段按任务节点字段权限过滤（只读/隐藏不入变量）——
        # 被拒值无法经流程变量落到下游节点写入，上游只读声明不可被绕过
        def_ = await self.repo.find_define_by_id(inst.defineId)
        flow = parse_flow_model(json.loads(def_.content))
        args = _filter_field_by_perm(args or {}, _find_node(flow, task.taskName))
        vars_ = {**inst.variables, **task.variables, **args}
        await self._add_user_info(operator, vars_)
        now = datetime.now()
        # 聚合根：完成任务（子实体状态转换 + 实例变量合并）
        inst.complete_task(task, operator, vars_, now)
        await self.repo.update_task(task)
        # v1.0.1：update_instance 级联持久化依赖聚合内任务副本为最新状态，
        # complete_task 改的是外部任务对象，需同步回聚合根
        _sync_task_to_aggregate(inst, task)
        await self._fire_event(ProcessEvent(EventType.TASK_COMPLETE, inst.id, task.id, task.taskName, operator))

        def_ = await self.repo.find_define_by_id(inst.defineId)
        inst.variables = vars_
        await self.repo.update_instance(inst)

        cur_node = _find_node(flow, task.taskName)
        if cur_node:
            # 1.8.0：任务完成节点自身的后置拦截器（SYNC 同步演进——任务节点推进更新状态/字段）。
            # _create_task 不再触发（引擎语义修正），此处为完成任务节点的唯一触发点
            await self._fire_post(cur_node, inst)
            ct = cur_node.properties.get("countersignType", "")
            if ct == "SEQUENTIAL":
                doing = await self.repo.find_doing_tasks(inst.id)
                if not doing:
                    actors, lc = _get_cs_state(vars_, cur_node.id)
                    if actors and lc + 1 < len(actors):
                        # 聚合根：创建串行会签下一步任务
                        nt = inst.create_task(self._next_id(), cur_node.id, cur_node.text.get("value", ""),
                                              actors[lc + 1], operator, cur_node.properties.get("form", ""), now)
                        nt.variables = {f"operatorList_{cur_node.id}": actors, f"loopCounter_{cur_node.id}": lc + 1,
                                        f"nrOfInstances_{cur_node.id}": len(actors)}
                        await self.repo.save_task(nt)
                        return await self.repo.find_instance_by_id(inst.id)
                else:
                    return await self.repo.find_instance_by_id(inst.id)
            if ct in ("PARALLEL",) or ct.startswith("RATIO"):
                doing = await self.repo.find_doing_tasks(inst.id)
                if doing: return await self.repo.find_instance_by_id(inst.id)

            for node in _follow_edges(flow, cur_node.id):
                # 统一走 _execute_node：结束节点也经节点执行链（拦截器/事件完整触发），
                # _execute_node 内部 TYPE_END 分支完成聚合根 finish + 事件发布
                await self._execute_node(flow, inst, node, operator, vars_)
        return await self.repo.find_instance_by_id(inst.id)

    # ─── Reject ───────────────────────────────────────────────────────────────

    async def execute_and_jump_to_end(self, task_id: int, operator: str, args: dict[str, Any] = None) -> ProcessInstance:
        task, inst = await self._load_and_check(task_id, operator)
        now = datetime.now()
        # 聚合根：废弃所有进行中任务
        for t in inst.abandon_all_doing(now):
            await self.repo.update_task(t)
        # 子实体：完成任务
        task.finish(operator, task.variables, now)
        await self.repo.update_task(task)
        # v1.0.1：同步回聚合根，避免 update_instance 级联把任务写回旧状态
        _sync_task_to_aggregate(inst, task)
        # 聚合根：驳回
        inst.reject(now)
        await self.repo.update_instance(inst)
        await self._fire_event(ProcessEvent(EventType.PROCESS_REJECT, inst.id, task_id, operator=operator))
        return inst

    # ─── Jump ────────────────────────────────────────────────────────────────

    async def execute_and_jump_task(self, task_id: int, operator: str, args: dict[str, Any] = None,
                                     target_task_name: str = None) -> ProcessInstance:
        task, inst = await self._load_and_check(task_id, operator)
        now = datetime.now()
        # 聚合根：废弃所有进行中任务
        for t in inst.abandon_all_doing(now):
            await self.repo.update_task(t)
        # 子实体：完成任务
        task.finish(operator, task.variables, now)
        await self.repo.update_task(task)
        if target_task_name:
            def_ = await self.repo.find_define_by_id(inst.defineId)
            flow = parse_flow_model(json.loads(def_.content))
            target = _find_node(flow, target_task_name)
            if target: await self._execute_node(flow, inst, target, operator, inst.variables)
        return inst

    # ─── Jump To First Task（退回发起人，boot2 ROLLBACK_TO_OPERATOR=6）───────

    async def execute_and_jump_to_first_task_node(self, task_id: int, operator: str,
                                                   args: dict[str, Any] = None) -> ProcessInstance:
        task, inst = await self._load_and_check(task_id, operator)
        now = datetime.now()
        # 聚合根：废弃所有进行中任务
        for t in inst.abandon_all_doing(now):
            await self.repo.update_task(t)
        # 子实体：完成任务
        task.finish(operator, task.variables, now)
        await self.repo.update_task(task)
        # 找到第一个任务节点，强制参与者为发起人，重新执行
        def_ = await self.repo.find_define_by_id(inst.defineId)
        flow = parse_flow_model(json.loads(def_.content))
        start_node = _find_by_type(flow, TYPE_START)
        if start_node:
            for node in _follow_edges(flow, start_node.id):
                if node.type in (TYPE_TASK, TYPE_CUSTOM):
                    node.properties["assignee"] = inst.operator
                    await self._execute_node(flow, inst, node, operator, inst.variables)
                    break
        return inst

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
            await self._create_task(node, inst, operator, vars_)
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
                inst.variables = vars_
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

    async def _create_task(self, node: FlowNode, inst: ProcessInstance, operator: str, vars_: dict):
        actors = await self._resolve_actors(node, inst, operator, vars_)
        if not actors: return
        # performType 容错解析（对齐 Java codeOf）：int 优先；字符串枚举名（如旧版 "ANY"）映射；
        # 未知回落 0（普通参与）——issues 待反馈：python 引擎解析兼容 Java 流程格式
        _pt = node.properties.get("performType", 0)
        try:
            perform_type = int(_pt)
        except (ValueError, TypeError):
            try:
                perform_type = PerformType[str(_pt).upper()].value
            except (KeyError, AttributeError):
                perform_type = 0
        ct = node.properties.get("countersignType", "")
        now = datetime.now()
        form = node.properties.get("form", "")
        if perform_type == 1 and ct:
            if ct == "PARALLEL":
                for a in actors: await self.repo.save_task(inst.create_task(self._next_id(), node.id, node.text.get("value", ""), a, operator, form, now))
            elif ct == "SEQUENTIAL":
                nt = inst.create_task(self._next_id(), node.id, node.text.get("value", ""), actors[0], operator, form, now)
                nt.variables = {f"operatorList_{node.id}": actors, f"loopCounter_{node.id}": 0, f"nrOfInstances_{node.id}": len(actors)}
                await self.repo.save_task(nt)
            else:
                for a in actors: await self.repo.save_task(inst.create_task(self._next_id(), node.id, node.text.get("value", ""), a, operator, form, now))
        else:
            # 普通任务：一个任务承载全部参与者（对齐 boot3 createTask + addTaskActor，多参与者任一可办）
            nt = inst.create_task(self._next_id(), node.id, node.text.get("value", ""), actors[0], operator, form, now)
            if len(actors) > 1:
                nt.actorIds = actors
            await self.repo.save_task(nt)

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
        未声明 → 回落引擎级列表（向后兼容现状）。结果按 defineId 缓存。"""
        if not self.ext:
            return []
        define_id = getattr(inst, "defineId", None)
        if define_id is None:
            return list(self.ext.interceptors)
        cached = self._ic_cache.get(define_id)
        if cached is not None:
            return cached
        ic_list = list(self.ext.interceptors)
        try:
            def_ = await self.repo.find_define_by_id(define_id)
            if def_ is not None:
                content = def_.content
                meta = json.loads(content) if isinstance(content, str) else json.loads(content.decode("utf-8"))
                declared = str(meta.get("postInterceptors") or "").strip()
                if declared:
                    ic_list = []
                    for name in declared.split(","):
                        name = name.strip()
                        if name and name in (self.ext.interceptor_registry or {}):
                            ic_list.append(self.ext.interceptor_registry[name])
        except Exception:
            pass
        self._ic_cache[define_id] = ic_list
        return ic_list

    async def _fire_event(self, evt: ProcessEvent):
        if self.ext and self.ext.event_listener:
            result = self.ext.event_listener(evt)
            if hasattr(result, '__await__'):
                await result

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
