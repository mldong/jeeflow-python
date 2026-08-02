"""统一门面（v1.1.0）——"接口即 POST + JSON body"风格的单入口

集成方只实现一个转发端点：把 body JSON 转成 dict 传入 flow()，
所有流程能力按 action（boot2/boot3 端点短名）路由。返回统一结构
{code, msg, data}（code=0 成功 / 99999999 失败）。

操作人约定：门面不感知登录态，args["operator"] 显式传入。
"""
from __future__ import annotations

import inspect
import json
from datetime import datetime
from typing import Any, Optional

from .engine import Engine
from .model import ProcessDefine, ProcessDesign, ProcessDesignHis, ProcessSurrogate
from .spi import ProcessExtRepository, ProcessRepository

# submitType 枚举（对齐 boot3）
SUBMIT_APPLY = 0
SUBMIT_AGREE = 1
SUBMIT_REJECT = 2
SUBMIT_ROLLBACK = 3
SUBMIT_JUMP = 4
SUBMIT_ROLLBACK_TO_OPERATOR = 6
SUBMIT_COUNTERSIGN_DISAGREE = 20


class JeeflowFacade:
    """统一门面——flow(action, args) -> dict"""

    def __init__(self, engine: Engine, repo: ProcessRepository,
                 ext_repo: Optional[ProcessExtRepository] = None,
                 user_search: Optional[callable] = None):
        self._engine = engine
        self._repo = repo
        self._ext = ext_repo
        self._user_search = user_search  # 可空：candidatePage 用户分页搜索依赖

    def set_user_search(self, fn: callable) -> "JeeflowFacade":
        """注入用户搜索钩子：fn(query: dict) -> (rows: list[dict], total: int)"""
        self._user_search = fn
        return self

    async def flow(self, action: str, args: Optional[dict] = None) -> dict:
        args = args or {}
        try:
            handler = getattr(self, "_" + action.replace("/", "_"), None)
            if handler is None:
                return self._error(f"未知 action: {action}")
            data = await handler(args)
            return self._ok(data)
        except Exception as e:
            return self._error(str(e))

    # ── 流程定义 / 实例 ─────────────────────────────────────────────────────

    async def _processDefine_startAndExecute(self, args: dict) -> dict:
        return await self._startAndExecute(args)

    async def _processInstance_startAndExecute(self, args: dict) -> dict:
        return await self._startAndExecute(args)

    async def _startAndExecute(self, args: dict) -> dict:
        define_id = self._to_int(args.get("processDefineId"))
        if not define_id:
            raise ValueError("processDefineId 缺失或非法")
        operator = str(args.get("operator", "user1"))
        flow_args = {k: v for k, v in args.items() if k not in ("processDefineId", "operator")}
        inst = await self._engine.start_process_instance_by_id(define_id, operator, flow_args)
        # startAndExecute：自动完成申请节点（assignee="applicant" → 发起人）
        doing = await self._repo.find_doing_tasks(inst.id)
        for task in doing:
            await self._repo.add_task_actor(task.id, [operator])
            flow_args["submitType"] = SUBMIT_APPLY
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
        flow = json.loads(content)
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
        flow = json.loads(content)
        def_ = ProcessDefine(id=define_id, name=flow.get("name", ""),
                             displayName=flow.get("displayName", ""),
                             type=flow.get("type", "approval"),
                             content=content,
                             updateUser=str(args.get("operator", "system")))
        await self._repo.update_define(def_)
        return None

    async def _processDefine_remove(self, args: dict) -> dict:
        define_id = self._to_int(args.get("id"))
        if not define_id:
            raise ValueError("id 缺失或非法")
        await self._repo.remove_define(define_id)
        return None

    async def _processDefine_upAndDown(self, args: dict) -> dict:
        define_id = self._to_int(args.get("id"))
        state = self._to_int(args.get("state"))
        if not define_id or state is None:
            raise ValueError("id/state 缺失或非法")
        await self._repo.update_define_state(define_id, state)
        return None

    async def _processInstance_withdraw(self, args: dict) -> dict:
        instance_id = self._to_int(args.get("id"))
        if not instance_id:
            raise ValueError("id 缺失或非法")
        inst = await self._repo.find_instance_by_id(instance_id)
        if not inst:
            raise ValueError("流程实例不存在")
        # 撤回：废弃全部 doing 任务 + 实例状态（v1.0.1：update_instance 级联落库）
        operator = str(args.get("operator", "user1"))
        now = datetime.now()
        abandoned = inst.abandon_all_doing(now)
        inst.reject(now)
        inst.updateUser = operator
        for t in abandoned:
            await self._repo.update_task(t)
        await self._repo.update_instance(inst)
        return None

    # ── 流程任务 ─────────────────────────────────────────────────────────────

    async def _processTask_execute(self, args: dict) -> dict:
        task_id = self._to_int(args.get("processTaskId"))
        if not task_id:
            raise ValueError("processTaskId 缺失或非法")
        operator = str(args.get("operator", "user1"))
        submit_type = self._to_int(args.get("submitType")) or SUBMIT_AGREE
        flow_args = {k: v for k, v in args.items() if k not in ("processTaskId", "operator")}
        flow_args["submitType"] = submit_type
        # boot3 execute 分发（spec §11.2）
        if submit_type == SUBMIT_REJECT:
            await self._engine.execute_and_jump_to_end(task_id, operator, flow_args)
        elif submit_type == SUBMIT_ROLLBACK:
            await self._engine.execute_and_jump_task(task_id, operator, flow_args)
        elif submit_type == SUBMIT_JUMP:
            await self._engine.execute_and_jump_task(task_id, operator, flow_args,
                                                     str(args.get("taskName", "")))
        elif submit_type == SUBMIT_ROLLBACK_TO_OPERATOR:
            await self._engine.execute_and_jump_to_first_task_node(task_id, operator, flow_args)
        elif submit_type == SUBMIT_COUNTERSIGN_DISAGREE:
            flow_args["countersignDisagreeFlag"] = 1
            await self._engine.execute_process_task(task_id, operator, flow_args)
        else:  # 0 APPLY / 1 AGREE / 5 重新提交
            await self._engine.execute_process_task(task_id, operator, flow_args)
        return None

    # ── 流程设计（需扩展仓储） ───────────────────────────────────────────────

    async def _processDesign_page(self, args: dict) -> dict:
        ext = self._ext_repo()
        rows, total = await ext.page_designs(self._to_int(args.get("pageNum")) or 1,
                                             self._to_int(args.get("pageSize")) or 10)
        return self._page_data(rows, total)

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
        if his_list:
            try:
                data["jsonObject"] = json.loads(his_list[0].content)
            except Exception:
                pass
        data["his"] = his_list
        return data

    async def _processDesign_save(self, args: dict) -> dict:
        ext = self._ext_repo()
        operator = str(args.get("operator", "user1"))
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
            await ext.update_design(design)
        # 内容快照（设计稿内容存历史表）
        content = self._content(args, required=False)
        if content:
            await ext.save_design_his(ProcessDesignHis(processDesignId=design.id,
                                                       content=content, createUser=operator))
        return {"id": design.id}

    async def _processDesign_remove(self, args: dict) -> dict:
        design_id = self._to_int(args.get("id"))
        if not design_id:
            raise ValueError("id 缺失或非法")
        await self._ext_repo().remove_design(design_id)
        return None

    # ── 委托代理（需扩展仓储） ───────────────────────────────────────────────

    async def _processSurrogate_page(self, args: dict) -> dict:
        ext = self._ext_repo()
        rows, total = await ext.page_surrogates(self._to_int(args.get("pageNum")) or 1,
                                                self._to_int(args.get("pageSize")) or 10,
                                                filters={"operator": str(args["operator"])}
                                                if args.get("operator") else None)
        return self._page_data(rows, total)

    async def _processSurrogate_save(self, args: dict) -> dict:
        ext = self._ext_repo()
        operator = str(args.get("operator", "user1"))
        surrogate_id = self._to_int(args.get("id"))
        if not surrogate_id:
            surrogate = ProcessSurrogate(operator=operator,  # 授权人 = 操作人
                                         surrogate=str(args.get("surrogate", "")),
                                         processName=str(args.get("processName", "")),
                                         enabled=self._to_int(args.get("enabled")) or 1,
                                         createUser=operator, updateUser=operator)
            await ext.save_surrogate(surrogate)
        else:
            surrogate = await ext.find_surrogate_by_id(surrogate_id)
            if not surrogate:
                raise ValueError("委托记录不存在")
            if args.get("surrogate") is not None:
                surrogate.surrogate = str(args["surrogate"])
            if args.get("processName") is not None:
                surrogate.processName = str(args["processName"])
            if args.get("enabled") is not None:
                surrogate.enabled = self._to_int(args["enabled"])
            surrogate.updateUser = operator
            await ext.update_surrogate(surrogate)
        return {"id": surrogate.id}

    async def _processSurrogate_remove(self, args: dict) -> dict:
        surrogate_id = self._to_int(args.get("id"))
        if not surrogate_id:
            raise ValueError("id 缺失或非法")
        await self._ext_repo().remove_surrogate(surrogate_id)
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
        def_ = await self._repo.find_define_by_id(inst.defineId)
        if def_:
            try:
                flow = json.loads(def_.content)
                self._collect_path(flow, "start", "", active, history, edges, set())
            except Exception:
                pass
        return {"activeNodeNames": active, "historyNodeNames": history, "historyEdgeNames": edges}

    def _collect_path(self, flow: dict, node_id: str, edge_name: str,
                      active: list, history: list, edges: list, visited: set):
        if node_id in visited:
            return
        visited.add(node_id)
        if edge_name and edge_name not in edges:
            edges.append(edge_name)
        for e in flow.get("edges", []):
            if e.get("sourceNodeId") != node_id:
                continue
            target = self._find_node(flow, e.get("targetNodeId"))
            if not target:
                continue
            tid = target.get("id")
            if tid not in active and tid not in history:
                history.append(tid)
            if tid in active:
                continue
            self._collect_path(flow, tid, e.get("id"), active, history, edges, visited)

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
            "operator": t.actorId, "finishTime": str(t.finishTime),
            "variable": t.variables,
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
        operator = str(args.get("operator", "user1"))
        actor_ids = self._to_str_list(args.get("actorIds"))
        if not instance_id or not actor_ids:
            raise ValueError("processInstanceId/actorIds 缺失")
        await self._repo.create_cc_instance(instance_id, operator, *actor_ids)
        return None

    async def _processInstance_updateCCStatus(self, args: dict) -> dict:
        instance_id = self._to_int(args.get("processInstanceId"))
        operator = str(args.get("operator", "user1"))
        if not instance_id:
            raise ValueError("processInstanceId 缺失或非法")
        await self._repo.update_cc_status(instance_id, operator)
        return None

    async def _processInstance_ccList(self, args: dict) -> dict:
        raise ValueError("ccList 需要核心分页 SPI（page_cc_instances），当前语言 1.3.0 补齐")

    async def _processTask_detail(self, args: dict) -> dict:
        task_id = self._to_int(args.get("id"))
        operator = str(args.get("operator", "user1"))
        if not task_id:
            raise ValueError("id 缺失或非法")
        task = await self._repo.find_task_by_id(task_id)
        if not task:
            raise ValueError("任务不存在")
        actors = await self._repo.find_task_actors(task_id)
        vo = {
            "id": task.id, "processInstanceId": task.processInstanceId,
            "taskName": task.taskName, "displayName": task.displayName,
            "taskType": int(task.taskType) if task.taskType is not None else None,
            "performType": int(task.performType) if task.performType is not None else None,
            "taskState": int(task.taskState) if task.taskState is not None else None,
            "operator": task.actorId, "formKey": task.formKey,
            "taskActorIdList": actors, "executable": task.is_allowed(operator),
        }
        # taskModel：流程定义中对应节点
        inst = await self._repo.find_instance_by_id(task.processInstanceId)
        if inst:
            def_ = await self._repo.find_define_by_id(inst.defineId)
            if def_:
                try:
                    flow = json.loads(def_.content)
                    for n in flow.get("nodes", []):
                        if n.get("id") == task.taskName:
                            vo["taskModel"] = {"name": n.get("id"),
                                               "displayName": (n.get("text") or {}).get("value", ""),
                                               "type": n.get("type")}
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
                candidates = self._next_task_candidates(flow, task.taskName)
            except Exception:
                pass
        if candidates:
            rows = [{"userId": c, "realName": c} for c in candidates]
            return self._page_data(rows, len(rows))
        # 无模型候选 → 用户分页搜索（依赖 user_search 钩子）
        if self._user_search is None:
            raise ValueError("未配置 user_search（用户搜索钩子）")
        result = self._user_search(args)
        if inspect.isawaitable(result):
            result = await result
        rows, total = result
        return self._page_data(rows, total)

    def _next_task_candidates(self, flow: dict, task_name: str) -> list:
        result = []
        visited = set()

        def collect(node: dict):
            v = (node.get("properties") or {}).get("candidateUsers", "")
            if v:
                for s in str(v).split(","):
                    s = s.strip()
                    if s and s not in result:
                        result.append(s)

        def walk(node_id: str):
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
                    collect(target)
                    continue
                if target.get("type") in ("snaker:fork", "snaker:join", "snaker:decision"):
                    walk(target.get("id"))

        walk(task_name)
        return result

    async def _processTask_surrogate(self, args: dict) -> dict:
        return await self._taskAddActor(args)

    async def _processTask_addCandidate(self, args: dict) -> dict:
        return await self._taskAddActor(args)

    async def _taskAddActor(self, args: dict) -> dict:
        task_id = self._to_int(args.get("processTaskId"))
        actor_ids = self._to_str_list(args.get("actorIds"))
        if not task_id or not actor_ids:
            raise ValueError("processTaskId/actorIds 缺失")
        await self._repo.add_task_actor(task_id, actor_ids)
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

    @staticmethod
    def _to_str_list(v) -> list:
        if isinstance(v, (list, tuple)):
            return [str(x) for x in v]
        if isinstance(v, str):
            return [s.strip() for s in v.split(",") if s.strip()]
        return []

    # ── 工具 ─────────────────────────────────────────────────────────────────

    def _ext_repo(self) -> ProcessExtRepository:
        if self._ext is None:
            raise ValueError("未配置 ProcessExtRepository（扩展仓储）")
        return self._ext

    @staticmethod
    def _content(args: dict, required: bool = True) -> Optional[str]:
        content = args.get("content")
        if content is None:
            if required:
                raise ValueError("content 缺失")
            return None
        if isinstance(content, bytes):
            return content.decode("utf-8")
        return str(content)

    @staticmethod
    def _to_int(v) -> Optional[int]:
        if v is None:
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _page_data(rows, total: int) -> dict:
        return {"rows": rows, "recordCount": total}

    @staticmethod
    def _ok(data) -> dict:
        return {"code": 0, "msg": "成功", "data": data}

    @staticmethod
    def _error(msg: str) -> dict:
        return {"code": 99999999, "msg": msg}
