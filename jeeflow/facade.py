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
from .model import ProcessDefine, ProcessDesign, ProcessDesignHis, ProcessSurrogate, TaskState
from .spi import ProcessExtRepository, ProcessRepository, QueryCondition

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
                 user_search: Optional[callable] = None,
                 org_prov: Optional["OrgUserProvider"] = None):
        self._engine = engine
        self._repo = repo
        self._ext = ext_repo
        self._user_search = user_search  # 可空：candidatePage 用户分页搜索依赖
        self._org_prov = org_prov  # 可空：candidatePage candidateGroups 角色取人（v1.6.0）

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
            return self._ok(data)
        except Exception as e:
            return self._error(str(e))

    # ── 流程定义 / 实例 ─────────────────────────────────────────────────────

    async def _processDefine_page(self, args: dict) -> dict:
        """流程定义分页（v1.5.0 补齐）"""
        page_num = self._to_int(args.get("pageNum")) or 1
        page_size = self._to_int(args.get("pageSize")) or 10
        rows, total = await self._repo.page_defines(page_num, page_size, self._parse_m_query(args))
        return self._page_data([self._define_row_to_dict(r) for r in rows], total)

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
        operator = str(args.get("operator", "user1"))
        rows, total = await self._repo.page_instances(page_num, page_size, operator, self._parse_m_query(args))
        return self._page_data([self._instance_row_to_dict(r) for r in rows], total)

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
            ext["isFirstTaskNode"] = doing and t.taskName == first_task_id
            vo["ext"] = ext
            tasks.append(vo)
            if doing:
                active_task_list.append(vo)
        data = {
            "id": inst.id, "parentId": inst.parentId, "processDefineId": inst.defineId,
            "state": inst.state, "parentNodeName": inst.parentNodeName,
            "businessNo": inst.businessNo, "operator": inst.operator,
            "variables": inst.variables,
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

    async def _processTask_todoList(self, args: dict) -> dict:
        """我的待办分页（operator 作为待办人过滤，v1.5.0 补齐）"""
        page_num = self._to_int(args.get("pageNum")) or 1
        page_size = self._to_int(args.get("pageSize")) or 10
        actor_id = str(args.get("operator", "user1"))
        rows, total = await self._repo.page_todo_tasks(page_num, page_size, actor_id, self._parse_m_query(args))
        return self._page_data([self._task_row_to_dict(r) for r in rows], total)

    async def _processTask_doneList(self, args: dict) -> dict:
        """我的已办分页（operator 过滤，v1.5.0 补齐）"""
        page_num = self._to_int(args.get("pageNum")) or 1
        page_size = self._to_int(args.get("pageSize")) or 10
        operator = str(args.get("operator", "user1"))
        rows, total = await self._repo.page_done_tasks(page_num, page_size, operator, self._parse_m_query(args))
        return self._page_data([self._task_row_to_dict(r) for r in rows], total)

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
                                             self._to_int(args.get("pageSize")) or 10,
                                             conditions=self._parse_m_query(args))
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
        import json as _json
        try:
            flow = _json.loads(content)
        except Exception as e:
            raise ValueError(f"流程定义 JSON 解析失败: {e}")
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
                                                if args.get("operator") else None,
                                                conditions=self._parse_m_query(args))
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
                await self._collect_path(flow, "start", "", active, history, edges, set(),
                                         inst.variables, his)
            except Exception:
                pass
        return {"activeNodeNames": active, "historyNodeNames": history, "historyEdgeNames": edges}

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
            "variable": t.variables,
            "ext": t.variables,  # issues/15：前端读 ext.tf_approvalComment
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
        """我的抄送分页（v1.3.0）：operator 作为抄送人过滤"""
        page_num = self._to_int(args.get("pageNum")) or 1
        page_size = self._to_int(args.get("pageSize")) or 10
        actor_id = str(args.get("operator", "user1"))
        rows, total = await self._repo.page_cc_instances(page_num, page_size, actor_id, self._parse_m_query(args))
        return self._page_data([self._cc_row_to_dict(r) for r in rows], total)

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
                vo["jsonObject"] = self._parse_graph(def_.content)  # issues/05
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
                candidates = await self._next_task_candidates(flow, task.taskName)
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
    def _page_data(rows, total: int) -> dict:
        return {"rows": rows, "recordCount": total}

    @staticmethod
    def _ok(data) -> dict:
        return {"code": 0, "msg": "成功", "data": data}

    @staticmethod
    def _error(msg: str) -> dict:
        return {"code": 99999999, "msg": msg}

    @staticmethod
    def _task_vo(t) -> dict:
        """任务 VO（instanceDetail 任务列表用，对齐 Java taskVo）"""
        import json as _json
        try:
            variable = _json.dumps(t.variables, ensure_ascii=False) if t.variables else None
        except Exception:
            variable = None
        return {
            "id": t.id, "processInstanceId": t.processInstanceId, "taskName": t.taskName,
            "displayName": t.displayName, "taskType": t.taskType, "performType": t.performType,
            "taskState": t.taskState, "operator": t.actorId, "finishTime": t.finishTime,
            "expireTime": t.expireTime, "formKey": t.formKey, "taskParentId": t.parentTaskId,
            "variable": variable, "createTime": t.createTime, "createUser": t.createUser,
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
                "expireTime": self._fmt_time(r.expireTime), "variable": r.variables,
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
            "expireTime": self._fmt_time(r.expireTime), "variable": r.variables,
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
        ext = r.variables or {}
        if not ext:
            ext = instance_ext
        return {"id": r.id, "processInstanceId": r.processInstanceId, "taskName": r.taskName,
                "displayName": r.displayName, "taskType": r.taskType, "performType": r.performType,
                "taskState": int(r.taskState) if r.taskState is not None else None,
                "operator": r.operator, "finishTime": self._fmt_time(r.finishTime),
                "expireTime": self._fmt_time(r.expireTime), "formKey": r.formKey,
                "taskParentId": r.taskParentId, "variable": r.variables,
                "createTime": self._fmt_time(r.createTime), "createUser": r.createUser,
                "updateTime": self._fmt_time(r.updateTime), "updateUser": r.updateUser,
                "processDefineName": r.processDefineName,
                "processDefineDisplayName": r.processDefineDisplayName,
                "instanceVariable": r.instanceVariable,
                "instanceCreateTime": self._fmt_time(r.instanceCreateTime),
                "ext": ext, "instanceExt": instance_ext, "version": r.defineVersion,
                "taskFormData": self._form_data_of(ext, "tf_")}  # issues/15
