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
from datetime import datetime, timedelta
from typing import Any, Optional

from .engine import Engine, KEY_NEXT_NODE_OPERATOR, KEY_PROCESS_START_NEXT_NODE_OPERATOR
from .extensions import EventType, ProcessEvent
from .model import ProcessDefine, ProcessDesign, ProcessDesignHis, ProcessSurrogate, TaskState, InstanceState
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
        self._meta_reader = None  # 可空：bizData 业务数据读取器（issue 30，注入式）

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
        operator = str(args.get("operator", "user1"))
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
        # issues/56 E28：发起时抄送（f_ccActors）创建 cc 实例（对齐 Java enableCcActors 语义）
        cc = flow_args.get("f_ccActors")
        if cc is not None:
            if isinstance(cc, str):
                cc_list = [x.strip() for x in cc.split(",") if x.strip()]
            elif isinstance(cc, (list, tuple)):
                cc_list = [str(x) for x in cc]
            else:
                cc_list = []
            if cc_list:
                await self._repo.create_cc_instance(inst.id, operator, *cc_list)
                # issues/102：CC 实例落库后逐抄送人 fire CC_CREATE（ccActorId 直传事件体，
                # 在 start 事务内；监听器据此落抄送知会 NOTICE）
                for actor in cc_list:
                    await self._engine.fire_event(
                        ProcessEvent(type=EventType.CC_CREATE, instanceId=inst.id, ccActorId=actor))
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
        inst = await self._repo.find_instance_by_id(instance_id)
        if not inst:
            raise ValueError("流程实例不存在")
        # 撤回：废弃全部 doing 任务 + 实例状态（v1.0.1：update_instance 级联落库）
        # 注意：find_instance_by_id 不加载 tasks（空），必须按实例查 doing 任务废弃
        operator = str(args.get("operator", "user1"))
        now = datetime.now()
        abandoned = []
        for t in await self._repo.find_doing_tasks(instance_id):
            t.abandon(now)
            abandoned.append(t)
        inst.withdraw(now)  # issues/53 E25：撤回状态 Withdraw(30) 而非 Reject(45)
        inst.updateUser = operator
        # 级联覆盖防护（issues/57 补正）：废弃副本同步回聚合（update_instance 级联覆盖防护）
        inst.tasks = abandoned
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
        return self._page_data([self._task_row_to_dict(r) for r in rows], total, page_num, page_size)

    async def _processTask_doneList(self, args: dict) -> dict:
        """我的已办分页（operator 过滤，v1.5.0 补齐）"""
        page_num = self._to_int(args.get("pageNum")) or 1
        page_size = self._to_int(args.get("pageSize")) or 10
        operator = str(args.get("operator", "user1"))
        rows, total = await self._repo.page_done_tasks(page_num, page_size, operator, self._parse_m_query(args))
        return self._page_data([self._task_row_to_dict(r) for r in rows], total, page_num, page_size)

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
        operator = str(args.get("operator", "user1"))
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
        operator = str(args.get("operator", "user1"))
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
        enabled = JeeflowFacade._to_int(args.get("enabled"))
        s.enabled = 1 if enabled is None else enabled  # 显式 0 不得被 or 1 吞掉（对齐 Java/Go toIntDef）
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
        # issues/102：手动 CC 与发起路径同语义——逐抄送人 fire CC_CREATE
        for actor in actor_ids:
            await self._engine.fire_event(
                ProcessEvent(type=EventType.CC_CREATE, instanceId=instance_id, ccActorId=actor))
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
        return self._page_data([self._cc_row_to_dict(r) for r in rows], total, page_num, page_size)

    async def _processTask_detail(self, args: dict) -> dict:
        task_id = self._to_int(args.get("id"))
        operator = str(args.get("operator", "user1"))
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
        t_ext["isFirstTaskNode"] = False
        vo = {
            "id": task.id, "processInstanceId": task.processInstanceId,
            "taskName": task.taskName, "displayName": task.displayName,
            "taskType": int(task.taskType) if task.taskType is not None else None,
            "performType": int(task.performType) if task.performType is not None else None,
            "taskState": int(task.taskState) if task.taskState is not None else None,
            "operator": task.actorId, "formKey": task.formKey,
            "taskActorIdList": actors, "executable": task.is_allowed(operator),
            "ext": t_ext,
        }
        # taskModel：流程定义中对应节点
        inst = await self._repo.find_instance_by_id(task.processInstanceId)
        if inst:
            def_ = await self._repo.find_define_by_id(inst.defineId)
            if def_:
                vo["jsonObject"] = self._parse_graph(def_.content)  # issues/05
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
        today_insts = await self._repo.query_instances_for_stats(
            self._DEFAULT_STATE_IN, "create_time", today_start, today_end)
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

        insts = await self._repo.query_instances_for_stats(self._DEFAULT_STATE_IN, "create_time", start, end)
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
        return {"granularity": granularity, "series": series}

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
            insts = await self._repo.query_instances_for_stats(self._DEFAULT_STATE_IN, "create_time", start, end)
            grouped: dict[str, int] = {}
            for r in insts:
                k = str(r.state)
                grouped[k] = grouped.get(k, 0) + 1
            entries = sorted(grouped.items(), key=lambda x: x[1], reverse=True)[:limit]
            rows = [{"key": k, "label": None, "count": c, "avgDurationSeconds": None} for k, c in entries]

        elif dimension == "category":
            insts = await self._repo.query_instances_for_stats(self._DEFAULT_STATE_IN, "create_time", start, end)
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
            insts = await self._repo.query_instances_for_stats(self._DEFAULT_STATE_IN, "create_time", start, end)
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

        return {"dimension": dimension, "rows": rows}

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
