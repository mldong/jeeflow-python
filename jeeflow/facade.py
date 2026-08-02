"""统一门面（v1.1.0）——"接口即 POST + JSON body"风格的单入口

集成方只实现一个转发端点：把 body JSON 转成 dict 传入 flow()，
所有流程能力按 action（boot2/boot3 端点短名）路由。返回统一结构
{code, msg, data}（code=0 成功 / 99999999 失败）。

操作人约定：门面不感知登录态，args["operator"] 显式传入。
"""
from __future__ import annotations

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
                 ext_repo: Optional[ProcessExtRepository] = None):
        self._engine = engine
        self._repo = repo
        self._ext = ext_repo

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
