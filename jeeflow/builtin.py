"""内置通用参与者处理器（issues/16）——注册名与 Java 类全限定名一致，跨语言流程 JSON 通用。

- OperatorAssignmentHandler / FormFieldAssigneeHandler：纯引擎语义，零外部依赖
- 组织维度 handler：通过 OrgUserProvider SPI 取数据，业务方只实现数据接口
"""
import re
from typing import Optional, Union

from .extensions import HandlerRegistry, IAssignmentHandler
from .model import FlowNode, ProcessInstance
from .spi import OrgUserProvider, UserProvider

# ─── 注册名（与 Java 类全限定名一致）────────────────────────────────────────────

HANDLER_OPERATOR_ASSIGNMENT = "com.mldong.jeeflow.interceptor.impl.OperatorAssignmentHandler"
HANDLER_FORM_FIELD_ASSIGNEE = "com.mldong.jeeflow.interceptor.impl.FormFieldAssigneeHandler"
_ORG_HANDLERS_PREFIX = "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$"
HANDLER_DEPT_LEADER = _ORG_HANDLERS_PREFIX + "DeptLeaderAssignmentHandler"
HANDLER_DEPT_MAIN_LEADER = _ORG_HANDLERS_PREFIX + "DeptMainLeaderAssignmentHandler"
HANDLER_APPLICANT_DEPT_LEADER = _ORG_HANDLERS_PREFIX + "ApplicantDeptLeaderAssignmentHandler"
HANDLER_APPLICANT_DEPT_MAIN_LEADER = _ORG_HANDLERS_PREFIX + "ApplicantDeptMainLeaderAssignmentHandler"
HANDLER_TASK_ROLE_ASSIGNEE = _ORG_HANDLERS_PREFIX + "TaskRoleAssigneeHandler"

# 表单字段编号后缀正则（task_01 → task）
_NUMBER_SUFFIX_PATTERN = re.compile(r"^(.+?)_(\d+)$")


# ─── 纯引擎语义 ─────────────────────────────────────────────────────────────────

class OperatorAssignmentHandler(IAssignmentHandler):
    """流程发起人（兜底 "apply.operator"）"""

    async def assign(self, node, inst, operator) -> list[str]:
        if inst is not None and inst.operator:
            return [inst.operator]
        return ["apply.operator"]


class FormFieldAssigneeHandler(IAssignmentHandler):
    """按表单字段值分配参与者：精确匹配 node.id → vars 字段；_数字 后缀去后缀再匹配。

    字段值支持逗号分隔字符串 / list。
    """

    async def assign(self, node, inst, operator) -> list[str]:
        if inst is None or node is None:
            return []
        value = self._find_field_value(inst.variables, node.id)
        if value is None:
            return []
        return self._collect(value)

    def _find_field_value(self, variables: dict, field_name: str):
        if field_name in variables:
            return variables[field_name]
        m = _NUMBER_SUFFIX_PATTERN.match(field_name)
        if m and m.group(1) in variables:
            return variables[m.group(1)]
        return None

    def _collect(self, value) -> list[str]:
        ids: list[str] = []
        if isinstance(value, (list, tuple)):
            for item in value:
                self._add(ids, str(item))
        else:
            self._add(ids, str(value))
        return ids

    def _add(self, ids: list[str], token: str):
        for s in token.split(","):
            s = s.strip()
            if s and s not in ids:
                ids.append(s)


# ─── 组织维度（OrgUserProvider SPI）────────────────────────────────────────────

class _OrgBase:
    """组织维度 handler 公共依赖"""

    def __init__(self, user_prov: Optional[UserProvider], org_prov: Optional[OrgUserProvider]):
        self.user_prov = user_prov
        self.org_prov = org_prov

    async def by_dept(self, dept_id: str, main: bool) -> list[str]:
        if not dept_id or self.org_prov is None:
            return []
        if main:
            return await self.org_prov.find_dept_main_leaders(dept_id) or []
        return await self.org_prov.find_dept_leaders(dept_id) or []

    async def dept_id_of(self, user_id: str) -> str:
        if not user_id or self.user_prov is None:
            return ""
        u = await self.user_prov.get_user(user_id)
        return (u.deptId or "") if u else ""


class DeptLeaderAssignmentHandler(_OrgBase, IAssignmentHandler):
    """当前用户（任务操作人）部门领导"""

    async def assign(self, node, inst, operator) -> list[str]:
        return await self.by_dept(await self.dept_id_of(operator), False)


class DeptMainLeaderAssignmentHandler(_OrgBase, IAssignmentHandler):
    """当前用户（任务操作人）部门分管领导"""

    async def assign(self, node, inst, operator) -> list[str]:
        return await self.by_dept(await self.dept_id_of(operator), True)


class ApplicantDeptLeaderAssignmentHandler(_OrgBase, IAssignmentHandler):
    """发起人部门领导"""

    async def assign(self, node, inst, operator) -> list[str]:
        if inst is None:
            return []
        return await self.by_dept(await self.dept_id_of(inst.operator), False)


class ApplicantDeptMainLeaderAssignmentHandler(_OrgBase, IAssignmentHandler):
    """发起人部门分管领导"""

    async def assign(self, node, inst, operator) -> list[str]:
        if inst is None:
            return []
        return await self.by_dept(await self.dept_id_of(inst.operator), True)


class TaskRoleAssigneeHandler(IAssignmentHandler):
    """任务节点唯一编码关联角色（roleCode = 节点 id）"""

    def __init__(self, org_prov: Optional[OrgUserProvider] = None):
        self.org_prov = org_prov

    async def assign(self, node, inst, operator) -> list[str]:
        if node is None or self.org_prov is None:
            return []
        return await self.org_prov.find_by_role(node.id) or []


# ─── 注册 ───────────────────────────────────────────────────────────────────────

def register_builtin_assignments(registry: HandlerRegistry,
                                 user_prov: Optional[UserProvider] = None,
                                 org_prov: Optional[OrgUserProvider] = None):
    """注册内置通用参与者处理器到注册表（组织维度 handler 依赖 user_prov/org_prov）"""
    registry.register_assignment(HANDLER_OPERATOR_ASSIGNMENT, OperatorAssignmentHandler())
    registry.register_assignment(HANDLER_FORM_FIELD_ASSIGNEE, FormFieldAssigneeHandler())
    registry.register_assignment(HANDLER_DEPT_LEADER, DeptLeaderAssignmentHandler(user_prov, org_prov))
    registry.register_assignment(HANDLER_DEPT_MAIN_LEADER, DeptMainLeaderAssignmentHandler(user_prov, org_prov))
    registry.register_assignment(HANDLER_APPLICANT_DEPT_LEADER, ApplicantDeptLeaderAssignmentHandler(user_prov, org_prov))
    registry.register_assignment(HANDLER_APPLICANT_DEPT_MAIN_LEADER, ApplicantDeptMainLeaderAssignmentHandler(user_prov, org_prov))
    registry.register_assignment(HANDLER_TASK_ROLE_ASSIGNEE, TaskRoleAssigneeHandler(org_prov))
