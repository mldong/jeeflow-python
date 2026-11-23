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
# 两个「表单指定用户」类是**顶级类**（不带 OrgUserAssignmentHandlers$ 前缀），注册名与其余语言逐字一致
HANDLER_FORM_USER_DEPT_LEADER = "com.mldong.jeeflow.interceptor.impl.FormUserDeptLeaderAssignmentHandler"
HANDLER_FORM_USER_DEPT_MAIN_LEADER = "com.mldong.jeeflow.interceptor.impl.FormUserDeptMainLeaderAssignmentHandler"

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
    """按表单字段值分配参与者：f_ 前缀优先 → 裸名回落 → _数字 后缀去后缀再匹配。

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
        """issues/48：f_ 前缀优先 → 裸名回落 → 编号后缀去后缀匹配裸名"""
        if "f_" + field_name in variables:
            return variables["f_" + field_name]
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

    async def by_form_user(self, node, inst, main: bool) -> list[str]:
        """表单字段 → 用户 id → 该用户部门 → 领导（main=True 取分管领导）。

        字段缺失／值为空／用户不存在／无部门／部门无领导／SPI 未注册 ⇒ 空列表，不抛异常。
        """
        if node is None or inst is None:
            return []
        user_id = _first_form_user_id(_find_form_user_field_value(inst.variables, node.id))
        if user_id == "":
            return []
        return await self.by_dept(await self.dept_id_of(user_id), main)


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


# ─── 表单指定用户 → 该用户所属部门领导 ──────────────────────────────────────────
#
# 场景（参照 mldong-fastapi-wf 提交 fe9233d）：HR 代员工提交、代理申请——表单里给出被代理人的
# 用户 id，取**该员工**所属部门的经理／分管领导，与当前操作人无关。


class FormUserDeptLeaderAssignmentHandler(_OrgBase, IAssignmentHandler):
    """表单指定用户所属部门的经理"""

    async def assign(self, node, inst, operator) -> list[str]:
        return await self.by_form_user(node, inst, False)


class FormUserDeptMainLeaderAssignmentHandler(_OrgBase, IAssignmentHandler):
    """表单指定用户所属部门的分管领导"""

    async def assign(self, node, inst, operator) -> list[str]:
        return await self.by_form_user(node, inst, True)


def _find_form_user_field_value(variables: dict, node_key: str):
    """两个「表单指定用户」handler 专用的四档并集查找（顺序即优先级）：

    1. ``f_<nodeKey>``（jeeflow 变量带 f_ 前缀，优先）
    2. ``<nodeKey>``（裸名回落）
    3. nodeKey 含 ``#`` 时取最后一个 # 之后的 field：先 ``<field>``、再 ``f_<field>``
    4. nodeKey 匹配 ``^(.+?)_(\\d+)$`` 时取 base：先 ``f_<base>``、再 ``<base>``

    与 ``FormFieldAssigneeHandler._find_field_value`` 的差别只在 3、4 两档各补了 f_ 变体
    （参照实现没有 f_ 分支，jeeflow 家族变量存 f_ 必须有）。**现存 7 个 handler 仍走原查找。**
    """
    if not variables or not node_key:
        return None
    if "f_" + node_key in variables:
        return variables["f_" + node_key]
    if node_key in variables:
        return variables[node_key]
    if "#" in node_key:
        field = node_key.rsplit("#", 1)[1]
        if field in variables:
            return variables[field]
        if "f_" + field in variables:
            return variables["f_" + field]
    m = _NUMBER_SUFFIX_PATTERN.match(node_key)
    if m:
        base = m.group(1)
        if "f_" + base in variables:
            return variables["f_" + base]
        if base in variables:
            return variables[base]
    return None


def _first_form_user_id(value) -> str:
    """取**第一个**非空用户 id（多值语义不在本轮承诺）。

    判空只比空串 ⇒ ``"0"`` 是合法 id；列表元素不再拆逗号（与 Go/Node 同形）。
    """
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        for item in value:
            token = str(item).strip()
            if token != "":
                return token
        return ""
    for token in str(value).split(","):
        token = token.strip()
        if token != "":
            return token
    return ""


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
    registry.register_assignment(HANDLER_FORM_USER_DEPT_LEADER,
                                 FormUserDeptLeaderAssignmentHandler(user_prov, org_prov))
    registry.register_assignment(HANDLER_FORM_USER_DEPT_MAIN_LEADER,
                                 FormUserDeptMainLeaderAssignmentHandler(user_prov, org_prov))
