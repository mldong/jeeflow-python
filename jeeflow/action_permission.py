"""action → 权限码映射 SPI（hub issues/248 的 python 腿；语义参照实现＝java
``com.mldong.jeeflow.spi.DefaultActionPermissionProvider``，issues/29，其测试是权威）。

**边界（与 java／go 逐字同口径）**：引擎**不依赖任何鉴权框架**（sa-token / JWT / 中间件
都是宿主框架的事），本 SPI 只提供"action → 权限码"这一份**元数据**；校验执行仍由
集成方框架层完成。

**为什么要在引擎里放这张表**（而不是各集成壳自备）：现读各壳各抄一份
（python 三壳 ``wf_permission.py``／go 三壳 ``wf_permission.go``／nestjs ``wf-permission.ts``／
laravel ``WfPermission.php``／salvo ``wf_permission.rs``／csharp ``WfEndpoints.cs``），
新 action 或粒度调整要改十处＋引擎，且"只用引擎、自写集成层"的客户跨栈读出两套码。
本文件与 java 的 OR／放行两张表**逐条一致**。

三档优先级（java ``permissionCodes`` 的判据次序，不许自创）：
① 放行清单（``NO_PERM_ACTIONS``，11 条）→ ``None``（登录即可，boot3 无注解＝登录即可；
   统计三条是 issues/103 的收口成果）；
② OR 表（``OR_RULES``，8 条）→ 权限码列表，**OR 语义**：任一持有即有权
   （boot3 ``@SaCheckPermission(value={...}, mode=OR)``）；
③ 默认派生：``"wf:" + action.replace("/", ":")``。

``processTask/recall``／``processTask/revokeDynamicCountersign`` 两枚（issues/246／247）
定 B 案（owner 2026-11-04）：**复用"办理"这枚码**，不另建菜单按钮种子。判据是既有惯例——
``transfer``／``removeTaskActor``／``candidatePage``／``latest`` 都没有 ``sys_menu`` 按钮行，
``candidatePage``／``jumpAbleTaskNameList`` 就是走本表复用 execute；而撤回／撤会签的主体
按引擎判据本来就必然是该行参与者（＝有办理权的人），拆独立码实际给不出去。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

# ─── SPI 接口 ────────────────────────────────────────────────────────────────

class ActionPermissionProvider(ABC):
    """action → 权限码映射提供者（java ``IActionPermissionProvider`` 的同位）。"""

    @abstractmethod
    def permission_codes(self, action: str) -> Optional[list[str]]:
        """返回 action 的权限码集合（**OR 语义**——任一持有即有权）。

        返回 ``None``／空列表 ＝ 放行（登录即可）。``action`` 为 ``None`` 时同样放行
        （对齐 java ``if (action == null) return null;``）。
        """
        ...


# ─── 内置默认件（java DefaultActionPermissionProvider 的逐条同款）─────────────

# OR 规则表（对齐 java OR_RULES，8 条——条目数是跨栈契约形状锚，加一条要八栈同批改）
OR_RULES: dict[str, list[str]] = {
    "processDefine/detail": ["wf:processDefine:detail", "wf:processDesign:listByType"],
    "processDefine/startAndExecute": ["wf:processDefine:startAndExecute", "wf:processDesign:listByType"],
    "processDefine/getLastByName": ["wf:processDefine:detail", "wf:processDesign:listByType",
                                    "wf:processDefine:getLastByName"],
    "processTask/candidatePage": ["wf:processTask:execute", "wf:processTask:candidatePage"],
    "processTask/jumpAbleTaskNameList": ["wf:processTask:execute"],
    # issues/246／247 两枚新 action（B 案）：复用 execute，见模块 docstring
    "processTask/recall": ["wf:processTask:execute"],
    "processTask/revokeDynamicCountersign": ["wf:processTask:execute"],
    # issues/253 动态协办：照 246/247 B 案惯例复用"办理"这枚码，不建按钮种子行——
    # 派生协办的主体按引擎判据本来就必然是主办行名册内的人（＝有办理权的人）。
    "processTask/aidant": ["wf:processTask:execute"],
}

# 放行清单（对齐 java NO_PERM_ACTIONS，11 条；统计三条＝issues/103 收口）
NO_PERM_ACTIONS: frozenset[str] = frozenset((
    "processInstance/detail", "processInstance/highLight", "processInstance/approvalRecord",
    "processInstance/getAssigneeTextData", "processInstance/bizData",
    "processTask/detail", "processTask/addCandidate", "processTask/latest",
    "processInstance/stats/overview", "processInstance/stats/trend", "processInstance/stats/group",
))


class DefaultActionPermissionProvider(ActionPermissionProvider):
    """引擎内置默认实现：放行 → OR → 默认派生 ``wf:{action 的 / 换 :}``。"""

    def permission_codes(self, action: str) -> Optional[list[str]]:
        if action is None:
            return None                  # 对齐 java null 档：放行
        if action in NO_PERM_ACTIONS:
            return None                  # 放行
        or_codes = OR_RULES.get(action)
        if or_codes is not None:
            return list(or_codes)        # OR 语义（拷贝一份，防调用方改表）
        return ["wf:" + action.replace("/", ":")]


# ─── 可选覆盖位（java ServiceContext.find(IActionPermissionProvider) 的同位）──
#
# 与 java 一致的兜底语义：**没注册就等于用内置默认件**，集成方不配也有统一权限语义；
# 特殊集成方（另一套权限码体系）覆盖注册后按自家规则走。

_default_provider: ActionPermissionProvider = DefaultActionPermissionProvider()


def set_action_permission_provider(provider: Optional[ActionPermissionProvider]) -> None:
    """覆盖注册（传 ``None`` 回落到内置默认件，对齐 go ``SetActionPermissionProvider``）。"""
    global _default_provider
    _default_provider = provider if provider is not None else DefaultActionPermissionProvider()


def get_action_permission_provider() -> ActionPermissionProvider:
    """取当前生效的提供者（恒非 ``None``，故调用方不必判空）。"""
    return _default_provider


def permission_codes(action: str) -> Optional[list[str]]:
    """便捷出口：按当前生效提供者取 action 的权限码集合。"""
    return get_action_permission_provider().permission_codes(action)


# ─── 引擎元数据清单条目（java HandlerRegistry／go ActionPermissionProviderMeta
#     ／c# Metadata.cs／rust metadata.rs 同一约定：内置默认件也暴露给前端设计器）──

def action_permission_provider_meta() -> tuple[str, str]:
    """(类型名, 类名串)——与 java HandlerRegistry issues/29 那条登记逐字一致。"""
    return ("ActionPermissionProvider",
            "com.mldong.jeeflow.spi.DefaultActionPermissionProvider")
