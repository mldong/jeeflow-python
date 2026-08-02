"""引擎元数据能力（v1.4.0，issues/04）

两类元数据：
1. ``enum_dict_keys()`` / ``enum_dict(key)`` —— 内置状态枚举字典（key 对齐 boot3：
   ``wf_process_define_state`` 等），value/label 与 Java enums 完全一致，杜绝集成方
   重复定义导致的值漂移；
2. ``HandlerRegistry`` —— SPI 实现清单（AssignmentHandler / CandidateHandler /
   FlowInterceptor），集成方显式注册可用实现（handlerName + 显示名/排序/分组），
   作为前端设计器字典源，与运行时引擎加载的 handlerName 天然一致。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

# ─── 枚举字典 ────────────────────────────────────────────────────────────────

@dataclass
class DictItem:
    """单个字典项"""
    value: str
    label: str

# 内置字典表（值顺序与 Java enums 声明顺序一致）
_DICTS: dict[str, list[DictItem]] = {
    "wf_process_define_state": [
        DictItem("0", "禁用"), DictItem("1", "启用"),
    ],
    "wf_process_instance_state": [
        DictItem("10", "进行中"), DictItem("20", "已完成"), DictItem("30", "已撤回"),
        DictItem("40", "强行终止"), DictItem("45", "已拒绝"), DictItem("50", "挂起"),
        DictItem("99", "已废弃"),
    ],
    "wf_process_submit_type": [
        DictItem("0", "发起申请"), DictItem("1", "同意申请"), DictItem("2", "拒绝申请"),
        DictItem("3", "退回上一步"), DictItem("4", "跳转"), DictItem("5", "重新提交"),
        DictItem("6", "退回发起人"), DictItem("20", "拒绝申请"),
    ],
    "wf_process_task_state": [
        DictItem("10", "进行中"), DictItem("20", "已完成"), DictItem("30", "已撤回"),
        DictItem("40", "强行终止"), DictItem("50", "挂起"), DictItem("99", "已废弃"),
    ],
    "wf_process_task_type": [
        DictItem("0", "主办"), DictItem("1", "协办"), DictItem("2", "记录"),
    ],
    "wf_process_task_perform_type": [
        DictItem("0", "普通参与"), DictItem("1", "会签参与"),
    ],
    "wf_countersign_type": [
        DictItem("0", "并行会签"), DictItem("1", "串行会签"),
    ],
}


def enum_dict_keys() -> list[str]:
    """内置枚举字典 key 清单（对齐 boot3 字典 key，存量前端零改动）"""
    return list(_DICTS.keys())


def enum_dict(key: str) -> list[DictItem]:
    """按 key 取字典（[{value, label}]），未知 key 返回空列表"""
    return list(_DICTS.get(key, []))


# ─── SPI 实现清单 ────────────────────────────────────────────────────────────

@dataclass
class HandlerMeta:
    """处理器元数据"""
    type: str                                      # 处理器类型名（AssignmentHandler / CandidateHandler / FlowInterceptor）
    className: str                                 # 节点配置的 handlerName（与字典 value 一致）
    displayName: str = ""                          # 显示名（字典 label）
    order: int = 0                                 # 排序（小在前）
    group: Optional[str] = None                    # 拦截器 pre/post 分组，可为空


class HandlerRegistry:
    """处理器注册中心（可选能力：不注册不影响引擎加载行为）"""

    def __init__(self):
        self._handlers: dict[str, list[HandlerMeta]] = {}

    def register(self, meta: HandlerMeta) -> None:
        self._handlers.setdefault(meta.type, []).append(meta)

    def register_all(self, metas: list[HandlerMeta]) -> None:
        for m in metas:
            self.register(m)

    def list_handlers(self, type_name: str) -> list[HandlerMeta]:
        """按处理器类型列出（按 order 升序）"""
        return sorted(self._handlers.get(type_name, []), key=lambda m: m.order)

    def list_handlers_group(self, type_name: str, group: str) -> list[HandlerMeta]:
        """按处理器类型 + 分组列出（拦截器 pre/post）"""
        return sorted(
            (m for m in self._handlers.get(type_name, []) if m.group == group),
            key=lambda m: m.order)

    def list_handler_types(self) -> list[str]:
        """已注册的处理器类型名清单"""
        return list(self._handlers.keys())
