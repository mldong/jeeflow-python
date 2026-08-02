"""引擎元数据能力测试（v1.4.0，issues/04）——枚举字典 + SPI 实现清单"""
import pytest

from jeeflow.metadata import (
    HandlerMeta, HandlerRegistry,
    enum_dict, enum_dict_keys,
)


# ─── 枚举字典 ────────────────────────────────────────────────────────────────

def test_enum_dict_keys():
    keys = enum_dict_keys()
    assert len(keys) == 7
    # 对齐 boot3 字典 key（存量前端零改动）
    assert "wf_process_define_state" in keys
    assert "wf_process_instance_state" in keys
    assert "wf_process_submit_type" in keys
    assert "wf_process_task_state" in keys
    assert "wf_process_task_type" in keys
    assert "wf_process_task_perform_type" in keys
    assert "wf_countersign_type" in keys


def test_instance_state_dict():
    items = enum_dict("wf_process_instance_state")
    assert len(items) == 7
    assert items[0].value == "10" and items[0].label == "进行中"
    assert items[4].value == "45" and items[4].label == "已拒绝"
    assert items[6].value == "99" and items[6].label == "已废弃"


def test_submit_type_dict():
    items = enum_dict("wf_process_submit_type")
    assert len(items) == 8
    assert items[0].value == "0" and items[0].label == "发起申请"
    assert items[7].value == "20" and items[7].label == "拒绝申请"


def test_unknown_key_returns_empty():
    assert enum_dict("wf_no_such_dict") == []


# ─── SPI 实现清单 ────────────────────────────────────────────────────────────

def test_handler_registry():
    r = HandlerRegistry()
    r.register(HandlerMeta(type="AssignmentHandler", className="com.example.DeptLeaderHandler",
                           displayName="部门领导审批", order=2))
    r.register(HandlerMeta(type="AssignmentHandler", className="com.example.BossHandler",
                           displayName="老板审批", order=1))
    r.register(HandlerMeta(type="FlowInterceptor", className="com.example.TimeInterceptor",
                           displayName="耗时记录", order=0, group="post"))
    r.register(HandlerMeta(type="FlowInterceptor", className="com.example.LogInterceptor",
                           displayName="日志记录", order=1, group="pre"))

    assignments = r.list_handlers("AssignmentHandler")
    assert len(assignments) == 2
    # order 升序
    assert assignments[0].className == "com.example.BossHandler"
    assert assignments[0].displayName == "老板审批"

    pre = r.list_handlers_group("FlowInterceptor", "pre")
    assert len(pre) == 1 and pre[0].className == "com.example.LogInterceptor"
    post = r.list_handlers_group("FlowInterceptor", "post")
    assert len(post) == 1 and post[0].className == "com.example.TimeInterceptor"
    assert r.list_handlers_group("FlowInterceptor", "unknown") == []


def test_empty_registry():
    r = HandlerRegistry()
    assert r.list_handlers("AssignmentHandler") == []
    assert r.list_handler_types() == []
