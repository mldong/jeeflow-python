"""action → 权限码映射默认件的跨栈对齐测试（hub issues/248 python 腿）。

语义参照实现＝java ``DefaultActionPermissionProvider``（issues/29，其测试是权威），
go 腿 ``jeeflow-go/spi/action_permission_test.go`` 同形。断言的是"**与 java 逐条一致**"，
不是"与这份实现一致"——改这张表必须同时改 java 那份，否则本测试必红。
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow.action_permission import (                                          # noqa: E402
    ActionPermissionProvider, DefaultActionPermissionProvider,
    NO_PERM_ACTIONS, OR_RULES,
    action_permission_provider_meta, get_action_permission_provider,
    permission_codes, set_action_permission_provider,
)
from jeeflow.metadata import HandlerRegistry                                     # noqa: E402


# ─── 三档判据（正向抽样）─────────────────────────────────────────────────────

def test_default_derivation():
    # ③ 默认派生：wf:{action 的 / 换 :}
    assert permission_codes("processDesign/save") == ["wf:processDesign:save"]
    assert permission_codes("processDefine/page") == ["wf:processDefine:page"]


def test_or_rules():
    # ② OR 族（boot3 @SaCheckPermission(mode=OR)：任选其一即有权）
    assert permission_codes("processDefine/detail") == [
        "wf:processDefine:detail", "wf:processDesign:listByType"]
    assert permission_codes("processTask/candidatePage") == [
        "wf:processTask:execute", "wf:processTask:candidatePage"]
    assert permission_codes("processDefine/getLastByName") == [
        "wf:processDefine:detail", "wf:processDesign:listByType",
        "wf:processDefine:getLastByName"]


def test_no_perm_family():
    # ① 放行族：None = 登录即可（11 条逐条钉死，与 java NO_PERM_ACTIONS 同源）
    for action in (
        "processInstance/detail", "processInstance/highLight", "processInstance/approvalRecord",
        "processInstance/getAssigneeTextData", "processInstance/bizData",
        "processTask/detail", "processTask/addCandidate", "processTask/latest",
    ):
        assert permission_codes(action) is None, f"{action} 应放行"
    # 统计三条（issues/103 收口：普通用户分析页被默认规则拦截的修复）
    for stats in ("processInstance/stats/overview", "processInstance/stats/trend",
                  "processInstance/stats/group"):
        assert permission_codes(stats) is None, f"统计 action {stats} 应放行"


def test_none_action_is_open():
    # 对齐 java `if (action == null) return null;`
    assert permission_codes(None) is None
    assert DefaultActionPermissionProvider().permission_codes(None) is None


# ─── issues/246／247 B 案粒度 + 反向哨兵 ─────────────────────────────────────

def test_new_actions_ride_execute_code():
    # 两枚新 action 恰为 ["wf:processTask:execute"]：不派生成
    # wf:processTask:recall／wf:processTask:revokeDynamicCountersign，也不新建按钮种子
    for action in ("processTask/recall", "processTask/revokeDynamicCountersign"):
        assert permission_codes(action) == ["wf:processTask:execute"], \
            f"{action} 须复用 execute（B 案，owner 2026-11-04）"
    # 反向哨兵：没配进两张表的 action 必须仍走默认派生，
    # 否则"整族都被塞进 execute"这种错法会被本测试放过。
    assert permission_codes("processTask/transfer") == ["wf:processTask:transfer"]
    assert permission_codes("processTask/removeTaskActor") == ["wf:processTask:removeTaskActor"]


# ─── 表形状锚（与 java OR_RULES 7 条／NO_PERM_ACTIONS 11 条对齐）─────────────

def test_table_shape_matches_java():
    # 刻意写死数字：不是"会漂的读数"，而是跨栈契约的形状锚——加一条要八栈同批改
    assert len(OR_RULES) == 7, f"OR 规则条数应为 7（对齐 java），现读 {len(OR_RULES)}"
    assert len(NO_PERM_ACTIONS) == 11, f"放行清单条数应为 11（含统计三条），现读 {len(NO_PERM_ACTIONS)}"


# ─── 覆盖与回落（java ServiceContext 同语义）─────────────────────────────────

class _CustomProvider(ActionPermissionProvider):
    def permission_codes(self, action):
        return ["custom:1"]


@pytest.fixture
def restore_provider():
    yield
    set_action_permission_provider(None)  # 用例之间不互相污染


def test_override_and_fallback(restore_provider):
    assert isinstance(get_action_permission_provider(), DefaultActionPermissionProvider)
    set_action_permission_provider(_CustomProvider())
    assert permission_codes("anything/here") == ["custom:1"], "覆盖注册未生效"
    set_action_permission_provider(None)
    assert permission_codes("processDesign/save") == ["wf:processDesign:save"], "回落内置默认件"


# ─── SPI 元数据清单（java HandlerRegistry／go Meta 同约定）───────────────────

def test_provider_meta_exposed():
    typ, class_name = action_permission_provider_meta()
    assert typ == "ActionPermissionProvider"
    assert class_name == "com.mldong.jeeflow.spi.DefaultActionPermissionProvider"


def test_meta_registered_in_handler_registry():
    metas = HandlerRegistry().list_handlers("ActionPermissionProvider")
    assert len(metas) == 1
    assert metas[0].className == "com.mldong.jeeflow.spi.DefaultActionPermissionProvider"
