"""任务行携带「所属流程实例的状态」（hub issues/250）——内存仓 + 门面出口腿。

契约（跨语言逐字一致）：新键 ``instanceState``（整数）＝该行所属流程实例的 state
（10 进行中 / 20 已完成 / 30 撤回 / 40 终止 / 45 拒绝 / 50 挂起 / 99 废弃）；来源＝列表查询
本来就关联的 ``wf_process_instance``（别名 pi）的 ``pi.state``，SQL 别名 ``instance_state``；
**待办 todoList 与已办 doneList 都要带**；不新增关联、不解析流程定义、不改任何撤回判据。

本文件两格（内存仓＋门面出口）：
  ① 实例进行中 ⇒ 待办行 ``instanceState == 10``；
  ② 末节点办结使实例变成 20 之后再读已办行 ⇒ ``taskState`` 仍是 20，但 ``instanceState == 20``。
合起来挡住「恒 10 / 恒 20 / 恒 None」三种假绿。

「拿 taskState 冒充 instanceState」那一格（两态分离：申请行已办结 20 而实例仍 10）放在**真 SQL 腿**
``tests/jdbc_test.py`` 的 ⑳ 组：内存仓的已办归属按 ``t.actorId`` 过滤、与真库按 ``t.operator`` 列
过滤不同口径，申请行在内存腿读不到，所以不在这边假装覆盖。

前端正是靠 ② 把实例已结束的
已办行撤回按钮置灰（任务行自身的 taskState 照不出这一格：实例办结后已办行照样是 20）。

⚠️ 本文件只打内存仓。**真 SQL 腿在 tests/jdbc_test.py 的 ⑳ 组**（page_todo_tasks /
page_done_tasks 的真库投影 + 门面 camelCase 出口）：java 侧实测摘掉 SQL 投影读之后，
内存仓那 66 格全绿、只有真库腿报红（issues/249 同形教训）⇒ 只加内存仓断言等于没加。
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository                      # noqa: E402
from jeeflow.facade import JeeflowFacade                              # noqa: E402
from jeeflow.memory import MemoryExtRepository                        # noqa: E402
from jeeflow.model import InstanceState, TaskState, UserInfo          # noqa: E402
from jeeflow.spi import IDGenerator, UserProvider                     # noqa: E402

import flows_resolver                                                 # noqa: E402

FLOW_DIR = flows_resolver.dir()


class _UserProv(UserProvider):
    async def get_user(self, user_id: str):
        return UserInfo(userId=user_id, realName=f"用户{user_id}", deptId="D01",
                        deptName="测试部门", postId="P01", postName="测试岗位")


class _IDGen(IDGenerator):
    def __init__(self):
        self.n = 0

    def next_id(self) -> int:
        self.n += 1
        return self.n


def _setup():
    repo = MemoryRepository()
    return EngineImpl(repo, _UserProv(), _IDGen()), repo


async def _deploy_start(facade, fixture, operator):
    with open(os.path.join(FLOW_DIR, fixture), encoding="utf-8") as f:
        content = f.read()
    r = await facade.flow("processDefine/deploy", {"content": content})
    assert r["code"] == 0, r
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": r["data"]["processDefineId"], "operator": operator,
                           "amount": "1000"})
    assert r["code"] == 0, r
    return r


@pytest.mark.asyncio
async def test_list_row_carries_instance_state():
    """issues/250：待办行带实例态 10；实例办结后已办行 taskState 仍 20 而 instanceState 变 20。"""
    eng, repo = _setup()
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())

    r = await _deploy_start(facade, "01-simple.json", "zhangsan")
    instance_id = int(r["data"]["processInstanceId"])

    # ── ① 实例进行中 ⇒ 待办行 instanceState = 10 ──
    inst = await repo.find_instance_by_id(instance_id)
    assert int(inst.state) == int(InstanceState.DOING), f"前置：实例应进行中 → {inst.state}"
    r = await facade.flow("processTask/todoList", {"operator": "leader"})
    assert r["code"] == 0, r
    assert r["data"]["rows"], f"正向对照：leader 应有待办，否则下面的断言恒真空 → {r}"
    row = [x for x in r["data"]["rows"] if int(x["processInstanceId"]) == instance_id]
    assert len(row) == 1, f"本单待办行应恰一条 → {r['data']['rows']}"
    row = row[0]
    assert "instanceState" in row, f"待办行出口必须投出 camelCase 键 instanceState → {sorted(row)}"
    assert isinstance(row["instanceState"], int) and not isinstance(row["instanceState"], bool), \
        f"instanceState 必须是整数（不是串/不是 None）→ {row['instanceState']!r}"
    assert row["instanceState"] == 10, f"实例进行中 ⇒ 待办行 instanceState=10 → {row['instanceState']}"
    # 仓储层同一格（门面出口不得靠自己算一个值糊上去）
    todo_rows, _ = await repo.page_todo_tasks(1, 100, "leader")
    todo_row = [x for x in todo_rows if x.processInstanceId == instance_id]
    assert len(todo_row) == 1 and int(todo_row[0].instanceState) == 10, \
        f"内存仓 TaskRow.instanceState 应随 pi 关联投出 10 → {todo_row}"

    # ── ①b 两态分离格在**真 SQL 腿**（tests/jdbc_test.py ⑳「两态分离格」）。
    # 内存腿这里做不了：内存仓 page_done_tasks 的归属按 `t.actorId` 过滤（repository/base.py
    # 与真库按 `t.operator` 列过滤不同口径），申请行 actorId 不是发起人 id ⇒ 读不到那行。
    # 该格的价值是挡「instanceState 拿 taskState 冒充」，真库腿已覆盖，这里不重复也不假装覆盖。

    # ── ② 办结末节点 ⇒ 实例 20；已办行 taskState 仍 20，但 instanceState 必须是 20 ──
    doing = await repo.find_doing_tasks(instance_id)
    assert len(doing) == 1 and doing[0].taskName == "task1", doing
    r = await facade.flow("processTask/execute",
                          {"processTaskId": doing[0].id, "operator": "leader", "submitType": 1})
    assert r["code"] == 0, r
    inst = await repo.find_instance_by_id(instance_id)
    assert int(inst.state) == int(InstanceState.DONE), f"前置：末节点办结后实例应已完成 → {inst.state}"

    r = await facade.flow("processTask/doneList", {"operator": "leader"})
    assert r["code"] == 0, r
    assert r["data"]["rows"], f"正向对照：leader 应有已办 → {r}"
    after = [x for x in r["data"]["rows"] if int(x["processInstanceId"]) == instance_id]
    assert len(after) == 1, f"本单已办行应恰一条 → {r['data']['rows']}"
    after = after[0]
    assert "instanceState" in after, f"已办行出口同样必须带 instanceState → {sorted(after)}"
    assert after["taskState"] == 20, f"前置：任务行自身仍是已完成 → {after['taskState']}"
    assert after["instanceState"] == 20, \
        f"实例已结束 ⇒ 已办行 instanceState 必须是 20，否则前端无从置灰 → {after['instanceState']}"
    done_rows, _ = await repo.page_done_tasks(1, 100, "leader")
    done_row = [x for x in done_rows if x.processInstanceId == instance_id]
    assert len(done_row) == 1, done_row
    assert int(done_row[0].taskState) == int(TaskState.DONE), done_row[0]
    assert int(done_row[0].instanceState) == 20, \
        f"内存仓已办行 instanceState 应读**这一刻**的实例态（不是建单快照）→ {done_row[0].instanceState}"