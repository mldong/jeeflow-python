"""门面第 **47** 个 action ``processTask/removeTaskActor``（issues/115 残留 · Python 栈腿）。

契约逐字依据＝ jeeflow-doc spec 06-facade.md §processTask/removeTaskActor（八条语义＋守卫次序），
判据基准＝ jeeflow-java ``RemoveTaskActorActionTest``（17 格逐格对照）。

它填的是 SPI 与门面之间那段空档：``spi.ProcessRepository.remove_task_actor`` 从第一天起就是必选方法、
两仓都实现，但门面没有对应 action，摘人只能靠 ``processTask/transfer``（摘 A **并**加 B）。

三个兄弟 action 的分工是本文件的判据主线：``surrogate``/``addCandidate`` 只加、``transfer`` 换人＋留痕、
本 action **只摘不加零留痕**（不写任务变量、不覆写任务 actorId/updateUser/updateTime、**不 fire 事件**
——issues/132 §11.3 定稿的事件集没有"摘人"码，码 7 的语义是"参与者被替换"）。每条负向都同时断言
"参与者一动不动"：摘人是删除操作，报错却删了一半比报错更糟。

``test_whitespace_padded_ids_are_removed_and_dirty_rows_survive`` 等三格对应门禁新格
「带空格入参可删 ∧ 空值不误删 ``actor_id=''`` 脏行 ∧ DELETE 拿的是行上的原值」：本栈内存仓写侧
（``MemoryRepository.add_task_actor``）会归一，正常路径建不出空串/未 trim 行 ⇒ 用继承内存仓的
:class:`_R115SpyRepo` 把脏行从外部塞进来，并记录每次喂进 DELETE 的实参。
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository
from jeeflow.facade import JeeflowFacade
from jeeflow.memory import MemoryExtRepository
from jeeflow.model import TaskState, UserInfo
from jeeflow.spi import UserProvider, IDGenerator, ExpressionEvaluator

import flows_resolver

FLOW_DIR = flows_resolver.dir()


# ─── 夹具 ────────────────────────────────────────────────────────────────────────

class _R115UserProv(UserProvider):
    async def get_user(self, user_id: str):
        return UserInfo(userId=user_id, realName=f"用户{user_id}", deptId="D01",
                        deptName="测试部门", postId="P01", postName="测试岗位")


class _R115IDGen(IDGenerator):
    def __init__(self):
        self.n = 0

    def next_id(self) -> int:
        self.n += 1
        return self.n


class _R115ExprEval(ExpressionEvaluator):
    async def eval(self, expr: str, vars: dict):
        return False


class _R115SpyRepo(MemoryRepository):
    """复刻"库里已经存在的历史脏行"与 ``DELETE ... actor_id IN (?)`` 的逐字语义。

    脏行只能从外部塞进来（内存仓写侧归一后建不出来）。``find_task_actors`` 把真人那一半与脏行
    并起来返回（与 SQL 仓一条裸 ``SELECT`` 同形——脏行本来就会被读出来），``remove_task_actor``
    记录每次喂进 DELETE 的实参，并按 ``IN`` 的精确等值命中删除（**不 trim、不归一**，同 SQL 那句 WHERE）。
    """

    def __init__(self):
        super().__init__()
        self._dirty: dict[int, list[str]] = {}
        self.remove_calls: list[list[str]] = []

    def seed_dirty_row(self, task_id: int, actor_id: str) -> None:
        self._dirty.setdefault(task_id, []).append(actor_id)

    def dirty_remaining(self, task_id: int) -> list[str]:
        return list(self._dirty.get(task_id, []))

    def real_actors(self, task_id: int) -> list[str]:
        """只取"真人"那一半（脏行不进这个读数，否则"其余参与人原样保留"那类判据会被脏行干扰）。"""
        return list(self._actors.get(task_id, []))

    async def find_task_actors(self, task_id):
        return [*self._actors.get(task_id, []), *self._dirty.get(task_id, [])]

    async def remove_task_actor(self, task_id, actors):
        self.remove_calls.append(list(actors))
        rows = self._dirty.get(task_id)
        if rows:
            drop = set(actors)
            self._dirty[task_id] = [r for r in rows if r not in drop]
        await super().remove_task_actor(task_id, actors)


async def _deploy_115(facade: JeeflowFacade) -> int:
    with open(os.path.join(FLOW_DIR, "01-simple.json"), encoding="utf-8") as f:
        r = await facade.flow("processDefine/deploy", {"content": f.read()})
    assert r["code"] == 0, r
    return int(r["data"]["processDefineId"])


async def _setup_115():
    """01-simple（start → apply[applicant] → task1[leader] → end）→ startAndExecute ⇒ 停在 task1。

    task1 的参与者就是 ``"leader"`` 一人；多参与者现场一律用兄弟 action ``addCandidate`` 造，
    不直接塞仓储。返回 ``(facade, spy_repo, instance_id, task_id, events)``，事件读数已清空
    （建流/发起那一段与本次判据无关）。
    """
    repo = _R115SpyRepo()
    eng = EngineImpl(repo, _R115UserProv(), _R115IDGen(), _R115ExprEval())
    events: list = []
    eng.add_event_listener(lambda evt: events.append(evt))
    facade = JeeflowFacade(eng, repo, MemoryExtRepository())
    define_id = await _deploy_115(facade)
    r = await facade.flow("processInstance/startAndExecute",
                          {"processDefineId": define_id, "operator": "zhangsan"})
    assert r["code"] == 0, r
    iid = int(r["data"]["processInstanceId"])
    doing = await repo.find_doing_tasks(iid)
    task = next(t for t in doing if t.taskName == "task1")
    events.clear()
    return facade, repo, iid, task.id, events


async def _add_actors(facade, task_id, *actors):
    r = await facade.flow("processTask/addCandidate",
                          {"processTaskId": task_id, "actorIds": list(actors)})
    assert r["code"] == 0, r


async def _remove(facade, task_id, actor_ids, operator):
    return await facade.flow("processTask/removeTaskActor",
                             {"processTaskId": task_id, "actorIds": actor_ids,
                              "operator": operator})


async def _finish_task(facade, task_id):
    """办结 task1 ⇒ 该任务离开 DOING（"历史任务"那一档的夹具）。"""
    r = await facade.flow("processTask/execute",
                          {"processTaskId": task_id, "operator": "leader", "submitType": 1})
    assert r["code"] == 0, r


def _ok(label, r):
    assert r["code"] == 0, f"{label} 应成功（code=0 + msg=成功），实得 {r}"
    assert r["msg"] == "成功", f"{label} 成功信封 msg 应逐字为「成功」，实得 {r}"


def _fail_exact(label, r, want):
    """断言失败信封 msg **逐字相等**（spec 同节：八栈文案一模一样）。

    本栈既有的 ``keyword in r["msg"]`` 只做包含，照不住"多带后缀/写成英文方言"这种分叉。
    """
    assert r["code"] == 99999999, f"{label} 应失败且 code=99999999（禁止静默成功），实得 {r}"
    assert r["msg"] == want, f'{label}: msg={r.get("msg")!r}, want 逐字 {want!r}'


# ─── 语义 1「只摘不加」＋ 正向核心 ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_removes_only_the_named_actor_and_keeps_the_rest():
    facade, repo, _, tid, _ = await _setup_115()
    await _add_actors(facade, tid, "9001", "9002")
    assert await repo.find_task_actors(tid) == ["leader", "9001", "9002"]

    r = await _remove(facade, tid, ["9001"], "flow.admin")

    _ok("只摘点名的人", r)
    assert await repo.find_task_actors(tid) == ["leader", "9002"], "只删点名的 9001，其余按顺序原样保留"
    assert r["data"] is None, f"data 出 null（spec 同节：前端消费面不读 data）: {r}"


@pytest.mark.asyncio
async def test_removes_several_actors_in_one_call():
    facade, repo, _, tid, _ = await _setup_115()
    await _add_actors(facade, tid, "9001", "9002", "9003")

    _ok("一次摘多人", await _remove(facade, tid, ["9001", "9002"], "flow.admin"))

    assert await repo.find_task_actors(tid) == ["leader", "9003"]


@pytest.mark.asyncio
async def test_comma_string_shape_removes_the_same_people():
    """逗号串腿与数组腿同判据（§2.11 第 1 行「两形一把尺子」，摘人腿不得另抄一份）。"""
    facade, repo, _, tid, _ = await _setup_115()
    await _add_actors(facade, tid, "9001", "9002")

    _ok("逗号串腿", await _remove(facade, tid, "9001, 9002 ", "flow.admin"))

    assert await repo.find_task_actors(tid) == ["leader"], "逗号串带空格照样命中"


# ─── 语义 3「归属判据同 transfer」 ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_self_removal_needs_no_privilege():
    facade, repo, _, tid, _ = await _setup_115()
    await _add_actors(facade, tid, "9001")

    _ok("本人摘自己", await _remove(facade, tid, ["9001"], "9001"))

    assert await repo.find_task_actors(tid) == ["leader"]


@pytest.mark.asyncio
async def test_removing_someone_else_without_privilege_is_rejected():
    """借道摘他人必须拦下（transfer 能"摘 A 加 B"是因为 A＝操作人本人，本 action 同理）。"""
    facade, repo, _, tid, _ = await _setup_115()
    await _add_actors(facade, tid, "9001")
    before = await repo.find_task_actors(tid)

    _fail_exact("借道摘他人", await _remove(facade, tid, ["9001"], "leader"), "无权限摘除该任务参与人")

    assert await repo.find_task_actors(tid) == before, "报错后一条都不许删"


@pytest.mark.asyncio
async def test_auto_system_operator_is_also_privileged():
    """``flow.auto`` 与 ``flow.admin`` 同档放行（大小写不敏感沿用本栈既有 ``lower()`` 写法）。"""
    facade, repo, _, tid, _ = await _setup_115()
    await _add_actors(facade, tid, "9001")

    _ok("flow.auto 代摘", await _remove(facade, tid, ["9001"], "flow.auto"))

    assert await repo.find_task_actors(tid) == ["leader"]
    _ok("FLOW.ADMIN 同档放行", await _remove(facade, tid, ["nobody-here"], "FLOW.ADMIN"))


# ─── 语义 5「不得摘空」：判据是集合差，不是入参条数 ─────────────────────────────

@pytest.mark.asyncio
async def test_never_empties_the_task():
    facade, repo, _, tid, _ = await _setup_115()
    assert await repo.find_task_actors(tid) == ["leader"]

    r = await _remove(facade, tid, ["leader"], "leader")

    _fail_exact("摘空", r, "至少需保留一名参与人")
    assert await repo.find_task_actors(tid) == ["leader"], "摘空会造出无人可办又无法重派的死单，人必须还在"


@pytest.mark.asyncio
async def test_mixed_non_participant_id_cannot_bypass_the_keep_one_floor():
    """绕过档：``actorIds`` 里混进非参与者 id，"入参条数 < 参与人数"这种判据会放过去，
    集合差判据必须照样拦下（spec 语义 5 的第二句）。"""
    facade, repo, _, tid, _ = await _setup_115()
    await _add_actors(facade, tid, "9001")

    _fail_exact("混入非参与者 id 绕过下限",
                await _remove(facade, tid, ["leader", "9001", "ghost"], "leader"),
                "至少需保留一名参与人")

    assert await repo.find_task_actors(tid) == ["leader", "9001"]


# ─── 语义 4「只作用于进行中任务」 ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_finished_task_is_protected():
    """历史参与人行是审批链的取证依据（approvalRecord 读全状态任务行），非 DOING 一律拦下。"""
    facade, repo, _, tid, _ = await _setup_115()
    await _finish_task(facade, tid)
    before = await repo.find_task_actors(tid)

    _fail_exact("已办结任务摘人", await _remove(facade, tid, ["leader"], "flow.admin"),
                "任务非进行中，不可摘除参与人")

    assert await repo.find_task_actors(tid) == before, "已办结任务的参与人行不得被改写历史"


# ─── 语义 2「不留痕、不 fire 事件」 ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_leaves_no_trace_and_fires_no_event():
    facade, repo, _, tid, events = await _setup_115()
    await _add_actors(facade, tid, "9001")
    # 任务行留痕列的"改前读数"：建单路径本来就会写 update_user/update_time（不是摘人写的），
    # 判据只能是"摘人这一步没动它"，**不能假定它本来是 None**。内存仓 find_task_by_id 返回 deepcopy，
    # 快照直接留住即可（变量字典也是深拷贝的一份）。
    before = await repo.find_task_by_id(tid)
    vars_before = dict(before.variables or {})
    update_user_before = before.updateUser
    update_time_before = before.updateTime
    events.clear()

    _ok("摘人", await _remove(facade, tid, ["9001"], "flow.admin"))

    assert events == [], f"摘人不在 132 定稿事件集里，一律不 fire（码 7 的语义是「参与者被替换」）: {events}"
    after = await repo.find_task_by_id(tid)
    for key in ("submitType", "tf_transferHistory", "tf_transferTo"):
        assert key not in (after.variables or {}), f"不留痕：任务变量里不得出现 {key}: {after.variables}"
    assert dict(after.variables or {}) == vars_before, \
        f"不写任何任务变量: before={vars_before} after={after.variables}"
    assert after.updateUser == update_user_before, "不覆写任务留痕列 updateUser"
    assert after.updateTime == update_time_before, "不覆写任务留痕列 updateTime"
    assert after.actorId in ("", None), f"不覆写任务 actorId 列: {after.actorId!r}"


# ─── 语义 7「幂等」：非参与者静默忽略，重放第二次仍成功 ─────────────────────────

@pytest.mark.asyncio
async def test_removing_a_non_participant_is_idempotent():
    facade, repo, _, tid, events = await _setup_115()
    await _add_actors(facade, tid, "9001")

    _ok("首次摘人", await _remove(facade, tid, ["9001"], "flow.admin"))
    assert await repo.find_task_actors(tid) == ["leader"]

    _ok("同一次摘人重放第二次应得成功信封（前端双点/集成层重放）",
        await _remove(facade, tid, ["9001"], "flow.admin"))
    assert await repo.find_task_actors(tid) == ["leader"], "重放不得再有副作用"
    assert events == [], "全程零事件（幂等腿也一样不 fire）"


# ─── 必填档（逐字文案）与守卫次序 ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_missing_arms_reuse_the_existing_error_envelope():
    """三档逐字文案：operator 缺省/纯空白 ⇒ ``operator 必填``（严禁回落 user1）；主键缺失或
    ``actorIds`` 丢完为空 ⇒ 与 ``surrogate`` 同一文案 ``processTaskId/actorIds 缺失``；查不到 ⇒ ``任务不存在``。"""
    facade, repo, _, tid, _ = await _setup_115()
    # 取证快照必须是副本：内存仓 find_task_actors 返回内部活列表，拿引用比引用会恒真
    before = list(await repo.find_task_actors(tid))

    _fail_exact("缺省 operator ⇒ 必填档", await _remove(facade, tid, ["leader"], None), "operator 必填")
    _fail_exact("纯空白 operator 也不给过", await _remove(facade, tid, ["leader"], "   "), "operator 必填")
    _fail_exact("主键空串 ⇒ 兄弟 action 同文案",
                await _remove(facade, "", ["9001"], "flow.admin"), "processTaskId/actorIds 缺失")
    _fail_exact("主键纯空白 ⇒ 同一档",
                await _remove(facade, "   ", ["9001"], "flow.admin"), "processTaskId/actorIds 缺失")
    _fail_exact("actorIds 丢完为空 ⇒ 兄弟 action 同文案",
                await _remove(facade, tid, ["", "  ", None], "flow.admin"), "processTaskId/actorIds 缺失")
    # spec 语义 8：缺参数档收齐 缺键/空串/纯空白/0/负数 五种形状——0 与负数不得改口成「任务不存在」
    _fail_exact("主键 0 ⇒ 缺参数档（不拿 0 当 id 去查）",
                await _remove(facade, 0, ["9001"], "flow.admin"), "processTaskId/actorIds 缺失")
    _fail_exact("主键负数 ⇒ 缺参数档",
                await _remove(facade, -1, ["9001"], "flow.admin"), "processTaskId/actorIds 缺失")
    _fail_exact("任务不存在", await _remove(facade, 424242, ["9001"], "flow.admin"), "任务不存在")

    assert await repo.find_task_actors(tid) == before, "七个报错档一条都不许删"


@pytest.mark.asyncio
async def test_guard_order_is_fixed_across_stacks():
    """守卫次序（spec 同节末尾那段，逐栈一致，不接受本栈自行排序）：``operator 必填`` 排在缺参数
    之前——否则"参数全缺"会先报主键缺失，把鉴权缺口藏进参数报错里；权限档排在 DOING 档之前——
    否则外人可以靠"任务已完成"探到别人的任务状态。"""
    facade, repo, _, tid, _ = await _setup_115()

    _fail_exact("operator 必填排在主键缺失档之前",
                await _remove(facade, "", ["9001"], None), "operator 必填")

    await _finish_task(facade, tid)  # task1 已非 DOING，operator 又不是参与者
    _fail_exact("权限档先于非进行中档",
                await _remove(facade, tid, ["leader"], "outsider"), "无权限摘除该任务参与人")


# ─── 门禁新格：带空格入参可删 ∧ 空值不误删 actor_id='' 脏行 ─────────────────────

@pytest.mark.asyncio
async def test_whitespace_padded_ids_are_removed_and_dirty_rows_survive():
    """``" 9001 "`` 必须命中库里的人（硬要求②「落库与比较一律取 trim 后的值」）；同时喂进 DELETE 的
    实参永不能含空串/纯空白——历史 ``actor_id=''`` 脏行是 ``DELETE ... actor_id IN (?)`` 的受害者，
    判据打在实参与脏行存活两处。"""
    facade, repo, _, tid, _ = await _setup_115()
    await _add_actors(facade, tid, "9001", "9002")
    repo.seed_dirty_row(tid, "")      # 复刻历史脏行（内存仓写侧归一后建不出来）
    repo.seed_dirty_row(tid, "   ")   # 纯空白那一支也算脏行

    _ok("带空格入参 + 空值混喂",
        await _remove(facade, tid, [" 9001 ", "", None, "   ", "9002"], "flow.admin"))

    assert repo.real_actors(tid) == ["leader"], "带空格的入参删得掉真人，其余参与人不动"
    assert repo.dirty_remaining(tid) == ["", "   "], "空串/纯空白绝不能喂进 DELETE ⇒ 历史脏行必须原样还在"
    assert len(repo.remove_calls) == 1, f"只该有一次 DELETE 调用: {repo.remove_calls}"
    for call in repo.remove_calls:
        assert all(str(a).strip() != "" for a in call), f"喂给 DELETE 的实参不得含空串/纯空白: {call}"


# ─── 语义 6「匹配取归一值、DELETE 取行上的原值」＋语义 5「脏行不算一个人」 ───────

@pytest.mark.asyncio
async def test_untrimmed_historical_row_is_matched_and_deleted_by_row_value():
    """库里的行是修复前落下的未 trim 原值 ``" 9101 "``，入参给 ``"9101"``：判据必须把它当成同一个人
    **并真删掉**，且喂进 DELETE 的实参是**那一行的原值**。反面形状＝拿归一值去删：判成同一人却一条
    没删，门面报成功而被摘的人待办还在（**假成功**）。"""
    facade, repo, _, tid, _ = await _setup_115()
    repo.seed_dirty_row(tid, " 9101 ")  # 历史未 trim 行（写侧归一后正常路径造不出来）

    _ok("归一匹配未 trim 历史行", await _remove(facade, tid, ["9101"], "flow.admin"))

    assert repo.real_actors(tid) == ["leader"], "真人那一半不许被动"
    assert repo.dirty_remaining(tid) == [], "未 trim 的历史行应被归一匹配命中并删除"
    assert repo.remove_calls[-1] == [" 9101 "], \
        f"DELETE 的实参是行上的原值，不是归一后的值（否则删不掉）: {repo.remove_calls}"


@pytest.mark.asyncio
async def test_dirty_rows_do_not_prop_up_the_keep_one_floor():
    """「至少剩一人」的下限按**能办单的人数**算：库里只剩 ``actor_id=''`` 脏行时，摘走最后一个真人
    必须报错——脏行谁也办不了，拿它撑住下限等于让"摘空"伪装成成功。"""
    facade, repo, _, tid, _ = await _setup_115()
    repo.seed_dirty_row(tid, "")
    repo.seed_dirty_row(tid, "   ")

    _fail_exact("脏行不算一个人", await _remove(facade, tid, ["leader"], "flow.admin"),
                "至少需保留一名参与人")

    assert repo.real_actors(tid) == ["leader"], "报错后真人那行还在"


# ─── 回归：三个兄弟 action 的分工不被稀释 ───────────────────────────────────────

@pytest.mark.asyncio
async def test_sibling_actions_keep_their_own_semantics():
    facade, repo, _, tid, events = await _setup_115()

    _ok("surrogate 只加", await facade.flow("processTask/surrogate",
                                           {"processTaskId": tid, "actorIds": ["9101"]}))
    assert await repo.find_task_actors(tid) == ["leader", "9101"], "surrogate 仍旧只加不摘"

    _ok("removeTaskActor 只摘", await _remove(facade, tid, ["9101"], "flow.admin"))
    assert await repo.find_task_actors(tid) == ["leader"], "摘人不带加人"

    # transfer 换人语义不变：摘 A 加 B ＋ submitType=7 留痕照写（本 action 不碰那条腿）
    events.clear()
    _ok("transfer 换人", await facade.flow("processTask/transfer",
                                          {"processTaskId": tid, "operator": "leader",
                                           "fromActor": "leader", "toActor": "boss"}))
    assert await repo.find_task_actors(tid) == ["boss"], "transfer 换人后参与人"
    stored = await repo.find_task_by_id(tid)
    assert int(stored.variables["submitType"]) == 7, "transfer 仍写 submitType=7 留痕"
    assert events, "transfer 仍要 fire TASK_TRANSFER（只有摘人这一条腿不发事件）"
