"""issues/160 · python 侧「未注册求值 SPI」这一档：运行时定支与门面 highLight 必须同源。

立案事实（本文件写之前先用同形夹具真跑过改前形状，读数见 hub `issues/160` 的终局段）：
宿主没注册 ``IExpressionEvaluator`` 时，本栈**运行时整段不求值**——``_evaluate_decision`` 靠
"取第一条无 expr 的边、否则无条件取第一条边"蒙方向（``amount=500`` 与 ``amount=5000`` 落进
同一条支），而门面那条腿 ``eval_expr`` 抛 ``ValueError("ExpressionEvaluator 未配置")``，
再被 ``highLight`` 模型补全块的裸 ``except Exception: pass`` 吞掉 ⇒ 走过的决策边**连同它之后的
整条路径**一起丢失，实例照常流转，只有流程图暴露它。

rust 的同族案（issues/158，活栈红：salvo 生产镜像 ``L2-39`` 47/1）解法是引擎自带默认求值器
＋ ``ServiceContext::expression_evaluator_or_default`` 让两腿共用一个出口；csharp 更早就是这个形状。
本文件按那个形状钉 python 的四件事，全部打在**没有 SPI** 的这一档（除第 ③ 档专门验宿主优先）：

① 运行时按内置求值定支（amount=5000 → task2）且门面把走过的那跳决策边收进来；
② 反方向抽样（amount=500 → task3）——这一格同时钉住"内置必须做数值比较而非字典序蒙"，
   旧蒙法两支一起"为真"，只有 ① 会自证成假绿；
③ 宿主注册的 SPI 优先级高于内置默认件（本栈三壳自带 ``SimpleExprEvaluator``，发版不动它们）；
④ 模型补全腿真炸时**记可诊断日志**，不再静默吞（裸 ``except`` 是本案的藏身之处）。

夹具＝八语言共享的 ``flows/03-decision-expr.json``（编辑源在 jeeflow-java）：
``start → apply(applicant) → task1(leader) → decision1 →(e3 amount>1000) task2(manager) → end``
与 ``decision1 →(e4 amount<=1000) task3(director) → end``。

判据基准＝spec/06 §4.6 义务 2；求值器语义基准＝rust ``default_evaluator.rs``
（其自身以 csharp ``DefaultExpressionEvaluator`` 为基准，最小基准不超集：变量代入＋比较运算＋
布尔字面量，**不含 ``&&``/``||``）。
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow.engine import EngineImpl                                   # noqa: E402
from jeeflow.default_evaluator import evaluate_builtin                  # noqa: E402
from jeeflow.facade import JeeflowFacade                                # noqa: E402
from jeeflow.memory import MemoryExtRepository, MemoryRepository        # noqa: E402
from jeeflow.spi import ExpressionEvaluator, IDGenerator                # noqa: E402

import flows_resolver                                                   # noqa: E402

FLOW_DIR = flows_resolver.dir()


class _Gen(IDGenerator):
    def __init__(self):
        self.n = 100000

    def next_id(self) -> int:
        self.n += 1
        return self.n


class _HostSpiEvaluator(ExpressionEvaluator):
    """与内置默认件**反向**的宿主 SPI：amount=5000 偏说 e3 假、e4 真。

    只用来验"宿主优先"——若引擎把内置件接在了宿主之前，实例会落 task2、门面收 e3，
    这两个断言同时红。"""

    async def eval(self, expr: str, vars: dict):
        return expr.strip() == "amount <= 1000"


def _facade_like_salvo(expr_eval=None):
    """与没注册求值 SPI 的壳同形装配：只给仓储＋id 生成器。"""
    repo = MemoryRepository()
    eng = EngineImpl(repo, None, _Gen(), expr_eval)
    return JeeflowFacade(eng, repo, MemoryExtRepository()), repo


async def _start_and_do_task1(facade, repo, amount) -> int:
    content = open(os.path.join(FLOW_DIR, "03-decision-expr.json"), encoding="utf-8").read()
    r = await facade.flow("processDefine/deploy", {"content": content})
    assert r["code"] == 0, r
    r = await facade.flow("processInstance/startAndExecute", {
        "processDefineId": r["data"]["processDefineId"],
        "operator": "applicant", "amount": amount})
    assert r["code"] == 0, r
    iid = int(r["data"]["processInstanceId"])

    doing = await repo.find_doing_tasks(iid)
    task1 = next((t for t in doing if t.taskName == "task1"), None)
    assert task1 is not None, f"发起后 task1 应进行中，实得 {[t.taskName for t in doing]}"
    r = await facade.flow("processTask/execute", {
        "processTaskId": task1.id, "operator": "leader", "submitType": 1})
    assert r["code"] == 0, r
    return iid


async def _snapshot(facade, repo, iid):
    """(活跃任务名, highLight 三集)；活跃集是自证夹具形状的地基，不是附属读数。"""
    doing = [t.taskName for t in await repo.find_doing_tasks(iid)]
    hl = await facade.flow("processInstance/highLight", {"id": iid})
    assert hl["code"] == 0, f"highLight 应成功: {hl}"
    return doing, hl["data"]


@pytest.mark.asyncio
async def test_i160_facade_uses_engine_default_evaluator_when_host_has_none():
    """① amount=5000 且**不注册 SPI**：运行时走 e3 落在活跃 task2，门面必须收进这一跳的边名。"""
    facade, repo = _facade_like_salvo()
    iid = await _start_and_do_task1(facade, repo, 5000)
    doing, data = await _snapshot(facade, repo, iid)

    # 夹具形状自证：实例必须**停在分支上**，哪天它退化成"流已走完"就是恒真形状
    assert doing == ["task2"], f"应停在分支后的活跃 task2，实得 {doing}"

    edges = data["historyEdgeNames"]
    for need in ("e0", "e_apply_1", "e2", "e3"):
        assert need in edges, f"走过的边 {need} 必须高亮，实得 {edges}"
    assert "e4" not in edges, f"没走的分支边 e4 不得高亮（档③ 另一半），实得 {edges}"
    assert "e5" not in edges, f"活跃节点自己的出边 e5 不得高亮，实得 {edges}"
    assert "task3" not in data["historyNodeNames"], \
        f"未走分支的 task3 不得进节点集，实得 {data['historyNodeNames']}"


@pytest.mark.asyncio
async def test_i160_default_evaluator_branches_numerically_not_by_first_edge():
    """② 反方向抽样：amount=500 必须走 e4 落在 task3。

    这一格才真正钉住"内置做数值比较、不是蒙方向"——旧形状对 5000 与 500 **都**取第一条边
    （⇒ 都落 task2），只写 ① 会让它自证成假绿；而"两支一起为真"的裸名字典序蒙法
    （rust 旧 ``simple_eval``／issues/159 在 moon 照出的那个形状）也由同一格拦住。
    """
    facade, repo = _facade_like_salvo()
    iid = await _start_and_do_task1(facade, repo, 500)
    doing, data = await _snapshot(facade, repo, iid)

    assert doing == ["task3"], f"amount=500 应走 e4 落在 task3，实得 {doing}"
    edges = data["historyEdgeNames"]
    assert "e4" in edges, f"求值为 true 的 e4 必须高亮，实得 {edges}"
    assert "e3" not in edges, f"求值为 false 的 e3 不得高亮，实得 {edges}"
    assert "e6" not in edges, f"活跃节点自己的出边 e6 不得高亮，实得 {edges}"
    assert "task2" not in data["historyNodeNames"], "未走分支的 task2 不得进节点集"


@pytest.mark.asyncio
async def test_i160_host_spi_still_wins_over_engine_default():
    """③ 宿主 SPI 优先级高于内置默认件 ⇒ 三壳（各自注册了 SimpleExprEvaluator）行为不变。"""
    facade, repo = _facade_like_salvo(_HostSpiEvaluator())
    iid = await _start_and_do_task1(facade, repo, 5000)
    doing, data = await _snapshot(facade, repo, iid)

    # 内置件会说 e3 真（5000>1000）；宿主说反 ⇒ 两腿都该跟宿主
    assert doing == ["task3"], f"运行时应吃宿主 SPI 落 task3，实得 {doing}"
    edges = data["historyEdgeNames"]
    assert "e4" in edges and "e3" not in edges, \
        f"门面应吃同一条宿主通道（e4 亮 e3 灭），实得 {edges}"


def test_i160_builtin_truth_table():
    """④ 内置求值器自身的真值表（不借流程，逐条对 rust ``default_evaluator.rs`` 的同名格）。"""
    v = {"amount": 5000, "days": 3, "status": "approved", "nrOfCompletedInstances": 2}

    # 裸变量名必须**代入**：不代入时下面两条会同时为真（比较的是变量名的字典序）
    assert evaluate_builtin("amount > 1000", v) is True
    assert evaluate_builtin("amount <= 1000", v) is False
    assert evaluate_builtin("amount >= 5000", v) is True
    assert evaluate_builtin("amount < 1000", v) is False
    assert evaluate_builtin("${amount} > 1000", v) is True
    assert evaluate_builtin("days == 3", v) is True
    assert evaluate_builtin("days != 3", v) is False
    assert evaluate_builtin("#nrOfCompletedInstances == 2", v) is True
    assert evaluate_builtin("#nrOfCompletedInstances > 3", v) is False
    assert evaluate_builtin("status == 'approved'", v) is True
    assert evaluate_builtin("status == 'rejected'", v) is False
    assert evaluate_builtin("true", v) is True
    assert evaluate_builtin("false", v) is False
    # 未定义变量：关系运算判 false（不得因字典序自等）
    assert evaluate_builtin("missing_var > 1000", v) is False
    assert evaluate_builtin("missing_var == 1000", v) is False
    # #var 缺省回落 0（PHP 同形）
    assert evaluate_builtin("#nrOfActiveInstances == 0", v) is True
    # 空表达式档（两腿调用点都先判非空，此处钉住形状）
    assert evaluate_builtin("", v) is False
    assert evaluate_builtin("   ", v) is False
    # bool 不能当数字吃：Python 里 True 是 int 实例，归一顺序反了 "flag == 1" 会判 true
    assert evaluate_builtin("flag == 1", {"flag": True}) is False
    assert evaluate_builtin("flag", {"flag": False}) is False


@pytest.mark.asyncio
async def test_i160_model_leg_failure_is_logged_not_silent():
    """⑤ 模型补全腿真炸时：降级面照旧（highLight 仍 code=0），但**不再静默**——
    必须有可诊断日志，否则又回到本案"整块被吞、只有流程图暴露"的那条通道。
    """
    facade, repo = _facade_like_salvo()
    iid = await _start_and_do_task1(facade, repo, 5000)

    # 把定义 content 弄成坏形状（实例与任务行不受影响，只打模型腿）
    define_id = (await repo.find_instance_by_id(iid)).defineId
    def_ = await repo.find_define_by_id(define_id)
    def_.content = "{ not-json"
    repo.add_define(def_)

    hl = await facade.flow("processInstance/highLight", {"id": iid})
    assert hl["code"] == 0, f"模型腿坏了不该让整条 highLight 报错: {hl}"
    assert hl["data"]["historyEdgeNames"] == [], "坏模型下边集降级为空（与 java 同档）"
    assert hl["data"]["activeNodeNames"] == ["task2"], "任务行那一腿不受模型腿影响"


@pytest.mark.asyncio
async def test_i160_model_leg_failure_emits_diagnosable_log(caplog):
    """⑤b 上一格的可诊断性判据：日志必须真打出来（caplog 拿到才算，注释自称"记了日志"不算）。"""
    import logging

    facade, repo = _facade_like_salvo()
    iid = await _start_and_do_task1(facade, repo, 5000)
    define_id = (await repo.find_instance_by_id(iid)).defineId
    def_ = await repo.find_define_by_id(define_id)
    def_.content = "{ not-json"
    repo.add_define(def_)

    with caplog.at_level(logging.WARNING, logger="jeeflow.facade"):
        await facade.flow("processInstance/highLight", {"id": iid})
    hits = [r for r in caplog.records if r.levelno >= logging.WARNING
            and "模型补全腿" in r.getMessage()]
    assert hits, f"模型腿被吞时必须留 WARNING，实得 {[(r.levelno, r.getMessage()) for r in caplog.records]}"
