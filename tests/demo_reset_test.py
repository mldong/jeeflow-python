"""demo /api/reset 契约测试（issues/11）：清空业务数据并重载种子。

T003 起 reset 会复跑业务种子 driver（引擎真实启动），断言从「清空」改为「回到矩阵规模」。

⚠️ 规模数值 2026-09-30 按 issues/141 G9 裁定重算（16/9 ⇒ **15/10**，实例总数 25 不变）：
   owner 原话「这个得根据任务类型来，自定义类型这种记录类的，不会有参与人，是正常行为」
   （立法见 jeeflow-doc/docs/spec/02-flow-definition.md §6.1，只读）。种子矩阵里
   `IN_PROGRESS` 第 15 行（I15）用的是 `flows/08-custom-node.json`（define=10），
   该流程只有 start → apply(任务) → custom1(记录类) → end 三个节点：
   - **旧数值 16/9 出自被撤掉的兜底做法**——上一笔 commit `ac8b557` 为消本文件长期红，
     在 `engine._create_task` 给"参与者解析为空"的 custom 节点兜底挂当前操作人建了一行
     DOING，于是 I15 停在 state=10、计数被抬成 16/9。那个形状 spec §6.1 明写禁止
     （②兜底把行挂给当前操作人＝伪造一条他不该收到的待办）。
   - **改回 java 同构形状后**：custom 节点落一条 DONE 历史行、令牌继续流到 end ⇒
     I15 与 F4 一样办结成 state=20 ⇒ 本栈实测读数 **15 进行中 / 10 已完成**
     （任务行同步：DOING 25→24、DONE 49→50）。
   - **java 基准**：`CustomModel.exec` 无条件 `createHistoryTask`（task_state=FINISHED）
     ＋ `runOutTransition` ⇒ 该定义发起即办结（java 自己 `JeeflowFullTest#test08CustomNode`
     的注释逐字写着「记录 history task，接着执行输出边 → end → finish」）；
     即 java-faithful 也是 15/10 这一侧。⚠️ java demo 的**运行态**读数本轮未实测（未验），
     基准取自 java 源码与那条测试注释。
   这不是"改期望值蒙过红"，而是 spec §6.1 硬结论 3 说的"跟着裁定重算数值"。

   ⚠️ 2026-09-30 追加复核（§6.1 硬结论 1 的**任务类**那一腿兜底同日撤掉后）：读数 **不变，仍 15/10**
   （任务行也仍 24 DOING / 50 DONE，实例总数 25）。原因实测过：种子矩阵里没有任何"任务类节点参与者
   解析为空"的行（reset 后零参与者任务行计数为 0），撤兜底只改这种行的参与者集合，改不到状态分布；
   该形状由 jeeflow-python/tests/spec_test.py 的 `test_i142_hr1_*` 四格单独钉（实测：把旧兜底
   还原回来，本文件仍绿、那四格按预期红 ⇒ 顶到的是形状格，不是这里的读数）。
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "demo"))

from main import repo, ext_repo, api_reset  # noqa: E402


async def test_reset_clears_and_reseeds():
    # 造点数据：一个实例 + 一个任务 + 一条委托
    before_defines = len(repo._defines)
    assert before_defines > 0
    repo._instances[999999] = object()
    repo._tasks[999999] = object()

    await api_reset()

    # T003：reset 后 = 完整业务种子矩阵，非空库
    assert len(repo._instances) == 25, f"reset 后实例应回到矩阵规模 25，实际 {len(repo._instances)}"
    assert len(repo._tasks) > 0, "reset 后任务应随业务种子重建"
    assert len(repo._actors) > 0, "reset 后参与者应随业务种子重建"
    assert len(repo._cc) > 0, "reset 后抄送应随业务种子重建"
    assert len(ext_repo._surrogates) == 8, f"reset 后委托应为 8 条，实际 {len(ext_repo._surrogates)}"
    assert len(repo._defines) == before_defines, "种子定义应重载"

    # 落点：15 state=10 + 10 state=20（issues/141 G9 裁定后重算，理由见模块 docstring）
    states = [inst.state for inst in repo._instances.values()]
    assert states.count(10) == 15, f"进行中应 15 条，实际 {states.count(10)}"
    assert states.count(20) == 10, f"已完成应 10 条，实际 {states.count(20)}"
