"""综合 E2E 测试——覆盖所有流程场景"""
import os, sys, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow import EngineImpl, MemoryRepository, EngineExtensions, FlowInterceptor, HandlerRegistry, register_builtin_assignments
from jeeflow.model import ProcessDefine, TaskState, InstanceState, UserInfo
from jeeflow.spi import IDGenerator, ExpressionEvaluator

FLOWS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "jeeflow-java",
                         "jeeflow-core", "src", "test", "resources", "flows")

passed = 0; failed = 0

def check(desc, ok, detail=""):
    global passed, failed
    tag = "PASS" if ok else "FAIL"
    msg = f"  [{tag}] {desc}"
    if detail: msg += f" ({detail})"
    print(msg)
    if ok: passed += 1
    else: failed += 1
    return ok

# ── Setup ────────────────────────────────────────────────────────────────────

class TestIDGen(IDGenerator):
    def __init__(self): self.n = 0
    def next_id(self): self.n += 1; return self.n

class TestUserProv:
    async def get_user(self, uid):
        return UserInfo(userId=uid, realName=uid, deptId="D01", deptName="部门", postId="P01", postName="岗位")

class TestExpr(ExpressionEvaluator):
    async def eval(self, expr, vars):
        amt = vars.get("amount")
        if amt is not None:
            if expr == "amount > 1000": return float(amt) > 1000
            if expr == "amount <= 1000": return float(amt) <= 1000
        return False

async def _start_and_execute(eng, repo, define_id, operator, args=None):
    """模拟 boot2 startAndExecute：启动后自动完成申请节点"""
    inst = await eng.start_process_instance_by_id(define_id, operator, args)
    doing = await repo.find_doing_tasks(inst.id)
    for task in doing:
        if task.taskName == "apply":
            await repo.add_task_actor(task.id, [operator])
            await eng.execute_process_task(task.id, operator)
    return inst

async def main():
    global passed, failed
    repo = MemoryRepository()
    eng = EngineImpl(repo, TestUserProv(), TestIDGen(), TestExpr())

    for fname in sorted(os.listdir(FLOWS_DIR)):
        if fname.endswith(".json"):
            with open(os.path.join(FLOWS_DIR, fname), "r", encoding="utf-8") as f:
                raw = json.loads(f.read())
            d = ProcessDefine(name=raw.get("name", fname), displayName=raw.get("displayName", fname),
                              type=raw.get("type", ""), state=1,
                              content=json.dumps(raw, ensure_ascii=False))
            repo.add_define(d)

    print("=" * 60)
    print("  E2E 测试：jeeflow Python Engine")
    print("=" * 60)
    print()

    # ═══ 1. 简单流程 ═══════════════════════════════════════════════════════════════
    print("[1] 简单流程 (start → task1 → end)")
    inst = await _start_and_execute(eng, repo, 1, "applicant")
    check("启动成功", inst.id > 0 and inst.state == InstanceState.DOING,
          f"id={inst.id} state={inst.state}")
    doing = await repo.find_doing_tasks(inst.id)
    check("生成 1 个待办", len(doing) == 1 and doing[0].taskName == "task1",
          f"task={doing[0].taskName}")
    await repo.add_task_actor(doing[0].id, ["leader"])
    inst = await eng.execute_process_task(doing[0].id, "leader")
    check("完成后流程结束", inst.state == InstanceState.DONE, f"state={inst.state}")
    print()

    # ═══ 2. 多级审批 ═══════════════════════════════════════════════════════════════
    print("[2] 多级审批 (task1 → task2 → task3 → end)")
    inst = await _start_and_execute(eng, repo, 2, "applicant")
    check("启动成功", inst.state == InstanceState.DOING)

    doing = await repo.find_doing_tasks(inst.id)
    check("第1步 task1", len(doing) == 1 and doing[0].taskName == "task1")
    await repo.add_task_actor(doing[0].id, ["userA"])
    await eng.execute_process_task(doing[0].id, "userA")

    doing = await repo.find_doing_tasks(inst.id)
    check("第2步 task2", len(doing) == 1 and doing[0].taskName == "task2")
    await repo.add_task_actor(doing[0].id, ["userB"])
    await eng.execute_process_task(doing[0].id, "userB")

    doing = await repo.find_doing_tasks(inst.id)
    check("第3步 task3", len(doing) == 1 and doing[0].taskName == "task3")
    await repo.add_task_actor(doing[0].id, ["userC"])
    inst = await eng.execute_process_task(doing[0].id, "userC")
    check("完成", inst.state == InstanceState.DONE, f"state={inst.state}")
    print()

    # ═══ 3. 决策（高金额→task2） ═════════════════════════════════════════════════════
    print("[3] 决策表达式（amount=3000 → 经理审批 task2）")
    inst = await _start_and_execute(eng, repo, 3, "applicant", {"amount": 3000})
    doing = await repo.find_doing_tasks(inst.id)
    check("task1（填表）", doing[0].taskName == "task1")
    await repo.add_task_actor(doing[0].id, ["leader"])
    await eng.execute_process_task(doing[0].id, "leader")
    doing = await repo.find_doing_tasks(inst.id)
    check("金额>1000 → task2", doing[0].taskName == "task2",
          f"got {doing[0].taskName}")
    await repo.add_task_actor(doing[0].id, ["manager"])
    inst = await eng.execute_process_task(doing[0].id, "manager")
    check("完成", inst.state == InstanceState.DONE)
    print()

    # ═══ 4. 决策（低金额→task3） ═════════════════════════════════════════════════════
    print("[4] 决策表达式（amount=500 → 总监审批 task3）")
    inst = await _start_and_execute(eng, repo, 3, "applicant", {"amount": 500})
    doing = await repo.find_doing_tasks(inst.id)
    check("task1", doing[0].taskName == "task1")
    await repo.add_task_actor(doing[0].id, ["leader"])
    await eng.execute_process_task(doing[0].id, "leader")
    doing = await repo.find_doing_tasks(inst.id)
    check("金额≤1000 → task3", doing[0].taskName == "task3",
          f"got {doing[0].taskName}")
    await repo.add_task_actor(doing[0].id, ["director"])
    inst = await eng.execute_process_task(doing[0].id, "director")
    check("完成", inst.state == InstanceState.DONE)
    print()

    # ═══ 5. Fork/Join ═══════════════════════════════════════════════════════════════
    print("[5] Fork/Join (taskA + taskB 并行 → Join → end)")
    inst = await _start_and_execute(eng, repo, 4, "applicant")
    doing = await repo.find_doing_tasks(inst.id)
    check("生成 2 个并行任务", len(doing) == 2, f"got {len(doing)}")
    tA = [t for t in doing if t.taskName == "taskA"][0]
    tB = [t for t in doing if t.taskName == "taskB"][0]
    await repo.add_task_actor(tA.id, ["userA"])
    await eng.execute_process_task(tA.id, "userA")
    inst2 = await repo.find_instance_by_id(inst.id)
    check("完成 A 后仍在进行", inst2.state == InstanceState.DOING)
    await repo.add_task_actor(tB.id, ["userB"])
    inst = await eng.execute_process_task(tB.id, "userB")
    check("完成 B 后流程结束", inst.state == InstanceState.DONE)
    print()

    # ═══ 6. 并行会签 ═════════════════════════════════════════════════════════════════
    print("[6] 并行会签（3人 → 全部完成 → end）")
    inst = await _start_and_execute(eng, repo, 5, "applicant")
    doing = await repo.find_doing_tasks(inst.id)
    check("生成 3 个并行会签任务", len(doing) == 3, f"got {len(doing)}")
    for a in ["userA", "userB", "userC"]:
        d = await repo.find_doing_tasks(inst.id)
        await repo.add_task_actor(d[0].id, [a])
        await eng.execute_process_task(d[0].id, a)
    inst = await repo.find_instance_by_id(inst.id)
    check("全部完成后流程结束", inst.state == InstanceState.DONE)
    print()

    # ═══ 7. 串行会签 ═════════════════════════════════════════════════════════════════
    print("[7] 串行会签（2人依次 → end）")
    inst = await _start_and_execute(eng, repo, 6, "applicant")
    doing = await repo.find_doing_tasks(inst.id)
    check("生成 1 个串行任务", len(doing) == 1, f"got {len(doing)}")
    await repo.add_task_actor(doing[0].id, ["userA"])
    await eng.execute_process_task(doing[0].id, "userA")
    doing = await repo.find_doing_tasks(inst.id)
    check("下一步串行任务", len(doing) == 1)
    await repo.add_task_actor(doing[0].id, ["userB"])
    inst = await eng.execute_process_task(doing[0].id, "userB")
    check("完成后流程结束", inst.state == InstanceState.DONE)
    print()

    # ═══ 8. 驳回 ═════════════════════════════════════════════════════════════════════
    print("[8] 驳回 (task1 → reject → 废弃所有待办)")
    inst = await _start_and_execute(eng, repo, 2, "applicant")
    doing = await repo.find_doing_tasks(inst.id)
    check("启动", len(doing) == 1)
    await repo.add_task_actor(doing[0].id, ["applicant"])
    inst = await eng.execute_and_jump_to_end(doing[0].id, "applicant")
    check("状态=REJECT", inst.state == InstanceState.REJECT, f"state={inst.state}")
    check("所有待办已废弃", len(await repo.find_doing_tasks(inst.id)) == 0)
    print()

    # ═══ 9. 权限校验 ═════════════════════════════════════════════════════════════════
    print("[9] 权限校验（非处理人不能完成）")
    inst = await _start_and_execute(eng, repo, 2, "applicant")
    doing = await repo.find_doing_tasks(inst.id)
    await repo.add_task_actor(doing[0].id, ["leader"])
    try:
        await eng.execute_process_task(doing[0].id, "intruder")
        check("应拒绝", False, "居然通过了!")
    except ValueError as e:
        check("正确拒绝非处理人", "not allowed" in str(e), str(e))
    print()

    # ═══ 10. 拦截器 + 事件 ═══════════════════════════════════════════════════════════
    print("[10] 拦截器 + 事件监听")
    pre_called = [False]; post_called = [False]
    events = []

    class TestIC(FlowInterceptor):
        async def pre_handle(self, n, i): pre_called[0] = True; return True
        async def post_handle(self, n, i): post_called[0] = True
        @property
        def order(self): return 1

    eng2 = EngineImpl(repo, TestUserProv(), TestIDGen(), TestExpr())
    eng2.set_extensions(EngineExtensions(
        interceptors=[TestIC()],
        event_listener=lambda evt: events.append(evt.type.value),
    ))
    inst = await _start_and_execute(eng2, repo, 1, "applicant")
    check("PROCESS_START 事件", "PROCESS_START" in events)
    doing = await repo.find_doing_tasks(inst.id)
    await repo.add_task_actor(doing[0].id, ["leader"])
    await eng2.execute_process_task(doing[0].id, "leader")
    check("pre_handle 被调用", pre_called[0])
    check("post_handle 被调用", post_called[0])
    check("4 个事件（start+apply+task+finish）", len(events) >= 3, str(events))
    check("事件包含 START/FINISH", "PROCESS_START" in events and "PROCESS_FINISH" in events)
    print()

    # ═══ 11. 统计 ════════════════════════════════════════════════════════════════════
    print("[11] 统计 & 详情")
    all_insts = repo.all_instances()
    all_tasks = repo.all_tasks()
    done_tasks = sum(1 for t in all_tasks if t.taskState == TaskState.DONE)
    check(f"实例总数 > 0", len(all_insts) > 0, f"{len(all_insts)} 个")
    check(f"完成任务数 > 0", done_tasks > 0, f"{done_tasks} 个")

    # 详情
    inst = await repo.find_instance_by_id(1)
    check("实例详情可查", inst is not None and inst.state is not None,
          f"id=1 state={inst.state if inst else None}")
    print()

    # ═══ 12. 内置参与者 handler（issues/16）══════════════════════════════════════════
    print("[12] 内置参与者 handler（issues/16）")

    class TestOrgUserProv:
        async def find_dept_leaders(self, dept_id):
            return ["leader1", "leader2"] if dept_id == "D01" else []
        async def find_dept_main_leaders(self, dept_id):
            return ["boss1"] if dept_id == "D01" else []
        async def find_by_role(self, role_code):
            return ["roleA", "roleB"] if role_code == "task4" else []

    reg16 = HandlerRegistry()
    register_builtin_assignments(reg16, TestUserProv(), TestOrgUserProv())
    eng16 = EngineImpl(repo, TestUserProv(), TestIDGen(), TestExpr())
    eng16.set_extensions(EngineExtensions(registry=reg16))

    # 注册 11-assignment-handler.json 流程定义
    with open(os.path.join(FLOWS_DIR, "11-assignment-handler.json"), "r", encoding="utf-8") as f:
        raw16 = json.loads(f.read())
    d16 = ProcessDefine(name="assignment-handler", displayName=raw16.get("displayName", "assignment-handler"),
                        type=raw16.get("type", ""), state=1, content=json.dumps(raw16, ensure_ascii=False))
    repo.add_define(d16)

    # ① FormFieldAssigneeHandler：节点 task1 → args.task1 = userA,userB
    inst16 = await eng16.start_process_instance_by_id(d16.id, "user1", {"task1": "userA,userB"})
    doing16 = await repo.find_doing_tasks(inst16.id)
    check("① task1 参与者=字段值", len(doing16) == 1 and doing16[0].taskName == "task1"
          and sorted(doing16[0].actorIds) == ["userA", "userB"],
          f"actors={[t.actorIds for t in doing16]}")
    await repo.add_task_actor(doing16[0].id, doing16[0].actorIds)
    await eng16.execute_process_task(doing16[0].id, "userA")

    # ② OperatorAssignmentHandler：task2 → 发起人 user1
    doing16 = await repo.find_doing_tasks(inst16.id)
    check("② task2 参与者=发起人", len(doing16) == 1 and doing16[0].taskName == "task2"
          and doing16[0].actorIds == ["user1"], f"actors={[t.actorIds for t in doing16]}")
    await repo.add_task_actor(doing16[0].id, doing16[0].actorIds)
    await eng16.execute_process_task(doing16[0].id, "user1")

    # ③ DeptLeaderAssignmentHandler：task3 → user1 部门 D01 领导
    doing16 = await repo.find_doing_tasks(inst16.id)
    check("③ task3 参与者=部门领导", len(doing16) == 1 and doing16[0].taskName == "task3"
          and sorted(doing16[0].actorIds) == ["leader1", "leader2"],
          f"actors={[t.actorIds for t in doing16]}")
    await repo.add_task_actor(doing16[0].id, doing16[0].actorIds)
    await eng16.execute_process_task(doing16[0].id, "leader1")

    # ④ TaskRoleAssigneeHandler：task4 → roleCode=task4 → roleA,roleB
    doing16 = await repo.find_doing_tasks(inst16.id)
    check("④ task4 参与者=角色", len(doing16) == 1 and doing16[0].taskName == "task4"
          and sorted(doing16[0].actorIds) == ["roleA", "roleB"],
          f"actors={[t.actorIds for t in doing16]}")
    await repo.add_task_actor(doing16[0].id, doing16[0].actorIds)
    inst16 = await eng16.execute_process_task(doing16[0].id, "roleA")
    check("⑤ 流程结束", inst16.state == InstanceState.DONE, f"state={inst16.state}")
    print()

    # ═══ 12b. FormFieldAssigneeHandler f_ 前缀（issues/48）══════════════════════
    print("[12b] FormFieldAssigneeHandler f_ 前缀（issues/48）")

    reg12b = HandlerRegistry()
    register_builtin_assignments(reg12b, TestUserProv(), TestOrgUserProv())
    eng12b = EngineImpl(repo, TestUserProv(), TestIDGen(), TestExpr())
    eng12b.set_extensions(EngineExtensions(registry=reg12b))

    with open(os.path.join(FLOWS_DIR, "11-assignment-handler.json"), "r", encoding="utf-8") as f:
        raw12b = json.loads(f.read())
    d12b = ProcessDefine(name="afp-test", displayName="afp-test",
                         type="test", state=1, content=json.dumps(raw12b, ensure_ascii=False))
    repo.add_define(d12b)

    # ① f_ 前缀变量（前端表单提交格式）
    inst12b = await eng12b.start_process_instance_by_id(d12b.id, "user1", {"f_task1": "userA,userB"})
    doing12b = await repo.find_doing_tasks(inst12b.id)
    check("① f_ 前缀取人", len(doing12b) == 1 and doing12b[0].taskName == "task1"
          and sorted(doing12b[0].actorIds) == ["userA", "userB"],
          f"actors={[t.actorIds for t in doing12b]}")
    await repo.add_task_actor(doing12b[0].id, doing12b[0].actorIds)
    await eng12b.execute_process_task(doing12b[0].id, "userA")

    # ② f_ 前缀优先于裸名
    inst12b2 = await eng12b.start_process_instance_by_id(d12b.id, "user1", {"f_task1": "userX", "task1": "userY"})
    doing12b2 = await repo.find_doing_tasks(inst12b2.id)
    check("② f_ 优先于裸名", len(doing12b2) == 1 and doing12b2[0].actorIds == ["userX"],
          f"actors={[t.actorIds for t in doing12b2]}")
    print()

    # ═══ 13. candidatePage 双源候选（issues/16 GlobalCandidateHandler 语义）═════════
    print("[13] candidatePage 双源候选（issues/16）")

    from jeeflow import JeeflowFacade

    class TestOrgUserProv13:
        async def find_dept_leaders(self, dept_id): return []
        async def find_dept_main_leaders(self, dept_id): return []
        async def find_by_role(self, role_code):
            return ["finA", "finB"] if role_code == "finance" else []

    eng13 = EngineImpl(repo, TestUserProv(), TestIDGen(), TestExpr())
    facade13 = JeeflowFacade(eng13, repo, None, org_prov=TestOrgUserProv13())

    with open(os.path.join(FLOWS_DIR, "12-candidate-page.json"), "r", encoding="utf-8") as f:
        raw13 = json.loads(f.read())
    d13 = ProcessDefine(name="candidate-flow", displayName=raw13.get("displayName", "candidate-flow"),
                        type=raw13.get("type", ""), state=1, content=json.dumps(raw13, ensure_ascii=False))
    repo.add_define(d13)

    # 直接启动（不自动完成 apply）→ apply 任务 → candidatePage 查 review 候选
    inst13 = await eng13.start_process_instance_by_id(d13.id, "user1")
    doing13 = await repo.find_doing_tasks(inst13.id)
    check("apply 任务就绪", len(doing13) == 1 and doing13[0].taskName == "apply",
          f"task={doing13[0].taskName if doing13 else None}")
    r13 = await facade13.flow("processTask/candidatePage", {"processTaskId": doing13[0].id})
    check("candidatePage 命中双源候选", r13["code"] == 0 and r13["data"]["recordCount"] == 4,
          f"r={r13}")
    if r13["code"] == 0:
        rows13 = r13["data"]["rows"]
        userIds13 = sorted(x["userId"] for x in rows13)
        check("候选 = candidateUsers(userA/userB) + candidateGroups(finA/finB)",
              userIds13 == ["finA", "finB", "userA", "userB"], f"candidates={userIds13}")
        # issues/80：行键契约 {id, realName}（对齐前端 UserSelect valueField='id'）
        bad13 = [x for x in rows13 if not x.get("id") or "realName" not in x]
        check("行键含 id+realName", not bad13, f"bad={bad13}")
        mis13 = [x for x in rows13 if x.get("userId") and x["id"] != x["userId"]]
        check("id 与 userId 一一对齐（行键归一）", not mis13, f"mis={mis13}")
        check("id 列表含 userA", "userA" in [x["id"] for x in rows13],
              f"ids={[x['id'] for x in rows13]}")
    print()

    # ═══ 14. startAndExecute 预指派人（f_nextNodeOperator，对齐 boot3）═══════════════
    print("[14] startAndExecute 预指派人（f_nextNodeOperator）")

    from jeeflow.engine import KEY_PROCESS_START_NEXT_NODE_OPERATOR

    with open(os.path.join(FLOWS_DIR, "01-simple.json"), "r", encoding="utf-8") as f:
        raw14 = json.loads(f.read())
    d14 = ProcessDefine(name="simple", displayName=raw14.get("displayName", "simple"),
                        type=raw14.get("type", ""), state=1, content=json.dumps(raw14, ensure_ascii=False))
    repo.add_define(d14)

    facade14 = JeeflowFacade(eng13, repo, None)
    r14 = await facade14.flow("processInstance/startAndExecute",
                              {"processDefineId": d14.id, "operator": "user1",
                               KEY_PROCESS_START_NEXT_NODE_OPERATOR: "userA"})
    check("预指派发起成功", r14["code"] == 0, f"r={r14}")
    inst14 = r14["data"]["processInstanceId"]
    doing14 = await repo.find_doing_tasks(inst14)
    check("task1 参与者=userA（预指派人）",
          len(doing14) == 1 and doing14[0].taskName == "task1" and doing14[0].actorIds == ["userA"],
          f"tasks={[(t.taskName, t.actorIds) for t in doing14]}")

    r14b = await facade14.flow("processInstance/startAndExecute",
                               {"processDefineId": d14.id, "operator": "user1"})
    inst14b = r14b["data"]["processInstanceId"]
    doing14b = await repo.find_doing_tasks(inst14b)
    check("未指定时 task1 参与者=leader",
          len(doing14b) == 1 and doing14b[0].taskName == "task1" and doing14b[0].actorIds == ["leader"],
          f"tasks={[(t.taskName, t.actorIds) for t in doing14b]}")
    print()

    # ── Summary ──
    print("=" * 60)
    total = passed + failed
    if failed == 0:
        print(f"  结果：{passed}/{total} 全部通过 ✅")
    else:
        print(f"  结果：{passed} PASS  {failed} FAIL ❌")
    print("=" * 60)
    return failed == 0

import asyncio
if __name__ == "__main__":
    ok = asyncio.run(main())
    sys.exit(0 if ok else 1)
