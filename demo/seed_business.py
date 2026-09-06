"""T003：业务数据种子 driver——引擎真实启动（startAndExecute + execute），不直插 repo。

矩阵 = 八语言共用 canonical（day-shift 已在 Rust demo 实测全绿，照 rust seed_business.rs 移植）：
16 进行中(state=10) + 9 已完成(advance 推到 state=20) + 8 委托。
8 用户 × 5 菜单（待办/已办/发起/抄送/委托）全覆盖。
"""

IN_PROGRESS = [
    # (defineId, operator, extraVars, 抄送 actorIds)
    (1, "user1", {}, ["userA", "userB"]),           # I1
    (2, "user1", {}, []),                           # I2
    (3, "userA", {"amount": 500}, []),              # I3 冻结 task1/leader（决策前）
    (4, "manager", {}, ["userC", "leader"]),        # I4
    (5, "userB", {}, []),                           # I5
    (6, "director", {}, ["manager", "boss"]),       # I6
    (7, "userC", {}, ["user1"]),                    # I7
    (1, "boss", {}, []),                            # I8 boss「发起」来源
    (12, "user1", {"deptLeader": "manager"}, []),   # I9
    (12, "userC", {"deptLeader": "director"}, []),  # I10
    (12, "userB", {"deptLeader": "user1"}, []),     # I11 user1/张三「待办」来源
    (15, "userA", {}, ["boss"]),                    # I12
    (14, "leader", {}, ["director", "userC"]),      # I13
    (2, "userA", {}, []),                           # I14 发起后再办 leader/manager → 停 boss
    (10, "userB", {}, []),                          # I15 冻结 task1/leader（驳回前）
    (8, "user1", {}, []),                           # I16
]

FINISHED = [
    (1, "userA", {}, ["user1", "director"]),        # F1
    (8, "userB", {}, ["boss", "manager"]),          # F2
    (2, "manager", {}, ["boss"]),                   # F3
    (10, "director", {}, []),                       # F4
    (12, "userC", {"deptLeader": "leader"}, []),    # F5
    (1, "director", {}, []),                        # F6
    (5, "manager", {}, []),                         # F7
    (12, "userA", {"deptLeader": "director"}, []),  # F8
    (12, "userB", {"deptLeader": "user1"}, []),     # F9
]

SURROGATES = [
    ("user1", "userA"), ("userA", "userB"), ("userB", "userC"), ("userC", "leader"),
    ("leader", "manager"), ("manager", "director"), ("director", "boss"), ("boss", "user1"),
]


async def seed_business(facade):
    """种业务数据；失败逐条打日志不抛异常（demo 启动不被单条卡死）。"""
    ok_in = ok_fin = ok_surr = 0
    for define_id, op, extra, cc in IN_PROGRESS:
        args = {"processDefineId": define_id, "operator": op, **extra}
        resp = await facade.flow("processDefine/startAndExecute", args)
        iid = (resp.get("data") or {}).get("processInstanceId")
        if not iid:
            print(f"[seed] startAndExecute define={define_id} op={op} 失败: {resp}")
            continue
        if define_id == 2 and op == "userA":  # I14：发起后再办 leader、manager → 停 boss
            for actor in ("leader", "manager"):
                row = await _todo_row(facade, actor, iid)
                if row:
                    await facade.flow("processTask/execute", {
                        "processTaskId": row["id"], "operator": actor, "submitType": 1})
                else:
                    print(f"[seed] I14 todoRow actor={actor} iid={iid} 未找到")
        if cc:
            await facade.flow("processInstance/createCCInstance", {
                "processInstanceId": iid, "operator": op, "actorIds": cc})
        ok_in += 1

    for define_id, op, extra, cc in FINISHED:
        args = {"processDefineId": define_id, "operator": op, **extra}
        resp = await facade.flow("processDefine/startAndExecute", args)
        iid = (resp.get("data") or {}).get("processInstanceId")
        if not iid:
            print(f"[seed] FIN startAndExecute define={define_id} op={op} 失败: {resp}")
            continue
        state = await _advance(facade, iid)
        if state != 20:
            print(f"[seed] FIN define={define_id} op={op} iid={iid} 终态={state}（期望 20）")
        if cc:
            await facade.flow("processInstance/createCCInstance", {
                "processInstanceId": iid, "operator": op, "actorIds": cc})
        ok_fin += 1

    for op, surrogate in SURROGATES:
        resp = await facade.flow("processSurrogate/save", {
            "operator": op, "surrogate": surrogate, "processName": "",
            "startTime": "2026-01-01 00:00:00", "endTime": "2027-12-31 23:59:59"})
        if resp.get("code") == 0:
            ok_surr += 1
        else:
            print(f"[seed] surrogate {op}->{surrogate} 失败: {resp}")

    print(f"[seedBusiness] done: in-progress {ok_in}/16, finished {ok_fin}/9, surrogates {ok_surr}/8")


async def _advance(facade, iid):
    """advance 原语：循环读 detail，对每个 doing 任务以其自身 actor execute(submitType=1)。
    doing 任务 operator 为 None，actor 取 taskActorIdList[0]。"""
    for _ in range(30):
        resp = await facade.flow("processInstance/detail", {"id": iid})
        data = resp.get("data") or {}
        state = data.get("state")
        if state != 10:
            return state
        doing = [t for t in (data.get("tasks") or []) if t.get("taskState") == 10]
        if not doing:
            return state
        progress = False
        for t in doing:
            actor = t.get("operator") or next(iter(t.get("taskActorIdList") or []), None)
            if not actor:
                continue
            r = await facade.flow("processTask/execute", {
                "processTaskId": t["id"], "operator": actor, "submitType": 1})
            if r.get("code") == 0:
                progress = True
            else:
                print(f"[seed] advance execute iid={iid} actor={actor} 失败: {r}")
        if not progress:
            return state
    return (await facade.flow("processInstance/detail", {"id": iid})).get("data", {}).get("state")


async def _todo_row(facade, op, iid):
    """仅 I14 用：在该实例里找 op 的 doing 任务行。"""
    resp = await facade.flow("processTask/todoList", {"operator": op, "pageNum": 1, "pageSize": 200})
    for row in (resp.get("data") or {}).get("rows") or []:
        if str(row.get("processInstanceId")) == str(iid) and row.get("taskState") == 10:
            return row
    return None
