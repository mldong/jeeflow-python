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

# 发起表单字段（实例 f_*，前端「申请信息」回显）——八语言共用 canonical，逐字同表同值同序；
# 13 无 apply 节点故不列。硬规则：①日期一律写死字面量，不按当前时钟算（各栈机器时区/系统时间各异，算出来会漂）；
# ②字段名严禁 amount / finalAmount——它们是 03-decision-expr、10-mixed-mode 条件表达式的判定变量，撞上会改流程走向。
FORM_BY_DEFINE: dict = {
    1: {"f_reason": "家中有事需请假", "f_days": 3, "f_leaveType": "annual",
        "f_startDate": "2026-09-01", "f_endDate": "2026-09-03"},
    2: {"f_reason": "项目上线后调休", "f_days": 2, "f_leaveType": "annual",
        "f_startDate": "2026-09-07", "f_endDate": "2026-09-08"},
    3: {"f_reason": "出差报销申请", "f_days": 1, "f_leaveType": "personal",
        "f_startDate": "2026-09-10", "f_endDate": "2026-09-10"},
    4: {"f_reason": "培训进修请假", "f_days": 5, "f_leaveType": "sick",
        "f_startDate": "2026-09-14", "f_endDate": "2026-09-18"},
    5: {"f_reason": "年假出行", "f_days": 4, "f_leaveType": "annual",
        "f_startDate": "2026-09-21", "f_endDate": "2026-09-24"},
    6: {"f_reason": "婚假申请", "f_days": 10, "f_leaveType": "personal",
        "f_startDate": "2026-09-28", "f_endDate": "2026-10-07"},
    7: {"f_reason": "病假休养", "f_days": 6, "f_leaveType": "sick",
        "f_startDate": "2026-10-12", "f_endDate": "2026-10-17"},
    8: {"f_reason": "产检假", "f_days": 3, "f_leaveType": "sick",
        "f_startDate": "2026-10-19", "f_endDate": "2026-10-21"},
    9: {"f_reason": "陪产假", "f_days": 5, "f_leaveType": "personal",
        "f_startDate": "2026-10-26", "f_endDate": "2026-10-30"},
    10: {"f_reason": "事假处理家务", "f_days": 2, "f_leaveType": "personal",
         "f_startDate": "2026-11-02", "f_endDate": "2026-11-03"},
    11: {"f_bizType": "purchase", "f_budget": 12000, "f_urgency": "normal",
         "f_desc": "采购一批开发板与传感器"},
    12: {"f_reason": "部门例行调休", "f_days": 1, "f_leaveType": "annual",
         "f_startDate": "2026-11-09", "f_endDate": "2026-11-09"},
    14: {"f_reason": "外派学习请假", "f_days": 7, "f_leaveType": "annual",
         "f_startDate": "2026-11-16", "f_endDate": "2026-11-22"},
    15: {"f_reason": "丧假", "f_days": 3, "f_leaveType": "personal",
         "f_startDate": "2026-11-23", "f_endDate": "2026-11-25"},
}

# 任务表单字段（节点 tf_*，落任务 ext，前端「办理表单」+ 审批记录读 ext.tf_*）：
# 键 = 审批节点的 formKey；表里没有的 formKey 只落通用审批意见，不臆造字段。
TF_BY_FORM: dict = {
    "leave-form": {"tf_approvedDays": 3, "tf_needExtra": "no",
                   "tf_remark": "按项目排期核准，注意工作交接"},
    "review-form": {"tf_riskLevel": "low", "tf_needLegalDoc": "no",
                    "tf_reviewOpinion": "条款与预算均无风险"},
    "boss-form": {"tf_finalDecision": "agree", "tf_finalAmount": 8000,
                  "tf_bossNote": "同意，走年度预算"},
    "check-form": {"tf_invoiceOk": "yes", "tf_amountChecked": 8000,
                   "tf_checkNote": "票据齐全，计入差旅科目"},
    "countersign-form": {"tf_signVote": "support", "tf_signAmount": 5000,
                         "tf_signOpinion": "本条线无异议"},
    "seq-form": {"tf_seqStage": "first", "tf_seqVote": "pass",
                 "tf_seqOpinion": "初审通过，转下一人"},
    "approve-form": {"tf_approveResult": "ok", "tf_approveAmount": 8000,
                     "tf_approveNote": "审批通过"},
    "ratio-form": {"tf_ratioVote": "agree", "tf_ratioOpinion": "达到比例即可通过"},
    "veto-form": {"tf_vetoResult": "pass", "tf_vetoReason": "无异议"},
    "form-a": {"tf_branchA": "a1", "tf_branchANote": "A 分支选方案 A1"},
    "form-b": {"tf_branchB": "b1", "tf_branchBNote": "B 分支选方案 B1"},
    "field-form": {"tf_ownerName": "张三", "tf_field": "tech",
                   "tf_fieldNote": "技术域评估通过"},
    "operator-form": {"tf_selfCheck": "done", "tf_operatorNote": "发起人自查无误"},
    "dept-form": {"tf_deptAgree": "yes", "tf_deptQuota": 8000,
                  "tf_deptNote": "同意占用本部门额度"},
    "role-form": {"tf_roleResult": "pass", "tf_roleNote": "角色审批通过"},
}


def _with_task_form(ex: dict, form_key) -> None:
    """execute 入参补任务表单字段：每次必带通用审批意见，再按 formKey 铺专属字段。"""
    ex["tf_approvalComment"] = "同意，情况已核实"
    ex.update(TF_BY_FORM.get(str(form_key or ""), {}))


async def seed_business(facade):
    """种业务数据；失败逐条打日志不抛异常（demo 启动不被单条卡死）。"""
    ok_in = ok_fin = ok_surr = 0
    for define_id, op, extra, cc in IN_PROGRESS:
        # f_* 先铺、extra 后铺：流程变量 amount / deptLeader 优先，不被表单值盖掉
        args = {"processDefineId": define_id, "operator": op,
                **FORM_BY_DEFINE.get(define_id, {}), **extra}
        resp = await facade.flow("processDefine/startAndExecute", args)
        iid = (resp.get("data") or {}).get("processInstanceId")
        if not iid:
            print(f"[seed] startAndExecute define={define_id} op={op} 失败: {resp}")
            continue
        if define_id == 2 and op == "userA":  # I14：发起后再办 leader、manager → 停 boss
            for actor in ("leader", "manager"):
                row = await _todo_row(facade, actor, iid)
                if row:
                    ex = {"processTaskId": row["id"], "operator": actor, "submitType": 1}
                    _with_task_form(ex, row.get("formKey"))
                    await facade.flow("processTask/execute", ex)
                else:
                    print(f"[seed] I14 todoRow actor={actor} iid={iid} 未找到")
        if cc:
            await facade.flow("processInstance/createCCInstance", {
                "processInstanceId": iid, "operator": op, "actorIds": cc})
        ok_in += 1

    for define_id, op, extra, cc in FINISHED:
        # 同上：f_* 先铺、extra 后铺
        args = {"processDefineId": define_id, "operator": op,
                **FORM_BY_DEFINE.get(define_id, {}), **extra}
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
            ex = {"processTaskId": t["id"], "operator": actor, "submitType": 1}
            _with_task_form(ex, t.get("formKey"))
            r = await facade.flow("processTask/execute", ex)
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
