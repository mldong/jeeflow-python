"""引擎内置默认表达式求值器（issues/160）。

**为什么存在**：本栈的运行时在宿主没注册 ``IExpressionEvaluator`` 时**根本不求值**——
``engine.py`` 的 ``_evaluate_decision`` 整段跳过求值，靠"取第一条无 expr 的边、否则无条件取
第一条边"蒙一个方向；而同一条 SPI 缺失在门面的 ``highLight`` 那一腿是 ``eval_expr`` 抛
``ValueError("ExpressionEvaluator 未配置")``，再被 ``facade.py`` 模型补全块的裸 ``except`` 吞掉
⇒ 同一个实例两条腿两个答案：运行时"照样进了某一支"，门面把走过的决策边**连同它之后的整条路径**
一起丢掉，且流程照常流转，只有流程图暴露它（比 issues/158 的 rust 形状更宽）。
rust（``jeeflow-core/src/default_evaluator.rs``，1.0.27）与 csharp
（``ServiceContext.ExpressionEvaluatorOrDefault``）的解法是**引擎自带默认求值器＋两腿共用一个出口**，
让"未注册"这一档结构性不可达；本模块即 python 侧的同形补齐。

**语义基准**＝rust ``evaluate_builtin``（其自身以 csharp ``DefaultExpressionEvaluator`` 为基准、
后者以 PHP ``WfExpressionEvaluator`` 为最小基准不超集）：变量代入（``${var}`` / ``#var`` / 裸名）
＋ 比较运算 ＋ 布尔字面量。不含 ``&&`` / ``||``——rust 没有、csharp 基准也没有，不在此扩集。

两处继承自 rust 的**收紧**（不是本栈新发明）：
1. 裸变量名**代入**：不代入时 ``amount > 1000`` 比的是变量名的字典序，同一条比较对
   ``amount <= 1000`` 也成立 ⇒ 两支一起"为真"。
2. 关系运算（``>`` ``>=`` ``<`` ``<=``）**只在两侧都是数字时判**，否则 false：
   非数字操作数的字典序没有业务含义，用它定分支就是把"字母排在数字后面"当流程语义。

空表达式判 false 与 rust 同档（本栈两腿调用点都先判 ``expr`` 非空才求值，该档实际不可达）。

⚠ 一条**不承诺与多数派一致**的边：操作数是"数字字符串"（``amount`` 存成 ``"5000"``）时，
本件按 rust 档判 false（``Str`` 不参与定序），csharp 按文本替换后再转数判 true。跨栈门禁
``L2-39`` 的夹具送的是 JSON 数字（``runner.py`` 现读 ``"amount": 5000``），该档不在门禁面上；
本栈宿主 SPI（三壳的 ``SimpleExprEvaluator`` 走 ``float(actual)``）优先级高于本件，
注册了宿主的栈行为不变。
"""
import re
from typing import Any

from .spi import ExpressionEvaluator

# 两位运算符必须先于其一位前缀被试（``<=`` 先于 ``<``），否则 ``amount <= 1000`` 会被拆错形状。
_OPS = (">=", "<=", "!=", "==", ">", "<")

_NUM = "num"
_STR = "str"
_BOOL = "bool"
_NULL = "null"

_NUMBER_PATTERN = re.compile(r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$")


def _parse_num(token: str):
    """只认十进制数字字面量（不接受 ``inf`` / ``nan`` 这类拼写——非数字档位交给下面判 false）。"""
    return float(token) if _NUMBER_PATTERN.match(token) else None


def _from_value(v: Any) -> tuple:
    """上下文取数归一。``bool`` 必须先判：Python 里 ``True`` 是 ``int`` 实例，顺序反了就成 ``1``。"""
    if isinstance(v, bool):
        return (_BOOL, v)
    if isinstance(v, (int, float)):
        return (_NUM, float(v))
    if isinstance(v, str):
        return (_STR, v)
    if v is None:
        return (_NULL, None)
    # 复合值（list/dict）不参与比较：流程表达式不该拿数组/对象当操作数
    return (_NULL, None)


def _truthy(val: tuple) -> bool:
    kind, payload = val
    if kind == _NUM:
        return payload != 0.0
    if kind == _BOOL:
        return payload
    if kind == _NULL:
        return False
    return payload != "" and payload != "false" and payload != "0"


def _resolve_token(token: str, context: dict) -> tuple:
    """单侧词元解析（基准＝PHP ``resolveValue``：``${}`` 剥壳 → 引号字面量 → 数字 →
    布尔/null → ``#名``（缺省 0）→ 流程变量（缺省视为未定义）"""
    t = token.strip()
    if t.startswith("${") and t.endswith("}") and len(t) >= 3:
        name = t[2:-1].strip()
    else:
        name = t

    if len(name) >= 2 and ((name[0] == name[-1] == "'") or (name[0] == name[-1] == '"')):
        return (_STR, name[1:-1])
    num = _parse_num(name)
    if num is not None:
        return (_NUM, num)
    lowered = name.lower()
    if lowered == "true":
        return (_BOOL, True)
    if lowered == "false":
        return (_BOOL, False)
    if lowered == "null":
        return (_NULL, None)
    if name.startswith("#"):
        raw = name[1:]
        num = _parse_num(raw)
        if num is not None:
            return (_NUM, num)
        # 会签门控变量（``#nrOfCompletedInstances`` 等）：缺省回落 0，与 PHP 同形
        return _from_value(context.get(raw, 0.0))
    # 裸变量名：查得到才代入（不代入就成了比较变量名自己的字典序）
    return _from_value(context.get(name))


def _same_value(left: tuple, right: tuple) -> bool:
    if left[0] != right[0]:
        return False
    if left[0] == _NULL:
        return True
    return left[1] == right[1]


def _compare(left: tuple, right: tuple, op: str) -> bool:
    if left[0] == _NUM and right[0] == _NUM:
        l, r = left[1], right[1]
        if op == ">":
            return l > r
        if op == ">=":
            return l >= r
        if op == "<":
            return l < r
        if op == "<=":
            return l <= r
        if op == "==":
            return l == r
        if op == "!=":
            return l != r
        return False
    if op == "==":
        return _same_value(left, right)
    if op == "!=":
        return not _same_value(left, right)
    # 关系运算要求两侧都是数字：非数字操作数的字典序不是流程语义（见模块头的收紧说明）
    return False


def evaluate_builtin(expression: str, context: dict) -> bool:
    """引擎内置求值：表达式 → 布尔。运行时定支与门面 ``highLight`` 共用这一个出口。"""
    expr = (expression or "").strip()
    if not expr:
        return False
    context = context if isinstance(context, dict) else {}
    for op in _OPS:
        pos = expr.find(op)
        if pos < 0:
            continue
        return _compare(_resolve_token(expr[:pos], context),
                        _resolve_token(expr[pos + len(op):], context), op)
    # 无运算符：单个变量 / 布尔字面量
    return _truthy(_resolve_token(expr, context))


class DefaultExpressionEvaluator(ExpressionEvaluator):
    """SPI 形状（与宿主注册的求值器同接口，故运行时/门面只需换一个出口）。"""

    async def eval(self, expr: str, vars: dict[str, Any]) -> Any:
        return evaluate_builtin(expr, vars)
