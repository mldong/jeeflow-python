"""参与者**删除腿**「原值 ∪ trim 值」两形并集收口（issues/137 §3-6 · spec 06-facade.md
§processTask/removeTaskActor 语义 6 ＋ §2.11 写点表末行 · owner 2026-10-02 拍「两形并集」）。

python 栈此前是**裸传**（go/node/python/java 四栈九处那一派）：内存仓 ``remove = set(actors)``
直接比、SQL 仓把 ``actors`` 原样绑进 ``IN``——既不产出 trim 形（第三方绕过门面直连仓储传
``" 8601 "`` 时删不掉写侧归一后的规范行 ``8601``，issues/142 §9.2 的既有判据），也不丢空值
（``""`` 入参会把历史 ``actor_id=''`` 脏行删掉，那是替脏数据做掉唯一痕迹）。

本文件钉三层：
① 单点纯函数 ``spi.actor_delete_forms``（八栈同名件之一，判据本体复用 ``normalize_actors``，
   只加"原值也进集合"这一层）；
② 内存仓 ``MemoryRepository.remove_task_actor``；
③ SQL 仓 ``JdbcRepository.remove_task_actor``——断言**直接查库里的真实列值**（SQLite 腿），
   不看内存对象。

脏行夹具一律用**前导空格**（``" 9101 "``）：MySQL 5.7 PAD SPACE 只忽略尾部、8.0 NO PAD 连尾部
也算，前导空格在任何排序规则下都与规范行不等，判据不会漂。种脏行**绕开写侧归一**
（``add_task_actor`` 会 trim＋丢空，正常路径建不出脏行）：SQL 腿直插库、内存腿直接改
``_actors`` dict。内存仓与 SQL 仓**同一条判据、同一个答案**（issues/117 场景 27）。
"""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jeeflow.memory import MemoryRepository
from jeeflow.repository.base import JdbcRepository
from jeeflow.spi import IDGenerator, actor_delete_forms, normalize_actors


# ═══ ① 单点纯函数：spi.actor_delete_forms ═══════════════════════════════════════

def test_forms_drops_every_empty_shape():
    """①空值一律丢弃：None/空串/纯空白/制表/换行都不进删除集（判空＝strip()==""，单点内部
    复用 normalize_actors 那一枚，不抄第二份）。"""
    assert actor_delete_forms([None, "", "  ", "\t", "\n ", "\r\n", "a"]) == ["a"]
    assert "None" not in actor_delete_forms([None, "x"]), \
        "None 元素绝不允许被串化成字符串 \"None\" 再去匹配"


def test_forms_padded_value_yields_two_forms_original_first():
    """②带空格值产出两形且**原值在前**：原值形保住未 trim 历史脏行，trim 形保住规范行。"""
    assert actor_delete_forms([" 9101 "]) == [" 9101 ", "9101"]


def test_forms_already_trimmed_value_single_entry():
    """②两形相同则只一份：已 trim 值不得重复进 IN。"""
    assert actor_delete_forms(["8601"]) == ["8601"]


def test_forms_cross_element_dedup():
    """跨元素按**字面**去重（保序）：后到的元素两形都已在集合里就不再进。"""
    assert actor_delete_forms([" 9101 ", "9101"]) == [" 9101 ", "9101"]
    assert actor_delete_forms(["9101", " 9101 "]) == ["9101", " 9101 "]
    assert actor_delete_forms(["a", "a", " a ", "a"]) == ["a", " a "]


def test_forms_distinct_original_forms_each_preserved():
    """去重按字面做，**不按 strip 后相同折叠原值形**：``" 9101 "`` 与 ``"  9101  "`` 是两种
    不同的原值形（库里可能各自躺着这样的历史脏行），都要保留；trim 形相同只一份。"""
    assert actor_delete_forms([" 9101 ", "  9101  "]) == [" 9101 ", "9101", "  9101  "]


def test_forms_zero_sentinel_is_never_eaten():
    """④反向哨兵：``"0"`` 是合法 id 必须留下（**严禁 ``if not x`` 假值判据**），
    且 ``"0"`` 与 ``"00"`` 是两个不同的人。"""
    assert actor_delete_forms(["0"]) == ["0"]
    assert actor_delete_forms([" 0 "]) == [" 0 ", "0"]
    assert actor_delete_forms([" 0 ", "0", "00"]) == [" 0 ", "0", "00"]


def test_forms_order_preserved():
    """保序：按入参顺序逐元素先原值后 trim 值。"""
    assert actor_delete_forms(["b", " a ", "c"]) == ["b", " a ", "a", "c"]


def test_forms_empty_inputs_yield_empty_list():
    """入参 None/空集合/全空值 ⇒ **空列表**——调用方据此早退（③），
    不得退化成"清空该任务全部参与者"。"""
    assert actor_delete_forms(None) == []
    assert actor_delete_forms([]) == []
    assert actor_delete_forms(()) == []
    assert actor_delete_forms([""]) == []
    assert actor_delete_forms(["", None, "  ", "\t"]) == []


def test_forms_input_shapes_match_normalize_actors_dispatch():
    """入参形状分派与 normalize_actors 同款（逗号串/数组/标量），且 trim 形部分与归一单点
    **逐输入同答案**（判据本体就是那一枚，本函数只加"原值也进集合"这一层）。"""
    assert actor_delete_forms(" 9101 ,9102") == [" 9101 ", "9101", "9102"]
    assert actor_delete_forms(" 9101 ") == [" 9101 ", "9101"]      # 标量
    assert actor_delete_forms([123, " 123 "]) == ["123", " 123 "], \
        "数字元素 str() 后按原值进集合；\" 123 \" 是不同的字面原值形，照留（不按 strip 折叠）"
    assert actor_delete_forms(0) == ["0"], "标量 0 也是一个人，不得被假值判据折成没填"
    for raw in ([None, "", "  ", " a ", "a", " 9101 ", "0", "00", "b"],
                " a ,, 9101 ", [" 0 ", "00"], None, [], ""):
        trimmed_leg = [f for f in dict.fromkeys(v.strip() for v in actor_delete_forms(raw))]
        assert trimmed_leg == normalize_actors(raw), \
            f"trim 形判据必须与归一单点同答案: {raw!r}"


# ═══ 夹具：两仓现场 ═════════════════════════════════════════════════════════════

class _FormsIDGen(IDGenerator):
    def __init__(self):
        self.n = 0

    def next_id(self) -> int:
        self.n += 1
        return self.n


class _FormsSqliteConn:
    """sqlite3 同步驱动套 async 壳（与 spec_test._SqliteConn 同形状，`?` 原生占位符）"""

    def __init__(self, raw):
        self._raw = raw

    async def execute(self, sql, args):
        self._raw.execute(sql, tuple(args))
        self._raw.commit()

    async def fetchone(self, sql, args):
        return self._raw.execute(sql, tuple(args)).fetchone()

    async def fetchall(self, sql, args):
        return self._raw.execute(sql, tuple(args)).fetchall()

    async def begin(self):
        pass

    async def commit(self):
        self._raw.commit()

    async def rollback(self):
        pass


class _FormsSqliteAdapter:
    placeholder = "?"

    def __init__(self, raw):
        self._conn = _FormsSqliteConn(raw)

    async def acquire(self):
        return self._conn

    async def release(self, conn):
        pass


def _sql_repo():
    """真 SQLite ＋ 真 ``JdbcRepository``——断言直接查库里的**真实列值**，不看内存对象。"""
    raw = sqlite3.connect(":memory:")
    raw.execute("CREATE TABLE wf_process_task_actor (id INTEGER PRIMARY KEY,"
                " process_task_id INTEGER, actor_id TEXT, create_time TEXT, create_user TEXT)")
    return raw, JdbcRepository(_FormsSqliteAdapter(raw), _FormsIDGen())


def _sql_rows(raw, task_id) -> list:
    """actor 表取证：某任务落库的真实 actor_id 行（按插入序）。"""
    return [r[0] for r in raw.execute(
        "SELECT actor_id FROM wf_process_task_actor WHERE process_task_id=?"
        " ORDER BY id ASC", (task_id,)).fetchall()]


def _sql_seed_dirty(raw, task_id, *actor_ids):
    """种脏行**绕开写侧归一**：直插库（add_task_actor 会 trim＋丢空，正常路径建不出脏行）。"""
    for i, a in enumerate(actor_ids):
        raw.execute("INSERT INTO wf_process_task_actor (id, process_task_id, actor_id,"
                    " create_time, create_user) VALUES (?,?,?,?,?)",
                    (900000 + i, task_id, a, "2026-10-02 00:00:00", "dirty-seed"))
    raw.commit()


def _mem_repo(task_id, *rows) -> MemoryRepository:
    """内存仓现场：种行**绕开写侧归一**（直接改 ``_actors`` dict）。"""
    repo = MemoryRepository()
    repo._actors[task_id] = list(rows)
    return repo


# ═══ ② 内存仓删除腿 ═════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_memory_untrimmed_dirty_row_is_really_deleted():
    """N 档：库里躺着修复前落下的未 trim 历史脏行 ``" 9101 "``，删 ``[" 9101 "]`` ⇒ 脏行真消失
    （旧裸传恰好删得掉这一格，但它是变异 A 的照妖镜——单点若只产 trim 形，本格仍绿；
    真正打红变异 A 的是下面"两形并存"与"直连仓储传未 trim 值删规范行"两格的对偶）。"""
    repo = _mem_repo(101, " 9101 ", "leader")
    await repo.remove_task_actor(101, [" 9101 "])
    assert await repo.find_task_actors(101) == ["leader"], "脏行必须真消失，其余参与人一行不动"


@pytest.mark.asyncio
async def test_memory_untrimmed_input_deletes_canonical_row():
    """N 档（issues/142 §9.2 既有判据，必须绿）：第三方绕过门面直连仓储传 ``" 8601 "``，
    库里是写侧归一后落库的规范行 ``8601`` ⇒ trim 形命中，照样删得掉。"""
    repo = _mem_repo(102, "8601", "leader")
    await repo.remove_task_actor(102, [" 8601 "])
    assert await repo.find_task_actors(102) == ["leader"]


@pytest.mark.asyncio
async def test_memory_dirty_and_canonical_coexist_both_removed():
    """N 档：脏行 ``" 9101 "`` 与规范行 ``9101`` 并存，入参 ``[" 9101 "]`` ⇒ 两形并集
    （``[" 9101 ", "9101"]``）各命中一行，两行都摘掉（§2.11 归一口径下本就是同一个人），
    其余参与人一行不动。"""
    repo = _mem_repo(103, "leader", " 9101 ", "9101", "9002")
    await repo.remove_task_actor(103, [" 9101 "])
    assert await repo.find_task_actors(103) == ["leader", "9002"]


@pytest.mark.asyncio
async def test_memory_empty_inputs_never_delete_dirty_rows():
    """P 档：空值入参不得删掉 ``actor_id=''`` 脏行；全空入参（[""]／[]／None）⇒ **零删除**，
    不得清空全部参与者（③早退）。"""
    for bad in ([""], [], None, ["", "  ", None, "\t"]):
        repo = _mem_repo(104, "", "   ", "leader")
        await repo.remove_task_actor(104, bad)
        assert await repo.find_task_actors(104) == ["", "   ", "leader"], \
            f"{bad!r}: 空值一律丢弃不喂删除，脏行是 issues/129 家族的唯一痕迹，必须原样还在"


@pytest.mark.asyncio
async def test_memory_none_element_is_not_stringified():
    """None 元素不得被串化成 ``"None"`` 再去匹配（历史 ``actor_id='None'`` 脏行同理不许误删）。"""
    repo = _mem_repo(105, "None", "leader")
    await repo.remove_task_actor(105, [None, "ghost"])
    assert await repo.find_task_actors(105) == ["None", "leader"]


@pytest.mark.asyncio
async def test_memory_non_participant_ignored_and_missing_task_noop():
    """非参与者静默忽略；任务不存在 ⇒ 零操作不抛异常。"""
    repo = _mem_repo(106, "leader")
    await repo.remove_task_actor(106, ["ghost", " 9101 "])
    assert await repo.find_task_actors(106) == ["leader"]

    await repo.remove_task_actor(424242, ["x"])          # 不该抛
    assert await repo.find_task_actors(424242) == []
    await repo.remove_task_actor(424242, [])             # 全空＋任务不存在，同样零操作
    assert await repo.find_task_actors(424242) == []


# ═══ ③ SQL 仓删除腿（真 SQLite，断言查库里的真实列值） ═══════════════════════════

@pytest.mark.asyncio
async def test_sql_untrimmed_dirty_row_is_really_deleted():
    """N 档：直插的未 trim 脏行 ``" 9101 "`` ＋ 规范行，删 ``[" 9101 "]`` ⇒ 脏行真消失
    （库里按真实列值取证）。"""
    raw, repo = _sql_repo()
    _sql_seed_dirty(raw, 201, " 9101 ", "leader")
    await repo.remove_task_actor(201, [" 9101 "])
    assert _sql_rows(raw, 201) == ["leader"]


@pytest.mark.asyncio
async def test_sql_untrimmed_input_deletes_canonical_row():
    """N 档（issues/142 §9.2 既有判据，必须绿）：直连仓储传 ``" 8601 "``，库里是规范行
    ``8601`` ⇒ trim 形命中，DELETE 真删掉。"""
    raw, repo = _sql_repo()
    await repo.add_task_actor(202, ["8601", "leader"])       # 写侧归一路径落规范行
    assert _sql_rows(raw, 202) == ["8601", "leader"]
    await repo.remove_task_actor(202, [" 8601 "])
    assert _sql_rows(raw, 202) == ["leader"]


@pytest.mark.asyncio
async def test_sql_dirty_and_canonical_coexist_both_removed():
    """N 档：脏行 ``" 9101 "``（直插）与规范行 ``9101``（写侧归一落库）并存，入参
    ``[" 9101 "]`` ⇒ 两形并集一条 DELETE 两行都摘掉，其余参与人一行不动。"""
    raw, repo = _sql_repo()
    await repo.add_task_actor(203, ["9101", "leader", "9002"])
    _sql_seed_dirty(raw, 203, " 9101 ")
    assert sorted(_sql_rows(raw, 203)) == sorted(["9101", "leader", "9002", " 9101 "])
    await repo.remove_task_actor(203, [" 9101 "])
    assert sorted(_sql_rows(raw, 203)) == sorted(["leader", "9002"])


@pytest.mark.asyncio
async def test_sql_empty_inputs_never_delete_dirty_rows():
    """P 档：空值入参不得删掉 ``actor_id=''``/纯空白脏行；全空入参 ⇒ **一条 DELETE 都不发**
    （占位符列表为空时早退，连 ``IN ()`` 语法错都不许出现），参与者一行不少。"""
    for bad in ([""], [], None, ["", "  ", None, "\t"]):
        raw, repo = _sql_repo()
        await repo.add_task_actor(204, ["leader"])
        _sql_seed_dirty(raw, 204, "", "   ")
        await repo.remove_task_actor(204, bad)
        assert sorted(_sql_rows(raw, 204)) == sorted(["leader", "", "   "]), \
            f"{bad!r}: 空值一律不喂 DELETE，历史 actor_id='' 脏行必须原样还在"


@pytest.mark.asyncio
async def test_sql_none_element_is_not_stringified():
    """None 元素不得被串化成 ``"None"`` 再去匹配库里的行。"""
    raw, repo = _sql_repo()
    _sql_seed_dirty(raw, 205, "None", "leader")
    await repo.remove_task_actor(205, [None, "ghost"])
    assert _sql_rows(raw, 205) == ["None", "leader"]


@pytest.mark.asyncio
async def test_sql_non_participant_ignored_and_missing_task_noop():
    """非参与者静默忽略；任务不存在 ⇒ 零操作不抛异常（DELETE 命中 0 行是正常路径）。"""
    raw, repo = _sql_repo()
    await repo.add_task_actor(206, ["leader"])
    await repo.remove_task_actor(206, ["ghost", " 9101 "])
    assert _sql_rows(raw, 206) == ["leader"]

    await repo.remove_task_actor(424242, ["x"])          # 不该抛
    assert _sql_rows(raw, 424242) == []
    await repo.remove_task_actor(424242, [])
    assert _sql_rows(raw, 424242) == []


@pytest.mark.asyncio
async def test_sql_zero_sentinel_rows_are_two_people():
    """④反向哨兵落到 DELETE 上：``"0"`` 是合法 id 删得掉；``"00"`` 是另一个人一行不动。"""
    raw, repo = _sql_repo()
    await repo.add_task_actor(207, ["0", "00", "leader"])
    await repo.remove_task_actor(207, ["0"])
    assert _sql_rows(raw, 207) == ["00", "leader"]


# ═══ 两仓同一条判据、同一个答案（issues/117 场景 27） ═══════════════════════════

@pytest.mark.asyncio
async def test_memory_and_sql_repos_give_the_same_answer():
    """同一份行现场 ＋ 同一个删除入参，两仓的存活行读数必须**逐格一致**。"""
    matrix = [
        # (库里的行, 删除入参, 期望存活行·按集合比)
        ([" 9101 ", "9101", "leader"], [" 9101 "], {"leader"}),
        (["8601", "leader"], [" 8601 "], {"leader"}),
        (["", "   ", "leader"], [""], {"", "   ", "leader"}),
        (["", "leader"], [], {"", "leader"}),
        (["None", "leader"], [None], {"None", "leader"}),
        (["0", "00", "leader"], [" 0 "], {"00", "leader"}),
        (["leader"], ["ghost"], {"leader"}),
        ([" 7101 ", "leader", "9002"], [" 7101 ", "9002"], {"leader"}),
    ]
    for rows, arg, expect in matrix:
        mem = _mem_repo(301, *rows)
        await mem.remove_task_actor(301, arg)
        mem_left = set(await mem.find_task_actors(301))

        raw, sql = _sql_repo()
        _sql_seed_dirty(raw, 301, *rows)     # 全部直插，保证两仓种的是逐字节同一份行
        await sql.remove_task_actor(301, arg)
        sql_left = set(_sql_rows(raw, 301))

        assert mem_left == expect, f"内存仓分叉: rows={rows!r} arg={arg!r} → {mem_left}"
        assert sql_left == expect, f"SQL 仓分叉: rows={rows!r} arg={arg!r} → {sql_left}"
        assert mem_left == sql_left, f"两仓两个答案（issues/117 场景 27）: rows={rows!r} arg={arg!r}"
