"""动态表写入组件（引擎无关）+ 工作流入库适配拦截器——issues/18。

对标 Java jeeflow-persist 契约：

- ``DynamicTableWriter``：给「表名 + 字段 Map」安全写入任意业务表
  （列过滤 / 参数化 INSERT / 幂等 / 系统字段），不依赖工作流引擎
- ``JdbcDynamicTableWriter``：DB-API 2.0 默认实现（sqlite 走 PRAGMA table_info，
  MySQL/PG/H2 走 information_schema.columns，UPPER 兼容 H2 大写存储）
- ``PersistPostInterceptor``：流程结束同意后，f_ 表单数据写入业务表
"""
from __future__ import annotations

import json
import re
import sqlite3
import time
from abc import ABC, abstractmethod
from typing import Any, Callable, NamedTuple, Optional, Sequence

from .extensions import FlowInterceptor
from .model import ProcessDefine, ProcessInstance, TYPE_END, TYPE_TASK, TYPE_CUSTOM, InstanceState
from .engine import KEY_SUBMIT_TYPE, KEY_DEPT_ID
from .model import SubmitType

# ─── DynamicTableWriter ────────────────────────────────────────────────────────

class DynamicTableWriter(ABC):
    """动态表写入组件接口——引擎无关，四语言契约一致"""

    @abstractmethod
    def filter_columns(self, table_name: str, columns: Sequence[str]) -> list[str]:
        """按目标表过滤列（表结构探测），返回表内实际存在的列"""

    @abstractmethod
    def insert(self, table_name: str, data: dict[str, Any]) -> Any:
        """参数化 INSERT（按列过滤结果落库），返回生成主键"""

    @abstractmethod
    def update(self, table_name: str, data: dict[str, Any], where_column: str, where_value: Any) -> int:
        """参数化 UPDATE（按列过滤结果组装 SET；条件列排除，防注入），返回受影响行数
        （SYNC 同步演进，issues/24）"""

    @abstractmethod
    def exists(self, table_name: str, biz_key: str, biz_key_value: Any) -> bool:
        """幂等检查：指定业务键（如 process_instance_id）是否已存在"""

    @abstractmethod
    def fill_system_fields(self, data: dict[str, Any], is_insert: bool) -> None:
        """按配置列名填充系统字段（未配置的列跳过）"""


# ─── JdbcDynamicTableWriter ────────────────────────────────────────────────────

_TABLE_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")
_TIME_LAYOUT = "%Y-%m-%d %H:%M:%S"


class _Column(NamedTuple):
    """表列元数据（issues/21：主键/自增用于主键生成决策）"""
    name: str          # 表列原名（UPPER）
    primary_key: bool
    auto_increment: bool


class JdbcDynamicTableWriter(DynamicTableWriter):
    """DB-API 2.0 默认实现。

    :param conn: 数据库连接（sqlite3 连接自动识别方言；其他连接默认
        information_schema 探测 + ``?`` 占位符）
    :param dialect: 显式方言 "sqlite" / "mysql" / "postgres" / "h2"，
        None 时自动探测（sqlite3.Connection → sqlite，其余 → information_schema）
    :param create_time_column: 系统字段列名，None 禁用（默认 "create_time"）
    :param create_user_column: 默认 "create_user"
    :param update_time_column: 默认 "update_time"
    :param update_user_column: 默认 "update_user"
    :param is_deleted_column: 默认 "is_deleted"
    """

    def __init__(self, conn, dialect: Optional[str] = None,
                 create_time_column: Optional[str] = "create_time",
                 create_user_column: Optional[str] = "create_user",
                 update_time_column: Optional[str] = "update_time",
                 update_user_column: Optional[str] = "update_user",
                 is_deleted_column: Optional[str] = "is_deleted"):
        self._conn = conn
        if dialect is None:
            dialect = "sqlite" if isinstance(conn, sqlite3.Connection) else "information_schema"
        self._dialect = dialect
        self.create_time_column = create_time_column
        self.create_user_column = create_user_column
        self.update_time_column = update_time_column
        self.update_user_column = update_user_column
        self.is_deleted_column = is_deleted_column
        # 用户列默认值（issues/19）：优先取 data 中已注入的 apply_user_id=流程 operator，
        # 否则用此配置值，缺省 "system"——多数框架业务表 create_user/update_user 为 BIGINT 存 userId
        self.default_user_value: Any = "system"
        # 列匹配（issues/20）：默认宽松——驼峰↔下划线归一匹配（表单字段 companyName ↔ 表列 company_name）；
        # 需要精确控制列名的集成方显式开启严格模式（忽略大小写精确匹配）
        self.strict_column_match: bool = False
        # 主键生成器（issues/21）：非自增主键表（雪花/应用生成）插入时生成主键值，入参表名
        self.primary_key_generator: Optional[Callable[[str], Any]] = None
        self._cache: dict[str, list[_Column]] = {}

    # ── 表结构探测 ──

    def _execute(self, query: str, params: tuple = ()):
        """统一执行入口（issues/33）：兼容 sqlite3 快捷 execute 与标准 DB-API 连接
        （pymysql/aiomysql 等只有 cursor()，无 execute 快捷方法）——两者返回均视为 cursor。"""
        if hasattr(self._conn, "execute"):
            return self._conn.execute(query, params)
        cur = self._conn.cursor()
        cur.execute(query, params)
        return cur

    def _placeholder(self) -> str:
        """方言占位符（issues/33）：sqlite/h2 用 ?，mysql/postgres 用 %s"""
        return "?" if self._dialect in ("sqlite", "h2") else "%s"

    def _table_columns(self, table_name: str) -> list[_Column]:
        cached = self._cache.get(table_name)
        if cached is not None:
            return cached
        if self._dialect == "sqlite":
            # PRAGMA table_info：cid,name,type,notnull,dflt_value,pk——INTEGER PRIMARY KEY 为 rowid 别名（自增）
            rows = self._execute(f"PRAGMA table_info({table_name})").fetchall()
            cols = [_Column(r[1].upper(), r[5] == 1,
                            r[5] == 1 and r[2].strip().upper() == "INTEGER") for r in rows]
        elif self._dialect == "mysql":
            # issues/22：限定当前 schema（DATABASE()），防多库同名表列重复
            rows = self._execute(
                "SELECT column_name, extra, column_key FROM information_schema.columns "
                f"WHERE UPPER(table_name) = UPPER({self._placeholder()}) AND table_schema = DATABASE() "
                "ORDER BY ordinal_position",
                (table_name,)).fetchall()
            cols = [_Column(r[0].upper(), r[2].upper() == "PRI",
                            "auto_increment" in (r[1] or "").lower()) for r in rows]
        else:
            # PG/H2 标准 SQL：IS_IDENTITY（identity）+ column_default nextval（PG serial）+ 主键约束 JOIN
            # issues/22：限定当前 schema（CURRENT_SCHEMA()，H2/PG 均支持）
            rows = self._execute(
                "SELECT c.column_name, c.is_identity, c.column_default, "
                "CASE WHEN kcu.column_name IS NOT NULL THEN 'PRI' ELSE '' END AS column_key "
                "FROM information_schema.columns c "
                "LEFT JOIN information_schema.table_constraints tc "
                "  ON tc.table_name = c.table_name AND tc.constraint_type = 'PRIMARY KEY' "
                "  AND tc.table_schema = c.table_schema "
                "LEFT JOIN information_schema.key_column_usage kcu "
                "  ON kcu.constraint_name = tc.constraint_name AND kcu.column_name = c.column_name "
                "  AND kcu.table_schema = c.table_schema "
                f"WHERE UPPER(c.table_name) = UPPER({self._placeholder()}) AND c.table_schema = CURRENT_SCHEMA() "
                "ORDER BY c.ordinal_position",
                (table_name,)).fetchall()
            cols = [_Column(r[0].upper(), r[3].upper() == "PRI",
                            (r[1] or "").upper() == "YES" or "nextval" in (r[2] or "")) for r in rows]
        if not cols:
            raise ValueError(f"persist: table {table_name!r} not found")
        self._cache[table_name] = cols
        return cols

    # ── 公共接口 ──

    def filter_columns(self, table_name: str, columns: Sequence[str]) -> list[str]:
        _check_table_name(table_name)
        cols = self._table_columns(table_name)
        return [c for c in columns if self._find_table_column(cols, c) is not None]

    def insert(self, table_name: str, data: dict[str, Any]) -> Any:
        _check_table_name(table_name)
        cols = self._table_columns(table_name)
        names: list[str] = []
        values: list[Any] = []
        # 保持插入顺序稳定（data 无序，按表列顺序取）；写入用表列原名（issues/20）
        for col in cols:
            key = self._find_data_key(data, col.name)
            if key is not None:
                names.append(col.name)
                values.append(data[key])
                continue
            # 主键生成（issues/21）：非自增主键表且 data 无主键值 → 调生成器；未配置 → 清晰报错
            if col.primary_key and not col.auto_increment:
                if self.primary_key_generator is None:
                    raise ValueError(
                        f"persist: table {table_name!r} primary key {col.name!r} is not auto-increment "
                        "and no primary key generator configured (set primary_key_generator, e.g. snowflake)")
                names.append(col.name)
                values.append(self.primary_key_generator(table_name))
        if not names:
            raise ValueError(f"persist: no matching columns for {table_name}")
        placeholder = self._placeholder()
        placeholders = ",".join([placeholder] * len(names))
        query = f"INSERT INTO {table_name} ({','.join(names)}) VALUES ({placeholders})"
        cur = self._execute(query, values)
        self._conn.commit()
        return cur.lastrowid

    def exists(self, table_name: str, biz_key: str, biz_key_value: Any) -> bool:
        _check_table_name(table_name)
        self._table_columns(table_name)  # 表不存在提前报错
        query = f"SELECT COUNT(1) FROM {table_name} WHERE {biz_key} = ?"
        row = self._execute(query, (biz_key_value,)).fetchone()
        return bool(row and row[0] > 0)

    def update(self, table_name: str, data: dict[str, Any], where_column: str, where_value: Any) -> int:
        """参数化 UPDATE（SYNC 同步演进，issues/24）：列过滤（宽松匹配）+ 条件列排除 +
        参数化 SET，返回受影响行数。对齐 Java JdbcDynamicTableWriter.update。"""
        _check_table_name(table_name)
        if not where_column:
            raise ValueError(f"persist: update {table_name} requires where column")
        cols = self._table_columns(table_name)
        sets: list[str] = []
        values: list[Any] = []
        for col in cols:
            if self._normalize(col.name) == self._normalize(where_column):
                continue  # 条件列不参与 SET
            key = self._find_data_key(data, col.name)
            if key is not None:
                sets.append(f"{col.name} = ?")
                values.append(data[key])
        if not sets:
            return 0  # 无更新列（如结束节点仅状态探测未命中）
        placeholder = self._placeholder()
        query = f"UPDATE {table_name} SET {','.join(sets)} WHERE {where_column} = {placeholder}"
        values.append(where_value)
        cur = self._execute(query, values)
        self._conn.commit()
        return cur.rowcount

    def fill_system_fields(self, data: dict[str, Any], is_insert: bool) -> None:
        now = time.strftime(_TIME_LAYOUT)
        if is_insert:
            if self.create_time_column:
                data.setdefault(self.create_time_column, now)
            if self.create_user_column:
                data.setdefault(self.create_user_column, self._resolve_default_user(data))
            if self.update_time_column:
                data.setdefault(self.update_time_column, now)
            if self.update_user_column:
                data.setdefault(self.update_user_column, self._resolve_default_user(data))
            if self.is_deleted_column:
                data.setdefault(self.is_deleted_column, 0)
        else:
            if self.update_time_column:
                data[self.update_time_column] = now
            if self.update_user_column:
                data.setdefault(self.update_user_column, self._resolve_default_user(data))

    def _resolve_default_user(self, data: dict[str, Any]) -> Any:
        """默认用户值（issues/19）：优先取 data 中已注入的 apply_user_id
        （拦截器场景 = 流程 operator，BIGINT 用户列表开箱即用），否则回落配置默认值。"""
        operator = data.get("apply_user_id")
        return operator if operator is not None else self.default_user_value

    # ── 列匹配（issues/20）──

    def _find_table_column(self, cols: Sequence[_Column], key: str) -> Optional[str]:
        """在表列中匹配输入 key：宽松（默认）驼峰↔下划线归一；严格忽略大小写精确。"""
        for m in cols:
            if self.strict_column_match:
                if m.name.upper() == key.upper():
                    return m.name
            elif self._normalize(m.name) == self._normalize(key):
                return m.name
        return None

    def _find_data_key(self, data: dict[str, Any], col: str) -> Optional[str]:
        for k in data:
            if self.strict_column_match:
                if col.upper() == k.upper():
                    return k
            elif self._normalize(col) == self._normalize(k):
                return k
        return None

    @staticmethod
    def _normalize(name: str) -> str:
        """列名归一：转小写 + 去下划线（companyName / company_name / COMPANY_NAME 等价）"""
        return name.lower().replace("_", "")


def _check_table_name(table_name: str) -> None:
    """表名安全校验：非空、合法字符、拒绝 sys_ 前缀"""
    if not table_name:
        raise ValueError("persist: table name is empty")
    if table_name.startswith("sys_"):
        raise ValueError(f"persist: table {table_name!r} with sys_ prefix is not allowed")
    if not _TABLE_NAME_RE.match(table_name):
        raise ValueError(f"persist: table {table_name!r} contains illegal characters")


# ─── PersistPostInterceptor ────────────────────────────────────────────────────

DefineLoader = Any  # 类型别名：Callable[[int], Awaitable[ProcessDefine]]

# 持久化模式（流程定义顶层 persistMode，缺省 ARCHIVE）
PERSIST_MODE_ARCHIVE = "ARCHIVE"  # 结束归档（现状）：流程结束同意后落库
PERSIST_MODE_SYNC = "SYNC"        # 同步演进：发起 INSERT → 任务节点 UPDATE → 结束定稿

# 字段权限值（任务节点 properties.field 的 PERMISSION_{字段名}，vben5-wf 机制）
PERM_READ_ONLY = 1  # 只读：不更新
PERM_EDIT = 2       # 可编辑：更新
PERM_HIDDEN = 3     # 隐藏：不更新


class PersistPostInterceptor(FlowInterceptor):
    """工作流业务数据入库适配拦截器——按流程定义顶层 persistMode 分派：

    - ARCHIVE（缺省）：流程结束同意后，f_ 表单数据写入业务表（一次落库）
    - SYNC（1.8.0，issues/24 同步演进）：提交申请即入库（start 节点 INSERT 全量），
      任务节点推进 UPDATE（f_ 按节点字段权限过滤 + tf_ 冗余 + 状态字段=DOING），
      结束节点定稿 UPDATE（最终状态 FINISHED/REJECT）——不管成功失败都入库

    对标 Java PersistPostInterceptor（1.8.0）。

    :param writer: 动态表写入器（必须注入，否则静默跳过）
    :param loader: 流程定义加载器（``async def loader(define_id) -> ProcessDefine``，
        通常透传 ``repo.find_define_by_id``）
    :param field_prefix: 实例表单字段前缀，默认 "f_"
    :param task_field_prefix: 任务冗余字段前缀（审批意见等，SYNC 下冗余到业务表对应列），默认 "tf_"
    """

    def __init__(self, writer: Optional[DynamicTableWriter] = None,
                 loader: Optional[DefineLoader] = None,
                 field_prefix: str = "f_",
                 task_field_prefix: str = "tf_"):
        self.writer = writer
        self.loader = loader
        self.field_prefix = field_prefix
        self.task_field_prefix = task_field_prefix

    async def pre_handle(self, node, instance) -> bool:
        return True

    @property
    def order(self) -> int:
        return 0

    async def post_handle(self, node, instance: ProcessInstance) -> None:
        if self.writer is None or self.loader is None:
            return  # 未注入：静默跳过
        if node is None or instance is None:
            return
        table_name, persist_mode = await self._resolve_define(instance)
        if not table_name:
            return  # 未配置：静默跳过
        if str(persist_mode).upper() == PERSIST_MODE_SYNC:
            await self._handle_sync(node, instance, table_name)
            return
        await self._handle_archive(node, instance, table_name)

    # ── ARCHIVE（现状：结束同意归档） ──

    async def _handle_archive(self, node, instance: ProcessInstance, table_name: str) -> None:
        # 时机：仅结束节点 + 流程正常完成（DONE）+ 同意
        if node.type != TYPE_END:
            return
        if instance.state != InstanceState.DONE:
            return
        submit_type = instance.variables.get(KEY_SUBMIT_TYPE)
        if submit_type is None or int(submit_type) != int(SubmitType.AGREE):
            return
        if not self._mark_chain(node, instance):
            return
        # 幂等：以 process_instance_id 为键，先查后插
        if self.writer.exists(table_name, "process_instance_id", instance.id):
            return
        data = self._extract_fields(instance, None, False, True)  # 只 f_ 全量
        self._fill_context(data, instance)
        self.writer.fill_system_fields(data, True)
        self.writer.insert(table_name, data)

    # ── SYNC（1.8.0 同步演进：发起入库 → 节点推进 → 结束定稿） ──

    async def _handle_sync(self, node, instance: ProcessInstance, table_name: str) -> None:
        if not self._mark_chain(node, instance):
            return  # 同链同节点不重复（节点级，issues/19 演进）
        exists = self.writer.exists(table_name, "process_instance_id", instance.id)

        # 任务节点（TYPE_TASK/TYPE_CUSTOM）才更新业务字段：f_ 按节点字段权限过滤；
        # 结束/网关等非任务节点只定稿状态，避免全量覆盖任务节点的只读/隐藏限制
        is_task = node.type in (TYPE_TASK, TYPE_CUSTOM)
        field_perm = self._resolve_field_permission(node) if is_task else None
        data = self._extract_fields(instance, field_perm, not exists or is_task, not exists or is_task)

        # 状态字段：优先 {节点ID}_{状态码} 列，无则 {节点ID} 列。
        # 任务节点写 DOING(10)——任务推进状态；结束节点写实例最终状态（FINISHED/REJECT）
        state_code = int(instance.state)
        if is_task:
            state_code = int(InstanceState.DOING)
        self._put_state_field(table_name, data, node.id, state_code)

        self._fill_context(data, instance)
        if not exists:
            self.writer.fill_system_fields(data, True)
            self.writer.insert(table_name, data)
            return
        self.writer.fill_system_fields(data, False)  # 只填 update 组
        self.writer.update(table_name, data, "process_instance_id", instance.id)

    # ── 公共 ──

    async def _resolve_define(self, instance: ProcessInstance) -> tuple[str, str]:
        define: Optional[ProcessDefine] = await self.loader(instance.defineId)
        if define is None:
            return "", ""
        try:
            meta = json.loads(define.content) if isinstance(define.content, str) else json.loads(define.content.decode("utf-8"))
        except (ValueError, AttributeError):
            return "", ""
        table_name = (meta.get("relTableName") or "").strip()
        if not table_name:
            table_name = (meta.get("name") or "").strip()  # 缺省回落流程 name
        return table_name, str(meta.get("persistMode") or "").strip()

    def _mark_chain(self, node, instance: ProcessInstance) -> bool:
        """同链重复触发防护（issues/19，1.8.0 节点级）：同一执行链中**每个节点**
        触发一次（任务推进更新 + 结束定稿是不同节点，都要生效），同节点不重复；
        exists 兜底跨请求。"""
        chain_key = f"__persist_executed_{instance.id}_{node.id}"
        if instance.variables.get(chain_key) is True:
            return False
        instance.variables[chain_key] = True
        return True

    def _extract_fields(self, instance: ProcessInstance, field_perm: Optional[dict],
                        include_task_fields: bool, include_form_fields: bool) -> dict[str, Any]:
        """提取字段：f_ 去前缀（SYNC 下按字段权限过滤——只读/隐藏不更新；
        include_form_fields=False 时不带出，用于非任务节点定稿避免覆盖只读限制）；
        tf_ 去前缀冗余（有列则写，列过滤由 writer 做）。"""
        prefix = self.field_prefix or "f_"
        task_prefix = self.task_field_prefix or "tf_"
        data: dict[str, Any] = {}
        for k, v in instance.variables.items():
            if include_form_fields and k.startswith(prefix) and len(k) > len(prefix):
                name = k[len(prefix):]
                if not self._is_editable(field_perm, name):
                    continue
                data[name] = v
            elif include_task_fields and k.startswith(task_prefix) and len(k) > len(task_prefix):
                data[k[len(task_prefix):]] = v
        return data

    def _resolve_field_permission(self, node) -> Optional[dict]:
        """任务节点字段权限（node.properties.field 的 PERMISSION_x；缺省 None=全部可编辑）"""
        props = getattr(node, "properties", None) or {}
        field = props.get("field")
        if isinstance(field, dict) and field:
            return field
        return None

    def _is_editable(self, field_perm: Optional[dict], field_name: str) -> bool:
        """字段可编辑判定：无声明或 EDIT(2) 可更新；READ_ONLY(1)/HIDDEN(3) 不更新。
        键格式兼容两种（issues/25）：
        - ``PERMISSION_f_{表单字段全名}``——前端 vben5-wf 设计器约定（优先）
        - ``PERMISSION_{去前缀名}``——后端 1.8.0 首版格式（兼容）"""
        if not field_perm:
            return True
        prefix = self.field_prefix or "f_"
        perm = field_perm.get(f"PERMISSION_{prefix}{field_name}")
        if perm is None:
            perm = field_perm.get(f"PERMISSION_{field_name}")
        if perm is None:
            return True
        return int(perm) == PERM_EDIT

    def _put_state_field(self, table_name: str, data: dict[str, Any], node_id: str, state_code: int) -> None:
        """状态字段写入：优先 {节点ID}_{状态码} 列，无则 {节点ID} 列（列探测过滤）"""
        if not node_id:
            return
        kept = self.writer.filter_columns(table_name, [f"{node_id}_{state_code}", node_id])
        if kept:
            data[kept[0]] = state_code

    def _fill_context(self, data: dict[str, Any], instance: ProcessInstance) -> None:
        """流程上下文字段（蛇形列名约定，与 writer 系统字段一致）"""
        data.setdefault("process_instance_id", instance.id)
        data.setdefault("apply_user_id", instance.operator)
        data.setdefault("apply_dept_id", instance.variables.get(KEY_DEPT_ID))
