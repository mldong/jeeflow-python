"""解析本仓 flows/ 流程定义目录（维护者与用户统一入口）。

唯一编辑源是 jeeflow-java 仓的 test/resources/flows/。本仓 flows/ 是其副本，
入库 commit（单语言用户下载即用，不依赖隔壁 Java 仓）。

dir() 的语义：
  1. 环境变量 JEEFLOW_FLOWS_DIR 显式覆盖（容器/特殊部署）
  2. 否则从当前工作目录向上找第一个含 flows/（且有 .json）的目录 = 本仓根
  3. 若本仓根的兄弟目录里有 Java 源（维护者机器）→ 精确镜像进本仓 flows/
     （拷贝所有 .json + 删除本仓多出的孤儿 .json，防 id 按文件名排序错位）
  4. 始终返回本仓 flows/ 路径 —— 所有读取点只读这里，Java 仓不再被直接读取

放在仓根（不进 jeeflow* 发布包）：demo 已把仓根 insert 进 sys.path，
pytest 从仓根跑，两者都能 import flows_resolver。
"""
import os
import time

# java 源目录相对本仓根的位置（jeeflow-java 与本仓是 jeeflow-hub 下的兄弟目录）
_JAVA_FLOWS_REL = os.path.join("..", "jeeflow-java", "jeeflow-core", "src", "test", "resources", "flows")


def dir() -> str:
    """返回本仓 flows/ 绝对路径；维护者机器上会先把 Java 源精确镜像进来。"""
    env = os.environ.get("JEEFLOW_FLOWS_DIR")
    if env:
        return env
    root = _find_flows_root(os.getcwd())
    if root is None:
        raise FileNotFoundError("flows_resolver: no flows/ directory found from %s" % os.getcwd())
    _mirror(root)
    return os.path.join(root, "flows")


def _find_flows_root(start):
    """从 start 向上找第一个含 flows/ 且内有 .json 的目录（本仓根）。"""
    d = os.path.abspath(start)
    while True:
        if _has_flows(d):
            return d
        parent = os.path.dirname(d)
        if parent == d:  # 到文件系统顶
            return None
        d = parent


def _has_flows(d):
    fdir = os.path.join(d, "flows")
    if not os.path.isdir(fdir):
        return False
    return any(f.endswith(".json") for f in os.listdir(fdir))


def _mirror(root):
    """若 Java 源存在则精确镜像到本仓 flows/（拷所有 + 删孤儿），不存在则原样返回。"""
    src = os.path.join(root, _JAVA_FLOWS_REL)
    dst = os.path.join(root, "flows")
    if not os.path.isdir(src):  # 用户单仓 / 容器：无 Java 源，跳过镜像
        return
    os.makedirs(dst, exist_ok=True)
    src_names = set()
    for f in os.listdir(src):
        if not f.endswith(".json"):
            continue
        src_names.add(f)
        _write_atomic(os.path.join(dst, f), _read_retry(os.path.join(src, f)))
    # 孤儿清理：本仓有、Java 源已无的 .json（防 id 错位）
    for f in os.listdir(dst):
        if f.endswith(".json") and f not in src_names:
            os.remove(os.path.join(dst, f))


_REPLACE_RETRY = 300
_REPLACE_RETRY_S = 0.01
_READ_RETRY = 100


def _read_retry(path):
    """读源件，撞 Windows 共享冲突（PermissionError）时短重试（上限 1s）。

    理由与写入腿同一档：隔壁进程正用 replace 换这份源件的那一瞬，CPython 的 `open()` 会被挡
    （libuv 带 `FILE_SHARE_DELETE`，node 那侧不挡 ⇒ 这是本栈自己的运行时差异，不是照抄 node）。
    真场景里源件只读、撞不上；撞上时宁可等一等，也不要把"读不到夹具"变成整轮测试的红。
    """
    for attempt in range(_READ_RETRY + 1):
        try:
            with open(path, "rb") as fh:
                return fh.read()
        except PermissionError:
            if attempt >= _READ_RETRY:
                raise
            time.sleep(_REPLACE_RETRY_S)


def _write_atomic(target, data):
    """原子镜像写腿（audit8 R81，形状照 node 仓 `flows-resolver.ts` 的 issues/170/187 修法）。

    旧形状＝`shutil.copyfile`：truncate→write 非原子，且每个进程每次 import 都逐份重写 ⇒
    读者（隔壁测试进程）撞在半写态上就随机 JSONDecodeError。三件事：
      1. **稳态零写**：内容逐字节相同就一个字节都不动 ⇒ 绝大多数测试轮里这条腿根本不写盘，窗口不存在；
      2. **`.tmp.<pid>` + `os.replace`**：名字带 pid ⇒ 并发进程互不覆写同一条临时件；同目录 replace
         对读者是原子的，只会看到旧件或新件；
      3. **失败上抛且不丢临时件**：Windows 上读者持句柄时 replace 报 PermissionError，重试
         `_REPLACE_RETRY` 次仍失败就抛（不静默丢夹具），抛出前清掉自己的临时件。
         ⚠ 预算比 node 大 3 倍是**本栈运行时差异**，不是照抄 node：CPython 的 `open()` 不带
         `FILE_SHARE_DELETE`（libuv 带），读者正在读目标文件时 python 的 replace 会被挡到秒级，
         而读者那一侧撞到的是**瞬时 PermissionError（重试即好）而不是半写内容**——两件事在判据里
         分档钉死（`tests/test_flows_mirror_atomic_r81.py`：storm 档只判"内容不得撕裂＋拒绝可重试"，
         原子性另有结构档兜底）。校准后形状（2 写进程 ×3 翻面轮 ×6 份 512KB ＋ 2 读者 ×2.5s）
         连跑 8 次全绿，三条变异（摘稳态零写／就地非原子写／静默吞失败）各红对应档。
    """
    try:
        with open(target, "rb") as fh:
            if fh.read() == data:
                return
    except OSError:
        pass  # 目标还不存在（或正被别的进程持有）：走写入腿
    tmp = "%s.tmp.%d" % (target, os.getpid())
    for attempt in range(_REPLACE_RETRY + 1):
        try:
            with open(tmp, "wb") as fh:
                fh.write(data)
            os.replace(tmp, target)
            return
        except OSError as err:
            if attempt >= _REPLACE_RETRY:
                try:
                    os.remove(tmp)
                except OSError:
                    pass
                raise OSError(
                    "flows_resolver: 镜像 %s 失败（重试 %d 次）：%s" % (target, _REPLACE_RETRY, err)
                )
            time.sleep(_REPLACE_RETRY_S)
