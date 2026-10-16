"""audit8 R81（issues/187 §5 的 python 同形档）：flows 镜像腿的原子性与稳态零写。

旧形状＝逐份 `shutil.copyfile`（truncate→write 非原子）＋每次调用都逐份重写 ⇒ 读侧
（隔壁测试进程）可能撞在半写态上。案面「范围」列的纪律是**先实测撞不撞得上再定档**：
本栈 pytest 门票默认单进程（`pyproject.toml` 无 xdist/`-n`），真暴露面是
「T0 与 T1 真库腿并跑」这类多进程场景 ⇒ 这里把"撞得上"做成稳定形状（大夹具＋多进程对拍），
而不是等偶现。

三档各打一件事，形状照 node 仓 `__tests__/flows-mirror-187.test.ts`（同一修法、同一判据面）：
  ① 镜像语义＝全量复制＋删孤儿＋**稳态零写**＋无 Java 源静默跳过；
  ② 并发读写＝多进程镜像时读者一次半写都不得读到（ok 数下限自证"真读到过"，防空转假绿）；
  ③ 失败上抛不吞、抛出前清掉自己的临时件。

变异对照（本轮实跑）见 hub `docs/audit8-2026-10/notes/R81.md`。
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import flows_resolver as FR

JAVA_REL = os.path.join("jeeflow-java", "jeeflow-core", "src", "test", "resources", "flows")
FILES = ["01-a.json", "02-b.json", "03-c.json", "04-d.json", "05-e.json", "06-f.json"]
PAD = 512 * 1024  # 撑开写入窗口：真件 JSON 只有几 KB，窗口微秒级，非原子写根本撞不上


def body(ch, n=PAD):
    return json.dumps({"id": 1, "pad": ch * n}).encode()


def make_tree():
    """造一棵假仓树：<tmp>/proj/flows ＋ <tmp>/jeeflow-java/.../flows（resolver 认的兄弟目录布局）。"""
    root = tempfile.mkdtemp(prefix="r81-")
    proj = os.path.join(root, "proj")
    java_flows = os.path.join(root, JAVA_REL)
    os.makedirs(os.path.join(proj, "flows"), exist_ok=True)
    os.makedirs(java_flows, exist_ok=True)
    return proj, java_flows, root


def cleanup(root):
    shutil.rmtree(root, ignore_errors=True)


def src_of(proj):
    """resolver 眼里 Java 源的路径（proj 的兄弟目录）。"""
    return os.path.join(proj, FR._JAVA_FLOWS_REL)


def write(path, data):
    with open(path, "wb") as f:
        f.write(data)


def read(path):
    with open(path, "rb") as f:
        return f.read()


def tmp_leftovers(d):
    return [f for f in os.listdir(d) if ".tmp." in f]


def test_镜像语义_全量复制加删孤儿加稳态零写加无源静默():
    proj, java_flows, root = make_tree()
    try:
        write(os.path.join(java_flows, "01-a.json"), body("a", 64))
        write(os.path.join(proj, "flows", "orphan.json"), b'{"id":99}')

        FR._mirror(proj)
        assert read(os.path.join(proj, "flows", "01-a.json")) == body("a", 64), "Java 源应整份镜像"
        assert not os.path.exists(os.path.join(proj, "flows", "orphan.json")), "孤儿 .json 应被删"

        # 稳态零写：内容没变就一个字节都不许动（mtime 不变 ⇒ 读者的窗口根本不存在）
        before = os.stat(os.path.join(proj, "flows", "01-a.json")).st_mtime_ns
        FR._mirror(proj)
        FR._mirror(proj)
        assert os.stat(os.path.join(proj, "flows", "01-a.json")).st_mtime_ns == before, "内容相同不得重写"
        assert tmp_leftovers(os.path.join(proj, "flows")) == [], "不该留下临时件"

        # 内容变了要落盘
        write(os.path.join(java_flows, "01-a.json"), body("b", 64))
        FR._mirror(proj)
        assert read(os.path.join(proj, "flows", "01-a.json")) == body("b", 64), "源变更应镜像落盘"

        # 无 Java 源（用户单仓 / 容器）：静默跳过，不抛不写
        bare = tempfile.mkdtemp(prefix="r81-bare-")
        try:
            os.makedirs(os.path.join(bare, "flows"), exist_ok=True)
            write(os.path.join(bare, "flows", "keep.json"), b'{"id":7}')
            FR._mirror(bare)
            assert read(os.path.join(bare, "flows", "keep.json")) == b'{"id":7}'
        finally:
            shutil.rmtree(bare, ignore_errors=True)
    finally:
        cleanup(root)


WRITER = """
import json, os, sys, time
sys.path.insert(0, sys.argv[1])
import flows_resolver as FR

repo_root, proj, java_flows, iters, pad_len, files_arg = sys.argv[1:7]
files = files_arg.split(",")
A = json.dumps({"id": 1, "pad": "a" * int(pad_len)}).encode()
B = json.dumps({"id": 1, "pad": "b" * int(pad_len)}).encode()
# 源腿自己走原子替换（写临时件再 replace）：本用例只审"镜像写 dst"这条腿，
# 别让源侧半写混进来——那会让红灯归因错地方（假红）。
# Windows 上隔壁写进程 _mirror() 里的 open(src) 同样会咬住 replace ⇒ 这条腿也要重试。
def put(p, buf):
    tmp = p + ".tmp.%d" % os.getpid()
    for i in range(301):
        with open(tmp, "wb") as f:
            f.write(buf)
        try:
            os.replace(tmp, p)
            return
        except OSError:
            if i == 300:
                raise
            time.sleep(0.01)

for i in range(int(iters)):
    for fn in files:
        put(os.path.join(java_flows, fn), B if i % 2 else A)
    FR._mirror(proj)
"""

READER = """
import json, os, random, sys, time
flows_dir, files_arg, dur = sys.argv[1], sys.argv[2], float(sys.argv[3])
files = files_arg.split(",")
t0 = time.time()
ok = content_torn = denied_retry_ok = denied_unresolved = 0
samples = []
# 像真实测试那样消费：每轮挑一份读一次、留一点间隙（不是死盯一份文件不放句柄）
while time.time() - t0 < dur:
    fn = random.choice(files)
    data = None
    for attempt in range(4):
        try:
            with open(os.path.join(flows_dir, fn), "rb") as f:
                data = f.read()
            break
        except PermissionError as e:
            # Windows 专属一档：CPython 的 open 不带 FILE_SHARE_DELETE，replace 那一瞬
            # 新读者可能拿到瞬时 EACCES（node/libuv 带了共享删除位 ⇒ 同一场景不会）。
            # 这不是"半写"——判据把它单列，重试到成功就记 retry_ok，四次仍不开才判红。
            if attempt == 3:
                denied_unresolved += 1
                if len(samples) < 2:
                    samples.append("%s: %s" % (fn, str(e)[:60]))
            else:
                denied_retry_ok += 1
                time.sleep(0.005)
        except OSError as e:
            denied_unresolved += 1
            if len(samples) < 2:
                samples.append("%s: %s" % (fn, str(e)[:60]))
            break
    if data is None:
        continue
    try:
        json.loads(data.decode())
        ok += 1
    except Exception as e:
        content_torn += 1
        if len(samples) < 2:
            samples.append("%s: %s" % (fn, str(e)[:60]))
    time.sleep(0.001)
print(json.dumps({"ok": ok, "content_torn": content_torn, "denied_retry_ok": denied_retry_ok,
                  "denied_unresolved": denied_unresolved, "samples": samples}))
"""


def test_并发读写_多进程镜像时读者不得撞半写():
    proj, java_flows, root = make_tree()
    reader_dir = tempfile.mkdtemp(prefix="r81-r-")
    try:
        for fn in FILES:
            write(os.path.join(java_flows, fn), body("a"))
        FR._mirror(proj)  # 先就位，读者首轮就有东西读

        writer_py = os.path.join(proj, "writer.py")
        reader_py = os.path.join(reader_dir, "reader.py")
        write(writer_py, WRITER.encode())
        write(reader_py, READER.encode())

        # 门票实测（案面"定档前先测撞不撞得上"的兑现）：T0 单进程串行为常态，
        # 这里按最坏真实场景＝两个写进程（T0 与 T1 各一）＋三个读进程并行。
        ws = [
            subprocess.Popen(
                [sys.executable, writer_py, os.path.dirname(os.path.abspath(FR.__file__)),
                 proj, java_flows, "4", str(PAD), ",".join(FILES)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            for _ in range(2)
        ]
        rs = [
            subprocess.Popen(
                [sys.executable, reader_py, os.path.join(proj, "flows"), ",".join(FILES), "2.5"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            for _ in range(3)
        ]
        w_out = [(p.wait(), p.stderr.read().decode(errors="replace")) for p in ws]
        r_out = [(p.wait(), p.stdout.read().decode(), p.stderr.read().decode(errors="replace")) for p in rs]

        # 写腿的契约＝"要么写进去，要么响亮失败"，不是"必须写进去"：Windows 上读者持句柄时
        # replace 可被挡到重试预算之外（CPython 的 open 不带 FILE_SHARE_DELETE，node/libuv 带），
        # 那种挡是瞬时资源竞争、不是数据损坏 ⇒ 允许退出码非 0，但**必须是我们自己那条镜像错**
        # （静默丢夹具、抛别的异常、或压根没跑完都算红）。
        for code, err in w_out:
            if code == 0:
                continue
            assert "镜像" in err and "失败" in err, "写进程失败必须是响亮的镜像失败：%s" % err[:300]
        # 无论写腿成没成，目标目录里每一份 .json 都必须是完整 JSON（半写＝判据要抓的那件事）
        for fn in FILES:
            p = os.path.join(proj, "flows", fn)
            if os.path.exists(p):
                json.loads(read(p).decode())
        parsed = []
        for code, out, err in r_out:
            assert code == 0, "读者进程异常退出：%s" % err[:300]
            parsed.append(json.loads(out.strip() or "{}"))
        total_ok = sum(j.get("ok", 0) for j in parsed)
        total_torn = sum(j.get("content_torn", 0) for j in parsed)
        total_denied = sum(j.get("denied_retry_ok", 0) for j in parsed)
        total_unresolved = sum(j.get("denied_unresolved", 0) for j in parsed)
        assert total_ok > 100, "读者得真读到足够多次才构成判据（实读 ok=%d）" % total_ok
        assert total_torn == 0, "内容撕裂读数（半写）：%s" % parsed
        assert total_unresolved == 0, "重试后仍读不到的次数：%s" % parsed
        # 记录性断言（不是判据）：denied 档真出现过才说明这条腿测到了 Windows 的 replace 窗口；
        # 一次都没有也不判红（同一台机器不同时刻表现不同），但要把读数打出来供台账引用。
        print("\n[R81] ok=%d content_torn=0 denied_retry=%d denied_unresolved=0" % (total_ok, total_denied))
        assert tmp_leftovers(os.path.join(proj, "flows")) == [], "并发跑完不得留临时件"
    finally:
        shutil.rmtree(reader_dir, ignore_errors=True)
        cleanup(root)


def test_写入腿结构判据_只走带pid临时件加原子替换():
    """②的 Storm 档在本机对"就地 truncate→write"这种坏形状**撞不出红**（实测＝变异 MB 三档全绿），
    所以原子性另立一条确定性结构判据：写入腿只许"写 `*.tmp.<pid>` → `os.replace`"，
    一旦有哪个'wb'直接开在目标文件上，半写窗口就存在 ⇒ 红。probabilistic 与 deterministic 两档
    各管一件事，别拿 storm 档当原子性的唯一证据。"""
    proj, java_flows, root = make_tree()
    try:
        write(os.path.join(java_flows, "01-a.json"), body("d", 64))
        FR._mirror(proj)  # 先把目标建好，下面才只测"内容变了要重写"这条腿

        real_open = open
        opens = []
        replaces = []

        def spy_open(file, mode="r", *a, **kw):
            if "w" in mode:
                opens.append((str(file), mode))
            return real_open(file, mode, *a, **kw)

        real_replace = os.replace

        def spy_replace(src, dst, *a, **kw):
            replaces.append((str(src), str(dst)))
            return real_replace(src, dst, *a, **kw)

        builtins_open_module = sys.modules["builtins"]
        builtins_open_module.open = spy_open
        FR.os.replace = spy_replace
        try:
            write(os.path.join(java_flows, "01-a.json"), body("e", 64))
            FR._mirror(proj)
        finally:
            builtins_open_module.open = real_open
            FR.os.replace = real_replace

        target = os.path.join(proj, "flows", "01-a.json")
        assert read(target) == body("e", 64), "前置条件：源变更确实落到了目标"
        direct = [p for p, m in opens if p == target and "w" in m]
        assert direct == [], "写入腿不得以写模式直接打开目标文件（那＝truncate→write 的半写窗口）"
        assert replaces, "写入腿必须经一次原子替换"
        src, dst = replaces[-1]
        assert dst == target, "replace 的目标应是镜像件本身"
        assert (".tmp.%d" % os.getpid()) in os.path.basename(src), "临时件名要带 pid（并发互不覆写）"
    finally:
        cleanup(root)


def test_镜像失败上抛不吞_不留孤儿临时件():
    proj, java_flows, root = make_tree()
    try:
        write(os.path.join(java_flows, "01-a.json"), body("c", 64))
        os.mkdir(os.path.join(proj, "flows", "01-a.json"))  # 目标名被目录占位 ⇒ 写入必失败
        raised = None
        try:
            FR._mirror(proj)
        except Exception as e:  # noqa: BLE001 - 判据就是"必须抛"
            raised = e
        assert raised is not None, "镜像失败必须抛出，不能静默丢夹具"
        assert tmp_leftovers(os.path.join(proj, "flows")) == [], "抛出前应清掉自己的临时件"
    finally:
        cleanup(root)
