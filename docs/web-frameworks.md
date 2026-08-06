# Python · Web 框架接入（统一门面转发层）

> 目标：**任意 Python Web 框架都能在 10 分钟内接入统一门面（JeeflowFacade）**——
> 门面接入 = **1 个路由 + 3 个注入点**，框架差异只在这 ~20 行转发层代码里。
> 引擎初始化、SPI、id 契约都是框架无关的（见 [SDK 集成](./getting-started.md) 与
> [规范 06 统一门面](../../spec/06-facade)）。

## 1. 门面接入模式（四步总则）

```
框架层                                jeeflow 引擎层
┌──────────────────────────┐         ┌──────────────────────┐
│ POST /wf/{action} 路由     │  body   │ JeeflowFacade         │
│ ① 登录校验（框架已有）      │ ──────→ │  flow(action, args)   │
│ ② 权限码动态校验            │  args   │  40 个 action 内置路由  │
│ ③ operator 注入            │         └──────────────────────┘
│ ④ listByType 结构转换（可选）│
└──────────────────────────┘
```

| # | 步骤 | 说明 |
|---|------|------|
| 1 | 路由捕获 action | `POST /wf/{action}`，action 是多段路径（`processDefine/page`） |
| 2 | 登录校验 | 用框架已有的登录中间件/装饰器（门面不感知登录态） |
| 3 | 权限码校验 | 引擎 SPI 提供映射（默认 `wf:{action.replace('/',':')}`），superAdmin 放行（见 [规范 06 §2.6](../../spec/06-facade)） |
| 4 | operator 注入 | `body["operator"] = 当前登录用户 id`——"我的"语义 action 依赖它过滤 |

> **雪花 id 精度**：Python 引擎出口已 `stringifyIDs` 保证 id 为字符串，前端按字符串处理即可。
> **listByType 转换**：`processDesign/listByType` 引擎返回 `Map<type, items>`；若前端按
> boot3 惯例期望 `[{type, title, items}]`，转发层做一次转换（见各框架示例）。

## 2. FastAPI（参考实现）

```python
from fastapi import APIRouter, Body

router = APIRouter(tags=["流程"])

@router.post("/wf/{action:path}", summary="jeeflow 门面转发")
async def wf_flow(action: str, body: dict = Body(default=None)):
    current_user = get_current_user()        # ① 框架登录上下文
    # ② 权限码动态校验（superAdmin 万能放行）
    if not current_user.superAdmin:
        codes = permission_codes(action)      # 引擎默认映射规则同款
        if codes and not any(c in current_user.permissions for c in codes):
            raise PermissionDenied()
    # ③ 注入操作人
    body = dict(body or {})
    body["operator"] = current_user.id
    # ④ 门面转发
    result = await facade.flow(action, body)
    # ⑤ listByType 结构转换（按需）
    if action == "processDesign/listByType" and result.get("code") == 0:
        result["data"] = [{"type": k, "title": "", "items": v}
                          for k, v in (result.get("data") or {}).items()]
    return result
```

- `{action:path}` 捕获多段路径（含 `/`），无需手动还原
- facade 单例由 `get_facade()` 提供（引擎初始化见 [FastAPI 集成](./fastapi.md)）
- 完整参考实现：mldong-fastapi 集成仓 `modules/wf/controllers/wf_controller.py`

## 3. Django REST Framework

```python
# urls.py
from django.urls import path, re_path
urlpatterns = [
    re_path(r"^wf/(?P<action>.+)$", wf_flow),   # action 多段路径
]

# views.py
import json
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

@csrf_exempt                        # ① 跨站防护豁免（API 用 token 鉴权）
@require_POST
def wf_flow(request, action):
    # ① 登录校验（框架已有：request.user / DRF 认证类）
    user = request.user
    if not user.is_authenticated:
        return JsonResponse({"code": 99990403, "msg": "未登录"}, status=401)
    # ② 权限码动态校验（superAdmin 万能放行）
    codes = permission_codes(action)
    if not user.is_superuser and codes and not any(
        c in user.permissions for c in codes):
        return JsonResponse({"code": 99990406, "msg": "无权限"}, status=403)
    # ③ 注入操作人
    body = json.loads(request.body or b"{}")
    body["operator"] = user.id
    # ④ 门面转发（同步引擎：asyncio.run 或引擎同步入口）
    result = facade.flow(action, body)
    # ⑤ listByType 转换（同 FastAPI 示例）
    return JsonResponse(result)
```

> **CSRF 必踩**：API 端点用 token 认证时需 `@csrf_exempt`；若走 DRF 的
> `@api_view` + `AuthenticationClasses`，CSRF 由 DRF 内置处理（SessionAuth 时保留）。
> **异步引擎**：若引擎为 asyncio 风格，同步视图用 `asyncio.run(facade.flow(...))`，
> 或直接用 `async def` 视图（Django 3.1+ 支持）。

## 4. Flask

```python
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.post("/wf/<path:action>")      # <path:> 捕获多段路径（含 /）
def wf_flow(action):
    # ① 登录校验（框架已有：session / flask-login）
    user = get_current_user()       # 自定义：从 session/token 解析
    if user is None:
        return jsonify({"code": 99990403, "msg": "未登录"}), 401
    # ② 权限码动态校验（superAdmin 万能放行）
    codes = permission_codes(action)
    if not user.superAdmin and codes and not any(
        c in user.permissions for c in codes):
        return jsonify({"code": 99990406, "msg": "无权限"}), 403
    # ③ 注入操作人
    body = request.get_json(silent=True) or {}
    body["operator"] = user.id
    # ④ 门面转发
    result = facade.flow(action, body)
    # ⑤ listByType 转换（同 FastAPI 示例）
    return jsonify(result)
```

## 5. 差异点对照表

| 要点 | FastAPI | Django DRF | Flask |
|------|---------|-----------|-------|
| 多段路径捕获 | `{action:path}` | `(?P<action>.+)` | `<path:action>` |
| CSRF | 无（自带） | 需 `@csrf_exempt` 或 DRF 认证 | 无（扩展才有） |
| 登录上下文 | 依赖注入/中间件 | `request.user` | session / 自定义 |
| 异步引擎 | 原生 async | `asyncio.run` 或 async 视图 | `asyncio.run` 或 async 视图 |
| 参考实现 | mldong-fastapi 集成仓 | — | — |

> 其他框架（Tornado/Quart/Sanic…）同理：套「1 路由 + 3 注入点」模式即可。
> 引擎初始化（仓储/SPI/用户体系映射）见 [SDK 集成](./getting-started.md) 与 [SPI 实现指南](./spi-guide.md)。
