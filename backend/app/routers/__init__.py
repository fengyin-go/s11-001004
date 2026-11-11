"""业务模块路由汇总。

这里统一按别名导入再暴露 ROUTERS：模块名有可能和内置名撞车（某个业务模块就叫 dict、list
这种名字时），按名字直接 import 会把内置类型覆盖掉，函数注解在运行时求值就会报
'module' object is not subscriptable。
"""
from __future__ import annotations

from app.routers import collector as router_collector
from app.routers import crawlrule as router_crawlrule
from app.routers import parser as router_parser
from app.routers import increment as router_increment
from app.routers import ratelimit as router_ratelimit
from app.routers import retry as router_retry
from app.routers import rpaflow as router_rpaflow
from app.routers import captcha as router_captcha
from app.routers import proxy as router_proxy
from app.routers import session as router_session
from app.routers import router as router_router
from app.routers import validate as router_validate
from app.routers import dedup as router_dedup
from app.routers import format as router_format
from app.routers import pipeline as router_pipeline
from app.routers import storage as router_storage
from app.routers import monitor as router_monitor
from app.routers import scheduler as router_scheduler
from app.routers.batch_workspace import receipt_router as router_batch_receipts
from app.routers.batch_workspace import router as router_batch_workspace

ROUTERS = [router_collector, router_crawlrule, router_parser, router_increment, router_ratelimit, router_retry, router_rpaflow, router_captcha, router_proxy, router_session, router_router, router_validate, router_dedup, router_format, router_pipeline, router_storage, router_monitor, router_scheduler, router_batch_workspace, router_batch_receipts]
