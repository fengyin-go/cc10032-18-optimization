"""业务路由汇总：序号18专用路由需在通用资源路由之前注册，避免被动态路径抢先匹配。"""
from __future__ import annotations

from app.routers import resource, sampling_debug

ROUTERS = [sampling_debug, resource]
