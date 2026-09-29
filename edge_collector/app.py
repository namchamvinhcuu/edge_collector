# -*- coding: utf-8 -*-
"""Điểm lắp ráp: FastAPI app phục vụ chiều Odoo -> edge, cộng với EdgeAgent
chạy nền phục vụ chiều edge -> Odoo (hello/config/measurements/heartbeat/print).
"""
import logging
from contextlib import asynccontextmanager
from logging.handlers import RotatingFileHandler

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from .config import settings
from .inbound_api import router as inbound_router
from .node_api import router as node_router
from .ops_api import router as ops_router
from .scheduler import EdgeAgent
from .settings_api import router as settings_router

# 20 MB x 5 bản lưu = 100 MB. Đo 19/09: edge_collector in cả payload của
# TỪNG lần gửi measurements (hai dòng: gọi và phản hồi), ra 21 MB/ngày — với
# vòng 5 MB cũ thì nhật ký chỉ giữ được ~1,4 ngày, không đủ để sang hôm sau
# đọc lại một sự cố đêm qua. Đĩa còn 172 GB, 100 MB là rẻ.
_LOG_MAX_BYTES = 20 * 1024 * 1024
_LOG_BACKUP_COUNT = 5


def _configure_logging() -> None:
    """Console-only KHÔNG đủ cho production (log mất hết khi docker logs bị
    xóa/container restart, không còn lịch sử để troubleshoot tại site khách
    hàng) - thêm RotatingFileHandler ghi ra settings.state_dir/logs, cùng
    thư mục với SQLite outbox/history (đã là nơi state được persist qua
    volume - xem docker-compose.yml) nên KHÔNG cần cấu hình đường dẫn riêng.
    Xoay vòng 5MB x 5 file (~25MB) - đủ cho troubleshoot mà không làm đầy
    đĩa thiết bị edge. Giữ NGUYÊN console handler (không bỏ basicConfig cũ
    hoàn toàn) để `docker logs`/chạy qua venv trực tiếp vẫn xem được ngay -
    xem review 2026-09-17 (Observability by Default)."""
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    log_dir = settings.state_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        log_dir / "edge_collector.log", maxBytes=_LOG_MAX_BYTES,
        backupCount=_LOG_BACKUP_COUNT, encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(console_handler)
    root.addHandler(file_handler)

    # httpx ghi một dòng INFO cho TỪNG request HTTP, và nhịp gửi lên Odoo là
    # 1 giây — đo được 19/09: httpx + edge.odoo_client chiếm ~93% số dòng,
    # làm vòng log 30 MB quay hết trong chưa tới 5 giờ. Một sự cố lúc 2 giờ
    # sáng thì 7 giờ sáng đã không còn dấu vết nào để đọc.
    #
    # Hạ xuống WARNING thì lỗi và timeout VẪN ghi đầy đủ, chỉ bỏ phần "đã
    # gọi thành công" lặp lại 86400 lần một ngày.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Gọi ở đây (lúc ASGI lifespan startup THẬT), KHÔNG ở module-level như
    # trước - import edge_collector.app (vd pytest collection, hoặc bất kỳ
    # tool nào chỉ cần đọc module) sẽ KHÔNG còn tự động tạo thư mục/ghi file
    # log của settings.state_dir THẬT đang chạy dev. Đã tái hiện được bug
    # này: chạy pytest trên máy dev (EDGE_STATE_DIR thật trỏ tới ./var_dev)
    # sẽ ghi lẫn log httpx của test suite vào đúng file log của instance
    # dev thật đang chạy - phá mục đích Observability + rủi ro 2 process
    # cùng mở RotatingFileHandler trên 1 file (rollover của bên này làm fd
    # bên kia stale, mất log âm thầm) - xem python-reviewer 2026-09-17.
    _configure_logging()
    agent = EdgeAgent()
    app.state.agent = agent
    app.state.manager = agent.manager
    app.state.store = agent.store
    await agent.start()
    try:
        yield
    finally:
        await agent.stop()


def create_app() -> FastAPI:
    app = FastAPI(title="PCM Edge Collector", lifespan=lifespan)
    app.include_router(inbound_router)
    app.include_router(node_router)
    app.include_router(settings_router)
    app.include_router(ops_router)

    @app.get("/", include_in_schema=False)
    async def root_redirect():
        return RedirectResponse(url="/setup")

    @app.get("/healthz")
    async def healthz():
        agent: EdgeAgent = app.state.agent
        mqtt_stats = agent.mqtt_consumer.stats
        return {
            "ok": True,
            "config_rev": agent.manager.config_rev,
            "outbox": agent.store.outbox_count(),
            "sources": agent.manager.status_rows(),
            # CHỈ số tổng hợp - "/healthz" công khai KHÔNG qua auth (xem
            # docstring ops_api.py), nên bỏ "by_serial"/"online" (định danh
            # + trạng thái sống/chết của từng thiết bị vật lý) khỏi đây; chi
            # tiết đó đã có ở "/ops/api/state" (gate bởi _check_setup_auth) -
            # xem python-reviewer 2026-09-24 (finding từ vòng merge patch).
            "mqtt_consumer": {
                "connected": mqtt_stats["connected"],
                "messages": mqtt_stats["messages"],
                "items": mqtt_stats["items"],
                "forwarded": mqtt_stats["forwarded"],
                "bad": mqtt_stats["bad"],
                "last_ts": mqtt_stats["last_ts"],
                "cmd_sent": mqtt_stats["cmd_sent"],
                "cmd_acked": mqtt_stats["cmd_acked"],
                "ts_dropped": mqtt_stats["ts_dropped"],
            },
        }

    return app


app = create_app()
