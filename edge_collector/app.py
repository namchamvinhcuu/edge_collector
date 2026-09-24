# -*- coding: utf-8 -*-
"""Diem lap rap: FastAPI app phuc vu chieu Odoo -> edge, cong voi EdgeAgent
chay nen phuc vu chieu edge -> Odoo (hello/config/measurements/heartbeat/print).
"""
import logging
from contextlib import asynccontextmanager
from logging.handlers import RotatingFileHandler

from fastapi import FastAPI

from .config import settings
from .inbound_api import router as inbound_router
from .node_api import router as node_router
from .ops_api import router as ops_router
from .scheduler import EdgeAgent
from .settings_api import router as settings_router

# 20 MB x 5 ban luu = 100 MB. Do 19/09: edge_collector in ca payload cua
# TUNG lan gui measurements (hai dong: goi va phan hoi), ra 21 MB/ngay — voi
# vong 5 MB cu thi nhat ky chi giu duoc ~1,4 ngay, khong du de sang hom sau
# doc lai mot su co dem qua. Dia con 172 GB, 100 MB la re.
_LOG_MAX_BYTES = 20 * 1024 * 1024
_LOG_BACKUP_COUNT = 5


def _configure_logging() -> None:
    """Console-only KHONG du cho production (log mat het khi docker logs bi
    xoa/container restart, khong con lich su de troubleshoot tai site khach
    hang) - them RotatingFileHandler ghi ra settings.state_dir/logs, cung
    thu muc voi SQLite outbox/history (da la noi state duoc persist qua
    volume - xem docker-compose.yml) nen KHONG can cau hinh duong dan rieng.
    Xoay vong 5MB x 5 file (~25MB) - du cho troubleshoot ma khong lam day
    dia thiet bi edge. Giu NGUYEN console handler (khong bo basicConfig cu
    hoan toan) de `docker logs`/chay qua venv truc tiep van xem duoc ngay -
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

    # httpx ghi mot dong INFO cho TUNG request HTTP, va nhip gui len Odoo la
    # 1 giay — do duoc 19/09: httpx + edge.odoo_client chiem ~93% so dong,
    # lam vong log 30 MB quay het trong chua toi 5 gio. Mot su co luc 2 gio
    # sang thi 7 gio sang da khong con dau vet nao de doc.
    #
    # Ha xuong WARNING thi loi va timeout VAN ghi day du, chi bo phan "da
    # goi thanh cong" lap lai 86400 lan mot ngay.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Goi o day (luc ASGI lifespan startup THAT), KHONG o module-level nhu
    # truoc - import edge_collector.app (vd pytest collection, hoac bat ky
    # tool nao chi can doc module) se KHONG con tu dong tao thu muc/ghi file
    # log cua settings.state_dir THAT dang chay dev. Da tai hien duoc bug
    # nay: chay pytest tren may dev (EDGE_STATE_DIR that tro toi ./var_dev)
    # se ghi lan log httpx cua test suite vao dung file log cua instance
    # dev that dang chay - pha muc dich Observability + rui ro 2 process
    # cung mo RotatingFileHandler tren 1 file (rollover cua ben nay lam fd
    # ben kia stale, mat log am tham) - xem python-reviewer 2026-09-17.
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

    @app.get("/healthz")
    async def healthz():
        agent: EdgeAgent = app.state.agent
        mqtt_stats = agent.mqtt_consumer.stats
        return {
            "ok": True,
            "config_rev": agent.manager.config_rev,
            "outbox": agent.store.outbox_count(),
            "sources": agent.manager.status_rows(),
            # CHI so tong hop - "/healthz" cong khai KHONG qua auth (xem
            # docstring ops_api.py), nen bo "by_serial"/"online" (dinh danh
            # + trang thai song/chet cua tung thiet bi vat ly) khoi day; chi
            # tiet do da co o "/ops/api/state" (gate boi _check_setup_auth) -
            # xem python-reviewer 2026-09-24 (finding tu vong merge patch).
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
