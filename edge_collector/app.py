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
from .scheduler import EdgeAgent
from .settings_api import router as settings_router

_LOG_MAX_BYTES = 5 * 1024 * 1024
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

    @app.get("/healthz")
    async def healthz():
        agent: EdgeAgent = app.state.agent
        return {
            "ok": True,
            "config_rev": agent.manager.config_rev,
            "outbox": agent.store.outbox_count(),
            "sources": agent.manager.status_rows(),
        }

    return app


app = create_app()
