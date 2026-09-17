# -*- coding: utf-8 -*-
"""Diem lap rap: FastAPI app phuc vu chieu Odoo -> edge, cong voi EdgeAgent
chay nen phuc vu chieu edge -> Odoo (hello/config/measurements/heartbeat/print).
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .inbound_api import router as inbound_router
from .node_api import router as node_router
from .scheduler import EdgeAgent
from .settings_api import router as settings_router

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
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
