# -*- coding: utf-8 -*-
"""Cac duong Odoo -> edge (dong bo, do NGUOI DUNG bam nut tren man hinh):
    POST /api/command       zero/tare/read/write mot kenh
    GET  /api/latest        gia tri moi nhat (bo qua, edge_client._latest)
    POST /api/browse        duyet tag cua mot nguon (OPC UA/Modbus...)
    POST /api/source/test   thu ket noi mot cau hinh nguon
    GET  /api/stats         thong ke lich su cuc bo (mau, ty le loi, stale)

Xac thuc: pcm_base/tools/edge_client.py CHI gui header X-Edge-Code, khong co
khoa bi mat (thiet ke coi day la 'chi trong LAN', giong edge_compat.py cua
fms_iot_edge). O day kiem tra header do khop settings.edge_code khi co mat -
khong chan cung neu thieu (de tuong thich thiet ke goc) nhung se ghi log canh
bao. HAY tu chan tuong lua/route rieng cho cong nay, dung de tran ra Internet.
"""
import logging
import time
from typing import Optional

from fastapi import APIRouter, Header, Request

from .config import settings

_logger = logging.getLogger("edge.inbound_api")
router = APIRouter()


def _check_edge_code(x_edge_code: Optional[str]):
    if x_edge_code and x_edge_code != settings.edge_code:
        _logger.warning("X-Edge-Code khong khop (%s) - kiem tra tuong lua cho cong nay", x_edge_code)


@router.post("/api/command")
async def api_command(request: Request, x_edge_code: Optional[str] = Header(default=None)):
    _check_edge_code(x_edge_code)
    body = await request.json()
    serial, ch, cmd = body.get("serial"), body.get("channel"), body.get("cmd") or "read"
    value = body.get("value")
    manager = request.app.state.manager
    driver = manager.driver_for_channel(serial, ch)
    if driver:
        return await driver.command(ch, cmd, value)
    if serial in manager.known_node_serials():
        # Node (http_node) khong bi goi nguoc duoc — xep hang cho no tu poll.
        return await manager.queue_command(serial, ch, cmd, value)
    return {"ok": False,
            "error": "khong tim thay kenh %s cua %s dang chay tren edge nay" % (ch, serial)}


@router.get("/api/latest")
async def api_latest(request: Request, serial: str = "", ch: str = "",
                      x_edge_code: Optional[str] = Header(default=None)):
    _check_edge_code(x_edge_code)
    store = request.app.state.store
    row = store.history_latest(serial, ch)
    if not row:
        return {"serial": serial, "ch": ch, "never": True, "age_ms": 10 ** 9}
    age_ms = int((time.time() - row["ts"]) * 1000)
    return {
        "serial": serial, "ch": ch, "v": row["v"], "s": row["s"] or "",
        "q": row["q"], "stable": row["stable"],
        "ts": int(row["ts"] * 1000), "age_ms": age_ms, "never": False,
    }


@router.post("/api/browse")
async def api_browse(request: Request, x_edge_code: Optional[str] = Header(default=None)):
    _check_edge_code(x_edge_code)
    body = await request.json()
    source_code, node_id, path = body.get("source"), body.get("node_id"), body.get("path")
    manager = request.app.state.manager
    driver = manager.get_driver(source_code)
    if not driver:
        return {"ok": False, "error": "nguon '%s' chua chay tren edge nay" % source_code}
    return await driver.browse(node_id=node_id, path=path)


@router.post("/api/source/test")
async def api_source_test(request: Request, x_edge_code: Optional[str] = Header(default=None)):
    _check_edge_code(x_edge_code)
    body = await request.json()
    src_cfg = body.get("source") or {}
    manager = request.app.state.manager
    try:
        drv = manager.build_probe(src_cfg)
    except Exception as exc:                                        # noqa: BLE001
        return {"ok": False, "items": [], "error": str(exc)[:200]}
    return await drv.test()


@router.get("/api/stats")
async def api_stats(request: Request, serial: str = "", ch: str = "", hours: float = 24,
                     x_edge_code: Optional[str] = Header(default=None)):
    _check_edge_code(x_edge_code)
    store = request.app.state.store
    since_ts = time.time() - max(0.1, hours) * 3600
    stats = store.history_stats(serial, ch, since_ts)
    stats["minutes"] = int(hours * 60)
    return dict(stats, ok=True)
