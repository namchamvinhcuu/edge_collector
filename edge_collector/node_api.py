# -*- coding: utf-8 -*-
"""Hop dong NODE -> EDGE (Pi / PC bridge / thiet bi tu day HTTP, pcm.source
kind='http_node'). KHONG phai mot phan cua /pcm/api/v1/* (do la hop dong RIENG
cua pcm_base, edge<->Main) — day la phan MOI, edge_collector tu dinh nghia,
vi pcm_base coi node<->edge la viec noi bo cua edge ("아래층이 위층을 부른다":
node goi edge, edge goi Main, khong bao gio nguoc lai).

    POST /node/v1/hello              dang ky/xin biet minh da duoc Odoo nhan chua
    POST /node/v1/measurements       {items:[{ch,v,s,q,ts,stable}], bid, seq}
    POST /node/v1/heartbeat          {fw, ip, uptime_s, rssi, buffered}
    GET  /node/v1/commands           node poll lenh dang cho (zero/tare/... tu Odoo)
    POST /node/v1/commands/ack       {id, ok, detail}

Xac thuc: header X-Device-Serial luon can. X-API-Key CHI bat buoc tren cac
duong DU LIEU (measurements/heartbeat/commands) neu edge da tung thay Odoo
cap khoa cho serial nay. RIENG /hello KHONG doi hoi khoa dung — do la duong
DUY NHAT de node hoc/hoc lai khoa, giong het cach pcm_edge._hello() ben Odoo
luon cho qua khi khong co key kem theo (chi tu choi khi CO gui key va key do
SAI). Neu /hello cung bi chan cung nhu cac duong khac thi node se khong bao
gio hoc duoc khoa that (bug da gap: 'sai X-API-Key' lap vinh vien tren chinh
/hello) — xem lich su sua loi nay trong DEV_STATUS/README.
"""
import logging
import time
from typing import Optional

from fastapi import APIRouter, Header, Request

_logger = logging.getLogger("edge.node_api")
router = APIRouter(prefix="/node/v1")


def _auth(request: Request, serial: Optional[str], api_key: Optional[str]):
    if not serial:
        return None, {"ok": False, "error": "thieu header X-Device-Serial"}
    manager = request.app.state.manager
    cached = manager.cached_node_api_key(serial)
    if cached and cached != api_key:
        return None, {"ok": False, "error": "sai X-API-Key cho thiet bi %s" % serial}
    if not cached:
        _logger.info("node %s: edge chua co api_key cho serial nay (Odoo chua tao/chua dong bo)",
                     serial)
    manager.touch_node(serial)
    return serial, None


@router.post("/hello")
async def node_hello(request: Request, x_device_serial: Optional[str] = Header(default=None),
                      x_api_key: Optional[str] = Header(default=None)):
    """KHONG dung _auth() — day la duong de node HOC khoa, khong phai duong
    can khoa. Chi tu choi khi node gui kem mot khoa SAI ro rang (bao ve khoi
    mao danh); khong gui khoa (truong hop binh thuong khi chua hoc duoc) luon
    duoc cho qua va tra ve khoa dung neu edge da biet."""
    if not x_device_serial:
        return {"ok": False, "error": "thieu header X-Device-Serial"}
    manager = request.app.state.manager
    cached = manager.cached_node_api_key(x_device_serial)
    if cached and x_api_key and cached != x_api_key:
        return {"ok": False, "error": "sai X-API-Key cho thiet bi %s" % x_device_serial}
    manager.touch_node(x_device_serial)
    return {
        "ok": True,
        "known": cached is not None,
        "api_key": cached,          # None neu Odoo chua tao pcm.device cho serial nay
        "server_time_ms": int(time.time() * 1000),
    }


@router.post("/measurements")
async def node_measurements(request: Request, x_device_serial: Optional[str] = Header(default=None),
                             x_api_key: Optional[str] = Header(default=None)):
    serial, err = _auth(request, x_device_serial, x_api_key)
    if err:
        return err
    body = await request.json()
    items = body.get("items")
    if not isinstance(items, list):
        return {"ok": False, "error": "items phai la list"}

    agent = request.app.state.agent
    n = 0
    for it in items[:1000]:
        if not isinstance(it, dict):
            continue
        ch = it.get("ch")
        if not ch:
            continue
        ts = it.get("ts")
        agent.push_node_reading(
            serial, ch, it.get("v"), it.get("s"), int(it.get("q") or 0),
            (ts / 1000.0) if isinstance(ts, (int, float)) else None,
            it.get("stable"),
        )
        n += 1
    return {"ok": True, "accepted": n}


@router.post("/heartbeat")
async def node_heartbeat(request: Request, x_device_serial: Optional[str] = Header(default=None),
                         x_api_key: Optional[str] = Header(default=None)):
    serial, err = _auth(request, x_device_serial, x_api_key)
    if err:
        return err
    body = await request.json()
    agent = request.app.state.agent
    res = await agent.forward_node_heartbeat(serial, body)
    return {"ok": bool(res.get("ok")), "config_version": res.get("config_version")}


@router.get("/commands")
async def node_commands(request: Request, x_device_serial: Optional[str] = Header(default=None),
                        x_api_key: Optional[str] = Header(default=None)):
    serial, err = _auth(request, x_device_serial, x_api_key)
    if err:
        return err
    cmd = request.app.state.manager.node_pull_command(serial)
    return {"command": cmd}


@router.post("/commands/ack")
async def node_commands_ack(request: Request, x_device_serial: Optional[str] = Header(default=None),
                            x_api_key: Optional[str] = Header(default=None)):
    serial, err = _auth(request, x_device_serial, x_api_key)
    if err:
        return err
    body = await request.json()
    cmd_id = body.get("id")
    if not isinstance(cmd_id, int):
        return {"ok": False, "error": "id phai la so nguyen"}
    ok = request.app.state.manager.node_ack_command(cmd_id, bool(body.get("ok")),
                                                     body.get("detail") or "")
    return {"ok": ok}


@router.get("/config")
async def node_config(request: Request, x_device_serial: Optional[str] = Header(default=None),
                      x_api_key: Optional[str] = Header(default=None)):
    """Tien ich them (khong bat buoc): node co the hoi lai nhan/don vi kenh
    cua chinh no da khai tren Odoo, thay vi hard-code trong firmware."""
    serial, err = _auth(request, x_device_serial, x_api_key)
    if err:
        return err
    dev = request.app.state.manager.device_meta(serial)
    return {"ok": True, "channels": dev.get("channels", [])}
