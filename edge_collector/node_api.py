# -*- coding: utf-8 -*-
"""Hợp đồng NODE -> EDGE (Pi / PC bridge / thiết bị tự đẩy HTTP, pcm.source
kind='http_node'). KHÔNG phải một phần của /pcm/api/v1/* (đó là hợp đồng RIÊNG
của pcm_base, edge<->Main) — đây là phần MỚI, edge_collector tự định nghĩa,
vì pcm_base coi node<->edge là việc nội bộ của edge ("아래층이 위층을 부른다":
node gọi edge, edge gọi Main, không bao giờ ngược lại).

    POST /node/v1/hello              đăng ký/xin biết mình đã được Odoo nhận chưa
    POST /node/v1/measurements       {items:[{ch,v,s,q,ts,stable}], bid, seq}
    POST /node/v1/heartbeat          {fw, ip, uptime_s, rssi, buffered}
    GET  /node/v1/commands           node poll lệnh đang chờ (zero/tare/... từ Odoo)
    POST /node/v1/commands/ack       {id, ok, detail}

Xác thực: header X-Device-Serial luôn cần. X-API-Key CHỈ bắt buộc trên các
đường DỮ LIỆU (measurements/heartbeat/commands) nếu edge đã từng thay Odoo
cấp khóa cho serial này. RIÊNG /hello KHÔNG đòi hỏi khóa đúng — đó là đường
DUY NHẤT để node học/học lại khóa, giống hệt cách pcm_edge._hello() bên Odoo
luôn cho qua khi không có key kèm theo (chỉ từ chối khi CÓ gửi key và key đó
SAI). Nếu /hello cũng bị chặn cứng như các đường khác thì node sẽ không bao
giờ học được khóa thật (bug đã gặp: 'sai X-API-Key' lặp vĩnh viễn trên chính
/hello) — xem lịch sử sửa lỗi này trong DEV_STATUS/README.
"""
import logging
import time
from typing import Optional

from fastapi import APIRouter, Header, Request

_logger = logging.getLogger("edge.node_api")
router = APIRouter(prefix="/node/v1")


def _auth(request: Request, serial: Optional[str], api_key: Optional[str]):
    if not serial:
        return None, {"ok": False, "error": "thiếu header X-Device-Serial"}
    manager = request.app.state.manager
    cached = manager.cached_node_api_key(serial)
    if cached and cached != api_key:
        return None, {"ok": False, "error": "sai X-API-Key cho thiết bị %s" % serial}
    if not cached:
        _logger.info("node %s: edge chưa có api_key cho serial này (Odoo chưa tạo/chưa đồng bộ)",
                     serial)
    manager.touch_node(serial)
    return serial, None


@router.post("/hello")
async def node_hello(request: Request, x_device_serial: Optional[str] = Header(default=None),
                      x_api_key: Optional[str] = Header(default=None)):
    """KHÔNG dùng _auth() — đây là đường để node HỌC khóa, không phải đường
    cần khóa. Chỉ từ chối khi node gửi kèm một khóa SAI rõ ràng (bảo vệ khỏi
    mạo danh); không gửi khóa (trường hợp bình thường khi chưa học được) luôn
    được cho qua và trả về khóa đúng nếu edge đã biết."""
    if not x_device_serial:
        return {"ok": False, "error": "thiếu header X-Device-Serial"}
    manager = request.app.state.manager
    cached = manager.cached_node_api_key(x_device_serial)
    if cached and x_api_key and cached != x_api_key:
        return {"ok": False, "error": "sai X-API-Key cho thiết bị %s" % x_device_serial}
    manager.touch_node(x_device_serial)
    return {
        "ok": True,
        "known": cached is not None,
        "api_key": cached,          # None nếu Odoo chưa tạo pcm.device cho serial này
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
        return {"ok": False, "error": "items phải là list"}

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
    if n:
        _logger.info("node %s: accepted %d/%d readings", serial, n, len(items))
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
        return {"ok": False, "error": "id phải là số nguyên"}
    ok = request.app.state.manager.node_ack_command(cmd_id, bool(body.get("ok")),
                                                     body.get("detail") or "")
    return {"ok": ok}


@router.get("/config")
async def node_config(request: Request, x_device_serial: Optional[str] = Header(default=None),
                      x_api_key: Optional[str] = Header(default=None)):
    """Tiện ích thêm (không bắt buộc): node có thể hỏi lại nhãn/đơn vị kênh
    của chính nó đã khai trên Odoo, thay vì hard-code trong firmware."""
    serial, err = _auth(request, x_device_serial, x_api_key)
    if err:
        return err
    dev = request.app.state.manager.device_meta(serial)
    return {"ok": True, "channels": dev.get("channels", [])}
