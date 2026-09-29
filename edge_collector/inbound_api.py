# -*- coding: utf-8 -*-
"""Các đường Odoo -> edge (đồng bộ, do NGƯỜI DÙNG bấm nút trên màn hình):
    POST /api/command       zero/tare/read/write một kênh
    GET  /api/latest        giá trị mới nhất (bỏ qua, edge_client._latest)
    POST /api/browse        duyệt tag của một nguồn (OPC UA/Modbus...)
    POST /api/source/test   thử kết nối một cấu hình nguồn
    GET  /api/stats         thống kê lịch sử cục bộ (mẫu, tỷ lệ lỗi, stale)

Xác thực: pcm_base/tools/edge_client.py CHỈ gửi header X-Edge-Code, không có
khóa bí mật (thiết kế coi đây là 'chỉ trong LAN', giống edge_compat.py của
fms_iot_edge). Ở đây kiểm tra header đó khớp settings.edge_code khi có mặt -
không chặn cứng nếu thiếu (để tương thích thiết kế gốc) nhưng sẽ ghi log cảnh
báo. HÃY tự chặn tường lửa/route riêng cho cổng này, đừng để tràn ra Internet.
"""
import collections
import logging
import time
from typing import Optional

from fastapi import APIRouter, Header, Request

from .config import settings

_logger = logging.getLogger("edge.inbound_api")
router = APIRouter()

# Ring-buffer TRONG BỘ NHỚ (KHÔNG persist SQLite) cho panel 'PCM requests' ở
# /setup - hiển thị live request từ Odoo Main gọi xuống edge này. Mất khi
# restart là chấp nhận được (đây là telemetry hiển thị, khác history/outbox
# cần durable) - xem settings_api.py::setup_pcm_requests(). An toàn với
# 1-worker constraint (module-level singleton, không có await xen giữa
# deque.appendleft nên không cần lock, giống _pending trong scheduler.py).
_RECENT_MAXLEN = 50
_recent_requests = collections.deque(maxlen=_RECENT_MAXLEN)


def _log_request(endpoint: str, **fields) -> None:
    _recent_requests.appendleft({"ts": time.time(), "endpoint": endpoint, **fields})


def recent_requests() -> list:
    """Đọc cho panel 'PCM requests' ở /setup - xem settings_api.py."""
    return list(_recent_requests)


def _check_edge_code(x_edge_code: Optional[str]):
    if x_edge_code and x_edge_code != settings.edge_code:
        _logger.warning("X-Edge-Code không khớp (%s) - kiểm tra tường lửa cho cổng này", x_edge_code)


@router.post("/api/command")
async def api_command(request: Request, x_edge_code: Optional[str] = Header(default=None)):
    _check_edge_code(x_edge_code)
    body = await request.json()
    serial, ch, cmd = body.get("serial"), body.get("channel"), body.get("cmd") or "read"
    value = body.get("value")
    # Tham số phụ cho các kiểu phát của đèn: {"ms": 10000} = sáng 10 giây rồi
    # tự tắt, {"cmd":"blink","period_ms":500,"ms":30000} = chớp 30 giây.
    #
    # Chỉ chuyển tiếp những khóa ĐÃ BIẾT, không bung nguyên body xuống node:
    # firmware phân tích gói này bằng một bộ đệm 192 byte, nên một body thừa
    # trường sẽ bị cắt mất và lệnh im lặng không chạy.
    # type() thay isinstance(): bool là subclass của int trong Python, {"ms": true}
    # từ Odoo sẽ lọt qua isinstance(x, (int, float)) và merge xuống firmware dưới
    # dạng JSON "true" - firmware đợi số nguyên cho "ms", hỏng lặng lẽ.
    extra = {k: body[k] for k in ("ms", "period_ms") if type(body.get(k)) in (int, float)}
    _log_request("/api/command", serial=serial, ch=ch, cmd=cmd)
    manager = request.app.state.manager
    driver = manager.driver_for_channel(serial, ch)
    if driver:
        return await driver.command(ch, cmd, value)
    if serial in manager.known_node_serials():
        # Node không bị gọi ngược được: hoặc đẩy xuống qua MQTT, hoặc xếp
        # hàng cho firmware cũ tự poll — manager tự chọn, xem queue_command().
        return await manager.queue_command(serial, ch, cmd, value, extra=extra)
    return {"ok": False,
            "error": "không tìm thấy kênh %s của %s đang chạy trên edge này" % (ch, serial)}


@router.get("/api/latest")
async def api_latest(request: Request, serial: str = "", ch: str = "",
                      x_edge_code: Optional[str] = Header(default=None)):
    _check_edge_code(x_edge_code)
    _log_request("/api/latest", serial=serial, ch=ch)
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
    _log_request("/api/browse", source=source_code)
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
    _log_request("/api/source/test", kind=src_cfg.get("kind"))
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
    _log_request("/api/stats", serial=serial, ch=ch, hours=hours)
    store = request.app.state.store
    since_ts = time.time() - max(0.1, hours) * 3600
    stats = store.history_stats(serial, ch, since_ts)
    stats["minutes"] = int(hours * 60)
    return dict(stats, ok=True)
