# -*- coding: utf-8 -*-
"""Các đường Odoo -> edge (đồng bộ, do NGƯỜI DÙNG bấm nút trên màn hình):
    POST /api/command       zero/tare/read/write một kênh
    GET  /api/latest        giá trị mới nhất (bỏ qua, edge_client._latest)
    POST /api/browse        duyệt tag của một nguồn (OPC UA/Modbus...)
    POST /api/source/test   thử kết nối một cấu hình nguồn
    GET  /api/stats         thống kê lịch sử cục bộ (mẫu, tỷ lệ lỗi, stale)
    POST /api/publish       publish MQTT tùy ý (node mqtt_publish), cấm topic lệnh

Xác thực (từ 2026-10-02, Nam duyệt - trước đây chỉ log warning, coi là LAN):
mọi /api/* đi qua _require_downlink_auth (dependency của router - path mới
thêm vào router này tự được bảo vệ). Odoo gửi X-Edge-Code + X-Edge-Ts (unix
giây) + X-Edge-Nonce (ngẫu nhiên mỗi request, ≤64 ký tự - để 2 request giống
hệt trong cùng giây không bị coi là replay) + X-Edge-Sig = hex
HMAC-SHA256(downlink_key, "METHOD\\npath[?query]\\nts\\nnonce\\nsha256_hex(body)").
downlink_key do Odoo cấp qua
/pcm/api/v1/edge/config (odoo_client.pull_config), KHÔNG bao giờ đi trên dây
chiều này - Odoo gọi edge qua Internet bằng http. Sai/thiếu/lệch giờ >30s/
replay/edge chưa có khóa -> HTTP 401 {"detail": {ok:false,status:"rejected"}}.
NGOẠI LỆ: /api/latest, /api/stats (chỉ đọc) - tablet trình duyệt gọi thẳng,
không ký được: không có X-Edge-Sig thì chỉ kiểm X-Edge-Code NẾU có; có chữ ký
thì kiểm đủ như trên. edge_code chỉ là mã định danh, không phải bí mật.
Vẫn nên chặn tường lửa cho cổng này.
"""
import collections
import hashlib
import hmac
import json
import logging
import time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request

from .config import settings

_logger = logging.getLogger("edge.inbound_api")

# Đường chỉ đọc mà tablet trình duyệt gọi thẳng (không ký được) - xem docstring.
_OPTIONAL_SIG_PATHS = frozenset({"/api/latest", "/api/stats"})
# 30s chứ không 60s: replay cache nằm trong RAM, mất khi restart edge - cửa
# sổ giờ là lớp chặn replay còn lại. Lệch giờ đã được bù theo header Date của
# Odoo (odoo_client._note_server_clock) nên 30s vẫn đủ rộng.
_SIG_MAX_SKEW_S = 30
# Chữ ký đã dùng -> hạn (monotonic). Chặn phát lại nguyên gói trong cửa sổ
# giờ hợp lệ. RAM là đủ: 1 worker (xem __main__.py). Giới hạn số mục.
# Dư gấp 4 cửa sổ: hạn cache tính bằng monotonic, còn cửa sổ ts theo giờ hệ
# thống - giờ edge bị lùi giữa 2 lần pull_config không mở lại khe replay.
_SEEN_SIG_TTL_S = 4 * _SIG_MAX_SKEW_S
_SEEN_SIG_MAX = 10000
_seen_sigs: dict = {}


def _remember_sig(sig: str) -> bool:
    """False nếu chữ ký đã thấy trong cửa sổ (replay), True và ghi nhận nếu chưa."""
    now = time.monotonic()
    for old in [s for s, exp in _seen_sigs.items() if exp <= now]:
        del _seen_sigs[old]
    if sig in _seen_sigs:
        return False
    if len(_seen_sigs) >= _SEEN_SIG_MAX:
        del _seen_sigs[next(iter(_seen_sigs))]
    _seen_sigs[sig] = now + _SEEN_SIG_TTL_S
    return True


def _downlink_string_to_sign(request: Request, ts: str, nonce: str, body: bytes) -> bytes:
    query = request.url.query
    return ("%s\n%s%s\n%s\n%s\n%s" % (
        request.method.upper(), request.url.path, "?" + query if query else "",
        ts, nonce, hashlib.sha256(body).hexdigest())).encode()


async def _downlink_auth_error(request: Request) -> Optional[str]:
    """Lý do từ chối (không chứa giá trị bí mật), None khi hợp lệ."""
    h = request.headers
    code, ts, sig = h.get("x-edge-code"), h.get("x-edge-ts"), h.get("x-edge-sig")
    if request.url.path in _OPTIONAL_SIG_PATHS and sig is None:
        if code is None or hmac.compare_digest(code.encode(), settings.edge_code.encode()):
            return None
        return "X-Edge-Code sai"
    if not code or not hmac.compare_digest(code.encode(), settings.edge_code.encode()):
        return "X-Edge-Code %s" % ("sai" if code else "thiếu")
    nonce = h.get("x-edge-nonce")
    if not ts or not sig or not nonce:
        return "thiếu X-Edge-Ts/X-Edge-Nonce/X-Edge-Sig"
    if len(nonce) > 64:
        return "X-Edge-Nonce quá dài"
    store = getattr(request.app.state, "store", None)
    key = store.kv_get("downlink_key") if store is not None else None
    if not key or not isinstance(key, str):
        return "edge chưa có downlink_key (chưa kéo config từ Main)"
    try:
        ts_int = int(ts)
    except ValueError:
        return "X-Edge-Ts không phải số nguyên"
    odoo = getattr(getattr(request.app.state, "agent", None), "odoo", None)
    now = time.time() + getattr(odoo, "clock_offset_s", 0.0)
    if abs(now - ts_int) > _SIG_MAX_SKEW_S:
        return "stale (lệch giờ %+.0fs)" % (ts_int - now)
    body = await request.body()
    expected = hmac.new(key.encode(), _downlink_string_to_sign(request, ts, nonce, body),
                        hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig.encode(), expected.encode()):
        return "X-Edge-Sig sai"
    if not _remember_sig(sig):
        return "replay"
    return None


async def _require_downlink_auth(request: Request) -> None:
    reason = await _downlink_auth_error(request)
    if reason:
        _logger.warning("từ chối %s %s: %s", request.method, request.url.path, reason)
        raise HTTPException(status_code=401, detail={
            "ok": False, "status": "rejected", "error": "xác thực thất bại: %s" % reason})


router = APIRouter(dependencies=[Depends(_require_downlink_auth)])

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


async def _read_json(request: Request):
    """(body, None) khi body là JSON object, (None, lỗi dạng {ok:false}) khi không."""
    try:
        body = await request.json()
    except ValueError:
        return None, {"ok": False, "status": "error", "error": "body không phải JSON hợp lệ"}
    if not isinstance(body, dict):
        return None, {"ok": False, "status": "error", "error": "body phải là JSON object"}
    return body, None


@router.post("/api/command")
async def api_command(request: Request):
    body, err = await _read_json(request)
    if err:
        return err
    serial, ch, cmd = body.get("serial"), body.get("channel"), body.get("cmd") or "read"
    value = body.get("value")
    if not serial or not ch:
        return {"ok": False, "status": "error", "error": "thiếu serial hoặc channel"}
    # request_id: uuid hex Odoo sinh cho mỗi lần bấm, node dùng để chống chạy
    # trùng. Chỉ nhận chuỗi ngắn - nó đi xuống firmware bộ đệm nhỏ.
    request_id = body.get("request_id")
    if not (isinstance(request_id, str) and 0 < len(request_id) <= 64):
        request_id = None
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
    # request.json() nhận NaN/Infinity (và 1e400 -> inf); json.dumps lại phát ra
    # NaN/Infinity - không phải JSON hợp lệ, cJSON trên node bỏ cả gói, lệnh
    # mất im lặng không có ack. Chặn ở đây để Odoo nhận ok:false rõ ràng.
    # allow_nan=False bắt cả NaN lồng trong value dạng list/dict.
    for k, v in (*extra.items(), ("value", value)):
        try:
            json.dumps(v, allow_nan=False)
        except ValueError:
            return {"ok": False, "status": "error",
                    "error": "%s không phải số hữu hạn" % k}
    _log_request("/api/command", serial=serial, ch=ch, cmd=cmd)
    manager = request.app.state.manager
    try:
        driver = manager.driver_for_channel(serial, ch)
        if driver:
            return await driver.command(ch, cmd, value)
        if serial in manager.known_node_serials():
            # Node không bị gọi ngược được: hoặc đẩy xuống qua MQTT, hoặc xếp
            # hàng cho firmware cũ tự poll — manager tự chọn, xem queue_command().
            return await manager.queue_command(serial, ch, cmd, value, extra=extra,
                                               request_id=request_id)
    except TimeoutError:
        # Driver timeout SAU khi đã gửi frame ghi: PLC có thể đã chạy lệnh -
        # cùng nghĩa "unknown" như node không ACK (manager.queue_command).
        _logger.warning("timeout thực thi lệnh %s trên %s/%s", cmd, serial, ch)
        return {"ok": False, "status": "unknown",
                "error": "thiết bị không trả lời kịp - có thể đã thực thi"}
    except Exception as exc:                                        # noqa: BLE001
        _logger.exception("lỗi thực thi lệnh %s trên %s/%s", cmd, serial, ch)
        return {"ok": False, "status": "error", "error": str(exc)[:200]}
    return {"ok": False, "status": "error",
            "error": "không tìm thấy kênh %s của %s đang chạy trên edge này" % (ch, serial)}


@router.get("/api/latest")
async def api_latest(request: Request, serial: str = "", ch: str = ""):
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
async def api_browse(request: Request):
    body, err = await _read_json(request)
    if err:
        return err
    source_code, node_id, path = body.get("source"), body.get("node_id"), body.get("path")
    _log_request("/api/browse", source=source_code)
    manager = request.app.state.manager
    driver = manager.get_driver(source_code)
    if not driver:
        return {"ok": False, "error": "nguon '%s' chua chay tren edge nay" % source_code}
    return await driver.browse(node_id=node_id, path=path)


@router.post("/api/source/test")
async def api_source_test(request: Request):
    body, err = await _read_json(request)
    if err:
        return err
    src_cfg = body.get("source") or {}
    if not isinstance(src_cfg, dict):
        return {"ok": False, "items": [], "error": "source phải là JSON object"}
    _log_request("/api/source/test", kind=src_cfg.get("kind"))
    manager = request.app.state.manager
    try:
        drv = manager.build_probe(src_cfg)
    except Exception as exc:                                        # noqa: BLE001
        return {"ok": False, "items": [], "error": str(exc)[:200]}
    return await drv.test()


@router.get("/api/stats")
async def api_stats(request: Request, serial: str = "", ch: str = "", hours: float = 24):
    _log_request("/api/stats", serial=serial, ch=ch, hours=hours)
    store = request.app.state.store
    since_ts = time.time() - max(0.1, hours) * 3600
    stats = store.history_stats(serial, ch, since_ts)
    stats["minutes"] = int(hours * 60)
    return dict(stats, ok=True)


_PUBLISH_MAX_BYTES = 4096
_TOPIC_MAX_CHARS = 256


def _under(topic: str, prefix: str) -> bool:
    """So theo LEVEL: "a/b" phủ "a/b" và "a/b/..." nhưng không phủ "a/bc".
    Không dùng startswith thô: thiết bị subscribe "a/b/#" cũng nhận "a/b"."""
    p = prefix.rstrip("/")
    return bool(p) and (topic == p or topic.startswith(p + "/"))


def _allowed_topic_prefixes() -> list:
    return [p.strip() for p in (settings.publish_topic_allow or "").split(",") if p.strip()]


def _reserved_topic_prefixes(manager) -> list:
    """Lớp chặn PHỤ (allowlist mới là lớp chính - review-rules.json): gốc
    giao thức node "fms" (cứng, kể cả khi EDGE_MQTT_CONSUMER_TOPIC có gốc
    wildcard), gốc consumer đang cấu hình, và gốc của mọi nguồn MQTT đang
    chạy (dưới đó vừa là lệnh <gốc>/cmd/, vừa là số đo <gốc>/#)."""
    root = (settings.mqtt_consumer_topic or "").split("/")[0]
    prefixes = ["fms"]
    if root and not any(c in root for c in ("+", "#")):
        prefixes.append(root)
    return prefixes + manager.mqtt_topic_bases()


def _publish_payload(raw) -> str:
    """str giữ nguyên; None -> ""; còn lại (số, bool, dict, list) -> JSON."""
    if raw is None:
        return ""
    if isinstance(raw, str):
        return raw
    return json.dumps(raw, ensure_ascii=False)


@router.post("/api/publish")
async def api_publish(request: Request):
    """Publish MQTT cho node `mqtt_publish` của ppd_process (Nam duyệt
    2026-10-02). Ký HMAC như mọi /api/* (router dependency). Topic phải khớp
    EDGE_PUBLISH_TOPIC_ALLOW (rỗng = tắt) VÀ không khớp _reserved_topic_prefixes."""
    body, err = await _read_json(request)
    if err:
        return err
    allow = _allowed_topic_prefixes()
    if not allow:
        return _publish_error("publish_disabled", "publish bị tắt (EDGE_PUBLISH_TOPIC_ALLOW rỗng)")
    topic = body.get("topic")
    if not isinstance(topic, str) or not topic:
        return _publish_error("invalid_topic", "thiếu topic")
    try:
        topic.encode("utf-8")
    except UnicodeEncodeError:
        return _publish_error("invalid_topic", "topic không mã hóa được UTF-8")
    if (len(topic) > _TOPIC_MAX_CHARS or topic.startswith(("/", "$"))
            or any(c in topic for c in ("+", "#", "\x00"))):
        return _publish_error("invalid_topic", "topic không hợp lệ")
    manager = request.app.state.manager
    if (any(_under(topic, p) for p in _reserved_topic_prefixes(manager))
            or not any(_under(topic, p) for p in allow)):
        _log_request("/api/publish", topic=topic, rejected="not allowed")
        return _publish_error("topic_not_allowed",
                              "topic không được phép (ngoài EDGE_PUBLISH_TOPIC_ALLOW "
                              "hoặc là topic thiết bị - lệnh phải qua /api/command)")
    payload = _publish_payload(body.get("payload"))
    try:
        size = len(payload.encode("utf-8"))
    except UnicodeEncodeError:
        return _publish_error("invalid_payload", "payload không mã hóa được UTF-8")
    if size > _PUBLISH_MAX_BYTES:
        return _publish_error("payload_too_large",
                              "payload %d byte vượt %d" % (size, _PUBLISH_MAX_BYTES))
    _log_request("/api/publish", topic=topic, size=size)
    mq = getattr(manager, "mqtt_cmd", None)
    # scheduler luôn gán mqtt_cmd kể cả khi consumer tắt -> phải xem cờ cấu
    # hình, nếu không publish_raw sẽ trả broker_down (Odoo coi là retry được).
    if mq is None or not settings.mqtt_consumer_enabled:
        return _publish_error("publish_disabled", "MQTT consumer chưa bật trên edge")
    return await mq.publish_raw(topic, payload)


def _publish_error(reason: str, error: str) -> dict:
    """`reason` máy đọc được - Odoo map: publish_disabled/topic_not_allowed/
    invalid_topic/invalid_payload/payload_too_large = lỗi cấu hình (không retry);
    broker_down = retry được; status "unknown" (reason no_puback) = có thể đã gửi."""
    return {"ok": False, "status": "error", "reason": reason, "error": error}
