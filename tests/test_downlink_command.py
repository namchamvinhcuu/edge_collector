# -*- coding: utf-8 -*-
"""Test task pcm-downlink-command (2026-10-02): làm cứng đường lệnh Odoo ->
edge -> node (POST /api/command điều khiển 1 device cụ thể).

Phạm vi:
  1. inbound_api: mọi /api/* CHẶN CỨNG khi X-Edge-Code thiếu/sai (401).
  2. inbound_api: JSON hỏng / body không phải object / thiếu serial|channel /
     exception từ driver|manager -> {ok:false,status:"error"}, không 500;
     request_id forward xuống queue_command (chỉ str 1..64 ký tự).
  3. manager: chủ lệnh (_node_cmd_owner) - ack từ serial khác bị bỏ; dọn
     _node_futures/_node_cmd_owner mọi nhánh; _node_cmd_seq khởi tạo ngẫu nhiên.
  4. manager: ký HMAC khi node khai sig_cmd; request_id <= 192 byte khi không ký.
  5. node_pull_command bỏ lệnh đã hết hạn chờ ACK.
  6. mqtt_consumer: sig_caps theo status; cmdack truyền serial.
  7. node_api /node/v1/commands/ack truyền serial từ header.

KHÔNG chạm thiết bị/broker thật: driver, store, mqtt client đều là object giả.
"""
import asyncio
import json
import re

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import edge_collector.inbound_api as inbound_api
import edge_collector.manager as manager_mod
import edge_collector.node_api as node_api
from edge_collector.config import settings
from edge_collector.manager import NODE_CMD_MAX_BYTES, NODE_SIGNED_CMD_MAX_BYTES, SourceManager
from edge_collector.mqtt_consumer import MqttConsumer, _canonical, _verify_sig

API_KEY = "node-key-123"  # secret-allow: test fixture, không phải credential thật


# ===========================================================================
# 1-2. inbound_api
# ===========================================================================


class _FakeDriver:
    def __init__(self, result=None, exc=None):
        self.result = result if result is not None else {"ok": True, "status": "ok"}
        self.exc = exc
        self.calls = []

    async def command(self, ch, cmd, value):
        self.calls.append((ch, cmd, value))
        if self.exc:
            raise self.exc
        return self.result

    async def browse(self, node_id=None, path=None):
        return {"ok": True, "items": [], "node_id": node_id}

    async def test(self):
        return {"ok": True, "items": []}


class _FakeInboundManager:
    def __init__(self, driver=None, node_serials=("NODE1",), queue_exc=None):
        self.driver = driver
        self.node_serials = list(node_serials)
        self.queue_exc = queue_exc
        self.queue_calls = []
        self.driver_lookups = []

    def driver_for_channel(self, serial, ch):
        self.driver_lookups.append((serial, ch))
        return self.driver

    def known_node_serials(self):
        return self.node_serials

    async def queue_command(self, serial, ch, cmd, value, extra=None, request_id=None):
        self.queue_calls.append({"serial": serial, "ch": ch, "cmd": cmd,
                                 "value": value, "extra": extra, "request_id": request_id})
        if self.queue_exc:
            raise self.queue_exc
        return {"ok": True, "status": "ok", "error": None}

    def get_driver(self, source_code):
        return self.driver

    def build_probe(self, src_cfg):
        return _FakeDriver()


class _FakeStore:
    def history_latest(self, serial, ch):
        return None

    def history_stats(self, serial, ch, since_ts):
        return {"samples": 0}


def _inbound_client(manager=None, headers=None):
    app = FastAPI()
    app.include_router(inbound_api.router)
    app.state.manager = manager or _FakeInboundManager()
    app.state.store = _FakeStore()
    return TestClient(app, headers=headers or {}), app.state.manager


def _good_headers():
    return {"X-Edge-Code": settings.edge_code}


_ENDPOINTS = [
    ("post", "/api/command", {"serial": "NODE1", "channel": "relay_red", "cmd": "on"}),
    ("get", "/api/latest?serial=NODE1&ch=t1", None),
    ("post", "/api/browse", {"source": "S1"}),
    ("post", "/api/source/test", {"source": {"kind": "sim"}}),
    ("get", "/api/stats?serial=NODE1&ch=t1", None),
]


def _call(client, method, url, body):
    if method == "post":
        return client.post(url, json=body)
    return client.get(url)


@pytest.mark.parametrize("method,url,body", _ENDPOINTS)
@pytest.mark.parametrize("headers", [{}, {"X-Edge-Code": "WRONG-CODE"}, {"X-Edge-Code": ""}],
                         ids=["missing", "wrong", "empty"])
def test_api_endpoints_reject_missing_or_wrong_edge_code_with_401(method, url, body, headers):
    manager = _FakeInboundManager(driver=_FakeDriver())
    client, _ = _inbound_client(manager, headers=headers)
    inbound_api._recent_requests.clear()

    resp = _call(client, method, url, body)

    assert resp.status_code == 401
    data = resp.json()
    assert data["ok"] is False
    assert data["status"] == "rejected"
    assert data["error"]
    # Bị chặn TRƯỚC khi chạm manager/driver/thiết bị - và không log như request hợp lệ.
    assert manager.queue_calls == [] and manager.driver_lookups == []
    assert manager.driver.calls == []
    assert inbound_api.recent_requests() == []


def test_api_command_rejects_edge_code_that_is_prefix_of_real_code():
    """So sánh phải là khớp TOÀN BỘ, không phải startswith/chứa."""
    client, manager = _inbound_client(headers={"X-Edge-Code": settings.edge_code[:-1]})

    resp = client.post("/api/command", json={"serial": "NODE1", "channel": "c"})

    assert resp.status_code == 401
    assert manager.queue_calls == []


@pytest.mark.parametrize("method,url,body", _ENDPOINTS)
def test_api_endpoints_accept_correct_edge_code(method, url, body):
    client, _ = _inbound_client(_FakeInboundManager(driver=_FakeDriver()),
                                headers=_good_headers())

    resp = _call(client, method, url, body)

    assert resp.status_code == 200
    assert resp.json().get("status") != "rejected"


@pytest.mark.parametrize("url", ["/api/command", "/api/browse", "/api/source/test"])
def test_api_post_endpoints_return_error_not_500_on_invalid_json(url):
    client, manager = _inbound_client(headers=_good_headers())

    resp = client.post(url, content=b"{not-json",
                       headers={"Content-Type": "application/json"})

    assert resp.status_code == 200
    assert resp.json()["ok"] is False
    assert resp.json()["status"] == "error"
    assert manager.queue_calls == []


@pytest.mark.parametrize("url", ["/api/command", "/api/browse", "/api/source/test"])
@pytest.mark.parametrize("body", [[1, 2], "chuoi", 42, None], ids=["list", "str", "int", "null"])
def test_api_post_endpoints_return_error_when_body_is_not_object(url, body):
    client, manager = _inbound_client(headers=_good_headers())

    # Gửi bytes JSON thô: httpx coi json=None là "không có body" chứ không phải "null".
    resp = client.post(url, content=json.dumps(body).encode(),
                       headers={"Content-Type": "application/json"})

    assert resp.status_code == 200
    assert resp.json() == {"ok": False, "status": "error", "error": "body phải là JSON object"}
    assert manager.queue_calls == []


@pytest.mark.parametrize("body", [
    {"channel": "relay_red", "cmd": "on"},
    {"serial": "NODE1", "cmd": "on"},
    {"serial": "", "channel": "relay_red"},
    {"serial": "NODE1", "channel": ""},
], ids=["no-serial", "no-channel", "empty-serial", "empty-channel"])
def test_api_command_missing_serial_or_channel_is_error(body):
    manager = _FakeInboundManager(driver=_FakeDriver())
    client, _ = _inbound_client(manager, headers=_good_headers())

    resp = client.post("/api/command", json=body)

    assert resp.status_code == 200
    assert resp.json()["ok"] is False and resp.json()["status"] == "error"
    assert manager.driver_lookups == [] and manager.queue_calls == []


def test_api_command_driver_path_returns_driver_result():
    driver = _FakeDriver(result={"ok": True, "status": "ok", "v": 1})
    client, manager = _inbound_client(_FakeInboundManager(driver=driver), headers=_good_headers())

    resp = client.post("/api/command", json={"serial": "PLC1", "channel": "c1",
                                             "cmd": "write", "value": 7})

    assert resp.json() == {"ok": True, "status": "ok", "v": 1}
    assert driver.calls == [("c1", "write", 7)]
    assert manager.queue_calls == []


def test_api_command_driver_exception_returns_error_not_500():
    driver = _FakeDriver(exc=RuntimeError("modbus timeout"))
    client, _ = _inbound_client(_FakeInboundManager(driver=driver), headers=_good_headers())

    resp = client.post("/api/command", json={"serial": "PLC1", "channel": "c1", "cmd": "zero"})

    assert resp.status_code == 200
    assert resp.json() == {"ok": False, "status": "error", "error": "modbus timeout"}


def test_api_command_queue_command_exception_returns_error_not_500():
    manager = _FakeInboundManager(queue_exc=ValueError("hỏng"))
    client, _ = _inbound_client(manager, headers=_good_headers())

    resp = client.post("/api/command", json={"serial": "NODE1", "channel": "relay_red"})

    assert resp.status_code == 200
    assert resp.json() == {"ok": False, "status": "error", "error": "hỏng"}
    assert len(manager.queue_calls) == 1


def test_api_command_unknown_serial_returns_error_status():
    manager = _FakeInboundManager(node_serials=())
    client, _ = _inbound_client(manager, headers=_good_headers())

    resp = client.post("/api/command", json={"serial": "GHOST", "channel": "c"})

    assert resp.json()["ok"] is False and resp.json()["status"] == "error"
    assert manager.queue_calls == []


def test_api_command_default_cmd_is_read():
    manager = _FakeInboundManager()
    client, _ = _inbound_client(manager, headers=_good_headers())

    client.post("/api/command", json={"serial": "NODE1", "channel": "relay_red"})

    assert manager.queue_calls[0]["cmd"] == "read"


@pytest.mark.parametrize("rid,expected", [
    ("a" * 32, "a" * 32),
    ("x", "x"),
    ("b" * 64, "b" * 64),
    ("c" * 65, None),
    ("", None),
    (12345, None),
    (None, None),
    (["r"], None),
], ids=["uuid-hex", "len1", "len64", "len65", "empty", "int", "null", "list"])
def test_api_command_forwards_request_id_only_when_short_string(rid, expected):
    manager = _FakeInboundManager()
    client, _ = _inbound_client(manager, headers=_good_headers())

    client.post("/api/command", json={"serial": "NODE1", "channel": "relay_red",
                                      "cmd": "on", "request_id": rid})

    assert manager.queue_calls[0]["request_id"] == expected


def test_api_command_without_request_id_key_forwards_none():
    manager = _FakeInboundManager()
    client, _ = _inbound_client(manager, headers=_good_headers())

    client.post("/api/command", json={"serial": "NODE1", "channel": "relay_red"})

    assert manager.queue_calls[0]["request_id"] is None


# ===========================================================================
# 3-5. manager.queue_command / node_ack_command / node_pull_command
# ===========================================================================


class _FakeMqtt:
    def __init__(self, publish_result=True, caps=None, sig_caps=None):
        self.caps = caps if caps is not None else {"NODE1": True}
        if sig_caps is not None:
            self.sig_caps = sig_caps
        self._publish_result = publish_result
        self.published = []

    def publish_command(self, serial, payload):
        self.published.append((serial, dict(payload)))
        return self._publish_result


def _mgr(api_key=None, serial="NODE1"):
    m = SourceManager(on_value=lambda *a: None)
    if api_key:
        m.devices_by_serial = {serial: {"serial": serial, "api_key": api_key}}
    return m


async def _queue_then(manager, after, serial="NODE1", timeout=1.0, **kw):
    """Chạy queue_command như task, cho nó tới điểm await rồi gọi after(cmd_id)."""
    task = asyncio.ensure_future(manager.queue_command(
        serial, kw.pop("ch", "relay_red"), kw.pop("cmd", "on"), kw.pop("value", 1),
        timeout=timeout, **kw))
    await asyncio.sleep(0)
    side = after(manager._node_cmd_seq)
    return await task, side


def _assert_clean(manager):
    assert manager._node_futures == {}
    assert manager._node_cmd_owner == {}


def test_node_cmd_seq_starts_random_in_range_and_increments_by_one():
    m = _mgr()
    start = m._node_cmd_seq
    assert 1 <= start <= 2 ** 30
    m.mqtt_cmd = _FakeMqtt()

    asyncio.run(_queue_then(m, lambda cid: m.node_ack_command(cid, True, serial="NODE1")))

    assert m._node_cmd_seq == start + 1
    assert m.mqtt_cmd.published[0][1]["id"] == start + 1


def test_node_cmd_seq_uses_randint_1_to_2pow30(monkeypatch):
    calls = []
    monkeypatch.setattr(manager_mod.random, "randint",
                        lambda a, b: calls.append((a, b)) or 777)

    m = _mgr()

    assert calls == [(1, 2 ** 30)]
    assert m._node_cmd_seq == 777


def test_ack_from_owner_serial_resolves_and_cleans_up():
    m = _mgr()
    m.mqtt_cmd = _FakeMqtt()

    result, acked = asyncio.run(_queue_then(
        m, lambda cid: m.node_ack_command(cid, True, serial="NODE1")))

    assert acked is True
    assert result == {"ok": True, "status": "ok", "error": None}
    _assert_clean(m)


def test_ack_from_other_serial_is_rejected_and_future_stays_pending():
    """Node B không được ack (giả mạo kết quả) lệnh gửi cho node A."""
    m = _mgr()
    m.mqtt_cmd = _FakeMqtt()
    seen = {}

    def _after(cid):
        seen["wrong"] = m.node_ack_command(cid, True, serial="NODE2")
        seen["owner_kept"] = m._node_cmd_owner.get(cid)
        seen["pending"] = not m._node_futures[cid].done()
        return None

    result, _ = asyncio.run(_queue_then(m, _after, timeout=0.05))

    assert seen == {"wrong": False, "owner_kept": "NODE1", "pending": True}
    # Không ai ack đúng -> timeout thật, KHÔNG phải ok giả mạo.
    assert result["ok"] is False and result["status"] == "unknown"
    _assert_clean(m)


def test_ack_wrong_serial_then_owner_still_resolves():
    m = _mgr()
    m.mqtt_cmd = _FakeMqtt()

    def _after(cid):
        return (m.node_ack_command(cid, True, serial="NODE2"),
                m.node_ack_command(cid, False, "kẹt", serial="NODE1"))

    result, acks = asyncio.run(_queue_then(m, _after))

    assert acks == (False, True)
    assert result == {"ok": False, "status": "error", "error": "kẹt"}


def test_node_ack_command_requires_serial_keyword():
    m = _mgr()
    with pytest.raises(TypeError):
        m.node_ack_command(1, True, "")          # thiếu serial
    with pytest.raises(TypeError):
        m.node_ack_command(1, True, "", "NODE1")  # serial positional bị cấm


def test_ack_unknown_or_already_done_cmd_returns_false():
    m = _mgr()
    m.mqtt_cmd = _FakeMqtt()
    assert m.node_ack_command(999, True, serial="NODE1") is False

    def _after(cid):
        first = m.node_ack_command(cid, True, serial="NODE1")
        second = m.node_ack_command(cid, False, "trễ", serial="NODE1")
        return first, second

    result, acks = asyncio.run(_queue_then(m, _after))

    assert acks == (True, False)
    assert result["ok"] is True


def test_broker_down_branch_cleans_owner_and_future():
    m = _mgr()
    m.mqtt_cmd = _FakeMqtt(publish_result=False, caps={"NODE1": True})

    result = asyncio.run(m.queue_command("NODE1", "relay_red", "on", 1, timeout=1.0))

    assert result["ok"] is False and result["status"] == "error"
    _assert_clean(m)
    assert m._node_queues.get("NODE1") is None


def test_timeout_branch_cleans_owner_and_future():
    m = _mgr()
    m.mqtt_cmd = _FakeMqtt()

    result = asyncio.run(m.queue_command("NODE1", "relay_red", "on", 1, timeout=0.05))

    assert result["status"] == "unknown"
    _assert_clean(m)


def test_unexpected_exception_in_dispatch_still_cleans_up():
    class _Boom(_FakeMqtt):
        def publish_command(self, serial, payload):
            raise RuntimeError("paho nổ")

    m = _mgr()
    m.mqtt_cmd = _Boom()

    with pytest.raises(RuntimeError):
        asyncio.run(m.queue_command("NODE1", "relay_red", "on", 1, timeout=1.0))
    _assert_clean(m)


# --- 4. ký HMAC ------------------------------------------------------------


def test_signed_command_when_node_declares_sig_cmd(monkeypatch):
    monkeypatch.setattr(manager_mod.time, "time", lambda: 1_700_000_000.9)
    m = _mgr(api_key=API_KEY)
    m.mqtt_cmd = _FakeMqtt(sig_caps={"NODE1": True})

    asyncio.run(_queue_then(m, lambda cid: m.node_ack_command(cid, True, serial="NODE1"),
                            cmd="blink", value=None, extra={"ms": 30000},
                            request_id="abc123"))

    _, payload = m.mqtt_cmd.published[0]
    assert payload["request_id"] == "abc123"
    assert payload["ts"] == 1_700_000_000 and isinstance(payload["ts"], int)
    assert payload["ms"] == 30000
    assert isinstance(payload["sig"], str) and len(payload["sig"]) == 64
    assert _verify_sig(API_KEY, payload) is True
    # Bị sửa sau khi ký, hoặc ký bằng khóa khác -> node phải từ chối được.
    assert _verify_sig(API_KEY, dict(payload, cmd="off")) is False
    assert _verify_sig("other-key", payload) is False


def test_signed_command_without_request_id_has_no_request_id_key():
    m = _mgr(api_key=API_KEY)
    m.mqtt_cmd = _FakeMqtt(sig_caps={"NODE1": True})

    asyncio.run(_queue_then(m, lambda cid: m.node_ack_command(cid, True, serial="NODE1")))

    _, payload = m.mqtt_cmd.published[0]
    assert "request_id" not in payload
    assert set(payload) == {"id", "channel", "cmd", "value", "ts", "sig"}
    assert _verify_sig(API_KEY, payload)


_FIXED_TS = 1_700_000_000


def _signed_value_for_len(target, rid):
    """Chuỗi value để len(_canonical(gói đã ký, có rid nếu rid)) == target.
    Cố định id=1000 (randint->999) và ts (time.time) để độ dài tất định."""
    base = {"id": 1000, "channel": "c", "cmd": "on", "value": ""}
    measured = len(_canonical(SourceManager._sign_node_command(API_KEY, base, rid)))
    pad = target - measured
    assert pad >= 0
    return "x" * pad


@pytest.fixture()
def _fixed_id_ts(monkeypatch):
    monkeypatch.setattr(manager_mod.random, "randint", lambda a, b: 999)   # -> id 1000
    monkeypatch.setattr(manager_mod.time, "time", lambda: _FIXED_TS + 0.5)


def _run_signed(value, rid):
    m = _mgr(api_key=API_KEY)
    m.mqtt_cmd = _FakeMqtt(sig_caps={"NODE1": True})
    result, _ = asyncio.run(_queue_then(
        m, lambda cid: m.node_ack_command(cid, True, serial="NODE1"),
        ch="c", cmd="on", value=value, request_id=rid))
    return m, result


@pytest.mark.parametrize("target", [193, 250, 319], ids=["193", "250", "319"])
def test_signed_command_keeps_request_id_between_192_and_319_bytes(_fixed_id_ts, target):
    """Gói ký dùng giới hạn riêng 320 byte (firmware mqtt_link.c bỏ khi >= 320),
    KHÔNG phải 192 của gói không ký."""
    rid = "r" * 32
    m, result = _run_signed(_signed_value_for_len(target, rid), rid)

    _, payload = m.mqtt_cmd.published[0]
    assert len(_canonical(payload)) == target
    assert payload["request_id"] == rid
    assert _verify_sig(API_KEY, payload)
    assert result["ok"] is True


def test_signed_command_at_320_bytes_drops_request_id_and_resigns(_fixed_id_ts):
    rid = "r" * 32
    value = _signed_value_for_len(NODE_SIGNED_CMD_MAX_BYTES, rid)
    m, result = _run_signed(value, rid)

    _, payload = m.mqtt_cmd.published[0]
    assert "request_id" not in payload
    assert payload["value"] == value                  # lệnh vẫn được gửi
    assert len(_canonical(payload)) < NODE_SIGNED_CMD_MAX_BYTES
    # Ký LẠI trên gói không có request_id -> node verify được.
    assert _verify_sig(API_KEY, payload)
    assert result["ok"] is True
    _assert_clean(m)


def test_signed_command_still_too_long_without_request_id_is_error(_fixed_id_ts):
    rid = "r" * 32
    # Ngay cả khi bỏ request_id gói ký vẫn dài đúng 320 byte.
    value = _signed_value_for_len(NODE_SIGNED_CMD_MAX_BYTES, None)
    m = _mgr(api_key=API_KEY)
    m.mqtt_cmd = _FakeMqtt(sig_caps={"NODE1": True})

    result = asyncio.run(m.queue_command("NODE1", "c", "on", value,
                                         timeout=1.0, request_id=rid))

    assert result["ok"] is False and result["status"] == "error"
    assert m.mqtt_cmd.published == []
    assert m._node_queues.get("NODE1") is None
    _assert_clean(m)


@pytest.mark.parametrize("target,ok", [(319, True), (320, False)], ids=["319", "320"])
def test_signed_command_without_request_id_boundary_319_320(_fixed_id_ts, target, ok):
    value = _signed_value_for_len(target, None)
    m = _mgr(api_key=API_KEY)
    m.mqtt_cmd = _FakeMqtt(sig_caps={"NODE1": True})

    if ok:
        result, _ = asyncio.run(_queue_then(
            m, lambda cid: m.node_ack_command(cid, True, serial="NODE1"),
            ch="c", cmd="on", value=value))
        _, payload = m.mqtt_cmd.published[0]
        assert len(_canonical(payload)) == 319 and _verify_sig(API_KEY, payload)
    else:
        result = asyncio.run(m.queue_command("NODE1", "c", "on", value, timeout=1.0))
        assert m.mqtt_cmd.published == []
    assert result["ok"] is ok
    _assert_clean(m)


def test_sig_cmd_without_cached_api_key_returns_error_and_sends_nothing():
    m = _mgr(api_key=None)
    m.mqtt_cmd = _FakeMqtt(sig_caps={"NODE1": True})

    result = asyncio.run(m.queue_command("NODE1", "relay_red", "on", 1, timeout=1.0))

    assert result["ok"] is False and result["status"] == "error"
    assert "NODE1" in result["error"]
    assert m.mqtt_cmd.published == []
    assert m._node_queues.get("NODE1") is None
    _assert_clean(m)


def test_sig_caps_false_for_this_serial_sends_unsigned():
    m = _mgr(api_key=API_KEY)
    m.mqtt_cmd = _FakeMqtt(sig_caps={"NODE1": False, "NODE2": True})

    asyncio.run(_queue_then(m, lambda cid: m.node_ack_command(cid, True, serial="NODE1")))

    _, payload = m.mqtt_cmd.published[0]
    assert "sig" not in payload and "ts" not in payload


def test_no_sig_caps_attribute_sends_unsigned_with_small_request_id():
    m = _mgr(api_key=API_KEY)
    m.mqtt_cmd = _FakeMqtt()      # không có thuộc tính sig_caps (consumer cũ)

    asyncio.run(_queue_then(m, lambda cid: m.node_ack_command(cid, True, serial="NODE1"),
                            request_id="abc"))

    _, payload = m.mqtt_cmd.published[0]
    assert payload["request_id"] == "abc"
    assert "sig" not in payload and "ts" not in payload


def _value_for_len(target, rid):
    """Chọn chuỗi value để json.dumps(payload có request_id) dài đúng target byte."""
    seq = 1000
    base = {"id": seq, "channel": "c", "cmd": "on", "value": "", "request_id": rid}
    pad = target - len(json.dumps(base))
    assert pad >= 0
    return seq, "x" * pad


@pytest.mark.parametrize("delta,kept", [(-1, True), (0, False), (1, False)],
                         ids=["191", "eq192", "193"])
def test_unsigned_request_id_kept_below_192_bytes_dropped_at_or_beyond(monkeypatch, delta, kept):
    """Firmware bỏ gói khi data_len >= 192 -> chỉ gắn request_id khi < 192."""
    monkeypatch.setattr(manager_mod.random, "randint", lambda a, b: 999)  # -> id 1000
    rid = "r" * 32
    seq, value = _value_for_len(NODE_CMD_MAX_BYTES + delta, rid)
    m = _mgr()
    m.mqtt_cmd = _FakeMqtt()

    asyncio.run(_queue_then(m, lambda cid: m.node_ack_command(cid, True, serial="NODE1"),
                            ch="c", cmd="on", value=value, request_id=rid))

    _, payload = m.mqtt_cmd.published[0]
    assert payload["id"] == seq
    assert ("request_id" in payload) is kept
    if kept:
        assert len(json.dumps(payload)) == NODE_CMD_MAX_BYTES - 1
    else:
        # Bỏ request_id nhưng lệnh VẪN được gửi (không chặn lệnh).
        assert payload["value"] == value


def test_unsigned_request_id_also_applies_to_poll_queue():
    m = _mgr()     # không mqtt_cmd -> hàng đợi poll

    def _after(cid):
        p = m.node_pull_command("NODE1")
        m.node_ack_command(cid, True, serial="NODE1")
        return p

    result, payload = asyncio.run(_queue_then(m, _after, request_id="rid-1"))

    assert payload["request_id"] == "rid-1"
    assert "sig" not in payload
    assert result["ok"] is True


# --- 5. node_pull_command bỏ lệnh hết hạn -----------------------------------


def test_poll_after_timeout_does_not_return_expired_command():
    m = _mgr()

    async def _run():
        result = await m.queue_command("NODE1", "relay_red", "on", 1, timeout=0.05)
        return result, m.node_pull_command("NODE1")

    result, pulled = asyncio.run(_run())

    assert result["status"] == "unknown"
    assert pulled is None
    # Đã rút khỏi hàng đợi, không nằm lại chiếm chỗ.
    assert m._node_queues["NODE1"].empty()


def test_poll_skips_expired_and_returns_next_live_command():
    m = _mgr()

    async def _run():
        expired = await m.queue_command("NODE1", "relay_red", "on", 1, timeout=0.01)
        expired_id = m._node_cmd_seq
        task = asyncio.ensure_future(m.queue_command("NODE1", "relay_red", "off", 0, timeout=1.0))
        await asyncio.sleep(0)
        live_id = m._node_cmd_seq
        pulled = m.node_pull_command("NODE1")
        m.node_ack_command(live_id, True, serial="NODE1")
        return expired, expired_id, live_id, pulled, await task

    expired, expired_id, live_id, pulled, live = asyncio.run(_run())

    assert expired["status"] == "unknown"
    assert pulled["id"] == live_id != expired_id
    assert pulled["cmd"] == "off"
    assert live["ok"] is True
    assert m.node_pull_command("NODE1") is None


def test_poll_live_command_returned_once():
    m = _mgr()

    def _after(cid):
        first = m.node_pull_command("NODE1")
        second = m.node_pull_command("NODE1")
        m.node_ack_command(cid, True, serial="NODE1")
        return first, second

    _, (first, second) = asyncio.run(_queue_then(m, _after))

    assert first is not None and second is None


def test_poll_unknown_serial_returns_none():
    assert _mgr().node_pull_command("NOBODY") is None


# ===========================================================================
# 6. mqtt_consumer sig_caps + tích hợp ký thật qua MqttConsumer
# ===========================================================================


class _AgentWithRealManager:
    def __init__(self, manager):
        self.manager = manager

    def push_node_reading(self, *a):
        pass


def _status(consumer, serial="NODE1", **data):
    consumer._handle("fms/%s/status" % serial, json.dumps(data).encode())


def _consumer():
    return MqttConsumer(_AgentWithRealManager(_mgr()))


def test_status_with_sig_cmd_sets_sig_caps_true():
    c = _consumer()
    _status(c, online=True, cmd=True, sig_cmd=True)
    assert c.sig_caps["NODE1"] is True


def test_status_cmd_without_sig_cmd_sets_sig_caps_false():
    c = _consumer()
    _status(c, online=True, cmd=True)
    assert c.sig_caps["NODE1"] is False


def test_firmware_downgrade_resets_sig_caps_to_false():
    c = _consumer()
    _status(c, online=True, cmd=True, sig_cmd=True)
    _status(c, online=True, cmd=True, sig_cmd=False)
    assert c.sig_caps["NODE1"] is False
    _status(c, online=True, cmd=True, sig_cmd=1)
    assert c.sig_caps["NODE1"] is True      # bool() hoá giá trị truthy
    _status(c, online=True, cmd=True)       # bản không biết sig_cmd
    assert c.sig_caps["NODE1"] is False


def test_lwt_offline_does_not_clear_sig_caps():
    c = _consumer()
    _status(c, online=True, cmd=True, sig_cmd=True)
    _status(c, online=False)
    assert c.sig_caps["NODE1"] is True
    assert c.caps["NODE1"] is True


def test_status_without_cmd_flag_does_not_set_sig_caps():
    c = _consumer()
    _status(c, online=True, sig_cmd=True)
    assert "NODE1" not in c.sig_caps


def test_cmdack_from_other_serial_topic_does_not_resolve_real_manager_command():
    """Tích hợp MqttConsumer + SourceManager thật: cmdack trên topic của NODE2
    với id lệnh gửi cho NODE1 không được hoàn tất lệnh."""
    m = _mgr()
    c = MqttConsumer(_AgentWithRealManager(m))
    m.mqtt_cmd = _FakeMqtt()

    def _after(cid):
        c._handle("fms/NODE2/cmdack", json.dumps({"id": cid, "ok": True}).encode())
        pending_after_spoof = not m._node_futures[cid].done()
        c._handle("fms/NODE1/cmdack", json.dumps({"id": cid, "ok": True}).encode())
        return pending_after_spoof

    result, pending_after_spoof = asyncio.run(_queue_then(m, _after))

    assert pending_after_spoof is True
    assert result["ok"] is True


class _PubInfo:
    rc = 0   # mqtt.MQTT_ERR_SUCCESS


class _FakePahoClient:
    def __init__(self):
        self.sent = []

    def publish(self, topic, data, qos=0):
        self.sent.append((topic, data, qos))
        return _PubInfo()


def test_end_to_end_signed_command_via_real_consumer_is_verifiable():
    """node khai sig_cmd qua status -> manager ký -> MqttConsumer.publish_command
    gửi lên (client giả) -> gói thật trên dây verify được bằng api_key."""
    m = _mgr(api_key=API_KEY)
    c = MqttConsumer(_AgentWithRealManager(m))
    c._cli = _FakePahoClient()
    c._connected = True
    _status(c, online=True, cmd=True, sig_cmd=True)
    m.mqtt_cmd = c

    result, _ = asyncio.run(_queue_then(
        m, lambda cid: c._handle("fms/NODE1/cmdack",
                                 json.dumps({"id": cid, "ok": True}).encode()),
        request_id="rid-e2e"))

    assert result["ok"] is True
    assert len(c._cli.sent) == 1
    topic, raw, qos = c._cli.sent[0]
    assert topic.endswith("/NODE1/cmd") and qos == 1
    wire = json.loads(raw)
    assert wire["request_id"] == "rid-e2e"
    assert _verify_sig(API_KEY, wire)


def _online_consumer():
    c = MqttConsumer(_AgentWithRealManager(_mgr()))
    c._cli = _FakePahoClient()
    c._connected = True
    _status(c, online=True, cmd=True, sig_cmd=True)
    return c


_SIG_RE = r'"sig":"[0-9a-f]{64}",'


@pytest.mark.parametrize("extra", [
    {},
    {"request_id": "abc123", "ms": 30000, "period_ms": 500},
    {"value": "Nhiệtđộ"},                       # unicode -> \u escape cả 2 phía
], ids=["minimal", "with-extras", "unicode-value"])
def test_publish_signed_command_sends_canonical_bytes_and_sig_is_strippable(extra):
    """ESP32 cắt chuỗi con `"sig":"<64hex>",` khỏi raw để ra đúng chuỗi băm."""
    from edge_collector.mqtt_consumer import _sign
    c = _online_consumer()
    payload = dict({"id": 5, "channel": "relay_red", "cmd": "on", "value": 1,
                    "ts": 1_700_000_000}, **extra)
    payload["sig"] = _sign(API_KEY, payload)

    assert c.publish_command("NODE1", payload) is True

    raw = c._cli.sent[0][1]
    assert raw == _canonical(payload).decode()
    assert " " not in raw                          # không khoảng trắng phân cách
    unsigned = {k: v for k, v in payload.items() if k != "sig"}
    stripped = re.sub(_SIG_RE, "", raw)
    assert stripped == _canonical(unsigned).decode()
    assert len(re.findall(_SIG_RE, raw)) == 1
    assert _verify_sig(API_KEY, json.loads(raw))


def test_publish_unsigned_command_keeps_default_json_dumps():
    c = _online_consumer()
    payload = {"id": 5, "channel": "relay_red", "cmd": "on", "value": 1, "request_id": "r"}

    assert c.publish_command("NODE1", payload) is True

    raw = c._cli.sent[0][1]
    assert raw == json.dumps(payload)              # thứ tự chèn + ", " / ": " như cũ
    assert raw != _canonical(payload).decode()


def test_end_to_end_signed_wire_bytes_strip_to_hashed_string():
    """Tích hợp manager ký -> consumer publish: raw trên dây cắt sig ra đúng
    chuỗi đã băm bằng api_key."""
    import hashlib
    import hmac as _hmac
    m = _mgr(api_key=API_KEY)
    c = MqttConsumer(_AgentWithRealManager(m))
    c._cli = _FakePahoClient()
    c._connected = True
    _status(c, online=True, cmd=True, sig_cmd=True)
    m.mqtt_cmd = c

    asyncio.run(_queue_then(
        m, lambda cid: c._handle("fms/NODE1/cmdack",
                                 json.dumps({"id": cid, "ok": True}).encode()),
        extra={"ms": 1000}, request_id="rid-x"))

    raw = c._cli.sent[0][1]
    sig = json.loads(raw)["sig"]
    hashed = re.sub(_SIG_RE, "", raw).encode()
    assert _hmac.new(API_KEY.encode(), hashed, hashlib.sha256).hexdigest() == sig


def test_end_to_end_downgraded_node_gets_unsigned_command():
    m = _mgr(api_key=API_KEY)
    c = MqttConsumer(_AgentWithRealManager(m))
    c._cli = _FakePahoClient()
    c._connected = True
    _status(c, online=True, cmd=True, sig_cmd=True)
    _status(c, online=True, cmd=True)          # hạ firmware
    m.mqtt_cmd = c

    asyncio.run(_queue_then(
        m, lambda cid: c._handle("fms/NODE1/cmdack",
                                 json.dumps({"id": cid, "ok": True}).encode())))

    wire = json.loads(c._cli.sent[0][1])
    assert "sig" not in wire and "ts" not in wire


# ===========================================================================
# 7. node_api /node/v1/commands/ack truyền serial
# ===========================================================================


class _FakeNodeManager:
    def __init__(self, api_key=None):
        self._api_key = api_key
        self.acks = []

    def cached_node_api_key(self, serial):
        return self._api_key

    def touch_node(self, serial):
        pass

    def node_ack_command(self, cmd_id, ok, detail="", *, serial):
        self.acks.append((cmd_id, ok, detail, serial))
        return True


def _node_client(manager):
    app = FastAPI()
    app.include_router(node_api.router)
    app.state.manager = manager
    return TestClient(app)


def test_node_api_ack_forwards_serial_from_header():
    manager = _FakeNodeManager(api_key=API_KEY)
    client = _node_client(manager)

    resp = client.post("/node/v1/commands/ack", json={"id": 7, "ok": True, "detail": "x"},
                       headers={"X-Device-Serial": "NODE9", "X-API-Key": API_KEY})

    assert resp.json() == {"ok": True}
    assert manager.acks == [(7, True, "x", "NODE9")]


def test_node_api_ack_wrong_api_key_never_reaches_manager():
    manager = _FakeNodeManager(api_key=API_KEY)
    client = _node_client(manager)

    resp = client.post("/node/v1/commands/ack", json={"id": 7, "ok": True},
                       headers={"X-Device-Serial": "NODE9", "X-API-Key": "sai"})

    assert resp.json()["ok"] is False
    assert manager.acks == []


def test_node_api_ack_with_real_manager_rejects_other_serial():
    """HTTP ack từ node khác (header serial khác chủ lệnh) -> {"ok": false}."""
    m = _mgr()
    client = _node_client(m)
    cid = 4242
    loop = asyncio.new_event_loop()
    try:
        m._node_futures[cid] = loop.create_future()
        m._node_cmd_owner[cid] = "NODE1"

        resp = client.post("/node/v1/commands/ack", json={"id": cid, "ok": True},
                           headers={"X-Device-Serial": "NODE2"})

        assert resp.json() == {"ok": False}
        assert not m._node_futures[cid].done()
    finally:
        loop.close()
