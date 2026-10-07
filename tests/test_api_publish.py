# -*- coding: utf-8 -*-
"""Test task pcm-edge-publish (2026-10-02): POST /api/publish cho node
`mqtt_publish` của ppd_process (Odoo) publish MQTT tùy ý qua edge.

Mục đích an toàn: endpoint này KHÔNG được thành đường đi vòng điều khiển
thiết bị. Vòng 2 (Nam chốt ALLOWLIST): topic phải nằm dưới
EDGE_PUBLISH_TOPIC_ALLOW (rỗng = tắt) VÀ KHÔNG nằm dưới gốc reserved ("fms"
cứng + gốc consumer + gốc của mọi nguồn MQTT). So khớp theo LEVEL (_under).

Phạm vi:
  1. Ký HMAC bắt buộc (router dependency) - không ký -> 401.
  2. Validate topic (thiếu/rỗng/không phải str, >256, "/", "+", "#", "\\x00").
  3. Reserved topic (gốc node + cmd prefix của MqttDriver THẬT qua SourceManager THẬT).
  4. Chuẩn hoá payload (str / None / số / bool / dict / list) + giới hạn 4096 BYTE.
  5. manager.mqtt_cmd None -> lỗi.
  6. MqttConsumer.publish_raw: chưa kết nối / NO_CONN (paho Client THẬT chưa
     connect - mid phải bị rút khỏi _out_messages) / rc lỗi / exception /
     PUBACK trong hạn / quá hạn -> "unknown"; qos=1; log không chứa payload.
  7. MqttDriver.command vẫn publish "<base>/cmd/<ch>" sau refactor.

KHÔNG chạm broker/thiết bị thật: paho Client thật nhưng KHÔNG connect.
"""
import asyncio
import logging
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import edge_collector.inbound_api as inbound_api
import edge_collector.mqtt_consumer as mqtt_consumer
from edge_collector.config import settings
from edge_collector.drivers.mqtt import MqttDriver
from edge_collector.manager import SourceManager
from edge_collector.mqtt_consumer import MqttConsumer

from downlink_signing import DownlinkSigner, KeyStore

SECRET_PAYLOAD = "marker-payload-khong-duoc-vao-log"  # secret-allow: chuỗi đánh dấu giả để soi log, không phải credential


# --- fake cho endpoint --------------------------------------------------


class _FakeMqttCmd:
    def __init__(self, result=None):
        self.calls = []
        self.result = result or {"ok": True, "status": "ok"}

    async def publish_raw(self, topic, payload):
        self.calls.append((topic, payload))
        return self.result


class _Manager:
    def __init__(self, topic_bases=(), mqtt_cmd="default"):
        self._bases = list(topic_bases)
        self.mqtt_cmd = _FakeMqttCmd() if mqtt_cmd == "default" else mqtt_cmd

    def mqtt_topic_bases(self):
        return list(self._bases)


def _client(manager=None, signed=True):
    app = FastAPI()
    app.include_router(inbound_api.router)
    app.state.manager = manager if manager is not None else _Manager()
    app.state.store = KeyStore()
    client = TestClient(app, headers={"X-Edge-Code": settings.edge_code})
    if signed:
        client.auth = DownlinkSigner()
    return client, app.state.manager


DEFAULT_ALLOW = "plant,a,fmsx"


@pytest.fixture(autouse=True)
def _default_consumer_topic(monkeypatch):
    monkeypatch.setattr(settings, "mqtt_consumer_topic", "fms/+/meas")
    monkeypatch.setattr(settings, "publish_topic_allow", DEFAULT_ALLOW)
    monkeypatch.setattr(settings, "mqtt_consumer_enabled", True)
    inbound_api._recent_requests.clear()
    yield
    inbound_api._recent_requests.clear()


# --- 1. HMAC ------------------------------------------------------------


def test_publish_without_signature_is_401_and_not_published():
    client, manager = _client(signed=False)

    resp = client.post("/api/publish", json={"topic": "plant/line1/out", "payload": "x"})

    assert resp.status_code == 401
    assert manager.mqtt_cmd.calls == []


def test_publish_signed_happy_path_forwards_topic_and_payload():
    client, manager = _client()

    resp = client.post("/api/publish", json={"topic": "plant/line1/out", "payload": "hello"})

    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "status": "ok"}
    assert manager.mqtt_cmd.calls == [("plant/line1/out", "hello")]


def test_publish_returns_publish_raw_result_verbatim():
    result = {"ok": False, "status": "unknown", "reason": "no_puback", "error": "chưa nhận PUBACK"}
    client, _ = _client(_Manager(mqtt_cmd=_FakeMqttCmd(result=dict(result))))

    resp = client.post("/api/publish", json={"topic": "a/b", "payload": "x"})

    assert resp.json() == result


# --- 2. validate topic --------------------------------------------------


@pytest.mark.parametrize("body", [
    {"payload": "x"},                 # thiếu
    {"topic": "", "payload": "x"},    # rỗng
    {"topic": None, "payload": "x"},
    {"topic": 123, "payload": "x"},   # không phải str
    {"topic": ["a/b"], "payload": "x"},
])
def test_publish_missing_or_non_str_topic_is_error(body):
    client, manager = _client()

    resp = client.post("/api/publish", json=body)

    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is False and data["status"] == "error"
    assert data["error"] == "thiếu topic"
    assert data["reason"] == "invalid_topic"
    assert manager.mqtt_cmd.calls == []


@pytest.mark.parametrize("topic", [
    "a/" + "x" * 255,                 # 257 ký tự
    "$CONTROL/x",
    "$SYS/broker",
    "/plant/out",
    "plant/+/out",
    "plant/#",
    "plant/\x00/out",
])
def test_publish_invalid_topic_is_rejected(topic):
    client, manager = _client()

    resp = client.post("/api/publish", json={"topic": topic, "payload": "x"})

    data = resp.json()
    assert data == {"ok": False, "status": "error", "reason": "invalid_topic",
                    "error": "topic không hợp lệ"}
    assert manager.mqtt_cmd.calls == []


@pytest.mark.parametrize("topic_json", ['"a/\\ud800"', '"\\udfffa/b"'])
def test_publish_lone_surrogate_topic_is_invalid_topic_not_500(topic_json):
    client, manager = _client()
    body = ('{"topic": %s, "payload": "x"}' % topic_json).encode("ascii")

    resp = client.post("/api/publish", content=body,
                       headers={"Content-Type": "application/json"})

    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is False and data["reason"] == "invalid_topic"
    assert manager.mqtt_cmd.calls == []


def test_publish_topic_exactly_256_chars_is_accepted():
    client, manager = _client()
    topic = "a/" + "x" * 254
    assert len(topic) == 256

    resp = client.post("/api/publish", json={"topic": topic, "payload": "x"})

    assert resp.json()["ok"] is True
    assert manager.mqtt_cmd.calls == [(topic, "x")]


# --- 3. reserved topic --------------------------------------------------


@pytest.mark.parametrize("topic", ["fms/NODE1/cmd", "fms/NODE1/meas", "fms/anything"])
def test_publish_to_node_protocol_root_is_reserved(topic, monkeypatch):
    monkeypatch.setattr(settings, "publish_topic_allow", "fms,plant")   # reserved thắng allow
    client, manager = _client()

    resp = client.post("/api/publish", json={"topic": topic, "payload": "on"})

    data = resp.json()
    assert data["ok"] is False and data["status"] == "error"
    assert "không được phép" in data["error"]
    assert manager.mqtt_cmd.calls == []
    assert inbound_api.recent_requests()[0]["rejected"] == "not allowed"


def test_reserved_root_follows_configured_consumer_topic(monkeypatch):
    monkeypatch.setattr(settings, "mqtt_consumer_topic", "site9/+/meas")
    monkeypatch.setattr(settings, "publish_topic_allow", "site9,fms,site99")
    client, manager = _client()

    blocked = client.post("/api/publish", json={"topic": "site9/N1/cmd", "payload": "x"}).json()
    fms = client.post("/api/publish", json={"topic": "fms/N1/cmd", "payload": "x"}).json()
    other = client.post("/api/publish", json={"topic": "site99/N1", "payload": "x"}).json()

    assert blocked["ok"] is False and "không được phép" in blocked["error"]
    assert blocked["reason"] == fms["reason"] == "topic_not_allowed"
    assert fms["ok"] is False, "fms là gốc CỨNG - vẫn chặn khi consumer đổi gốc"
    assert other["ok"] is True, "so theo level: site9 không phủ site99"
    assert manager.mqtt_cmd.calls == [("site99/N1", "x")]


def test_topic_only_sharing_root_text_without_slash_is_not_reserved():
    """Gốc "fms" chỉ chặn "fms/..." - "fmsx/out" là topic khác, được phép."""
    client, manager = _client()

    resp = client.post("/api/publish", json={"topic": "fmsx/out", "payload": "x"})

    assert resp.json()["ok"] is True
    assert manager.mqtt_cmd.calls == [("fmsx/out", "x")]


@pytest.mark.parametrize("consumer_topic", ["+/+/meas", "#", "/fms/+/meas"])
def test_wildcard_or_empty_consumer_root_still_blocks_hardcoded_fms(consumer_topic, monkeypatch):
    monkeypatch.setattr(settings, "mqtt_consumer_topic", consumer_topic)
    monkeypatch.setattr(settings, "publish_topic_allow", "fms,plant")
    client, manager = _client()

    blocked = client.post("/api/publish", json={"topic": "fms/N1/cmd", "payload": "x"}).json()
    ok = client.post("/api/publish", json={"topic": "plant/x", "payload": "x"}).json()

    assert blocked["ok"] is False and "không được phép" in blocked["error"]
    assert ok["ok"] is True
    assert manager.mqtt_cmd.calls == [("plant/x", "x")]


def _real_manager_with_mqtt_drivers(*topic_bases):
    m = SourceManager(on_value=lambda *a: None)
    for i, base in enumerate(topic_bases):
        cfg = {"code": "SRC%d" % i, "kind": "mqtt"}
        if base is not None:
            cfg["topic_base"] = base
        m._drivers[cfg["code"]] = MqttDriver(cfg, [], lambda *a: None)
    m.mqtt_cmd = _FakeMqttCmd()
    return m


def test_source_manager_mqtt_topic_bases_lists_only_mqtt_drivers():
    m = _real_manager_with_mqtt_drivers("plantA", None)
    m._drivers["OTHER"] = object()   # driver không phải MQTT -> bỏ qua

    assert sorted(m.mqtt_topic_bases()) == ["factory", "plantA"]


@pytest.mark.parametrize("topic", ["plantA/cmd/relay1", "factory/cmd/x", "plantA/cmd/",
                                   "plantA/cmd", "plantA", "factory", "plantA/report/line1"])
def test_publish_under_mqtt_driver_base_is_reserved_even_if_allowed(topic, monkeypatch):
    """Gốc nguồn MQTT vừa là lệnh (<gốc>/cmd/) vừa là số đo (<gốc>/#) -> cấm
    toàn bộ cây, kể cả khi quản trị lỡ đưa gốc đó vào allowlist."""
    monkeypatch.setattr(settings, "publish_topic_allow", "plantA,factory")
    m = _real_manager_with_mqtt_drivers("plantA", None)
    client, _ = _client(m)

    resp = client.post("/api/publish", json={"topic": topic, "payload": "1"})

    data = resp.json()
    assert data["ok"] is False and "không được phép" in data["error"]
    assert data["reason"] == "topic_not_allowed"
    assert m.mqtt_cmd.calls == []


def test_publish_sibling_of_mqtt_driver_base_is_allowed(monkeypatch):
    monkeypatch.setattr(settings, "publish_topic_allow", "plantAB")
    m = _real_manager_with_mqtt_drivers("plantA")
    client, _ = _client(m)

    resp = client.post("/api/publish", json={"topic": "plantAB/report", "payload": "1"})

    assert resp.json()["ok"] is True
    assert m.mqtt_cmd.calls == [("plantAB/report", "1")]


# --- 3b. allowlist ------------------------------------------------------


@pytest.mark.parametrize("allow", ["", "  ", " , ,"])
def test_empty_allowlist_disables_publish(allow, monkeypatch):
    monkeypatch.setattr(settings, "publish_topic_allow", allow)
    client, manager = _client()

    data = client.post("/api/publish", json={"topic": "plant/x", "payload": "x"}).json()

    assert data["ok"] is False and data["status"] == "error"
    assert "publish bị tắt" in data["error"]
    assert data["reason"] == "publish_disabled"
    assert manager.mqtt_cmd.calls == []


def test_disabled_check_runs_before_topic_validation(monkeypatch):
    monkeypatch.setattr(settings, "publish_topic_allow", "")
    client, _ = _client()

    data = client.post("/api/publish", json={"payload": "x"}).json()

    assert "publish bị tắt" in data["error"]


def test_allowlist_is_read_per_request_hot_reload(monkeypatch):
    client, manager = _client()
    monkeypatch.setattr(settings, "publish_topic_allow", "")
    off = client.post("/api/publish", json={"topic": "plant/x", "payload": "1"}).json()
    monkeypatch.setattr(settings, "publish_topic_allow", "plant")
    on = client.post("/api/publish", json={"topic": "plant/x", "payload": "1"}).json()

    assert off["ok"] is False and on["ok"] is True
    assert manager.mqtt_cmd.calls == [("plant/x", "1")]


def test_allowlist_multiple_prefixes_with_whitespace(monkeypatch):
    monkeypatch.setattr(settings, "publish_topic_allow", " line1/out , line2/out/ ,, ")
    client, manager = _client()

    r1 = client.post("/api/publish", json={"topic": "line1/out/a", "payload": "1"}).json()
    r2 = client.post("/api/publish", json={"topic": "line2/out/b", "payload": "2"}).json()
    r3 = client.post("/api/publish", json={"topic": "line2/out", "payload": "3"}).json()

    assert (r1["ok"], r2["ok"], r3["ok"]) == (True, True, True)
    assert manager.mqtt_cmd.calls == [("line1/out/a", "1"), ("line2/out/b", "2"), ("line2/out", "3")]


@pytest.mark.parametrize("topic", ["other/x", "plan/x", "line1"])
def test_topic_outside_allowlist_is_rejected(topic, monkeypatch):
    monkeypatch.setattr(settings, "publish_topic_allow", "plant,line1/out")
    client, manager = _client()

    data = client.post("/api/publish", json={"topic": topic, "payload": "x"}).json()

    assert data["ok"] is False and "không được phép" in data["error"]
    assert data["reason"] == "topic_not_allowed"
    assert manager.mqtt_cmd.calls == []
    assert inbound_api.recent_requests()[0] == dict(
        inbound_api.recent_requests()[0], endpoint="/api/publish", topic=topic, rejected="not allowed")


@pytest.mark.parametrize("topic, ok", [("plant/out", True), ("plant/out/x", True),
                                       ("plant/outx", False), ("plant/ou", False)])
def test_allowlist_matches_by_topic_level(topic, ok, monkeypatch):
    monkeypatch.setattr(settings, "publish_topic_allow", "plant/out")
    client, manager = _client()

    data = client.post("/api/publish", json={"topic": topic, "payload": "x"}).json()

    assert data["ok"] is ok
    assert len(manager.mqtt_cmd.calls) == (1 if ok else 0)


@pytest.mark.parametrize("topic", ["fms/x", "fms", "factory", "factory/cmd/r1"])
def test_reserved_wins_over_allowlist(topic, monkeypatch):
    """allow="fms,factory" (quản trị cấu hình nhầm) vẫn chặn gốc node + gốc nguồn MQTT."""
    monkeypatch.setattr(settings, "publish_topic_allow", "fms,factory")
    m = _real_manager_with_mqtt_drivers(None)          # nguồn MQTT gốc mặc định "factory"
    client, _ = _client(m)

    data = client.post("/api/publish", json={"topic": topic, "payload": "x"}).json()

    assert data["ok"] is False and "không được phép" in data["error"]
    assert data["reason"] == "topic_not_allowed"
    assert m.mqtt_cmd.calls == []


def test_under_helper_level_semantics():
    assert inbound_api._under("a/b", "a/b")
    assert inbound_api._under("a/b/c", "a/b/")
    assert not inbound_api._under("a/bc", "a/b")
    assert not inbound_api._under("a/b", "/")         # prefix rỗng sau rstrip không phủ gì
    assert not inbound_api._under("x", "")


# --- 4. payload ---------------------------------------------------------


@pytest.mark.parametrize("raw, expected", [
    ("plain text", "plain text"),
    ('{"already":"json"}', '{"already":"json"}'),   # str KHÔNG bị json-dumps lại
    ("", ""),
    (None, ""),
    (42, "42"),
    (3.5, "3.5"),
    (True, "true"),
    (False, "false"),
    ({"tên": "Máy ép"}, '{"tên": "Máy ép"}'),     # ensure_ascii=False - không escape \u
    ({"a": 1, "b": [1, 2]}, '{"a": 1, "b": [1, 2]}'),
    ([1, "x"], '[1, "x"]'),
])
def test_publish_payload_normalisation(raw, expected):
    client, manager = _client()

    resp = client.post("/api/publish", json={"topic": "a/b", "payload": raw})

    assert resp.json()["ok"] is True
    assert manager.mqtt_cmd.calls == [("a/b", expected)]


def test_publish_missing_payload_key_sends_empty_string():
    client, manager = _client()

    client.post("/api/publish", json={"topic": "a/b"})

    assert manager.mqtt_cmd.calls == [("a/b", "")]


def test_publish_payload_exactly_4096_bytes_is_accepted():
    client, manager = _client()

    resp = client.post("/api/publish", json={"topic": "a/b", "payload": "x" * 4096})

    assert resp.json()["ok"] is True
    assert len(manager.mqtt_cmd.calls) == 1


def test_publish_payload_over_4096_bytes_is_rejected():
    client, manager = _client()

    resp = client.post("/api/publish", json={"topic": "a/b", "payload": "x" * 4097})

    data = resp.json()
    assert data["ok"] is False and data["status"] == "error"
    assert "4097" in data["error"]
    assert data["reason"] == "payload_too_large"
    assert manager.mqtt_cmd.calls == []


def test_publish_payload_limit_counts_utf8_bytes_not_chars():
    """2049 ký tự "é" = 4098 byte utf-8 -> phải bị chặn dù < 4096 ký tự."""
    client, manager = _client()

    resp = client.post("/api/publish", json={"topic": "a/b", "payload": "é" * 2049})

    assert resp.json()["ok"] is False
    assert resp.json()["reason"] == "payload_too_large"
    assert manager.mqtt_cmd.calls == []


@pytest.mark.parametrize("raw", ['"\\ud800"', '{"k": "\\udfff"}'])
def test_publish_lone_surrogate_payload_is_error_not_500(raw):
    """Body JSON hợp lệ (ascii, escape \\ud800) nhưng sau parse là lone surrogate."""
    client, manager = _client()
    body = ('{"topic": "a/b", "payload": %s}' % raw).encode("ascii")

    resp = client.post("/api/publish", content=body,
                       headers={"Content-Type": "application/json"})

    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is False and data["status"] == "error"
    assert "UTF-8" in data["error"]
    assert data["reason"] == "invalid_payload"
    assert manager.mqtt_cmd.calls == []


@pytest.mark.parametrize("raw", ["NaN", "Infinity", "-Infinity", "1e400",
                                 '{"t": NaN}', "[1, -Infinity]", '{"a": [{"b": 1e400}]}'],
                         ids=["nan", "inf", "neg-inf", "overflow", "dict-nan",
                              "list-neg-inf", "nested-overflow"])
def test_publish_nonfinite_payload_is_invalid_payload_not_published(raw):
    """Regression esp32-nonfinite-cmd: request.json() nhận NaN/Infinity (1e400 ->
    inf); json.dumps mặc định phát lại NaN/Infinity - JSON không hợp lệ, subscriber
    cJSON bỏ gói im lặng trong khi Odoo tưởng đã publish."""
    client, manager = _client()
    body = ('{"topic": "plant/line1/out", "payload": %s}' % raw).encode("ascii")

    resp = client.post("/api/publish", content=body,
                       headers={"Content-Type": "application/json"})

    assert resp.status_code == 200
    assert resp.json() == {"ok": False, "status": "error", "reason": "invalid_payload",
                           "error": "payload chứa số không hữu hạn (NaN/Infinity)"}
    assert manager.mqtt_cmd.calls == []


@pytest.mark.parametrize("raw,expected", [
    ('{"t": 21.5, "n": [1, -2, 1e308]}', '{"t": 21.5, "n": [1, -2, 1e+308]}'),
    ("3.5", "3.5"),
    ('"NaN"', "NaN"),
    ('"Infinity"', "Infinity"),
], ids=["finite-dict", "finite-number", "str-NaN", "str-Infinity"])
def test_publish_finite_or_string_payload_still_published(raw, expected):
    client, manager = _client()
    body = ('{"topic": "plant/line1/out", "payload": %s}' % raw).encode("ascii")

    resp = client.post("/api/publish", content=body,
                       headers={"Content-Type": "application/json"})

    assert resp.json() == {"ok": True, "status": "ok"}
    assert manager.mqtt_cmd.calls == [("plant/line1/out", expected)]


def test_publish_request_log_records_size_but_not_payload():
    client, _ = _client()

    client.post("/api/publish", json={"topic": "a/b", "payload": SECRET_PAYLOAD})

    row = inbound_api.recent_requests()[0]
    assert row["endpoint"] == "/api/publish"
    assert row["topic"] == "a/b"
    assert row["size"] == len(SECRET_PAYLOAD)
    assert SECRET_PAYLOAD not in repr(inbound_api.recent_requests())


# --- 5. mqtt_cmd None ---------------------------------------------------


def test_publish_when_mqtt_consumer_disabled_is_error():
    client, _ = _client(_Manager(mqtt_cmd=None))

    resp = client.post("/api/publish", json={"topic": "a/b", "payload": "x"})

    data = resp.json()
    assert data["ok"] is False and data["status"] == "error"
    assert "chưa bật" in data["error"]
    assert data["reason"] == "publish_disabled"


def test_publish_when_mqtt_consumer_setting_disabled_is_error_even_if_mqtt_cmd_exists(monkeypatch):
    """r3: consumer đã tắt trong settings (hot-reload) nhưng object mqtt_cmd cũ
    vẫn còn trên manager -> KHÔNG được publish qua client cũ."""
    monkeypatch.setattr(settings, "mqtt_consumer_enabled", False)
    client, manager = _client()

    data = client.post("/api/publish", json={"topic": "a/b", "payload": "x"}).json()

    assert data["ok"] is False and data["status"] == "error"
    assert data["reason"] == "publish_disabled"
    assert manager.mqtt_cmd.calls == []


@pytest.mark.parametrize("content", [b"not json", b"[1, 2]"])
def test_publish_bad_json_body_error_has_no_reason(content):
    client, manager = _client()

    data = client.post("/api/publish", content=content,
                       headers={"Content-Type": "application/json"}).json()

    assert data["ok"] is False and data["status"] == "error"
    assert "reason" not in data
    assert manager.mqtt_cmd.calls == []


# --- 6. MqttConsumer.publish_raw ---------------------------------------


class _Info:
    def __init__(self, rc, mid=1, published_after=0):
        self.rc = rc
        self.mid = mid
        self._polls = 0
        self._after = published_after

    def is_published(self):
        self._polls += 1
        return self._after is not None and self._polls > self._after


class _FakeCli:
    def __init__(self, info=None, exc=None):
        self.info = info
        self.exc = exc
        self.calls = []
        import threading
        self._out_message_mutex = threading.Lock()
        self._out_messages = {}

    def publish(self, topic, payload, **kw):
        self.calls.append((topic, payload, kw))
        if self.exc:
            raise self.exc
        return self.info


class _Agent:
    manager = None


def _consumer(cli, connected=True):
    c = MqttConsumer(_Agent())
    c._cli = cli
    c._connected = connected
    return c


@pytest.mark.parametrize("cli, connected", [(None, True), (_FakeCli(), False)])
def test_publish_raw_not_connected_returns_error_without_publishing(cli, connected):
    c = _consumer(cli, connected)

    res = asyncio.run(c.publish_raw("a/b", "x"))

    assert res == {"ok": False, "status": "error", "reason": "broker_down", "error": "broker not connected"}
    if cli is not None:
        assert cli.calls == []


def test_publish_raw_success_uses_qos1_and_logs_traffic_event():
    cli = _FakeCli(_Info(mqtt_consumer.mqtt.MQTT_ERR_SUCCESS, published_after=2))
    c = _consumer(cli)

    res = asyncio.run(c.publish_raw("a/b", "héllo", wait_s=2.0))

    assert res == {"ok": True, "status": "ok"}
    assert cli.calls == [("a/b", "héllo", {"qos": 1})]
    ev = c.traffic[-1]
    assert ev["dir"] == "down" and ev["topic"] == "a/b"
    assert ev["bytes"] == len("héllo".encode())


def test_publish_raw_without_puback_within_wait_s_is_unknown():
    cli = _FakeCli(_Info(mqtt_consumer.mqtt.MQTT_ERR_SUCCESS, published_after=None))
    c = _consumer(cli)

    res = asyncio.run(c.publish_raw("a/b", "x", wait_s=0.12))

    assert res["ok"] is False
    assert res["status"] == "unknown"
    assert res["reason"] == "no_puback"


def test_publish_raw_other_rc_error_is_error():
    cli = _FakeCli(_Info(mqtt_consumer.mqtt.MQTT_ERR_QUEUE_SIZE))
    c = _consumer(cli)

    res = asyncio.run(c.publish_raw("a/b", "x"))

    assert res["ok"] is False and res["status"] == "error"
    assert res["reason"] == "broker_down"
    assert "rc=" in res["error"]


def test_publish_raw_exception_from_paho_is_error():
    cli = _FakeCli(exc=ValueError("Invalid topic."))
    c = _consumer(cli)

    res = asyncio.run(c.publish_raw("a/b", "x"))

    assert res == {"ok": False, "status": "error", "reason": "broker_down",
                   "error": "Invalid topic."}


def test_publish_raw_no_conn_real_paho_removes_held_message():
    """paho Client THẬT chưa connect: publish QoS1 -> rc=NO_CONN, paho GIỮ gói
    trong _out_messages để tự gửi khi reconnect. publish_raw KHÔNG xếp hàng ->
    phải báo lỗi VÀ rút gói đó ra (không được gửi muộn sau này)."""
    c = _consumer(mqtt_consumer.mqtt.Client(client_id="pytest-no-broker"))
    before = dict(c._cli._out_messages)

    res = asyncio.run(c.publish_raw("plant/out", "x", wait_s=0.1))

    assert res == {"ok": False, "status": "error", "reason": "broker_down", "error": "broker not connected"}
    assert c._cli._out_messages == before == {}


def _msg(state=None, dup=False):
    return SimpleNamespace(state=mqtt_consumer.mqtt.mqtt_ms_publish if state is None else state,
                           dup=dup)


def test_publish_raw_no_conn_only_removes_its_own_mid():
    cli = _FakeCli(_Info(mqtt_consumer.mqtt.MQTT_ERR_NO_CONN, mid=7))
    other = _msg()
    cli._out_messages = {7: _msg(), 8: other}
    c = _consumer(cli)

    res = asyncio.run(c.publish_raw("a/b", "x"))

    assert res == {"ok": False, "status": "error", "reason": "broker_down", "error": "broker not connected"}
    assert cli._out_messages == {8: other}


@pytest.mark.parametrize("held", [
    "missing",                                                    # paho đã gửi xong + xoá
    "dup",                                                        # đã gửi ít nhất 1 lần
    "sent_state",                                                 # đã ra socket, chờ PUBACK
])
def test_publish_raw_no_conn_but_maybe_already_sent_is_unknown_and_kept(held):
    """Giữa publish() và lúc giành mutex paho có thể đã reconnect + gửi: KHÔNG
    được báo "chưa gửi" (Odoo sẽ retry -> gửi 2 lần) và không động vào gói."""
    cli = _FakeCli(_Info(mqtt_consumer.mqtt.MQTT_ERR_NO_CONN, mid=7))
    if held == "dup":
        msg = _msg(dup=True)
    elif held == "sent_state":
        msg = _msg(state=mqtt_consumer.mqtt.mqtt_ms_wait_for_puback)
    if held != "missing":
        cli._out_messages = {7: msg}
    before = dict(cli._out_messages)
    c = _consumer(cli)

    res = asyncio.run(c.publish_raw("a/b", "x"))

    assert res["ok"] is False and res["status"] == "unknown"
    assert res["reason"] == "no_puback"
    assert cli._out_messages == before


def test_publish_raw_puback_timeout_keeps_message_and_inflight_untouched():
    """Theo 🟠 reviewer vòng 2: gói đã ở wait_for_puback mà bị rút thì paho
    KHÔNG giảm _inflight_messages -> rò slot inflight, có thể kẹt
    publish_command. Hết hạn PUBACK chỉ báo "unknown", KHÔNG đụng paho."""
    cli = _FakeCli(_Info(mqtt_consumer.mqtt.MQTT_ERR_SUCCESS, mid=5, published_after=None))
    mine = _msg(state=mqtt_consumer.mqtt.mqtt_ms_wait_for_puback)
    other = _msg()
    cli._out_messages = {5: mine, 6: other}
    cli._inflight_messages = 1
    c = _consumer(cli)

    res = asyncio.run(c.publish_raw("a/b", "x", wait_s=0.06))

    assert res == {"ok": False, "status": "unknown", "reason": "no_puback",
                   "error": res["error"]}
    assert cli._out_messages == {5: mine, 6: other}
    assert cli._inflight_messages == 1


def test_publish_raw_default_wait_s_is_2s():
    import inspect
    assert inspect.signature(MqttConsumer.publish_raw).parameters["wait_s"].default == 2.0


def test_publish_raw_logs_do_not_contain_payload(caplog):
    caplog.set_level(logging.DEBUG)
    ok_cli = _FakeCli(_Info(mqtt_consumer.mqtt.MQTT_ERR_SUCCESS))
    slow_cli = _FakeCli(_Info(mqtt_consumer.mqtt.MQTT_ERR_SUCCESS, published_after=None))
    err_cli = _FakeCli(exc=RuntimeError("boom"))

    c1 = _consumer(ok_cli)
    asyncio.run(c1.publish_raw("a/b", SECRET_PAYLOAD))
    asyncio.run(_consumer(slow_cli).publish_raw("a/b", SECRET_PAYLOAD, wait_s=0.06))
    asyncio.run(_consumer(err_cli).publish_raw("a/b", SECRET_PAYLOAD))

    assert caplog.records, "phải có log để truy vết"
    assert SECRET_PAYLOAD not in caplog.text
    assert all(SECRET_PAYLOAD not in str(ev) for ev in c1.traffic)


# --- 7. MqttDriver.command giữ topic cũ ---------------------------------


class _DriverCli:
    def __init__(self):
        self.calls = []

    def publish(self, topic, payload):
        self.calls.append((topic, payload))


@pytest.mark.parametrize("base, expected", [("plantA", "plantA/cmd/relay1"),
                                            (None, "factory/cmd/relay1")])
def test_mqtt_driver_command_publishes_legacy_cmd_topic(base, expected):
    cfg = {"code": "S1", "kind": "mqtt"}
    if base:
        cfg["topic_base"] = base
    d = MqttDriver(cfg, [], lambda *a: None)
    d._cli = _DriverCli()

    res = asyncio.run(d.command("relay1", "on", 1))

    assert res == {"ok": True, "status": "ok"}
    assert d._cli.calls == [(expected, "1")]
    assert expected.startswith(d.cmd_topic_prefix())


# --- 8. config hot-reload ----------------------------------------------


def test_settings_reload_picks_up_publish_topic_allow(monkeypatch):
    monkeypatch.setattr(settings, "publish_topic_allow", "")
    monkeypatch.setenv("EDGE_PUBLISH_TOPIC_ALLOW", "plant/out,line2")

    settings.reload()

    assert settings.publish_topic_allow == "plant/out,line2"
    assert inbound_api._allowed_topic_prefixes() == ["plant/out", "line2"]
