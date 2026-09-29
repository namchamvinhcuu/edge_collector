# -*- coding: utf-8 -*-
"""Test edge_collector/mqtt_consumer.py (MOI, patch MQTT merge tu production
192.168.5.190) - CHI phan logic thuan/goi lai qua object gia, KHONG dung
broker MQTT that (khong co broker gia lap san trong project, khac
drivers/sim.py von co san mot driver 'sim' cho luong Modbus/OPC-UA).

Scope: _parse_topic()/_split_url()/_topic_sibling() (+ _cmd_topic/_ack_topic)
la ham thuan tuy; MqttConsumer._handle() va .publish_command() la method goi
duoc TRUC TIEP (khong async, khong can vong lap paho that) mien la tu cap
`self._cli`/`self._connected`/`self.caps`/`self.stats["online"]` truoc -
day chinh la 2 diem noi voi manager.queue_command() (xem
tests/test_manager_queue_command.py cho phia SourceManager) va voi
scheduler.push_node_reading()/manager.node_ack_command() (xem duoi).

start()/stop() (can paho.mqtt.client.Client that ket noi mang that) BI SKIP -
xem 'Scope da KHONG cover' trong bao cao cuoi."""
import json

import pytest

import edge_collector.config as config
import edge_collector.mqtt_consumer as mqtt_consumer
from edge_collector.mqtt_consumer import (
    MqttConsumer, _ack_topic, _canonical, _cmd_topic, _parse_topic, _sign,
    _split_url, _topic_sibling, _verify_sig,
)


# --- ham thuan tuy -----------------------------------------------------


@pytest.mark.parametrize("topic,expected", [
    ("fms/68EE8F4F06A8/meas", ("68EE8F4F06A8", "meas")),
    ("fms/NODE-01/status", ("NODE-01", "status")),
    ("fms/NODE-01/cmdack", ("NODE-01", "cmdack")),
    ("meas", (None, None)),                 # thieu segment - khong the tach serial
    ("a/b", (None, None)),
    ("", (None, None)),
])
def test_parse_topic(topic, expected):
    assert _parse_topic(topic) == expected


def test_parse_topic_none_input_does_not_crash():
    assert _parse_topic(None) == (None, None)


@pytest.mark.parametrize("url,expected", [
    ("mqtt://192.168.5.190:1883", ("192.168.5.190", 1883)),
    ("tcp://broker.local:8883", ("broker.local", 8883)),
    ("192.168.5.190:1883", ("192.168.5.190", 1883)),
    ("192.168.5.190", ("192.168.5.190", 1883)),          # khong co port -> mac dinh 1883
    ("", ("127.0.0.1", 1883)),                           # rong -> mac dinh an toan
    ("mqtt://broker.local:abc", ("broker.local", 1883)),  # port khong phai so -> fallback
])
def test_split_url(url, expected):
    assert _split_url(url) == expected


def test_topic_sibling_and_derived_topics(monkeypatch):
    monkeypatch.setattr(config.settings, "mqtt_consumer_topic", "fms/+/meas")

    assert _topic_sibling("cmd") == "fms/+/cmd"
    assert _ack_topic() == "fms/+/cmdack"
    assert _cmd_topic("NODE1") == "fms/NODE1/cmd"


def test_topic_sibling_returns_none_when_pattern_too_short(monkeypatch):
    """Mau chu de tuy chinh chi co 1 segment - khong du de suy ra chu de anh
    em (cmd/cmdack), phai tra None an toan thay vi crash/sinh chu de sai."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_topic", "meas")

    assert _topic_sibling("cmd") is None
    assert _cmd_topic("NODE1") is None
    # _ack_topic() co fallback cung "fms/+/cmdack" khi khong suy ra duoc.
    assert _ack_topic() == "fms/+/cmdack"


def test_cmd_topic_returns_none_when_serial_contains_wildcard_char():
    """Serial khong bao gio chua '+'/'#' trong thuc te, nhung ham phai an
    toan (tra None) neu the - gui lenh xuong mot chu de con la wildcard se
    lam broker tu choi hoac phat rong khap noi khong ai mong doi."""
    assert _cmd_topic("+") is None


# --- MqttConsumer._handle() ---------------------------------------------


class _FakeAgentManager:
    def __init__(self, api_key=None):
        self.touched = []
        self.acked = []
        self._api_key = api_key

    def touch_node(self, serial):
        self.touched.append(serial)

    def node_ack_command(self, cmd_id, ok, detail=""):
        self.acked.append((cmd_id, ok, detail))

    def cached_node_api_key(self, serial):
        """Gia lap Odoo da cap (hoac chua cap, tra None) api_key cho serial -
        dung cho test xac minh HMAC trong _handle()."""
        return self._api_key


class _FakeAgent:
    def __init__(self, api_key=None):
        self.manager = _FakeAgentManager(api_key=api_key)
        self.readings = []

    def push_node_reading(self, serial, ch, v, s, q, ts, stable):
        self.readings.append((serial, ch, v, s, q, ts, stable))


def _signed(api_key, payload):
    """payload (KHONG co 'sig') -> bytes JSON da gan them 'sig' dung cho
    dung api_key do - dung de dung test giong nhu node_agent that (mqtt_
    uplink.py) se lam."""
    body = dict(payload)
    body["sig"] = _sign(api_key, payload)
    return json.dumps(body).encode()


@pytest.fixture()
def consumer():
    return MqttConsumer(_FakeAgent())


def test_handle_invalid_json_increments_bad_and_does_not_crash(consumer):
    consumer._handle("fms/NODE1/meas", b"{not-json")

    assert consumer.stats["bad"] == 1
    assert consumer._agent.readings == []


def test_handle_non_dict_json_increments_bad_and_does_not_crash(consumer):
    consumer._handle("fms/NODE1/meas", b"[1, 2, 3]")

    assert consumer.stats["bad"] == 1


def test_handle_unparseable_topic_is_ignored_silently(consumer):
    """Topic khong tach duoc serial (vd < 3 segment) - _handle() phai return
    som, KHONG dem vao stats['bad'] (day khong phai goi tin hong, la topic
    khong khop mau minh dang nghe)."""
    consumer._handle("unrelated", b"{}")

    assert consumer.stats["bad"] == 0
    assert consumer.stats["messages"] == 0


def test_handle_status_message_marks_online_and_mqtt_capability(consumer):
    consumer._handle("fms/NODE1/status", json.dumps({"online": True, "cmd": True}).encode())

    assert consumer.stats["online"]["NODE1"] is True
    assert consumer.caps["NODE1"] is True
    assert len(consumer.traffic) == 1
    assert consumer.traffic[0]["dir"] == "up"


def test_handle_status_offline_does_not_erase_previously_declared_mqtt_capability():
    """'Bao khai nhan lenh qua MQTT la thuoc tinh cua FIRMWARE, con song hay
    chet la chuyen khac' (xem docstring _handle()) - node rot mang (offline,
    khong 'cmd') KHONG duoc xoa self.caps[serial] da True truoc do, neu
    khong manager se tuong day la firmware cu va xep lenh vao hang doi poll
    ma khong ai con poll nua."""
    consumer = MqttConsumer(_FakeAgent())
    consumer._handle("fms/NODE1/status", json.dumps({"online": True, "cmd": True}).encode())
    assert consumer.caps["NODE1"] is True

    consumer._handle("fms/NODE1/status", json.dumps({"online": False}).encode())

    assert consumer.stats["online"]["NODE1"] is False
    assert consumer.caps["NODE1"] is True   # KHONG bi xoa


def test_handle_cmdack_message_forwards_to_manager_node_ack_command(consumer):
    consumer._handle("fms/NODE1/cmdack",
                      json.dumps({"id": 5, "ok": True}).encode())

    assert consumer.stats["cmd_acked"] == 1
    assert consumer._agent.manager.acked == [(5, True, "")]


def test_handle_cmdack_with_non_int_id_increments_bad_and_skips_ack(consumer):
    consumer._handle("fms/NODE1/cmdack",
                      json.dumps({"id": "not-an-int", "ok": True}).encode())

    assert consumer.stats["bad"] == 1
    assert consumer._agent.manager.acked == []


def test_handle_measurement_items_forwards_each_to_push_node_reading(consumer, monkeypatch):
    monkeypatch.setattr(config.settings, "mqtt_consumer_forward", True)
    consumer._handle("fms/NODE1/meas", json.dumps({
        "items": [
            {"ch": "temp", "v": 21.5, "s": "ok", "q": 1, "ts": 1_700_000_000_000, "stable": True},
            {"ch": "hum", "v": 55.0, "s": "ok", "q": 1},
        ],
    }).encode())

    assert consumer._agent.manager.touched == ["NODE1"]
    assert consumer._agent.readings[0][:2] == ("NODE1", "temp")
    assert consumer._agent.readings[1][:2] == ("NODE1", "hum")
    assert consumer.stats["items"] == 2
    assert consumer.stats["by_serial"]["NODE1"] == 2
    assert consumer.stats["forwarded"] == 2


def test_handle_measurement_drops_epoch_insane_timestamp(consumer, monkeypatch):
    """Dau thoi gian truoc EPOCH_SANE_S (dong ho node chua dong bo, vd mat
    SNTP) phai bi bo (ts_s=None) va dem vao ts_dropped, KHONG duoc chuyen
    thang vao Odoo thanh nam 1970. ts_dropped tinh TRUOC nhanh gate
    mqtt_consumer_forward nen phai dung ca khi forward dang tat (mac dinh)."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_forward", True)
    consumer._handle("fms/NODE1/meas", json.dumps({
        "items": [{"ch": "temp", "v": 1, "ts": 1000}],  # 1000 ms = gan epoch 0
    }).encode())

    assert consumer.stats["ts_dropped"] == 1
    ts_arg = consumer._agent.readings[0][5]
    assert ts_arg is None


def test_handle_measurement_drops_epoch_insane_timestamp_even_when_forward_disabled(consumer):
    assert config.settings.mqtt_consumer_forward is False   # baseline mac dinh, khong monkeypatch

    consumer._handle("fms/NODE1/meas", json.dumps({
        "items": [{"ch": "temp", "v": 1, "ts": 1000}],
    }).encode())

    assert consumer.stats["ts_dropped"] == 1


def test_handle_measurement_does_not_forward_when_consumer_forward_disabled(consumer, monkeypatch):
    """EDGE_MQTT_CONSUMER_FORWARD=false (mac dinh) - CHI DEM (che do 'bong'),
    khong duoc goi push_node_reading() (tranh Odoo nhan doi khi node con
    phat ca HTTP lan MQTT cung luc - xem docstring dau file)."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_forward", False)

    consumer._handle("fms/NODE1/meas", json.dumps({
        "items": [{"ch": "temp", "v": 21.5}],
    }).encode())

    assert consumer._agent.readings == []
    assert consumer.stats["items"] == 1        # van dem so ban ghi thay duoc
    assert consumer.stats["forwarded"] == 0     # nhung khong day di dau


def test_handle_measurement_items_not_a_list_increments_bad(consumer):
    consumer._handle("fms/NODE1/meas", json.dumps({"items": "not-a-list"}).encode())

    assert consumer.stats["bad"] == 1


def test_handle_measurement_tracks_relay_channel_as_lamp(consumer):
    consumer._handle("fms/NODE1/meas", json.dumps({
        "items": [{"ch": "relay_red", "v": 1}],
    }).encode())

    assert consumer.lamps["relay_red"]["v"] == 1
    assert consumer.lamps["relay_red"]["serial"] == "NODE1"


def test_handle_measurement_lamp_state_tracked_even_when_forward_disabled():
    """Regression: self.lamps[...] (dung cho trang /ops '4 den') PHAI cap
    nhat doc lap voi EDGE_MQTT_CONSUMER_FORWARD (mac dinh false trong giai
    doan node con phat ca HTTP lan MQTT - xem settings_api.py hint 'Keep
    false while a node still sends...') - trang /ops phai xem duoc dung
    trang thai vat ly ngay ca khi chua bat forward-vao-Odoo (xem docstring
    dau ops_api.py: 'trang nay phai xem duoc dung luc Odoo hong'). Ban dau
    (truoc fix) self.lamps[...] nam SAU nhanh 'if not
    settings.mqtt_consumer_forward: continue' nen /ops se khong bao gio
    thay den cap nhat trong che do forward=false - da duoc doi vi tri trong
    mqtt_consumer.py::_handle() de tach khoi gate do."""
    consumer = MqttConsumer(_FakeAgent())
    assert config.settings.mqtt_consumer_forward is False   # baseline mac dinh

    consumer._handle("fms/NODE1/meas", json.dumps({
        "items": [{"ch": "relay_red", "v": 1}],
    }).encode())

    assert consumer.stats["items"] == 1
    assert consumer.lamps["relay_red"]["v"] == 1
    assert consumer.stats["forwarded"] == 0   # forward vao Odoo van tat, dung thiet ke


# --- HMAC: _canonical()/_sign()/_verify_sig() (ham thuan tuy) ------------


def test_canonical_sorts_keys_and_strips_whitespace():
    assert _canonical({"b": 1, "a": 2}) == b'{"a":2,"b":1}'


def test_canonical_is_independent_of_input_dict_key_order():
    assert _canonical({"b": 1, "a": 2}) == _canonical({"a": 2, "b": 1})


def test_sign_is_deterministic_for_same_key_and_payload():
    assert _sign("k1", {"a": 1}) == _sign("k1", {"a": 1})


def test_sign_differs_when_api_key_differs():
    assert _sign("k1", {"a": 1}) != _sign("k2", {"a": 1})


def test_sign_differs_when_payload_differs():
    assert _sign("k1", {"a": 1}) != _sign("k1", {"a": 2})


def test_verify_sig_accepts_correctly_signed_payload():
    payload = {"ch": "temp", "v": 21.5}
    data = dict(payload, sig=_sign("secret123", payload))

    assert _verify_sig("secret123", data) is True


def test_verify_sig_is_independent_of_original_dict_key_order():
    """_canonical() dung sort_keys=True nen thu tu field trong dict GOC
    (truoc khi ky/xac minh) khong duoc anh huong ket qua - node/edge co the
    serialize object theo thu tu bat ky, mien noi dung field giong nhau."""
    payload_signed_as = {"ch": "temp", "v": 1, "ts": 123}
    payload_received_as = {"ts": 123, "v": 1, "ch": "temp"}
    sig = _sign("secret123", payload_signed_as)
    data = dict(payload_received_as, sig=sig)

    assert _verify_sig("secret123", data) is True


def test_verify_sig_rejects_payload_tampered_after_signing():
    payload = {"ch": "temp", "v": 21.5}
    sig = _sign("secret123", payload)
    tampered = {"ch": "temp", "v": 999.0, "sig": sig}   # doi 1 field sau khi ky

    assert _verify_sig("secret123", tampered) is False


def test_verify_sig_rejects_signature_made_with_wrong_key():
    payload = {"ch": "temp", "v": 21.5}
    data = dict(payload, sig=_sign("attacker-key", payload))

    assert _verify_sig("secret123", data) is False


def test_verify_sig_rejects_missing_sig_field():
    assert _verify_sig("secret123", {"ch": "temp", "v": 21.5}) is False


def test_verify_sig_rejects_non_string_sig():
    assert _verify_sig("secret123", {"ch": "temp", "sig": 12345}) is False


# --- MqttConsumer._handle() + xac minh HMAC (tich hop qua object gia) ---


def test_handle_measurement_without_sig_is_processed_as_before(monkeypatch):
    """Tuong thich nguoc: ESP32 firmware cu khong biet ky, khong gui 'sig' -
    PHAI van xu ly binh thuong du Odoo DA cap api_key cho serial nay (khong
    ep buoc ky chi vi da co key cache)."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_forward", True)
    consumer = MqttConsumer(_FakeAgent(api_key="secret123"))  # secret-allow: test fixture, khong phai credential that

    consumer._handle("fms/NODE1/meas", json.dumps({
        "items": [{"ch": "temp", "v": 21.5}],
    }).encode())

    assert consumer.stats["sig_rejected"] == 0
    assert consumer._agent.readings[0][:2] == ("NODE1", "temp")


def test_handle_measurement_with_valid_sig_is_forwarded(monkeypatch):
    monkeypatch.setattr(config.settings, "mqtt_consumer_forward", True)
    consumer = MqttConsumer(_FakeAgent(api_key="secret123"))  # secret-allow: test fixture, khong phai credential that
    payload = {"items": [{"ch": "temp", "v": 21.5}]}

    consumer._handle("fms/NODE1/meas", _signed("secret123", payload))

    assert consumer.stats["sig_rejected"] == 0
    assert consumer._agent.readings[0][:2] == ("NODE1", "temp")
    assert consumer.stats["forwarded"] == 1


def test_handle_measurement_with_invalid_sig_is_rejected_and_not_forwarded(monkeypatch):
    """Doi 1 gia tri trong payload SAU khi da ky (gia mao) - sig cu khong con
    khop, phai bi tu choi TRUOC ca touch_node()/push_node_reading()."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_forward", True)
    consumer = MqttConsumer(_FakeAgent(api_key="secret123"))  # secret-allow: test fixture, khong phai credential that
    payload = {"items": [{"ch": "temp", "v": 21.5}]}
    signed = json.loads(_signed("secret123", payload))
    signed["items"][0]["v"] = 999.0

    consumer._handle("fms/NODE1/meas", json.dumps(signed).encode())

    assert consumer.stats["sig_rejected"] == 1
    assert consumer._agent.readings == []
    assert consumer._agent.manager.touched == []


def test_handle_measurement_with_sig_but_no_cached_api_key_is_rejected(monkeypatch):
    """Odoo chua cap api_key cho serial nay (cached_node_api_key tra None) -
    khong the xac minh duoc thi TU CHOI, khong co duong ha tieu chuan."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_forward", True)
    consumer = MqttConsumer(_FakeAgent(api_key=None))
    payload = {"items": [{"ch": "temp", "v": 21.5}]}

    consumer._handle("fms/NODE1/meas", _signed("any-key", payload))

    assert consumer.stats["sig_rejected"] == 1
    assert consumer._agent.readings == []


def test_handle_status_with_valid_sig_is_processed_normally():
    consumer = MqttConsumer(_FakeAgent(api_key="secret123"))  # secret-allow: test fixture, khong phai credential that
    payload = {"online": True, "cmd": True}

    consumer._handle("fms/NODE1/status", _signed("secret123", payload))

    assert consumer.stats["online"]["NODE1"] is True
    assert consumer.caps["NODE1"] is True
    assert consumer.stats["sig_rejected"] == 0


def test_handle_status_with_invalid_sig_is_rejected_and_online_not_updated():
    consumer = MqttConsumer(_FakeAgent(api_key="secret123"))  # secret-allow: test fixture, khong phai credential that
    payload = {"online": True, "cmd": True}
    signed = json.loads(_signed("secret123", payload))
    signed["online"] = False   # gia mao sau khi ky, sig cu khong con khop

    consumer._handle("fms/NODE1/status", json.dumps(signed).encode())

    assert consumer.stats["sig_rejected"] == 1
    assert "NODE1" not in consumer.stats["online"]
    assert "NODE1" not in consumer.caps


def test_handle_status_offline_lwt_without_sig_still_marks_offline():
    """LWT ({"online": false}) do BROKER tu phat khi mat ket noi voi node -
    khong the ky dong (khong phai node chu dong gui), nen KHONG co 'sig'.
    Logic 'sig tuy chon' phai cho qua binh thuong du serial nay DA co
    api_key cache (khong ep buoc ky cho thong diep broker tu phat)."""
    consumer = MqttConsumer(_FakeAgent(api_key="secret123"))  # secret-allow: test fixture, khong phai credential that

    consumer._handle("fms/NODE1/status", json.dumps({"online": False}).encode())

    assert consumer.stats["online"]["NODE1"] is False
    assert consumer.stats["sig_rejected"] == 0


def test_handle_cmdack_with_valid_sig_is_acked_normally():
    consumer = MqttConsumer(_FakeAgent(api_key="secret123"))  # secret-allow: test fixture, khong phai credential that
    payload = {"id": 5, "ok": True}

    consumer._handle("fms/NODE1/cmdack", _signed("secret123", payload))

    assert consumer._agent.manager.acked == [(5, True, "")]
    assert consumer.stats["sig_rejected"] == 0


def test_handle_cmdack_with_invalid_sig_is_rejected_and_not_acked():
    consumer = MqttConsumer(_FakeAgent(api_key="secret123"))  # secret-allow: test fixture, khong phai credential that
    payload = {"id": 5, "ok": True}
    signed = json.loads(_signed("secret123", payload))
    signed["ok"] = False   # gia mao ket qua lenh sau khi ky

    consumer._handle("fms/NODE1/cmdack", json.dumps(signed).encode())

    assert consumer.stats["sig_rejected"] == 1
    assert consumer._agent.manager.acked == []


# --- MqttConsumer.publish_command() -------------------------------------


class _FakePublishInfo:
    def __init__(self, rc):
        self.rc = rc


class _FakeClient:
    def __init__(self, rc=0, raise_exc=None):
        self._rc = rc
        self._raise_exc = raise_exc
        self.published = []

    def publish(self, topic, payload, qos=1):
        if self._raise_exc:
            raise self._raise_exc
        self.published.append((topic, payload, qos))
        return _FakePublishInfo(self._rc)


def _wired_consumer(client_rc=0, client_exc=None, connected=True,
                    caps=None, online=None):
    c = MqttConsumer(_FakeAgent())
    c._cli = _FakeClient(rc=client_rc, raise_exc=client_exc)
    c._connected = connected
    c.caps = caps if caps is not None else {"NODE1": True}
    c.stats["online"] = online if online is not None else {"NODE1": True}
    return c


def test_publish_command_succeeds_and_updates_stats(monkeypatch):
    monkeypatch.setattr(config.settings, "mqtt_consumer_topic", "fms/+/meas")
    c = _wired_consumer()

    ok = c.publish_command("NODE1", {"id": 1, "cmd": "write", "channel": "relay_red", "value": 1})

    assert ok is True
    assert c.stats["cmd_sent"] == 1
    # Counter rieng cua nhanh NO_CONN (them 2026-09-24) KHONG duoc tang nham
    # o case thanh cong thuong nay - xem test_publish_command_returns_true_
    # and_counts_via_cmd_sent_no_conn_when_broker_rc_is_no_conn ben duoi.
    assert c.stats.get("cmd_sent_no_conn", 0) == 0
    assert len(c._cli.published) == 1
    topic, payload, qos = c._cli.published[0]
    assert topic == "fms/NODE1/cmd"
    assert qos == 1
    assert json.loads(payload) == {"id": 1, "cmd": "write", "channel": "relay_red", "value": 1}
    assert len(c.traffic) == 1 and c.traffic[0]["dir"] == "down"


def test_publish_command_fails_when_no_client_or_not_connected():
    c = _wired_consumer(connected=False)
    assert c.publish_command("NODE1", {"id": 1}) is False

    c2 = MqttConsumer(_FakeAgent())    # _cli ban dau la None (chua start())
    assert c2.publish_command("NODE1", {"id": 1}) is False


def test_publish_command_fails_when_node_has_not_declared_mqtt_capability(monkeypatch):
    monkeypatch.setattr(config.settings, "mqtt_consumer_topic", "fms/+/meas")
    c = _wired_consumer(caps={})   # NODE1 chua khai "cmd" qua status

    assert c.publish_command("NODE1", {"id": 1}) is False
    assert c._cli.published == []


def test_publish_command_fails_when_node_currently_offline(monkeypatch):
    """Chu de lenh KHONG retain va node khong dung phien ben - goi cho mot
    node dang OFFLINE se bi broker vut di ma khong ai bao, nen phai tra
    False NGAY thay vi 'da gui' roi im lang (xem docstring publish_command())."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_topic", "fms/+/meas")
    c = _wired_consumer(online={"NODE1": False})

    assert c.publish_command("NODE1", {"id": 1}) is False
    assert c._cli.published == []


def test_publish_command_fails_when_topic_cannot_be_derived(monkeypatch):
    """Mau chu de cau hinh qua ngan (khong suy ra duoc chu de 'cmd' anh em) -
    _cmd_topic() tra None, publish_command() phai tra False an toan thay vi
    crash hoac goi client.publish(None, ...)."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_topic", "meas")
    c = _wired_consumer()

    assert c.publish_command("NODE1", {"id": 1}) is False
    assert c._cli.published == []


def test_publish_command_returns_false_on_client_exception(monkeypatch):
    monkeypatch.setattr(config.settings, "mqtt_consumer_topic", "fms/+/meas")
    c = _wired_consumer(client_exc=OSError("connection reset"))

    assert c.publish_command("NODE1", {"id": 1}) is False
    assert c.stats["cmd_sent"] == 0


def test_publish_command_returns_false_when_broker_rc_not_success(monkeypatch):
    """Regression: rc loi THUONG (khac ca SUCCESS lan NO_CONN) van phai tra
    False - xac nhan rc=1 (MQTT_ERR_NOMEM) o day KHONG PHAI truong hop dac
    biet NO_CONN=4 (xem test_publish_command_returns_true_and_counts_as_sent_
    when_broker_rc_is_no_conn ngay duoi, phan biet 2 nhanh)."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_topic", "fms/+/meas")
    assert 1 != mqtt_consumer.mqtt.MQTT_ERR_NO_CONN   # xac nhan rc dung o day KHONG phai NO_CONN
    c = _wired_consumer(client_rc=1)   # != mqtt_consumer.mqtt.MQTT_ERR_SUCCESS (0)

    assert c.publish_command("NODE1", {"id": 1}) is False
    assert c.stats["cmd_sent"] == 0


def test_publish_command_returns_false_when_broker_rc_is_queue_size_error(monkeypatch):
    """Regression bo sung: mot rc loi KHAC nua (MQTT_ERR_QUEUE_SIZE=15, khac
    han rc=1 o test tren) cung phai roi vao nhanh False thuong, KHONG duoc
    an nham vao nhanh dac biet NO_CONN=4 chi vi "khac SUCCESS"."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_topic", "fms/+/meas")
    c = _wired_consumer(client_rc=mqtt_consumer.mqtt.MQTT_ERR_QUEUE_SIZE)

    assert c.publish_command("NODE1", {"id": 1}) is False
    assert c.stats["cmd_sent"] == 0
    assert len(c.traffic) == 0


def test_publish_command_returns_true_and_counts_via_cmd_sent_no_conn_when_broker_rc_is_no_conn(monkeypatch):
    """Case moi (fix-mqtt-no-conn-race): rc == MQTT_ERR_NO_CONN la truong hop
    DAC BIET cua paho-mqtt - message van duoc GIU trong self._out_messages va
    TU DONG republish khi reconnect (da doc source paho-mqtt that, xem
    docstring publish_command() trong production code). publish_command()
    phai tra True (coi nhu da "giao" cho paho tu gui lai, KHONG phai da gui
    that toi broker) de manager.queue_command() tiep tuc cho ACK that thay vi
    bao loi chac chan ngay - neu ACK khong toi kip se tu roi vao nhanh
    timeout ("status": "unknown", xem test_manager_queue_command.py::
    test_queue_command_real_timeout_sets_ok_false_and_status_unknown).

    Update 2026-09-24 (Nam yeu cau, finding tach counter tu review truoc):
    nhanh NO_CONN dem vao counter RIENG "cmd_sent_no_conn", KHONG con gop
    chung vao "cmd_sent" nua - de /ops phan biet duoc lenh da gui THAT xong
    (rc=SUCCESS) voi lenh dang cho paho tu gui lai (rc=NO_CONN)."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_topic", "fms/+/meas")
    c = _wired_consumer(client_rc=mqtt_consumer.mqtt.MQTT_ERR_NO_CONN)

    ok = c.publish_command("NODE1", {"id": 1, "cmd": "write", "channel": "relay_red", "value": 1})

    assert ok is True
    assert c.stats["cmd_sent_no_conn"] == 1
    assert c.stats["cmd_sent"] == 0   # KHONG con tang nham counter "da gui that" cu
    # publish() van duoc GOI (khac voi cac nhanh "fails_when_..." o tren, noi
    # publish() khong bao gio duoc goi) - chi RESULT cua no la NO_CONN.
    assert len(c._cli.published) == 1
    assert len(c.traffic) == 1 and c.traffic[0]["dir"] == "down"
    assert "gui lai" in c.traffic[0]["note"] or "mat ket noi" in c.traffic[0]["note"]
