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
    MqttConsumer, _ack_topic, _cmd_topic, _parse_topic, _split_url, _topic_sibling,
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
    def __init__(self):
        self.touched = []
        self.acked = []

    def touch_node(self, serial):
        self.touched.append(serial)

    def node_ack_command(self, cmd_id, ok, detail=""):
        self.acked.append((cmd_id, ok, detail))


class _FakeAgent:
    def __init__(self):
        self.manager = _FakeAgentManager()
        self.readings = []

    def push_node_reading(self, serial, ch, v, s, q, ts, stable):
        self.readings.append((serial, ch, v, s, q, ts, stable))


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
