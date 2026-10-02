# -*- coding: utf-8 -*-
"""Test edge_collector/mqtt_consumer.py (MỚI, patch MQTT merge từ production
192.168.5.190) - CHỈ phần logic thuần/gọi lại qua object giả, KHÔNG dùng
broker MQTT thật (không có broker giả lập sẵn trong project, khác
drivers/sim.py vốn có sẵn một driver 'sim' cho luồng Modbus/OPC-UA).

Scope: _parse_topic()/_split_url()/_topic_sibling() (+ _cmd_topic/_ack_topic)
là hàm thuần túy; MqttConsumer._handle() và .publish_command() là method gọi
được TRỰC TIẾP (không async, không cần vòng lặp paho thật) miễn là tự cấp
`self._cli`/`self._connected`/`self.caps`/`self.stats["online"]` trước -
đây chính là 2 điểm nối với manager.queue_command() (xem
tests/test_manager_queue_command.py cho phía SourceManager) và với
scheduler.push_node_reading()/manager.node_ack_command() (xem dưới).

start()/stop() (cần paho.mqtt.client.Client thật kết nối mạng thật) BỊ SKIP -
xem 'Scope đã KHÔNG cover' trong báo cáo cuối."""
import json

import pytest

import edge_collector.config as config
import edge_collector.mqtt_consumer as mqtt_consumer
from edge_collector.mqtt_consumer import (
    MqttConsumer, _ack_topic, _canonical, _cmd_topic, _parse_topic, _sign,
    _split_url, _topic_sibling, _verify_sig,
)


# --- hàm thuần túy -----------------------------------------------------


@pytest.mark.parametrize("topic,expected", [
    ("fms/68EE8F4F06A8/meas", ("68EE8F4F06A8", "meas")),
    ("fms/NODE-01/status", ("NODE-01", "status")),
    ("fms/NODE-01/cmdack", ("NODE-01", "cmdack")),
    ("meas", (None, None)),                 # thiếu segment - không thể tách serial
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
    ("192.168.5.190", ("192.168.5.190", 1883)),          # không có port -> mặc định 1883
    ("", ("127.0.0.1", 1883)),                           # rỗng -> mặc định an toàn
    ("mqtt://broker.local:abc", ("broker.local", 1883)),  # port không phải số -> fallback
])
def test_split_url(url, expected):
    assert _split_url(url) == expected


def test_topic_sibling_and_derived_topics(monkeypatch):
    monkeypatch.setattr(config.settings, "mqtt_consumer_topic", "fms/+/meas")

    assert _topic_sibling("cmd") == "fms/+/cmd"
    assert _ack_topic() == "fms/+/cmdack"
    assert _cmd_topic("NODE1") == "fms/NODE1/cmd"


def test_topic_sibling_returns_none_when_pattern_too_short(monkeypatch):
    """Mẫu chủ đề tùy chỉnh chỉ có 1 segment - không đủ để suy ra chủ đề anh
    em (cmd/cmdack), phải trả None an toàn thay vì crash/sinh chủ đề sai."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_topic", "meas")

    assert _topic_sibling("cmd") is None
    assert _cmd_topic("NODE1") is None
    # _ack_topic() có fallback cứng "fms/+/cmdack" khi không suy ra được.
    assert _ack_topic() == "fms/+/cmdack"


def test_cmd_topic_returns_none_when_serial_contains_wildcard_char():
    """Serial không bao giờ chứa '+'/'#' trong thực tế, nhưng hàm phải an
    toàn (trả None) nếu thế - gửi lệnh xuống một chủ đề còn là wildcard sẽ
    làm broker từ chối hoặc phát rộng khắp nơi không ai mong đợi."""
    assert _cmd_topic("+") is None


# --- MqttConsumer._handle() ---------------------------------------------


class _FakeAgentManager:
    def __init__(self, api_key=None):
        self.touched = []
        self.acked = []
        self.acked_serials = []
        self._api_key = api_key

    def touch_node(self, serial):
        self.touched.append(serial)

    def node_ack_command(self, cmd_id, ok, detail="", *, serial):
        # serial BẮT BUỘC keyword (khớp chữ ký thật manager.node_ack_command).
        self.acked.append((cmd_id, ok, detail))
        self.acked_serials.append(serial)

    def cached_node_api_key(self, serial):
        """Giả lập Odoo đã cấp (hoặc chưa cấp, trả None) api_key cho serial -
        dùng cho test xác minh HMAC trong _handle()."""
        return self._api_key


class _FakeAgent:
    def __init__(self, api_key=None):
        self.manager = _FakeAgentManager(api_key=api_key)
        self.readings = []

    def push_node_reading(self, serial, ch, v, s, q, ts, stable):
        self.readings.append((serial, ch, v, s, q, ts, stable))


def _signed(api_key, payload):
    """payload (KHÔNG có 'sig') -> bytes JSON đã gắn thêm 'sig' đúng cho
    đúng api_key đó - dùng để dựng test giống như node_agent thật (mqtt_
    uplink.py) sẽ làm."""
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
    """Topic không tách được serial (vd < 3 segment) - _handle() phải return
    sớm, KHÔNG đếm vào stats['bad'] (đây không phải gói tin hỏng, là topic
    không khớp mẫu mình đang nghe)."""
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
    """'Báo khả năng nhận lệnh qua MQTT là thuộc tính của FIRMWARE, còn sống hay
    chết là chuyện khác' (xem docstring _handle()) - node rớt mạng (offline,
    không 'cmd') KHÔNG được xóa self.caps[serial] đã True trước đó, nếu
    không manager sẽ tưởng đây là firmware cũ và xếp lệnh vào hàng đợi poll
    mà không ai còn poll nữa."""
    consumer = MqttConsumer(_FakeAgent())
    consumer._handle("fms/NODE1/status", json.dumps({"online": True, "cmd": True}).encode())
    assert consumer.caps["NODE1"] is True

    consumer._handle("fms/NODE1/status", json.dumps({"online": False}).encode())

    assert consumer.stats["online"]["NODE1"] is False
    assert consumer.caps["NODE1"] is True   # KHÔNG bị xóa


def test_handle_cmdack_message_forwards_to_manager_node_ack_command(consumer):
    consumer._handle("fms/NODE1/cmdack",
                      json.dumps({"id": 5, "ok": True}).encode())

    assert consumer.stats["cmd_acked"] == 1
    assert consumer._agent.manager.acked == [(5, True, "")]
    # serial lấy từ TOPIC (fms/<serial>/cmdack), chuyển xuống để manager đối
    # chiếu chủ lệnh - chống node A ack lệnh của node B.
    assert consumer._agent.manager.acked_serials == ["NODE1"]


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
    """Dấu thời gian trước EPOCH_SANE_S (đồng hồ node chưa đồng bộ, vd mất
    SNTP) phải bị bỏ (ts_s=None) và đếm vào ts_dropped, KHÔNG được chuyển
    thẳng vào Odoo thành năm 1970. ts_dropped tính TRƯỚC nhánh gate
    mqtt_consumer_forward nên phải đúng cả khi forward đang tắt (mặc định)."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_forward", True)
    consumer._handle("fms/NODE1/meas", json.dumps({
        "items": [{"ch": "temp", "v": 1, "ts": 1000}],  # 1000 ms = gần epoch 0
    }).encode())

    assert consumer.stats["ts_dropped"] == 1
    ts_arg = consumer._agent.readings[0][5]
    assert ts_arg is None


def test_handle_measurement_drops_epoch_insane_timestamp_even_when_forward_disabled(consumer):
    assert config.settings.mqtt_consumer_forward is False   # baseline mặc định, không monkeypatch

    consumer._handle("fms/NODE1/meas", json.dumps({
        "items": [{"ch": "temp", "v": 1, "ts": 1000}],
    }).encode())

    assert consumer.stats["ts_dropped"] == 1


def test_handle_measurement_does_not_forward_when_consumer_forward_disabled(consumer, monkeypatch):
    """EDGE_MQTT_CONSUMER_FORWARD=false (mặc định) - CHỈ ĐẾM (chế độ 'bóng'),
    không được gọi push_node_reading() (tránh Odoo nhận đôi khi node còn
    phát cả HTTP lẫn MQTT cùng lúc - xem docstring đầu file)."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_forward", False)

    consumer._handle("fms/NODE1/meas", json.dumps({
        "items": [{"ch": "temp", "v": 21.5}],
    }).encode())

    assert consumer._agent.readings == []
    assert consumer.stats["items"] == 1        # vẫn đếm số bản ghi thấy được
    assert consumer.stats["forwarded"] == 0     # nhưng không đẩy đi đâu


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
    """Regression: self.lamps[...] (dùng cho trang /ops '4 đèn') PHẢI cập
    nhật độc lập với EDGE_MQTT_CONSUMER_FORWARD (mặc định false trong giai
    đoạn node còn phát cả HTTP lẫn MQTT - xem settings_api.py hint 'Keep
    false while a node still sends...') - trang /ops phải xem được đúng
    trạng thái vật lý ngay cả khi chưa bật forward-vào-Odoo (xem docstring
    đầu ops_api.py: 'trang này phải xem được đúng lúc Odoo hỏng'). Ban đầu
    (trước fix) self.lamps[...] nằm SAU nhánh 'if not
    settings.mqtt_consumer_forward: continue' nên /ops sẽ không bao giờ
    thấy đèn cập nhật trong chế độ forward=false - đã được dời vị trí trong
    mqtt_consumer.py::_handle() để tách khỏi gate đó."""
    consumer = MqttConsumer(_FakeAgent())
    assert config.settings.mqtt_consumer_forward is False   # baseline mặc định

    consumer._handle("fms/NODE1/meas", json.dumps({
        "items": [{"ch": "relay_red", "v": 1}],
    }).encode())

    assert consumer.stats["items"] == 1
    assert consumer.lamps["relay_red"]["v"] == 1
    assert consumer.stats["forwarded"] == 0   # forward vào Odoo vẫn tắt, đúng thiết kế


# --- HMAC: _canonical()/_sign()/_verify_sig() (hàm thuần túy) ------------


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
    """_canonical() dùng sort_keys=True nên thứ tự field trong dict GỐC
    (trước khi ký/xác minh) không được ảnh hưởng kết quả - node/edge có thể
    serialize object theo thứ tự bất kỳ, miễn nội dung field giống nhau."""
    payload_signed_as = {"ch": "temp", "v": 1, "ts": 123}
    payload_received_as = {"ts": 123, "v": 1, "ch": "temp"}
    sig = _sign("secret123", payload_signed_as)
    data = dict(payload_received_as, sig=sig)

    assert _verify_sig("secret123", data) is True


def test_verify_sig_rejects_payload_tampered_after_signing():
    payload = {"ch": "temp", "v": 21.5}
    sig = _sign("secret123", payload)
    tampered = {"ch": "temp", "v": 999.0, "sig": sig}   # đổi 1 field sau khi ký

    assert _verify_sig("secret123", tampered) is False


def test_verify_sig_rejects_signature_made_with_wrong_key():
    payload = {"ch": "temp", "v": 21.5}
    data = dict(payload, sig=_sign("attacker-key", payload))

    assert _verify_sig("secret123", data) is False


def test_verify_sig_rejects_missing_sig_field():
    assert _verify_sig("secret123", {"ch": "temp", "v": 21.5}) is False


def test_verify_sig_rejects_non_string_sig():
    assert _verify_sig("secret123", {"ch": "temp", "sig": 12345}) is False


# --- MqttConsumer._handle() + xác minh HMAC (tích hợp qua object giả) ---


def test_handle_measurement_without_sig_is_processed_as_before(monkeypatch):
    """Tương thích ngược: ESP32 firmware cũ không biết ký, không gửi 'sig' -
    PHẢI vẫn xử lý bình thường dù Odoo ĐÃ cấp api_key cho serial này (không
    ép buộc ký chỉ vì đã có key cache)."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_forward", True)
    consumer = MqttConsumer(_FakeAgent(api_key="secret123"))  # secret-allow: test fixture, không phải credential thật

    consumer._handle("fms/NODE1/meas", json.dumps({
        "items": [{"ch": "temp", "v": 21.5}],
    }).encode())

    assert consumer.stats["sig_rejected"] == 0
    assert consumer._agent.readings[0][:2] == ("NODE1", "temp")


def test_handle_measurement_with_valid_sig_is_forwarded(monkeypatch):
    monkeypatch.setattr(config.settings, "mqtt_consumer_forward", True)
    consumer = MqttConsumer(_FakeAgent(api_key="secret123"))  # secret-allow: test fixture, không phải credential thật
    payload = {"items": [{"ch": "temp", "v": 21.5}]}

    consumer._handle("fms/NODE1/meas", _signed("secret123", payload))

    assert consumer.stats["sig_rejected"] == 0
    assert consumer._agent.readings[0][:2] == ("NODE1", "temp")
    assert consumer.stats["forwarded"] == 1


def test_handle_measurement_with_invalid_sig_is_rejected_and_not_forwarded(monkeypatch):
    """Đổi 1 giá trị trong payload SAU khi đã ký (giả mạo) - sig cũ không còn
    khớp, phải bị từ chối TRƯỚC cả touch_node()/push_node_reading()."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_forward", True)
    consumer = MqttConsumer(_FakeAgent(api_key="secret123"))  # secret-allow: test fixture, không phải credential thật
    payload = {"items": [{"ch": "temp", "v": 21.5}]}
    signed = json.loads(_signed("secret123", payload))
    signed["items"][0]["v"] = 999.0

    consumer._handle("fms/NODE1/meas", json.dumps(signed).encode())

    assert consumer.stats["sig_rejected"] == 1
    assert consumer._agent.readings == []
    assert consumer._agent.manager.touched == []


def test_handle_measurement_with_sig_but_no_cached_api_key_is_rejected(monkeypatch):
    """Odoo chưa cấp api_key cho serial này (cached_node_api_key trả None) -
    không thể xác minh được thì TỪ CHỐI, không có đường hạ tiêu chuẩn."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_forward", True)
    consumer = MqttConsumer(_FakeAgent(api_key=None))
    payload = {"items": [{"ch": "temp", "v": 21.5}]}

    consumer._handle("fms/NODE1/meas", _signed("any-key", payload))

    assert consumer.stats["sig_rejected"] == 1
    assert consumer._agent.readings == []


def test_handle_status_with_valid_sig_is_processed_normally():
    consumer = MqttConsumer(_FakeAgent(api_key="secret123"))  # secret-allow: test fixture, không phải credential thật
    payload = {"online": True, "cmd": True}

    consumer._handle("fms/NODE1/status", _signed("secret123", payload))

    assert consumer.stats["online"]["NODE1"] is True
    assert consumer.caps["NODE1"] is True
    assert consumer.stats["sig_rejected"] == 0


def test_handle_status_with_invalid_sig_is_rejected_and_online_not_updated():
    consumer = MqttConsumer(_FakeAgent(api_key="secret123"))  # secret-allow: test fixture, không phải credential thật
    payload = {"online": True, "cmd": True}
    signed = json.loads(_signed("secret123", payload))
    signed["online"] = False   # giả mạo sau khi ký, sig cũ không còn khớp

    consumer._handle("fms/NODE1/status", json.dumps(signed).encode())

    assert consumer.stats["sig_rejected"] == 1
    assert "NODE1" not in consumer.stats["online"]
    assert "NODE1" not in consumer.caps


def test_handle_status_offline_lwt_without_sig_still_marks_offline():
    """LWT ({"online": false}) do BROKER tự phát khi mất kết nối với node -
    không thể ký được (không phải node chủ động gửi), nên KHÔNG có 'sig'.
    Logic 'sig tùy chọn' phải cho qua bình thường dù serial này ĐÃ có
    api_key cache (không ép buộc ký cho thông điệp broker tự phát)."""
    consumer = MqttConsumer(_FakeAgent(api_key="secret123"))  # secret-allow: test fixture, không phải credential thật

    consumer._handle("fms/NODE1/status", json.dumps({"online": False}).encode())

    assert consumer.stats["online"]["NODE1"] is False
    assert consumer.stats["sig_rejected"] == 0


def test_handle_cmdack_with_valid_sig_is_acked_normally():
    consumer = MqttConsumer(_FakeAgent(api_key="secret123"))  # secret-allow: test fixture, không phải credential thật
    payload = {"id": 5, "ok": True}

    consumer._handle("fms/NODE1/cmdack", _signed("secret123", payload))

    assert consumer._agent.manager.acked == [(5, True, "")]
    assert consumer.stats["sig_rejected"] == 0


def test_handle_cmdack_with_invalid_sig_is_rejected_and_not_acked():
    consumer = MqttConsumer(_FakeAgent(api_key="secret123"))  # secret-allow: test fixture, không phải credential thật
    payload = {"id": 5, "ok": True}
    signed = json.loads(_signed("secret123", payload))
    signed["ok"] = False   # giả mạo kết quả lệnh sau khi ký

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
    # Counter riêng của nhánh NO_CONN (thêm 2026-09-24) KHÔNG được tăng nhầm
    # ở case thành công thường này - xem test_publish_command_returns_true_
    # and_counts_via_cmd_sent_no_conn_when_broker_rc_is_no_conn bên dưới.
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

    c2 = MqttConsumer(_FakeAgent())    # _cli ban đầu là None (chưa start())
    assert c2.publish_command("NODE1", {"id": 1}) is False


def test_publish_command_fails_when_node_has_not_declared_mqtt_capability(monkeypatch):
    monkeypatch.setattr(config.settings, "mqtt_consumer_topic", "fms/+/meas")
    c = _wired_consumer(caps={})   # NODE1 chưa khai "cmd" qua status

    assert c.publish_command("NODE1", {"id": 1}) is False
    assert c._cli.published == []


def test_publish_command_fails_when_node_currently_offline(monkeypatch):
    """Chủ đề lệnh KHÔNG retain và node không dùng phiên bền - gửi cho một
    node đang OFFLINE sẽ bị broker vứt đi mà không ai báo, nên phải trả
    False NGAY thay vì 'đã gửi' rồi im lặng (xem docstring publish_command())."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_topic", "fms/+/meas")
    c = _wired_consumer(online={"NODE1": False})

    assert c.publish_command("NODE1", {"id": 1}) is False
    assert c._cli.published == []


def test_publish_command_fails_when_topic_cannot_be_derived(monkeypatch):
    """Mẫu chủ đề cấu hình quá ngắn (không suy ra được chủ đề 'cmd' anh em) -
    _cmd_topic() trả None, publish_command() phải trả False an toàn thay vì
    crash hoặc gọi client.publish(None, ...)."""
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
    """Regression: rc lỗi THƯỜNG (khác cả SUCCESS lẫn NO_CONN) vẫn phải trả
    False - xác nhận rc=1 (MQTT_ERR_NOMEM) ở đây KHÔNG PHẢI trường hợp đặc
    biệt NO_CONN=4 (xem test_publish_command_returns_true_and_counts_as_sent_
    when_broker_rc_is_no_conn ngay dưới, phân biệt 2 nhánh)."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_topic", "fms/+/meas")
    assert 1 != mqtt_consumer.mqtt.MQTT_ERR_NO_CONN   # xác nhận rc dùng ở đây KHÔNG phải NO_CONN
    c = _wired_consumer(client_rc=1)   # != mqtt_consumer.mqtt.MQTT_ERR_SUCCESS (0)

    assert c.publish_command("NODE1", {"id": 1}) is False
    assert c.stats["cmd_sent"] == 0


def test_publish_command_returns_false_when_broker_rc_is_queue_size_error(monkeypatch):
    """Regression bổ sung: một rc lỗi KHÁC nữa (MQTT_ERR_QUEUE_SIZE=15, khác
    hẳn rc=1 ở test trên) cũng phải rơi vào nhánh False thường, KHÔNG được
    ẩn nhầm vào nhánh đặc biệt NO_CONN=4 chỉ vì "khác SUCCESS"."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_topic", "fms/+/meas")
    c = _wired_consumer(client_rc=mqtt_consumer.mqtt.MQTT_ERR_QUEUE_SIZE)

    assert c.publish_command("NODE1", {"id": 1}) is False
    assert c.stats["cmd_sent"] == 0
    assert len(c.traffic) == 0


def test_publish_command_returns_true_and_counts_via_cmd_sent_no_conn_when_broker_rc_is_no_conn(monkeypatch):
    """Case mới (fix-mqtt-no-conn-race): rc == MQTT_ERR_NO_CONN là trường hợp
    ĐẶC BIỆT của paho-mqtt - message vẫn được GIỮ trong self._out_messages và
    TỰ ĐỘNG republish khi reconnect (đã đọc source paho-mqtt thật, xem
    docstring publish_command() trong production code). publish_command()
    phải trả True (coi như đã "giao" cho paho tự gửi lại, KHÔNG phải đã gửi
    thật tới broker) để manager.queue_command() tiếp tục chờ ACK thật thay vì
    báo lỗi chắc chắn ngay - nếu ACK không tới kịp sẽ tự rơi vào nhánh
    timeout ("status": "unknown", xem test_manager_queue_command.py::
    test_queue_command_real_timeout_sets_ok_false_and_status_unknown).

    Update 2026-09-24 (Nam yêu cầu, finding tách counter từ review trước):
    nhánh NO_CONN đếm vào counter RIÊNG "cmd_sent_no_conn", KHÔNG còn gộp
    chung vào "cmd_sent" nữa - để /ops phân biệt được lệnh đã gửi THẬT xong
    (rc=SUCCESS) với lệnh đang chờ paho tự gửi lại (rc=NO_CONN)."""
    monkeypatch.setattr(config.settings, "mqtt_consumer_topic", "fms/+/meas")
    c = _wired_consumer(client_rc=mqtt_consumer.mqtt.MQTT_ERR_NO_CONN)

    ok = c.publish_command("NODE1", {"id": 1, "cmd": "write", "channel": "relay_red", "value": 1})

    assert ok is True
    assert c.stats["cmd_sent_no_conn"] == 1
    assert c.stats["cmd_sent"] == 0   # KHÔNG còn tăng nhầm counter "đã gửi thật" cũ
    # publish() vẫn được GỌI (khác với các nhánh "fails_when_..." ở trên, nơi
    # publish() không bao giờ được gọi) - chỉ RESULT của nó là NO_CONN.
    assert len(c._cli.published) == 1
    assert len(c.traffic) == 1 and c.traffic[0]["dir"] == "down"
    assert "gui lai" in c.traffic[0]["note"] or "mat ket noi" in c.traffic[0]["note"]
