# -*- coding: utf-8 -*-
"""Test SourceManager.queue_command() - patch MQTT gộp 8/9 file merge từ
production (site 192.168.5.190). Trước patch, queue_command() CHỈ biết một
đường: xếp vào _node_queues cho node tự poll (/node/v1/commands). Sau patch,
nó thử đường MQTT (self.mqtt_cmd, gán bởi scheduler.py từ MqttConsumer) TRƯỚC,
và chỉ rơi về hàng đợi poll cũ khi không có đường MQTT hoặc node chưa khai
capability - xem chú thích manager.py::queue_command().

Dùng FAKE object đơn giản cho `mqtt_cmd` (không import mqtt_consumer.py thật)
- test này chỉ xác nhận SourceManager ĐIỀU HƯỚNG đúng, không xác nhận logic
nội bộ của MqttConsumer.publish_command() (xem tests/test_mqtt_consumer.py)."""
import asyncio

import pytest

from edge_collector.manager import SourceManager


class _FakeMqttCmd:
    """Giả lập MqttConsumer ở mức tối thiểu queue_command() cần tới: .caps
    (dict serial -> bool, node tự khai qua chủ đề status) và publish_command()
    trả True/False đúng như MqttConsumer thật."""

    def __init__(self, publish_result: bool, caps: dict = None):
        self.caps = caps or {}
        self._publish_result = publish_result
        self.published = []          # [(serial, payload), ...]

    def publish_command(self, serial, payload):
        self.published.append((serial, payload))
        return self._publish_result


def _manager():
    return SourceManager(on_value=lambda *a: None)


async def _queue_and_ack(manager, serial="NODE1", ch="relay_red", cmd="write",
                          value=1, extra=None, ack_ok=True, ack_detail=""):
    """Bắt đầu queue_command() như một task, cho nó chạy tới điểm await đầu
    tiên (asyncio.wait_for(fut)) rồi tự ack ngay (mô phỏng cmdack tới từ
    broker/node) - tránh phải chờ hết `timeout` mặc định 8s trong test."""
    task = asyncio.ensure_future(
        manager.queue_command(serial, ch, cmd, value, extra=extra))
    await asyncio.sleep(0)
    # cmd_id là _node_cmd_seq HIỆN TẠI (queue_command đã tăng trước khi await).
    cmd_id = manager._node_cmd_seq
    acked = manager.node_ack_command(cmd_id, ack_ok, ack_detail)
    result = await task
    return result, acked


def test_queue_command_uses_mqtt_when_publish_succeeds_and_skips_poll_queue():
    """Case 1: mqtt_cmd.publish_command() trả True -> KHÔNG được xếp vào
    hàng đợi poll cũ (_node_queues), và payload đẩy đi đúng 4 khóa cơ bản."""
    manager = _manager()
    fake_mqtt = _FakeMqttCmd(publish_result=True, caps={"NODE1": True})
    manager.mqtt_cmd = fake_mqtt

    result, acked = asyncio.run(_queue_and_ack(manager, value=1))

    assert acked is True
    assert result == {"ok": True, "status": "ok", "error": None}
    assert result["status"] != "unknown"   # ACK đến kịp thời -> không phải "unknown"
    assert len(fake_mqtt.published) == 1
    sent_serial, payload = fake_mqtt.published[0]
    assert sent_serial == "NODE1"
    assert payload["channel"] == "relay_red" and payload["cmd"] == "write" and payload["value"] == 1
    # KHÔNG được tạo hàng đợi poll cho node này - đường MQTT đã phục vụ xong.
    assert manager._node_queues.get("NODE1") is None


def test_queue_command_merges_extra_into_mqtt_payload():
    manager = _manager()
    fake_mqtt = _FakeMqttCmd(publish_result=True, caps={"NODE1": True})
    manager.mqtt_cmd = fake_mqtt

    asyncio.run(_queue_and_ack(manager, cmd="blink", value=None,
                                extra={"ms": 30000, "period_ms": 500}))

    _, payload = fake_mqtt.published[0]
    assert payload["ms"] == 30000
    assert payload["period_ms"] == 500


def test_queue_command_returns_error_immediately_when_node_has_caps_but_publish_fails():
    """Case 2: node ĐÃ khai nhận lệnh qua MQTT (caps=True) nhưng publish thất
    bại (rớt mạng/broker) -> trả lỗi thật NGAY, KHÔNG được rơi vào hàng đợi
    poll (firmware đã tắt HTTP, lệnh sẽ nằm đó mãi mãi - xem chú thích
    manager.py::queue_command())."""
    manager = _manager()
    fake_mqtt = _FakeMqttCmd(publish_result=False, caps={"NODE1": True})
    manager.mqtt_cmd = fake_mqtt

    async def _run():
        return await manager.queue_command("NODE1", "relay_red", "write", 1, timeout=5.0)

    result = asyncio.run(_run())

    assert result["ok"] is False
    assert "NODE1" in result["error"]
    # Lỗi CHẮC CHẮN đã biết ngay (nhánh publish-fail-với-caps, KHÔNG đi qua
    # timeout) -> khác với các nhánh khác, nhánh này KHÔNG đặt key "status"
    # trong dict trả về (đã verify thực nghiệm trên source thật: chỉ có "ok"
    # và "error"). Dùng .get() để vừa an toàn vừa đúng ngữ nghĩa: dù có key
    # hay không, giá trị KHÔNG được là "unknown".
    assert result.get("status") != "unknown"
    # KHÔNG rơi vào hàng đợi poll cũ.
    assert manager._node_queues.get("NODE1") is None
    # Future phải được dọn ngay (không rò rỉ bộ nhớ cho lệnh sẽ không bao giờ
    # được ack, vì không còn ai poll /node/v1/commands nữa).
    assert manager._node_futures == {}


def test_queue_command_falls_back_to_poll_queue_when_no_mqtt_cmd_attribute():
    """Case 3a: không có self.mqtt_cmd (vd scheduler.py chưa gán, hoặc test
    trực tiếp SourceManager) -> hành vi Y HỆT trước patch: xếp vào hàng đợi
    poll cũ cho node tự lấy qua node_pull_command()."""
    manager = _manager()
    assert not hasattr(manager, "mqtt_cmd")

    async def _run():
        task = asyncio.ensure_future(
            manager.queue_command("NODE1", "relay_red", "zero", None, timeout=0.05))
        await asyncio.sleep(0)
        payload = manager.node_pull_command("NODE1")
        result = await task
        return payload, result

    payload, result = asyncio.run(_run())

    assert payload is not None
    assert payload["channel"] == "relay_red" and payload["cmd"] == "zero"
    # Không ai ack (mô phỏng firmware cũ không trả lời qua MQTT) -> timeout.
    assert result["ok"] is False
    assert "không trả lời" in result["error"]
    # Timeout THẬT (không ai gọi node_ack_command) -> phải có "status": "unknown"
    # để caller (pcm_edge._command / Tags.tsx) phân biệt được với lỗi chắc chắn.
    assert result["status"] == "unknown"


def test_queue_command_falls_back_to_poll_queue_when_node_has_not_declared_mqtt_caps():
    """Case 3b: có mqtt_cmd nhưng node CHƯA khai capability (caps rỗng) ->
    publish_command() vẫn được GỌI (mô phỏng đúng MqttConsumer thật: nó tự
    trả False khi không biết serial), rồi manager tự quay về hàng đợi poll -
    đúng behavior trước patch, không phải lỗi."""
    manager = _manager()
    fake_mqtt = _FakeMqttCmd(publish_result=False, caps={})   # node chua khai
    manager.mqtt_cmd = fake_mqtt

    async def _run():
        task = asyncio.ensure_future(
            manager.queue_command("NODE1", "relay_red", "zero", None, timeout=0.05))
        await asyncio.sleep(0)
        payload = manager.node_pull_command("NODE1")
        result = await task
        return payload, result

    payload, result = asyncio.run(_run())

    assert len(fake_mqtt.published) == 1        # đã THỬ qua MQTT trước
    assert payload is not None                   # nhưng cuối cùng vẫn xếp hàng đợi poll
    assert result["ok"] is False                 # không ai ack -> timeout
    assert result["status"] == "unknown"          # timeout thật -> status = "unknown"


def test_queue_command_ack_ok_false_status_is_error_not_unknown():
    """Regression cho giá trị thứ 3 "unknown" của field "status" (phối hợp
    2026-09-24, dùng LẠI field "status" có sẵn thay vì thêm field "unknown"
    riêng - Odoo/Tags.tsx đã whitelist sẵn "status"): ACK đến KỊP THỜI nhưng
    node tự báo thất bại (vd relay kẹt, không đổi được trạng thái) -> đây là
    lỗi CHẮC CHẮN, node_ack_command() đặt "status": "error" (không phải
    "unknown" - giá trị đó CHỈ dành cho nhánh timeout). Rò rỉ "unknown" sang
    đây sẽ làm người vận hành hiểu nhầm một lỗi rõ ràng thành "có thể đã
    thực thi", sai UX nghiêm trọng hơn cả bug gốc."""
    manager = _manager()
    fake_mqtt = _FakeMqttCmd(publish_result=True, caps={"NODE1": True})
    manager.mqtt_cmd = fake_mqtt

    result, acked = asyncio.run(
        _queue_and_ack(manager, ack_ok=False, ack_detail="relay ket"))

    assert acked is True
    assert result == {"ok": False, "status": "error", "error": "relay ket"}
    assert result["status"] == "error"
    assert result["status"] != "unknown"


def test_queue_command_real_timeout_sets_ok_false_and_status_unknown():
    """Regression tập trung cho đúng giá trị "status": "unknown" (phối hợp
    firmware ESP32 + Odoo 2026-09-24): khi node KHÔNG BAO GIỜ gọi
    node_ack_command() cho cmd_id đang chờ (khác với các test trước, ở đây
    KHÔNG có pull/ack gì cả - mô phỏng đúng tình huống gốc: ACK bị mất mạng)
    -> cả hai phải đúng CÙNG LÚC: "ok": False (tương thích ngược) VÀ
    "status": "unknown" (tái sử dụng field "status" đã được pcm_base/Tags.tsx
    whitelist sẵn, thay vì field "unknown" riêng sẽ bị lọc mất phía Odoo)."""
    manager = _manager()
    fake_mqtt = _FakeMqttCmd(publish_result=True, caps={"NODE1": True})
    manager.mqtt_cmd = fake_mqtt

    async def _run():
        # timeout NGẮN để test không phải chờ 8s mặc định; không ai
        # node_pull_command()/node_ack_command() -> chắc chắn rơi vào
        # nhánh asyncio.TimeoutError.
        return await manager.queue_command(
            "NODE1", "relay_red", "write", 1, timeout=0.05)

    result = asyncio.run(_run())

    assert result["ok"] is False
    assert result["status"] == "unknown"
    assert "không trả lời" in result["error"]
    # Future phải được dọn sau timeout, không rò rỉ bộ nhớ.
    assert manager._node_futures == {}


def test_queue_command_extra_none_does_not_pollute_payload():
    """extra=None (mặc định, giá trị cũ trước patch) không được thêm khóa lạ
    vào payload - regression cho hành vi trước patch khi API /api/command cũ
    (không có "extra") vẫn gọi queue_command()."""
    manager = _manager()
    fake_mqtt = _FakeMqttCmd(publish_result=True, caps={"NODE1": True})
    manager.mqtt_cmd = fake_mqtt

    asyncio.run(_queue_and_ack(manager, extra=None))

    _, payload = fake_mqtt.published[0]
    assert set(payload.keys()) == {"id", "channel", "cmd", "value"}
