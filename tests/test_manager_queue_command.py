# -*- coding: utf-8 -*-
"""Test SourceManager.queue_command() - patch MQTT gop 8/9 file merge tu
production (site 192.168.5.190). Truoc patch, queue_command() CHI biet mot
duong: xep vao _node_queues cho node tu poll (/node/v1/commands). Sau patch,
no thu duong MQTT (self.mqtt_cmd, gan boi scheduler.py tu MqttConsumer) TRUOC,
va chi roi ve hang doi poll cu khi khong co duong MQTT hoac node chua khai
capability - xem chu thich manager.py::queue_command().

Dung FAKE object don gian cho `mqtt_cmd` (khong import mqtt_consumer.py that)
- test nay chi xac nhan SourceManager DIEU HUONG dung, khong xac nhan logic
noi bo cua MqttConsumer.publish_command() (xem tests/test_mqtt_consumer.py)."""
import asyncio

import pytest

from edge_collector.manager import SourceManager


class _FakeMqttCmd:
    """Gia lap MqttConsumer o muc toi thieu queue_command() can toi: .caps
    (dict serial -> bool, node tu khai qua chu de status) va publish_command()
    tra True/False dung nhu MqttConsumer that."""

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
    """Bat dau queue_command() nhu mot task, cho no chay toi diem await dau
    tien (asyncio.wait_for(fut)) roi tu ack ngay (mo phong cmdack toi tu
    broker/node) - tranh phai cho het `timeout` mac dinh 8s trong test."""
    task = asyncio.ensure_future(
        manager.queue_command(serial, ch, cmd, value, extra=extra))
    await asyncio.sleep(0)
    # cmd_id la _node_cmd_seq HIEN TAI (queue_command da tang truoc khi await).
    cmd_id = manager._node_cmd_seq
    acked = manager.node_ack_command(cmd_id, ack_ok, ack_detail)
    result = await task
    return result, acked


def test_queue_command_uses_mqtt_when_publish_succeeds_and_skips_poll_queue():
    """Case 1: mqtt_cmd.publish_command() tra True -> KHONG duoc xep vao
    hang doi poll cu (_node_queues), va payload day di dung 4 khoa co ban."""
    manager = _manager()
    fake_mqtt = _FakeMqttCmd(publish_result=True, caps={"NODE1": True})
    manager.mqtt_cmd = fake_mqtt

    result, acked = asyncio.run(_queue_and_ack(manager, value=1))

    assert acked is True
    assert result == {"ok": True, "status": "ok", "error": None}
    assert result["status"] != "unknown"   # ACK den kip thoi -> khong phai "unknown"
    assert len(fake_mqtt.published) == 1
    sent_serial, payload = fake_mqtt.published[0]
    assert sent_serial == "NODE1"
    assert payload["channel"] == "relay_red" and payload["cmd"] == "write" and payload["value"] == 1
    # KHONG duoc tao hang doi poll cho node nay - duong MQTT da phuc vu xong.
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
    """Case 2: node DA khai nhan lenh qua MQTT (caps=True) nhung publish that
    bai (rot mang/broker) -> tra loi that NGAY, KHONG duoc roi vao hang doi
    poll (firmware da tat HTTP, lenh se nam do mai mai - xem chu thich
    manager.py::queue_command())."""
    manager = _manager()
    fake_mqtt = _FakeMqttCmd(publish_result=False, caps={"NODE1": True})
    manager.mqtt_cmd = fake_mqtt

    async def _run():
        return await manager.queue_command("NODE1", "relay_red", "write", 1, timeout=5.0)

    result = asyncio.run(_run())

    assert result["ok"] is False
    assert "NODE1" in result["error"]
    # Loi CHAC CHAN da biet ngay (nhanh publish-fail-voi-caps, KHONG di qua
    # timeout) -> khac voi cac nhanh khac, nhanh nay KHONG dat key "status"
    # trong dict tra ve (da verify thuc nghiem tren source that: chi co "ok"
    # va "error"). Dung .get() de vua an toan vua dung ngu nghia: du co key
    # hay khong, gia tri KHONG duoc la "unknown".
    assert result.get("status") != "unknown"
    # KHONG roi vao hang doi poll cu.
    assert manager._node_queues.get("NODE1") is None
    # Future phai duoc don ngay (khong ro ri bo nho cho lenh se khong bao gio
    # duoc ack, vi khong con ai poll /node/v1/commands nua).
    assert manager._node_futures == {}


def test_queue_command_falls_back_to_poll_queue_when_no_mqtt_cmd_attribute():
    """Case 3a: khong co self.mqtt_cmd (vd scheduler.py chua gan, hoac test
    truc tiep SourceManager) -> hanh vi Y HET truoc patch: xep vao hang doi
    poll cu cho node tu lay qua node_pull_command()."""
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
    # Khong ai ack (mo phong firmware cu khong tra loi qua MQTT) -> timeout.
    assert result["ok"] is False
    assert "khong tra loi" in result["error"]
    # Timeout THAT (khong ai goi node_ack_command) -> phai co "status": "unknown"
    # de caller (pcm_edge._command / Tags.tsx) phan biet duoc voi loi chac chan.
    assert result["status"] == "unknown"


def test_queue_command_falls_back_to_poll_queue_when_node_has_not_declared_mqtt_caps():
    """Case 3b: co mqtt_cmd nhung node CHUA khai capability (caps rong) ->
    publish_command() van duoc GOI (mo phong dung MqttConsumer that: no tu
    tra False khi khong biet serial), roi manager tu quay ve hang doi poll -
    dung behavior truoc patch, khong phai loi."""
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

    assert len(fake_mqtt.published) == 1        # da THU qua MQTT truoc
    assert payload is not None                   # nhung cuoi cung van xep hang doi poll
    assert result["ok"] is False                 # khong ai ack -> timeout
    assert result["status"] == "unknown"          # timeout that -> status = "unknown"


def test_queue_command_ack_ok_false_status_is_error_not_unknown():
    """Regression cho gia tri thu 3 "unknown" cua field "status" (phoi hop
    2026-09-24, dung LAI field "status" co san thay vi them field "unknown"
    rieng - Odoo/Tags.tsx da whitelist san "status"): ACK den KIP THOI nhung
    node tu bao that bai (vd relay ket, khong doi duoc trang thai) -> day la
    loi CHAC CHAN, node_ack_command() dat "status": "error" (khong phai
    "unknown" - gia tri do CHI danh cho nhanh timeout). Ro ri "unknown" sang
    day se lam nguoi van hanh hieu nham mot loi ro rang thanh "co the da
    thuc thi", sai UX nghiem trong hon ca bug goc."""
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
    """Regression tap trung cho dung gia tri "status": "unknown" (phoi hop
    firmware ESP32 + Odoo 2026-09-24): khi node KHONG BAO GIO goi
    node_ack_command() cho cmd_id dang cho (khac voi cac test truoc, o day
    KHONG co pull/ack gi ca - mo phong dung tinh huong goc: ACK bi mat mang)
    -> ca hai phai dung CUNG LUC: "ok": False (tuong thich nguoc) VA
    "status": "unknown" (tai su dung field "status" da duoc pcm_base/Tags.tsx
    whitelist san, thay vi field "unknown" rieng se bi loc mat phia Odoo)."""
    manager = _manager()
    fake_mqtt = _FakeMqttCmd(publish_result=True, caps={"NODE1": True})
    manager.mqtt_cmd = fake_mqtt

    async def _run():
        # timeout NGAN de test khong phai cho 8s mac dinh; khong ai
        # node_pull_command()/node_ack_command() -> chac chan roi vao
        # nhanh asyncio.TimeoutError.
        return await manager.queue_command(
            "NODE1", "relay_red", "write", 1, timeout=0.05)

    result = asyncio.run(_run())

    assert result["ok"] is False
    assert result["status"] == "unknown"
    assert "khong tra loi" in result["error"]
    # Future phai duoc don sau timeout, khong ro ri bo nho.
    assert manager._node_futures == {}


def test_queue_command_extra_none_does_not_pollute_payload():
    """extra=None (mac dinh, gia tri cu truoc patch) khong duoc them khoa la
    vao payload - regression cho hanh vi truoc patch khi API /api/command cu
    (khong co "extra") van goi queue_command()."""
    manager = _manager()
    fake_mqtt = _FakeMqttCmd(publish_result=True, caps={"NODE1": True})
    manager.mqtt_cmd = fake_mqtt

    asyncio.run(_queue_and_ack(manager, extra=None))

    _, payload = fake_mqtt.published[0]
    assert set(payload.keys()) == {"id", "channel", "cmd", "value"}
