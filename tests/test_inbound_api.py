# -*- coding: utf-8 -*-
"""Test ring-buffer 'PCM requests' (edge_collector/inbound_api.py) -
_log_request()/recent_requests() phục vụ panel /setup/pcm_requests
(settings_api.py), đối xứng NGƯỢC CHIỀU với Store.history_recent()
(node_agent -> edge: đây là Odoo Main -> edge). Module-level deque singleton
dùng chung cả tiến trình pytest - MỖI test PHẢI tự dọn _recent_requests
trước/sau để không rò rỉ sang test khác (cùng tinh thần
_restore_settings_singleton trong conftest.py cho config.settings)."""
from fastapi import FastAPI
from fastapi.testclient import TestClient

import edge_collector.inbound_api as inbound_api


def _clear():
    inbound_api._recent_requests.clear()


def test_log_request_and_recent_requests_returns_copy_in_reverse_chronological_order():
    _clear()
    try:
        inbound_api._log_request("/api/command", serial="EDGE1", ch="CH01", cmd="zero")
        inbound_api._log_request("/api/latest", serial="EDGE1", ch="CH02")
        inbound_api._log_request("/api/stats", serial="EDGE1", ch="CH03", hours=24)

        rows = inbound_api.recent_requests()

        # appendleft() - request MỚI NHẤT (log sau cùng) phải đứng ĐẦU danh sách.
        assert [r["endpoint"] for r in rows] == ["/api/stats", "/api/latest", "/api/command"]

        rows.append({"endpoint": "/fake-mutation-should-not-leak"})
        assert len(inbound_api.recent_requests()) == 3, (
            "recent_requests() phải trả COPY - sửa list trả về không được ảnh "
            "hưởng deque gốc")
    finally:
        _clear()


def test_recent_requests_respects_maxlen():
    _clear()
    try:
        for i in range(60):
            inbound_api._log_request("/api/latest", serial="EDGE1", ch="CH%02d" % i)

        rows = inbound_api.recent_requests()

        assert len(rows) == 50
        assert rows[0]["ch"] == "CH59"  # mới nhất, đứng đầu (appendleft)
        assert rows[-1]["ch"] == "CH10"  # 10 request đầu (CH00-CH09) đã bị đẩy ra
    finally:
        _clear()


# --- POST /api/command "extra" (patch MQTT, merge từ production) -----------
#
# Chỉ chuyển tiếp những khóa ĐÃ BIẾT (ms/period_ms) tới manager.queue_command(),
# chỉ khi là số (int/float) - firmware phân tích gói bằng bộ đệm cố định, một
# body thừa trường sẽ bị cắt mất và lệnh im lặng không chạy (xem chú thích
# api_command()).


class _FakeManagerForExtra:
    """Đủ để làm api_command() đi vào nhánh queue_command() (driver=None,
    serial đã 'known_node_serials') - không cần SourceManager thật/driver
    thật, chỉ bắt lại được `extra` đã truyền xuống."""

    def __init__(self):
        self.queue_command_calls = []

    def driver_for_channel(self, serial, ch):
        return None

    def known_node_serials(self):
        return ["NODE1"]

    async def queue_command(self, serial, ch, cmd, value, extra=None):
        self.queue_command_calls.append(
            {"serial": serial, "ch": ch, "cmd": cmd, "value": value, "extra": extra})
        return {"ok": True}


def _client_with_fake_manager():
    app = FastAPI()
    app.include_router(inbound_api.router)
    manager = _FakeManagerForExtra()
    app.state.manager = manager
    return TestClient(app), manager


def test_api_command_extra_keeps_only_ms_and_period_ms_numeric_values():
    client, manager = _client_with_fake_manager()

    resp = client.post("/api/command", json={
        "serial": "NODE1", "channel": "relay_red", "cmd": "blink", "value": None,
        "ms": 30000, "period_ms": 500,
        "bogus": "should-be-dropped", "channel_extra": "also-dropped",
    })

    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
    assert len(manager.queue_command_calls) == 1
    assert manager.queue_command_calls[0]["extra"] == {"ms": 30000, "period_ms": 500}


def test_api_command_extra_drops_string_typed_ms_and_period_ms():
    """Firmware đọc "ms"/"period_ms" như số nguyên trong bộ đệm cố định - giá
    trị kiểu string (vd người gọi API gửi "30000" thay vì 30000) phải bị bỏ
    qua, KHÔNG được chuyển tiếp nguyên văn xuống node."""
    client, manager = _client_with_fake_manager()

    resp = client.post("/api/command", json={
        "serial": "NODE1", "channel": "relay_red", "cmd": "blink", "value": None,
        "ms": "30000", "period_ms": "500",
    })

    assert resp.status_code == 200
    assert manager.queue_command_calls[0]["extra"] == {}


def test_api_command_extra_drops_bool_typed_ms_and_period_ms():
    """bool là subclass của int trong Python - {"ms": true} phải bị loại
    (type() check, không phải isinstance()), không được lọt xuống thành
    JSON "true" mà firmware đợi số nguyên."""
    client, manager = _client_with_fake_manager()

    resp = client.post("/api/command", json={
        "serial": "NODE1", "channel": "relay_red", "cmd": "blink", "value": None,
        "ms": True, "period_ms": False,
    })

    assert resp.status_code == 200
    assert manager.queue_command_calls[0]["extra"] == {}


def test_api_command_extra_empty_dict_when_no_extra_fields_present():
    """Hành vi TRƯỚC patch (không có "extra" trong body) vẫn phải hoạt động -
    extra luôn là {} (không phải None) khi không có ms/period_ms, và payload
    rỗng không làm mqtt/poll payload bị nhiễm bẩn (xem
    tests/test_manager_queue_command.py::test_queue_command_extra_none_does_not_pollute_payload
    cho phía manager)."""
    client, manager = _client_with_fake_manager()

    resp = client.post("/api/command", json={
        "serial": "NODE1", "channel": "relay_red", "cmd": "zero", "value": None,
    })

    assert resp.status_code == 200
    assert manager.queue_command_calls[0]["extra"] == {}
