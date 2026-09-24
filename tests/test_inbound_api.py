# -*- coding: utf-8 -*-
"""Test ring-buffer 'PCM requests' (edge_collector/inbound_api.py) -
_log_request()/recent_requests() phuc vu panel /setup/pcm_requests
(settings_api.py), doi xung NGUOC CHIEU voi Store.history_recent()
(node_agent -> edge: do la Odoo Main -> edge). Module-level deque singleton
dung chung ca tien trinh pytest - MOI test PHAI tu don _recent_requests
truoc/sau de khong ro ri sang test khac (cung tinh than
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

        # appendleft() - request MOI NHAT (log sau cung) phai dung DAU danh sach.
        assert [r["endpoint"] for r in rows] == ["/api/stats", "/api/latest", "/api/command"]

        rows.append({"endpoint": "/fake-mutation-should-not-leak"})
        assert len(inbound_api.recent_requests()) == 3, (
            "recent_requests() phai tra COPY - sua list tra ve khong duoc anh "
            "huong deque goc")
    finally:
        _clear()


def test_recent_requests_respects_maxlen():
    _clear()
    try:
        for i in range(60):
            inbound_api._log_request("/api/latest", serial="EDGE1", ch="CH%02d" % i)

        rows = inbound_api.recent_requests()

        assert len(rows) == 50
        assert rows[0]["ch"] == "CH59"  # moi nhat, dung dau (appendleft)
        assert rows[-1]["ch"] == "CH10"  # 10 request dau (CH00-CH09) da bi day ra
    finally:
        _clear()


# --- POST /api/command "extra" (patch MQTT, merge tu production) -----------
#
# Chi chuyen tiep nhung khoa DA BIET (ms/period_ms) toi manager.queue_command(),
# chi khi la so (int/float) - firmware phan tich goi bang bo dem co dinh, mot
# body thua truong se bi cat mat va lenh im lang khong chay (xem chu thich
# api_command()).


class _FakeManagerForExtra:
    """Du de lam api_command() di vao nhanh queue_command() (driver=None,
    serial da 'known_node_serials') - khong can SourceManager that/driver
    that, chi bat lai duoc `extra` da truyen xuong."""

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
    """Firmware doc "ms"/"period_ms" nhu so nguyen trong bo dem co dinh - gia
    tri kieu string (vd nguoi goi API gui "30000" thay vi 30000) phai bi bo
    qua, KHONG duoc chuyen tiep nguyen van xuong node."""
    client, manager = _client_with_fake_manager()

    resp = client.post("/api/command", json={
        "serial": "NODE1", "channel": "relay_red", "cmd": "blink", "value": None,
        "ms": "30000", "period_ms": "500",
    })

    assert resp.status_code == 200
    assert manager.queue_command_calls[0]["extra"] == {}


def test_api_command_extra_drops_bool_typed_ms_and_period_ms():
    """bool la subclass cua int trong Python - {"ms": true} phai bi loai
    (type() check, khong phai isinstance()), khong duoc lot xuong thanh
    JSON "true" ma firmware doi so nguyen."""
    client, manager = _client_with_fake_manager()

    resp = client.post("/api/command", json={
        "serial": "NODE1", "channel": "relay_red", "cmd": "blink", "value": None,
        "ms": True, "period_ms": False,
    })

    assert resp.status_code == 200
    assert manager.queue_command_calls[0]["extra"] == {}


def test_api_command_extra_empty_dict_when_no_extra_fields_present():
    """Hanh vi TRUOC patch (khong co "extra" trong body) van phai hoat dong -
    extra luon la {} (khong phai None) khi khong co ms/period_ms, va payload
    rong khong lam mqtt/poll payload bi nhiem ban (xem
    tests/test_manager_queue_command.py::test_queue_command_extra_none_does_not_pollute_payload
    cho phia manager)."""
    client, manager = _client_with_fake_manager()

    resp = client.post("/api/command", json={
        "serial": "NODE1", "channel": "relay_red", "cmd": "zero", "value": None,
    })

    assert resp.status_code == 200
    assert manager.queue_command_calls[0]["extra"] == {}
