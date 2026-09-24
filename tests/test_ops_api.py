# -*- coding: utf-8 -*-
"""Test edge_collector/ops_api.py (MOI, patch MQTT merge tu production) -
trang /ops ('Đèn & gói tin') dung chung EDGE_SETUP_TOKEN gate voi /setup
(settings_api._check_setup_auth) va doc THANG tu app.state.agent.mqtt_consumer
trong bo nho, khong qua Odoo/SQLite - xem docstring dau file."""
from fastapi import FastAPI
from fastapi.testclient import TestClient

import edge_collector.config as config
import edge_collector.ops_api as ops_api


class _FakeConsumer:
    def __init__(self):
        self.stats = {"connected": True, "messages": 3, "items": 5, "forwarded": 0,
                      "bad": 0, "cmd_sent": 1, "cmd_acked": 1, "ts_dropped": 0,
                      "online": {"NODE1": True}}
        self.lamps = {"relay_red": {"v": 1, "ts": 123.0, "serial": "NODE1"}}
        self.caps = {"NODE1": True}
        self.traffic = [
            {"seq": 1, "t": 100.0, "dir": "up", "topic": "fms/NODE1/meas", "bytes": 10, "note": "a"},
            {"seq": 2, "t": 101.0, "dir": "down", "topic": "fms/NODE1/cmd", "bytes": 20, "note": "b"},
        ]


class _FakeStore:
    def outbox_count(self):
        return 7


class _FakeAgent:
    def __init__(self):
        self.mqtt_consumer = _FakeConsumer()
        self.store = _FakeStore()


def _client():
    app = FastAPI()
    app.include_router(ops_api.router)
    app.state.agent = _FakeAgent()
    return TestClient(app)


def test_ops_page_returns_html_when_no_setup_token_set():
    assert config.settings.setup_token == ""
    resp = _client().get("/ops")

    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "Đèn" in resp.text


def test_ops_page_requires_basic_auth_when_setup_token_set():
    config.settings.setup_token = "sekret"  # secret-allow: test fixture
    client = _client()

    resp_no_auth = client.get("/ops")
    assert resp_no_auth.status_code == 401
    assert "Basic" in resp_no_auth.headers.get("www-authenticate", "")

    resp_wrong = client.get("/ops", auth=("anyuser", "wrong"))
    assert resp_wrong.status_code == 401

    resp_ok = client.get("/ops", auth=("anyuser", "sekret"))
    assert resp_ok.status_code == 200


def test_ops_api_state_returns_expected_json_shape():
    resp = _client().get("/ops/api/state")

    assert resp.status_code == 200
    body = resp.json()
    assert body["stats"]["connected"] is True
    assert body["lamps"] == {"relay_red": {"v": 1, "ts": 123.0, "serial": "NODE1"}}
    assert body["caps"] == {"NODE1": True}
    assert body["outbox"] == 7
    assert len(body["events"]) == 2
    assert body["cursor"] == 2
    assert "now" in body


def test_ops_api_state_filters_events_by_since_cursor():
    resp = _client().get("/ops/api/state", params={"since": 1})

    assert resp.status_code == 200
    body = resp.json()
    # Chi con goi tin co seq > 1 (goi seq=1 da duoc client thay tu lan poll truoc).
    assert len(body["events"]) == 1
    assert body["events"][0]["seq"] == 2


def test_ops_api_state_requires_basic_auth_when_setup_token_set():
    config.settings.setup_token = "sekret"  # secret-allow: test fixture
    client = _client()

    resp_no_auth = client.get("/ops/api/state")
    assert resp_no_auth.status_code == 401

    resp_ok = client.get("/ops/api/state", auth=("anyuser", "sekret"))
    assert resp_ok.status_code == 200


def test_ops_api_state_cursor_falls_back_to_since_when_no_traffic():
    """Consumer chua co goi tin nao (deque rong) - cursor phai tra lai dung
    gia tri 'since' da nhan, khong duoc crash tren traffic[-1] rong."""
    class _EmptyConsumer(_FakeConsumer):
        def __init__(self):
            super().__init__()
            self.traffic = []

    class _AgentNoTraffic(_FakeAgent):
        def __init__(self):
            self.mqtt_consumer = _EmptyConsumer()
            self.store = _FakeStore()

    app = FastAPI()
    app.include_router(ops_api.router)
    app.state.agent = _AgentNoTraffic()
    client = TestClient(app)

    resp = client.get("/ops/api/state", params={"since": 42})

    assert resp.status_code == 200
    assert resp.json()["cursor"] == 42
    assert resp.json()["events"] == []
