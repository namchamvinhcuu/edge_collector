# -*- coding: utf-8 -*-
"""Test ring-buffer 'PCM requests' (edge_collector/inbound_api.py) -
_log_request()/recent_requests() phuc vu panel /setup/pcm_requests
(settings_api.py), doi xung NGUOC CHIEU voi Store.history_recent()
(node_agent -> edge: do la Odoo Main -> edge). Module-level deque singleton
dung chung ca tien trinh pytest - MOI test PHAI tu don _recent_requests
truoc/sau de khong ro ri sang test khac (cung tinh than
_restore_settings_singleton trong conftest.py cho config.settings)."""
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
