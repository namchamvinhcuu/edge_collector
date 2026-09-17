# -*- coding: utf-8 -*-
"""Test Store (edge_collector/store.py) - hien chi cover history_recent()
(nguon du lieu panel 'Live activity' o /setup, xem settings_api.py va review
2026-09-17). Cac method khac (outbox/kv/seq/history_latest/history_stats)
chua co test rieng - ngoai scope task nay, xem 'Scope da KHONG cover'."""
from edge_collector.store import Store


def test_store_history_recent_orders_desc_and_respects_limit(tmp_path):
    store = Store(tmp_path / "test.db")
    store.history_insert_many([
        ("EDGE-A", "temp", 100.0, 20.0, None, 0, 1),
        ("EDGE-A", "temp", 200.0, 21.0, None, 0, 1),
        ("EDGE-B", "hum", 150.0, 55.0, None, 0, 1),
        ("EDGE-B", "hum", 300.0, 56.0, None, 0, 1),
    ])

    rows = store.history_recent(limit=2)

    assert len(rows) == 2
    assert [r["ts"] for r in rows] == [300.0, 200.0]
    assert rows[0]["serial"] == "EDGE-B" and rows[0]["ch"] == "hum"
    assert rows[1]["serial"] == "EDGE-A" and rows[1]["ch"] == "temp"


def test_store_history_recent_returns_all_when_limit_exceeds_row_count(tmp_path):
    store = Store(tmp_path / "test.db")
    store.history_insert_many([
        ("EDGE-A", "temp", 100.0, 20.0, None, 0, 1),
        ("EDGE-A", "temp", 200.0, 21.0, None, 0, 1),
    ])

    rows = store.history_recent(limit=50)

    assert len(rows) == 2
    assert [r["ts"] for r in rows] == [200.0, 100.0]
