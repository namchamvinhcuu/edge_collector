# -*- coding: utf-8 -*-
"""Test edge_collector/scheduler.py::EdgeAgent._should_skip_duplicate /
_on_value (29/09 - chống flood outbox từ cân điện tử continuous-output gửi
giá trị Y HỆT nhau liên tục, xem chú thích DEFAULT_HEARTBEAT_S trong
scheduler.py + Fix-History 2026-09-29).

Cùng tinh thần với tests/test_scheduler.py: KHÔNG dùng EdgeAgent() thật
(__init__ dùng Store thật + MqttConsumer(self) + OdooClient - quá nhiều
side-effect không liên quan) - gán thẳng 2 method đang test vào 1 object
tối thiểu chỉ có store/manager/_pending/_last_enqueued.

Dùng SourceManager() THẬT (không mock) cho `manager` - channel_meta_for()
là method đơn giản, không side-effect, và dùng bản thật cho phép các test
"không có meta"/"apply_config" ở đây khớp CHÍNH XÁC với hành vi production
(xem test_manager_channel_meta.py cho riêng SourceManager.apply_config()).

time.monotonic() được fake qua monkeypatch trên MODULE edge_collector.scheduler
(nó `import time` rồi gọi `time.monotonic()` - patch đúng biến module-level
này, KHÔNG patch builtin `time` toàn cục, để không ảnh hưởng test khác chạy
song song)."""
import logging
from unittest.mock import Mock

import pytest

from edge_collector.manager import SourceManager
from edge_collector.scheduler import EdgeAgent
from edge_collector.store import Store


class _FakeAgent:
    """Object tối thiểu đóng vai EdgeAgent cho _should_skip_duplicate/_on_value
    đang test - xem docstring module."""
    _should_skip_duplicate = EdgeAgent._should_skip_duplicate
    _on_value = EdgeAgent._on_value
    DEFAULT_HEARTBEAT_S = EdgeAgent.DEFAULT_HEARTBEAT_S

    def __init__(self, store, manager=None):
        self.store = store
        self.manager = manager or SourceManager(on_value=lambda *a: None)
        self._pending: dict = {}
        self._last_enqueued: dict = {}


@pytest.fixture
def fake_clock(monkeypatch):
    """Trả về holder có thể chỉnh `now["t"]` để giả lập thời gian trôi qua
    cho time.monotonic() mà _should_skip_duplicate() dùng."""
    now = {"t": 0.0}
    monkeypatch.setattr("edge_collector.scheduler.time.monotonic", lambda: now["t"])
    return now


def _history_count(store, serial, ch):
    rows = store.history_recent(limit=1000)
    return sum(1 for r in rows if r["serial"] == serial and r["ch"] == ch)


def _set_meta(manager, serial, ch, max_age_ms=None, must_send_every=False):
    manager._channel_meta[(serial, ch)] = {
        "max_age_ms": max_age_ms, "must_send_every": must_send_every,
    }


# ----------------------------------------------------------------------
# must_send_every=False - lọc trùng giá trị y hệt
# ----------------------------------------------------------------------

def test_exact_duplicate_within_heartbeat_is_not_enqueued_but_history_kept(tmp_path, fake_clock):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "weight", max_age_ms=None)  # -> DEFAULT_HEARTBEAT_S=5.0
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.0, True)
    fake_clock["t"] = 0.1                      # trôi qua rất ít, << 5.0s
    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.1, True)

    # KHÔNG được enqueue lần 2 (giá trị y hệt) - _pending chỉ có 1 item.
    assert agent._pending["S1"] == [
        {"ch": "weight", "v": 12.5, "s": "ok", "q": 1, "stable": True, "ts": 1000000}
    ]
    # NHƯNG history_insert_many() vẫn ghi CẢ HAI lần đọc (Live activity local
    # không bị ảnh hưởng bởi lọc trùng).
    assert _history_count(store, "S1", "weight") == 2


def test_value_change_always_enqueues_immediately(tmp_path, fake_clock):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "weight", max_age_ms=None)
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.0, True)
    fake_clock["t"] = 0.05                     # gần như tức thì
    agent._on_value("S1", "weight", 12.51, "ok", 1, 1000.05, True)   # đổi rất nhỏ

    assert len(agent._pending["S1"]) == 2
    assert agent._pending["S1"][1]["v"] == 12.51


def test_stable_flag_change_always_enqueues(tmp_path, fake_clock):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "weight", max_age_ms=None)
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.0, True)
    fake_clock["t"] = 0.05
    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.05, False)  # v/s/q y hệt, stable đổi

    assert len(agent._pending["S1"]) == 2
    assert agent._pending["S1"][1]["stable"] is False


def test_quality_change_always_enqueues(tmp_path, fake_clock):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "weight", max_age_ms=None)
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.0, True)
    fake_clock["t"] = 0.05
    agent._on_value("S1", "weight", 12.5, "ok", 0, 1000.05, True)   # q đổi 1 -> 0

    assert len(agent._pending["S1"]) == 2
    assert agent._pending["S1"][1]["q"] == 0


def test_string_field_change_always_enqueues(tmp_path, fake_clock):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "evt", max_age_ms=None)
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "evt", 0, "A", 1, 1000.0, True)
    fake_clock["t"] = 0.05
    agent._on_value("S1", "evt", 0, "B", 1, 1000.05, True)          # s đổi A -> B

    assert len(agent._pending["S1"]) == 2
    assert agent._pending["S1"][1]["s"] == "B"


# ----------------------------------------------------------------------
# heartbeat (max_age_ms*0.5) - vẫn gửi lại định kỳ dù giá trị không đổi
# ----------------------------------------------------------------------

def test_heartbeat_fires_after_max_age_elapsed(tmp_path, fake_clock):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "weight", max_age_ms=2000)   # heartbeat_s = 1.0
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.0, True)
    fake_clock["t"] = 1.1                       # vượt 1.0s heartbeat
    agent._on_value("S1", "weight", 12.5, "ok", 1, 1001.1, True)

    assert len(agent._pending["S1"]) == 2       # heartbeat ping được gửi lại


def test_heartbeat_not_yet_due_still_skips(tmp_path, fake_clock):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "weight", max_age_ms=2000)   # heartbeat_s = 1.0
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.0, True)
    fake_clock["t"] = 0.5                       # mới qua 1 phần nhỏ, chưa đủ 1.0s
    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.5, True)

    assert len(agent._pending["S1"]) == 1       # vẫn bị skip


# ----------------------------------------------------------------------
# must_send_every=True (default hoặc explicit) - regression guard QUAN
# TRỌNG NHẤT: channel counter/raw-forward/trigger KHÔNG được lọc trùng.
# ----------------------------------------------------------------------

@pytest.mark.parametrize("must_send_every", [True, "default"])
def test_must_send_every_true_always_enqueues_even_identical_values(
        tmp_path, fake_clock, must_send_every):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    if must_send_every == "default":
        # KHÔNG set _channel_meta cho channel này - channel_meta_for() trả
        # về default {"must_send_every": True} (xem test bên dưới).
        pass
    else:
        _set_meta(manager, "S1", "counter", max_age_ms=None, must_send_every=True)
    agent = _FakeAgent(store, manager)

    for i in range(5):
        fake_clock["t"] = i * 0.01              # rất gần nhau, << heartbeat
        agent._on_value("S1", "counter", 7, "ok", 1, 1000.0 + i, True)

    # 5 lần đọc GIỐNG HỆT nhau (giá trị counter không đổi lần đọc này) vẫn
    # phải được enqueue ĐỦ 5 - đây chính là channel loại counter/raw-forward/
    # trigger mà session pcm_base cảnh báo KHÔNG được lọc.
    assert len(agent._pending["S1"]) == 5


def test_channel_without_meta_defaults_to_send_every(tmp_path, fake_clock):
    """Channel CHƯA TỪNG thay trong config (không có trong _channel_meta) -
    channel_meta_for() phải fallback {"must_send_every": True} (an toàn,
    giống hệt hành vi TRƯỚC KHI có tính năng lọc trùng này)."""
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    assert manager._channel_meta == {}          # chưa apply_config() lần nào
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "unknown_ch", 1, "ok", 1, 1000.0, True)
    fake_clock["t"] = 0.01
    agent._on_value("S1", "unknown_ch", 1, "ok", 1, 1000.01, True)

    assert len(agent._pending["S1"]) == 2


# ----------------------------------------------------------------------
# max_age_ms thiếu/sai -> fallback DEFAULT_HEARTBEAT_S, không crash.
# ----------------------------------------------------------------------

@pytest.mark.parametrize("bad_max_age_ms", [None, 0, -500, "abc", [1, 2]])
def test_invalid_max_age_ms_falls_back_to_default_heartbeat(tmp_path, fake_clock, bad_max_age_ms):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "weight", max_age_ms=bad_max_age_ms)
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.0, True)

    # Ngay trước mốc DEFAULT_HEARTBEAT_S (5.0s) -> vẫn phải skip.
    fake_clock["t"] = EdgeAgent.DEFAULT_HEARTBEAT_S - 0.1
    agent._on_value("S1", "weight", 12.5, "ok", 1, 1004.9, True)
    assert len(agent._pending["S1"]) == 1, f"bad_max_age_ms={bad_max_age_ms!r} khong duoc skip dung"

    # Vượt mốc DEFAULT_HEARTBEAT_S -> phải enqueue lại (không bị crash/treo
    # ở nhánh isinstance check dù max_age_ms không hợp lệ).
    fake_clock["t"] = EdgeAgent.DEFAULT_HEARTBEAT_S + 0.1
    agent._on_value("S1", "weight", 12.5, "ok", 1, 1005.1, True)
    assert len(agent._pending["S1"]) == 2, f"bad_max_age_ms={bad_max_age_ms!r} khong fallback dung DEFAULT_HEARTBEAT_S"


# ----------------------------------------------------------------------
# state _last_enqueued độc lập theo đúng key (serial, ch)
# ----------------------------------------------------------------------

def test_last_enqueued_state_independent_per_serial_and_channel(tmp_path, fake_clock):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "ch1", max_age_ms=None)
    _set_meta(manager, "S1", "ch2", max_age_ms=None)
    _set_meta(manager, "S2", "ch1", max_age_ms=None)
    agent = _FakeAgent(store, manager)

    # Enqueue lần đầu cho cả 3 key - thiết lập _last_enqueued riêng cho từng key.
    agent._on_value("S1", "ch1", 1, "ok", 1, 1000.0, True)
    agent._on_value("S1", "ch2", 99, "ok", 1, 1000.0, True)
    agent._on_value("S2", "ch1", 1, "ok", 1, 1000.0, True)

    fake_clock["t"] = 0.05
    # (S1, ch1) gửi giá trị Y HỆT -> phải skip (dedup riêng key này).
    agent._on_value("S1", "ch1", 1, "ok", 1, 1000.05, True)
    # (S1, ch2) khác giá trị lần đầu của (S1, ch1) nhưng KHÔNG đổi so với
    # chính nó -> cũng phải skip (dùng state của (S1, ch2), không bị lệch
    # sang key khác).
    agent._on_value("S1", "ch2", 99, "ok", 1, 1000.05, True)
    # (S2, ch1) TRÙNG mã channel code với (S1, ch1) nhưng KHÁC serial -> giá
    # trị đổi (2 thay vì 1) phải được enqueue NGAY, không bị ảnh hưởng bởi
    # state của (S1, ch1).
    agent._on_value("S2", "ch1", 2, "ok", 1, 1000.05, True)

    assert len(agent._pending["S1"]) == 2       # ch1(1) + ch2(1), không có bản trùng
    assert [it["v"] for it in agent._pending["S1"]] == [1, 99]
    assert len(agent._pending["S2"]) == 2       # lần đầu + lần đổi giá trị
    assert [it["v"] for it in agent._pending["S2"]] == [1, 2]


# ----------------------------------------------------------------------
# _on_value(): lỗi ghi history() KHÔNG được chặn đường dedup/outbox
# (regression cho finding python-reviewer 2026-09-29 - trước fix, đảo thứ
# tự sẽ làm _should_skip_duplicate()/append vào _pending không bao giờ chạy
# nếu history_insert_many() raise, vì không có try/except quanh nó).
# ----------------------------------------------------------------------

def test_on_value_history_write_failure_does_not_block_pending_append(caplog):
    """Regression cho finding 🟠 python-reviewer 2026-09-29 (exception-ordering):
    history_insert_many() raise (vd SQLite disk full/locked) - _on_value()
    PHẢI (1) KHÔNG để exception lan ra ngoài (gọi trực tiếp, KHÔNG bọc trong
    pytest.raises - nếu còn raise, chính lỗi gọi này sẽ làm test ERROR chứ
    không phải FAIL, vẫn chứng minh được regression); (2) item VẪN được đẩy
    vào _pending như bình thường (durable-first: lỗi history local KHÔNG
    được phép chặn đường outbox/Odoo)."""
    store = Mock()
    store.history_insert_many = Mock(side_effect=RuntimeError("gia lap loi ghi SQLite"))
    manager = SourceManager(on_value=lambda *a: None)   # không set meta -> must_send_every=True
    agent = _FakeAgent(store, manager)

    with caplog.at_level(logging.ERROR, logger="edge.scheduler"):
        # (1) Không exception nào được phép thoát ra khỏi lời gọi này.
        agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.0, True)

    # (2) Item vẫn được đẩy vào _pending bình thường - lỗi history KHÔNG
    # chặn đường outbox/Odoo.
    assert agent._pending["S1"] == [
        {"ch": "weight", "v": 12.5, "s": "ok", "q": 1, "stable": True, "ts": 1000000}
    ]
    # Lỗi KHÔNG được nuốt im lặng - phải có log ERROR nhắc tên serial/ch.
    assert any(r.levelno >= logging.ERROR and "S1" in r.getMessage() and "weight" in r.getMessage()
               for r in caplog.records), caplog.text


def test_on_value_history_write_failure_does_not_break_dedup_for_next_call(fake_clock):
    """Lỗi history (dù bị nuốt/log) KHÔNG làm hỏng logic dedup phía sau -
    gọi lần 2 giá trị y hệt (trong heartbeat window) vẫn phải bị skip như
    bình thường, đúng thứ tự: history (dù lỗi) -> should_skip_duplicate ->
    append."""
    store = Mock()
    store.history_insert_many = Mock(side_effect=RuntimeError("gia lap loi ghi SQLite"))
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "weight", max_age_ms=None)   # must_send_every=False
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.0, True)
    fake_clock["t"] = 0.05
    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.05, True)

    assert len(agent._pending["S1"]) == 1       # lần 2 vẫn bị skip như bình thường
