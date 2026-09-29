# -*- coding: utf-8 -*-
"""Test edge_collector/scheduler.py::EdgeAgent._should_skip_duplicate /
_on_value (29/09 - chong flood outbox tu can dien tu continuous-output gui
gia tri Y HET nhau lien tuc, xem chu thich DEFAULT_HEARTBEAT_S trong
scheduler.py + Fix-History 2026-09-29).

Cung tinh than voi tests/test_scheduler.py: KHONG dung EdgeAgent() that
(__init__ dung Store that + MqttConsumer(self) + OdooClient - qua nhieu
side-effect khong lien quan) - gan thang 2 method dang test vao 1 object
toi thieu chi co store/manager/_pending/_last_enqueued.

Dung SourceManager() THAT (khong mock) cho `manager` - channel_meta_for()
la method don gian, khong side-effect, va dung ban that cho phep cac test
"khong co meta"/"apply_config" o day khop CHINH XAC voi hanh vi production
(xem test_manager_channel_meta.py cho rieng SourceManager.apply_config()).

time.monotonic() duoc fake qua monkeypatch tren MODULE edge_collector.scheduler
(no `import time` roi goi `time.monotonic()` - patch dung bien module-level
nay, KHONG patch builtin `time` toan cuc, de khong anh huong test khac chay
song song)."""
import logging
from unittest.mock import Mock

import pytest

from edge_collector.manager import SourceManager
from edge_collector.scheduler import EdgeAgent
from edge_collector.store import Store


class _FakeAgent:
    """Object toi thieu dong vai EdgeAgent cho _should_skip_duplicate/_on_value
    dang test - xem docstring module."""
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
    """Tra ve holder co the chinh `now["t"]` de gia lap thoi gian troi qua
    cho time.monotonic() ma _should_skip_duplicate() dung."""
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
# must_send_every=False - loc trung gia tri y het
# ----------------------------------------------------------------------

def test_exact_duplicate_within_heartbeat_is_not_enqueued_but_history_kept(tmp_path, fake_clock):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "weight", max_age_ms=None)  # -> DEFAULT_HEARTBEAT_S=5.0
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.0, True)
    fake_clock["t"] = 0.1                      # troi qua rat it, << 5.0s
    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.1, True)

    # KHONG duoc enqueue lan 2 (gia tri y het) - _pending chi co 1 item.
    assert agent._pending["S1"] == [
        {"ch": "weight", "v": 12.5, "s": "ok", "q": 1, "stable": True, "ts": 1000000}
    ]
    # NHUNG history_insert_many() van ghi CA HAI lan doc (Live activity local
    # khong bi anh huong boi loc trung).
    assert _history_count(store, "S1", "weight") == 2


def test_value_change_always_enqueues_immediately(tmp_path, fake_clock):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "weight", max_age_ms=None)
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.0, True)
    fake_clock["t"] = 0.05                     # gan nhu tuc thi
    agent._on_value("S1", "weight", 12.51, "ok", 1, 1000.05, True)   # doi rat nho

    assert len(agent._pending["S1"]) == 2
    assert agent._pending["S1"][1]["v"] == 12.51


def test_stable_flag_change_always_enqueues(tmp_path, fake_clock):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "weight", max_age_ms=None)
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.0, True)
    fake_clock["t"] = 0.05
    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.05, False)  # v/s/q y het, stable doi

    assert len(agent._pending["S1"]) == 2
    assert agent._pending["S1"][1]["stable"] is False


def test_quality_change_always_enqueues(tmp_path, fake_clock):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "weight", max_age_ms=None)
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.0, True)
    fake_clock["t"] = 0.05
    agent._on_value("S1", "weight", 12.5, "ok", 0, 1000.05, True)   # q doi 1 -> 0

    assert len(agent._pending["S1"]) == 2
    assert agent._pending["S1"][1]["q"] == 0


def test_string_field_change_always_enqueues(tmp_path, fake_clock):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "evt", max_age_ms=None)
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "evt", 0, "A", 1, 1000.0, True)
    fake_clock["t"] = 0.05
    agent._on_value("S1", "evt", 0, "B", 1, 1000.05, True)          # s doi A -> B

    assert len(agent._pending["S1"]) == 2
    assert agent._pending["S1"][1]["s"] == "B"


# ----------------------------------------------------------------------
# heartbeat (max_age_ms*0.5) - van gui lai dinh ky du gia tri khong doi
# ----------------------------------------------------------------------

def test_heartbeat_fires_after_max_age_elapsed(tmp_path, fake_clock):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "weight", max_age_ms=2000)   # heartbeat_s = 1.0
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.0, True)
    fake_clock["t"] = 1.1                       # vuot 1.0s heartbeat
    agent._on_value("S1", "weight", 12.5, "ok", 1, 1001.1, True)

    assert len(agent._pending["S1"]) == 2       # heartbeat ping duoc gui lai


def test_heartbeat_not_yet_due_still_skips(tmp_path, fake_clock):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "weight", max_age_ms=2000)   # heartbeat_s = 1.0
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.0, True)
    fake_clock["t"] = 0.5                       # moi qua 1 phan nho, chua du 1.0s
    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.5, True)

    assert len(agent._pending["S1"]) == 1       # van bi skip


# ----------------------------------------------------------------------
# must_send_every=True (default hoac explicit) - regression guard QUAN
# TRONG NHAT: channel counter/raw-forward/trigger KHONG duoc loc trung.
# ----------------------------------------------------------------------

@pytest.mark.parametrize("must_send_every", [True, "default"])
def test_must_send_every_true_always_enqueues_even_identical_values(
        tmp_path, fake_clock, must_send_every):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    if must_send_every == "default":
        # KHONG set _channel_meta cho channel nay - channel_meta_for() tra
        # ve default {"must_send_every": True} (xem test ben duoi).
        pass
    else:
        _set_meta(manager, "S1", "counter", max_age_ms=None, must_send_every=True)
    agent = _FakeAgent(store, manager)

    for i in range(5):
        fake_clock["t"] = i * 0.01              # rat gan nhau, << heartbeat
        agent._on_value("S1", "counter", 7, "ok", 1, 1000.0 + i, True)

    # 5 lan doc GIONG HET nhau (gia tri counter khong doi lan doc nay) van
    # phai duoc enqueue DU 5 - day chinh la channel loai counter/raw-forward/
    # trigger ma session pcm_base canh bao KHONG duoc loc.
    assert len(agent._pending["S1"]) == 5


def test_channel_without_meta_defaults_to_send_every(tmp_path, fake_clock):
    """Channel CHUA TUNG thay trong config (khong co trong _channel_meta) -
    channel_meta_for() phai fallback {"must_send_every": True} (an toan,
    giong het hanh vi TRUOC KHI co tinh nang loc trung nay)."""
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    assert manager._channel_meta == {}          # chua apply_config() lan nao
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "unknown_ch", 1, "ok", 1, 1000.0, True)
    fake_clock["t"] = 0.01
    agent._on_value("S1", "unknown_ch", 1, "ok", 1, 1000.01, True)

    assert len(agent._pending["S1"]) == 2


# ----------------------------------------------------------------------
# max_age_ms thieu/sai -> fallback DEFAULT_HEARTBEAT_S, khong crash.
# ----------------------------------------------------------------------

@pytest.mark.parametrize("bad_max_age_ms", [None, 0, -500, "abc", [1, 2]])
def test_invalid_max_age_ms_falls_back_to_default_heartbeat(tmp_path, fake_clock, bad_max_age_ms):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "weight", max_age_ms=bad_max_age_ms)
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.0, True)

    # Ngay truoc moc DEFAULT_HEARTBEAT_S (5.0s) -> van phai skip.
    fake_clock["t"] = EdgeAgent.DEFAULT_HEARTBEAT_S - 0.1
    agent._on_value("S1", "weight", 12.5, "ok", 1, 1004.9, True)
    assert len(agent._pending["S1"]) == 1, f"bad_max_age_ms={bad_max_age_ms!r} khong duoc skip dung"

    # Vuot moc DEFAULT_HEARTBEAT_S -> phai enqueue lai (khong bi crash/treo
    # o nhanh isinstance check du max_age_ms khong hop le).
    fake_clock["t"] = EdgeAgent.DEFAULT_HEARTBEAT_S + 0.1
    agent._on_value("S1", "weight", 12.5, "ok", 1, 1005.1, True)
    assert len(agent._pending["S1"]) == 2, f"bad_max_age_ms={bad_max_age_ms!r} khong fallback dung DEFAULT_HEARTBEAT_S"


# ----------------------------------------------------------------------
# state _last_enqueued doc lap theo dung key (serial, ch)
# ----------------------------------------------------------------------

def test_last_enqueued_state_independent_per_serial_and_channel(tmp_path, fake_clock):
    store = Store(tmp_path / "t.db")
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "ch1", max_age_ms=None)
    _set_meta(manager, "S1", "ch2", max_age_ms=None)
    _set_meta(manager, "S2", "ch1", max_age_ms=None)
    agent = _FakeAgent(store, manager)

    # Enqueue lan dau cho ca 3 key - thiet lap _last_enqueued rieng cho tung key.
    agent._on_value("S1", "ch1", 1, "ok", 1, 1000.0, True)
    agent._on_value("S1", "ch2", 99, "ok", 1, 1000.0, True)
    agent._on_value("S2", "ch1", 1, "ok", 1, 1000.0, True)

    fake_clock["t"] = 0.05
    # (S1, ch1) gui gia tri Y HET -> phai skip (dedup rieng key nay).
    agent._on_value("S1", "ch1", 1, "ok", 1, 1000.05, True)
    # (S1, ch2) khac gia tri lan dau cua (S1, ch1) nhung KHONG doi so voi
    # chinh no -> cung phai skip (dung state cua (S1, ch2), khong bi lech
    # sang key khac).
    agent._on_value("S1", "ch2", 99, "ok", 1, 1000.05, True)
    # (S2, ch1) TRUNG ma channel code voi (S1, ch1) nhung KHAC serial -> gia
    # tri doi (2 thay vi 1) phai duoc enqueue NGAY, khong bi anh huong boi
    # state cua (S1, ch1).
    agent._on_value("S2", "ch1", 2, "ok", 1, 1000.05, True)

    assert len(agent._pending["S1"]) == 2       # ch1(1) + ch2(1), khong co ban trung
    assert [it["v"] for it in agent._pending["S1"]] == [1, 99]
    assert len(agent._pending["S2"]) == 2       # lan dau + lan doi gia tri
    assert [it["v"] for it in agent._pending["S2"]] == [1, 2]


# ----------------------------------------------------------------------
# _on_value(): loi ghi history() KHONG duoc chan duong dedup/outbox
# (regression cho finding python-reviewer 2026-09-29 - truoc fix, dao thu
# tu se lam _should_skip_duplicate()/append vao _pending khong bao gio chay
# neu history_insert_many() raise, vi khong co try/except quanh no).
# ----------------------------------------------------------------------

def test_on_value_history_write_failure_does_not_block_pending_append(caplog):
    """Regression cho finding 🟠 python-reviewer 2026-09-29 (exception-ordering):
    history_insert_many() raise (vd SQLite disk full/locked) - _on_value()
    PHAI (1) KHONG de exception lan ra ngoai (goi truc tiep, KHONG boc trong
    pytest.raises - neu con raise, chinh loi goi nay se lam test ERROR chu
    khong phai FAIL, van chung minh duoc regression); (2) item VAN duoc day
    vao _pending nhu binh thuong (durable-first: loi history local KHONG
    duoc phep chan duong outbox/Odoo)."""
    store = Mock()
    store.history_insert_many = Mock(side_effect=RuntimeError("gia lap loi ghi SQLite"))
    manager = SourceManager(on_value=lambda *a: None)   # khong set meta -> must_send_every=True
    agent = _FakeAgent(store, manager)

    with caplog.at_level(logging.ERROR, logger="edge.scheduler"):
        # (1) Khong exception nao duoc phep thoat ra khoi loi goi nay.
        agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.0, True)

    # (2) Item van duoc day vao _pending binh thuong - loi history KHONG
    # chan duong outbox/Odoo.
    assert agent._pending["S1"] == [
        {"ch": "weight", "v": 12.5, "s": "ok", "q": 1, "stable": True, "ts": 1000000}
    ]
    # Loi KHONG duoc nuot im lang - phai co log ERROR nhac ten serial/ch.
    assert any(r.levelno >= logging.ERROR and "S1" in r.getMessage() and "weight" in r.getMessage()
               for r in caplog.records), caplog.text


def test_on_value_history_write_failure_does_not_break_dedup_for_next_call(fake_clock):
    """Loi history (du bi nuot/log) KHONG lam hong logic dedup phia sau -
    goi lan 2 gia tri y het (trong heartbeat window) van phai bi skip nhu
    binh thuong, dung thu tu: history (du loi) -> should_skip_duplicate ->
    append."""
    store = Mock()
    store.history_insert_many = Mock(side_effect=RuntimeError("gia lap loi ghi SQLite"))
    manager = SourceManager(on_value=lambda *a: None)
    _set_meta(manager, "S1", "weight", max_age_ms=None)   # must_send_every=False
    agent = _FakeAgent(store, manager)

    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.0, True)
    fake_clock["t"] = 0.05
    agent._on_value("S1", "weight", 12.5, "ok", 1, 1000.05, True)

    assert len(agent._pending["S1"]) == 1       # lan 2 van bi skip nhu binh thuong
