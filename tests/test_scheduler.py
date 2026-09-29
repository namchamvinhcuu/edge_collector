# -*- coding: utf-8 -*-
"""Test edge_collector/scheduler.py::EdgeAgent._drain_serial / _sender_loop
(sua 24/09 - gioi han concurrency thay vi rut can outbox TUAN TU tung serial,
xem chu thich MAX_CONCURRENT_SERIALS trong scheduler.py).

KHONG dung EdgeAgent() that (__init__ dung Store that + SourceManager +
MqttConsumer(self) + OdooClient - qua nhieu side-effect khong lien quan) -
2 method dang test (_drain_serial, _sender_loop) la ham dinh nghia tren
class EdgeAgent, gan duoc thang vao 1 object toi thieu chi co store/manager/
odoo/_stopping/hang so lien quan (cung tinh than voi tests/test_mqtt_consumer.py
- goi method truc tiep tren object gia, khong dung framework that).

pytest-asyncio KHONG co trong requirements-dev.txt, nhung anyio (dependency
cua httpx/starlette, da co san trong venv) tu dang ky pytest plugin rieng -
dung @pytest.mark.anyio (qua pytestmark module-level) thay vi cai them
dependency moi (verify thuc nghiem: async def test khong mark -> loi "async
def functions are not natively supported"; co mark anyio -> chay & await
that, xac nhan qua ca truong hop assert False bi bat dung)."""
import asyncio
import contextlib
import logging
import time
from unittest.mock import Mock

import pytest

from edge_collector.scheduler import EdgeAgent
from edge_collector.store import Store

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _FakeAgent:
    """Object toi thieu dong vai EdgeAgent cho 2 method dang test - xem
    docstring module. Gan thang ham/hang so cua EdgeAgent (khong subclass,
    khong goi __init__ that)."""
    _drain_serial = EdgeAgent._drain_serial
    _sender_loop = EdgeAgent._sender_loop
    _note_backoff = EdgeAgent._note_backoff
    _clear_backoff = EdgeAgent._clear_backoff
    MAX_MOI_VONG = EdgeAgent.MAX_MOI_VONG
    MAX_CONCURRENT_SERIALS = EdgeAgent.MAX_CONCURRENT_SERIALS
    BACKOFF_BASE_S = EdgeAgent.BACKOFF_BASE_S
    BACKOFF_CAP_S = EdgeAgent.BACKOFF_CAP_S
    BACKOFF_JITTER = EdgeAgent.BACKOFF_JITTER

    def __init__(self, store, manager=None, odoo=None):
        self.store = store
        self.manager = manager or Mock(device_meta=Mock(return_value=None))
        self.odoo = odoo or Mock()
        self._stopping = asyncio.Event()
        self._backoff_until: dict = {}
        self._backoff_delay: dict = {}


def _push_rows(store, serial, n, bid="b1"):
    for i in range(n):
        seq = store.next_seq(serial)
        store.outbox_push(serial, bid, seq, {"items": [{"idx": i}]})


def _mock_store(serials, items_per_serial=1):
    """Store gia CHI cho test _sender_loop (kiem soat duoc thoi diem tra ve
    None de dung dung 1 vong drain) - khac cac test _drain_serial ben duoi
    dung Store SQLite that (tmp_path) vi can dung invariant thu tu that."""
    remaining = {s: items_per_serial for s in serials}
    call_count = {"n": 0}
    store = Mock()

    def outbox_serials():
        call_count["n"] += 1
        return list(serials) if call_count["n"] == 1 else []

    def outbox_oldest(serial):
        if remaining.get(serial, 0) > 0:
            remaining[serial] -= 1
            return {"id": f"{serial}-{remaining[serial]}", "bid": "b", "seq": 1,
                    "payload": {"items": []}}
        return None

    store.outbox_serials = outbox_serials
    store.outbox_oldest = outbox_oldest
    store.outbox_delete = Mock()
    return store


async def _run_sender_loop_until(agent, predicate, timeout=2.0):
    """Chay _sender_loop() nhu 1 task nen, doi predicate() dung roi cancel -
    tranh phai cho that 1s sleep cua nhanh 'khong con gi de gui'."""
    task = asyncio.create_task(agent._sender_loop())
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    try:
        while not predicate() and loop.time() < deadline:
            await asyncio.sleep(0.005)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    assert predicate(), "timeout cho _sender_loop hoan tat vong drain ky vong"


# ----------------------------------------------------------------------
# _drain_serial() — hanh vi tren MOT serial (Store SQLite that, tmp_path)
# ----------------------------------------------------------------------

async def test_drain_serial_stops_at_max_moi_vong(tmp_path):
    store = Store(tmp_path / "t.db")
    _push_rows(store, "S1", 40)
    calls = []

    async def measurements(serial, items, bid, seq, device_meta):
        calls.append(serial)
        return {"ok": True}

    agent = _FakeAgent(store, odoo=Mock(measurements=measurements))

    sent_any = await agent._drain_serial("S1")

    assert sent_any is True
    assert len(calls) == EdgeAgent.MAX_MOI_VONG == 30
    assert store.outbox_count() == 10          # 40 - 30 con lai trong outbox


async def test_drain_serial_stops_on_failure_keeps_order(tmp_path):
    store = Store(tmp_path / "t.db")
    _push_rows(store, "S1", 5)                 # idx 0..4, dung id tang dan
    calls = []

    async def measurements(serial, items, bid, seq, device_meta):
        idx = items[0]["idx"]
        calls.append(idx)
        if idx == 2:
            return {"ok": False, "error": "gia lap loi Odoo"}
        return {"ok": True}

    agent = _FakeAgent(store, odoo=Mock(measurements=measurements))

    sent_any = await agent._drain_serial("S1")

    assert sent_any is True                    # idx 0,1 da gui thanh cong truoc do
    assert calls == [0, 1, 2]                   # dung DUNG luc loi, KHONG thu idx 3,4
    remaining = store.outbox_oldest("S1")
    assert remaining["payload"]["items"][0]["idx"] == 2   # row loi KHONG bi xoa
    assert store.outbox_count() == 3            # idx 2,3,4 con nguyen, dung thu tu


async def test_drain_serial_returns_false_when_outbox_empty(tmp_path):
    store = Store(tmp_path / "t.db")
    odoo = Mock(measurements=Mock())
    agent = _FakeAgent(store, odoo=odoo)

    sent_any = await agent._drain_serial("KHONG-TON-TAI")

    assert sent_any is False
    odoo.measurements.assert_not_called()


async def test_drain_serial_returns_false_when_first_call_fails(tmp_path):
    store = Store(tmp_path / "t.db")
    _push_rows(store, "S1", 2)

    async def measurements(serial, items, bid, seq, device_meta):
        return {"ok": False, "error": "timeout"}

    agent = _FakeAgent(store, odoo=Mock(measurements=measurements))

    sent_any = await agent._drain_serial("S1")

    assert sent_any is False
    assert store.outbox_count() == 2            # khong xoa gi ca


# ----------------------------------------------------------------------
# _sender_loop() — nhieu serial: khong chan nhau + gioi han concurrency
# ----------------------------------------------------------------------

async def test_sender_loop_interleaves_across_serials_no_blocking(tmp_path):
    serials = ["A", "B", "C"]
    store = _mock_store(serials, items_per_serial=3)
    call_order = []

    async def measurements(serial, items, bid, seq, device_meta):
        call_order.append(serial)
        await asyncio.sleep(0.02)
        return {"ok": True}

    agent = _FakeAgent(store, odoo=Mock(measurements=measurements))

    await _run_sender_loop_until(agent, lambda: len(call_order) >= 9)

    assert len(call_order) == 9
    # Ca 3 serial deu duoc goi NGAY tu dau (khong phai xong het A moi toi B)
    # - dung la diem khac biet voi code cu (for-loop tuan tu tung serial).
    assert set(call_order[:3]) == set(serials)


async def test_sender_loop_never_exceeds_max_concurrent_serials(tmp_path):
    serials = [f"S{i}" for i in range(20)]
    store = _mock_store(serials, items_per_serial=1)
    active = {"n": 0, "max": 0}
    completed = {"n": 0}

    async def measurements(serial, items, bid, seq, device_meta):
        active["n"] += 1
        active["max"] = max(active["max"], active["n"])
        await asyncio.sleep(0.05)
        active["n"] -= 1
        completed["n"] += 1
        return {"ok": True}

    agent = _FakeAgent(store, odoo=Mock(measurements=measurements))

    await _run_sender_loop_until(agent, lambda: completed["n"] >= len(serials))

    assert active["max"] <= EdgeAgent.MAX_CONCURRENT_SERIALS
    # == (khong chi <=) de chung minh gioi han THAT SU bi cham toi (20 serial
    # dong thoi + sleep du dai chac chan vuot 8 neu khong co semaphore), chu
    # khong phai "tinh co <=8".
    assert active["max"] == EdgeAgent.MAX_CONCURRENT_SERIALS == 8


async def test_sender_loop_one_serial_failure_does_not_block_others(tmp_path):
    """Regression: code CU (for-loop tuan tu) da khong bi loi nay, nhung sau
    khi doi kien truc sang gather+semaphore can test tuong minh - 1 serial
    loi CHI dung lai CHINH serial do, KHONG lan sang serial khac."""
    store = Store(tmp_path / "t.db")
    _push_rows(store, "A", 2)
    _push_rows(store, "B", 2)
    _push_rows(store, "C", 2)
    call_count = {"n": 0}

    async def measurements(serial, items, bid, seq, device_meta):
        call_count["n"] += 1
        if serial == "B":
            return {"ok": False, "error": "gia lap loi rieng cho B"}
        return {"ok": True}

    agent = _FakeAgent(store, odoo=Mock(measurements=measurements))

    # Ky vong: A gui thanh cong 2 lan, C gui thanh cong 2 lan, B that bai
    # ngay lan dau va dung lai (khong thu them) = 5 lan goi tong cong.
    await _run_sender_loop_until(agent, lambda: call_count["n"] >= 5)

    assert store.outbox_oldest("A") is None     # A da gui het, KHONG bi B chan
    assert store.outbox_oldest("C") is None     # C da gui het, KHONG bi B chan
    remaining_b = store.outbox_oldest("B")
    assert remaining_b is not None
    assert remaining_b["payload"]["items"][0]["idx"] == 0   # dung dau hang doi
    assert store.outbox_count() == 2            # ca 2 row cua B con nguyen


async def test_concurrency_probe_catches_broken_limit(tmp_path):
    """Mutation sanity check THAY THE (KHONG duoc sua edge_collector/
    scheduler.py du chi tam thoi - rang buoc rieng cua task nay). Thay vi
    mutate 1 dong code nghiep vu, nang gioi han qua thuoc tinh CONG KHAI
    MAX_CONCURRENT_SERIALS TREN INSTANCE cua fake agent (_sender_loop doc
    dung `self.MAX_CONCURRENT_SERIALS` de tao semaphore, nen day la duong
    doc y het production, khong phai hack rieng cua test) - xac nhan probe
    o test_sender_loop_never_exceeds_max_concurrent_serials THAT SU do duoc
    muc tang concurrency, khong phai gia (luon bao PASS bat ke gia tri)."""
    serials = [f"S{i}" for i in range(20)]
    store = _mock_store(serials, items_per_serial=1)
    active = {"n": 0, "max": 0}
    completed = {"n": 0}

    async def measurements(serial, items, bid, seq, device_meta):
        active["n"] += 1
        active["max"] = max(active["max"], active["n"])
        await asyncio.sleep(0.05)
        active["n"] -= 1
        completed["n"] += 1
        return {"ok": True}

    agent = _FakeAgent(store, odoo=Mock(measurements=measurements))
    agent.MAX_CONCURRENT_SERIALS = 20          # "mutation" rieng cho instance nay

    await _run_sender_loop_until(agent, lambda: completed["n"] >= len(serials))

    # Neu semaphore van bi "cung" 8 bat ke thuoc tinh instance thi assert nay
    # se FAIL — dieu do CHUNG MINH probe dang do dung gia tri hieu luc, nen
    # test_sender_loop_never_exceeds_max_concurrent_serials moi co y nghia.
    assert active["max"] > EdgeAgent.MAX_CONCURRENT_SERIALS == 8


# ----------------------------------------------------------------------
# _sender_loop() — TONG QUAT HOA loi (24/09): khong chi loi tra ve
# {"ok": False} nhu tren, ma CA khi 1 serial RAISE EXCEPTION (bug khong
# luong truoc o _drain_serial/odoo.measurements, tuong tu lop loi
# OdooClient._parse() da fix) - gather(..., return_exceptions=True) +
# try/except bao ngoai than vong lap phai giu duoc vong lap song, khong
# de mot loi bat ky giet chet han task _sender_loop.
# ----------------------------------------------------------------------

async def test_sender_loop_one_serial_exception_does_not_block_others(tmp_path, caplog):
    """1 serial RAISE (khac voi tra {"ok": False}) qua asyncio.gather(
    return_exceptions=True) CHI bien thanh 1 phan tu Exception trong
    results - cac serial KHAC (A, C) van duoc xu ly/gui binh thuong,
    KHONG bi mat hay bo qua, va exception duoc LOG lai (khong nuot im
    lang) thay vi lam chet vong lap."""
    store = Store(tmp_path / "t.db")
    _push_rows(store, "A", 2)
    _push_rows(store, "B", 2)
    _push_rows(store, "C", 2)
    call_count = {"n": 0}

    async def measurements(serial, items, bid, seq, device_meta):
        call_count["n"] += 1
        if serial == "B":
            raise RuntimeError("loi gia lap khong luong truoc (vd bug tuong lai)")
        return {"ok": True}

    agent = _FakeAgent(store, odoo=Mock(measurements=measurements))

    with caplog.at_level(logging.ERROR, logger="edge.scheduler"):
        # Ky vong: A gui thanh cong 2 lan, C gui thanh cong 2 lan, B raise
        # ngay lan dau (khong catch trong _drain_serial nen bay thang len
        # gather, dung lai serial B tai do - khong thu lai idx con lai).
        await _run_sender_loop_until(agent, lambda: call_count["n"] >= 5)

    assert store.outbox_oldest("A") is None     # A da gui het, KHONG bi B chan
    assert store.outbox_oldest("C") is None     # C da gui het, KHONG bi B chan
    remaining_b = store.outbox_oldest("B")
    assert remaining_b is not None
    assert remaining_b["payload"]["items"][0]["idx"] == 0   # B khong mat du lieu
    assert store.outbox_count() == 2            # ca 2 row cua B con nguyen (chua xoa)

    # Exception KHONG bi nuot im lang - phai co log ERROR nhac ten serial B
    assert any(r.levelno >= logging.ERROR and "B" in r.getMessage()
               for r in caplog.records), caplog.text


# ----------------------------------------------------------------------
# _note_backoff / _clear_backoff (29/09) - backoff rieng theo serial cho
# 429/5xx tu /pcm/api/v1/measurements, xem chu thich BACKOFF_* trong
# scheduler.py.
# ----------------------------------------------------------------------

def test_note_backoff_doubles_delay_on_consecutive_calls(tmp_path):
    store = Store(tmp_path / "t.db")
    agent = _FakeAgent(store)

    agent._note_backoff("S1", None)
    delay_after_first = agent._backoff_delay["S1"]
    agent._note_backoff("S1", None)
    delay_after_second = agent._backoff_delay["S1"]

    assert delay_after_first == pytest.approx(EdgeAgent.BACKOFF_BASE_S * 2)
    assert delay_after_second > delay_after_first
    assert delay_after_second == pytest.approx(EdgeAgent.BACKOFF_BASE_S * 4)


def test_note_backoff_caps_delay_after_many_consecutive_calls(tmp_path):
    """Goi lien tiep du nhieu lan de vuot BACKOFF_CAP_S neu khong co cap ->
    _backoff_delay KHONG duoc vuot cap, va _backoff_until (co jitter +-20%)
    cung khong vuot cap * (1 + jitter)."""
    store = Store(tmp_path / "t.db")
    agent = _FakeAgent(store)

    before = time.monotonic()
    for _ in range(10):
        agent._note_backoff("S1", None)

    assert agent._backoff_delay["S1"] == pytest.approx(EdgeAgent.BACKOFF_CAP_S)
    max_until = before + EdgeAgent.BACKOFF_CAP_S * (1 + EdgeAgent.BACKOFF_JITTER) + 0.05
    assert agent._backoff_until["S1"] <= max_until


def test_note_backoff_uses_retry_after_as_floor_when_larger(tmp_path):
    """delay hien tai (BASE=1.0) nho hon retry_after=20 -> phai dung 20 lam
    san, KHONG dung delay cu nho hon (dung tinh than 'Retry-After la SAN')."""
    store = Store(tmp_path / "t.db")
    agent = _FakeAgent(store)
    before = time.monotonic()

    agent._note_backoff("S1", 20)

    # jittered quanh 20 (+-20%) - toi thieu phai >= 20*(1-jitter)
    min_expected = before + 20 * (1 - EdgeAgent.BACKOFF_JITTER) - 0.05
    assert agent._backoff_until["S1"] >= min_expected
    # va delay cho lan sau la 20*2=40 nhung tran BACKOFF_CAP_S=30
    assert agent._backoff_delay["S1"] == pytest.approx(EdgeAgent.BACKOFF_CAP_S)


def test_note_backoff_ignores_non_positive_retry_after(tmp_path):
    """retry_after None/0/am -> khong duoc dung lam san (san = delay hien
    tai, mac dinh BACKOFF_BASE_S) - tranh truong hop Odoo gui retry_after=0
    hoac am lam vo hieu hoa backoff."""
    store = Store(tmp_path / "t.db")
    agent = _FakeAgent(store)
    before = time.monotonic()

    agent._note_backoff("S1", 0)

    max_expected = before + EdgeAgent.BACKOFF_BASE_S * (1 + EdgeAgent.BACKOFF_JITTER) + 0.05
    assert agent._backoff_until["S1"] <= max_expected


def test_clear_backoff_removes_both_dict_entries(tmp_path):
    store = Store(tmp_path / "t.db")
    agent = _FakeAgent(store)
    agent._note_backoff("S1", 10)
    assert "S1" in agent._backoff_until and "S1" in agent._backoff_delay

    agent._clear_backoff("S1")

    assert "S1" not in agent._backoff_until
    assert "S1" not in agent._backoff_delay


def test_clear_backoff_on_serial_without_backoff_is_noop(tmp_path):
    """Goi _clear_backoff cho serial CHUA tung backoff -> khong raise
    (pop(..., None), khong phai del truc tiep)."""
    store = Store(tmp_path / "t.db")
    agent = _FakeAgent(store)

    agent._clear_backoff("KHONG-TON-TAI")  # khong duoc raise KeyError

    assert "KHONG-TON-TAI" not in agent._backoff_until


# ----------------------------------------------------------------------
# _drain_serial() + backoff (29/09)
# ----------------------------------------------------------------------

async def test_drain_serial_skips_when_in_backoff_window(tmp_path):
    """Serial dang trong thoi gian backoff (_backoff_until o tuong lai) ->
    return False NGAY, KHONG goi odoo.measurements() (spy khong duoc goi)."""
    store = Store(tmp_path / "t.db")
    _push_rows(store, "S1", 3)
    measurements = Mock()
    agent = _FakeAgent(store, odoo=Mock(measurements=measurements))
    agent._backoff_until["S1"] = time.monotonic() + 60.0

    sent_any = await agent._drain_serial("S1")

    assert sent_any is False
    measurements.assert_not_called()
    assert store.outbox_count() == 3            # khong dong gi ca


async def test_drain_serial_applies_backoff_on_429(tmp_path):
    store = Store(tmp_path / "t.db")
    _push_rows(store, "S1", 2)

    async def measurements(serial, items, bid, seq, device_meta):
        return {"ok": False, "error": "edge busy", "status_code": 429, "retry_after": 3}

    agent = _FakeAgent(store, odoo=Mock(measurements=measurements))
    before = time.monotonic()

    sent_any = await agent._drain_serial("S1")

    assert sent_any is False
    assert "S1" in agent._backoff_until
    assert agent._backoff_until["S1"] > before   # phai o TUONG LAI, khong phai 0/qua khu


async def test_drain_serial_applies_backoff_on_5xx(tmp_path):
    store = Store(tmp_path / "t.db")
    _push_rows(store, "S1", 2)

    async def measurements(serial, items, bid, seq, device_meta):
        return {"ok": False, "error": "internal", "status_code": 503}

    agent = _FakeAgent(store, odoo=Mock(measurements=measurements))
    before = time.monotonic()

    sent_any = await agent._drain_serial("S1")

    assert sent_any is False
    assert agent._backoff_until.get("S1", 0.0) > before


async def test_drain_serial_pure_network_error_does_not_apply_backoff(tmp_path):
    """Loi mang thuan tuy (khong co status_code, dung nhu OdooClient._post
    khi httpx.HTTPError) -> KHONG ap dung backoff, giu nguyen nhip cu cua
    _sender_loop (retry ngay vong sau, khong gian nhip them)."""
    store = Store(tmp_path / "t.db")
    _push_rows(store, "S1", 2)

    async def measurements(serial, items, bid, seq, device_meta):
        return {"ok": False, "error": "connection refused"}   # KHONG co status_code

    agent = _FakeAgent(store, odoo=Mock(measurements=measurements))

    sent_any = await agent._drain_serial("S1")

    assert sent_any is False
    assert "S1" not in agent._backoff_until
    assert "S1" not in agent._backoff_delay


async def test_drain_serial_success_clears_backoff_state(tmp_path):
    """Serial tung bi backoff (con state cu) -> gui thanh cong lan nay ->
    _clear_backoff duoc goi, state sach hoan toan."""
    store = Store(tmp_path / "t.db")
    _push_rows(store, "S1", 1)

    async def measurements(serial, items, bid, seq, device_meta):
        return {"ok": True}

    agent = _FakeAgent(store, odoo=Mock(measurements=measurements))
    agent._backoff_until["S1"] = time.monotonic() - 5.0   # da het han tu truoc
    agent._backoff_delay["S1"] = 16.0

    sent_any = await agent._drain_serial("S1")

    assert sent_any is True
    assert "S1" not in agent._backoff_until
    assert "S1" not in agent._backoff_delay


async def test_drain_serial_backoff_is_per_serial_no_cross_effect(tmp_path):
    """Serial A dang backoff, serial B khong - drain B vAn xu ly binh
    thuong, khong bi anh huong boi trang thai backoff cua A (dung comment
    'Backoff rieng theo serial' trong scheduler.py)."""
    store = Store(tmp_path / "t.db")
    _push_rows(store, "A", 2)
    _push_rows(store, "B", 2)
    calls = []

    async def measurements(serial, items, bid, seq, device_meta):
        calls.append(serial)
        return {"ok": True}

    agent = _FakeAgent(store, odoo=Mock(measurements=measurements))
    agent._backoff_until["A"] = time.monotonic() + 60.0

    sent_a = await agent._drain_serial("A")
    sent_b = await agent._drain_serial("B")

    assert sent_a is False
    assert calls == ["B", "B"]                  # A hoan toan khong goi measurements
    assert sent_b is True
    assert store.outbox_count() == 2            # 2 row cua A con nguyen, B da xoa het


async def test_sender_loop_outbox_serials_itself_raising_does_not_kill_loop(caplog):
    """Neu ham self.store.outbox_serials() TU NO raise (loi ngoai du kien,
    vd loi doc SQLite) - khac voi 1 serial rieng le loi ben trong gather -
    try/except bao NGOAI toan bo than vong lap (bao gom ca cau lenh nay)
    phai bat duoc, KHONG de exception bay ra ngoai _sender_loop() lam task
    chet han vinh vien (khong watchdog tu respawn)."""
    call_count = {"n": 0}
    store = Mock()

    def outbox_serials():
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("loi gia lap: DB khong doc duoc")
        return []

    store.outbox_serials = outbox_serials
    agent = _FakeAgent(store)

    with caplog.at_level(logging.ERROR, logger="edge.scheduler"):
        # Doi vong while CHAY DUOC lan thu 2 (goi outbox_serials lan nua) -
        # chinh dieu nay chung minh vong lap khong chet sau lan raise dau.
        await _run_sender_loop_until(agent, lambda: call_count["n"] >= 2, timeout=3.0)

    assert any(r.levelno >= logging.ERROR and "sender_loop" in r.getMessage()
               for r in caplog.records), caplog.text
