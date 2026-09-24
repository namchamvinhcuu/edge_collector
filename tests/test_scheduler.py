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
    MAX_MOI_VONG = EdgeAgent.MAX_MOI_VONG
    MAX_CONCURRENT_SERIALS = EdgeAgent.MAX_CONCURRENT_SERIALS

    def __init__(self, store, manager=None, odoo=None):
        self.store = store
        self.manager = manager or Mock(device_meta=Mock(return_value=None))
        self.odoo = odoo or Mock()
        self._stopping = asyncio.Event()


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
