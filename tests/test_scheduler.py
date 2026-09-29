# -*- coding: utf-8 -*-
"""Test edge_collector/scheduler.py::EdgeAgent._drain_serial / _sender_loop
(sửa 24/09 - giới hạn concurrency thay vì rút cạn outbox TUẦN TỰ từng serial,
xem chú thích MAX_CONCURRENT_SERIALS trong scheduler.py).

KHÔNG dùng EdgeAgent() thật (__init__ dùng Store thật + SourceManager +
MqttConsumer(self) + OdooClient - quá nhiều side-effect không liên quan) -
2 method đang test (_drain_serial, _sender_loop) là hàm định nghĩa trên
class EdgeAgent, gán được thẳng vào 1 object tối thiểu chỉ có store/manager/
odoo/_stopping/hằng số liên quan (cùng tinh thần với tests/test_mqtt_consumer.py
- gọi method trực tiếp trên object giả, không dùng framework thật).

pytest-asyncio KHÔNG có trong requirements-dev.txt, nhưng anyio (dependency
của httpx/starlette, đã có sẵn trong venv) tự đăng ký pytest plugin riêng -
dùng @pytest.mark.anyio (qua pytestmark module-level) thay vì cài thêm
dependency mới (verify thực nghiệm: async def test không mark -> lỗi "async
def functions are not natively supported"; có mark anyio -> chạy & await
thật, xác nhận qua cả trường hợp assert False bị bắt đúng)."""
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
    """Object tối thiểu đóng vai EdgeAgent cho 2 method đang test - xem
    docstring module. Gán thẳng hàm/hằng số của EdgeAgent (không subclass,
    không gọi __init__ thật)."""
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
    """Store giả CHỈ cho test _sender_loop (kiểm soát được thời điểm trả về
    None để dừng đúng 1 vòng drain) - khác các test _drain_serial bên dưới
    dùng Store SQLite thật (tmp_path) vì cần dùng invariant thứ tự thật."""
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
    """Chạy _sender_loop() như 1 task nền, đợi predicate() đúng rồi cancel -
    tránh phải chờ thật 1s sleep của nhánh 'không còn gì để gửi'."""
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
# _drain_serial() — hành vi trên MỘT serial (Store SQLite thật, tmp_path)
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
    assert store.outbox_count() == 10          # 40 - 30 còn lại trong outbox


async def test_drain_serial_stops_on_failure_keeps_order(tmp_path):
    store = Store(tmp_path / "t.db")
    _push_rows(store, "S1", 5)                 # idx 0..4, dùng id tăng dần
    calls = []

    async def measurements(serial, items, bid, seq, device_meta):
        idx = items[0]["idx"]
        calls.append(idx)
        if idx == 2:
            return {"ok": False, "error": "gia lap loi Odoo"}
        return {"ok": True}

    agent = _FakeAgent(store, odoo=Mock(measurements=measurements))

    sent_any = await agent._drain_serial("S1")

    assert sent_any is True                    # idx 0,1 đã gửi thành công trước đó
    assert calls == [0, 1, 2]                   # dừng ĐÚNG lúc lỗi, KHÔNG thử idx 3,4
    remaining = store.outbox_oldest("S1")
    assert remaining["payload"]["items"][0]["idx"] == 2   # row lỗi KHÔNG bị xóa
    assert store.outbox_count() == 3            # idx 2,3,4 còn nguyên, đúng thứ tự


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
    assert store.outbox_count() == 2            # không xóa gì cả


# ----------------------------------------------------------------------
# _sender_loop() — nhiều serial: không chặn nhau + giới hạn concurrency
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
    # Cả 3 serial đều được gọi NGAY từ đầu (không phải xong hết A mới tới B)
    # - đúng là điểm khác biệt với code cũ (for-loop tuần tự từng serial).
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
    # == (không chỉ <=) để chứng minh giới hạn THẬT SỰ bị chạm tới (20 serial
    # đồng thời + sleep đủ dài chắc chắn vượt 8 nếu không có semaphore), chứ
    # không phải "tình cờ <=8".
    assert active["max"] == EdgeAgent.MAX_CONCURRENT_SERIALS == 8


async def test_sender_loop_one_serial_failure_does_not_block_others(tmp_path):
    """Regression: code CŨ (for-loop tuần tự) đã không bị lỗi này, nhưng sau
    khi đổi kiến trúc sang gather+semaphore cần test tường minh - 1 serial
    lỗi CHỈ dừng lại CHÍNH serial đó, KHÔNG lan sang serial khác."""
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

    # Kỳ vọng: A gửi thành công 2 lần, C gửi thành công 2 lần, B thất bại
    # ngay lần đầu và dừng lại (không thử thêm) = 5 lần gọi tổng cộng.
    await _run_sender_loop_until(agent, lambda: call_count["n"] >= 5)

    assert store.outbox_oldest("A") is None     # A đã gửi hết, KHÔNG bị B chặn
    assert store.outbox_oldest("C") is None     # C đã gửi hết, KHÔNG bị B chặn
    remaining_b = store.outbox_oldest("B")
    assert remaining_b is not None
    assert remaining_b["payload"]["items"][0]["idx"] == 0   # đúng đầu hàng đợi
    assert store.outbox_count() == 2            # cả 2 row của B còn nguyên


async def test_concurrency_probe_catches_broken_limit(tmp_path):
    """Mutation sanity check THAY THẾ (KHÔNG được sửa edge_collector/
    scheduler.py dù chỉ tạm thời - ràng buộc riêng của task này). Thay vì
    mutate 1 dòng code nghiệp vụ, nâng giới hạn qua thuộc tính CÔNG KHAI
    MAX_CONCURRENT_SERIALS TRÊN INSTANCE của fake agent (_sender_loop đọc
    dùng `self.MAX_CONCURRENT_SERIALS` để tạo semaphore, nên đây là đường
    đọc y hệt production, không phải hack riêng của test) - xác nhận probe
    ở test_sender_loop_never_exceeds_max_concurrent_serials THẬT SỰ đo được
    mức tăng concurrency, không phải giả (luôn báo PASS bất kể giá trị)."""
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
    agent.MAX_CONCURRENT_SERIALS = 20          # "mutation" riêng cho instance này

    await _run_sender_loop_until(agent, lambda: completed["n"] >= len(serials))

    # Nếu semaphore vẫn bị "cứng" 8 bất kể thuộc tính instance thì assert này
    # sẽ FAIL — điều đó CHỨNG MINH probe đang đo đúng giá trị hiệu lực, nên
    # test_sender_loop_never_exceeds_max_concurrent_serials mới có ý nghĩa.
    assert active["max"] > EdgeAgent.MAX_CONCURRENT_SERIALS == 8


# ----------------------------------------------------------------------
# _sender_loop() — TỔNG QUÁT HÓA lỗi (24/09): không chỉ lỗi trả về
# {"ok": False} như trên, mà CẢ khi 1 serial RAISE EXCEPTION (bug không
# lường trước ở _drain_serial/odoo.measurements, tương tự lớp lỗi
# OdooClient._parse() đã fix) - gather(..., return_exceptions=True) +
# try/except bao ngoài thân vòng lặp phải giữ được vòng lặp sống, không
# để một lỗi bất kỳ giết chết hẳn task _sender_loop.
# ----------------------------------------------------------------------

async def test_sender_loop_one_serial_exception_does_not_block_others(tmp_path, caplog):
    """1 serial RAISE (khác với trả {"ok": False}) qua asyncio.gather(
    return_exceptions=True) CHỈ biến thành 1 phần tử Exception trong
    results - các serial KHÁC (A, C) vẫn được xử lý/gửi bình thường,
    KHÔNG bị mất hay bỏ qua, và exception được LOG lại (không nuốt im
    lặng) thay vì làm chết vòng lặp."""
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
        # Kỳ vọng: A gửi thành công 2 lần, C gửi thành công 2 lần, B raise
        # ngay lần đầu (không catch trong _drain_serial nên bay thẳng lên
        # gather, dừng lại serial B tại đó - không thử lại idx còn lại).
        await _run_sender_loop_until(agent, lambda: call_count["n"] >= 5)

    assert store.outbox_oldest("A") is None     # A đã gửi hết, KHÔNG bị B chặn
    assert store.outbox_oldest("C") is None     # C đã gửi hết, KHÔNG bị B chặn
    remaining_b = store.outbox_oldest("B")
    assert remaining_b is not None
    assert remaining_b["payload"]["items"][0]["idx"] == 0   # B không mất dữ liệu
    assert store.outbox_count() == 2            # cả 2 row của B còn nguyên (chưa xóa)

    # Exception KHÔNG bị nuốt im lặng - phải có log ERROR nhắc tên serial B
    assert any(r.levelno >= logging.ERROR and "B" in r.getMessage()
               for r in caplog.records), caplog.text


# ----------------------------------------------------------------------
# _note_backoff / _clear_backoff (29/09) - backoff riêng theo serial cho
# 429/5xx từ /pcm/api/v1/measurements, xem chú thích BACKOFF_* trong
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
    """Gọi liên tiếp đủ nhiều lần để vượt BACKOFF_CAP_S nếu không có cap ->
    _backoff_delay KHÔNG được vượt cap, và _backoff_until (có jitter +-20%)
    cũng không vượt cap * (1 + jitter)."""
    store = Store(tmp_path / "t.db")
    agent = _FakeAgent(store)

    before = time.monotonic()
    for _ in range(10):
        agent._note_backoff("S1", None)

    assert agent._backoff_delay["S1"] == pytest.approx(EdgeAgent.BACKOFF_CAP_S)
    max_until = before + EdgeAgent.BACKOFF_CAP_S * (1 + EdgeAgent.BACKOFF_JITTER) + 0.05
    assert agent._backoff_until["S1"] <= max_until


def test_note_backoff_uses_retry_after_as_floor_when_larger(tmp_path):
    """delay hiện tại (BASE=1.0) nhỏ hơn retry_after=20 -> phải dùng 20 làm
    sàn, KHÔNG dùng delay cũ nhỏ hơn (đúng tinh thần 'Retry-After là SÀN')."""
    store = Store(tmp_path / "t.db")
    agent = _FakeAgent(store)
    before = time.monotonic()

    agent._note_backoff("S1", 20)

    # jittered quanh 20 (+-20%) - tối thiểu phải >= 20*(1-jitter)
    min_expected = before + 20 * (1 - EdgeAgent.BACKOFF_JITTER) - 0.05
    assert agent._backoff_until["S1"] >= min_expected
    # và delay cho lần sau là 20*2=40 nhưng trần BACKOFF_CAP_S=30
    assert agent._backoff_delay["S1"] == pytest.approx(EdgeAgent.BACKOFF_CAP_S)


def test_note_backoff_ignores_non_positive_retry_after(tmp_path):
    """retry_after None/0/âm -> không được dùng làm sàn (sàn = delay hiện
    tại, mặc định BACKOFF_BASE_S) - tránh trường hợp Odoo gửi retry_after=0
    hoặc âm làm vô hiệu hóa backoff."""
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
    """Gọi _clear_backoff cho serial CHƯA từng backoff -> không raise
    (pop(..., None), không phải del trực tiếp)."""
    store = Store(tmp_path / "t.db")
    agent = _FakeAgent(store)

    agent._clear_backoff("KHONG-TON-TAI")  # không được raise KeyError

    assert "KHONG-TON-TAI" not in agent._backoff_until


# ----------------------------------------------------------------------
# _drain_serial() + backoff (29/09)
# ----------------------------------------------------------------------

async def test_drain_serial_skips_when_in_backoff_window(tmp_path):
    """Serial đang trong thời gian backoff (_backoff_until ở tương lai) ->
    return False NGAY, KHÔNG gọi odoo.measurements() (spy không được gọi)."""
    store = Store(tmp_path / "t.db")
    _push_rows(store, "S1", 3)
    measurements = Mock()
    agent = _FakeAgent(store, odoo=Mock(measurements=measurements))
    agent._backoff_until["S1"] = time.monotonic() + 60.0

    sent_any = await agent._drain_serial("S1")

    assert sent_any is False
    measurements.assert_not_called()
    assert store.outbox_count() == 3            # không đụng gì cả


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
    assert agent._backoff_until["S1"] > before   # phải ở TƯƠNG LAI, không phải 0/quá khứ


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
    """Lỗi mạng thuần túy (không có status_code, đúng như OdooClient._post
    khi httpx.HTTPError) -> KHÔNG áp dụng backoff, giữ nguyên nhịp cũ của
    _sender_loop (retry ngay vòng sau, không giãn nhịp thêm)."""
    store = Store(tmp_path / "t.db")
    _push_rows(store, "S1", 2)

    async def measurements(serial, items, bid, seq, device_meta):
        return {"ok": False, "error": "connection refused"}   # KHÔNG có status_code

    agent = _FakeAgent(store, odoo=Mock(measurements=measurements))

    sent_any = await agent._drain_serial("S1")

    assert sent_any is False
    assert "S1" not in agent._backoff_until
    assert "S1" not in agent._backoff_delay


async def test_drain_serial_success_clears_backoff_state(tmp_path):
    """Serial từng bị backoff (còn state cũ) -> gửi thành công lần này ->
    _clear_backoff được gọi, state sạch hoàn toàn."""
    store = Store(tmp_path / "t.db")
    _push_rows(store, "S1", 1)

    async def measurements(serial, items, bid, seq, device_meta):
        return {"ok": True}

    agent = _FakeAgent(store, odoo=Mock(measurements=measurements))
    agent._backoff_until["S1"] = time.monotonic() - 5.0   # đã hết hạn từ trước
    agent._backoff_delay["S1"] = 16.0

    sent_any = await agent._drain_serial("S1")

    assert sent_any is True
    assert "S1" not in agent._backoff_until
    assert "S1" not in agent._backoff_delay


async def test_drain_serial_backoff_is_per_serial_no_cross_effect(tmp_path):
    """Serial A đang backoff, serial B không - drain B vẫn xử lý bình
    thường, không bị ảnh hưởng bởi trạng thái backoff của A (đúng comment
    'Backoff riêng theo serial' trong scheduler.py)."""
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
    assert calls == ["B", "B"]                  # A hoàn toàn không gọi measurements
    assert sent_b is True
    assert store.outbox_count() == 2            # 2 row của A còn nguyên, B đã xóa hết


async def test_sender_loop_outbox_serials_itself_raising_does_not_kill_loop(caplog):
    """Nếu hàm self.store.outbox_serials() TỰ NÓ raise (lỗi ngoài dự kiến,
    vd lỗi đọc SQLite) - khác với 1 serial riêng lẻ lỗi bên trong gather -
    try/except bao NGOÀI toàn bộ thân vòng lặp (bao gồm cả câu lệnh này)
    phải bắt được, KHÔNG để exception bay ra ngoài _sender_loop() làm task
    chết hẳn vĩnh viễn (không watchdog tự respawn)."""
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
        # Đợi vòng while CHẠY ĐƯỢC lần thứ 2 (gọi outbox_serials lần nữa) -
        # chính điều này chứng minh vòng lặp không chết sau lần raise đầu.
        await _run_sender_loop_until(agent, lambda: call_count["n"] >= 2, timeout=3.0)

    assert any(r.levelno >= logging.ERROR and "sender_loop" in r.getMessage()
               for r in caplog.records), caplog.text
