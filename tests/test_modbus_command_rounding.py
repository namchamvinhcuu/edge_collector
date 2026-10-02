# -*- coding: utf-8 -*-
"""Regression test cho bug làm tròn/cắt cụt (truncate) khi ghi setpoint xuống
thiết bị Modbus thật qua ModbusDriver.command().

Bug: trước fix, `_encode_32()` và nhánh i16/u16 trong `command()` dùng
`int(raw)` để ép `raw = (value - offset) / scale` về số nguyên trước khi ghi
xuống register. Sai số dấu phẩy động làm `raw` ra vd 42.99999999999999 thay vì
43.0 -> int() CẮT CỤT thành 42, ghi SAI 1 đơn vị xuống thiết bị vật lý thật
(biến tần/PLC), KHÔNG có exception/log nào báo.

Đã fix: đổi `int()` -> `round()` ở 3 chỗ (edge_collector/drivers/modbus.py
dòng 59/61/179 - số dòng có thể lệch +-1..2 sau edit).

Các giá trị intended/scale dưới đây là KẾT QUẢ QUÉT THỰC NGHIỆM thật (không
đoán) - xác nhận `int(raw) != intended` thật sự xảy ra với float thực tế của
Python, đảm bảo test không phải giả định suông.
"""
import struct

import pytest

from edge_collector.drivers.modbus import ModbusDriver, _encode_32

# pytest-asyncio KHÔNG có trong requirements-dev.txt; dự án dùng anyio (đã có
# sẵn qua httpx/starlette) - cùng convention với tests/test_scheduler.py.
pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _OkResponse:
    """pymodbus trả response object (không phải exception) kể cả khi PLC từ
    chối - command() gọi rr.isError() nên fake phải có method này."""

    def isError(self):
        return False


class _FakeAsyncModbusClient:
    """Fake tối thiểu cho pymodbus AsyncModbusTcpClient - chỉ ghi lại lời gọi
    write_register()/write_registers(), không thật sự nối mạng/thiết bị."""

    def __init__(self):
        self.calls = []            # [(method, addr, value_or_values, device_id), ...]

    async def write_register(self, addr, value, device_id=None):
        self.calls.append(("write_register", addr, value, device_id))
        return _OkResponse()

    async def write_registers(self, addr, values, device_id=None):
        self.calls.append(("write_registers", addr, values, device_id))
        return _OkResponse()


def _make_driver(dtype: str, scale: float, offset: float = 0.0, reg: str = "HR40001"):
    """Dùng ModbusDriver thật (không mock), chỉ bypass _connect() bằng cách
    gán thẳng self._client = fake - command() không gọi _connect()."""
    source_cfg = {
        "code": "SRC1",
        "kind": "modbus_tcp",
        "unit_id": 1,
        "points": [
            {"ch": "speed", "reg": reg, "dtype": dtype, "scale": scale,
             "offset": offset, "write": True},
        ],
    }
    driver = ModbusDriver(source_cfg, channels=[], on_reading=lambda *a, **kw: None)
    driver._client = _FakeAsyncModbusClient()
    return driver


def _expected_16bit_wire(intended: int) -> int:
    """Giá trị raw 16-bit (two's complement dùng cho cả i16/u16) mà
    round(raw) & 0xFFFF PHẢI cho ra khi raw thực chất = intended."""
    return intended & 0xFFFF


def _expected_i32_regs(intended: int):
    return list(struct.unpack(">HH", struct.pack(">i", intended)))


def _expected_u32_regs(intended: int):
    return list(struct.unpack(">HH", struct.pack(">I", intended & 0xFFFFFFFF)))


# ---------------------------------------------------------------------------
# 1) Happy-path regression qua command() - dtype u16 (mặc định)
# ---------------------------------------------------------------------------

async def test_command_u16_rounds_float_error_instead_of_truncating():
    """scale=0.1, value=4.3 -> raw = 4.3/0.1 = 42.99999999999999 (quét thực
    nghiệm xác nhận). int(raw) sẽ ra 42 (SAI - lệch 1 đơn vị), round(raw) phải
    ra 43 (đúng, khớp intended)."""
    intended = 43
    value = intended * 0.1          # = 4.3, tái tạo đúng công thức command()
    raw = value / 0.1
    assert int(raw) != intended and round(raw) == intended  # xác nhận tiền đề bug có thật

    driver = _make_driver(dtype="u16", scale=0.1)
    result = await driver.command("speed", "write", value)

    assert result == {"ok": True, "status": "ok"}
    assert driver._client.calls == [
        ("write_register", 0, _expected_16bit_wire(intended), 1),
    ]


# ---------------------------------------------------------------------------
# 2) Edge case: giá trị ÂM cho i16 - round() phải xử lý dấu đúng, không lệch
# ---------------------------------------------------------------------------

async def test_command_i16_negative_rounds_correctly_not_truncated_toward_zero():
    """scale=0.1, intended=-1531 -> raw = -1531*0.1/0.1 = -1530.9999999999998
    (quét thực nghiệm). int() cắt về phía 0 -> -1530 (SAI, lệch 1 đơn vị và
    sai hướng với số âm). round() phải ra đúng -1531."""
    intended = -1531
    value = intended * 0.1
    raw = value / 0.1
    assert int(raw) != intended and round(raw) == intended

    driver = _make_driver(dtype="i16", scale=0.1)
    result = await driver.command("speed", "write", value)

    assert result == {"ok": True, "status": "ok"}
    assert driver._client.calls == [
        ("write_register", 0, _expected_16bit_wire(intended), 1),
    ]


# ---------------------------------------------------------------------------
# 3) dtype i32 qua command() (nhánh else -> _encode_32) - đường tích hợp
# ---------------------------------------------------------------------------

async def test_command_i32_rounds_float_error_via_encode_32():
    intended = 102406
    value = intended * 0.01
    raw = value / 0.01
    assert int(raw) != intended and round(raw) == intended

    driver = _make_driver(dtype="i32", scale=0.01)
    result = await driver.command("speed", "write", value)

    assert result == {"ok": True, "status": "ok"}
    assert driver._client.calls == [
        ("write_registers", 0, _expected_i32_regs(intended), 1),
    ]


# ---------------------------------------------------------------------------
# 4) Edge case âm cho i32 - dùng trực tiếp _encode_32() (unit test thuần)
# ---------------------------------------------------------------------------

def test_encode_32_i32_negative_rounds_correctly():
    intended = -262143
    value = intended * 0.01
    raw = value / 0.01
    assert int(raw) != intended and round(raw) == intended

    assert _encode_32("i32", raw) == _expected_i32_regs(intended)


# ---------------------------------------------------------------------------
# 5) dtype u32 - dùng trực tiếp _encode_32() (unit test thuần)
# ---------------------------------------------------------------------------

def test_encode_32_u32_rounds_float_error():
    intended = 43
    value = intended * 0.1
    raw = value / 0.1
    assert int(raw) != intended and round(raw) == intended

    assert _encode_32("u32", raw) == _expected_u32_regs(intended)


# ---------------------------------------------------------------------------
# 6) f32 KHÔNG bị bug này (dùng float(value) từ đầu, không ép int) - guard
#    để đảm bảo không ai vô tình đổi nhánh f32 sang round()/int() sau này.
# ---------------------------------------------------------------------------

def test_encode_32_f32_keeps_fractional_value_not_rounded_to_integer():
    """42.5 biểu diễn CHÍNH XÁC được trong float32 (không dính lỗi làm tròn
    precision) - nếu ai vô tình đổi f32 sang round()/int() như 2 nhánh kia,
    kết quả sẽ thành 42 hoặc 43 thay vì giữ nguyên 42.5."""
    value = 42.5
    regs = _encode_32("f32", value)
    decoded = struct.unpack(">f", struct.pack(">HH", *regs))[0]
    assert decoded == value          # giữ nguyên phần thập phân, KHÔNG làm tròn về số nguyên
