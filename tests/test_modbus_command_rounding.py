# -*- coding: utf-8 -*-
"""Regression test cho bug lam tron/cat cut (truncate) khi ghi setpoint xuong
thiet bi Modbus that qua ModbusDriver.command().

Bug: truoc fix, `_encode_32()` va nhanh i16/u16 trong `command()` dung
`int(raw)` de ep `raw = (value - offset) / scale` ve so nguyen truoc khi ghi
xuong register. Sai so dau phay dong lam `raw` ra vd 42.99999999999999 thay vi
43.0 -> int() CAT CUT thanh 42, ghi SAI 1 don vi xuong thiet bi vat ly that
(bien tan/PLC), KHONG co exception/log nao bao.

Da fix: doi `int()` -> `round()` o 3 cho (edge_collector/drivers/modbus.py
dong 59/61/179 - so dong co the lech +-1..2 sau edit).

Cac gia tri intended/scale duoi day la KET QUA QUET THUC NGHIEM that (khong
doan) - xac nhan `int(raw) != intended` that su xay ra voi float thuc te cua
Python, dam bao test khong phai gia dinh suong.
"""
import struct

import pytest

from edge_collector.drivers.modbus import ModbusDriver, _encode_32

# pytest-asyncio KHONG co trong requirements-dev.txt; du an dung anyio (da co
# san qua httpx/starlette) - cung convention voi tests/test_scheduler.py.
pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _FakeAsyncModbusClient:
    """Fake toi thieu cho pymodbus AsyncModbusTcpClient - chi ghi lai loi goi
    write_register()/write_registers(), khong that su noi mang/thiet bi."""

    def __init__(self):
        self.calls = []            # [(method, addr, value_or_values, device_id), ...]

    async def write_register(self, addr, value, device_id=None):
        self.calls.append(("write_register", addr, value, device_id))
        return object()

    async def write_registers(self, addr, values, device_id=None):
        self.calls.append(("write_registers", addr, values, device_id))
        return object()


def _make_driver(dtype: str, scale: float, offset: float = 0.0, reg: str = "HR40001"):
    """Dung ModbusDriver that (khong mock), chi bypass _connect() bang cach
    gan thang self._client = fake - command() khong goi _connect()."""
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
    """Gia tri raw 16-bit (two's complement dung cho ca i16/u16) ma
    round(raw) & 0xFFFF PHAI cho ra khi raw thuc chat = intended."""
    return intended & 0xFFFF


def _expected_i32_regs(intended: int):
    return list(struct.unpack(">HH", struct.pack(">i", intended)))


def _expected_u32_regs(intended: int):
    return list(struct.unpack(">HH", struct.pack(">I", intended & 0xFFFFFFFF)))


# ---------------------------------------------------------------------------
# 1) Happy-path regression qua command() - dtype u16 (mac dinh)
# ---------------------------------------------------------------------------

async def test_command_u16_rounds_float_error_instead_of_truncating():
    """scale=0.1, value=4.3 -> raw = 4.3/0.1 = 42.99999999999999 (quet thuc
    nghiem xac nhan). int(raw) se ra 42 (SAI - lech 1 don vi), round(raw) phai
    ra 43 (dung, khop intended)."""
    intended = 43
    value = intended * 0.1          # = 4.3, tai tao dung cong thuc command()
    raw = value / 0.1
    assert int(raw) != intended and round(raw) == intended  # xac nhan tien de bug co that

    driver = _make_driver(dtype="u16", scale=0.1)
    result = await driver.command("speed", "write", value)

    assert result == {"ok": True, "status": "ok"}
    assert driver._client.calls == [
        ("write_register", 0, _expected_16bit_wire(intended), 1),
    ]


# ---------------------------------------------------------------------------
# 2) Edge case: gia tri AM cho i16 - round() phai xu ly dau dung, khong lech
# ---------------------------------------------------------------------------

async def test_command_i16_negative_rounds_correctly_not_truncated_toward_zero():
    """scale=0.1, intended=-1531 -> raw = -1531*0.1/0.1 = -1530.9999999999998
    (quet thuc nghiem). int() cat ve phia 0 -> -1530 (SAI, lech 1 don vi va
    sai huong voi so am). round() phai ra dung -1531."""
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
# 3) dtype i32 qua command() (nhanh else -> _encode_32) - duong tich hop
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
# 4) Edge case am cho i32 - dung truc tiep _encode_32() (unit test thuan)
# ---------------------------------------------------------------------------

def test_encode_32_i32_negative_rounds_correctly():
    intended = -262143
    value = intended * 0.01
    raw = value / 0.01
    assert int(raw) != intended and round(raw) == intended

    assert _encode_32("i32", raw) == _expected_i32_regs(intended)


# ---------------------------------------------------------------------------
# 5) dtype u32 - dung truc tiep _encode_32() (unit test thuan)
# ---------------------------------------------------------------------------

def test_encode_32_u32_rounds_float_error():
    intended = 43
    value = intended * 0.1
    raw = value / 0.1
    assert int(raw) != intended and round(raw) == intended

    assert _encode_32("u32", raw) == _expected_u32_regs(intended)


# ---------------------------------------------------------------------------
# 6) f32 KHONG bi bug nay (dung float(value) tu dau, khong ep int) - guard
#    de dam bao khong ai vo tinh doi nhanh f32 sang round()/int() sau nay.
# ---------------------------------------------------------------------------

def test_encode_32_f32_keeps_fractional_value_not_rounded_to_integer():
    """42.5 bieu dien CHINH XAC duoc trong float32 (khong dinh loi lam tron
    precision) - neu ai vo tinh doi f32 sang round()/int() nhu 2 nhanh kia,
    ket qua se thanh 42 hoac 43 thay vi giu nguyen 42.5."""
    value = 42.5
    regs = _encode_32("f32", value)
    decoded = struct.unpack(">f", struct.pack(">HH", *regs))[0]
    assert decoded == value          # giu nguyen phan thap phan, KHONG lam tron ve so nguyen
