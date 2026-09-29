# -*- coding: utf-8 -*-
"""Nguồn modbus_tcp / modbus_rtu — đọc theo 'points' trong pcm_source._as_config().

Địa chỉ thanh ghi dùng ký hiệu Modicon quen thuộc (SAMPLE_TAGS trong api_iot.py):
    HR40001.. -> Holding Register (func 03), địa chỉ 0-based = số - 40001
    IR30001.. -> Input Register   (func 04), địa chỉ 0-based = số - 30001
    số nguyên trần -> Holding Register, địa chỉ = chính số đó

Một nguồn = một slave (pcm.source.unit_id) — nhiều slave trên cùng bus RTU thì
khai báo nhiều pcm.source, mỗi cái một endpoint/serial port riêng.
"""
import asyncio
import logging
import re
import struct

from .base import SourceDriver

_logger = logging.getLogger("edge.modbus")

_HR = re.compile(r"^HR0*([0-9]+)$", re.I)
_IR = re.compile(r"^IR0*([0-9]+)$", re.I)


def _parse_reg(tag: str):
    tag = (tag or "").strip()
    m = _HR.match(tag)
    if m:
        return 3, int(m.group(1)) - 40001
    m = _IR.match(tag)
    if m:
        return 4, int(m.group(1)) - 30001
    try:
        return 3, int(tag)
    except ValueError:
        return 3, 0


def _decode(regs, dtype: str):
    dtype = (dtype or "u16").lower()
    if dtype in ("i16", "u16"):
        v = regs[0]
        return v - 0x10000 if dtype == "i16" and v >= 0x8000 else v
    raw = struct.pack(">HH", regs[0] & 0xFFFF, regs[1] & 0xFFFF)
    if dtype == "f32":
        return struct.unpack(">f", raw)[0]
    if dtype == "i32":
        return struct.unpack(">i", raw)[0]
    if dtype == "u32":
        return struct.unpack(">I", raw)[0]
    return regs[0]


def _encode_32(dtype: str, value: float):
    dtype = (dtype or "u16").lower()
    if dtype == "f32":
        raw = struct.pack(">f", float(value))
    elif dtype == "i32":
        # round(), KHÔNG int() - int() cắt cụt về 0 (truncate), sai số dấu
        # phẩy động của "(value - offset) / scale" ở command() có thể ra
        # 2.9999999999999996 thay vì 3.0 -> int() ghi nhầm 2 xuống thiết bị
        # thật (biến tần/PLC), không có exception/log nào báo - xem
        # Fix-History 2026-09-25 (phát hiện qua node_agent copy code này).
        raw = struct.pack(">i", round(value))
    else:
        raw = struct.pack(">I", round(value) & 0xFFFFFFFF)
    hi, lo = struct.unpack(">HH", raw)
    return [hi, lo]


class ModbusDriver(SourceDriver):
    kind = "modbus"

    def __init__(self, source_cfg, channels, on_reading):
        super().__init__(source_cfg, channels, on_reading)
        self._client = None
        self._task = None
        self._stop = asyncio.Event()
        self._unit = source_cfg.get("unit_id", 1) or 1
        self._last_err: dict = {}

    async def _connect(self):
        if self.cfg["kind"] == "modbus_tcp":
            from pymodbus.client import AsyncModbusTcpClient
            host, _, port = (self.cfg.get("endpoint") or "").partition(":")
            host, port = host or "127.0.0.1", int(port or 502)
            _logger.info("%s: kết nối modbus_tcp %s:%s (unit=%s)", self.code, host, port, self._unit)
            self._client = AsyncModbusTcpClient(host, port=port)
        else:
            from pymodbus.client import AsyncModbusSerialClient
            endpoint, baud = self.cfg.get("endpoint") or "/dev/ttyUSB0", self.cfg.get("baud") or 9600
            _logger.info("%s: kết nối modbus_rtu %s @%s baud (unit=%s)",
                         self.code, endpoint, baud, self._unit)
            self._client = AsyncModbusSerialClient(endpoint, baudrate=baud)
        await self._client.connect()
        if not self._client.connected:
            _logger.warning("%s: không kết nối được %s", self.code, self.cfg.get("endpoint"))
            raise ConnectionError("không kết nối được %s" % self.cfg.get("endpoint"))
        _logger.info("%s: đã kết nối", self.code)

    async def start(self) -> None:
        self._stop.clear()
        await self._connect()
        self._mark_online()
        points = self.cfg.get("points") or []
        _logger.info("%s: bắt đầu đọc %d điểm, mỗi %dms — %s", self.code, len(points),
                     self.cfg.get("poll_ms") or 1000,
                     ", ".join("%s@%s(%s)" % (p.get("ch"), p.get("reg"), p.get("dtype")) for p in points))
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):            # noqa: BLE001
                pass
            self._task = None
        if self._client:
            self._client.close()
            self._client = None
        _logger.info("%s: đã dừng", self.code)

    async def _read_point(self, point: dict):
        func, addr = _parse_reg(point.get("reg"))
        count = 1 if (point.get("dtype") or "u16").lower() in ("i16", "u16") else 2
        rr = (await self._client.read_holding_registers(addr, count=count, device_id=self._unit)) \
            if func == 3 else \
            (await self._client.read_input_registers(addr, count=count, device_id=self._unit))
        if rr.isError():
            raise IOError(str(rr))
        raw = _decode(rr.registers, point.get("dtype"))
        return raw * (point.get("scale") or 1) + (point.get("offset") or 0)

    async def _loop(self):
        poll_ms = self.cfg.get("poll_ms") or 1000
        try:
            tick = 0
            while not self._stop.is_set():
                tick += 1
                for point in self.cfg.get("points") or []:
                    ch = point.get("ch")
                    if not ch or point.get("write"):
                        continue
                    try:
                        v = await self._read_point(point)
                        self._mark_online()
                        if self._last_err.pop(ch, None) is not None:
                            _logger.info("%s.%s: đọc lại được, v=%s", self.code, ch, v)
                        else:
                            _logger.debug("%s.%s: v=%s", self.code, ch, v)
                        self._emit(ch, v=round(v, 6), q=0, stable=True)
                    except Exception as exc:                          # noqa: BLE001
                        msg = str(exc)[:200]
                        if self._last_err.get(ch) != msg:
                            _logger.warning("%s.%s (reg=%s dtype=%s unit=%s): lỗi đọc modbus: %s",
                                            self.code, ch, point.get("reg"), point.get("dtype"),
                                            self._unit, msg)
                            self._last_err[ch] = msg
                        self._mark_error(msg)
                        self._emit(ch, q=2)
                if tick == 1:
                    _logger.info("%s: đã chạy vòng đọc đầu tiên (%d điểm)", self.code,
                                 len(self.cfg.get("points") or []))
                await asyncio.sleep(max(0.2, poll_ms / 1000.0))
        except asyncio.CancelledError:
            raise
        except Exception:                                              # noqa: BLE001
            _logger.exception("%s: vòng đọc CRASH — dừng hẳn, không đọc nữa từ đây", self.code)
            self._mark_error("vòng đọc crash — xem log")

    async def command(self, channel_code: str, cmd: str, value=None) -> dict:
        point = next((p for p in self.cfg.get("points") or [] if p.get("ch") == channel_code), None)
        if not point:
            return {"ok": False, "error": "không tìm thấy kênh %s trên nguồn này" % channel_code}
        if cmd != "write" or value is None:
            return {"ok": False, "error": "modbus chỉ hỗ trợ cmd=write kèm value"}
        func, addr = _parse_reg(point.get("reg"))
        raw = (float(value) - (point.get("offset") or 0)) / (point.get("scale") or 1)
        try:
            dtype = (point.get("dtype") or "u16").lower()
            if dtype in ("i16", "u16"):
                # round(), KHÔNG int() - cùng lý do với _encode_32() ở trên.
                await self._client.write_register(addr, round(raw) & 0xFFFF, device_id=self._unit)
            else:
                await self._client.write_registers(addr, _encode_32(dtype, raw), device_id=self._unit)
            _logger.info("%s.%s: ghi reg=%s value=%s (raw=%s)", self.code, channel_code,
                        point.get("reg"), value, raw)
            return {"ok": True, "status": "ok"}
        except Exception as exc:                                  # noqa: BLE001
            _logger.warning("%s.%s: lỗi ghi modbus: %s", self.code, channel_code, exc)
            return {"ok": False, "error": str(exc)[:200]}

    async def browse(self, node_id=None, path=None) -> dict:
        nodes = [{"id": p.get("reg"), "name": p.get("ch") or p.get("reg"), "dtype": p.get("dtype"),
                  "access": "w" if p.get("write") else "r", "children": False}
                 for p in self.cfg.get("points") or []]
        return {"ok": True, "nodes": nodes}
