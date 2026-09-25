# -*- coding: utf-8 -*-
"""Nguon modbus_tcp / modbus_rtu — doc theo 'points' trong pcm_source._as_config().

Dia chi thanh ghi dung ky hieu Modicon quen thuoc (SAMPLE_TAGS trong api_iot.py):
    HR40001.. -> Holding Register (func 03), dia chi 0-based = so - 40001
    IR30001.. -> Input Register   (func 04), dia chi 0-based = so - 30001
    so nguyen tran -> Holding Register, dia chi = chinh so do

Mot nguon = mot slave (pcm.source.unit_id) — nhieu slave tren cung bus RTU thi
khai bao nhieu pcm.source, moi cai mot endpoint/serial port rieng.
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
        # round(), KHONG int() - int() cat cut ve 0 (truncate), sai so dau
        # phay dong cua "(value - offset) / scale" o command() co the ra
        # 2.9999999999999996 thay vi 3.0 -> int() ghi nham 2 xuong thiet bi
        # that (bien tan/PLC), khong co exception/log nao bao - xem
        # Fix-History 2026-09-25 (phat hien qua node_agent copy code nay).
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
            _logger.info("%s: ket noi modbus_tcp %s:%s (unit=%s)", self.code, host, port, self._unit)
            self._client = AsyncModbusTcpClient(host, port=port)
        else:
            from pymodbus.client import AsyncModbusSerialClient
            endpoint, baud = self.cfg.get("endpoint") or "/dev/ttyUSB0", self.cfg.get("baud") or 9600
            _logger.info("%s: ket noi modbus_rtu %s @%s baud (unit=%s)",
                         self.code, endpoint, baud, self._unit)
            self._client = AsyncModbusSerialClient(endpoint, baudrate=baud)
        await self._client.connect()
        if not self._client.connected:
            _logger.warning("%s: khong ket noi duoc %s", self.code, self.cfg.get("endpoint"))
            raise ConnectionError("khong ket noi duoc %s" % self.cfg.get("endpoint"))
        _logger.info("%s: da ket noi", self.code)

    async def start(self) -> None:
        self._stop.clear()
        await self._connect()
        self._mark_online()
        points = self.cfg.get("points") or []
        _logger.info("%s: bat dau doc %d diem, moi %dms — %s", self.code, len(points),
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
        _logger.info("%s: da dung", self.code)

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
                            _logger.info("%s.%s: doc lai duoc, v=%s", self.code, ch, v)
                        else:
                            _logger.debug("%s.%s: v=%s", self.code, ch, v)
                        self._emit(ch, v=round(v, 6), q=0, stable=True)
                    except Exception as exc:                          # noqa: BLE001
                        msg = str(exc)[:200]
                        if self._last_err.get(ch) != msg:
                            _logger.warning("%s.%s (reg=%s dtype=%s unit=%s): loi doc modbus: %s",
                                            self.code, ch, point.get("reg"), point.get("dtype"),
                                            self._unit, msg)
                            self._last_err[ch] = msg
                        self._mark_error(msg)
                        self._emit(ch, q=2)
                if tick == 1:
                    _logger.info("%s: da chay vong doc dau tien (%d diem)", self.code,
                                 len(self.cfg.get("points") or []))
                await asyncio.sleep(max(0.2, poll_ms / 1000.0))
        except asyncio.CancelledError:
            raise
        except Exception:                                              # noqa: BLE001
            _logger.exception("%s: vong doc CRASH — dung han, khong doc nua tu day", self.code)
            self._mark_error("vong doc crash — xem log")

    async def command(self, channel_code: str, cmd: str, value=None) -> dict:
        point = next((p for p in self.cfg.get("points") or [] if p.get("ch") == channel_code), None)
        if not point:
            return {"ok": False, "error": "khong tim thay kenh %s tren nguon nay" % channel_code}
        if cmd != "write" or value is None:
            return {"ok": False, "error": "modbus chi ho tro cmd=write kem value"}
        func, addr = _parse_reg(point.get("reg"))
        raw = (float(value) - (point.get("offset") or 0)) / (point.get("scale") or 1)
        try:
            dtype = (point.get("dtype") or "u16").lower()
            if dtype in ("i16", "u16"):
                # round(), KHONG int() - cung ly do voi _encode_32() o tren.
                await self._client.write_register(addr, round(raw) & 0xFFFF, device_id=self._unit)
            else:
                await self._client.write_registers(addr, _encode_32(dtype, raw), device_id=self._unit)
            _logger.info("%s.%s: ghi reg=%s value=%s (raw=%s)", self.code, channel_code,
                        point.get("reg"), value, raw)
            return {"ok": True, "status": "ok"}
        except Exception as exc:                                  # noqa: BLE001
            _logger.warning("%s.%s: loi ghi modbus: %s", self.code, channel_code, exc)
            return {"ok": False, "error": str(exc)[:200]}

    async def browse(self, node_id=None, path=None) -> dict:
        nodes = [{"id": p.get("reg"), "name": p.get("ch") or p.get("reg"), "dtype": p.get("dtype"),
                  "access": "w" if p.get("write") else "r", "children": False}
                 for p in self.cfg.get("points") or []]
        return {"ok": True, "nodes": nodes}
