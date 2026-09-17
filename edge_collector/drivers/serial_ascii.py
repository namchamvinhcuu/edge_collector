# -*- coding: utf-8 -*-
"""Nguon 'serial' — cong USB serial cam thang vao edge (can · caliper ...),
theo cu phap cua PCM 계측기 프로파일 (pcm.serial.profile._as_config()).

Mot nguon serial = MOT kenh (pcm_source.py: 'ch = self.channel_ids[:1]').
profile.link:
    ascii   — moi dong ket thuc bang 'terminator', boc tach bang 'pattern'
              (regex), gia tri o value_group, don vi o unit_group, on dinh
              quyet dinh boi stable_group == stable_ok.
    modbus  — thiet bi RS-485 mot thanh ghi duy nhat tren cong serial nay.
"""
import asyncio
import re
import struct

import serial as pyserial

from .base import SourceDriver


class SerialDriver(SourceDriver):
    kind = "serial"

    def __init__(self, source_cfg, channels, on_reading, profile: dict):
        super().__init__(source_cfg, channels, on_reading)
        self.profile = profile or {}
        self._ser = None
        self._mb = None
        self._task = None
        self._stop = asyncio.Event()
        self._ch = source_cfg.get("ch") or (channels[0]["code"] if channels else "")

    async def start(self) -> None:
        self._stop.clear()
        if not self.profile:
            raise ValueError("nguon serial '%s' chua gan pcm.serial.profile" % self.code)
        if self.profile.get("link") == "modbus":
            await self._start_modbus()
        else:
            await self._start_ascii()
        self._mark_online()
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
        if self._ser:
            self._ser.close()
            self._ser = None
        if self._mb:
            self._mb.close()
            self._mb = None

    # -- ASCII (can dien tu, caliper ...) ------------------------------
    async def _start_ascii(self):
        p = self.profile
        parity_map = {"none": pyserial.PARITY_NONE, "even": pyserial.PARITY_EVEN,
                      "odd": pyserial.PARITY_ODD}
        self._ser = pyserial.Serial(
            port=self.cfg.get("endpoint") or "/dev/ttyUSB0",
            baudrate=self.cfg.get("baud") or p.get("baud_rate") or 9600,
            bytesize=p.get("data_bits") or 8,
            stopbits=p.get("stop_bits") or 1,
            parity=parity_map.get(p.get("parity") or "none", pyserial.PARITY_NONE),
            timeout=1.0,
        )
        self._pattern = re.compile(p.get("pattern") or r"(.+)")
        self._term = (p.get("terminator") or "\r\n").encode()

    async def _read_line_ascii(self) -> str:
        loop = asyncio.get_event_loop()
        buf = b""
        while not buf.endswith(self._term):
            b = await loop.run_in_executor(None, self._ser.read, 1)
            if not b:
                return ""
            buf += b
        return buf[:-len(self._term)].decode(errors="replace")

    def _parse_ascii(self, line: str):
        m = self._pattern.match(line.strip())
        if not m:
            return None
        p = self.profile
        try:
            # mot so can (A&D/CAS) dem khoang trang giua dau +/- va so o dinh
            # dang do rong co dinh (vd "+     18.13") - bo trang truoc khi doi float.
            raw_val = re.sub(r"\s+", "", m.group(p.get("value_group") or 1))
            value = float(raw_val)
        except (IndexError, ValueError, TypeError):
            return None
        stable = True
        sg, ok = p.get("stable_group") or 0, p.get("stable_ok")
        if sg and ok:
            try:
                stable = m.group(sg).strip() == ok.strip()
            except IndexError:
                stable = True
        return value, stable

    async def _cmd_ascii(self, raw: str) -> dict:
        if not raw:
            return {"ok": False, "error": "lenh rong tren profile"}
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._ser.write, raw.encode() + self._term)
        return {"ok": True, "status": "ok"}

    # -- Modbus-RTU mot thanh ghi (cam bien RS-485 don gian) -----------
    async def _start_modbus(self):
        from pymodbus.client import AsyncModbusSerialClient
        self._mb = AsyncModbusSerialClient(
            self.cfg.get("endpoint") or "/dev/ttyUSB0",
            baudrate=self.cfg.get("baud") or self.profile.get("baud_rate") or 9600,
        )
        await self._mb.connect()
        if not self._mb.connected:
            raise ConnectionError("khong ket noi duoc %s" % self.cfg.get("endpoint"))

    async def _read_modbus(self) -> float:
        p = self.profile
        func = int(p.get("mb_func") or 4)
        addr = p.get("mb_reg") or 1
        slave = p.get("mb_slave") or 1
        dtype = (p.get("mb_dtype") or "i16").lower()
        count = 1 if dtype in ("i16", "u16") else 2
        rr = (await self._mb.read_holding_registers(addr, count=count, device_id=slave)) if func == 3 \
            else (await self._mb.read_input_registers(addr, count=count, device_id=slave))
        if rr.isError():
            raise IOError(str(rr))
        if dtype in ("i16", "u16"):
            v = rr.registers[0]
            if dtype == "i16" and v >= 0x8000:
                v -= 0x10000
        else:
            raw = struct.pack(">HH", rr.registers[0] & 0xFFFF, rr.registers[1] & 0xFFFF)
            v = struct.unpack(">f" if dtype == "f32" else (">i" if dtype == "i32" else ">I"), raw)[0]
        return v * (p.get("mb_scale") or 1) + (p.get("mb_offset") or 0)

    # -- vong lap chung -------------------------------------------------
    async def _loop(self):
        is_modbus = self.profile.get("link") == "modbus"
        poll_ms = self.profile.get("mb_poll_ms") if is_modbus else 200
        while not self._stop.is_set():
            try:
                if is_modbus:
                    v = await self._read_modbus()
                    self._mark_online()
                    self._emit(self._ch, v=round(v, 6), q=0, stable=True)
                    await asyncio.sleep(max(0.2, (poll_ms or 2000) / 1000.0))
                else:
                    line = await self._read_line_ascii()
                    if not line:
                        continue
                    parsed = self._parse_ascii(line)
                    self._mark_online()
                    if parsed is None:
                        continue
                    value, stable = parsed
                    if self.cfg.get("event"):
                        self._emit(self._ch, s=line.strip(), q=0, stable=stable)
                    else:
                        self._emit(self._ch, v=value, q=0, stable=stable)
            except Exception as exc:                               # noqa: BLE001
                self._mark_error(str(exc))
                self._emit(self._ch, q=2)
                await asyncio.sleep(1.0)

    async def command(self, channel_code: str, cmd: str, value=None) -> dict:
        if self.profile.get("link") == "modbus":
            return {"ok": False, "error": "lenh khong ho tro tren serial+modbus"}
        raw = {"zero": self.profile.get("cmd_zero"), "tare": self.profile.get("cmd_tare"),
               "read": self.profile.get("cmd_read")}.get(cmd)
        return await self._cmd_ascii(raw)
