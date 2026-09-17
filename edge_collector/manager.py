# -*- coding: utf-8 -*-
"""SourceManager — nap 'config' tra ve tu GET/POST /pcm/api/v1/edge/config
(edge_config() ben pcm_edge.py) thanh cac driver dang chay, dinh tuyen lenh/
browse/status theo dung kenh, va bao gia tri moi ve cho scheduler ghi so +
day len Odoo.

Chi RESTART driver cua nguon nao co config_rev doi (moi nguon mang config_rev
rieng trong _as_config) — sua mot nguon khong lam gian doan cac nguon khac.

Ngoai driver theo pcm.source, SourceManager con giu hang doi lenh cho NODE
day HTTP truc tiep (kind=http_node — Pi/PC bridge, xem node_api.py): node
khong the bi goi nguoc (chi outbound), nen lenh tu Odoo duoc XEP HANG cho
node tu POLL lay, ket qua duoc ACK ve va tra loi lai cho /api/command dang
cho (queue_command / node_pull_command / node_ack_command).
"""
import asyncio
import logging
import time
from typing import Callable, Optional

from .drivers.base import SourceDriver
from .drivers.mqtt import MqttDriver
from .drivers.modbus import ModbusDriver
from .drivers.opcua import OpcuaDriver
from .drivers.serial_ascii import SerialDriver
from .drivers.sim import SimDriver

_logger = logging.getLogger("edge.manager")

OnValue = Callable[[str, str, object, object, int, object, object], None]


class SourceManager:
    def __init__(self, on_value: OnValue):
        self.on_value = on_value
        self.config_rev = 0
        self.devices_by_serial: dict = {}
        self.printers: list = []
        self.profiles_by_code: dict = {}
        self.api_keys: dict = {}
        self.raw_channels: set = set()
        self._drivers: dict[str, SourceDriver] = {}
        self._source_rev: dict[str, int] = {}
        self._route: dict[tuple, str] = {}          # (serial, ch) -> source code

        self._node_last_seen: dict[str, float] = {}
        self._node_queues: dict[str, "asyncio.Queue"] = {}
        self._node_futures: dict[int, "asyncio.Future"] = {}
        self._node_cmd_seq = 0

    # ------------------------------------------------------------------
    def is_raw_forward(self, ch_code: str) -> bool:
        return ch_code in self.raw_channels

    def device_meta(self, serial: str) -> dict:
        return self.devices_by_serial.get(serial) or {}

    def known_serials(self) -> list:
        return list(self.devices_by_serial.keys())

    def get_driver(self, source_code: str) -> Optional[SourceDriver]:
        return self._drivers.get(source_code)

    def driver_for_channel(self, serial: str, ch_code: str) -> Optional[SourceDriver]:
        code = self._route.get((serial, ch_code))
        return self._drivers.get(code) if code else None

    def status_rows(self) -> list:
        rows = []
        for code, drv in self._drivers.items():
            st = drv.status()
            rows.append({"code": code, "status": st.status, "error": st.error, "stats": st.stats})
        return rows

    # ------------------------------------------------------------------
    async def apply_config(self, cfg: dict) -> None:
        self.config_rev = cfg.get("config_version", self.config_rev)
        self.api_keys = cfg.get("api_keys") or {}
        self.raw_channels = set(cfg.get("raw_channels") or [])
        self.printers = cfg.get("printers") or []
        self.profiles_by_code = {p["code"]: p for p in (cfg.get("profiles") or [])}
        self.devices_by_serial = {d["serial"]: d for d in (cfg.get("devices") or [])}

        channels_by_source: dict = {}
        route: dict = {}
        for dev in cfg.get("devices") or []:
            for ch in dev.get("channels") or []:
                src_code = ch.get("source")
                if src_code:
                    channels_by_source.setdefault(src_code, []).append(ch)
                    route[(dev["serial"], ch["code"])] = src_code
        self._route = route

        wanted = {s["code"]: s for s in (cfg.get("sources") or []) if s.get("kind") not in
                  ("edge", "http_node")}

        for code in list(self._drivers):
            if code not in wanted:
                await self._stop_source(code)

        for code, src_cfg in wanted.items():
            rev = src_cfg.get("config_rev", 0)
            if code in self._drivers and self._source_rev.get(code) == rev:
                continue
            await self._stop_source(code)
            try:
                drv = self._build(src_cfg, channels_by_source.get(code, []))
                await drv.start()
                self._drivers[code] = drv
                self._source_rev[code] = rev
                _logger.info("nguon %s (%s) da khoi dong, rev=%s", code, src_cfg.get("kind"), rev)
            except Exception as exc:                                # noqa: BLE001
                _logger.warning("nguon %s khoi dong that bai: %s", code, exc)

    async def _stop_source(self, code: str) -> None:
        drv = self._drivers.pop(code, None)
        self._source_rev.pop(code, None)
        if drv:
            try:
                await drv.stop()
            except Exception:                                       # noqa: BLE001
                pass

    def build_probe(self, src_cfg: dict) -> SourceDriver:
        """Driver dung mot lan cho pcm.source.action_test() — khong dang ky
        vao self._drivers, khong anh huong toi thu thap dang chay."""
        return self._build(src_cfg, [])

    def _build(self, src_cfg: dict, channels: list) -> SourceDriver:
        kind = src_cfg.get("kind")
        serial = src_cfg.get("serial") or src_cfg.get("code")

        def emit(ch, v=None, s=None, q=0, ts=None, stable=None):
            self.on_value(serial, ch, v, s, q, ts, stable)

        if kind == "sim":
            return SimDriver(src_cfg, channels, emit)
        if kind in ("modbus_tcp", "modbus_rtu"):
            return ModbusDriver(src_cfg, channels, emit)
        if kind == "opcua":
            return OpcuaDriver(src_cfg, channels, emit)
        if kind == "mqtt":
            return MqttDriver(src_cfg, channels, emit)
        if kind == "serial":
            profile = self.profiles_by_code.get(src_cfg.get("profile"))
            return SerialDriver(src_cfg, channels, emit, profile)
        raise ValueError("khong ho tro kind=%s" % kind)

    async def shutdown(self) -> None:
        for code in list(self._drivers):
            await self._stop_source(code)

    # ------------------------------------------------------------------
    # Node http (Pi/PC bridge) — node.py trong node_api.py goi vao day.
    # ------------------------------------------------------------------
    def cached_node_api_key(self, serial: str) -> Optional[str]:
        """Khoa Odoo da cap cho thiet bi nay (device._as_config()['api_key']),
        neu edge da tung keo config va biet ve serial nay."""
        dev = self.devices_by_serial.get(serial)
        return (dev or {}).get("api_key") or None

    def touch_node(self, serial: str) -> None:
        self._node_last_seen[serial] = time.time()

    def known_node_serials(self) -> list:
        return list(self._node_last_seen.keys())

    def has_node_or_driver(self, serial: str, ch_code: str) -> bool:
        return (serial, ch_code) in self._route or serial in self._node_last_seen

    async def queue_command(self, serial: str, ch: str, cmd: str, value, timeout: float = 8.0) -> dict:
        """Xep mot lenh cho NODE (khong co driver dieu khien duoc — vd http_node),
        cho node tu poll roi ack. Dung khi driver_for_channel() tra ve None."""
        self._node_cmd_seq += 1
        cmd_id = self._node_cmd_seq
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._node_futures[cmd_id] = fut
        self._node_queues.setdefault(serial, asyncio.Queue()).put_nowait(
            {"id": cmd_id, "channel": ch, "cmd": cmd, "value": value})
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            return {"ok": False, "error": "node khong tra loi trong %.0fs" % timeout}
        finally:
            self._node_futures.pop(cmd_id, None)

    def node_pull_command(self, serial: str) -> Optional[dict]:
        q = self._node_queues.get(serial)
        if not q or q.empty():
            return None
        return q.get_nowait()

    def node_ack_command(self, cmd_id: int, ok: bool, detail: str = "") -> bool:
        fut = self._node_futures.get(cmd_id)
        if not fut or fut.done():
            return False
        fut.set_result({"ok": ok, "status": "ok" if ok else "error", "error": None if ok else detail})
        return True
