# -*- coding: utf-8 -*-
"""Giao dien chung cho moi driver nguon (pcm.source kind = opcua/modbus_tcp/
modbus_rtu/serial/mqtt/sim). manager.py nap dung config._as_config() cua Odoo
(xem pcm_source.py) va goi Reading callback moi khi co gia tri moi.

on_reading(ch_code, v, s, q, ts, stable) — ts la epoch giay (float) hoac None
(= 'bay gio', giong quy uoc _ts() trong ingest.py).
"""
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

ReadingCb = Callable[[str, Optional[float], Optional[str], int, Optional[float], Optional[bool]], None]


@dataclass
class DriverStatus:
    status: str = "untested"          # untested | online | offline | error
    error: Optional[str] = None
    stats: dict = field(default_factory=dict)


class SourceDriver:
    """Lop co so — moi driver ke thua va override cac ham can thiet."""

    kind = "base"

    def __init__(self, source_cfg: dict, channels: list, on_reading: ReadingCb):
        self.cfg = source_cfg
        self.channels = channels or []          # channel._as_config() cua tung kenh gan vao nguon nay
        self.code = source_cfg.get("code") or ""
        self.on_reading = on_reading
        self._status = DriverStatus()

    # -- vong doi ----------------------------------------------------
    async def start(self) -> None:
        raise NotImplementedError

    async def stop(self) -> None:
        pass

    # -- quan sat ------------------------------------------------------
    def status(self) -> DriverStatus:
        return self._status

    def _mark_online(self):
        self._status.status, self._status.error = "online", None

    def _mark_error(self, err: str):
        self._status.status, self._status.error = "error", err[:200]

    def _emit(self, ch: str, v=None, s=None, q=0, ts=None, stable=None):
        self.on_reading(ch, v, s, int(q or 0), ts if ts is not None else time.time(), stable)

    # -- dieu khien (Odoo -> edge -> thiet bi) -------------------------
    async def command(self, channel_code: str, cmd: str, value=None) -> dict:
        return {"ok": False, "error": "command khong ho tro tren nguon '%s'" % self.kind}

    async def browse(self, node_id=None, path=None) -> dict:
        return {"ok": False, "error": "browse khong ho tro tren nguon '%s'" % self.kind}

    async def test(self) -> dict:
        """Thu ket noi mot lan, dung cho pcm.source.action_test(). Mac dinh:
        coi start() thanh cong la du (driver tu overide neu can chi tiet hon)."""
        t0 = time.time()
        try:
            await self.start()
            ok = self._status.status != "error"
            return {"ok": ok, "items": [{"name": self.code, "ok": ok,
                    "ms": int((time.time() - t0) * 1000), "detail": self._status.error or ""}]}
        except Exception as exc:                                  # noqa: BLE001
            return {"ok": False, "items": [{"name": self.code, "ok": False,
                    "ms": int((time.time() - t0) * 1000), "detail": str(exc)[:200]}],
                    "error": str(exc)[:200]}
        finally:
            await self.stop()
