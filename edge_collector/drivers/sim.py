# -*- coding: utf-8 -*-
"""Nguồn 'sim' — tạo giá trị giả cho từng kênh gắn vào nguồn này, không cần
phần cứng. Dùng để dựng thử đường ống edge <-> Odoo (khác với pcm.simulator,
model đó chạy Ở PHÍA ODOO và không liên quan edge thật).

Odoo không gửi thông số 'kiểu dáng' cho nguồn sim (xem pcm_source._as_config),
nên ở đây chỉ là một bộ tạo dạng sóng ngẫu nhiên đơn giản — đủ để kiểm tra
toàn bộ ống dẫn hello -> config -> measurements -> submit.
"""
import asyncio
import math
import random
import time

from .base import SourceDriver


class SimDriver(SourceDriver):
    kind = "sim"

    def __init__(self, source_cfg, channels, on_reading):
        super().__init__(source_cfg, channels, on_reading)
        self._task = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        self._stop.clear()
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

    async def _loop(self):
        t0 = time.time()
        while not self._stop.is_set():
            for ch in self.channels:
                code = ch.get("code")
                if not code:
                    continue
                kind = ch.get("kind", "measure")
                if kind == "event":
                    self._emit(code, s="SIM-%d" % int(time.time()), q=8, stable=True)
                else:
                    v = 50.0 + 10.0 * math.sin(time.time() - t0) + random.uniform(-1, 1)
                    self._emit(code, v=round(v, 3), q=8, stable=True)
            poll_ms = min([c.get("poll_ms") or 1000 for c in self.channels] or [1000])
            await asyncio.sleep(max(0.2, poll_ms / 1000.0))

    async def command(self, channel_code: str, cmd: str, value=None) -> dict:
        return {"ok": True, "status": "ok"}
