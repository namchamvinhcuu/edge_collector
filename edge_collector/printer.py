# -*- coding: utf-8 -*-
"""Gui lenh in (ZPL/ESC-POS) toi may in qua socket tcp:// - dung dinh dang
pcm.print.job._as_payload() (pcm_profile.py): {id, printer, copies, kind, data, lot}.

may in usb:// (cam thang vao edge) khong the goi tu Python thuan mot cach
portable tren moi HDH - o day chi ho tro tcp:// (Zebra/EPSON co card mang,
pho bien nhat trong xuong). usb:// duoc bao loi ro rang de nguoi van hanh biet.
"""
import asyncio
import logging

_logger = logging.getLogger("edge.printer")


async def send_job(payload: dict) -> dict:
    printer = payload.get("printer") or {}
    target = (printer.get("target") or "").strip()
    data = payload.get("data") or ""
    copies = max(1, int(payload.get("copies") or 1))
    kind = payload.get("kind") or "zpl"

    if not target:
        return {"ok": False, "error": "may in khong co 'target'"}
    if not target.startswith("tcp://"):
        return {"ok": False, "error": "chi ho tro target tcp://host:port (nhan '%s')" % target}

    host, _, port = target[len("tcp://"):].partition(":")
    try:
        port = int(port or 9100)
    except ValueError:
        return {"ok": False, "error": "port khong hop le trong target: %s" % target}

    encoding = "latin-1" if kind == "escpos" else "utf-8"
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5)
        try:
            for _ in range(copies):
                writer.write(data.encode(encoding, errors="replace"))
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
        return {"ok": True, "status": "ok"}
    except Exception as exc:                                        # noqa: BLE001
        _logger.warning("gui lenh in that bai (%s): %s", target, exc)
        return {"ok": False, "error": str(exc)[:200]}
