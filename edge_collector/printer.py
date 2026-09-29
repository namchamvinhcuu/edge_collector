# -*- coding: utf-8 -*-
"""Gửi lệnh in (ZPL/ESC-POS) tới máy in qua socket tcp:// - dùng định dạng
pcm.print.job._as_payload() (pcm_profile.py): {id, printer, copies, kind, data, lot}.

máy in usb:// (cắm thẳng vào edge) không thể gọi từ Python thuần một cách
portable trên mọi HĐH - ở đây chỉ hỗ trợ tcp:// (Zebra/EPSON có card mạng,
phổ biến nhất trong xưởng). usb:// được báo lỗi rõ ràng để người vận hành biết.
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
        return {"ok": False, "error": "máy in không có 'target'"}
    if not target.startswith("tcp://"):
        return {"ok": False, "error": "chỉ hỗ trợ target tcp://host:port (nhận '%s')" % target}

    host, _, port = target[len("tcp://"):].partition(":")
    try:
        port = int(port or 9100)
    except ValueError:
        return {"ok": False, "error": "port không hợp lệ trong target: %s" % target}

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
        _logger.warning("gửi lệnh in thất bại (%s): %s", target, exc)
        return {"ok": False, "error": str(exc)[:200]}
