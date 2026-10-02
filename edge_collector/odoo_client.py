# -*- coding: utf-8 -*-
"""Client HTTP gọi VÀO Odoo — dùng 'hợp đồng' /pcm/api/v1/edge/* mô tả trong
pcm_base/controllers/ingest.py. Chỉ một địa chỉ tin cậy (Main); không node nào
nói chuyện thẳng với Odoo — nguyên tắc 'tầng dưới gọi tầng trên'.

Xác thực: header X-Edge-Code (luôn có) + X-API-Key (rỗng ở lần hello đầu tiên,
Odoo trả về một lần rồi phải lưu lại — _hello() trong pcm_edge.py).
"""
import datetime
import email.utils
import json
import logging
import time
from typing import Any, Optional

import httpx

from .config import settings
from .store import Store

_logger = logging.getLogger("edge.odoo_client")

_TIMEOUT = httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=5.0)


class OdooClient:
    def __init__(self, store: Store):
        self.store = store
        self._client = httpx.AsyncClient(base_url=settings.main_url, timeout=_TIMEOUT)
        # giờ Odoo - giờ edge (giây), cập nhật mỗi lần pull_config. Dùng để
        # kiểm X-Edge-Ts của lệnh Odoo ký (inbound_api._downlink_auth_error).
        self.clock_offset_s = 0.0

    @property
    def api_key(self) -> Optional[str]:
        return self.store.kv_get("api_key")

    def refresh_base_url(self) -> None:
        """Gọi sau config.reload_settings() (tính năng hot-reload /setup) -
        httpx.AsyncClient bake base_url vào lúc __init__, KHÔNG tự động đổi
        theo settings.main_url thay đổi sau đó (đã verify thực nghiệm:
        AsyncClient.base_url là property GÁN LẠI được, xem review
        2026-09-17), nên phải gọi hàm này tường minh mỗi lần reload."""
        self._client.base_url = settings.main_url

    def _headers(self) -> dict:
        h = {"X-Edge-Code": settings.edge_code, "Content-Type": "application/json"}
        key = self.api_key
        if key:
            h["X-API-Key"] = key
        return h

    async def aclose(self):
        await self._client.aclose()

    async def _post(self, path: str, body: dict, on_response=None) -> dict:
        try:
            r = await self._client.post(path, json=body, headers=self._headers())
        except httpx.HTTPError as e:
            _logger.info("main unreachable POST %s: %s", path, e)
            return {"ok": False, "error": str(e)}
        if on_response is not None:
            on_response(r)
        return self._parse(r)

    def _note_server_clock(self, r: httpx.Response) -> None:
        """Độ lệch giờ edge so với Odoo, lấy từ header HTTP Date (độ phân giải
        1 giây, đủ cho cửa sổ 30s của chữ ký downlink - inbound_api). Edge tại
        site khách không đảm bảo có NTP."""
        raw = r.headers.get("Date")
        if not raw:
            return
        try:
            dt = email.utils.parsedate_to_datetime(raw)
        except (TypeError, ValueError):
            _logger.debug("header Date không đọc được: %r", raw)
            return
        if dt.tzinfo is None:
            # "-0000" cho datetime không múi giờ; .timestamp() sẽ hiểu theo
            # giờ máy edge (lệch cả tiếng). HTTP Date luôn là UTC.
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        self.clock_offset_s = dt.timestamp() - time.time()

    async def _get(self, path: str, params: Optional[dict] = None) -> dict:
        try:
            r = await self._client.get(path, params=params or {}, headers=self._headers())
        except httpx.HTTPError as e:
            _logger.info("main unreachable GET %s: %s", path, e)
            return {"ok": False, "error": str(e)}
        return self._parse(r)

    @staticmethod
    def _parse(r: httpx.Response) -> dict:
        try:
            body = r.json()
        except ValueError:
            body = {"raw": r.text[:500]}
        if not isinstance(body, dict):
            # JSON hợp lệ nhưng KHÔNG phải object (vd Odoo trả về list/chuỗi/số
            # kèm status lỗi) - body.setdefault(...) dưới đây sẽ raise
            # AttributeError không bị bắt, lan ra tận _sender_loop qua
            # asyncio.gather() làm chết cả task không hồi phục - xem
            # python-reviewer 2026-09-24 (finding tử vong song song hóa
            # _sender_loop, phát hiện là pre-existing ở đây).
            body = {"raw": body}
        if not r.is_success:
            body.setdefault("ok", False)
            body.setdefault("error", body.get("error") or ("http %s" % r.status_code))
            body["status_code"] = r.status_code
            if r.status_code == 429:
                # Header header ưu tiên (dùng định dạng contract đã chốt với
                # pcm_base: delta-seconds, KHÔNG phải HTTP-date); body["retry_after"]
                # là phòng hộ khi client nào đó strip header - xem scheduler.py
                # _drain_serial cho nội dung áp dụng.
                retry_after = None
                raw = r.headers.get("Retry-After")
                if raw is not None:
                    try:
                        retry_after = int(raw)
                    except ValueError:
                        retry_after = None
                if retry_after is None:
                    retry_after = body.get("retry_after")
                body["retry_after"] = retry_after
            return body
        body.setdefault("ok", True)
        return body

    # ------------------------------------------------------------------
    # hello — đăng ký/sống còn của CHÍNH edge này (30s, PCM 04/07)
    # ------------------------------------------------------------------
    async def hello(self, *, lag: int, forward_state: str, mqtt_connected: bool,
                     version: str = "0.1.0") -> dict:
        body = {
            "code": settings.edge_code,
            "name": settings.edge_name or None,
            "platform": settings.edge_platform,
            "base_url": settings.edge_base_url,
            "version": version,
            "lag": lag,
            "forward_state": forward_state,
            "mqtt_connected": mqtt_connected,
            "config_version": self.store.kv_get("config_rev", 0),
        }
        if not self.api_key:
            body["api_key"] = None  # lần đầu: chưa có key, Odoo sẽ cấp
        res = await self._post("/pcm/api/v1/edge/hello", body)
        if res.get("ok") and res.get("api_key"):
            self.store.kv_set("api_key", res["api_key"])
            _logger.info("đã nhận api_key từ Main (lần đầu đăng ký)")
        return res

    # ------------------------------------------------------------------
    async def pull_config(self) -> dict:
        body = {"config_version": self.store.kv_get("config_rev", 0)}
        res = await self._post("/pcm/api/v1/edge/config", body,
                               on_response=self._note_server_clock)
        # Khóa ký lệnh Odoo -> edge. Đọc ở MỌI response ok, không phụ thuộc
        # config_version (lần deploy đầu version không đổi). Thiếu thì GIỮ
        # khóa cũ. KHÔNG log giá trị.
        if res.get("ok"):
            cfg = res.get("config")
            key = res.get("downlink_key") or (cfg.get("downlink_key") if isinstance(cfg, dict) else None)
            if isinstance(key, str) and key and key != self.store.kv_get("downlink_key"):
                # json.dumps: kv_get() luôn json.loads, khóa trông như số
                # ("1e5") lưu thô sẽ đọc ra float - lưu dạng chuỗi JSON để
                # đọc lại luôn là str.
                self.store.kv_set("downlink_key", json.dumps(key))
                _logger.info("đã nhận downlink_key mới từ Main")
        return res

    async def source_status(self, rows: list, mqtt_connected: bool) -> dict:
        return await self._post("/pcm/api/v1/edge/source_status", {
            "sources": rows, "mqtt_connected": mqtt_connected,
        })

    # ------------------------------------------------------------------
    async def measurements(self, serial: str, items: list, bid: str, seq: int,
                            device_meta: Optional[dict] = None) -> dict:
        body = {"serial": serial, "items": items, "bid": bid, "seq": seq}
        if device_meta:
            body["device"] = device_meta
        _logger.info("gửi measurements %s bid=%s seq=%s: %s", serial, bid, seq, body)
        res = await self._post("/pcm/api/v1/measurements", body)
        _logger.info("phản hồi measurements %s: %s", serial, res)
        return res

    async def heartbeat(self, serial: str, meta: dict) -> dict:
        body = dict(meta)
        body["serial"] = serial
        return await self._post("/pcm/api/v1/heartbeat", body)

    # ------------------------------------------------------------------
    async def print_job_next(self) -> dict:
        return await self._get("/pcm/api/v1/print_jobs/next")

    async def print_job_ack(self, job_id: int, ok: bool, detail: str = "") -> dict:
        return await self._post("/pcm/api/v1/print_jobs/ack", {
            "id": job_id, "ok": ok, "detail": detail,
        })
