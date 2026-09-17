# -*- coding: utf-8 -*-
"""Client HTTP goi VAO Odoo — dung 'hop dong' /pcm/api/v1/edge/* mo ta trong
pcm_base/controllers/ingest.py. Chi mot dia chi tin cay (Main); khong node nao
noi chuyen thang voi Odoo — nguyen tac 'tang duoi goi tang tren'.

Xac thuc: header X-Edge-Code (luon co) + X-API-Key (rong o lan hello dau tien,
Odoo tra ve mot lan roi phai luu lai — _hello() trong pcm_edge.py).
"""
import logging
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

    @property
    def api_key(self) -> Optional[str]:
        return self.store.kv_get("api_key")

    def refresh_base_url(self) -> None:
        """Goi sau config.reload_settings() (tinh nang hot-reload /setup) -
        httpx.AsyncClient bake base_url vao luc __init__, KHONG tu dong doi
        theo settings.main_url thay doi sau do (da verify thuc nghiem:
        AsyncClient.base_url la property GAN LAI duoc, xem review
        2026-09-17), nen phai goi ham nay tuong minh moi lan reload."""
        self._client.base_url = settings.main_url

    def _headers(self) -> dict:
        h = {"X-Edge-Code": settings.edge_code, "Content-Type": "application/json"}
        key = self.api_key
        if key:
            h["X-API-Key"] = key
        return h

    async def aclose(self):
        await self._client.aclose()

    async def _post(self, path: str, body: dict) -> dict:
        try:
            r = await self._client.post(path, json=body, headers=self._headers())
        except httpx.HTTPError as e:
            _logger.info("main unreachable POST %s: %s", path, e)
            return {"ok": False, "error": str(e)}
        return self._parse(r)

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
        if not r.is_success:
            body.setdefault("ok", False)
            body.setdefault("error", body.get("error") or ("http %s" % r.status_code))
            return body
        if isinstance(body, dict):
            body.setdefault("ok", True)
        return body

    # ------------------------------------------------------------------
    # hello — dang ky/song con cua CHINH edge nay (30s, PCM 04/07)
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
            body["api_key"] = None  # lan dau: chua co key, Odoo se cap
        res = await self._post("/pcm/api/v1/edge/hello", body)
        if res.get("ok") and res.get("api_key"):
            self.store.kv_set("api_key", res["api_key"])
            _logger.info("da nhan api_key tu Main (lan dau dang ky)")
        return res

    # ------------------------------------------------------------------
    async def pull_config(self) -> dict:
        body = {"config_version": self.store.kv_get("config_rev", 0)}
        return await self._post("/pcm/api/v1/edge/config", body)

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
        _logger.info("gui measurements %s bid=%s seq=%s: %s", serial, bid, seq, body)
        res = await self._post("/pcm/api/v1/measurements", body)
        _logger.info("phan hoi measurements %s: %s", serial, res)
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
