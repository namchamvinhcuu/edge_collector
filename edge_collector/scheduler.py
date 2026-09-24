# -*- coding: utf-8 -*-
"""EdgeAgent — dieu phoi toan bo vong doi: hello (30s) -> config (debounce) ->
thu thap (driver.on_value) -> dem/outbox -> gui /pcm/api/v1/measurements ->
heartbeat -> hang doi in. Day la 'nguoi goi' o chieu edge -> Odoo; chieu
nguoc lai (Odoo -> edge) do inbound_api.py phuc vu qua HTTP.

Nguyen tac: MOI request gui di deu qua outbox truoc (durable) - mat mang giua
chung khong lam mat mau, chi lam cham (PCM: 'nhu ngat vao xuong').
"""
import asyncio
import logging
import time
import uuid

from .config import settings
from .manager import SourceManager
from .mqtt_consumer import MqttConsumer
from .odoo_client import OdooClient
from .printer import send_job
from .store import Store

_logger = logging.getLogger("edge.scheduler")

HISTORY_RETENTION_DAYS = 14


class EdgeAgent:
    def __init__(self):
        self.store = Store(settings.sqlite_path)
        self.odoo = OdooClient(self.store)
        self.manager = SourceManager(self._on_value)
        # Ben DOC cua duong MQTT. Mac dinh chi DEM, khong day vao outbox —
        # xem chu thich dau mqtt_consumer.py (che do bong).
        self.mqtt_consumer = MqttConsumer(self)
        # SourceManager can duong MQTT de day lenh xuong node. Noc vao day
        # thay vi truyen qua __init__ de khong doi chu ky ham dung cua no
        # — manager chi dung khi thuoc tinh nay co, va tu quay ve hang doi
        # poll khi khong.
        self.manager.mqtt_cmd = self.mqtt_consumer
        self._pending: dict = {}          # serial -> list[item dict], cho tung dot flush
        self._boot_id = self.store.kv_get("boot_id")
        if not self._boot_id:
            self._boot_id = uuid.uuid4().hex
            self.store.kv_set("boot_id", self._boot_id)
        self._pending_rev = None
        self._pending_rev_since = 0.0
        self._tasks: list = []
        self._stopping = asyncio.Event()

    # ------------------------------------------------------------------
    # driver -> day gia tri vao buffer cho serial do (chua gui ngay)
    # ------------------------------------------------------------------
    def _on_value(self, serial, ch, v, s, q, ts, stable):
        item = {"ch": ch, "v": v, "s": s, "q": int(q or 0), "stable": stable}
        if ts is not None:
            item["ts"] = int(ts * 1000)
        self._pending.setdefault(serial, []).append(item)
        self.store.history_insert_many([(serial, ch, ts or time.time(), v, s, int(q or 0),
                                         1 if stable else 0)])

    def push_node_reading(self, serial, ch, v, s, q, ts, stable):
        """Loi vao tu node_api.py (node HTTP day thang, kind=http_node) — cung
        mot duong ong voi driver.on_reading()."""
        self._on_value(serial, ch, v, s, q, ts, stable)

    async def forward_node_heartbeat(self, serial: str, meta: dict) -> dict:
        return await self.odoo.heartbeat(serial, meta)

    def _mqtt_connected(self) -> bool:
        return any(d.kind == "mqtt" and d.status().status == "online"
                    for d in self.manager._drivers.values())

    # ------------------------------------------------------------------
    async def start(self):
        await self.mqtt_consumer.start()
        self._tasks = [
            asyncio.create_task(self._hello_loop()),
            asyncio.create_task(self._config_loop()),
            asyncio.create_task(self._flush_loop()),
            asyncio.create_task(self._sender_loop()),
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._print_loop()),
            asyncio.create_task(self._gc_loop()),
        ]

    async def stop(self):
        self._stopping.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.mqtt_consumer.stop()
        await self.manager.shutdown()
        await self.odoo.aclose()

    # ------------------------------------------------------------------
    async def _hello_loop(self):
        while not self._stopping.is_set():
            try:
                res = await self.odoo.hello(
                    lag=self.store.outbox_count(),
                    forward_state="ok" if self.store.outbox_count() == 0 else "backlog",
                    mqtt_connected=self._mqtt_connected(),
                )
                if not res.get("ok"):
                    _logger.warning("hello that bai: %s", res.get("error"))
                await self.odoo.source_status(self.manager.status_rows(), self._mqtt_connected())
            except Exception:                                        # noqa: BLE001
                _logger.exception("loi trong hello_loop")
            await asyncio.sleep(settings.hello_interval_s)

    async def _config_loop(self):
        applied_rev = -1
        while not self._stopping.is_set():
            try:
                res = await self.odoo.pull_config()
                if res.get("ok") and res.get("config"):
                    cfg = res["config"]
                    rev = cfg.get("config_version", 0)
                    now = time.time()
                    if rev != self._pending_rev:
                        self._pending_rev, self._pending_rev_since = rev, now
                    stable_long_enough = (now - self._pending_rev_since) >= settings.config_debounce_s
                    if rev != applied_rev and stable_long_enough:
                        await self.manager.apply_config(cfg)
                        self.store.kv_set("config_rev", rev)
                        applied_rev = rev
                        _logger.info("da ap dung config_version=%s", rev)
            except Exception:                                        # noqa: BLE001
                _logger.exception("loi trong config_loop")
            await asyncio.sleep(settings.config_poll_interval_s)

    async def _flush_loop(self):
        """Gop buffer -> outbox moi submit_interval_s (bid=boot_id co dinh,
        seq tang dan ben ngoai — cho phep dedup (serial,bid,seq) o Odoo)."""
        while not self._stopping.is_set():
            await asyncio.sleep(settings.submit_interval_s)
            if not self._pending:
                continue
            batch, self._pending = self._pending, {}
            for serial, items in batch.items():
                seq = self.store.next_seq(serial)
                self.store.outbox_push(serial, self._boot_id, seq, {"items": items})

    # 21/09: mot vong CHI gui mot ban ghi roi ngu 0,2 s — toi da ~2,2 ban/giay.
    # _flush_loop day ra 1/submit_interval_s = 4 ban/giay. San xuat > tieu thu
    # nen ton kho lon dan va do tre tang theo thoi gian: "chay mot hoi la no
    # tre so voi can nhay thuc te". Rut can ton kho trong mot vong, va chi ngu
    # khi khong con gi de gui.
    MAX_MOI_VONG = 30

    # 24/09: rut can outbox tung serial TUAN TU (1 luc 1 serial) khong scale
    # khi so serial len toi hang tram - 1 vong drain se can (so serial) lan
    # round-trip HTTP noi tiep toi Odoo, co the vuot han submit_interval_s va
    # lam do tre forward tang dan theo thoi gian (KHONG mat du lieu - outbox
    # van giu - chi cham). Gioi han so serial gui DONG THOI bang semaphore,
    # tha long chieu song song ma khong ban pha Odoo bang hang tram request
    # cung luc. Thu tu ben TRONG 1 serial VAN tuan tu (giu dung invariant
    # "dung lai cho serial nay khi loi" - chi serial KHAC nhau moi chay
    # song song voi nhau).
    MAX_CONCURRENT_SERIALS = 8

    async def _drain_serial(self, serial: str) -> bool:
        sent_any = False
        for _ in range(self.MAX_MOI_VONG):
            row = self.store.outbox_oldest(serial)
            if not row:
                break
            meta = self.manager.device_meta(serial)
            device_meta = {"name": meta.get("name")} if meta else None
            res = await self.odoo.measurements(
                serial, row["payload"]["items"], row["bid"], row["seq"],
                device_meta)
            if not res.get("ok"):
                _logger.info("gui measurements that bai cho %s: %s",
                             serial, res.get("error"))
                break    # giu thu tu — dung lai cho serial nay
            self.store.outbox_delete(row["id"])
            sent_any = True
        return sent_any

    async def _sender_loop(self):
        sem = asyncio.Semaphore(self.MAX_CONCURRENT_SERIALS)

        async def _drain_with_limit(serial: str) -> bool:
            async with sem:
                return await self._drain_serial(serial)

        while not self._stopping.is_set():
            sent_any = False
            try:
                serials = self.store.outbox_serials()
                if serials:
                    # return_exceptions=True: 1 serial raise (vd loi mang la, bug
                    # driver...) KHONG duoc phep giet ca gather() - mac dinh cua
                    # asyncio.gather() se lam CA task _sender_loop chet vinh vien
                    # (khong watchdog tu respawn), y het loai bug OdooClient._parse
                    # da fix 2026-09-24 nhung ap dung cho MOI nguon loi tuong lai,
                    # khong chi rieng loi do.
                    results = await asyncio.gather(
                        *(_drain_with_limit(s) for s in serials), return_exceptions=True)
                    for serial, res in zip(serials, results):
                        if isinstance(res, BaseException):
                            _logger.exception("loi khong luong truoc khi gui outbox cho %s",
                                              serial, exc_info=res)
                            continue
                        if res:
                            sent_any = True
            except Exception:                                          # noqa: BLE001
                # Cung pattern try/except+log nhu 5 vong lap con lai cua EdgeAgent
                # (_hello_loop/_config_loop/_heartbeat_loop/_print_loop/_gc_loop) -
                # truoc day _sender_loop la vong DUY NHAT thieu, nen 1 loi ngoai du
                # kien (vd outbox_serials() tu no loi) se giet ca vong lap ma
                # khong ai biet - xem review 2026-09-24 (cau hoi scale hang tram
                # sensor cua Nam).
                _logger.exception("loi trong sender_loop")
            await asyncio.sleep(0.02 if sent_any else 1.0)

    async def _heartbeat_loop(self):
        while not self._stopping.is_set():
            for serial in self.manager.known_serials():
                try:
                    await self.odoo.heartbeat(serial, {
                        "lag": self.store.outbox_count(),
                        "config_version": self.manager.config_rev,
                    })
                except Exception:                                    # noqa: BLE001
                    _logger.exception("heartbeat that bai cho %s", serial)
            await asyncio.sleep(settings.heartbeat_interval_s)

    async def _print_loop(self):
        while not self._stopping.is_set():
            try:
                res = await self.odoo.print_job_next()
                job = res.get("job")
                if job:
                    result = await send_job(job)
                    await self.odoo.print_job_ack(job["id"], result.get("ok", False),
                                                  result.get("error") or "")
                    continue          # co job vua roi -> kiem tra ngay job ke tiep
            except Exception:                                        # noqa: BLE001
                _logger.exception("loi trong print_loop")
            await asyncio.sleep(settings.print_poll_interval_s)

    async def _gc_loop(self):
        while not self._stopping.is_set():
            cutoff = time.time() - HISTORY_RETENTION_DAYS * 86400
            removed = self.store.history_gc(cutoff)
            if removed:
                _logger.info("da don %d dong lich su cu hon %d ngay", removed, HISTORY_RETENTION_DAYS)
            await asyncio.sleep(3600)
