# -*- coding: utf-8 -*-
"""EdgeAgent — điều phối toàn bộ vòng đời: hello (30s) -> config (debounce) ->
thu thập (driver.on_value) -> đếm/outbox -> gửi /pcm/api/v1/measurements ->
heartbeat -> hàng đợi in. Đây là 'người gọi' ở chiều edge -> Odoo; chiều
ngược lại (Odoo -> edge) do inbound_api.py phục vụ qua HTTP.

Nguyên tắc: MỌI request gửi đi đều qua outbox trước (durable) - mất mạng giữa
chừng không làm mất mẫu, chỉ làm chậm (PCM: 'như ngắt vào xương').
"""
import asyncio
import logging
import random
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
        # Bên ĐỌC của đường MQTT. Mặc định chỉ ĐẾM, không đẩy vào outbox —
        # xem chú thích đầu mqtt_consumer.py (chế độ bóng).
        self.mqtt_consumer = MqttConsumer(self)
        # SourceManager cần đường MQTT để đẩy lệnh xuống node. Móc vào đây
        # thay vì truyền qua __init__ để không đổi chữ ký hàm dựng của nó
        # — manager chỉ dùng khi thuộc tính này có, và tự quay về hàng đợi
        # poll khi không.
        self.manager.mqtt_cmd = self.mqtt_consumer
        self._pending: dict = {}          # serial -> list[item dict], cho từng đợt flush
        # (serial, ch) -> {"v","s","q","stable","ts"} lần gần nhất ĐÃ enqueue lên
        # outbox (khác history - history luôn ghi mỗi lần đọc). Dùng cho
        # change-detection ở _on_value(): xem DEFAULT_HEARTBEAT_S.
        self._last_enqueued: dict = {}
        self._boot_id = self.store.kv_get("boot_id")
        if not self._boot_id:
            self._boot_id = uuid.uuid4().hex
            self.store.kv_set("boot_id", self._boot_id)
        self._pending_rev = None
        self._pending_rev_since = 0.0
        self._tasks: list = []
        self._stopping = asyncio.Event()
        # Backoff riêng theo serial cho 429/5xx từ /pcm/api/v1/measurements
        # (contract đã chốt với pcm_base 2026-09-29) - xem hằng số BACKOFF_*
        # và _drain_serial() bên dưới.
        self._backoff_until: dict = {}   # serial -> time.monotonic() được phép gửi lại
        self._backoff_delay: dict = {}   # serial -> delay hiện tại (s), tăng dần

    # 29/09 (contract chốt với pcm_base): channel có must_send_every=False (không
    # phải counter/trigger/raw-forward - xem manager.channel_meta_for()) được phép
    # BỎ QUA enqueue-lên-Odoo khi giá trị/quality/stable y hệt lần gửi trước (vd
    # cân điện tử đứng yên lâu, tránh flood outbox - xem Fix-History 2026-09-29).
    # Vẫn GỬI LẠI định kỳ theo max_age_ms*0.5 của channel đó (riêng từng channel,
    # KHÔNG dùng 1 hằng số chung - ngưỡng lệch nhau nhiều giữa các loại sensor)
    # để UI Odoo không hiện "không đổi" quá lâu. Thiếu/sai max_age_ms -> fallback
    # DEFAULT_HEARTBEAT_S. history_insert_many() (local, Live activity) KHÔNG bị
    # ảnh hưởng - vẫn ghi MỌI lần đọc như cũ.
    DEFAULT_HEARTBEAT_S = 5.0

    def _should_skip_duplicate(self, serial, ch, v, s, q, stable) -> bool:
        meta = self.manager.channel_meta_for(serial, ch)
        if meta.get("must_send_every", True):
            return False
        key = (serial, ch)
        now = time.monotonic()
        prev = self._last_enqueued.get(key)
        qn = int(q or 0)
        same = (prev is not None and stable and prev["stable"]
                and v == prev["v"] and s == prev["s"] and qn == prev["q"])
        max_age_ms = meta.get("max_age_ms")
        heartbeat_s = (max_age_ms / 1000.0 * 0.5
                       if isinstance(max_age_ms, (int, float)) and max_age_ms > 0
                       else self.DEFAULT_HEARTBEAT_S)
        if same and (now - prev["ts"]) < heartbeat_s:
            return True
        self._last_enqueued[key] = {"v": v, "s": s, "q": qn, "stable": stable, "ts": now}
        return False

    # ------------------------------------------------------------------
    # driver -> đẩy giá trị vào buffer cho serial đó (chưa gửi ngay)
    # ------------------------------------------------------------------
    def _on_value(self, serial, ch, v, s, q, ts, stable):
        try:
            self.store.history_insert_many([(serial, ch, ts or time.time(), v, s, int(q or 0),
                                             1 if stable else 0)])
        except Exception:                                            # noqa: BLE001
            # history là phụ trợ cho Live activity local - lỗi ở đây KHÔNG được
            # phép chặn đường outbox/Odoo (durable-first, xem finding python-reviewer
            # 2026-09-29: đảo thứ tự trước đó vô tình làm mất cả outbox nếu history throw).
            _logger.exception("lỗi ghi history cho %s/%s", serial, ch)
        if self._should_skip_duplicate(serial, ch, v, s, q, stable):
            return
        item = {"ch": ch, "v": v, "s": s, "q": int(q or 0), "stable": stable}
        if ts is not None:
            item["ts"] = int(ts * 1000)
        self._pending.setdefault(serial, []).append(item)

    def push_node_reading(self, serial, ch, v, s, q, ts, stable):
        """Lối vào từ node_api.py (node HTTP đẩy thẳng, kind=http_node) — cùng
        một đường ống với driver.on_reading()."""
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
                    _logger.warning("hello thất bại: %s", res.get("error"))
                await self.odoo.source_status(self.manager.status_rows(), self._mqtt_connected())
            except Exception:                                        # noqa: BLE001
                _logger.exception("lỗi trong hello_loop")
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
                        _logger.info("đã áp dụng config_version=%s", rev)
            except Exception:                                        # noqa: BLE001
                _logger.exception("lỗi trong config_loop")
            await asyncio.sleep(settings.config_poll_interval_s)

    async def _flush_loop(self):
        """Gộp buffer -> outbox mỗi submit_interval_s (bid=boot_id cố định,
        seq tăng dần bên ngoài — cho phép dedup (serial,bid,seq) ở Odoo)."""
        while not self._stopping.is_set():
            await asyncio.sleep(settings.submit_interval_s)
            if not self._pending:
                continue
            batch, self._pending = self._pending, {}
            for serial, items in batch.items():
                seq = self.store.next_seq(serial)
                self.store.outbox_push(serial, self._boot_id, seq, {"items": items})

    # 21/09: một vòng CHỈ gửi một bản ghi rồi ngủ 0,2 s — tối đa ~2,2 bản/giây.
    # _flush_loop đẩy ra 1/submit_interval_s = 4 bản/giây. Sản xuất > tiêu thụ
    # nên tồn kho lớn dần và độ trễ tăng theo thời gian: "chạy một hồi là nó
    # trễ so với cân nhảy thực tế". Rút cạn tồn kho trong một vòng, và chỉ ngủ
    # khi không còn gì để gửi.
    MAX_MOI_VONG = 30

    # 24/09: rút cạn outbox từng serial TUẦN TỰ (1 lúc 1 serial) không scale
    # khi số serial lên tới hàng trăm - 1 vòng drain sẽ cần (số serial) lần
    # round-trip HTTP nối tiếp tới Odoo, có thể vượt hẳn submit_interval_s và
    # làm độ trễ forward tăng dần theo thời gian (KHÔNG mất dữ liệu - outbox
    # vẫn giữ - chỉ chậm). Giới hạn số serial gửi ĐỒNG THỜI bằng semaphore,
    # thả lỏng chiều song song mà không bắn phá Odoo bằng hàng trăm request
    # cùng lúc. Thứ tự bên TRONG 1 serial VẪN tuần tự (giữ đúng invariant
    # "dừng lại cho serial này khi lỗi" - chỉ serial KHÁC nhau mới chạy
    # song song với nhau).
    MAX_CONCURRENT_SERIALS = 8

    # 29/09: 429 ("edge busy, retry", Retry-After delta-seconds) hoặc 5xx từ
    # /pcm/api/v1/measurements -> giãn nhịp GỬI TIẾP cho ĐÚNG serial đó, tránh
    # "đồng loạt gửi lại" (thundering herd) khi Odoo vừa phục hồi sau downtime
    # mà nhiều edge/serial cùng retry cùng lúc. Retry-After là SÀN (không gửi
    # sớm hơn), nhân đôi mỗi lần liên tiếp thất bại, trần BACKOFF_CAP_S, +-
    # jitter để các serial không đồng bộ nhau. Reset về BACKOFF_BASE_S ngay
    # khi 1 lần gửi thành công. KHÔNG áp dụng cho lỗi mạng thuần túy (không có
    # status_code — vẫn theo nhịp cũ của _sender_loop) và KHÔNG áp dụng cho
    # hello/config/heartbeat (khác loop, khác endpoint — đúng theo đúng phạm
    # vi contract đã chốt với pcm_base).
    BACKOFF_BASE_S = 1.0
    BACKOFF_CAP_S = 30.0
    BACKOFF_JITTER = 0.2

    def _note_backoff(self, serial: str, retry_after) -> None:
        delay = self._backoff_delay.get(serial, self.BACKOFF_BASE_S)
        if isinstance(retry_after, (int, float)) and retry_after > 0:
            delay = max(delay, float(retry_after))
        delay = min(delay, self.BACKOFF_CAP_S)
        jittered = delay * (1 + random.uniform(-self.BACKOFF_JITTER, self.BACKOFF_JITTER))
        self._backoff_until[serial] = time.monotonic() + max(jittered, 0.0)
        self._backoff_delay[serial] = min(delay * 2, self.BACKOFF_CAP_S)
        # Chỉ log Ở ĐÂY (lúc SET), không log ở nhánh skip đầu _drain_serial -
        # _sender_loop có thể gọi lại serial này mỗi 0.02s trong lúc chờ hết
        # backoff (khi serial KHÁC đang sent_any=True), log ở đó sẽ spam hàng
        # chục dòng/giây - xem python-reviewer 2026-09-29 (finding thiếu log
        # khiến vận hành viên tưởng nhầm bug khác khi thấy 1 serial "im lặng").
        _logger.info("giãn nhịp gửi %s: %.1fs (retry_after=%s)",
                     serial, jittered, retry_after)

    def _clear_backoff(self, serial: str) -> None:
        self._backoff_until.pop(serial, None)
        self._backoff_delay.pop(serial, None)

    async def _drain_serial(self, serial: str) -> bool:
        if time.monotonic() < self._backoff_until.get(serial, 0.0):
            return False    # đang trong thời gian giãn nhịp cho serial này
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
                _logger.info("gửi measurements thất bại cho %s: %s",
                             serial, res.get("error"))
                status = res.get("status_code")
                if status == 429 or (isinstance(status, int) and status >= 500):
                    self._note_backoff(serial, res.get("retry_after"))
                break    # giữ thứ tự — dừng lại cho serial này
            self.store.outbox_delete(row["id"])
            self._clear_backoff(serial)
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
                    # return_exceptions=True: 1 serial raise (vd lỗi mạng lạ, bug
                    # driver...) KHÔNG được phép giết cả gather() - mặc định của
                    # asyncio.gather() sẽ làm CẢ task _sender_loop chết vĩnh viễn
                    # (không watchdog tự respawn), y hệt loại bug OdooClient._parse
                    # đã fix 2026-09-24 nhưng áp dụng cho MỌI nguồn lỗi tương lai,
                    # không chỉ riêng lỗi đó.
                    results = await asyncio.gather(
                        *(_drain_with_limit(s) for s in serials), return_exceptions=True)
                    for serial, res in zip(serials, results):
                        if isinstance(res, BaseException):
                            _logger.exception("lỗi không lường trước khi gửi outbox cho %s",
                                              serial, exc_info=res)
                            continue
                        if res:
                            sent_any = True
            except Exception:                                          # noqa: BLE001
                # Cùng pattern try/except+log như 5 vòng lặp còn lại của EdgeAgent
                # (_hello_loop/_config_loop/_heartbeat_loop/_print_loop/_gc_loop) -
                # trước đây _sender_loop là vòng DUY NHẤT thiếu, nên 1 lỗi ngoài dự
                # kiến (vd outbox_serials() tự nó lỗi) sẽ giết cả vòng lặp mà
                # không ai biết - xem review 2026-09-24 (câu hỏi scale hàng trăm
                # sensor của Nam).
                _logger.exception("lỗi trong sender_loop")
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
                    _logger.exception("heartbeat thất bại cho %s", serial)
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
                    continue          # có job vừa rồi -> kiểm tra ngay job kế tiếp
            except Exception:                                        # noqa: BLE001
                _logger.exception("lỗi trong print_loop")
            await asyncio.sleep(settings.print_poll_interval_s)

    async def _gc_loop(self):
        while not self._stopping.is_set():
            cutoff = time.time() - HISTORY_RETENTION_DAYS * 86400
            removed = self.store.history_gc(cutoff)
            if removed:
                _logger.info("đã dọn %d dòng lịch sử cũ hơn %d ngày", removed, HISTORY_RETENTION_DAYS)
            await asyncio.sleep(3600)
