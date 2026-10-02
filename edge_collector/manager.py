# -*- coding: utf-8 -*-
"""SourceManager — nạp 'config' trả về từ GET/POST /pcm/api/v1/edge/config
(edge_config() bên pcm_edge.py) thành các driver đang chạy, định tuyến lệnh/
browse/status theo đúng kênh, và báo giá trị mới về cho scheduler ghi sổ +
đẩy lên Odoo.

Chỉ RESTART driver của nguồn nào có config_rev đổi (mỗi nguồn mang config_rev
riêng trong _as_config) — sửa một nguồn không làm gián đoạn các nguồn khác.

Ngoài driver theo pcm.source, SourceManager còn giữ hàng đợi lệnh cho NODE
đẩy HTTP trực tiếp (kind=http_node — Pi/PC bridge, xem node_api.py): node
không thể bị gọi ngược (chỉ outbound), nên lệnh từ Odoo được XẾP HÀNG cho
node tự POLL lấy, kết quả được ACK về và trả lời lại cho /api/command đang
chờ (queue_command / node_pull_command / node_ack_command).
"""
import asyncio
import json
import logging
import random
import time
from typing import Callable, Optional

from .mqtt_consumer import _canonical, _sign
from .drivers.base import SourceDriver
from .drivers.mqtt import MqttDriver
from .drivers.modbus import ModbusDriver
from .drivers.opcua import OpcuaDriver
from .drivers.serial_ascii import SerialDriver
from .drivers.sim import SimDriver

_logger = logging.getLogger("edge.manager")

OnValue = Callable[[str, str, object, object, int, object, object], None]

# Firmware ESP32 phân tích gói lệnh bằng bộ đệm 192 byte - gói dài hơn bị cắt
# và lệnh im lặng không chạy. Chỉ dùng để quyết có gắn request_id cho node
# CHƯA khai sig_cmd hay không.
NODE_CMD_MAX_BYTES = 192
# Node đã khai sig_cmd (firmware mới) dùng bộ đệm 320 byte (mqtt_link.c
# CMD_JSON_MAX, bỏ gói khi data_len >= 320) - gói ký đi dạng canonical.
NODE_SIGNED_CMD_MAX_BYTES = 320


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
        # (serial, ch) -> {"max_age_ms": int|None, "must_send_every": bool}. Odoo
        # tính must_send_every = raw_forward or write_mode=='add' or bool(trigger_ids)
        # (contract chốt 2026-09-29 với pcm_base) - thiếu field/không rõ -> mặc định
        # True (an toàn, giữ hành vi gửi-mỗi-lần cũ) ở channel_meta_for() bên dưới.
        self._channel_meta: dict[tuple, dict] = {}

        self._node_last_seen: dict[str, float] = {}
        self._node_queues: dict[str, "asyncio.Queue"] = {}
        self._node_futures: dict[int, "asyncio.Future"] = {}
        self._node_cmd_owner: dict[int, str] = {}   # cmd_id -> serial được gửi lệnh
        # Bắt đầu từ số ngẫu nhiên mỗi lần khởi động: ack muộn của lệnh trước
        # restart không trùng id với lệnh mới (vẫn là int - firmware đọc số).
        self._node_cmd_seq = random.randint(1, 2 ** 30)

    # ------------------------------------------------------------------
    def is_raw_forward(self, ch_code: str) -> bool:
        return ch_code in self.raw_channels

    def device_meta(self, serial: str) -> dict:
        return self.devices_by_serial.get(serial) or {}

    def channel_meta_for(self, serial: str, ch_code: str) -> dict:
        """max_age_ms/must_send_every cho 1 channel - dùng ở EdgeAgent._on_value()
        để quyết định có lọc-trùng/heartbeat được không (xem _channel_meta)."""
        return self._channel_meta.get((serial, ch_code)) or {
            "max_age_ms": None, "must_send_every": True,
        }

    def known_serials(self) -> list:
        return list(self.devices_by_serial.keys())

    def get_driver(self, source_code: str) -> Optional[SourceDriver]:
        return self._drivers.get(source_code)

    def mqtt_topic_bases(self) -> list:
        """Gốc topic của mọi nguồn MQTT đang chạy (xem /api/publish)."""
        return [d.topic_base() for d in self._drivers.values() if isinstance(d, MqttDriver)]

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
        channel_meta: dict = {}
        for dev in cfg.get("devices") or []:
            for ch in dev.get("channels") or []:
                src_code = ch.get("source")
                if src_code:
                    channels_by_source.setdefault(src_code, []).append(ch)
                    route[(dev["serial"], ch["code"])] = src_code
                # Chuẩn hóa None -> True Ở ĐÂY (không phải ở nơi đọc): .get(key, True)
                # chỉ áp default khi key VẮNG MẶT, còn key có mặt với value None (vd
                # Odoo serialize JSON null) sẽ lọt qua thành None (falsy) - nguy hiểm thật
                # cho channel counter/trigger/raw-forward - xem finding python-reviewer
                # 2026-09-29.
                me = ch.get("must_send_every")
                channel_meta[(dev["serial"], ch["code"])] = {
                    "max_age_ms": ch.get("max_age_ms"),
                    "must_send_every": True if me is None else bool(me),
                }
        self._route = route
        self._channel_meta = channel_meta

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
                _logger.info("nguồn %s (%s) đã khởi động, rev=%s", code, src_cfg.get("kind"), rev)
            except Exception as exc:                                # noqa: BLE001
                _logger.warning("nguồn %s khởi động thất bại: %s", code, exc)

    async def _stop_source(self, code: str) -> None:
        drv = self._drivers.pop(code, None)
        self._source_rev.pop(code, None)
        if drv:
            try:
                await drv.stop()
            except Exception:                                       # noqa: BLE001
                pass

    def build_probe(self, src_cfg: dict) -> SourceDriver:
        """Driver dùng một lần cho pcm.source.action_test() — không đăng ký
        vào self._drivers, không ảnh hưởng tới thu thập đang chạy."""
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
        raise ValueError("không hỗ trợ kind=%s" % kind)

    async def shutdown(self) -> None:
        for code in list(self._drivers):
            await self._stop_source(code)

    # ------------------------------------------------------------------
    # Node http (Pi/PC bridge) — node.py trong node_api.py gọi vào đây.
    # ------------------------------------------------------------------
    def cached_node_api_key(self, serial: str) -> Optional[str]:
        """Khóa Odoo đã cấp cho thiết bị này (device._as_config()['api_key']),
        nếu edge đã từng kéo config và biết về serial này."""
        dev = self.devices_by_serial.get(serial)
        return (dev or {}).get("api_key") or None

    def touch_node(self, serial: str) -> None:
        self._node_last_seen[serial] = time.time()

    def known_node_serials(self) -> list:
        return list(self._node_last_seen.keys())

    def has_node_or_driver(self, serial: str, ch_code: str) -> bool:
        return (serial, ch_code) in self._route or serial in self._node_last_seen

    async def queue_command(self, serial: str, ch: str, cmd: str, value,
                            timeout: float = 8.0, extra: Optional[dict] = None,
                            request_id: Optional[str] = None) -> dict:
        """Xếp một lệnh cho NODE (không có driver điều khiển được — vd http_node),
        cho node tự poll rồi ack. Dùng khi driver_for_channel() trả về None."""
        self._node_cmd_seq += 1
        cmd_id = self._node_cmd_seq
        payload = {"id": cmd_id, "channel": ch, "cmd": cmd, "value": value}
        if extra:
            payload.update(extra)

        mq = getattr(self, "mqtt_cmd", None)
        # Ký HMAC chỉ khi node TỰ KHAI "sig_cmd" trong status: firmware cũ
        # (bộ đệm 192 byte) nhận gói có sig/ts sẽ bị cắt và lệnh không chạy.
        # Format chốt 2026-10-02 với node_agent: thêm request_id/ts trước,
        # ký sau cùng, canonical y như chiều lên (mqtt_consumer._sign).
        if mq is not None and getattr(mq, "sig_caps", {}).get(serial):
            api_key = self.cached_node_api_key(serial)
            if not api_key:
                return {"ok": False, "status": "error",
                        "error": "edge chưa có api_key của %s để ký lệnh" % serial}
            signed = self._sign_node_command(api_key, payload, request_id)
            if len(_canonical(signed)) >= NODE_SIGNED_CMD_MAX_BYTES and request_id:
                _logger.info("lệnh #%s cho %s: bỏ request_id vì gói ký vượt %d byte",
                             cmd_id, serial, NODE_SIGNED_CMD_MAX_BYTES)
                signed = self._sign_node_command(api_key, payload, None)
            if len(_canonical(signed)) >= NODE_SIGNED_CMD_MAX_BYTES:
                return {"ok": False, "status": "error",
                        "error": "lệnh quá dài cho bộ đệm %d byte của node" % NODE_SIGNED_CMD_MAX_BYTES}
            payload = signed
        elif request_id:
            with_rid = dict(payload, request_id=request_id)
            # '<' chứ không '<=': firmware giữ 1 byte cho NUL (mqtt_link.c
            # bỏ gói khi data_len >= 192). Đo cả 2 dạng gói thật sự đi ra:
            # MQTT (json.dumps mặc định) và poll HTTP (Starlette JSON gọn, bọc
            # trong {"command": ...}, node_api.node_commands) - lấy cái dài hơn.
            sizes = (len(json.dumps(with_rid)),
                     len(json.dumps({"command": with_rid}, separators=(",", ":"))))
            if max(sizes) < NODE_CMD_MAX_BYTES:
                payload = with_rid
            else:
                _logger.info("lệnh #%s cho %s: bỏ request_id vì gói vượt %d byte",
                             cmd_id, serial, NODE_CMD_MAX_BYTES)

        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._node_futures[cmd_id] = fut
        self._node_cmd_owner[cmd_id] = serial
        try:
            return await self._dispatch_node_command(mq, serial, cmd_id, payload, fut, timeout)
        finally:
            self._node_futures.pop(cmd_id, None)
            self._node_cmd_owner.pop(cmd_id, None)
            # Dọn mục NO_CONN còn sót (ACK tới sau khi paho đã gửi lại, hoặc
            # task bị cancel) - lệnh đã gửi thì cancel_pending chỉ quên nó.
            cancel = getattr(mq, "cancel_pending", None)
            if cancel is not None:
                cancel(cmd_id)

    async def _dispatch_node_command(self, mq, serial: str, cmd_id: int, payload: dict,
                                     fut: "asyncio.Future", timeout: float) -> dict:
        # Đường MQTT trước, hàng đợi poll làm dự phòng.
        #
        # Khác biệt không nhỏ: hàng đợi poll bắt node tự đi hỏi mỗi 2 giây,
        # nên độ trễ trung bình của một lần bấm đến là ~1,1 giây CHỈ để biết
        # rằng có lệnh. MQTT đẩy thẳng xuống.
        #
        # Chọn đường dựa trên cờ "cmd" node tự báo trong <gốc>/<serial>/status
        # chứ không dựa trên cấu hình bên này: firmware cũ không biết nghe
        # MQTT vẫn phải được phục vụ bằng hàng đợi, và nó tự nói điều đó.
        if mq is not None and mq.publish_command(serial, payload):
            pass
        elif mq is not None and mq.caps.get(serial):
            # Firmware này nói MQTT nhưng gửi không được (node rớt, hoặc mất
            # broker). KHÔNG được xếp vào hàng đợi poll: firmware đã tắt HTTP
            # nên không còn ai gọi /node/v1/commands để lấy ra — lệnh sẽ nằm
            # đó mãi mãi, vừa rò rỉ bộ nhớ vừa báo sai nguyên nhân cho người
            # bấm nút. Báo thật luôn.
            return {"ok": False, "status": "error",
                    "error": "node %s đang không kết nối tới broker" % serial}
        else:
            # Firmware cũ, vẫn tự poll /node/v1/commands.
            self._node_queues.setdefault(serial, asyncio.Queue()).put_nowait(payload)
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            # Lệnh paho còn GIỮ vì mất broker (NO_CONN, chưa gửi) thì rút ra:
            # vừa không để node chạy muộn, vừa biết CHẮC là lệnh chưa đi.
            cancel = getattr(mq, "cancel_pending", None)
            if cancel is not None and cancel(cmd_id):
                return {"ok": False, "status": "error",
                        "error": "mất kết nối broker, lệnh chưa gửi được và đã hủy"}
            # KHÔNG thể phân biệt "lệnh thật sự không chạy được" với "đã chạy
            # nhưng ACK bị mất mạng" - node_ack_command() không bao giờ được
            # gọi thì future ở đây chỉ biết nó chờ quá lâu, không biết kết
            # quả thật sự là gì. "ok": False giữ NGUYÊN để tương thích ngược.
            #
            # "status": "unknown" dùng LẠI field "status" đã có sẵn (node_ack_
            # command() đặt "ok"/"error", đã được pcm_base whitelist xuyên qua
            # api_iot.py/pod_screen's api.py tới frontend Tags.tsx sẵn - phối
            # hợp 2026-09-24 với session Odoo: giá trị thứ 3 này không cần sửa
            # gì thêm phía Odoo, Tags.tsx tự hiển thị đúng chuỗi status/error
            # thay vì "Thất bại" cứng, tránh người vận hành bấm lại lệnh đã
            # chạy xong thật.
            return {"ok": False, "status": "unknown",
                    "error": "node không trả lời trong %.0fs - có thể đã thực thi "
                             "nhưng mất ACK" % timeout}

    @staticmethod
    def _sign_node_command(api_key: str, payload: dict, request_id: Optional[str]) -> dict:
        signed = dict(payload)
        if request_id:
            signed["request_id"] = request_id
        signed["ts"] = int(time.time())
        signed["sig"] = _sign(api_key, signed)
        return signed

    def node_pull_command(self, serial: str) -> Optional[dict]:
        """Lệnh kế tiếp còn hiệu lực cho node poll. Lệnh mà queue_command() đã
        thôi chờ (timeout) bị BỎ QUA: firmware cũ poll muộn không được chạy
        lệnh người dùng đã được báo là 'không xác nhận'."""
        q = self._node_queues.get(serial)
        while q and not q.empty():
            payload = q.get_nowait()
            if payload["id"] in self._node_futures:
                return payload
            _logger.info("bỏ lệnh #%s cho %s: đã hết hạn chờ ACK", payload["id"], serial)
        return None

    def node_ack_command(self, cmd_id: int, ok: bool, detail: str = "", *,
                         serial: str) -> bool:
        fut = self._node_futures.get(cmd_id)
        if not fut or fut.done():
            return False
        owner = self._node_cmd_owner.get(cmd_id)
        if owner != serial:
            _logger.warning("bỏ ACK lệnh #%s từ %s: lệnh này gửi cho %s", cmd_id, serial, owner)
            return False
        fut.set_result({"ok": ok, "status": "ok" if ok else "error", "error": None if ok else detail})
        return True
