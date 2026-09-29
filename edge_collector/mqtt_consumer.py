# -*- coding: utf-8 -*-
"""Bên ĐỌC của đường MQTT: nhận số đo node phát lên broker.

Vì sao có file này: node (component mqtt_link) đã phát lên
`fms/<serial>/meas`, nhưng một topic exchange không có ai đăng ký thì broker
VỨT thông điệp đi. Đó là trạng thái trước khi có file này — nửa đường ống.

Đường đi:

    node --MQTT--> broker --MQTT--> file này --> agent.push_node_reading()
                                                        |
                                            (đúng của mà node_api.py dùng)
                                                        |
                                              outbox SQLite --> Odoo

Dùng ĐÚNG của vào `push_node_reading()` của đường HTTP, nên không có bộ
phân tích thứ hai, không có đường ghi thứ hai vào outbox, và mọi thứ phía
sau (chống mất mẫu, gom lô, gửi lại khi đứt mạng) dùng y nguyên.


HAI CHIỀU, và từ 19/09 là đường DUY NHẤT
----------------------------------------
Chiều lên   `fms/<serial>/meas`    số đo  -> push_node_reading()
            `fms/<serial>/status`  online/Last Will, và có "cmd"
Chiều xuống `fms/<serial>/cmd`     lệnh   <- manager.queue_command()
            `fms/<serial>/cmdack`  kết quả -> manager.node_ack_command()

Node đã tắt `CONFIG_UPLINK_HTTP_ENABLE`, nên `/node/v1/*` không còn ai gọi.
`EDGE_MQTT_CONSUMER_FORWARD` PHẢI là true — để false thì số đo chạy tới đây
rồi dừng lại, không có đường nào khác tới Odoo.

Ngược lại cũng đúng: đừng bật forward khi node vẫn còn phát cả hai đường,
vì khi đó Odoo nhận mỗi mẫu hai lần.

Chọn đường cho LỆNH dựa trên có "cmd" node tự khai trong chủ đề status,
không dựa trên cấu hình bên này. Firmware cũ không biết nghe MQTT thì
không khai, và manager tự quay về hàng đợi poll cho nó — một hàm xóm có thể
chạy lẫn firmware mà không phải sửa gì ở đây.

Nhịp tim cũng do đây lo: trước kia node tự POST /node/v1/heartbeat, cắt HTTP
là mất cái đó và thiết bị sẽ chuyển "offline" bên Odoo trong khi số đo vẫn
chạy về đều. _heartbeat_loop() dịch trạng thái broker sang tiếng nói mà Odoo
đang nghe.


Bền bỉ khi consumer chết
------------------------
Dùng phiên MQTT bền bỉ: `clean_session=False` + client_id cố định + đăng ký
QoS 1. Broker giữ thông điệp lại cho đúng client_id đó trong lúc nó vắng
mặt (RabbitMQ: `mqtt.max_session_expiry_interval_seconds`, đang đặt 3600 s).
Sống lại là đọc tiếp từ chỗ đứt, không mất mẫu.

Đây là lý do client_id PHẢI cố định và PHẢI khác nhau giữa các tiến trình:
hai bên dùng chung một client_id sẽ đá nhau ra khỏi broker liên tục.
"""
import asyncio
import collections
import hashlib
import hmac
import json
import logging
import time

import paho.mqtt.client as mqtt

from .config import settings

_logger = logging.getLogger("edge.mqtt_consumer")

# Giới hạn một gói — node gửi tối đa 50 bản ghi một lô (CONFIG_MQTT_LINK_
# BATCH_MAX), lấy dư gấp bội để còn chỗ firmware khác, nhưng vẫn chặn một
# gói hỏng/ác ý làm nghẽn vòng lặp.
MAX_ITEMS = 1000

# Nhịp nhịp-tim thay mặt node. Xem _heartbeat_loop().
HEARTBEAT_S = 30

# Coi là đồng hồ chưa đồng bộ nếu trước mốc này (2020-09). Bằng đúng ngưỡng
# EPOCH_SANE_MS của firmware.
EPOCH_SANE_S = 1600000000

# Số gói giữ lại cho trang /ops. 300 là khoảng 5 phút ở nhịp hiện tại — đủ
# để nhìn thấy một lần bấm nút đi và về, mà không giữ lịch sử trong RAM của
# một tiến trình đang chạy 24/7.
TRAFFIC_MAX = 300


class MqttConsumer:
    """Đọc `fms/<serial>/meas` và `fms/<serial>/status` từ broker."""

    def __init__(self, agent):
        self._agent = agent
        self._cli = None
        self._loop = None
        self._connected = False
        self.stats = {
            "connected": False,
            "messages": 0,      # số gói MQTT nhận được
            "items": 0,         # số BẢN GHI số đo — con số để đối chiếu với HTTP
            "forwarded": 0,     # số bản ghi thực sự đẩy vào outbox
            "bad": 0,           # gói không phân tích được
            "last_ts": None,
            "by_serial": {},    # serial -> số bản ghi
            "online": {},       # serial -> True/False theo chủ đề status
            "cmd_sent": 0,      # số lệnh đã đẩy XONG xuống socket, xác nhận ngay
            "cmd_sent_no_conn": 0,  # số lệnh giao cho paho giữ (rc=NO_CONN),
                                    # chờ gửi lại khi reconnect - CHƯA chắc đã
                                    # ra khỏi tiến trình này - xem publish_command()
            "cmd_acked": 0,     # số ack lệnh nhận lại
            "ts_dropped": 0,    # số bản ghi có dấu thời gian vô lý
            "sig_rejected": 0,  # số gói có "sig" nhưng xác minh HMAC thất bại
        }
        # serial -> firmware có biết nhận lệnh qua MQTT không (có "cmd" trong
        # chủ đề status). Không đoán: node tự khai.
        self.caps = {}
        self._hb_task = None
        # Nhật ký gói tin cho trang /ops. Vòng đệm trong BỘ NHỚ, không ghi
        # đĩa: đây là kính lúc, không phải sổ sách. SQLite history mới là
        # nơi số liệu sống.
        self.traffic = collections.deque(maxlen=TRAFFIC_MAX)
        self._ev_seq = 0
        # code kênh đèn -> {"v": 0/1, "ts": ..., "serial": ...}
        self.lamps = {}

    # -- vong doi ------------------------------------------------------
    async def start(self) -> None:
        if not settings.mqtt_consumer_enabled:
            _logger.info("tắt trong cấu hình, không chạy")
            return
        self._loop = asyncio.get_event_loop()

        host, port = _split_url(settings.mqtt_consumer_url)
        # clean_session=False: xem phần "Bền bỉ" ở đầu file.
        self._cli = mqtt.Client(client_id=settings.mqtt_consumer_client_id,
                                clean_session=False)
        if settings.mqtt_consumer_user:
            self._cli.username_pw_set(settings.mqtt_consumer_user,
                                      settings.mqtt_consumer_pass or "")
        self._cli.on_connect = self._on_connect
        self._cli.on_message = self._on_message
        self._cli.on_disconnect = self._on_disconnect
        self._cli.connect_async(host, port, keepalive=30)
        self._cli.loop_start()
        self._hb_task = self._loop.create_task(self._heartbeat_loop())
        _logger.info("đang nối broker %s:%s, chủ đề %s (chế độ %s)",
                     host, port, settings.mqtt_consumer_topic,
                     "ĐẨY VÀO ODOO" if settings.mqtt_consumer_forward else "bóng/chỉ đếm")

    async def stop(self) -> None:
        if self._hb_task:
            self._hb_task.cancel()
            self._hb_task = None
        if self._cli:
            self._cli.loop_stop()
            self._cli.disconnect()
            self._cli = None

    # -- callback của paho (chạy trên THREAD RIÊNG) ---------------------
    #
    # Không dùng thẳng vào store/agent ở đây: chuyển về vòng lặp asyncio
    # bằng call_soon_threadsafe, giống drivers/mqtt.py. Giữ đúng thứ tự và
    # không để I/O SQLite chặn vòng mạng của paho.
    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            client.subscribe([(settings.mqtt_consumer_topic, 1),
                              (settings.mqtt_consumer_status_topic, 1),
                              (_ack_topic(), 1)])
            self._loop.call_soon_threadsafe(self._set_connected, True)
        else:
            self._loop.call_soon_threadsafe(
                self._log_error, "nối broker thất bại rc=%s" % rc)

    def _on_disconnect(self, client, userdata, rc):
        self._loop.call_soon_threadsafe(self._set_connected, False)

    def _on_message(self, client, userdata, msg):
        self._loop.call_soon_threadsafe(self._handle, msg.topic, msg.payload)

    # -- chạy trên vòng lặp asyncio -------------------------------------
    def _set_connected(self, ok: bool):
        self._connected = ok
        self.stats["connected"] = ok
        _logger.info("broker: %s", "đã nối" if ok else "mất kết nối")

    def _log_error(self, text: str):
        _logger.warning("%s", text)

    def _subscribe_status(self, serial: str) -> None:
        """Đăng ký chủ đề status chính xác của một serial (lấy ảnh chụp retained)."""
        if not self._cli:
            return
        topic = settings.mqtt_consumer_status_topic.replace("+", serial, 1)
        if "+" in topic or "#" in topic:
            return          # mẫu chủ đề không có chỗ để thay serial vào
        try:
            self._cli.subscribe(topic, 1)
            _logger.info("đăng ký thêm %s", topic)
        except Exception as exc:                                  # noqa: BLE001
            _logger.warning("không đăng ký được %s: %s", topic, exc)

    def _log_event(self, direction: str, topic: str, nbytes: int, note: str):
        self._ev_seq += 1
        self.traffic.append({"seq": self._ev_seq, "t": time.time(),
                             "dir": direction, "topic": topic,
                             "bytes": nbytes, "note": note[:160]})

    def _handle(self, topic: str, raw: bytes):
        serial, kind = _parse_topic(topic)
        if not serial:
            return
        try:
            data = json.loads(raw.decode("utf-8", errors="replace"))
        except ValueError:
            self.stats["bad"] += 1
            return
        if not isinstance(data, dict):
            self.stats["bad"] += 1
            return

        # HMAC tùy chọn: node cũ (ESP32 chưa nâng cấp) không gửi "sig", vẫn
        # cho qua như trước giờ - không phá tương thích ngược. Node MỚI (có
        # api_key đã học qua /hello) tự ký, từ đó có "sig" - lúc đó BẮT BUỘC
        # xác minh đúng, sai là từ chối luôn (không có đường hạ tiêu chuẩn).
        # Cách này tự động đúng cho cả LWT ({"online":false} broker tự phát,
        # không thể ký động) vì LWT không có "sig" - không cần biết riêng
        # "status"/"online" ở đây.
        if "sig" in data:
            api_key = self._agent.manager.cached_node_api_key(serial)
            if not api_key or not _verify_sig(api_key, data):
                self.stats["sig_rejected"] += 1
                _logger.warning("node %s: sig sai/chưa xác minh được trên "
                                "chủ đề %s, bỏ qua gói", serial, kind)
                return

        if kind == "status":
            online = bool(data.get("online"))
            self.stats["online"][serial] = online
            # Node tự khai có nhận được lệnh qua MQTT không.
            #
            # KHÔNG xóa lời khai này khi node rớt mạng: biết nghe MQTT là
            # thuộc tính của FIRMWARE, còn sống hay chết là chuyện khác.
            # Gộp hai thứ làm một thì lúc node rớt, manager tưởng đây là
            # firmware cũ và xếp lệnh vào hàng đợi poll — mà firmware mới
            # không bao giờ poll nữa.
            if online and data.get("cmd"):
                self.caps[serial] = True
            _logger.info("node %s: %s%s", serial,
                         "online" if online else "OFFLINE (Last Will)",
                         ", nhận lệnh qua MQTT" if self.caps.get(serial) else "")
            self._log_event("up", topic, len(raw),
                            "đang chạy" if online else "ĐÃ TẮT (Last Will)")
            return

        if kind == "cmdack":
            cmd_id = data.get("id")
            if not isinstance(cmd_id, int):
                self.stats["bad"] += 1
                return
            self.stats["cmd_acked"] += 1
            # Dùng cho trả lời mà /node/v1/commands/ack vẫn dùng: future của
            # queue_command đang chờ ở đây, không có đường thứ hai.
            ok = bool(data.get("ok"))
            self._log_event("up", topic, len(raw),
                            "lệnh #%s %s%s" % (cmd_id, "OK" if ok else "TỪ CHỐI",
                                               "" if ok else ": " + str(data.get("detail") or "")))
            self._agent.manager.node_ack_command(
                cmd_id, ok, data.get("detail") or "")
            return

        items = data.get("items")
        if not isinstance(items, list):
            self.stats["bad"] += 1
            return

        # Lần đầu thấy một serial: đăng ký thêm chủ đề status CHÍNH XÁC của nó.
        #
        # Vì sao phải làm thế, thay vì tin vào 'fms/+/status' đã đăng ký ở
        # _on_connect: kho retained của RabbitMQ KHÔNG phục vụ đăng ký có ký
        # tự đại diện. Đo thật 18/09 trên chính broker này:
        #     'fms/68EE8F4F06A8/status' -> nhận được {"online":true}
        #     'fms/+/status'            -> không nhận gì
        # Đăng ký đại diện vẫn bắt được thay đổi SỐNG (Last Will, lần node
        # báo online), chỉ thiếu ảnh chụp retained lúc mình vừa khởi động.
        # Nên: đại diện cho sự kiện sống + chính xác cho ảnh chụp.
        if serial not in self.stats["by_serial"]:
            self._subscribe_status(serial)

        # Cho manager biết node này còn sống, y hệt node_api.py làm ở đường
        # HTTP — has_node_or_driver() và /api/command dựa vào đây.
        self._agent.manager.touch_node(serial)

        n = 0
        preview = []
        for it in items[:MAX_ITEMS]:
            if not isinstance(it, dict):
                continue
            ch = it.get("ch")
            if not ch:
                continue
            n += 1
            # Dấu thời gian vô lý -> bỏ đi, để _on_value lấy giờ của edge.
            #
            # Trước đây node học giờ từ HAI nguồn: SNTP và trả lời của
            # /node/v1/hello. Cắt HTTP là mất nguồn thứ hai, nên một mạng
            # nhà máy không ra được pool.ntp.org sẽ làm mọi "ts" thành năm
            # 1970 — và nó chạy thẳng vào Odoo nếu không chặn ở đây.
            ts = it.get("ts")
            ts_s = (ts / 1000.0) if isinstance(ts, (int, float)) else None
            if ts_s is not None and ts_s < EPOCH_SANE_S:
                ts_s = None
                self.stats["ts_dropped"] += 1
            v = it.get("v")
            if len(preview) < 4:
                preview.append("%s=%s" % (ch, v))
            # Kênh đèn: giữ riêng giá trị mới nhất cho trang /ops. Node chỉ
            # báo khi có ai ghi (báo-khi-đổi), nên "ts" ở đây là lúc ĐỔI
            # gần nhất, không phải lúc đo gần nhất.
            #
            # LUÔN cập nhật, KHÔNG phụ thuộc mqtt_consumer_forward: /ops phải
            # phản ánh đúng trạng thái vật lý ngay cả khi chưa bật forward
            # vào Odoo (xem docstring đầu ops_api.py: "trang này phải xem
            # được đúng lúc Odoo hỏng") - xem python-test-writer 2026-09-24
            # (bắt qua test_handle_measurement_tracks_relay_channel_as_lamp).
            if str(ch).startswith("relay"):
                self.lamps[ch] = {"v": v, "ts": ts_s or time.time(),
                                  "serial": serial}
            if not settings.mqtt_consumer_forward:
                continue
            self._agent.push_node_reading(
                serial, ch, v, it.get("s"), int(it.get("q") or 0),
                ts_s, it.get("stable"),
            )
            self.stats["forwarded"] += 1

        self._log_event("up", topic, len(raw),
                        "%d bản ghi: %s" % (n, ", ".join(preview)))
        self.stats["messages"] += 1
        self.stats["items"] += n
        self.stats["last_ts"] = time.time()
        self.stats["by_serial"][serial] = self.stats["by_serial"].get(serial, 0) + n

    # -- chiều xuống: đẩy lệnh tới node ----------------------------------
    def publish_command(self, serial: str, payload: dict) -> bool:
        """Đẩy một lệnh xuống <gốc>/<serial>/cmd. Trả False nếu không gửi
        được — khi đó manager quay về hàng đợi poll của đường HTTP.

        QoS 1, KHÔNG retain: một lệnh bật đèn gửi lúc node mất điện không
        được phép tự bật lên khi nó sống lại nửa tiếng sau. Broker vứt đi
        lệnh gửi cho một node vắng mặt, đúng như ta muốn."""
        if not (self._cli and self._connected):
            return False
        if not self.caps.get(serial):
            return False        # node chưa khai là nhận được lệnh qua MQTT
        if not self.stats["online"].get(serial):
            # Chủ đề lệnh không retain và node không dùng phiên bền, nên gói
            # gửi cho một node vắng mặt bị broker vứt đi. Trả False để bên
            # gọi báo lỗi thật, thay vì báo "đã gửi" rồi im lặng.
            return False
        topic = _cmd_topic(serial)
        if topic is None:
            return False
        try:
            info = self._cli.publish(topic, json.dumps(payload), qos=1)
        except Exception as exc:                                  # noqa: BLE001
            _logger.warning("không đẩy được lệnh tới %s: %s", topic, exc)
            return False
        if info.rc == mqtt.MQTT_ERR_NO_CONN:
            # KHÔNG chắc chắn là chưa gửi - đây là 1 khe hẹp giữa self._connected
            # (có độ trễ qua call_soon_threadsafe) và socket thật của paho: nếu
            # socket vừa rớt đúng lúc gọi publish(), paho._send_publish() trả về
            # NO_CONN nhưng (đã đọc source paho-mqtt thật, không đoán) vẫn GIỮ
            # message trong self._out_messages (state=mqtt_ms_publish) và TỰ
            # ĐỘNG republish khi reconnect thành công - khác hẳn các rc lỗi khác
            # (thật sự không gửi được). Trả True ở đây để manager.queue_command()
            # tiếp tục chờ ACK thật (có thể tới từ paho tự gửi lại sau) thay vì
            # báo "chắc chắn thất bại" ngay - nếu ACK không tới kịp timeout, vòng
            # đó đã tự trả "status":"unknown" (khớp đúng ý nghĩa "có thể đã chạy
            # nhưng chưa xác nhận được") - xem review 2026-09-24 (finding từ vòng
            # phối hợp queue-command-unknown-timeout).
            #
            # Đếm riêng "cmd_sent_no_conn", KHÔNG đếm chung vào "cmd_sent" -
            # review 2026-09-24 (fix-mqtt-no-conn-race) chỉ ra gộp chung sẽ làm
            # "Lệnh gửi" ở /ops trông như đã gửi hết dù một phần đang chờ paho
            # gửi lại, dễ gây hiểu lầm lúc mạng site chập chờn.
            _logger.info("đẩy lệnh tới %s: NO_CONN, paho sẽ tự gửi lại khi "
                        "reconnect - coi như đã giao, chờ ACK thật", topic)
            self.stats["cmd_sent_no_conn"] += 1
            self._log_event("down", topic, len(json.dumps(payload)),
                            "lệnh #%s %s %s=%s (cho gui lai, mat ket noi tam thoi)" %
                            (payload.get("id"), payload.get("cmd"),
                             payload.get("channel"), payload.get("value")))
            return True
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            _logger.warning("đẩy lệnh tới %s thất bại rc=%s", topic, info.rc)
            return False
        self.stats["cmd_sent"] += 1
        self._log_event("down", topic, len(json.dumps(payload)),
                        "lệnh #%s %s %s=%s" % (payload.get("id"), payload.get("cmd"),
                                               payload.get("channel"), payload.get("value")))
        return True

    # -- nhịp tim thay mặt node ------------------------------------------
    async def _heartbeat_loop(self):
        """Báo Odoo rằng node còn sống.

        Trước kia chính node POST /node/v1/heartbeat mỗi 30 giây. Cắt HTTP
        là mất cái đó, và thiết bị sẽ chuyển sang "offline" bên Odoo trong
        khi số đo vẫn chạy về đều — một cái đèn báo nói dối.

        Nguồn sự thật mới là broker: "online" ở đây đến từ chủ đề status
        (retained) và từ Last Will, tức là broker TỰ báo khi node rớt chứ
        không phải đợi hết giờ một bộ đếm. Vòng này chỉ dịch điều đó sang
        tiếng nói mà Odoo đang nghe."""
        while True:
            try:
                await asyncio.sleep(HEARTBEAT_S)
                if not settings.mqtt_consumer_forward:
                    continue
                for serial, online in list(self.stats["online"].items()):
                    if not online:
                        continue
                    await self._agent.forward_node_heartbeat(
                        serial, {"transport": "mqtt", "buffered": 0})
            except asyncio.CancelledError:
                raise
            except Exception as exc:                              # noqa: BLE001
                _logger.warning("nhịp tim thất bại: %s", exc)


def _topic_sibling(last: str):
    """'fms/+/meas' -> 'fms/+/<last>'. Suy ra từ mẫu đã cấu hình, để không
    phải thêm một biến .env mới cho từng chủ đề."""
    parts = (settings.mqtt_consumer_topic or "").split("/")
    if len(parts) < 3:
        return None
    return "/".join(parts[:-1] + [last])


def _ack_topic():
    return _topic_sibling("cmdack") or "fms/+/cmdack"


def _cmd_topic(serial: str):
    t = _topic_sibling("cmd")
    if not t:
        return None
    t = t.replace("+", serial, 1)
    return None if ("+" in t or "#" in t) else t


def _canonical(payload: dict) -> bytes:
    """JSON dạng chính tắc (sorted keys, không khoảng trắng) để ký/xác minh -
    PHẢI khớp byte-for-byte với bên ký (mqtt_uplink.py của node_agent)."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def _sign(api_key: str, payload: dict) -> str:
    return hmac.new(api_key.encode(), _canonical(payload), hashlib.sha256).hexdigest()


def _verify_sig(api_key: str, data: dict) -> bool:
    """So sánh HMAC của data (trừ field "sig") với "sig" đính kèm. Dùng
    hmac.compare_digest để tránh timing attack."""
    sig = data.get("sig")
    if not isinstance(sig, str):
        return False
    rest = {k: v for k, v in data.items() if k != "sig"}
    return hmac.compare_digest(sig, _sign(api_key, rest))


def _split_url(url: str):
    """'mqtt://host:port' | 'host:port' | 'host' -> (host, port)."""
    raw = (url or "").strip()
    for prefix in ("mqtt://", "tcp://"):
        if raw.startswith(prefix):
            raw = raw[len(prefix):]
    host, _, port = raw.partition(":")
    try:
        return (host or "127.0.0.1"), int(port or 1883)
    except ValueError:
        return (host or "127.0.0.1"), 1883


def _parse_topic(topic: str):
    """'fms/68EE8F4F06A8/meas' -> ('68EE8F4F06A8', 'meas')."""
    parts = (topic or "").split("/")
    if len(parts) < 3:
        return None, None
    return parts[-2], parts[-1]
