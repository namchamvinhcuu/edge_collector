# -*- coding: utf-8 -*-
"""Ben DOC cua duong MQTT: nhan so do node phat len broker.

Vi sao co file nay: node (component mqtt_link) da phat len
`fms/<serial>/meas`, nhung mot topic exchange khong co ai dang ky thi broker
VUT thong diep di. Do la trang thai truoc khi co file nay — nua duong ong.

Duong di:

    node --MQTT--> broker --MQTT--> file nay --> agent.push_node_reading()
                                                        |
                                            (dung cua ma node_api.py dung)
                                                        |
                                              outbox SQLite --> Odoo

Dung DUNG cua vao `push_node_reading()` cua duong HTTP, nen khong co bo
phan tich thu hai, khong co duong ghi thu hai vao outbox, va moi thu phia
sau (chong mat mau, gom lo, gui lai khi dut mang) dung y nguyen.


HAI CHIEU, va tu 19/09 la duong DUY NHAT
----------------------------------------
Chieu len   `fms/<serial>/meas`    so do  -> push_node_reading()
            `fms/<serial>/status`  online/Last Will, va co "cmd"
Chieu xuong `fms/<serial>/cmd`     lenh   <- manager.queue_command()
            `fms/<serial>/cmdack`  ket qua -> manager.node_ack_command()

Node da tat `CONFIG_UPLINK_HTTP_ENABLE`, nen `/node/v1/*` khong con ai goi.
`EDGE_MQTT_CONSUMER_FORWARD` PHAI la true — de false thi so do chay toi day
roi dung lai, khong co duong nao khac toi Odoo.

Nguoc lai cung dung: dung bat forward khi node van con phat ca hai duong,
vi khi do Odoo nhan moi mau hai lan.

Chon duong cho LENH dua tren co "cmd" node tu khai trong chu de status,
khong dua tren cau hinh ben nay. Firmware cu khong biet nghe MQTT thi
khong khai, va manager tu quay ve hang doi poll cho no — mot ham xom co the
chay lan firmware ma khong phai sua gi o day.

Nhip tim cung do day lo: truoc kia node tu POST /node/v1/heartbeat, cat HTTP
la mat cai do va thiet bi se chuyen "offline" ben Odoo trong khi so do van
chay ve deu. _heartbeat_loop() dich trang thai broker sang tieng noi ma Odoo
dang nghe.


Ben bi khi consumer chet
------------------------
Dung phien MQTT ben bi: `clean_session=False` + client_id co dinh + dang ky
QoS 1. Broker giu thong diep lai cho dung client_id do trong luc no vang
mat (RabbitMQ: `mqtt.max_session_expiry_interval_seconds`, dang dat 3600 s).
Song lai la doc tiep tu cho dut, khong mat mau.

Day la ly do client_id PHAI co dinh va PHAI khac nhau giua cac tien trinh:
hai ben dung chung mot client_id se da nhau ra khoi broker lien tuc.
"""
import asyncio
import collections
import json
import logging
import time

import paho.mqtt.client as mqtt

from .config import settings

_logger = logging.getLogger("edge.mqtt_consumer")

# Gioi han mot goi — node gui toi da 50 ban ghi mot lo (CONFIG_MQTT_LINK_
# BATCH_MAX), lay du gap boi de con cho firmware khac, nhung van chan mot
# goi hong/ac y lam nghen vong lap.
MAX_ITEMS = 1000

# Nhip nhip-tim thay mat node. Xem _heartbeat_loop().
HEARTBEAT_S = 30

# Coi la dong ho chua dong bo neu truoc moc nay (2020-09). Bang dung nguong
# EPOCH_SANE_MS cua firmware.
EPOCH_SANE_S = 1600000000

# So goi giu lai cho trang /ops. 300 la khoang 5 phut o nhip hien tai — du
# de nhin thay mot lan bam nut di va ve, ma khong giu lich su trong RAM cua
# mot tien trinh dang chay 24/7.
TRAFFIC_MAX = 300


class MqttConsumer:
    """Doc `fms/<serial>/meas` va `fms/<serial>/status` tu broker."""

    def __init__(self, agent):
        self._agent = agent
        self._cli = None
        self._loop = None
        self._connected = False
        self.stats = {
            "connected": False,
            "messages": 0,      # so goi MQTT nhan duoc
            "items": 0,         # so BAN GHI so do — con so de doi chieu voi HTTP
            "forwarded": 0,     # so ban ghi thuc su day vao outbox
            "bad": 0,           # goi khong phan tich duoc
            "last_ts": None,
            "by_serial": {},    # serial -> so ban ghi
            "online": {},       # serial -> True/False theo chu de status
            "cmd_sent": 0,      # so lenh da day XONG xuong socket, xac nhan ngay
            "cmd_sent_no_conn": 0,  # so lenh giao cho paho giu (rc=NO_CONN),
                                    # cho gui lai khi reconnect - CHUA chac da
                                    # ra khoi tien trinh nay - xem publish_command()
            "cmd_acked": 0,     # so ack lenh nhan lai
            "ts_dropped": 0,    # so ban ghi co dau thoi gian vo ly
        }
        # serial -> firmware co biet nhan lenh qua MQTT khong (co "cmd" trong
        # chu de status). Khong doan: node tu khai.
        self.caps = {}
        self._hb_task = None
        # Nhat ky goi tin cho trang /ops. Vong dem trong BO NHO, khong ghi
        # dia: day la kinh luc, khong phai so sach. SQLite history moi la
        # noi so lieu song.
        self.traffic = collections.deque(maxlen=TRAFFIC_MAX)
        self._ev_seq = 0
        # code kenh den -> {"v": 0/1, "ts": ..., "serial": ...}
        self.lamps = {}

    # -- vong doi ------------------------------------------------------
    async def start(self) -> None:
        if not settings.mqtt_consumer_enabled:
            _logger.info("tat trong cau hinh, khong chay")
            return
        self._loop = asyncio.get_event_loop()

        host, port = _split_url(settings.mqtt_consumer_url)
        # clean_session=False: xem phan "Ben bi" o dau file.
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
        _logger.info("dang noi broker %s:%s, chu de %s (che do %s)",
                     host, port, settings.mqtt_consumer_topic,
                     "DAY VAO ODOO" if settings.mqtt_consumer_forward else "bong/chi dem")

    async def stop(self) -> None:
        if self._hb_task:
            self._hb_task.cancel()
            self._hb_task = None
        if self._cli:
            self._cli.loop_stop()
            self._cli.disconnect()
            self._cli = None

    # -- callback cua paho (chay tren THREAD RIENG) ---------------------
    #
    # Khong dung thang vao store/agent o day: chuyen ve vong lap asyncio
    # bang call_soon_threadsafe, giong drivers/mqtt.py. Giu dung thu tu va
    # khong de I/O SQLite chan vong mang cua paho.
    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            client.subscribe([(settings.mqtt_consumer_topic, 1),
                              (settings.mqtt_consumer_status_topic, 1),
                              (_ack_topic(), 1)])
            self._loop.call_soon_threadsafe(self._set_connected, True)
        else:
            self._loop.call_soon_threadsafe(
                self._log_error, "noi broker that bai rc=%s" % rc)

    def _on_disconnect(self, client, userdata, rc):
        self._loop.call_soon_threadsafe(self._set_connected, False)

    def _on_message(self, client, userdata, msg):
        self._loop.call_soon_threadsafe(self._handle, msg.topic, msg.payload)

    # -- chay tren vong lap asyncio -------------------------------------
    def _set_connected(self, ok: bool):
        self._connected = ok
        self.stats["connected"] = ok
        _logger.info("broker: %s", "da noi" if ok else "mat ket noi")

    def _log_error(self, text: str):
        _logger.warning("%s", text)

    def _subscribe_status(self, serial: str) -> None:
        """Dang ky chu de status chinh xac cua mot serial (lay anh chup retained)."""
        if not self._cli:
            return
        topic = settings.mqtt_consumer_status_topic.replace("+", serial, 1)
        if "+" in topic or "#" in topic:
            return          # mau chu de khong co cho de thay serial vao
        try:
            self._cli.subscribe(topic, 1)
            _logger.info("dang ky them %s", topic)
        except Exception as exc:                                  # noqa: BLE001
            _logger.warning("khong dang ky duoc %s: %s", topic, exc)

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

        if kind == "status":
            online = bool(data.get("online"))
            self.stats["online"][serial] = online
            # Node tu khai co nhan duoc lenh qua MQTT khong.
            #
            # KHONG xoa loi khai nay khi node rot mang: biet nghe MQTT la
            # thuoc tinh cua FIRMWARE, con song hay chet la chuyen khac.
            # Gop hai thu lam mot thi luc node rot, manager tuong day la
            # firmware cu va xep lenh vao hang doi poll — ma firmware moi
            # khong bao gio poll nua.
            if online and data.get("cmd"):
                self.caps[serial] = True
            _logger.info("node %s: %s%s", serial,
                         "online" if online else "OFFLINE (Last Will)",
                         ", nhan lenh qua MQTT" if self.caps.get(serial) else "")
            self._log_event("up", topic, len(raw),
                            "đang chạy" if online else "ĐÃ TẮT (Last Will)")
            return

        if kind == "cmdack":
            cmd_id = data.get("id")
            if not isinstance(cmd_id, int):
                self.stats["bad"] += 1
                return
            self.stats["cmd_acked"] += 1
            # Dung cho tra loi ma /node/v1/commands/ack van dung: future cua
            # queue_command dang cho o day, khong co duong thu hai.
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

        # Lan dau thay mot serial: dang ky them chu de status CHINH XAC cua no.
        #
        # Vi sao phai lam the, thay vi tin vao 'fms/+/status' da dang ky o
        # _on_connect: kho retained cua RabbitMQ KHONG phuc vu dang ky co ky
        # tu dai dien. Do that 18/09 tren chinh broker nay:
        #     'fms/68EE8F4F06A8/status' -> nhan duoc {"online":true}
        #     'fms/+/status'            -> khong nhan gi
        # Dang ky dai dien van bat duoc thay doi SONG (Last Will, lan node
        # bao online), chi thieu anh chup retained luc minh vua khoi dong.
        # Nen: dai dien cho su kien song + chinh xac cho anh chup.
        if serial not in self.stats["by_serial"]:
            self._subscribe_status(serial)

        # Cho manager biet node nay con song, y het node_api.py lam o duong
        # HTTP — has_node_or_driver() va /api/command dua vao day.
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
            # Dau thoi gian vo ly -> bo di, de _on_value lay gio cua edge.
            #
            # Truoc day node hoc gio tu HAI nguon: SNTP va tra loi cua
            # /node/v1/hello. Cat HTTP la mat nguon thu hai, nen mot mang
            # nha may khong ra duoc pool.ntp.org se lam moi "ts" thanh nam
            # 1970 — va no chay thang vao Odoo neu khong chan o day.
            ts = it.get("ts")
            ts_s = (ts / 1000.0) if isinstance(ts, (int, float)) else None
            if ts_s is not None and ts_s < EPOCH_SANE_S:
                ts_s = None
                self.stats["ts_dropped"] += 1
            v = it.get("v")
            if len(preview) < 4:
                preview.append("%s=%s" % (ch, v))
            # Kenh den: giu rieng gia tri moi nhat cho trang /ops. Node chi
            # bao khi co ai ghi (bao-khi-doi), nen "ts" o day la luc DOI
            # gan nhat, khong phai luc do gan nhat.
            #
            # LUON cap nhat, KHONG phu thuoc mqtt_consumer_forward: /ops phai
            # phan anh dung trang thai vat ly ngay ca khi chua bat forward
            # vao Odoo (xem docstring dau ops_api.py: "trang nay phai xem
            # duoc dung luc Odoo hong") - xem python-test-writer 2026-09-24
            # (bat qua test_handle_measurement_tracks_relay_channel_as_lamp).
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

    # -- chieu xuong: day lenh toi node ----------------------------------
    def publish_command(self, serial: str, payload: dict) -> bool:
        """Day mot lenh xuong <goc>/<serial>/cmd. Tra False neu khong gui
        duoc — khi do manager quay ve hang doi poll cua duong HTTP.

        QoS 1, KHONG retain: mot lenh bat den gui luc node mat dien khong
        duoc phep tu bat len khi no song lai nua tieng sau. Broker vut di
        lenh gui cho mot node vang mat, dung nhu ta muon."""
        if not (self._cli and self._connected):
            return False
        if not self.caps.get(serial):
            return False        # node chua khai la nhan duoc lenh qua MQTT
        if not self.stats["online"].get(serial):
            # Chu de lenh khong retain va node khong dung phien ben, nen goi
            # gui cho mot node vang mat bi broker vut di. Tra False de ben
            # goi bao loi that, thay vi bao "da gui" roi im lang.
            return False
        topic = _cmd_topic(serial)
        if topic is None:
            return False
        try:
            info = self._cli.publish(topic, json.dumps(payload), qos=1)
        except Exception as exc:                                  # noqa: BLE001
            _logger.warning("khong day duoc lenh toi %s: %s", topic, exc)
            return False
        if info.rc == mqtt.MQTT_ERR_NO_CONN:
            # KHONG chac chan la chua gui - day la 1 khe hep giua self._connected
            # (co do tre qua call_soon_threadsafe) va socket that cua paho: neu
            # socket vua rot dung luc goi publish(), paho._send_publish() tra ve
            # NO_CONN nhung (da doc source paho-mqtt that, khong doan) van GIU
            # message trong self._out_messages (state=mqtt_ms_publish) va TU
            # DONG republish khi reconnect thanh cong - khac han cac rc loi khac
            # (that su khong gui duoc). Tra True o day de manager.queue_command()
            # tiep tuc cho ACK that (co the toi tu paho tu gui lai sau) thay vi
            # bao "chac chan that bai" ngay - neu ACK khong toi kip timeout, vong
            # do da tu tra "status":"unknown" (khop dung y nghia "co the da chay
            # nhung chua xac nhan duoc") - xem review 2026-09-24 (finding tu vong
            # phoi hop queue-command-unknown-timeout).
            #
            # Dem rieng "cmd_sent_no_conn", KHONG dem chung vao "cmd_sent" -
            # review 2026-09-24 (fix-mqtt-no-conn-race) chi ra gop chung se lam
            # "Lenh gui" o /ops trong nhu da gui het du mot phan dang cho paho
            # gui lai, de gay hieu lam luc mang site chap chon.
            _logger.info("day lenh toi %s: NO_CONN, paho se tu gui lai khi "
                        "reconnect - coi nhu da giao, cho ACK that", topic)
            self.stats["cmd_sent_no_conn"] += 1
            self._log_event("down", topic, len(json.dumps(payload)),
                            "lệnh #%s %s %s=%s (cho gui lai, mat ket noi tam thoi)" %
                            (payload.get("id"), payload.get("cmd"),
                             payload.get("channel"), payload.get("value")))
            return True
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            _logger.warning("day lenh toi %s that bai rc=%s", topic, info.rc)
            return False
        self.stats["cmd_sent"] += 1
        self._log_event("down", topic, len(json.dumps(payload)),
                        "lệnh #%s %s %s=%s" % (payload.get("id"), payload.get("cmd"),
                                               payload.get("channel"), payload.get("value")))
        return True

    # -- nhip tim thay mat node ------------------------------------------
    async def _heartbeat_loop(self):
        """Bao Odoo rang node con song.

        Truoc kia chinh node POST /node/v1/heartbeat moi 30 giay. Cat HTTP
        la mat cai do, va thiet bi se chuyen sang "offline" ben Odoo trong
        khi so do van chay ve deu — mot cai den bao noi doi.

        Nguon su that moi la broker: "online" o day den tu chu de status
        (retained) va tu Last Will, tuc la broker TU bao khi node rot chu
        khong phai doi het gio mot bo dem. Vong nay chi dich dieu do sang
        tieng noi ma Odoo dang nghe."""
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
                _logger.warning("nhip tim that bai: %s", exc)


def _topic_sibling(last: str):
    """'fms/+/meas' -> 'fms/+/<last>'. Suy ra tu mau da cau hinh, de khong
    phai them mot bien .env moi cho tung chu de."""
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
