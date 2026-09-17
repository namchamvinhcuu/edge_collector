# -*- coding: utf-8 -*-
"""Nguon MQTT — broker mo (pcm_source.py: 'mqtt_in 이 학습한 토픽/키', nghia la
kenh KHONG khai bao truoc tung topic; edge tu suy ma kenh tu ten topic va de
Odoo tu tao channel inactive (Channel._upsert ben pcm_channel.py xu ly).

Neu payload JSON co san khoa 'ch' thi dung thang (khop voi kenh da khai bao
source_tag = topic day du). paho-mqtt chay callback tren thread rieng nen moi
notification duoc chuyen vao vong lap asyncio bang call_soon_threadsafe.
"""
import asyncio
import json
import logging

import paho.mqtt.client as mqtt

from .base import SourceDriver

_logger = logging.getLogger("edge.driver.mqtt")


class MqttDriver(SourceDriver):
    kind = "mqtt"

    def __init__(self, source_cfg, channels, on_reading):
        super().__init__(source_cfg, channels, on_reading)
        self._cli = None
        self._loop = None
        self._by_tag = {c.get("tag"): c for c in channels if c.get("tag")}

    async def start(self) -> None:
        self._loop = asyncio.get_event_loop()
        auth = self.cfg.get("auth") or {}
        host, _, port = (self.cfg.get("endpoint") or "").replace("mqtt://", "").partition(":")
        self._cli = mqtt.Client()
        if auth.get("username"):
            self._cli.username_pw_set(auth["username"], auth.get("password") or "")
        self._cli.on_connect = self._on_connect
        self._cli.on_message = self._on_message
        self._cli.on_disconnect = self._on_disconnect
        self._cli.connect_async(host or "127.0.0.1", int(port or 1883), keepalive=30)
        self._cli.loop_start()

    async def stop(self) -> None:
        if self._cli:
            self._cli.loop_stop()
            self._cli.disconnect()
            self._cli = None

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            client.subscribe((self.cfg.get("topic_base") or "factory") + "/#")
            self._loop.call_soon_threadsafe(self._mark_online)
        else:
            self._loop.call_soon_threadsafe(self._mark_error, "mqtt connect rc=%s" % rc)

    def _on_disconnect(self, client, userdata, rc):
        if rc != 0:
            self._loop.call_soon_threadsafe(self._mark_error, "mqtt disconnected rc=%s" % rc)

    def _on_message(self, client, userdata, msg):
        self._loop.call_soon_threadsafe(self._handle, msg.topic, msg.payload)

    def _handle(self, topic: str, raw: bytes):
        text = raw.decode(errors="replace")
        data = None
        try:
            data = json.loads(text)
        except ValueError:
            pass
        if isinstance(data, dict) and data.get("ch"):
            self._emit(data["ch"], v=data.get("v"), s=data.get("s"),
                       q=int(data.get("q") or 0), stable=data.get("stable"))
            return
        ch_row = self._by_tag.get(topic)
        base = self.cfg.get("topic_base") or "factory"
        code = ch_row["code"] if ch_row else topic[len(base):].strip("/").replace("/", "_") or topic
        try:
            self._emit(code, v=float(text), q=0, stable=True)
        except ValueError:
            self._emit(code, s=text, q=0, stable=True)

    async def command(self, channel_code: str, cmd: str, value=None) -> dict:
        if not self._cli:
            return {"ok": False, "error": "mqtt chua ket noi"}
        base = self.cfg.get("topic_base") or "factory"
        payload = value if value is not None else cmd
        self._cli.publish("%s/cmd/%s" % (base, channel_code), str(payload))
        return {"ok": True, "status": "ok"}
