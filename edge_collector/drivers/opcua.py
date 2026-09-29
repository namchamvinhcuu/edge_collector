# -*- coding: utf-8 -*-
"""Nguồn OPC UA (PLC) — đăng ký thay đổi (subscribe) theo danh sách
pcm_source._as_config()['subscribe'] và ghi lệnh theo ['write'].

GIỚI HẠN: pcm.source.cert_id (chứng chỉ) không được đưa vào _as_config(), nên
security_mode='sign'/'sign_encrypt' ở đây chỉ log cảnh báo rồi thử kết nối
Anonymous/Basic256Sha256 không chứng chỉ — cần bổ sung API trả chứng chỉ về
edge (thêm endpoint /pcm/api/v1/edge/config, hoặc upload trực tiếp) trước khi
dùng "Sign & Encrypt" thật trong sản xuất.
"""
import asyncio
import logging

from .base import SourceDriver

_logger = logging.getLogger("edge.driver.opcua")


class OpcuaDriver(SourceDriver):
    kind = "opcua"

    def __init__(self, source_cfg, channels, on_reading):
        super().__init__(source_cfg, channels, on_reading)
        self._client = None
        self._sub = None
        self._handles = []
        self._node_to_ch = {}
        self._write_nodes = {}       # ch -> ua Node, cho lệnh command

    async def start(self) -> None:
        from asyncua import Client
        self._client = Client(url=self.cfg.get("endpoint") or "")
        auth = self.cfg.get("auth") or {}
        if auth.get("kind") == "userpass" and auth.get("username"):
            self._client.set_user(auth["username"])
            self._client.set_password(auth.get("password") or "")
        if (self.cfg.get("security") or "none") != "none":
            _logger.warning(
                "nguồn %s: security_mode=%s nhưng không có chứng chỉ trong config "
                "- đang thử kết nối không mã hóa đầy đủ", self.code, self.cfg.get("security"))
        await self._client.connect()
        self._mark_online()

        handler = _ChangeHandler(self)
        self._sub = await self._client.create_subscription(200, handler)
        for row in self.cfg.get("subscribe") or []:
            try:
                node = self._client.get_node(row["node"])
                handle = await self._sub.subscribe_data_change(node)
                self._handles.append(handle)
                self._node_to_ch[node.nodeid.to_string()] = row
            except Exception as exc:                               # noqa: BLE001
                _logger.warning("không subscribe được %s: %s", row.get("node"), exc)
        for row in self.cfg.get("write") or []:
            try:
                self._write_nodes[row["ch"]] = self._client.get_node(row["node"])
            except Exception as exc:                                # noqa: BLE001
                _logger.warning("không resolve được write-node %s: %s", row.get("node"), exc)

    async def stop(self) -> None:
        if self._sub:
            try:
                await self._sub.delete()
            except Exception:                                       # noqa: BLE001
                pass
            self._sub = None
        if self._client:
            try:
                await self._client.disconnect()
            except Exception:                                       # noqa: BLE001
                pass
            self._client = None

    def _on_change(self, node, val, data):
        row = self._node_to_ch.get(node.nodeid.to_string())
        if not row:
            return
        ch = row["ch"]
        try:
            v = float(val) * (row.get("scale") or 1) + (row.get("offset") or 0)
        except (TypeError, ValueError):
            self._emit(ch, s=str(val), q=0, stable=True)
            return
        self._emit(ch, v=v, q=0, stable=True)

    async def command(self, channel_code: str, cmd: str, value=None) -> dict:
        node = self._write_nodes.get(channel_code)
        if not node:
            return {"ok": False, "error": "không có write-node cho kênh %s" % channel_code}
        try:
            from asyncua import ua
            variant = ua.Variant(value, ua.VariantType.Double if isinstance(value, float)
                                  else ua.VariantType.Boolean if isinstance(value, bool)
                                  else ua.VariantType.Int32)
            await node.write_value(ua.DataValue(variant))
            return {"ok": True, "status": "ok"}
        except Exception as exc:                                    # noqa: BLE001
            return {"ok": False, "error": str(exc)[:200]}

    async def browse(self, node_id=None, path=None) -> dict:
        try:
            node = self._client.get_node(node_id) if node_id else self._client.get_objects_node()
            children = await node.get_children()
            nodes = []
            for c in children:
                try:
                    name = (await c.read_browse_name()).Name
                    ntype = await c.read_node_class()
                except Exception:                                   # noqa: BLE001
                    name, ntype = str(c.nodeid), None
                nodes.append({"id": c.nodeid.to_string(), "name": name,
                              "children": ntype and ntype.name == "Object"})
            return {"ok": True, "nodes": nodes}
        except Exception as exc:                                    # noqa: BLE001
            return {"ok": False, "error": str(exc)[:200]}


class _ChangeHandler:
    def __init__(self, driver: OpcuaDriver):
        self.driver = driver

    def datachange_notification(self, node, val, data):
        self.driver._on_change(node, val, data)
