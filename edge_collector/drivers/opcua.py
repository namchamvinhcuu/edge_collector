# -*- coding: utf-8 -*-
"""Nguon OPC UA (PLC) — dang ky thay doi (subscribe) theo danh sach
pcm_source._as_config()['subscribe'] va ghi lenh theo ['write'].

GIOI HAN: pcm.source.cert_id (chung chi) khong duoc dua vao _as_config(), nen
security_mode='sign'/'sign_encrypt' o day chi log canh bao roi thu ket noi
Anonymous/Basic256Sha256 khong chung chi — can bo sung API tra chung chi ve
edge (them endpoint /pcm/api/v1/edge/config, hoac upload truc tiep) truoc khi
dung "Sign & Encrypt" that trong san xuat.
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
        self._write_nodes = {}       # ch -> ua Node, cho lenh command

    async def start(self) -> None:
        from asyncua import Client
        self._client = Client(url=self.cfg.get("endpoint") or "")
        auth = self.cfg.get("auth") or {}
        if auth.get("kind") == "userpass" and auth.get("username"):
            self._client.set_user(auth["username"])
            self._client.set_password(auth.get("password") or "")
        if (self.cfg.get("security") or "none") != "none":
            _logger.warning(
                "nguon %s: security_mode=%s nhung khong co chung chi trong config "
                "- dang thu ket noi khong ma hoa day du", self.code, self.cfg.get("security"))
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
                _logger.warning("khong subscribe duoc %s: %s", row.get("node"), exc)
        for row in self.cfg.get("write") or []:
            try:
                self._write_nodes[row["ch"]] = self._client.get_node(row["node"])
            except Exception as exc:                                # noqa: BLE001
                _logger.warning("khong resolve duoc write-node %s: %s", row.get("node"), exc)

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
            return {"ok": False, "error": "khong co write-node cho kenh %s" % channel_code}
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
