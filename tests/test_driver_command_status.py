# -*- coding: utf-8 -*-
"""Test pcm-edge-hardening: kết quả ghi xuống thiết bị (ModbusDriver/
OpcuaDriver.command) phân biệt 3 trạng thái:
  - "ok"      : thiết bị xác nhận đã ghi.
  - "error"   : CHẮC CHẮN không ghi (PLC trả exception response - pymodbus
                KHÔNG raise, phải tự kiểm rr.isError()).
  - "unknown" : frame đã gửi nhưng không có phản hồi (ModbusIOException của
                pymodbus / TimeoutError của asyncua) - thiết bị CÓ THỂ đã ghi.

KHÔNG chạm thiết bị thật: client Modbus/node OPC UA là object giả; response
lỗi dùng ExceptionResponse THẬT của pymodbus 3.15.
"""
import asyncio
import types

import pytest
from asyncua.client.ua_client import UASocketState
from pymodbus.exceptions import ConnectionException, ModbusIOException
from pymodbus.pdu import ExceptionResponse

from edge_collector.drivers.modbus import ModbusDriver
from edge_collector.drivers.opcua import OpcuaDriver

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _OkResponse:
    def isError(self):
        return False


class _FakeModbusClient:
    def __init__(self, result=None, exc=None):
        self.result = result if result is not None else _OkResponse()
        self.exc = exc
        self.calls = []

    async def _do(self, name, addr, value, device_id):
        self.calls.append((name, addr, value, device_id))
        if self.exc:
            raise self.exc
        return self.result

    async def write_register(self, addr, value, device_id=None):
        return await self._do("write_register", addr, value, device_id)

    async def write_registers(self, addr, values, device_id=None):
        return await self._do("write_registers", addr, values, device_id)


def _modbus(client, dtype="u16"):
    cfg = {"code": "SRC1", "kind": "modbus_tcp", "unit_id": 1,
           "points": [{"ch": "speed", "reg": "HR40001", "dtype": dtype, "scale": 1,
                       "offset": 0, "write": True}]}
    d = ModbusDriver(cfg, channels=[], on_reading=lambda *a, **kw: None)
    d._client = client
    return d


@pytest.mark.parametrize("dtype,method", [("u16", "write_register"), ("i16", "write_register"),
                                          ("i32", "write_registers"), ("f32", "write_registers")])
async def test_modbus_command_ok_when_device_confirms(dtype, method):
    client = _FakeModbusClient()

    res = await _modbus(client, dtype).command("speed", "write", 12)

    assert res == {"ok": True, "status": "ok"}
    assert client.calls[0][0] == method


@pytest.mark.parametrize("dtype", ["u16", "i16", "i32", "u32", "f32"])
async def test_modbus_exception_response_is_error_not_ok(dtype):
    """PLC trả exception response (ILLEGAL DATA ADDRESS...) -> pymodbus trả về
    object chứ KHÔNG raise: trước fix báo ok=True sai."""
    client = _FakeModbusClient(result=ExceptionResponse(6, 2))
    assert client.result.isError() is True

    res = await _modbus(client, dtype).command("speed", "write", 12)

    assert res["ok"] is False
    assert res["status"] == "error"
    assert "từ chối" in res["error"]


@pytest.mark.parametrize("dtype", ["u16", "i32"])
async def test_modbus_io_exception_no_response_is_unknown(dtype):
    """pymodbus raise ModbusIOException khi frame đã gửi mà không phản hồi -> PLC
    có thể đã ghi -> 'unknown', không phải 'error'."""
    client = _FakeModbusClient(exc=ModbusIOException("No response received after 3 retries"))

    res = await _modbus(client, dtype).command("speed", "write", 12)

    assert res["ok"] is False
    assert res["status"] == "unknown"
    assert "có thể đã ghi" in res["error"]
    assert len(client.calls) == 1


async def test_modbus_io_exception_error_message_is_truncated():
    client = _FakeModbusClient(exc=ModbusIOException("z" * 1000))

    res = await _modbus(client).command("speed", "write", 1)

    assert res["status"] == "unknown"
    assert len(res["error"]) < 300


@pytest.mark.parametrize("exc", [ConnectionException("mất kết nối"), RuntimeError("boom"),
                                 OSError("io")], ids=["connection", "runtime", "os"])
async def test_modbus_other_exception_keeps_old_error_shape(exc):
    """ConnectionException là ModbusException nhưng KHÔNG phải ModbusIOException
    -> không được rơi vào 'unknown'; giữ dạng cũ {ok:false,error} (không status)."""
    res = await _modbus(_FakeModbusClient(exc=exc)).command("speed", "write", 1)

    assert res["ok"] is False
    assert res.get("status") != "unknown"
    assert "status" not in res
    assert res["error"]


async def test_modbus_non_write_cmd_rejected_before_touching_client():
    client = _FakeModbusClient()

    res = await _modbus(client).command("speed", "zero", None)

    assert res["ok"] is False
    assert client.calls == []


# --- OPC UA ---------------------------------------------------------------


_SENTINEL = object()


class _FakeNode:
    def __init__(self, exc=None):
        self.exc = exc
        self.writes = []

    async def write_value(self, dv):
        self.writes.append(dv)
        if self.exc:
            raise self.exc


class _FakeUaClient:
    """Giả asyncua Client tối thiểu cho _socket_open(): .uaclient.protocol.state
    dùng enum THẬT UASocketState. protocol=None mô phỏng chưa connect."""

    def __init__(self, state=UASocketState.OPEN, no_protocol=False):
        proto = None if no_protocol else types.SimpleNamespace(state=state)
        self.uaclient = types.SimpleNamespace(protocol=proto)


def _opcua(node, client=_SENTINEL):
    d = OpcuaDriver({"code": "UA1", "kind": "opcua", "points": []}, channels=[],
                    on_reading=lambda *a, **kw: None)
    d._write_nodes = {"sp": node}
    d._client = _FakeUaClient() if client is _SENTINEL else client
    return d


async def test_opcua_command_ok():
    node = _FakeNode()

    res = await _opcua(node).command("sp", "write", 3.5)

    assert res == {"ok": True, "status": "ok"}
    assert len(node.writes) == 1


@pytest.mark.parametrize("exc", [TimeoutError(), asyncio.TimeoutError()],
                         ids=["builtin", "asyncio"])
async def test_opcua_timeout_is_unknown(exc):
    res = await _opcua(_FakeNode(exc=exc)).command("sp", "write", 1)

    assert res["ok"] is False
    assert res["status"] == "unknown"
    assert "có thể đã ghi" in res["error"]


async def test_opcua_other_exception_is_not_unknown():
    res = await _opcua(_FakeNode(exc=RuntimeError("BadNodeIdUnknown"))).command("sp", "write", 1)

    assert res["ok"] is False
    assert res.get("status") != "unknown"
    assert "BadNodeIdUnknown" in res["error"]


async def test_opcua_unknown_channel_is_error_without_write():
    node = _FakeNode()

    res = await _opcua(node).command("khac", "write", 1)

    assert res["ok"] is False
    assert node.writes == []


# --- r2 regression: asyncua 2.0.1 GÓI TimeoutError ------------------------
# UaClient.send_request (asyncua/client/ua_client.py:214-220) bắt mọi lỗi không
# phải UaError rồi raise Exception("Unhandled exception while sending request
# to OPC UA server") from ex -> driver nhận Exception có __cause__=TimeoutError.


class _WrappingNode:
    """Mô phỏng đúng cách asyncua gói lỗi: raise Exception(...) from cause."""

    def __init__(self, cause):
        self.cause = cause

    async def write_value(self, dv):
        try:
            raise self.cause
        except Exception as ex:                                     # noqa: BLE001
            raise Exception("Unhandled exception while sending request to OPC UA server") from ex


@pytest.mark.parametrize("cause", [TimeoutError(), asyncio.TimeoutError()],
                         ids=["builtin", "asyncio"])
async def test_opcua_wrapped_timeout_from_asyncua_is_unknown(cause):
    res = await _opcua(_WrappingNode(cause)).command("sp", "write", 1)

    assert res["ok"] is False
    assert res["status"] == "unknown"
    assert "có thể đã ghi" in res["error"]


async def test_opcua_wrapped_non_timeout_cause_is_not_unknown():
    res = await _opcua(_WrappingNode(ValueError("bad variant"))).command("sp", "write", 1)

    assert res["ok"] is False
    assert res.get("status") != "unknown"
    assert "Unhandled exception" in res["error"]


@pytest.mark.parametrize("exc", [ConnectionError("refused"),
                                 ConnectionError("Connection is closed")],
                         ids=["direct", "asyncua-closed"])
async def test_opcua_connection_lost_after_socket_check_is_unknown(exc):
    """r3: đã qua kiểm socket mở mà rớt kết nối lúc ghi -> request có thể đã
    tới server -> 'unknown' (trước đây sai là 'error')."""
    res = await _opcua(_FakeNode(exc=exc)).command("sp", "write", 1)

    assert res["ok"] is False
    assert res["status"] == "unknown"
    assert "có thể đã ghi" in res["error"]


class _ClosedFromNoneNode:
    """Đúng dạng asyncua ua_client.py:217-218: ConnectionError(...) from None."""

    async def write_value(self, dv):
        try:
            raise TimeoutError()
        except Exception:                                           # noqa: BLE001
            raise ConnectionError("Connection is closed") from None


async def test_opcua_asyncua_connection_closed_from_none_is_unknown():
    res = await _opcua(_ClosedFromNoneNode()).command("sp", "write", 1)

    assert res["status"] == "unknown"


async def test_opcua_wrapped_connection_error_cause_is_unknown():
    res = await _opcua(_WrappingNode(ConnectionResetError("reset"))).command("sp", "write", 1)

    assert res["status"] == "unknown"


@pytest.mark.parametrize("client", [None, _FakeUaClient(no_protocol=True),
                                    _FakeUaClient(state=UASocketState.CLOSED),
                                    _FakeUaClient(state=UASocketState.INITIALIZED),
                                    types.SimpleNamespace()],
                         ids=["no-client", "no-protocol", "closed", "initialized",
                              "no-uaclient"])
async def test_opcua_socket_not_open_is_error_and_does_not_write(client):
    """Socket chưa mở -> CHẮC CHẮN chưa ghi -> 'error', không gọi write_value."""
    node = _FakeNode()

    res = await _opcua(node, client=client).command("sp", "write", 1)

    assert res["ok"] is False
    assert res["status"] == "error"
    assert node.writes == []
