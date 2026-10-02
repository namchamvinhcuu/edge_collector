# -*- coding: utf-8 -*-
"""Test task pcm-edge-hmac (2026-10-02): chiều Odoo -> edge ký HMAC bằng
downlink_key riêng (Odoo gọi edge qua Internet bằng http, không gửi khóa trần).

Phạm vi:
  1. inbound_api._require_downlink_auth (dependency của router): mọi nhánh từ
     chối (code/ts/sig thiếu-sai, chưa có khóa, ts không phải int, stale, sig
     sai do khóa/body/path/method/query, replay) + happy path; bù clock_offset_s.
     X-Edge-Nonce (thêm sau finding 🟠 reviewer): thiếu / >64 ký tự / bị sửa;
     2 lệnh hợp lệ giống hệt nhau trong cùng giây khác nonce đều qua.
     Cửa sổ lệch giờ 30s.
  2. _remember_sig: hết hạn 120s, giới hạn 10000 mục.
  6. TEST VECTOR chung đã chốt với session Odoo (2 bên phải ra cùng chữ ký).
  3. Ngoại lệ /api/latest, /api/stats (tablet không ký được).
  4. Route mới thêm vào router tự bị bảo vệ.
  5. odoo_client.pull_config: lưu downlink_key + clock_offset_s (httpx MockTransport).

Chữ ký tính ĐỘC LẬP trong tests/downlink_signing.py theo contract đã chốt -
KHÔNG import hàm ký của inbound_api (nếu không test sẽ là tautology).
KHÔNG chạm mạng/thiết bị thật: manager/driver/store đều giả.
"""
import asyncio
import email.utils
import hashlib
import hmac
import json
import logging
import time
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

import edge_collector.inbound_api as inbound_api
from edge_collector.config import settings
from edge_collector.odoo_client import OdooClient

from downlink_signing import DOWNLINK_KEY, DownlinkSigner, KeyStore, sign

NOW = 1_790_000_000  # giờ edge cố định cho test lệch giờ


class _Manager:
    def __init__(self):
        self.queue_calls = []

    def driver_for_channel(self, serial, ch):
        return None

    def known_node_serials(self):
        return ["NODE1"]

    async def queue_command(self, serial, ch, cmd, value, extra=None, request_id=None):
        self.queue_calls.append((serial, ch, cmd, value))
        return {"ok": True, "status": "ok", "error": None}


class _Store(KeyStore):
    def __init__(self, key=DOWNLINK_KEY):
        super().__init__(key)
        self.latest_calls = []

    def history_latest(self, serial, ch):
        self.latest_calls.append((serial, ch))
        return None

    def history_stats(self, serial, ch, since_ts):
        return {"samples": 0}


def _app(store="default", agent=None):
    app = FastAPI()
    app.include_router(inbound_api.router)
    app.state.manager = _Manager()
    app.state.store = _Store() if store == "default" else store
    if agent is not None:
        app.state.agent = agent
    return app


def _client(app, signer=None, code=True):
    headers = {"X-Edge-Code": settings.edge_code} if code is True else (code or {})
    c = TestClient(app, headers=headers)
    if signer is not None:
        c.auth = signer
    return c


CMD = {"serial": "NODE1", "channel": "relay_red", "cmd": "on", "request_id": "r1"}


def _assert_rejected(resp, reason_part):
    assert resp.status_code == 401
    detail = resp.json()["detail"]
    assert detail["ok"] is False and detail["status"] == "rejected"
    assert detail["error"].startswith("xác thực thất bại: ")
    assert reason_part in detail["error"], detail["error"]


@pytest.fixture()
def frozen_time(monkeypatch):
    """Cố định time.time() để kiểm biên ±60s không phụ thuộc giờ chạy test."""
    monkeypatch.setattr(inbound_api.time, "time", lambda: float(NOW))
    return NOW


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_signed_post_command_with_body_reaches_manager():
    app = _app()
    client = _client(app, DownlinkSigner())

    resp = client.post("/api/command", json=CMD)

    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    assert app.state.manager.queue_calls == [("NODE1", "relay_red", "on", None)]


def test_signed_get_with_query_needing_percent_encoding_is_accepted():
    """Query chứa dấu cách / '&' / '/' / tiếng Việt - server ký trên query THÔ
    (percent-encoded) đúng như bytes đi trên dây."""
    app = _app()
    client = _client(app, DownlinkSigner())

    resp = client.get("/api/latest", params={"serial": "NODE 1&x", "ch": "nhiệt độ/1"})

    assert resp.status_code == 200
    assert app.state.store.latest_calls == [("NODE 1&x", "nhiệt độ/1")]


def test_signature_over_decoded_query_is_rejected():
    """Chốt contract: query = request.url.query THÔ. Odoo ký trên dạng đã
    decode -> chữ ký không khớp (bắt lệch contract giữa 2 bên)."""
    app = _app()
    client = _client(app, DownlinkSigner(sign_query="serial=NODE 1&ch=t1"))

    resp = client.get("/api/latest", params={"serial": "NODE 1", "ch": "t1"})

    _assert_rejected(resp, "X-Edge-Sig sai")


def test_signed_post_with_empty_body_uses_sha256_of_empty_bytes():
    app = _app()
    client = _client(app, DownlinkSigner())

    resp = client.post("/api/command", content=b"", headers={"Content-Type": "application/json"})

    # Qua cổng xác thực (không 401) -> api_command trả lỗi JSON bình thường.
    assert resp.status_code == 200
    assert resp.json()["status"] == "error"


# ---------------------------------------------------------------------------
# TEST VECTOR chung với Odoo (pcm-edge-hmac) - KHÔNG sửa giá trị nếu không
# đổi đồng thời phía Odoo.
# ---------------------------------------------------------------------------

VEC_KEY = "test-downlink-key-0123456789abcdefABCDEF"  # secret-allow: test vector công khai
VEC_TS = "1790000000"
VEC_NONCE = "0123456789abcdef"
VEC_POST_BODY = json.dumps({"serial": "SN1", "channel": "relay1", "cmd": "write",
                            "value": 1, "request_id": "abcd1234"}).encode()
VEC_POST_SIG = "4486f58133f2881376fbbc640490fdc5f3e481e729bca00ec3fb973f29e5e8bb"
VEC_GET_QUERY = "serial=SN+1&ch=temp1"
VEC_GET_SIG = "92d34ba939a71f03508eff113fa3f276768aae22ca36f83000debe91ff685e12"
_VECTORS = [
    ("POST", "/api/command", "", VEC_POST_BODY, VEC_POST_SIG),
    ("GET", "/api/latest", VEC_GET_QUERY, b"", VEC_GET_SIG),
]


def _scope_request(method, path, query):
    return Request({"type": "http", "method": method, "path": path, "root_path": "",
                    "query_string": query.encode(), "headers": [], "scheme": "http",
                    "server": ("edge.local", 8080)})


@pytest.mark.parametrize("method,path,query,body,expected", _VECTORS, ids=["post", "get"])
def test_shared_vector_string_to_sign_matches_odoo(method, path, query, body, expected):
    req = _scope_request(method, path, query)

    msg = inbound_api._downlink_string_to_sign(req, VEC_TS, VEC_NONCE, body)
    sig = hmac.new(VEC_KEY.encode(), msg, hashlib.sha256).hexdigest()

    assert sig == expected


@pytest.mark.parametrize("method,path,query,body,expected", _VECTORS, ids=["post", "get"])
def test_shared_vector_test_helper_matches_odoo(method, path, query, body, expected):
    """Helper ký của test (tests/downlink_signing.py) cũng ra đúng vector -
    các test HTTP khác dùng helper này nên đều đang ký theo contract Odoo."""
    assert sign(method, path, query, VEC_TS, VEC_NONCE, body, key=VEC_KEY) == expected


@pytest.mark.parametrize("method,path,query,body,expected", _VECTORS, ids=["post", "get"])
def test_shared_vector_accepted_end_to_end_over_http(monkeypatch, method, path, query, body,
                                                      expected):
    """Gửi đúng bytes của vector qua HTTP (giờ edge = VEC_TS) -> 200."""
    monkeypatch.setattr(inbound_api.time, "time", lambda: float(VEC_TS))
    app = _app(store=_Store(key=VEC_KEY))
    app.state.manager = _Manager()
    app.state.manager.known_node_serials = lambda: ["SN1"]
    client = _client(app)
    h = {"X-Edge-Ts": VEC_TS, "X-Edge-Nonce": VEC_NONCE, "X-Edge-Sig": expected}

    if method == "POST":
        resp = client.post(path, content=body, headers=dict(h, **{"Content-Type": "application/json"}))
    else:
        resp = client.get(path + "?" + query, headers=h)

    assert resp.status_code == 200, resp.text
    if method == "GET":
        assert app.state.store.latest_calls == [("SN 1", "temp1")]
    else:
        assert app.state.manager.queue_calls == [("SN1", "relay1", "write", 1)]


# ---------------------------------------------------------------------------
# Chữ ký sai: khóa / body / path / method
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("signer_kw", [
    {"key": "khoa-sai"},
    {"sign_body": json.dumps(dict(CMD, cmd="off")).encode()},
    {"sign_path": "/api/browse"},
    {"sign_method": "PUT"},
    {"sign_query": "serial=NODE1"},
], ids=["wrong-key", "body-tampered", "other-path", "other-method", "extra-query"])
def test_wrong_signature_is_rejected_before_touching_manager(signer_kw):
    app = _app()
    client = _client(app, DownlinkSigner(**signer_kw))
    inbound_api._recent_requests.clear()

    resp = client.post("/api/command", json=CMD)

    _assert_rejected(resp, "X-Edge-Sig sai")
    assert app.state.manager.queue_calls == []
    assert inbound_api.recent_requests() == []


def test_sig_compare_is_full_not_prefix():
    app = _app()
    client = _client(app)
    ts = str(int(time.time()))
    body = json.dumps(CMD).encode()
    good = sign("POST", "/api/command", "", ts, "n1", body)

    resp = client.post("/api/command", content=body, headers={
        "Content-Type": "application/json", "X-Edge-Ts": ts, "X-Edge-Nonce": "n1",
        "X-Edge-Sig": good[:-1]})

    _assert_rejected(resp, "X-Edge-Sig sai")


# ---------------------------------------------------------------------------
# Thiếu/sai header, chưa có khóa, ts hỏng
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("code", [{}, {"X-Edge-Code": "WRONG"}, {"X-Edge-Code": ""}],
                         ids=["missing", "wrong", "empty"])
def test_valid_sig_but_bad_edge_code_is_rejected(code):
    app = _app()
    client = _client(app, DownlinkSigner(), code=code)

    resp = client.post("/api/command", json=CMD)

    _assert_rejected(resp, "X-Edge-Code")
    assert app.state.manager.queue_calls == []


@pytest.mark.parametrize("drop", ["X-Edge-Ts", "X-Edge-Sig", "X-Edge-Nonce", "empty-nonce",
                                  "all"])
def test_missing_ts_nonce_or_sig_on_write_route_is_rejected(drop):
    app = _app()
    client = _client(app)
    ts = str(int(time.time()))
    body = json.dumps(CMD).encode()
    h = {"Content-Type": "application/json", "X-Edge-Ts": ts, "X-Edge-Nonce": "n1",
         "X-Edge-Sig": sign("POST", "/api/command", "", ts, "n1", body)}
    if drop == "empty-nonce":
        h["X-Edge-Nonce"] = ""
    else:
        for k in (["X-Edge-Ts", "X-Edge-Sig", "X-Edge-Nonce"] if drop == "all" else [drop]):
            del h[k]

    resp = client.post("/api/command", content=body, headers=h)

    _assert_rejected(resp, "thiếu X-Edge-Ts/X-Edge-Nonce/X-Edge-Sig")
    assert app.state.manager.queue_calls == []


@pytest.mark.parametrize("store", [_Store(key=None), _Store(key=""), None],
                         ids=["no-kv", "empty-kv", "no-store"])
def test_edge_without_downlink_key_rejects_everything(store):
    app = _app(store=store)
    client = _client(app, DownlinkSigner())

    resp = client.post("/api/command", json=CMD)

    _assert_rejected(resp, "chưa có downlink_key")
    assert app.state.manager.queue_calls == []


@pytest.mark.parametrize("ts", ["abc", "1790000000.5", " "], ids=["text", "float", "blank"])
def test_non_integer_ts_is_rejected(ts):
    app = _app()
    client = _client(app, DownlinkSigner(ts=ts))

    resp = client.post("/api/command", json=CMD)

    _assert_rejected(resp, "không phải số nguyên")


# ---------------------------------------------------------------------------
# Lệch giờ + bù clock_offset_s
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("delta,ok", [(-31, False), (-30, True), (-29, True), (0, True),
                                      (29, True), (30, True), (31, False)])
def test_ts_skew_window_is_30_seconds(frozen_time, delta, ok):
    app = _app()
    client = _client(app, DownlinkSigner(ts=frozen_time + delta))

    resp = client.post("/api/command", json=CMD)

    if ok:
        assert resp.status_code == 200 and resp.json()["ok"] is True
    else:
        _assert_rejected(resp, "stale")
        assert app.state.manager.queue_calls == []


def test_clock_offset_from_odoo_is_applied(frozen_time):
    """Edge chậm 1 giờ so với Odoo (offset=+3600): ts theo giờ Odoo phải qua,
    ts theo giờ edge (không bù) phải bị coi là stale."""
    agent = SimpleNamespace(odoo=SimpleNamespace(clock_offset_s=3600.0))
    app = _app(agent=agent)

    ok = _client(app, DownlinkSigner(ts=frozen_time + 3600 + 10)).post("/api/command", json=CMD)
    stale = _client(app, DownlinkSigner(ts=frozen_time)).post("/api/command", json=CMD)

    assert ok.status_code == 200
    _assert_rejected(stale, "stale")


def test_without_agent_offset_defaults_to_zero(frozen_time):
    app = _app()  # không có app.state.agent

    resp = _client(app, DownlinkSigner(ts=frozen_time + 3600)).post("/api/command", json=CMD)

    _assert_rejected(resp, "stale")


@pytest.mark.parametrize("nonce,ok", [("n" * 64, True), ("n" * 65, False), ("x", True)],
                         ids=["64", "65", "1"])
def test_nonce_length_limit_64(nonce, ok):
    app = _app()

    resp = _client(app, DownlinkSigner(nonce=nonce)).post("/api/command", json=CMD)

    if ok:
        assert resp.status_code == 200
    else:
        _assert_rejected(resp, "X-Edge-Nonce quá dài")
        assert app.state.manager.queue_calls == []


def test_nonce_tampered_after_signing_is_rejected():
    app = _app()

    resp = _client(app, DownlinkSigner(nonce="nonce-gui-di", sign_nonce="nonce-da-ky")) \
        .post("/api/command", json=CMD)

    _assert_rejected(resp, "X-Edge-Sig sai")
    assert app.state.manager.queue_calls == []


# ---------------------------------------------------------------------------
# Replay
# ---------------------------------------------------------------------------


def test_identical_commands_same_second_different_nonce_both_pass_regression():
    """Regression (finding 🟠 reviewer pcm-edge-hmac): trước khi có nonce, 2
    lệnh hợp lệ giống hệt nhau (cùng ts giây/body, vd bấm 'bật' 2 lần) ra cùng
    chữ ký -> lần 2 bị 401 'replay' oan. Khác nonce -> cả 2 phải qua."""
    app = _app()
    ts = int(time.time())

    first = _client(app, DownlinkSigner(ts=ts, nonce="a1")).post("/api/command", json=CMD)
    second = _client(app, DownlinkSigner(ts=ts, nonce="b2")).post("/api/command", json=CMD)

    assert first.status_code == 200 and second.status_code == 200
    assert len(app.state.manager.queue_calls) == 2


def test_replayed_identical_request_is_rejected():
    """Cùng ts + cùng nonce + cùng body = gói phát lại -> lần 2 'replay'."""
    app = _app()
    signer = DownlinkSigner(ts=int(time.time()), nonce="same-nonce")
    client = _client(app, signer)

    first = client.post("/api/command", json=CMD)
    second = client.post("/api/command", json=CMD)

    assert first.status_code == 200
    _assert_rejected(second, "replay")
    assert len(app.state.manager.queue_calls) == 1


def test_invalid_signature_is_not_remembered_as_seen():
    """Chữ ký sai không được ghi vào _seen_sigs (nếu không kẻ tấn công lấp đầy
    bộ nhớ chống replay bằng rác)."""
    app = _app()

    _client(app, DownlinkSigner(key="sai")).post("/api/command", json=CMD)

    assert inbound_api._seen_sigs == {}


def test_remember_sig_expires_after_120s(monkeypatch):
    """TTL = 4 x cửa sổ lệch giờ 30s = 120s (🟡 reviewer: phủ cả lệch 2 chiều
    + offset đồng hồ). 119s vẫn replay, 121s hết hạn."""
    clock = [1000.0]
    monkeypatch.setattr(inbound_api.time, "monotonic", lambda: clock[0])

    assert inbound_api._remember_sig("s1") is True
    clock[0] = 1000.0 + 59.9
    assert inbound_api._remember_sig("s1") is False
    clock[0] = 1000.0 + 119.0
    assert inbound_api._remember_sig("s1") is False
    clock[0] = 1000.0 + 121.0
    assert inbound_api._remember_sig("s1") is True  # hết hạn -> dọn, nhận lại


def test_remember_sig_purges_expired_entries(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(inbound_api.time, "monotonic", lambda: clock[0])
    inbound_api._seen_sigs.update({"old%d" % i: 999.0 for i in range(5)})

    inbound_api._remember_sig("new")

    assert list(inbound_api._seen_sigs) == ["new"]


def test_remember_sig_caps_at_10000_evicting_oldest(monkeypatch):
    monkeypatch.setattr(inbound_api.time, "monotonic", lambda: 1000.0)
    inbound_api._seen_sigs.update({"s%d" % i: 5000.0 for i in range(10000)})

    assert inbound_api._remember_sig("newest") is True

    assert len(inbound_api._seen_sigs) == 10000
    assert "s0" not in inbound_api._seen_sigs
    assert "s1" in inbound_api._seen_sigs and "newest" in inbound_api._seen_sigs


# ---------------------------------------------------------------------------
# Ngoại lệ /api/latest, /api/stats
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("url", ["/api/latest?serial=N&ch=t", "/api/stats?serial=N&ch=t"])
def test_tablet_get_without_any_header_is_allowed(url):
    app = _app()
    client = _client(app, code={})

    resp = client.get(url)

    assert resp.status_code == 200


@pytest.mark.parametrize("url", ["/api/latest?serial=N&ch=t", "/api/stats?serial=N&ch=t"])
def test_read_route_with_sig_present_is_fully_checked(url):
    app = _app()

    good = _client(app, DownlinkSigner()).get(url)
    bad = _client(app, DownlinkSigner(key="sai")).get(url)
    no_code = _client(app, DownlinkSigner(), code={}).get(url)

    assert good.status_code == 200
    _assert_rejected(bad, "X-Edge-Sig sai")
    _assert_rejected(no_code, "X-Edge-Code thiếu")


def test_read_route_with_sig_but_no_downlink_key_is_rejected():
    app = _app(store=_Store(key=None))

    resp = _client(app, DownlinkSigner()).get("/api/latest?serial=N&ch=t")

    _assert_rejected(resp, "chưa có downlink_key")


@pytest.mark.parametrize("url", ["/api/command", "/api/browse", "/api/source/test"])
def test_write_routes_without_sig_are_rejected_even_with_correct_code(url):
    """Ngoại lệ tablet KHÔNG lan sang đường ghi."""
    app = _app()

    resp = _client(app).post(url, json={"serial": "NODE1", "channel": "c"})

    _assert_rejected(resp, "thiếu X-Edge-Ts/X-Edge-Nonce/X-Edge-Sig")
    assert app.state.manager.queue_calls == []


# ---------------------------------------------------------------------------
# Route mới thêm vào router tự bị bảo vệ
# ---------------------------------------------------------------------------


@pytest.fixture()
def extra_routes():
    calls = []

    async def _new_write(request: Request):
        calls.append("write")
        return {"ok": True}

    async def _new_read():
        calls.append("read")
        return {"ok": True}

    before = len(inbound_api.router.routes)
    inbound_api.router.add_api_route("/api/new_write", _new_write, methods=["POST"])
    inbound_api.router.add_api_route("/api/new_read", _new_read, methods=["GET"])
    try:
        yield calls
    finally:
        del inbound_api.router.routes[before:]


def test_route_added_later_to_router_is_protected(extra_routes):
    app = _app()

    unsigned_post = _client(app).post("/api/new_write", json={})
    unsigned_get = _client(app).get("/api/new_read")
    signed_post = _client(app, DownlinkSigner()).post("/api/new_write", json={})

    _assert_rejected(unsigned_post, "thiếu X-Edge-Ts/X-Edge-Nonce/X-Edge-Sig")
    _assert_rejected(unsigned_get, "thiếu X-Edge-Ts/X-Edge-Nonce/X-Edge-Sig")  # không thuộc ngoại lệ tablet
    assert signed_post.status_code == 200
    assert extra_routes == ["write"]


# ---------------------------------------------------------------------------
# Không lộ khóa
# ---------------------------------------------------------------------------


def test_rejection_does_not_leak_key_in_response_or_log(caplog):
    app = _app()
    caplog.set_level(logging.DEBUG, logger="edge.inbound_api")

    resp = _client(app, DownlinkSigner(key="sai")).post("/api/command", json=CMD)

    assert resp.status_code == 401
    assert DOWNLINK_KEY not in resp.text
    assert DOWNLINK_KEY not in caplog.text
    assert "từ chối POST /api/command" in caplog.text


# ===========================================================================
# odoo_client.pull_config: downlink_key + clock_offset_s
# ===========================================================================


class _KvStore:
    """Bắt chước ĐÚNG ngữ nghĩa Store thật (store.py kv_get/kv_set): kv_set
    lưu str thô (khác str thì json.dumps), kv_get thử json.loads rồi mới trả
    thô. Nhờ vậy khóa trông như số ("1e5") đọc ra float như Store thật."""

    def __init__(self, **kv):
        self.raw = {k: (v if isinstance(v, str) else json.dumps(v)) for k, v in kv.items()}
        self.sets = []

    def kv_get(self, k, default=None):
        if k not in self.raw:
            return default
        try:
            return json.loads(self.raw[k])
        except ValueError:
            return self.raw[k]

    def kv_set(self, k, v):
        self.sets.append((k, v))
        self.raw[k] = v if isinstance(v, str) else json.dumps(v)


def _http_date(ts):
    return email.utils.formatdate(ts, usegmt=True)


def _pull(store, payload, status=200, headers=None, start_offset=0.0):
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(status, json=payload, headers=headers or {})

    async def run():
        oc = OdooClient(store)
        await oc._client.aclose()
        oc._client = httpx.AsyncClient(base_url="http://odoo.test",
                                       transport=httpx.MockTransport(handler))
        oc.clock_offset_s = start_offset
        try:
            res = await oc.pull_config()
        finally:
            await oc.aclose()
        return oc, res

    oc, res = asyncio.run(run())
    return oc, res, seen


@pytest.mark.parametrize("payload", [
    {"ok": True, "downlink_key": "k-top"},
    {"ok": True, "config": {"downlink_key": "k-top"}},
    {"ok": True, "downlink_key": "k-top", "config": {"downlink_key": "k-cfg"}},
], ids=["top-level", "in-config", "top-level-wins"])
def test_pull_config_stores_new_downlink_key(payload):
    store = _KvStore(config_rev=3)

    _, res, seen = _pull(store, payload)

    assert store.kv_get("downlink_key") == "k-top"
    assert [k for k, _ in store.sets] == ["downlink_key"]
    assert res["ok"] is True
    assert seen[0].url.path == "/pcm/api/v1/edge/config"
    assert json.loads(seen[0].content) == {"config_version": 3}


def test_pull_config_same_key_does_not_rewrite():
    store = _KvStore(downlink_key="k1")

    _pull(store, {"ok": True, "downlink_key": "k1"})

    assert store.sets == []


@pytest.mark.parametrize("payload", [
    {"ok": True},
    {"ok": True, "downlink_key": ""},
    {"ok": True, "downlink_key": None},
    {"ok": True, "downlink_key": 12345},
    {"ok": True, "config": {"downlink_key": ""}},
    {"ok": True, "config": "khong-phai-dict"},
    {"ok": True, "config": ["downlink_key"]},
    {"ok": False, "downlink_key": "k-new"},
], ids=["absent", "empty", "null", "int", "cfg-empty", "cfg-str", "cfg-list", "not-ok"])
def test_pull_config_keeps_old_key_when_missing_invalid_or_not_ok(payload):
    store = _KvStore(downlink_key="k-old")

    _, res, _ = _pull(store, payload)

    assert store.kv_get("downlink_key") == "k-old"
    assert store.sets == []
    assert isinstance(res, dict)


def test_pull_config_http_error_status_keeps_key():
    store = _KvStore(downlink_key="k-old")

    _, res, _ = _pull(store, {"downlink_key": "k-new"}, status=500)

    assert res["ok"] is False
    assert store.kv_get("downlink_key") == "k-old"


def test_pull_config_does_not_log_key_value(caplog):
    caplog.set_level(logging.DEBUG)
    store = _KvStore()

    _pull(store, {"ok": True, "downlink_key": "SIEU-BI-MAT-xyz"})

    assert store.kv_get("downlink_key") == "SIEU-BI-MAT-xyz"
    assert "SIEU-BI-MAT-xyz" not in caplog.text


def test_pull_config_sets_clock_offset_from_date_header(monkeypatch):
    import edge_collector.odoo_client as oc_mod
    monkeypatch.setattr(oc_mod.time, "time", lambda: 1_790_000_000.0)
    # Odoo nhanh hơn edge 100 giây (header Date dạng IMF-fixdate, GMT).
    date = _http_date(1_790_000_000 + 100)

    oc, _, _ = _pull(_KvStore(), {"ok": True}, headers={"Date": date})

    assert oc.clock_offset_s == pytest.approx(100.0)


@pytest.mark.parametrize("headers", [{}, {"Date": "rac-khong-phai-ngay"}, {"Date": ""}],
                         ids=["absent", "garbage", "empty"])
def test_pull_config_bad_or_missing_date_keeps_previous_offset(headers):
    oc, res, _ = _pull(_KvStore(), {"ok": True}, headers=headers, start_offset=42.0)

    assert oc.clock_offset_s == 42.0
    assert res["ok"] is True


def test_pull_config_offset_updated_even_when_response_not_ok(monkeypatch):
    """Date là của tầng HTTP - lệch giờ vẫn đo được cả khi Odoo trả lỗi."""
    import edge_collector.odoo_client as oc_mod
    monkeypatch.setattr(oc_mod.time, "time", lambda: 1_790_000_000.0)

    oc, _, _ = _pull(_KvStore(), {"ok": False}, status=403,
                     headers={"Date": _http_date(1_790_000_000 - 60)})

    assert oc.clock_offset_s == pytest.approx(-60.0)


def test_offset_learned_by_pull_config_lets_signed_command_through(monkeypatch):
    """Ghép 2 nửa: pull_config học offset từ Date -> inbound_api dùng đúng
    offset đó để chấp nhận ts theo giờ Odoo (edge chậm 10 phút, không NTP)."""
    import edge_collector.odoo_client as oc_mod
    edge_now = 1_790_000_000.0
    monkeypatch.setattr(oc_mod.time, "time", lambda: edge_now)
    oc, _, _ = _pull(_KvStore(), {"ok": True},
                     headers={"Date": _http_date(edge_now + 600)})
    app = _app(agent=SimpleNamespace(odoo=oc))

    resp = _client(app, DownlinkSigner(ts=int(edge_now) + 600)).post("/api/command", json=CMD)

    assert resp.status_code == 200


# ===========================================================================
# Regression bug "khóa trông như số" (pcm-edge-hmac r2): Store.kv_get chạy
# json.loads -> "1234567890" đọc ra int, "1e5" ra float -> trước fix
# inbound_api gọi key.encode() -> HTTP 500. Dùng Store SQLite THẬT.
# ===========================================================================


@pytest.mark.parametrize("key", ["1234567890", "1e5", "true", "null", '"abc"', "khoa-thuong"],
                         ids=["int-like", "float-like", "bool-like", "null-like", "json-str",
                              "plain"])
def test_numeric_looking_key_via_pull_config_and_real_store_signs_ok_regression(tmp_path, key):
    from edge_collector.store import Store
    store = Store(tmp_path / "edge.db")
    _pull(store, {"ok": True, "downlink_key": key})
    assert store.kv_get("downlink_key") == key  # đọc lại đúng str gốc
    app = _app(store=store)
    client = TestClient(app, headers={"X-Edge-Code": settings.edge_code},
                        raise_server_exceptions=False)
    client.auth = DownlinkSigner(key=key)

    resp = client.post("/api/command", json=CMD)

    assert resp.status_code == 200, resp.text
    assert app.state.manager.queue_calls == [("NODE1", "relay_red", "on", None)]


def test_pull_config_same_key_real_store_does_not_rewrite(tmp_path):
    from edge_collector.store import Store
    store = Store(tmp_path / "edge.db")
    _pull(store, {"ok": True, "downlink_key": "1e5"})
    calls = []
    orig = store.kv_set
    store.kv_set = lambda k, v: (calls.append(k), orig(k, v))

    _pull(store, {"ok": True, "downlink_key": "1e5"})

    assert calls == []


def test_legacy_raw_numeric_key_in_store_is_healed_by_next_pull(tmp_path):
    """Khóa đã lưu THÔ trước fix ("1234567890" -> kv_get ra int): lần pull
    sau so int != str -> ghi lại dạng JSON -> đọc ra str."""
    from edge_collector.store import Store
    store = Store(tmp_path / "edge.db")
    store.kv_set("downlink_key", "1234567890")
    assert store.kv_get("downlink_key") == 1234567890

    _pull(store, {"ok": True, "downlink_key": "1234567890"})

    assert store.kv_get("downlink_key") == "1234567890"


@pytest.mark.parametrize("bad", [1234567890, 1e5, True, ["k"], {"k": 1}],
                         ids=["int", "float", "bool", "list", "dict"])
def test_store_returning_non_str_key_rejects_401_not_500(bad):
    """Fail-closed: kv_get trả kiểu không phải str -> 401 'chưa có
    downlink_key', KHÔNG crash 500 (key.encode())."""
    store = _Store()
    store.kv["downlink_key"] = bad
    app = _app(store=store)
    client = TestClient(app, headers={"X-Edge-Code": settings.edge_code},
                        raise_server_exceptions=False)
    client.auth = DownlinkSigner(key=str(bad))

    resp = client.post("/api/command", json=CMD)

    _assert_rejected(resp, "chưa có downlink_key")
    assert app.state.manager.queue_calls == []


def test_date_header_minus_0000_is_treated_as_utc_regression(monkeypatch):
    """Regression: "-0000" cho datetime không múi giờ; trước fix .timestamp()
    hiểu theo giờ máy edge (TZ +07 -> lệch 7 tiếng). Phải bằng bản GMT.
    Ép TZ máy = +07 để test có nghĩa trên mọi máy chạy CI."""
    import os
    import edge_collector.odoo_client as oc_mod
    monkeypatch.setattr(oc_mod.time, "time", lambda: 1_790_000_000.0)
    gmt = _http_date(1_790_000_000 + 100)
    minus0 = gmt.replace("GMT", "-0000")
    old_tz = os.environ.get("TZ")
    os.environ["TZ"] = "Asia/Ho_Chi_Minh"
    time.tzset()
    try:
        oc_gmt, _, _ = _pull(_KvStore(), {"ok": True}, headers={"Date": gmt})
        oc_m0, _, _ = _pull(_KvStore(), {"ok": True}, headers={"Date": minus0})
    finally:
        if old_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = old_tz
        time.tzset()

    assert oc_gmt.clock_offset_s == pytest.approx(100.0)
    assert oc_m0.clock_offset_s == pytest.approx(100.0)
