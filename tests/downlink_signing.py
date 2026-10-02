# -*- coding: utf-8 -*-
"""Helper ký request Odoo -> edge cho test (task pcm-edge-hmac).

Tính chữ ký ĐỘC LẬP với inbound_api (không import _downlink_string_to_sign)
theo đúng contract đã chốt với session Odoo - để test bắt được nếu phía edge
lệch contract:
    X-Edge-Sig = hex HMAC-SHA256(downlink_key,
        f"{METHOD}\\n{path}{'?'+query if query else ''}\\n{ts}\\n{nonce}\\n{sha256_hex(body)}")
path = path đã decode (request.url.path), query = query thô (percent-encoded).
nonce = header X-Edge-Nonce (chuỗi khác rỗng <= 64 ký tự, mỗi request 1 giá trị
mới) - 2 lệnh hợp lệ giống hệt nhau trong cùng giây không bị coi là replay.

DownlinkSigner là httpx.Auth: gắn vào TestClient (client.auth = ...) thì MỌI
request tự được ký trên đúng method/path/query/bytes body mà server nhận.
KHÔNG tự thêm X-Edge-Code - test tự quyết header đó (để test được thiếu/sai).
"""
import hashlib
import hmac
import time
import uuid

import httpx

DOWNLINK_KEY = "test-downlink-key-0123456789"  # secret-allow: test fixture, không phải credential thật


def sign(method, path, query, ts, nonce, body=b"", key=DOWNLINK_KEY):
    msg = "%s\n%s%s\n%s\n%s\n%s" % (method.upper(), path, "?" + query if query else "",
                                    ts, nonce, hashlib.sha256(body).hexdigest())
    return hmac.new(key.encode(), msg.encode(), hashlib.sha256).hexdigest()


class DownlinkSigner(httpx.Auth):
    """ts=None -> giờ hiện tại; nonce=None -> uuid4 hex MỚI mỗi request.
    sign_* cho phép ký trên giá trị KHÁC giá trị thật gửi đi (giả lập kẻ sửa
    gói sau khi ký)."""
    requires_request_body = True

    def __init__(self, key=DOWNLINK_KEY, ts=None, nonce=None, sign_method=None,
                 sign_path=None, sign_query=None, sign_body=None, sign_nonce=None):
        self.key = key
        self.ts = ts
        self.nonce = nonce
        self.sign_nonce = sign_nonce
        self.sign_method = sign_method
        self.sign_path = sign_path
        self.sign_query = sign_query
        self.sign_body = sign_body
        self.last_sig = None

    def auth_flow(self, request):
        ts = str(int(time.time()) if self.ts is None else self.ts)
        nonce = uuid.uuid4().hex if self.nonce is None else self.nonce
        query = request.url.query.decode("ascii") if self.sign_query is None else self.sign_query
        self.last_sig = sign(
            self.sign_method or request.method,
            self.sign_path or request.url.path,
            query, ts,
            nonce if self.sign_nonce is None else self.sign_nonce,
            request.content if self.sign_body is None else self.sign_body,
            key=self.key)
        request.headers["X-Edge-Ts"] = ts
        request.headers["X-Edge-Nonce"] = nonce
        request.headers["X-Edge-Sig"] = self.last_sig
        yield request


class KeyStore:
    """Store giả tối thiểu cho _require_downlink_auth: chỉ kv_get('downlink_key')."""

    def __init__(self, key=DOWNLINK_KEY):
        self.kv = {"downlink_key": key} if key is not None else {}

    def kv_get(self, k, default=None):
        return self.kv.get(k, default)

    def kv_set(self, k, v):
        self.kv[k] = v
