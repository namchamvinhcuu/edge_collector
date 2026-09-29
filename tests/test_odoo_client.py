# -*- coding: utf-8 -*-
"""Test OdooClient._parse() - hàm static parse httpx.Response từ Odoo.

Regression cho bug: nếu Odoo trả JSON hợp lệ nhưng KHÔNG phải object (vd
list/chuỗi/số kèm status lỗi), `body.setdefault(...)` trước đây sẽ raise
AttributeError không bị bắt, lan ra measurements() -> _drain_serial() ->
asyncio.gather() trong _sender_loop, làm chết cả task không tự hồi phục.
Xem python-reviewer 2026-09-24 + fix ở _parse() (nhánh
`if not isinstance(body, dict): body = {"raw": body}` ngay sau khi parse
JSON, trước cả 2 nhánh is_success/not is_success).
"""
import httpx

from edge_collector.odoo_client import OdooClient


def test_parse_error_body_list_wrapped_no_raise_regression():
    """Regression: status lỗi (500) + body JSON là list -> KHÔNG được raise
    AttributeError, phải wrap thành {"raw": [...]}."""
    r = httpx.Response(status_code=500, json=[1, 2, 3])

    result = OdooClient._parse(r)

    assert result == {"raw": [1, 2, 3], "ok": False, "error": "http 500",
                       "status_code": 500}


def test_parse_error_body_string_wrapped_no_raise_regression():
    """Regression: status lỗi (500) + body JSON là string -> wrap thành
    {"raw": "oops"}, không raise."""
    r = httpx.Response(status_code=500, json="oops")

    result = OdooClient._parse(r)

    assert result == {"raw": "oops", "ok": False, "error": "http 500",
                       "status_code": 500}


def test_parse_success_body_number_wrapped():
    """Status thành công (200) + body JSON là number (không phải dict) ->
    wrap thành {"raw": 42, "ok": True}, không raise."""
    r = httpx.Response(status_code=200, json=42)

    result = OdooClient._parse(r)

    assert result == {"raw": 42, "ok": True}


def test_parse_success_body_null_wrapped():
    """Status thành công (200) + body JSON là null -> wrap thành
    {"raw": None, "ok": True}, không raise.

    Dùng content=b"null" (không dùng json=None) vì httpx.Response(json=None)
    bỏ qua encode và trả content rỗng b"" - làm r.json() raise ValueError
    (nhánh khác, không phải nhánh đang test ở đây). Đã verify thực nghiệm
    (venv_linux, httpx 0.28.1)."""
    r = httpx.Response(status_code=200, content=b"null", headers={"content-type": "application/json"})

    result = OdooClient._parse(r)

    assert result == {"raw": None, "ok": True}


def test_parse_success_body_dict_ok_not_overridden():
    """Case cũ (không được regress): body dict bình thường, status 200 ->
    setdefault("ok", True) KHÔNG đè lên "ok" đã có sẵn trong body."""
    r = httpx.Response(status_code=200, json={"ok": False, "value": 10})

    result = OdooClient._parse(r)

    assert result == {"ok": False, "value": 10}


def test_parse_error_body_dict_existing_error_not_overridden():
    """Case cũ (không được regress): body dict có sẵn field "error", status
    lỗi -> setdefault KHÔNG ghi đè field đó."""
    r = httpx.Response(status_code=400, json={"error": "invalid api key"})

    result = OdooClient._parse(r)

    assert result == {"error": "invalid api key", "ok": False, "status_code": 400}


def test_parse_invalid_json_wrapped_as_raw_text_regression():
    """Test regression (hành vi cũ, không đổi): body không parse được JSON ->
    vẫn wrap {"raw": r.text[:500]} như trước, không đổi hành vi."""
    r = httpx.Response(status_code=200, content=b"not a json body")

    result = OdooClient._parse(r)

    assert result == {"raw": "not a json body", "ok": True}


# ----------------------------------------------------------------------
# _parse() — 429 + Retry-After (contract chốt 2026-09-29 với pcm_base, xem
# scheduler.py _note_backoff cho phía tiêu thụ).
# ----------------------------------------------------------------------

def test_parse_429_uses_retry_after_header_priority():
    """Header Retry-After: 7 (delta-seconds) -> retry_after == 7, ưu tiên
    header trước, dùng kiểu int (không phải string)."""
    r = httpx.Response(status_code=429, json={"ok": False, "error": "edge busy"},
                        headers={"Retry-After": "7"})

    result = OdooClient._parse(r)

    assert result["retry_after"] == 7
    assert isinstance(result["retry_after"], int)
    assert result["status_code"] == 429


def test_parse_429_falls_back_to_body_retry_after_when_header_missing():
    """Không có header Retry-After, nhưng body JSON có sẵn field retry_after
    (contract phòng hờ client strip header) -> dùng giá trị đó."""
    r = httpx.Response(status_code=429,
                        json={"ok": False, "error": "edge busy", "retry_after": 5})

    result = OdooClient._parse(r)

    assert result["retry_after"] == 5


def test_parse_429_retry_after_none_when_absent_everywhere():
    """Không có header, không có field trong body -> retry_after là None,
    KHÔNG raise/crash (scheduler._note_backoff phải tự xử lý None này bằng
    cách dùng BACKOFF_BASE_S/delay hiện tại làm sàn thay thế)."""
    r = httpx.Response(status_code=429, json={"ok": False, "error": "edge busy"})

    result = OdooClient._parse(r)

    assert result["retry_after"] is None


def test_parse_500_has_status_code_but_no_retry_after_field():
    """500 (khác 429) -> có status_code=500 nhưng KHÔNG có field retry_after -
    field này chỉ có ý nghĩa với 429 theo contract, đừng áp đặt cho status
    khác (scheduler đọc res.get("retry_after") -> None mặc định, không sai)."""
    r = httpx.Response(status_code=500, json={"ok": False, "error": "internal"})

    result = OdooClient._parse(r)

    assert result["status_code"] == 500
    assert "retry_after" not in result
