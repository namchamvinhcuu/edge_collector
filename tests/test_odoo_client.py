# -*- coding: utf-8 -*-
"""Test OdooClient._parse() - ham static parse httpx.Response tu Odoo.

Regression cho bug: neu Odoo tra JSON hop le nhung KHONG phai object (vd
list/chuoi/so kem status loi), `body.setdefault(...)` truoc day se raise
AttributeError khong bi bat, lan ra measurements() -> _drain_serial() ->
asyncio.gather() trong _sender_loop, lam chet ca task khong tu hoi phuc.
Xem python-reviewer 2026-09-24 + fix o _parse() (nhanh
`if not isinstance(body, dict): body = {"raw": body}` ngay sau khi parse
JSON, truoc ca 2 nhanh is_success/not is_success).
"""
import httpx

from edge_collector.odoo_client import OdooClient


def test_parse_error_body_list_wrapped_no_raise_regression():
    """Regression: status loi (500) + body JSON la list -> KHONG duoc raise
    AttributeError, phai wrap thanh {"raw": [...]}."""
    r = httpx.Response(status_code=500, json=[1, 2, 3])

    result = OdooClient._parse(r)

    assert result == {"raw": [1, 2, 3], "ok": False, "error": "http 500"}


def test_parse_error_body_string_wrapped_no_raise_regression():
    """Regression: status loi (500) + body JSON la string -> wrap thanh
    {"raw": "oops"}, khong raise."""
    r = httpx.Response(status_code=500, json="oops")

    result = OdooClient._parse(r)

    assert result == {"raw": "oops", "ok": False, "error": "http 500"}


def test_parse_success_body_number_wrapped():
    """Status thanh cong (200) + body JSON la number (khong phai dict) ->
    wrap thanh {"raw": 42, "ok": True}, khong raise."""
    r = httpx.Response(status_code=200, json=42)

    result = OdooClient._parse(r)

    assert result == {"raw": 42, "ok": True}


def test_parse_success_body_null_wrapped():
    """Status thanh cong (200) + body JSON la null -> wrap thanh
    {"raw": None, "ok": True}, khong raise.

    Dung content=b"null" (khong dung json=None) vi httpx.Response(json=None)
    bo qua encode va tra content rong b"" - lam r.json() raise ValueError
    (nhanh khac, khong phai nhanh dang test o day). Da verify thuc nghiem
    (venv_linux, httpx 0.28.1)."""
    r = httpx.Response(status_code=200, content=b"null", headers={"content-type": "application/json"})

    result = OdooClient._parse(r)

    assert result == {"raw": None, "ok": True}


def test_parse_success_body_dict_ok_not_overridden():
    """Case cu (khong duoc regress): body dict binh thuong, status 200 ->
    setdefault("ok", True) KHONG de len "ok" da co san trong body."""
    r = httpx.Response(status_code=200, json={"ok": False, "value": 10})

    result = OdooClient._parse(r)

    assert result == {"ok": False, "value": 10}


def test_parse_error_body_dict_existing_error_not_overridden():
    """Case cu (khong duoc regress): body dict co san field "error", status
    loi -> setdefault KHONG ghi de field do."""
    r = httpx.Response(status_code=400, json={"error": "invalid api key"})

    result = OdooClient._parse(r)

    assert result == {"error": "invalid api key", "ok": False}


def test_parse_invalid_json_wrapped_as_raw_text_regression():
    """Test regression (hanh vi cu, khong doi): body khong parse duoc JSON ->
    van wrap {"raw": r.text[:500]} nhu truoc, khong doi hanh vi."""
    r = httpx.Response(status_code=200, content=b"not a json body")

    result = OdooClient._parse(r)

    assert result == {"raw": "not a json body", "ok": True}
