# -*- coding: utf-8 -*-
"""Test SourceManager.channel_meta_for() / SourceManager.apply_config()
phần populate `_channel_meta` (29/09 - contract chốt với pcm_base cho tính
năng lọc-trùng-giá-trị ở EdgeAgent._should_skip_duplicate(), xem chú thích
_channel_meta trong manager.py + tests/test_scheduler_dedup.py cho phía
scheduler dùng dữ liệu này).

Dùng SourceManager() THẬT - __init__ không có side-effect (không mở file/
socket), khác EdgeAgent (xem test_manager_queue_command.py cùng dùng pattern
này)."""
import asyncio

import pytest

from edge_collector.manager import SourceManager


def _manager():
    return SourceManager(on_value=lambda *a: None)


def test_channel_meta_for_missing_channel_returns_safe_default():
    """Channel CHƯA TỪNG được apply_config() (vd chưa nhận config lần nào,
    hoặc channel mới thay chưa có trong lần config gần nhất) -> phải fallback
    về giá trị AN TOÀN giống hệt hành vi TRƯỚC KHI có tính năng lọc trùng:
    gửi MỖI lần đọc."""
    manager = _manager()

    meta = manager.channel_meta_for("S1", "khong-ton-tai")

    assert meta == {"max_age_ms": None, "must_send_every": True}


def test_apply_config_populates_channel_meta_from_devices():
    manager = _manager()
    cfg = {
        "config_version": 1,
        "devices": [
            {"serial": "S1", "channels": [
                {"code": "weight", "max_age_ms": 4000, "must_send_every": False,
                 "source": "src1"},
                {"code": "counter", "must_send_every": True},
            ]},
        ],
    }

    asyncio.run(manager.apply_config(cfg))

    assert manager.channel_meta_for("S1", "weight") == {
        "max_age_ms": 4000, "must_send_every": False,
    }
    assert manager.channel_meta_for("S1", "counter") == {
        "max_age_ms": None, "must_send_every": True,
    }


def test_apply_config_defaults_must_send_every_true_when_field_missing():
    """Channel KHÔNG có field "must_send_every" trong payload config (Odoo
    cũ chưa nâng cấp / field optional) -> phải mặc định True (an toàn), không
    được suy ra False."""
    manager = _manager()
    cfg = {
        "config_version": 1,
        "devices": [
            {"serial": "S1", "channels": [{"code": "raw_evt"}]},
        ],
    }

    asyncio.run(manager.apply_config(cfg))

    assert manager.channel_meta_for("S1", "raw_evt") == {
        "max_age_ms": None, "must_send_every": True,
    }


def test_apply_config_populates_channel_meta_even_without_source():
    """Channel KHÔNG có "source" (không route được driver, sẽ KHÔNG có mặt
    trong channels_by_source/_route) - _channel_meta VẪN phải được populate
    cho nó (khác _route/channels_by_source - xem chú thích apply_config())."""
    manager = _manager()
    cfg = {
        "config_version": 1,
        "devices": [
            {"serial": "S1", "channels": [
                {"code": "no_source_ch", "max_age_ms": 1000, "must_send_every": False},
            ]},
        ],
    }

    asyncio.run(manager.apply_config(cfg))

    # Không route được driver cho channel này (không có "source").
    assert manager.driver_for_channel("S1", "no_source_ch") is None
    # NHƯNG channel_meta vẫn có dữ liệu đúng.
    assert manager.channel_meta_for("S1", "no_source_ch") == {
        "max_age_ms": 1000, "must_send_every": False,
    }


# ----------------------------------------------------------------------
# Chuẩn hóa must_send_every NGAY LÚC GHI trong apply_config() (regression
# cho finding python-reviewer 2026-09-29 - trước fix, `ch.get("must_send_every",
# True)` chỉ áp default khi KEY VẮNG MẶT; key có mặt với value None (vd Odoo
# serialize JSON null) lọt qua thành None (falsy) - nguy hiểm cho channel
# counter/trigger/raw-forward vì bị hiểu nhầm thành must_send_every=False).
# ----------------------------------------------------------------------

def test_apply_config_normalizes_explicit_null_must_send_every_to_true():
    """Field có mặt trong payload nhưng value là None (JSON null) - phải
    được chuẩn hóa thành True (an toàn), KHÔNG được lọt qua thành None/falsy."""
    manager = _manager()
    cfg = {
        "config_version": 1,
        "devices": [
            {"serial": "S1", "channels": [
                {"code": "counter", "must_send_every": None},
            ]},
        ],
    }

    asyncio.run(manager.apply_config(cfg))

    meta = manager.channel_meta_for("S1", "counter")
    assert meta["must_send_every"] is True
    assert meta["must_send_every"] is not None


@pytest.mark.parametrize("raw_value,expected", [
    (True, True),
    (False, False),
    (1, True),
    (0, False),
    ("yes", True),
    ("", False),
])
def test_apply_config_normalizes_must_send_every_to_proper_bool(raw_value, expected):
    """Mọi kiểu dữ liệu Odoo có thể gửi (bool/int/string) đều phải được ép
    về đúng bool() trước khi lưu - tránh giá trị "truthy lạ" (vd chuỗi rỗng)
    gây hiểu nhầm falsy/truthy sai khi đọc lại ở EdgeAgent._should_skip_duplicate()."""
    manager = _manager()
    cfg = {
        "config_version": 1,
        "devices": [
            {"serial": "S1", "channels": [
                {"code": "ch", "must_send_every": raw_value},
            ]},
        ],
    }

    asyncio.run(manager.apply_config(cfg))

    meta = manager.channel_meta_for("S1", "ch")
    assert meta["must_send_every"] is expected
    assert isinstance(meta["must_send_every"], bool)
