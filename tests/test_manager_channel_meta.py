# -*- coding: utf-8 -*-
"""Test SourceManager.channel_meta_for() / SourceManager.apply_config()
phan populate `_channel_meta` (29/09 - contract chot voi pcm_base cho tinh
nang loc-trung-gia-tri o EdgeAgent._should_skip_duplicate(), xem chu thich
_channel_meta trong manager.py + tests/test_scheduler_dedup.py cho phia
scheduler dung du lieu nay).

Dung SourceManager() THAT - __init__ khong co side-effect (khong mo file/
socket), khac EdgeAgent (xem test_manager_queue_command.py cung dung pattern
nay)."""
import asyncio

import pytest

from edge_collector.manager import SourceManager


def _manager():
    return SourceManager(on_value=lambda *a: None)


def test_channel_meta_for_missing_channel_returns_safe_default():
    """Channel CHUA TUNG duoc apply_config() (vd chua nhan config lan nao,
    hoac channel moi thay chua co trong lan config gan nhat) -> phai fallback
    ve gia tri AN TOAN giong het hanh vi TRUOC KHI co tinh nang loc trung:
    gui MOI lan doc."""
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
    """Channel KHONG co field "must_send_every" trong payload config (Odoo
    cu chua nang cap / field optional) -> phai mac dinh True (an toan), khong
    duoc suy ra False."""
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
    """Channel KHONG co "source" (khong route duoc driver, se KHONG co mat
    trong channels_by_source/_route) - _channel_meta VAN phai duoc populate
    cho no (khac _route/channels_by_source - xem chu thich apply_config())."""
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

    # Khong route duoc driver cho channel nay (khong co "source").
    assert manager.driver_for_channel("S1", "no_source_ch") is None
    # NHUNG channel_meta van co du lieu dung.
    assert manager.channel_meta_for("S1", "no_source_ch") == {
        "max_age_ms": 1000, "must_send_every": False,
    }


# ----------------------------------------------------------------------
# Chuan hoa must_send_every NGAY LUC GHI trong apply_config() (regression
# cho finding python-reviewer 2026-09-29 - truoc fix, `ch.get("must_send_every",
# True)` chi ap default khi KEY VANG MAT; key co mat voi value None (vd Odoo
# serialize JSON null) lot qua thanh None (falsy) - nguy hiem cho channel
# counter/trigger/raw-forward vi bi hieu nham thanh must_send_every=False).
# ----------------------------------------------------------------------

def test_apply_config_normalizes_explicit_null_must_send_every_to_true():
    """Field co mat trong payload nhung value la None (JSON null) - phai
    duoc chuan hoa thanh True (an toan), KHONG duoc lot qua thanh None/falsy."""
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
    """Moi kieu du lieu Odoo co the gui (bool/int/string) deu phai duoc ep
    ve dung bool() truoc khi luu - tranh gia tri "truthy la" (vd chuoi rong)
    gay hieu nham falsy/truthy sai khi doc lai o EdgeAgent._should_skip_duplicate()."""
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
