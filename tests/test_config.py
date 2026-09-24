# -*- coding: utf-8 -*-
"""Test truc tiep config.Settings.reload() / reload_settings() - doc lai
os.environ va cap nhat singleton `settings` TAI CHO (khong tao object moi,
vi odoo_client.py/scheduler.py da giu tham chieu toi CHINH object nay luc
import). Xem review 2026-09-17 (tinh nang hot-reload)."""
import os

import pytest

import edge_collector.config as config


def test_settings_reload_updates_in_place_same_object_identity(monkeypatch):
    """Pin lai invariant ma docstring Settings.reload() tuyen bo: object
    identity KHONG doi truoc/sau reload - neu sau nay ai do vo tinh doi
    thanh `self = Settings()` (khong co tac dung gi trong Python, hoac to te
    hon: gan lai `config.settings = Settings()`), cac module da import
    `settings` truoc do se KHONG thay thay doi - phai bat loi nay bang test,
    khong chi bang comment."""
    original_id = id(config.settings)
    monkeypatch.setenv("EDGE_MAIN_URL", "https://reload-test.example")
    monkeypatch.setenv("EDGE_HELLO_INTERVAL_S", "77")

    config.settings.reload()

    assert id(config.settings) == original_id
    assert config.settings.main_url == "https://reload-test.example"
    assert config.settings.hello_interval_s == 77


def test_settings_reload_ignores_restart_required_keys(monkeypatch):
    original_port = config.settings.listen_port
    original_state_dir = config.settings.state_dir
    monkeypatch.setenv("EDGE_LISTEN_PORT", "9999")
    monkeypatch.setenv("EDGE_STATE_DIR", "./should-not-apply")

    config.settings.reload()

    assert config.settings.listen_port == original_port
    assert config.settings.state_dir == original_state_dir


def test_settings_config_field_forwarded_allow_ips_default_and_restart_required(monkeypatch):
    """Regression cho fix CSRF false-reject qua reverse-proxy (xem review
    2026-09-17): field moi EDGE_FORWARDED_ALLOW_IPS phai (1) co default dung
    y het uvicorn ("127.0.0.1,::1" - chi trust loopback, khong doi hanh vi
    LAN-only hien tai), (2) nam trong RESTART_REQUIRED_KEYS (uvicorn.run() chi
    doc gia tri nay 1 lan luc khoi dong), va (3) KHONG bi Settings.reload()
    dung toi - doi field nay sau khi process da chay khong co tac dung gi,
    chi gay hieu lam."""
    assert config.Settings().forwarded_allow_ips == "127.0.0.1,::1"
    assert "EDGE_FORWARDED_ALLOW_IPS" in config.RESTART_REQUIRED_KEYS

    original = config.settings.forwarded_allow_ips
    monkeypatch.setenv("EDGE_FORWARDED_ALLOW_IPS", "10.0.0.5")

    config.settings.reload()

    assert config.settings.forwarded_allow_ips == original


def test_settings_reload_keeps_edge_code_when_env_var_blank(monkeypatch):
    """os.environ.get('EDGE_CODE') tra ve '' (rong, KHONG PHAI None) khi bien
    co ton tai nhung gia tri rong - '' or self.edge_code phai fall qua nhanh
    'or' vi '' la falsy, giu nguyen code dang chay."""
    config.settings.edge_code = "EDGE-KEEP-ME"
    monkeypatch.setenv("EDGE_CODE", "")

    config.settings.reload()

    assert config.settings.edge_code == "EDGE-KEEP-ME"


# --- patch MQTT (merge tu production 192.168.5.190) ------------------------


def test_settings_post_init_auto_generates_mqtt_client_id_from_edge_code(tmp_path):
    """__post_init__ phai tu sinh 'edge-consumer-<edge_code>' khi
    EDGE_MQTT_CONSUMER_CLIENT_ID de trong - client_id PHAI CO DINH va KHAC
    NHAU giua cac edge (xem chu thich dau mqtt_consumer.py, muc 'Ben bi khi
    consumer chet'): hai tien trinh dung chung client_id se da nhau ra khoi
    broker lien tuc. Truyen state_dir=tmp_path de tranh __post_init__ tao
    thu muc './var' that cua repo nhu mot side-effect ngoai y muon."""
    s = config.Settings(mqtt_consumer_client_id="", edge_code="EDGE-TEST123",
                        state_dir=tmp_path)

    assert s.mqtt_consumer_client_id == "edge-consumer-EDGE-TEST123"


def test_settings_post_init_keeps_explicit_mqtt_client_id(tmp_path):
    """Da co gia tri tuong minh (vd nguoi dung tu dat trong .env) -> KHONG
    duoc ghi de bang gia tri tu sinh."""
    s = config.Settings(mqtt_consumer_client_id="custom-fixed-id",
                        edge_code="EDGE-X", state_dir=tmp_path)

    assert s.mqtt_consumer_client_id == "custom-fixed-id"


@pytest.mark.parametrize("raw,expected", [
    ("true", True), ("True", True), ("TRUE", True),
    ("1", True), ("yes", True), ("YES", True), ("on", True), ("On", True),
    ("false", False), ("False", False), ("0", False),
    ("no", False), ("off", False), ("garbage", False),
])
def test_bool_parses_truthy_and_falsy_variants(monkeypatch, raw, expected):
    monkeypatch.setenv("EDGE_TEST_BOOL_FIELD", raw)

    assert config._bool("EDGE_TEST_BOOL_FIELD", not expected) is expected


def test_bool_returns_default_when_env_var_missing(monkeypatch):
    monkeypatch.delenv("EDGE_TEST_BOOL_FIELD", raising=False)

    assert config._bool("EDGE_TEST_BOOL_FIELD", True) is True
    assert config._bool("EDGE_TEST_BOOL_FIELD", False) is False


def test_bool_returns_default_when_env_var_blank(monkeypatch):
    """Gia tri RONG (bien co ton tai, vd EDGE_MQTT_CONSUMER_FORWARD= trong
    .env) khac voi bien KHONG TON TAI - ca hai phai cung roi ve default,
    khong duoc bi coi la falsy '0'/'false' mot cach am tham."""
    monkeypatch.setenv("EDGE_TEST_BOOL_FIELD", "")

    assert config._bool("EDGE_TEST_BOOL_FIELD", True) is True
    assert config._bool("EDGE_TEST_BOOL_FIELD", False) is False


def test_settings_mqtt_consumer_restart_required_keys_registered():
    """8 field EDGE_MQTT_CONSUMER* moi (tru _FORWARD, ma _FORWARD cung nam
    trong RESTART_REQUIRED_KEYS - xem config.py) phai nam trong
    RESTART_REQUIRED_KEYS: paho bind client/phien luc start(), doi cac gia
    tri nay sau do khong ai doc lai (xem chu thich Settings)."""
    for key in ("EDGE_MQTT_CONSUMER", "EDGE_MQTT_CONSUMER_URL",
                "EDGE_MQTT_CONSUMER_USER", "EDGE_MQTT_CONSUMER_PASS",
                "EDGE_MQTT_CONSUMER_TOPIC", "EDGE_MQTT_CONSUMER_STATUS_TOPIC",
                "EDGE_MQTT_CONSUMER_CLIENT_ID", "EDGE_MQTT_CONSUMER_FORWARD"):
        assert key in config.RESTART_REQUIRED_KEYS

    # KHONG duoc "hot" - Settings.reload() khong duoc dung toi field nay.
    original = config.settings.mqtt_consumer_forward
    os.environ["EDGE_MQTT_CONSUMER_FORWARD"] = "true" if not original else "false"
    try:
        config.settings.reload()
        assert config.settings.mqtt_consumer_forward == original
    finally:
        del os.environ["EDGE_MQTT_CONSUMER_FORWARD"]
