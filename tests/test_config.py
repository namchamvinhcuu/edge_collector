# -*- coding: utf-8 -*-
"""Test truc tiep config.Settings.reload() / reload_settings() - doc lai
os.environ va cap nhat singleton `settings` TAI CHO (khong tao object moi,
vi odoo_client.py/scheduler.py da giu tham chieu toi CHINH object nay luc
import). Xem review 2026-09-17 (tinh nang hot-reload)."""
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
