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


def test_settings_reload_keeps_edge_code_when_env_var_blank(monkeypatch):
    """os.environ.get('EDGE_CODE') tra ve '' (rong, KHONG PHAI None) khi bien
    co ton tai nhung gia tri rong - '' or self.edge_code phai fall qua nhanh
    'or' vi '' la falsy, giu nguyen code dang chay."""
    config.settings.edge_code = "EDGE-KEEP-ME"
    monkeypatch.setenv("EDGE_CODE", "")

    config.settings.reload()

    assert config.settings.edge_code == "EDGE-KEEP-ME"
