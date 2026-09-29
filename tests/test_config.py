# -*- coding: utf-8 -*-
"""Test trực tiếp config.Settings.reload() / reload_settings() - đọc lại
os.environ và cập nhật singleton `settings` TẠI CHỖ (không tạo object mới,
vì odoo_client.py/scheduler.py đã giữ tham chiếu tới CHÍNH object này lúc
import). Xem review 2026-09-17 (tính năng hot-reload)."""
import os

import pytest

import edge_collector.config as config


def test_settings_reload_updates_in_place_same_object_identity(monkeypatch):
    """Pin lại invariant mà docstring Settings.reload() tuyên bố: object
    identity KHÔNG đổi trước/sau reload - nếu sau này ai đó vô tình đổi
    thành `self = Settings()` (không có tác dụng gì trong Python, hoặc tệ
    hơn: gán lại `config.settings = Settings()`), các module đã import
    `settings` trước đó sẽ KHÔNG thấy thay đổi - phải bắt lỗi này bằng test,
    không chỉ bằng comment."""
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
    2026-09-17): field mới EDGE_FORWARDED_ALLOW_IPS phải (1) có default đúng
    y hệt uvicorn ("127.0.0.1,::1" - chỉ trust loopback, không đổi hành vi
    LAN-only hiện tại), (2) nằm trong RESTART_REQUIRED_KEYS (uvicorn.run() chỉ
    đọc giá trị này 1 lần lúc khởi động), và (3) KHÔNG bị Settings.reload()
    đụng tới - đổi field này sau khi process đã chạy không có tác dụng gì,
    chỉ gây hiểu lầm."""
    assert config.Settings().forwarded_allow_ips == "127.0.0.1,::1"
    assert "EDGE_FORWARDED_ALLOW_IPS" in config.RESTART_REQUIRED_KEYS

    original = config.settings.forwarded_allow_ips
    monkeypatch.setenv("EDGE_FORWARDED_ALLOW_IPS", "10.0.0.5")

    config.settings.reload()

    assert config.settings.forwarded_allow_ips == original


def test_settings_reload_keeps_edge_code_when_env_var_blank(monkeypatch):
    """os.environ.get('EDGE_CODE') trả về '' (rỗng, KHÔNG PHẢI None) khi biến
    có tồn tại nhưng giá trị rỗng - '' or self.edge_code phải fall qua nhánh
    'or' vì '' là falsy, giữ nguyên code đang chạy."""
    config.settings.edge_code = "EDGE-KEEP-ME"
    monkeypatch.setenv("EDGE_CODE", "")

    config.settings.reload()

    assert config.settings.edge_code == "EDGE-KEEP-ME"


# --- patch MQTT (merge từ production 192.168.5.190) ------------------------


def test_settings_post_init_auto_generates_mqtt_client_id_from_edge_code(tmp_path):
    """__post_init__ phải tự sinh 'edge-consumer-<edge_code>' khi
    EDGE_MQTT_CONSUMER_CLIENT_ID để trống - client_id PHẢI CỐ ĐỊNH và KHÁC
    NHAU giữa các edge (xem chú thích đầu mqtt_consumer.py, mục 'Bên bị khi
    consumer chết'): hai tiến trình dùng chung client_id sẽ đá nhau ra khỏi
    broker liên tục. Truyền state_dir=tmp_path để tránh __post_init__ tạo
    thư mục './var' thật của repo như một side-effect ngoài ý muốn."""
    s = config.Settings(mqtt_consumer_client_id="", edge_code="EDGE-TEST123",
                        state_dir=tmp_path)

    assert s.mqtt_consumer_client_id == "edge-consumer-EDGE-TEST123"


def test_settings_post_init_keeps_explicit_mqtt_client_id(tmp_path):
    """Đã có giá trị tường minh (vd người dùng tự đặt trong .env) -> KHÔNG
    được ghi đè bằng giá trị tự sinh."""
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
    """Giá trị RỖNG (biến có tồn tại, vd EDGE_MQTT_CONSUMER_FORWARD= trong
    .env) khác với biến KHÔNG TỒN TẠI - cả hai phải cùng rơi về default,
    không được bị coi là falsy '0'/'false' một cách âm thầm."""
    monkeypatch.setenv("EDGE_TEST_BOOL_FIELD", "")

    assert config._bool("EDGE_TEST_BOOL_FIELD", True) is True
    assert config._bool("EDGE_TEST_BOOL_FIELD", False) is False


def test_settings_mqtt_consumer_restart_required_keys_registered():
    """8 field EDGE_MQTT_CONSUMER* mới (trừ _FORWARD, mà _FORWARD cũng nằm
    trong RESTART_REQUIRED_KEYS - xem config.py) phải nằm trong
    RESTART_REQUIRED_KEYS: paho bind client/phiên lúc start(), đổi các giá
    trị này sau đó không ai đọc lại (xem chú thích Settings)."""
    for key in ("EDGE_MQTT_CONSUMER", "EDGE_MQTT_CONSUMER_URL",
                "EDGE_MQTT_CONSUMER_USER", "EDGE_MQTT_CONSUMER_PASS",
                "EDGE_MQTT_CONSUMER_TOPIC", "EDGE_MQTT_CONSUMER_STATUS_TOPIC",
                "EDGE_MQTT_CONSUMER_CLIENT_ID", "EDGE_MQTT_CONSUMER_FORWARD"):
        assert key in config.RESTART_REQUIRED_KEYS

    # KHÔNG được "hot" - Settings.reload() không được đụng tới field này.
    original = config.settings.mqtt_consumer_forward
    os.environ["EDGE_MQTT_CONSUMER_FORWARD"] = "true" if not original else "false"
    try:
        config.settings.reload()
        assert config.settings.mqtt_consumer_forward == original
    finally:
        del os.environ["EDGE_MQTT_CONSUMER_FORWARD"]
