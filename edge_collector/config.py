# -*- coding: utf-8 -*-
"""Cấu hình tiến trình, đọc từ biến môi trường (.env). Không liên quan Odoo config_rev
- đó là cấu hình KÊNH/NGUỒN, tải động bằng odoo_client + manager, không nằm ở đây.
"""
import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

# Đường dẫn .env CHIA SẺ với settings_api.py (trang /setup) - phải cùng MỘT cách
# resolve, không để mỗi nơi tự đoán lấy path riêng (sẽ lệch nhau tùy CWD lúc
# start process, vd systemd WorkingDirectory khác lúc chạy `python -m edge_collector`).
# Ưu tiên tìm theo CWD (đúng workflow README: chạy từ thư mục gốc edge_collector/);
# không thấy thì mặc định vào đúng thư mục gốc project (cạnh package này), không
# bao giờ đoán theo call-stack (hành vi ngầm của load_dotenv() không tham số).
DOTENV_PATH = Path(find_dotenv(usecwd=True) or (Path(__file__).resolve().parent.parent / ".env"))

load_dotenv(DOTENV_PATH)


def _int(name, default):
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _bool(name, default=False):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass
class Settings:
    main_url: str = os.environ.get("EDGE_MAIN_URL", "http://localhost:8069").rstrip("/")
    edge_code: str = os.environ.get("EDGE_CODE") or ("EDGE-" + uuid.uuid4().hex[:8].upper())
    edge_name: str = os.environ.get("EDGE_NAME", "")
    edge_platform: str = os.environ.get("EDGE_PLATFORM", "other")
    edge_base_url: str = os.environ.get("EDGE_BASE_URL", "")

    listen_host: str = os.environ.get("EDGE_LISTEN_HOST", "0.0.0.0")
    listen_port: int = _int("EDGE_LISTEN_PORT", 8000)
    # IP/CIDR của reverse-proxy/tunnel được TIN để đọc X-Forwarded-Proto/-Host -
    # truyền thẳng cho uvicorn.run(forwarded_allow_ips=...) (xem __main__.py).
    # Mặc định giữ NGUYÊN default của uvicorn ("127.0.0.1,::1" - chỉ trust
    # loopback) để không đổi hành vi các deployment LAN-only đang chạy; chỉ
    # cần đổi khi đặt edge_collector sau 1 reverse-proxy/tunnel TLS-terminating
    # (vd truy cập /setup qua domain public) - xem review 2026-09-17
    # (CSRF false-positive qua tunnel, _is_same_origin() settings_api.py).
    forwarded_allow_ips: str = os.environ.get("EDGE_FORWARDED_ALLOW_IPS", "127.0.0.1,::1")
    # HTTP Basic Auth token cho toàn bộ /setup/* - rỗng = không gate (mặc định,
    # tương thích ngược). Đọc lại mỗi request (settings_api._check_setup_auth)
    # nên là field "hot" thật sự - xem reload() bên dưới. Sinh ra từ finding
    # của python-reviewer 2026-09-17: GET /setup/api_key trả RAW credential
    # (không phải dữ liệu đo như /setup/activity) qua domain có thể public.
    setup_token: str = os.environ.get("EDGE_SETUP_TOKEN", "")
    # Prefix topic Odoo được phép publish qua /api/publish, cách nhau dấu
    # phẩy. RỖNG = TẮT endpoint (fail-closed): người quản trị site mở từng
    # prefix, workflow Odoo không tự mở rộng được. Đọc mỗi request -> hot-reload.
    publish_topic_allow: str = os.environ.get("EDGE_PUBLISH_TOPIC_ALLOW", "")

    state_dir: Path = field(default_factory=lambda: Path(os.environ.get("EDGE_STATE_DIR", "./var")))

    # --- bên ĐỌC của đường MQTT (mqtt_consumer.py) -----------------------
    # KHÔNG hot-reload được: paho bind client/phiên lúc start(), đổi các giá
    # trị này sau đó không ai đọc lại — nên chúng nằm trong
    # RESTART_REQUIRED_KEYS bên dưới và KHÔNG có trong Settings.reload().
    mqtt_consumer_enabled: bool = _bool("EDGE_MQTT_CONSUMER", False)
    mqtt_consumer_url: str = os.environ.get("EDGE_MQTT_CONSUMER_URL", "mqtt://127.0.0.1:1883")
    mqtt_consumer_user: str = os.environ.get("EDGE_MQTT_CONSUMER_USER", "")
    mqtt_consumer_pass: str = os.environ.get("EDGE_MQTT_CONSUMER_PASS", "")
    mqtt_consumer_topic: str = os.environ.get("EDGE_MQTT_CONSUMER_TOPIC", "fms/+/meas")
    mqtt_consumer_status_topic: str = os.environ.get(
        "EDGE_MQTT_CONSUMER_STATUS_TOPIC", "fms/+/status")
    # Phiên bền bỉ cần client_id CỐ ĐỊNH và DUY NHẤT — xem chú thích đầu
    # mqtt_consumer.py. Lấy theo edge_code để hai edge cạnh nhau không đá
    # nhau ra khỏi broker.
    mqtt_consumer_client_id: str = os.environ.get("EDGE_MQTT_CONSUMER_CLIENT_ID", "")
    # Mặc định TẮT: ở giai đoạn 2 node gửi CÙNG một số đo bằng cả HTTP lẫn
    # MQTT, bật cái này khi cả hai đang chạy sẽ làm Odoo nhận đôi mỗi mẫu.
    mqtt_consumer_forward: bool = _bool("EDGE_MQTT_CONSUMER_FORWARD", False)

    hello_interval_s: int = _int("EDGE_HELLO_INTERVAL_S", 30)
    heartbeat_interval_s: int = _int("EDGE_HEARTBEAT_INTERVAL_S", 30)
    config_poll_interval_s: int = _int("EDGE_CONFIG_POLL_INTERVAL_S", 30)
    print_poll_interval_s: int = _int("EDGE_PRINT_POLL_INTERVAL_S", 3)
    submit_interval_s: float = float(os.environ.get("EDGE_SUBMIT_INTERVAL_S", "2"))
    config_debounce_s: int = _int("EDGE_CONFIG_DEBOUNCE_S", 10)

    def __post_init__(self):
        self.state_dir = Path(self.state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        if not self.mqtt_consumer_client_id:
            self.mqtt_consumer_client_id = "edge-consumer-%s" % self.edge_code

    @property
    def state_json_path(self) -> Path:
        return self.state_dir / "edge_state.json"

    @property
    def sqlite_path(self) -> Path:
        return self.state_dir / "edge_collector.db"

    def reload(self) -> None:
        """Đọc lại os.environ (gọi SAU load_dotenv(..., override=True)) và
        CẬP NHẬT TẠI CHỖ thuộc tính của CHÍNH object này - odoo_client.py/
        scheduler.py đã `from .config import settings` giữ tham chiếu tới
        object này, gán `settings = Settings()` mới ở đây sẽ KHÔNG được các
        module đó thấy, phải sửa thuộc tính in-place.

        CHỈ reload các field có hiệu lực "hot" thật sự (đã xác nhận đọc
        settings.X tươi mỗi lần gọi/mỗi vòng lặp, không bị "bake" một lần):
        main_url/edge_code/edge_name/edge_platform/edge_base_url (odoo_client.py
        _headers()/hello() đọc lại mỗi call) + 6 interval (scheduler.py đọc
        lại mỗi vòng asyncio.sleep) + setup_token (settings_api._check_setup_auth
        đọc lại mỗi request). KHÔNG dùng cho listen_host/listen_port
        (uvicorn đã bind socket lúc khởi động, đổi vào đây không ai đọc lại),
        state_dir (Store đã mở SQLite cố định lúc EdgeAgent.__init__), và
        forwarded_allow_ips (uvicorn.run() đã đọc giá trị này 1 lần lúc
        khởi động để dựng ProxyHeadersMiddleware - xem __main__.py) -
        4 field này VẪN CẦN restart, xem review 2026-09-17 (tính năng
        hot-reload cho trang /setup).

        AN TOÀN với 7 background task của EdgeAgent (scheduler.py) đang đọc
        settings.X song song vì: (1) toàn bộ gán thuộc tính ở đây KHÔNG có
        `await` nào xen giữa - một khi bắt đầu chạy sẽ chạy hết trong 1 lượt
        của event loop (asyncio single-thread, cooperative), không coroutine
        nào khác (kể cả _hello_loop) có cơ hội xen vào giữa chúng; (2) chỉ 1
        uvicorn worker (xem __main__.py). Nếu SAU NÀY có code đọc settings.X
        từ THREAD RIÊNG (vd run_in_executor) hoặc bật multi-worker, invariant
        này KHÔNG còn đúng - phải thêm lock/đồng bộ thật."""
        self.main_url = os.environ.get("EDGE_MAIN_URL", "http://localhost:8069").rstrip("/")
        self.edge_code = os.environ.get("EDGE_CODE") or self.edge_code
        self.edge_name = os.environ.get("EDGE_NAME", "")
        self.edge_platform = os.environ.get("EDGE_PLATFORM", "other")
        self.edge_base_url = os.environ.get("EDGE_BASE_URL", "")
        self.hello_interval_s = _int("EDGE_HELLO_INTERVAL_S", self.hello_interval_s)
        self.heartbeat_interval_s = _int("EDGE_HEARTBEAT_INTERVAL_S", self.heartbeat_interval_s)
        self.config_poll_interval_s = _int("EDGE_CONFIG_POLL_INTERVAL_S", self.config_poll_interval_s)
        self.print_poll_interval_s = _int("EDGE_PRINT_POLL_INTERVAL_S", self.print_poll_interval_s)
        self.submit_interval_s = float(
            os.environ.get("EDGE_SUBMIT_INTERVAL_S", str(self.submit_interval_s)))
        self.config_debounce_s = _int("EDGE_CONFIG_DEBOUNCE_S", self.config_debounce_s)
        self.setup_token = os.environ.get("EDGE_SETUP_TOKEN", "")
        self.publish_topic_allow = os.environ.get("EDGE_PUBLISH_TOPIC_ALLOW", "")


settings = Settings()

# Field .env KHÔNG thể hot-reload (cần restart edge_collector) - dùng ở cả
# settings_api.py (hiện badge/thông báo) lẫn test, tránh 2 nơi liệt kê lệch
# nhau.
RESTART_REQUIRED_KEYS = {"EDGE_LISTEN_HOST", "EDGE_LISTEN_PORT", "EDGE_STATE_DIR",
                          "EDGE_FORWARDED_ALLOW_IPS",
                          "EDGE_MQTT_CONSUMER", "EDGE_MQTT_CONSUMER_URL",
                          "EDGE_MQTT_CONSUMER_USER", "EDGE_MQTT_CONSUMER_PASS",
                          "EDGE_MQTT_CONSUMER_TOPIC", "EDGE_MQTT_CONSUMER_STATUS_TOPIC",
                          "EDGE_MQTT_CONSUMER_CLIENT_ID", "EDGE_MQTT_CONSUMER_FORWARD"}


def reload_settings(path: Path = DOTENV_PATH) -> None:
    """Gọi sau khi /setup ghi xong .env - nạp lại os.environ TỪ FILE (override=
    True, khác với load_dotenv() lúc khởi động VỐN không để ghi đè biến đã có
    sẵn) rồi cập nhật singleton `settings`. Không tự động áp dụng cho
    RESTART_REQUIRED_KEYS (xem Settings.reload).

    Nhận `path` tường minh (mặc định DOTENV_PATH) thay vì hardcode - test dùng
    file .env riêng trong tmp_path (không phải file thật của project), phải
    truyền đúng path đó vào đây, nếu không reload sẽ đọc NHẦM file thật trên
    đĩa trong lúc test (settings_api._ENV_PATH đã monkeypatch nhưng hàm này
    trước đây không nhận path nên vẫn tự đọc DOTENV_PATH gốc) - xem review
    2026-09-17 (tính năng hot-reload)."""
    load_dotenv(path, override=True)
    settings.reload()
