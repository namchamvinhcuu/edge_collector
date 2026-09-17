# -*- coding: utf-8 -*-
"""Cau hinh tien trinh, doc tu bien moi truong (.env). Khong lien quan Odoo config_rev
- do la cau hinh KENH/NGUON, tai dong bang odoo_client + manager, khong nam o day.
"""
import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

# Duong dan .env CHIA SE voi settings_api.py (trang /setup) - phai cung MOT cach
# resolve, khong de moi noi tu doan lay path rieng (se lech nhau tuy CWD luc
# start process, vd systemd WorkingDirectory khac luc chay `python -m edge_collector`).
# Uu tien tim theo CWD (dung workflow README: chay tu thu muc goc edge_collector/);
# khong thay thi mac dinh vao dung thu muc goc project (canh package nay), khong
# bao gio doan theo call-stack (hanh vi ngam cua load_dotenv() khong tham so).
DOTENV_PATH = Path(find_dotenv(usecwd=True) or (Path(__file__).resolve().parent.parent / ".env"))

load_dotenv(DOTENV_PATH)


def _int(name, default):
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


@dataclass
class Settings:
    main_url: str = os.environ.get("EDGE_MAIN_URL", "http://localhost:8069").rstrip("/")
    edge_code: str = os.environ.get("EDGE_CODE") or ("EDGE-" + uuid.uuid4().hex[:8].upper())
    edge_name: str = os.environ.get("EDGE_NAME", "")
    edge_platform: str = os.environ.get("EDGE_PLATFORM", "other")
    edge_base_url: str = os.environ.get("EDGE_BASE_URL", "")

    listen_host: str = os.environ.get("EDGE_LISTEN_HOST", "0.0.0.0")
    listen_port: int = _int("EDGE_LISTEN_PORT", 8000)
    # IP/CIDR cua reverse-proxy/tunnel duoc TIN de doc X-Forwarded-Proto/-Host -
    # truyen thang cho uvicorn.run(forwarded_allow_ips=...) (xem __main__.py).
    # Mac dinh giu NGUYEN default cua uvicorn ("127.0.0.1,::1" - chi trust
    # loopback) de khong doi hanh vi cac deployment LAN-only dang chay; chi
    # can doi khi dat edge_collector sau 1 reverse-proxy/tunnel TLS-terminating
    # (vd truy cap /setup qua domain public) - xem review 2026-09-17
    # (CSRF false-positive qua tunnel, _is_same_origin() settings_api.py).
    forwarded_allow_ips: str = os.environ.get("EDGE_FORWARDED_ALLOW_IPS", "127.0.0.1,::1")
    # HTTP Basic Auth token cho toan bo /setup/* - rong = khong gate (mac dinh,
    # tuong thich nguoc). Doc lai moi request (settings_api._check_setup_auth)
    # nen la field "hot" that su - xem reload() ben duoi. Sinh ra tu finding
    # cua python-reviewer 2026-09-17: GET /setup/api_key tra RAW credential
    # (khong phai du lieu do nhu /setup/activity) qua domain co the public.
    setup_token: str = os.environ.get("EDGE_SETUP_TOKEN", "")

    state_dir: Path = field(default_factory=lambda: Path(os.environ.get("EDGE_STATE_DIR", "./var")))

    hello_interval_s: int = _int("EDGE_HELLO_INTERVAL_S", 30)
    heartbeat_interval_s: int = _int("EDGE_HEARTBEAT_INTERVAL_S", 30)
    config_poll_interval_s: int = _int("EDGE_CONFIG_POLL_INTERVAL_S", 30)
    print_poll_interval_s: int = _int("EDGE_PRINT_POLL_INTERVAL_S", 3)
    submit_interval_s: float = float(os.environ.get("EDGE_SUBMIT_INTERVAL_S", "2"))
    config_debounce_s: int = _int("EDGE_CONFIG_DEBOUNCE_S", 10)

    def __post_init__(self):
        self.state_dir = Path(self.state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    @property
    def state_json_path(self) -> Path:
        return self.state_dir / "edge_state.json"

    @property
    def sqlite_path(self) -> Path:
        return self.state_dir / "edge_collector.db"

    def reload(self) -> None:
        """Doc lai os.environ (goi SAU load_dotenv(..., override=True)) va
        CAP NHAT TAI CHO thuoc tinh cua CHINH object nay - odoo_client.py/
        scheduler.py da `from .config import settings` giu tham chieu toi
        object nay, gan `settings = Settings()` moi o day se KHONG duoc cac
        module do thay, phai sua thuoc tinh in-place.

        CHI reload cac field co hieu luc "hot" that su (da xac nhan doc
        settings.X tuoi moi lan goi/moi vong lap, khong bi "bake" mot lan):
        main_url/edge_code/edge_name/edge_platform/edge_base_url (odoo_client.py
        _headers()/hello() doc lai moi call) + 6 interval (scheduler.py doc
        lai moi vong asyncio.sleep) + setup_token (settings_api._check_setup_auth
        doc lai moi request). KHONG dung cho listen_host/listen_port
        (uvicorn da bind socket luc khoi dong, doi vao day khong ai doc lai),
        state_dir (Store da mo SQLite co dinh luc EdgeAgent.__init__), va
        forwarded_allow_ips (uvicorn.run() da doc gia tri nay 1 lan luc
        khoi dong de dung ProxyHeadersMiddleware - xem __main__.py) -
        4 field nay VAN CAN restart, xem review 2026-09-17 (tinh nang
        hot-reload cho trang /setup).

        AN TOAN voi 7 background task cua EdgeAgent (scheduler.py) dang doc
        settings.X song song vi: (1) toan bo gan thuoc tinh o day KHONG co
        `await` nao xen giua - mot khi bat dau chay se chay het trong 1 luot
        cua event loop (asyncio single-thread, cooperative), khong coroutine
        nao khac (ke ca _hello_loop) co co hoi xen vao giua chung; (2) chi 1
        uvicorn worker (xem __main__.py). Neu SAU NAY co code doc settings.X
        tu THREAD RIENG (vd run_in_executor) hoac bat multi-worker, invariant
        nay KHONG con dung - phai them lock/dong bo that."""
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


settings = Settings()

# Field .env KHONG the hot-reload (can restart edge_collector) - dung o ca
# settings_api.py (hien badge/thong bao) lan test, tranh 2 noi liet ke lech
# nhau.
RESTART_REQUIRED_KEYS = {"EDGE_LISTEN_HOST", "EDGE_LISTEN_PORT", "EDGE_STATE_DIR",
                          "EDGE_FORWARDED_ALLOW_IPS"}


def reload_settings(path: Path = DOTENV_PATH) -> None:
    """Goi sau khi /setup ghi xong .env - nap lai os.environ TU FILE (override=
    True, khac voi load_dotenv() luc khoi dong VON khong de ghi de bien da co
    san) roi cap nhat singleton `settings`. Khong tu dong ap dung cho
    RESTART_REQUIRED_KEYS (xem Settings.reload).

    Nhan `path` tuong minh (mac dinh DOTENV_PATH) thay vi hardcode - test dung
    file .env rieng trong tmp_path (khong phai file that cua project), phai
    truyen dung path do vao day, neu khong reload se doc NHAM file that tren
    dia trong luc test (settings_api._ENV_PATH da monkeypatch nhung ham nay
    truoc day khong nhan path nen van tu doc DOTENV_PATH goc) - xem review
    2026-09-17 (tinh nang hot-reload)."""
    load_dotenv(path, override=True)
    settings.reload()
