# -*- coding: utf-8 -*-
import uvicorn

from .config import settings

if __name__ == "__main__":
    # CHI 1 worker (khong truyen workers=) - tinh nang hot-reload cua /setup
    # (settings_api.py setup_post -> config.reload_settings()) gia dinh 1
    # process/1 singleton `settings` duy nhat. Bat multi-worker se khien moi
    # worker giu 1 ban `settings` rieng lech nhau, va _write_env_file() (doc-
    # sua-ghi .env, khong flock) co the mat-update neu 2 worker ghi dong
    # thoi - xem review 2026-09-17.
    # log_config=None: tat dictConfig rieng cua uvicorn (mac dinh dat
    # "uvicorn"/"uvicorn.access" propagate=False) de log khoi dong/access
    # cung chay vao rotating file handler dinh nghia o app.py._configure_logging()
    # thay vi chi ra rieng console - xem review 2026-09-17 (Observability by
    # Default, rotate logger).
    uvicorn.run("edge_collector.app:app", host=settings.listen_host, port=settings.listen_port,
                forwarded_allow_ips=settings.forwarded_allow_ips, log_config=None, reload=False)
