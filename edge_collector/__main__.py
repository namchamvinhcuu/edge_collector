# -*- coding: utf-8 -*-
import uvicorn

from .config import settings

if __name__ == "__main__":
    # CHỈ 1 worker (không truyền workers=) - tính năng hot-reload của /setup
    # (settings_api.py setup_post -> config.reload_settings()) giả định 1
    # process/1 singleton `settings` duy nhất. Bật multi-worker sẽ khiến mỗi
    # worker giữ 1 bản `settings` riêng lệch nhau, và _write_env_file() (đọc-
    # sửa-ghi .env, không flock) có thể mất-update nếu 2 worker ghi đồng
    # thời - xem review 2026-09-17.
    # log_config=None: tắt dictConfig riêng của uvicorn (mặc định đặt
    # "uvicorn"/"uvicorn.access" propagate=False) để log khởi động/access
    # cùng chạy vào rotating file handler định nghĩa ở app.py._configure_logging()
    # thay vì chỉ ra riêng console - xem review 2026-09-17 (Observability by
    # Default, rotate logger).
    uvicorn.run("edge_collector.app:app", host=settings.listen_host, port=settings.listen_port,
                forwarded_allow_ips=settings.forwarded_allow_ips, log_config=None, reload=False)
