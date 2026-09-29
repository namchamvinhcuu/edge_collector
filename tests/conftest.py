# -*- coding: utf-8 -*-
"""Import settings_api.py kéo theo config.py chạy load_dotenv() nạp .env THẬT
của project vào os.environ (xem review 2026-09-17 mục 2 - fix dùng chung
DOTENV_PATH). Các test hiện tại đọc giá trị qua _ENV_PATH/monkeypatch nên
không bị ảnh hưởng, nhưng test SAU NÀY cho config.py/Settings có thể vô tình
đọc nhầm EDGE_* từ .env thật thay vì môi trường sạch nếu không dọn trước."""
import copy
import os

import pytest

import edge_collector.config as _config


@pytest.fixture(autouse=True)
def _clean_edge_env(monkeypatch):
    for key in list(os.environ):
        if key.startswith("EDGE_"):
            monkeypatch.delenv(key, raising=False)


@pytest.fixture(autouse=True)
def _restore_settings_singleton():
    """`config.settings` là 1 singleton dùng chung cả tiến trình pytest - tính
    năng hot-reload (config.reload_settings(), gọi từ setup_post) mutate
    THUỘC TÍNH của chính object này, nên 1 test POST /setup thành công sẽ làm
    lệch settings cho MỌI test chạy SAU nó nếu không snapshot/khôi phục - xem
    review 2026-09-17 (tính năng hot-reload)."""
    original = copy.copy(_config.settings.__dict__)
    yield
    _config.settings.__dict__.clear()
    _config.settings.__dict__.update(original)
