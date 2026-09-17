# -*- coding: utf-8 -*-
"""Import settings_api.py keo theo config.py chay load_dotenv() nap .env THAT
cua project vao os.environ (xem review 2026-09-17 muc 2 - fix dung chung
DOTENV_PATH). Cac test hien tai doc gia tri qua _ENV_PATH/monkeypatch nen
khong bi anh huong, nhung test SAU NAY cho config.py/Settings co the vo tinh
doc nham EDGE_* tu .env that thay vi moi truong sach neu khong don truoc."""
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
    """`config.settings` la 1 singleton dung chung ca tien trinh pytest - tinh
    nang hot-reload (config.reload_settings(), goi tu setup_post) mutate
    THUOC TINH cua chinh object nay, nen 1 test POST /setup thanh cong se lam
    lech settings cho MOI test chay SAU no neu khong snapshot/khoi phuc - xem
    review 2026-09-17 (tinh nang hot-reload)."""
    original = copy.copy(_config.settings.__dict__)
    yield
    _config.settings.__dict__.clear()
    _config.settings.__dict__.update(original)
