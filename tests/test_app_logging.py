# -*- coding: utf-8 -*-
"""Test `_configure_logging()` (edge_collector/app.py) - rotating file handler
them vao ROOT logger cho Observability by Default (xem review 2026-09-17):
truoc day chi co `logging.basicConfig()` in ra console, mat het log khi
container restart/terminal dong.

Ham nay duoc goi BEN TRONG `lifespan()` (chi chay khi ASGI startup event
THAT su xay ra, vd `with TestClient(app):` hoac uvicorn that) - TRUOC DAY
tung goi o module-level (dong cuoi app.py), khien CHI import edge_collector.app
(vd pytest collection) da tu dong tao settings.state_dir/logs/ + ghi file,
o nhiem log THAT cua dev khi chay pytest; da doi vao lifespan() de fix (xem
python-reviewer 2026-09-17 + test_importing_app_module_does_not_configure_logging
o duoi - regression cho chinh bug nay).

Cac test GOI LAI truc tiep `app_module._configure_logging()` sau khi
monkeypatch `settings.state_dir` tro toi `tmp_path`, xac nhan ham idempotent
(goi lai van tao dir dung cho + add handler dung path) - KHONG dua vao side-
effect import/lifespan that. Fixture `_isolated_root_logger` don dung NHUNG
handler test vua them (khong dong RotatingFileHandler se ro ri file
descriptor tro toi tmp_path da bi xoa, anh huong cac test khac chay sau doc
`logging.getLogger().handlers`), giong tinh than fixture
`_restore_settings_singleton` da co trong conftest.py cho van de global-state-
leak tuong tu voi `config.settings`.
"""
import logging
from logging.handlers import RotatingFileHandler

import pytest

import edge_collector.app as app_module
import edge_collector.config as config


@pytest.fixture()
def _isolated_root_logger():
    """Snapshot handler cua root logger TRUOC khi test goi _configure_logging(),
    remove + close DUNG NHUNG handler test vua them sau khi test xong."""
    root = logging.getLogger()
    original = list(root.handlers)
    yield root
    for handler in list(root.handlers):
        if handler not in original:
            root.removeHandler(handler)
            handler.close()


def test_configure_logging_creates_log_dir_and_rotating_handler(
    tmp_path, monkeypatch, _isolated_root_logger
):
    monkeypatch.setattr(config.settings, "state_dir", tmp_path)

    app_module._configure_logging()

    assert (tmp_path / "logs").is_dir()

    file_handlers = [
        h for h in _isolated_root_logger.handlers if isinstance(h, RotatingFileHandler)
    ]
    assert file_handlers, "Ky vong it nhat 1 RotatingFileHandler duoc them vao root logger"
    handler = file_handlers[-1]
    assert handler.baseFilename == str((tmp_path / "logs" / "edge_collector.log").resolve())
    assert handler.maxBytes == app_module._LOG_MAX_BYTES
    assert handler.backupCount == app_module._LOG_BACKUP_COUNT


def test_rotating_handler_actually_writes_log_file(tmp_path, monkeypatch, _isolated_root_logger):
    monkeypatch.setattr(config.settings, "state_dir", tmp_path)
    app_module._configure_logging()

    logger = logging.getLogger("edge.test_x")
    logger.info("hello-from-regression-test-marker-987")

    log_file = tmp_path / "logs" / "edge_collector.log"
    assert log_file.is_file()
    content = log_file.read_text(encoding="utf-8")
    assert "hello-from-regression-test-marker-987" in content


def test_importing_app_module_does_not_configure_logging(tmp_path, monkeypatch, _isolated_root_logger):
    """Regression cho finding python-reviewer 2026-09-17: _configure_logging()
    da duoc DOI vao BEN TRONG lifespan() (ngay truoc `agent = EdgeAgent()`),
    KHONG con goi o module-level nhu truoc (dong cuoi app.py cung luc voi
    `app = create_app()`) - truoc fix, CHI import edge_collector.app (vd
    pytest collection, hoac bat ky tool nao chi doc module) da tu dong tao
    settings.state_dir/logs/ + ghi file, o nhiem log THAT cua dev khi chay
    pytest (da tai hien that boi python-reviewer: mtime/size file log that
    khong doi qua 2 lan chay pytest lien tiep SAU fix).

    Buoc kiem: `importlib.reload(app_module)` chay lai TOAN BO code top-level
    cua app.py (bao gom `app = create_app()`) voi settings.state_dir tro toi
    tmp_path - `FastAPI(lifespan=lifespan)` (trong create_app()) KHONG tu
    dong goi ham lifespan (lifespan chi chay khi co ASGI startup event THAT
    su, vd `with TestClient(app):` hoac uvicorn that), nen neu fix dung,
    reload phai KHONG tao thu muc logs/ va KHONG them handler nao vao root
    logger."""
    import importlib

    monkeypatch.setattr(config.settings, "state_dir", tmp_path)
    handlers_before = list(_isolated_root_logger.handlers)

    importlib.reload(app_module)

    assert not (tmp_path / "logs").exists()
    assert list(_isolated_root_logger.handlers) == handlers_before
