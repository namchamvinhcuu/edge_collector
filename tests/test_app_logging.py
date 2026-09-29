# -*- coding: utf-8 -*-
"""Test `_configure_logging()` (edge_collector/app.py) - rotating file handler
thêm vào ROOT logger cho Observability by Default (xem review 2026-09-17):
trước đây chỉ có `logging.basicConfig()` in ra console, mất hết log khi
container restart/terminal đóng.

Hàm này được gọi BÊN TRONG `lifespan()` (chỉ chạy khi ASGI startup event
THẬT sự xảy ra, vd `with TestClient(app):` hoặc uvicorn thật) - TRƯỚC ĐÂY
từng gọi ở module-level (dòng cuối app.py), khiến CHỈ import edge_collector.app
(vd pytest collection) đã tự động tạo settings.state_dir/logs/ + ghi file,
ô nhiễm log THẬT của dev khi chạy pytest; đã dời vào lifespan() để fix (xem
python-reviewer 2026-09-17 + test_importing_app_module_does_not_configure_logging
ở dưới - regression cho chính bug này).

Các test GỌI LẠI trực tiếp `app_module._configure_logging()` sau khi
monkeypatch `settings.state_dir` trỏ tới `tmp_path`, xác nhận hàm idempotent
(gọi lại vẫn tạo dir đúng chỗ + add handler đúng path) - KHÔNG dựa vào side-
effect import/lifespan thật. Fixture `_isolated_root_logger` dọn đúng NHỮNG
handler test vừa thêm (không đóng RotatingFileHandler sẽ rò rỉ file
descriptor trỏ tới tmp_path đã bị xóa, ảnh hưởng các test khác chạy sau đọc
`logging.getLogger().handlers`), giống tinh thần fixture
`_restore_settings_singleton` đã có trong conftest.py cho vấn đề global-state-
leak tương tự với `config.settings`.
"""
import logging
from logging.handlers import RotatingFileHandler

import pytest

import edge_collector.app as app_module
import edge_collector.config as config


@pytest.fixture()
def _isolated_root_logger():
    """Snapshot handler của root logger TRƯỚC khi test gọi _configure_logging(),
    remove + close ĐÚNG NHỮNG handler test vừa thêm sau khi test xong."""
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
    assert file_handlers, "Kỳ vọng ít nhất 1 RotatingFileHandler được thêm vào root logger"
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
    đã được DỜI vào BÊN TRONG lifespan() (ngay trước `agent = EdgeAgent()`),
    KHÔNG còn gọi ở module-level như trước (dòng cuối app.py cùng lúc với
    `app = create_app()`) - trước fix, CHỈ import edge_collector.app (vd
    pytest collection, hoặc bất kỳ tool nào chỉ đọc module) đã tự động tạo
    settings.state_dir/logs/ + ghi file, ô nhiễm log THẬT của dev khi chạy
    pytest (đã tái hiện thật bởi python-reviewer: mtime/size file log thật
    không đổi qua 2 lần chạy pytest liên tiếp SAU fix).

    Bước kiểm: `importlib.reload(app_module)` chạy lại TOÀN BỘ code top-level
    của app.py (bao gồm `app = create_app()`) với settings.state_dir trỏ tới
    tmp_path - `FastAPI(lifespan=lifespan)` (trong create_app()) KHÔNG tự
    động gọi hàm lifespan (lifespan chỉ chạy khi có ASGI startup event THẬT
    sự, vd `with TestClient(app):` hoặc uvicorn thật), nên nếu fix đúng,
    reload phải KHÔNG tạo thư mục logs/ và KHÔNG thêm handler nào vào root
    logger."""
    import importlib

    monkeypatch.setattr(config.settings, "state_dir", tmp_path)
    handlers_before = list(_isolated_root_logger.handlers)

    importlib.reload(app_module)

    assert not (tmp_path / "logs").exists()
    assert list(_isolated_root_logger.handlers) == handlers_before
