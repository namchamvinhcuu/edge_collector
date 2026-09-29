# -*- coding: utf-8 -*-
"""Test route GET "/" (edge_collector/app.py::create_app) - trước đây app chỉ
mount router /healthz, /setup, /node/v1/*, /ops nên GET "/" trả 404; vừa thêm
redirect "/" -> "/setup" (trang cấu hình) để người vào thẳng IP:port không
gặp trang trắng.

Dùng THẲNG `edge_collector.app.app` (singleton thực, được tạo bởi
`create_app()` ở module-level) thay vì tự dựng app tối giản như các test
router khác (test_ops_api.py/test_settings_api.py) - route redirect này
KHÔNG có dependency nào (không dùng app.state.agent/store) nên an toàn dùng
trực tiếp. TestClient(app) KHÔNG `with` sẽ KHÔNG chạy ASGI lifespan thật
(đã xác nhận bởi test_app_logging.py::test_importing_app_module_does_not_configure_logging),
nên sẽ KHÔNG khởi EdgeAgent / không ghi log thật - chỉ gọi request HTTP bình
thường vào ASGI app."""
from fastapi.testclient import TestClient

import edge_collector.app as app_module


def _client():
    return TestClient(app_module.app)


def test_root_redirects_to_setup_without_following_redirect():
    resp = _client().get("/", follow_redirects=False)

    assert resp.status_code == 307
    assert resp.headers["location"] == "/setup"


def test_root_redirect_followed_lands_on_setup_page_not_404():
    resp = _client().get("/")

    # EDGE_SETUP_TOKEN mặc định rỗng trong môi trường test (autouse fixture
    # _clean_edge_env/_restore_settings_singleton ở conftest.py) nên /setup
    # không gate auth - 200 với HTML trang cấu hình. Điểm chính cần xác nhận
    # là KHÔNG còn 404 (bug gốc) và URL cuối cùng đúng /setup.
    assert resp.status_code == 200
    assert resp.url.path == "/setup"
    assert "text/html" in resp.headers["content-type"]
