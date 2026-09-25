# -*- coding: utf-8 -*-
"""Test route GET "/" (edge_collector/app.py::create_app) - truoc day app chi
mount router /healthz, /setup, /node/v1/*, /ops nen GET "/" tra 404; vua them
redirect "/" -> "/setup" (trang cau hinh) de nguoi vao thang IP:port khong
gap trang trang.

Dung THANG `edge_collector.app.app` (singleton thuc, duoc tao boi
`create_app()` o module-level) thay vi tu dung app toi gian nhu cac test
router khac (test_ops_api.py/test_settings_api.py) - route redirect nay
KHONG co dependency nao (khong dung app.state.agent/store) nen an toan dung
truc tiep. TestClient(app) KHONG `with` se KHONG chay ASGI lifespan that
(da xac nhan boi test_app_logging.py::test_importing_app_module_does_not_configure_logging),
nen se KHONG khoi EdgeAgent / khong ghi log that - chi goi request HTTP binh
thuong vao ASGI app."""
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

    # EDGE_SETUP_TOKEN mac dinh rong trong moi truong test (autouse fixture
    # _clean_edge_env/_restore_settings_singleton o conftest.py) nen /setup
    # khong gate auth - 200 voi HTML trang cau hinh. Diem chinh can xac nhan
    # la KHONG con 404 (bug goc) va URL cuoi cung dung /setup.
    assert resp.status_code == 200
    assert resp.url.path == "/setup"
    assert "text/html" in resp.headers["content-type"]
