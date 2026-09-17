# -*- coding: utf-8 -*-
"""Test trang cau hinh /setup (settings_api.py) - khong dung lifespan/EdgeAgent
that de tranh goi mang, chi test router doc lap voi mot FastAPI app rong."""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import edge_collector.config as config
import edge_collector.settings_api as settings_api


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(settings_api, "_ENV_PATH", tmp_path / ".env")
    app = FastAPI()
    app.include_router(settings_api.router)
    return TestClient(app)


def _valid_form():
    return {
        "EDGE_MAIN_URL": "http://odoo-main.local:8069",
        "EDGE_CODE": "EDGE-LINE1",
        "EDGE_NAME": "Edge LINE1",
        "EDGE_PLATFORM": "ubuntu",
        "EDGE_BASE_URL": "http://10.10.1.50:8000",
        "EDGE_LISTEN_HOST": "0.0.0.0",
        "EDGE_LISTEN_PORT": "8000",
        "EDGE_STATE_DIR": "./var",
        "EDGE_HELLO_INTERVAL_S": "30",
        "EDGE_HEARTBEAT_INTERVAL_S": "30",
        "EDGE_CONFIG_POLL_INTERVAL_S": "30",
        "EDGE_PRINT_POLL_INTERVAL_S": "3",
        "EDGE_SUBMIT_INTERVAL_S": "2",
        "EDGE_CONFIG_DEBOUNCE_S": "10",
    }


def test_get_setup_renders_defaults_when_no_env_file(client):
    resp = client.get("/setup")
    assert resp.status_code == 200
    assert "EDGE_MAIN_URL" in resp.text
    assert "http://localhost:8069" in resp.text


def test_post_setup_saves_and_persists_to_env_file(client, tmp_path):
    resp = client.post("/setup", data=_valid_form())
    assert resp.status_code == 200
    assert "Saved" in resp.text

    env_path = tmp_path / ".env"
    assert env_path.exists()
    content = env_path.read_text()
    assert "EDGE_MAIN_URL='http://odoo-main.local:8069'" in content or \
        "EDGE_MAIN_URL=http://odoo-main.local:8069" in content
    assert "EDGE_LISTEN_PORT=8000" in content

    resp2 = client.get("/setup")
    assert "http://odoo-main.local:8069" in resp2.text


def test_write_env_file_drops_stale_duplicate_key(tmp_path):
    """Neu .env san co key TRUNG LAP (vd sua tay/loi tu truoc), dotenv doc lai
    theo kieu 'dong sau de dong truoc' - chi thay dong DAU khop se de gia
    tri MOI bi vo hieu am tham boi dong CU con sot lai phia duoi. Xem review
    2026-09-17 (repro that boi security-reviewer)."""
    from dotenv.main import dotenv_values

    import edge_collector.settings_api as settings_api

    env_path = tmp_path / ".env"
    env_path.write_text("EDGE_CODE=OLD_FIRST\nEDGE_NAME=foo\nEDGE_CODE=OLD_SECOND\n")

    settings_api._write_env_file(env_path, {"EDGE_CODE": "NEW_VALUE"})

    content = env_path.read_text()
    assert content.count("EDGE_CODE") == 1
    assert dotenv_values(str(env_path))["EDGE_CODE"] == "NEW_VALUE"


def test_post_setup_writes_in_place_does_not_replace_inode(client, tmp_path):
    """Docker bind-mount 1 file .env rieng -> os.replace() (tempfile+rename cua
    python-dotenv set_key) crash 'Device or resource busy' vi khong swap duoc
    inode dang bi mount - xem review 2026-09-17. Fix phai ghi TAI CHO (truncate)
    - inode truoc/sau save phai la MOT, khong duoc tao file moi roi rename de."""
    env_path = tmp_path / ".env"
    env_path.write_text("EDGE_CODE=OLD\n")
    inode_before = env_path.stat().st_ino

    resp = client.post("/setup", data=_valid_form())
    assert resp.status_code == 200

    inode_after = env_path.stat().st_ino
    assert inode_before == inode_after


def test_post_setup_rejects_invalid_main_url(client):
    form = _valid_form()
    form["EDGE_MAIN_URL"] = "not-a-url"
    resp = client.post("/setup", data=form)
    assert resp.status_code == 400
    assert "Odoo Main URL" in resp.text
    # field-level UI: error summary linked to the field, invalid input marked
    # for styling + aria-invalid, and an inline message right under the field.
    assert '<a href="#EDGE_MAIN_URL">' in resp.text
    assert 'id="EDGE_MAIN_URL"' in resp.text and 'class="invalid"' in resp.text
    assert 'aria-invalid="true"' in resp.text
    assert 'id="EDGE_MAIN_URL-error"' in resp.text


def test_post_setup_rejects_out_of_range_port(client):
    form = _valid_form()
    form["EDGE_LISTEN_PORT"] = "70000"
    resp = client.post("/setup", data=form)
    assert resp.status_code == 400
    assert "1-65535" in resp.text


def test_post_setup_rejects_non_numeric_interval(client):
    form = _valid_form()
    form["EDGE_HELLO_INTERVAL_S"] = "abc"
    resp = client.post("/setup", data=form)
    assert resp.status_code == 400


def test_post_setup_rejects_empty_state_dir(client):
    form = _valid_form()
    form["EDGE_STATE_DIR"] = "  "
    resp = client.post("/setup", data=form)
    assert resp.status_code == 400


def test_post_setup_rejects_newline_injection(client, tmp_path):
    form = _valid_form()
    form["EDGE_NAME"] = "Edge LINE1\nMALICIOUS_KEY=malicious_value"
    resp = client.post("/setup", data=form)
    assert resp.status_code == 400
    env_path = tmp_path / ".env"
    assert not env_path.exists() or "MALICIOUS_KEY" not in env_path.read_text()


def test_form_values_are_html_escaped(client):
    form = _valid_form()
    form["EDGE_NAME"] = "<script>alert(1)</script>"
    resp = client.post("/setup", data=form)
    assert resp.status_code == 200
    assert "<script>alert(1)</script>" not in resp.text
    assert "&lt;script&gt;" in resp.text


def test_post_setup_preserves_value_containing_hash(client, tmp_path):
    """quote_mode='never' cu tung lam dotenv coi '# ...' la comment va cat cut
    gia tri khi doc lai - xem review 2026-09-17."""
    form = _valid_form()
    form["EDGE_NAME"] = "Line #5 test"
    resp = client.post("/setup", data=form)
    assert resp.status_code == 200

    resp2 = client.get("/setup")
    assert "Line #5 test" in resp2.text


def test_post_setup_rejects_nan_interval(client):
    form = _valid_form()
    form["EDGE_SUBMIT_INTERVAL_S"] = "nan"
    resp = client.post("/setup", data=form)
    assert resp.status_code == 400


def test_post_setup_rejects_inf_interval(client):
    form = _valid_form()
    form["EDGE_SUBMIT_INTERVAL_S"] = "inf"
    resp = client.post("/setup", data=form)
    assert resp.status_code == 400


def test_post_setup_rejects_cross_origin_request(client, tmp_path):
    resp = client.post("/setup", data=_valid_form(),
                        headers={"origin": "http://evil.example"})
    assert resp.status_code == 403
    assert not (tmp_path / ".env").exists()


def test_post_setup_accepts_same_origin_request(client):
    resp = client.post("/setup", data=_valid_form(),
                        headers={"origin": "http://testserver"})
    assert resp.status_code == 200
    assert "Saved" in resp.text


def test_post_setup_hot_reloads_main_url_without_restart(client):
    """EDGE_MAIN_URL khong nam trong config.RESTART_REQUIRED_KEYS - phai co
    hieu luc NGAY tren singleton `settings` sau khi Save, khong can restart
    process. (config.settings duoc _restore_settings_singleton trong
    conftest.py tu dong khoi phuc sau moi test.)"""
    form = _valid_form()
    form["EDGE_MAIN_URL"] = "https://new-odoo.example"
    resp = client.post("/setup", data=form)
    assert resp.status_code == 200
    assert config.settings.main_url == "https://new-odoo.example"


def test_post_setup_blank_edge_code_keeps_currently_active_value(client, tmp_path):
    """De trong EDGE_CODE = 'giu nguyen' (dung UX hint), KHONG duoc ghi rong
    xuong .env - neu khong, restart THAT sau nay se tu sinh edge_code MOI
    (Settings.__init__) khac han code dang chay, mat khop voi pcm.edge.code
    da dang ky ben Odoo. Xem review 2026-09-17 (hot-reload) finding Critical."""
    config.settings.edge_code = "EDGE-ALREADY-REGISTERED"
    form = _valid_form()
    form["EDGE_CODE"] = ""
    resp = client.post("/setup", data=form)
    assert resp.status_code == 200

    content = (tmp_path / ".env").read_text()
    assert "EDGE-ALREADY-REGISTERED" in content
    assert "EDGE_CODE=\n" not in content and "EDGE_CODE=''\n" not in content
    assert config.settings.edge_code == "EDGE-ALREADY-REGISTERED"


def test_post_setup_hot_reloads_timing_interval_without_restart(client):
    form = _valid_form()
    form["EDGE_HELLO_INTERVAL_S"] = "45"
    resp = client.post("/setup", data=form)
    assert resp.status_code == 200
    assert config.settings.hello_interval_s == 45


def test_post_setup_does_not_hot_reload_restart_required_fields(client):
    """EDGE_LISTEN_PORT nam trong RESTART_REQUIRED_KEYS - Settings.reload()
    KHONG duoc dung toi field nay (socket da bind co dinh luc startup, cap
    nhat gia tri trong object cung khong co tac dung gi, chi de gay hieu lam)."""
    original_port = config.settings.listen_port
    form = _valid_form()
    form["EDGE_LISTEN_PORT"] = "9999"
    resp = client.post("/setup", data=form)
    assert resp.status_code == 200
    assert config.settings.listen_port == original_port


def test_post_setup_marks_exactly_restart_required_fields_in_ui(client):
    resp = client.post("/setup", data=_valid_form())
    assert resp.status_code == 200
    assert resp.text.count(" restart</span>") == len(config.RESTART_REQUIRED_KEYS)
    for key in config.RESTART_REQUIRED_KEYS:
        assert ('id="%s"' % key) in resp.text


def test_post_setup_calls_agent_refresh_base_url_when_present(tmp_path, monkeypatch):
    """/setup chi ghi settings.main_url - httpx.AsyncClient cua OdooClient
    dang chay bake base_url luc __init__, phai duoc goi refresh_base_url()
    tuong minh moi thay doi that su ap dung cho request TIEP THEO."""
    monkeypatch.setattr(settings_api, "_ENV_PATH", tmp_path / ".env")
    app = FastAPI()
    app.include_router(settings_api.router)

    calls = []

    class FakeOdoo:
        def refresh_base_url(self):
            calls.append(True)

    class FakeAgent:
        odoo = FakeOdoo()

    app.state.agent = FakeAgent()
    test_client = TestClient(app)

    resp = test_client.post("/setup", data=_valid_form())
    assert resp.status_code == 200
    assert calls == [True]


def test_post_setup_skips_agent_refresh_when_absent(client):
    """App test chuan (fixture `client`) khong gan app.state.agent - setup_post
    phai bo qua an toan (getattr default None), khong duoc crash 500."""
    resp = client.post("/setup", data=_valid_form())
    assert resp.status_code == 200
