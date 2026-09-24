# -*- coding: utf-8 -*-
"""Test trang cau hinh /setup (settings_api.py) - khong dung lifespan/EdgeAgent
that de tranh goi mang, chi test router doc lap voi mot FastAPI app rong."""
import base64

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import edge_collector.config as config
import edge_collector.inbound_api as inbound_api
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


def test_is_same_origin_rejects_when_scheme_mismatched(client):
    """Regression cho bug CSRF false-reject qua reverse-proxy/tunnel (xem
    review 2026-09-17, fix o tang uvicorn startup qua EDGE_FORWARDED_ALLOW_IPS
    - KHONG sua logic _is_same_origin). Mo phong tinh trang TRUOC/CHUA trust
    proxy dung: request.url.scheme van la http (fixture `client` mac dinh
    http://testserver) trong khi Origin tu trinh duyet qua tunnel TLS-terminating
    la https -> phai bi reject (403), dung hanh vi bug da gap thuc te."""
    resp = client.post("/setup", data=_valid_form(),
                        headers={"origin": "https://testserver"})
    assert resp.status_code == 403
    assert "did not originate from the /setup" in resp.text


def test_is_same_origin_accepts_when_scheme_matches(tmp_path, monkeypatch):
    """Cung Origin https nhung request.url.scheme CUNG la https (mo phong SAU
    KHI ProxyHeadersMiddleware rewrite dung scheme, nho EDGE_FORWARDED_ALLOW_IPS
    da duoc cau hinh trust dung peer IP cua proxy/tunnel) -> phai duoc chap
    nhan, khong con 403 CSRF false-reject. Dung TestClient rieng voi base_url
    https:// de request.url.scheme la https (fixture `client` mac dinh
    http://testserver, khong the ep scheme qua header)."""
    monkeypatch.setattr(settings_api, "_ENV_PATH", tmp_path / ".env")
    app = FastAPI()
    app.include_router(settings_api.router)
    https_client = TestClient(app, base_url="https://testserver")

    resp = https_client.post("/setup", data=_valid_form(),
                              headers={"origin": "https://testserver"})

    assert resp.status_code == 200
    assert "Saved" in resp.text


def test_setup_get_lists_forwarded_allow_ips_field(client):
    resp = client.get("/setup")
    assert resp.status_code == 200
    assert 'id="EDGE_FORWARDED_ALLOW_IPS"' in resp.text
    assert "Trusted reverse-proxy IPs" in resp.text
    # EDGE_FORWARDED_ALLOW_IPS nam trong RESTART_REQUIRED_KEYS -> so badge
    # "restart" phai dung bang so field trong tap do (khong thieu, khong thua).
    assert resp.text.count(" restart</span>") == len(config.RESTART_REQUIRED_KEYS)


def test_setup_activity_returns_empty_rows_when_store_missing():
    """FastAPI() tran (khong qua lifespan that, giong pattern test agent=None
    o setup_post) khong co app.state.store -> phai tra rong an toan, KHONG
    crash 500 - xem review 2026-09-17 (panel 'Live activity')."""
    app = FastAPI()
    app.include_router(settings_api.router)
    test_client = TestClient(app)

    resp = test_client.get("/setup/activity")

    assert resp.status_code == 200
    assert resp.json() == {"rows": []}


def test_setup_activity_returns_recent_rows_from_store(tmp_path):
    from edge_collector.store import Store

    store = Store(tmp_path / "activity.db")
    store.history_insert_many([
        ("EDGE-NODE-1", "temp", 1000.0, 21.5, None, 0, 1),
        ("EDGE-NODE-1", "hum", 1001.0, 55.0, None, 0, 1),
    ])
    app = FastAPI()
    app.include_router(settings_api.router)
    app.state.store = store
    test_client = TestClient(app)

    resp = test_client.get("/setup/activity")

    assert resp.status_code == 200
    body = resp.json()
    assert len(body["rows"]) == 2
    assert {row["serial"] for row in body["rows"]} == {"EDGE-NODE-1"}
    assert all(row["age_s"] >= 0 for row in body["rows"])
    # ts lon hon (1001.0, kenh hum) phai dung TRUOC (DESC theo ts).
    assert body["rows"][0]["ch"] == "hum"


def test_setup_get_activity_script_escapes_via_textcontent(client):
    """_ACTIVITY_SCRIPT phai duoc nhung nguyen ven vao HTML /setup - day la co
    che escape client-side DUY NHAT cho du lieu serial/ch/s den tu thiet bi
    NGOAI (node_agent) truoc khi noi vao innerHTML, tranh XSS luu tru qua
    history. Test string-contains don gian la du, khong can headless browser
    - xem review 2026-09-17 (panel 'Live activity')."""
    resp = client.get("/setup")
    assert resp.status_code == 200
    assert "document.createElement('div')" in resp.text
    assert "d.textContent=String(s)" in resp.text


def test_setup_post_returns_clear_error_when_env_write_fails(client, monkeypatch):
    """Regression cho finding docker-reviewer 2026-09-17: container chay
    non-root (uid 1000) co the khong ghi duoc .env bind-mount tu host (vd
    file thuoc so huu root/user khac tren host) - TRUOC KHI fix, OSError tu
    _write_env_file() khong duoc bat -> 500 mac dinh cua FastAPI (traceback
    tho, khong ro nguyen nhan la loi permission chu khong phai bug logic).
    Mo phong bang monkeypatch _write_env_file de raise PermissionError (dang
    con cua OSError) thay vi chmod file that - on dinh hon vi test co the
    chay boi root (bo qua permission bit)."""
    def _raise_permission_error(path, values):
        raise PermissionError("[Errno 13] Permission denied: '%s'" % path)

    monkeypatch.setattr(settings_api, "_write_env_file", _raise_permission_error)

    resp = client.post("/setup", data=_valid_form(),
                        headers={"origin": "http://testserver"})

    assert resp.status_code == 500
    assert "Could not write .env" in resp.text
    assert "Traceback" not in resp.text


def test_validate_rejects_invalid_forwarded_allow_ips(client, tmp_path):
    """Regression cho finding python-reviewer 2026-09-17: truoc day gia tri
    sai chinh ta cua EDGE_FORWARDED_ALLOW_IPS IM LANG khong co tac dung gi
    (uvicorn's _TrustedHosts fail-closed, khong bao gio crash, chi khong bao
    gio khop client that - khong ai biet TAI SAO trust proxy khong hoat dong).
    Gio phai bi tu choi NGAY luc Save (400), KHONG duoc ghi xuong .env."""
    form = _valid_form()
    form["EDGE_FORWARDED_ALLOW_IPS"] = "not-an-ip, 10.0.0.0/8"

    resp = client.post("/setup", data=form)

    assert resp.status_code == 400
    assert "EDGE_FORWARDED_ALLOW_IPS" in resp.text
    assert "Invalid IP/CIDR" in resp.text
    env_path = tmp_path / ".env"
    assert not env_path.exists() or "not-an-ip" not in env_path.read_text()


def test_validate_rejects_invalid_mqtt_consumer_url_scheme(client, tmp_path):
    """Patch MQTT (merge tu production): EDGE_MQTT_CONSUMER_URL sai scheme
    phai bi tu choi ngay luc Save - paho.mqtt.Client._split_url() (mqtt_consumer.py)
    chi biet strip 'mqtt://'/'tcp://', mot URL http:// se bi hieu nham thanh
    host 'http', im lang khong noi duoc broker nao ca."""
    form = _valid_form()
    form["EDGE_MQTT_CONSUMER_URL"] = "http://192.168.5.190:1883"

    resp = client.post("/setup", data=form)

    assert resp.status_code == 400
    assert "EDGE_MQTT_CONSUMER_URL" in resp.text
    assert "mqtt://" in resp.text
    env_path = tmp_path / ".env"
    assert not env_path.exists() or "http://192.168.5.190:1883" not in env_path.read_text()


def test_validate_accepts_mqtt_tcp_and_blank_consumer_url(client):
    for scheme_url in ("mqtt://192.168.5.190:1883", "tcp://broker.local:1883", ""):
        form = _valid_form()
        form["EDGE_MQTT_CONSUMER_URL"] = scheme_url

        resp = client.post("/setup", data=form)

        assert resp.status_code == 200, "gia tri %r phai duoc chap nhan" % scheme_url
        assert "Saved" in resp.text


def test_validate_rejects_mqtts_consumer_url(client):
    """mqtt_consumer._split_url() chi biet strip "mqtt://"/"tcp://" va khong
    goi tls_set() o dau ca - "mqtts://"/"ssl://" se bi parse SAI (host thanh
    chuoi "mqtts" thay vi hostname that) ma khong bao loi gi, nen phai bi
    chan tu luc validate - xem python-reviewer 2026-09-24."""
    form = _valid_form()
    form["EDGE_MQTT_CONSUMER_URL"] = "mqtts://broker.local:8883"

    resp = client.post("/setup", data=form)

    assert resp.status_code == 400
    assert "mqtt:// or tcp://" in resp.text


def test_validate_accepts_wildcard_and_valid_cidr(client):
    """"*" (wildcard - trust MOI proxy, canh bao rieng trong docstring field,
    khong phai loi validate) va danh sach IP/CIDR hop le (co the tron IP don
    le lan CIDR) deu phai duoc chap nhan."""
    form = _valid_form()
    form["EDGE_FORWARDED_ALLOW_IPS"] = "*"
    resp = client.post("/setup", data=form)
    assert resp.status_code == 200
    assert "Saved" in resp.text

    form2 = _valid_form()
    form2["EDGE_FORWARDED_ALLOW_IPS"] = "10.0.0.0/8,192.168.1.1"
    resp2 = client.post("/setup", data=form2)
    assert resp2.status_code == 200
    assert "Saved" in resp2.text


def test_setup_get_wraps_notice_text_in_single_span(client):
    """Regression cho bug CSS Flexbox co san (`.notice{display:flex}` chua
    text-node xen `<b>` khien vo thanh nhieu cot) - toan bo noi dung text
    (ke ca `<b>restart</b>`) phai nam trong DUY NHAT 1 `<span>` de la 1 flex-
    item duy nhat, khong tach rieng."""
    resp = client.get("/setup")
    assert resp.status_code == 200
    assert "<span>Saving applies most changes immediately." in resp.text
    assert "restarted (socket/storage opened once at startup).</span></p>" in resp.text


class _FakeStoreWithApiKey:
    """Store gia lap chi implement kv_get("api_key") - du cho test /setup va
    /setup/api_key doc api_key, khong can Store SQLite that."""

    def kv_get(self, key, default=None):
        return "testkey1234abcd" if key == "api_key" else default  # secret-allow: test fixture, khong phai key that


def test_mask_api_key_none_and_short_and_normal():
    """6 dau cham CO DINH (khong ti le theo do dai key that, tranh lo metadata
    do dai) + 4 ky tu cuoi - xem docstring _mask_api_key()."""
    assert settings_api._mask_api_key(None) is None
    assert settings_api._mask_api_key("") is None

    masked = settings_api._mask_api_key("abcd1234efgh")

    assert masked == "••••••efgh"
    assert masked.count("•") == 6
    assert masked.endswith("efgh")


def test_setup_get_shows_not_yet_received_when_no_api_key():
    """FastAPI() tran (khong co app.state.store, giong pattern test activity
    da co) -> phai hien thong bao "chua nhan duoc", KHONG co nut Copy (khong
    co gi de copy)."""
    app = FastAPI()
    app.include_router(settings_api.router)
    test_client = TestClient(app)

    resp = test_client.get("/setup")

    assert resp.status_code == 200
    assert "Not yet received" in resp.text
    assert 'id="copy-api-key-btn"' not in resp.text


def test_setup_get_shows_masked_key_and_copy_button_when_present():
    """Assertion quan trong nhat: full raw api_key KHONG BAO GIO duoc nhung
    vao HTML /setup (chi masked) - /setup co the truy cap qua domain public
    (tunnel), lo full secret qua view-source/devtools se rat nguy hiem. Chi
    GET /setup/api_key (endpoint rieng, goi luc bam Copy) moi duoc tra raw -
    xem docstring _mask_api_key()/setup_api_key()."""
    app = FastAPI()
    app.include_router(settings_api.router)
    app.state.store = _FakeStoreWithApiKey()
    test_client = TestClient(app)

    resp = test_client.get("/setup")

    assert resp.status_code == 200
    assert "••••••abcd" in resp.text
    assert 'id="copy-api-key-btn"' in resp.text
    assert "testkey1234abcd" not in resp.text


def test_setup_api_key_endpoint_returns_raw_value():
    """GET /setup/api_key la endpoint DUY NHAT duoc phep tra raw value - thiet
    ke co chu dich (goi tu JS luc bam Copy), khong phai thieu sot."""
    app = FastAPI()
    app.include_router(settings_api.router)
    app.state.store = _FakeStoreWithApiKey()
    test_client = TestClient(app)

    resp = test_client.get("/setup/api_key")

    assert resp.status_code == 200
    assert resp.json() == {"api_key": "testkey1234abcd"}  # secret-allow: test fixture


def test_setup_api_key_endpoint_returns_null_when_store_missing():
    app = FastAPI()
    app.include_router(settings_api.router)
    test_client = TestClient(app)

    resp = test_client.get("/setup/api_key")

    assert resp.status_code == 200
    assert resp.json() == {"api_key": None}


def test_setup_get_api_key_script_has_fallback_copy(client):
    """Regression cho bug that: navigator.clipboard.writeText() CHI hoat dong
    trong secure context (HTTPS/localhost) - truy cap /setup qua LAN HTTP
    thuong (vd http://192.168.5.190:8090/setup, threat-model goc cua project)
    khien nut Copy "khong lam gi" vi Clipboard API khong ton tai/bi chan va
    `.catch(function(){})` cu nuot loi im lang. Verify ca 2 nhanh (Clipboard
    API hien dai VA fallback execCommand('copy')) deu co mat nguyen ven trong
    HTML - string-contains don gian, khong chay JS that (headless browser
    ngoai scope pytest, xem 'Scope da KHONG cover' vong truoc)."""
    resp = client.get("/setup")
    assert resp.status_code == 200
    assert "navigator.clipboard && navigator.clipboard.writeText" in resp.text
    assert "document.execCommand('copy')" in resp.text
    assert "function fallbackCopy(text)" in resp.text


def test_setup_get_no_gate_when_token_empty(client):
    """Tuong thich nguoc: EDGE_SETUP_TOKEN mac dinh rong (`config.settings.
    setup_token == ""` - xem conftest.py `_clean_edge_env`/singleton baseline)
    -> KHONG gate gi, deployment cu chua cau hinh token khong duoc regression
    thanh 401 - xem python-reviewer 2026-09-17 (finding lo credential /setup/api_key)."""
    assert config.settings.setup_token == ""
    resp = client.get("/setup")
    assert resp.status_code == 200


def test_setup_requires_basic_auth_when_token_set(client):
    """`config.settings.setup_token` duoc `_restore_settings_singleton`
    (conftest.py, autouse) tu dong khoi phuc sau test nay - khong can fixture
    rieng. Basic Auth: username bat ky, password phai khop token qua
    secrets.compare_digest."""
    config.settings.setup_token = "sekret"  # secret-allow: test fixture

    resp_no_auth = client.get("/setup")
    assert resp_no_auth.status_code == 401
    assert "Basic" in resp_no_auth.headers.get("www-authenticate", "")

    resp_wrong = client.get("/setup", auth=("anyuser", "wrong-password"))
    assert resp_wrong.status_code == 401

    resp_ok = client.get("/setup", auth=("anyuser", "sekret"))
    assert resp_ok.status_code == 200


def test_setup_api_key_endpoint_requires_auth_when_token_set(client):
    """GET /setup/api_key la endpoint driver chinh cua finding (tra RAW
    credential) - phai co test rieng, khong chi dua vao test /setup."""
    config.settings.setup_token = "sekret"  # secret-allow: test fixture

    resp_no_auth = client.get("/setup/api_key")
    assert resp_no_auth.status_code == 401

    resp_wrong = client.get("/setup/api_key", auth=("anyuser", "wrong-password"))
    assert resp_wrong.status_code == 401

    resp_ok = client.get("/setup/api_key", auth=("anyuser", "sekret"))
    assert resp_ok.status_code == 200


def test_setup_post_requires_auth_when_token_set(client):
    """Gate auth phai chay TRUOC _is_same_origin check - thieu auth phai bi
    401 NGAY CA KHI Origin header dung (khong duoc lot qua den buoc CSRF)."""
    config.settings.setup_token = "sekret"  # secret-allow: test fixture

    resp = client.post("/setup", data=_valid_form(),
                        headers={"origin": "http://testserver"})

    assert resp.status_code == 401


def test_mask_api_key_short_key_fully_masked():
    """Regression cho finding Minor cung dot review: key <=8 ky tu -
    str(key)[-4:] tren chuoi ngan se tra ve NGUYEN VEN ca chuoi, lam 'mask' lo
    100% key - gio phai che TOAN BO ("••••••"),
    khong lo bat ky ky tu nao cua key that."""
    masked = settings_api._mask_api_key("abcdefgh")  # 8 ky tu, dung nguong <=8

    assert masked == "••••••"
    for ch in "abcdefgh":
        assert ch not in masked


def test_setup_requires_basic_auth_rejects_non_ascii_password_without_crash(client):
    """Regression cho bug crash that: secrets.compare_digest(str, str) RAISE
    TypeError khi 1 trong 2 chuoi chua ky tu non-ASCII - ai do go dai password
    non-ASCII (khong can biet token that) se lam route tra ve 500 thay vi 401
    (loi lo ra qua stack trace, te hon ca reject binh thuong). Gio phai encode
    utf-8 sang bytes truoc khi so - assertion quan trong nhat la 401, KHONG
    phai 500 - xem python-reviewer 2026-09-17."""
    config.settings.setup_token = "sekret"  # secret-allow: test fixture
    credentials = base64.b64encode("user:héllo".encode("utf-8")).decode("ascii")

    resp = client.get("/setup", headers={"Authorization": "Basic %s" % credentials})

    assert resp.status_code == 401


def test_summarize_pcm_request_for_each_endpoint():
    """Panel 'PCM requests' - doi xung NGUOC CHIEU voi 'Live activity'
    (Odoo Main -> edge, khong phai node_agent -> edge). Verify ca 5 endpoint
    + 2 gia tri cmd khac nhau cho /api/command (co trong _COMMAND_LABELS va
    khong co, de bat regression fallback ve raw cmd)."""
    assert settings_api._summarize_pcm_request(
        {"endpoint": "/api/command", "serial": "EDGE1", "ch": "CH01", "cmd": "zero"}
    ) == "Zero on EDGE1 / CH01"
    assert settings_api._summarize_pcm_request(
        {"endpoint": "/api/command", "serial": "EDGE1", "ch": "CH02", "cmd": "write"}
    ) == "Write value on EDGE1 / CH02"
    assert settings_api._summarize_pcm_request(
        {"endpoint": "/api/latest", "serial": "EDGE1", "ch": "CH02"}
    ) == "Read live value EDGE1 / CH02"
    assert settings_api._summarize_pcm_request(
        {"endpoint": "/api/browse", "source": "opcua-main"}
    ) == "Browse tags on 'opcua-main'"
    assert settings_api._summarize_pcm_request(
        {"endpoint": "/api/source/test", "kind": "modbus"}
    ) == "Connection test (modbus)"
    assert settings_api._summarize_pcm_request(
        {"endpoint": "/api/stats", "serial": "EDGE1", "ch": "CH03", "hours": 24}
    ) == "Channel statistics EDGE1 / CH03 (last 24h)"


def test_setup_pcm_requests_endpoint_returns_rows(client):
    """Module-level deque cua inbound_api.py dung chung ca tien trinh pytest -
    don truoc/sau de khong ro ri sang test khac (cung tinh than
    tests/test_inbound_api.py)."""
    inbound_api._recent_requests.clear()
    try:
        inbound_api._log_request("/api/command", serial="EDGE1", ch="CH01", cmd="zero")
        inbound_api._log_request("/api/source/test", kind="modbus")

        resp = client.get("/setup/pcm_requests")

        assert resp.status_code == 200
        rows = resp.json()["rows"]
        assert len(rows) == 2
        # appendleft - request MOI NHAT (log sau cung, /api/source/test) dung dau.
        assert rows[0]["endpoint"] == "/api/source/test"
        assert rows[0]["summary"] == "Connection test (modbus)"
        assert rows[1]["summary"] == "Zero on EDGE1 / CH01"
        assert all(row["age_s"] >= 0 for row in rows)
    finally:
        inbound_api._recent_requests.clear()


def test_setup_pcm_requests_requires_auth_when_token_set(client):
    """Route thu 5 cung phai duoc gate boi EDGE_SETUP_TOKEN giong 4 route
    /setup/* con lai - tranh sot consistency khi them route moi."""
    config.settings.setup_token = "sekret"  # secret-allow: test fixture

    resp_no_auth = client.get("/setup/pcm_requests")
    assert resp_no_auth.status_code == 401

    resp_ok = client.get("/setup/pcm_requests", auth=("anyuser", "sekret"))
    assert resp_ok.status_code == 200
