# -*- coding: utf-8 -*-
"""Test trang cấu hình /setup (settings_api.py) - không dùng lifespan/EdgeAgent
thật để tránh gọi mạng, chỉ test router độc lập với một FastAPI app rỗng."""
import base64
import html
import re

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


def _extract_fingerprint(html_text):
    """Lấy giá trị hidden input _env_fingerprint từ HTML trả về bởi GET /setup
    (xem _render()) - dùng để mô phỏng 1 tab trình duyệt đang giữ form."""
    m = re.search(r'name="_env_fingerprint" value="([^"]*)"', html_text)
    assert m is not None, "khong tim thay hidden input _env_fingerprint trong HTML"
    return m.group(1)


def _extract_rendered_form_values(html_text):
    """Đọc lại giá trị ĐANG HIỂN THỊ trên 1 trang HTML đã render bởi _render()
    cho từng field trong _FIELDS - mô phỏng ĐÚNG những gì 1 trình duyệt thật
    sẽ GỬI LÊN nếu bấm Save mà KHÔNG sửa field nào, bất kể trang đó đang hiển
    thị `values` (data POST cũ, trước fix) hay `_current_values()` (file thật
    trên đĩa, sau fix). KHÔNG hardcode _current_values() trong test - làm vậy
    sẽ không phân biệt được buggy/fixed vì cả 2 đều tính fingerprint MỚI giống
    hệt nhau, chỉ khác ở VALUE hiển thị cho từng field."""
    result = {}
    for f in settings_api._FIELDS:
        key = f["key"]
        m = re.search(r'id="%s" name="%s" value="([^"]*)"' % (re.escape(key), re.escape(key)),
                      html_text)
        assert m is not None, "khong tim thay input field %s trong HTML" % key
        result[key] = html.unescape(m.group(1))
    return result


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
    """Nếu .env sẵn có key TRÙNG LẶP (vd sửa tay/lỗi từ trước), dotenv đọc lại
    theo kiểu 'dòng sau đè dòng trước' - chỉ thay dòng ĐẦU khớp sẽ để giá
    trị MỚI bị vô hiệu âm thầm bởi dòng CŨ còn sót lại phía dưới. Xem review
    2026-09-17 (repro thật bởi security-reviewer)."""
    from dotenv.main import dotenv_values

    import edge_collector.settings_api as settings_api

    env_path = tmp_path / ".env"
    env_path.write_text("EDGE_CODE=OLD_FIRST\nEDGE_NAME=foo\nEDGE_CODE=OLD_SECOND\n")

    settings_api._write_env_file(env_path, {"EDGE_CODE": "NEW_VALUE"})

    content = env_path.read_text()
    assert content.count("EDGE_CODE") == 1
    assert dotenv_values(str(env_path))["EDGE_CODE"] == "NEW_VALUE"


def test_post_setup_writes_in_place_does_not_replace_inode(client, tmp_path):
    """Docker bind-mount 1 file .env riêng -> os.replace() (tempfile+rename của
    python-dotenv set_key) crash 'Device or resource busy' vì không swap được
    inode đang bị mount - xem review 2026-09-17. Fix phải ghi TẠI CHỖ (truncate)
    - inode trước/sau save phải là MỘT, không được tạo file mới rồi rename đè."""
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
    """quote_mode='never' cũ từng làm dotenv coi '# ...' là comment và cắt cụt
    giá trị khi đọc lại - xem review 2026-09-17."""
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
    """EDGE_MAIN_URL không nằm trong config.RESTART_REQUIRED_KEYS - phải có
    hiệu lực NGAY trên singleton `settings` sau khi Save, không cần restart
    process. (config.settings được _restore_settings_singleton trong
    conftest.py tự động khôi phục sau mỗi test.)"""
    form = _valid_form()
    form["EDGE_MAIN_URL"] = "https://new-odoo.example"
    resp = client.post("/setup", data=form)
    assert resp.status_code == 200
    assert config.settings.main_url == "https://new-odoo.example"


def test_post_setup_blank_edge_code_keeps_currently_active_value(client, tmp_path):
    """Để trống EDGE_CODE = 'giữ nguyên' (đúng UX hint), KHÔNG được ghi rỗng
    xuống .env - nếu không, restart THẬT sau này sẽ tự sinh edge_code MỚI
    (Settings.__init__) khác hẳn code đang chạy, mất khớp với pcm.edge.code
    đã đăng ký bên Odoo. Xem review 2026-09-17 (hot-reload) finding Critical."""
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
    """EDGE_LISTEN_PORT nằm trong RESTART_REQUIRED_KEYS - Settings.reload()
    KHÔNG được đụng tới field này (socket đã bind cố định lúc startup, cập
    nhật giá trị trong object cũng không có tác dụng gì, chỉ để gây hiểu lầm)."""
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
    """/setup chỉ ghi settings.main_url - httpx.AsyncClient của OdooClient
    đang chạy bake base_url lúc __init__, phải được gọi refresh_base_url()
    tường minh mới thay đổi thật sự áp dụng cho request TIẾP THEO."""
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
    """App test chuẩn (fixture `client`) không gán app.state.agent - setup_post
    phải bỏ qua an toàn (getattr default None), không được crash 500."""
    resp = client.post("/setup", data=_valid_form())
    assert resp.status_code == 200


def test_is_same_origin_rejects_when_scheme_mismatched(client):
    """Regression cho bug CSRF false-reject qua reverse-proxy/tunnel (xem
    review 2026-09-17, fix ở tầng uvicorn startup qua EDGE_FORWARDED_ALLOW_IPS
    - KHÔNG sửa logic _is_same_origin). Mô phỏng tình trạng TRƯỚC/CHƯA trust
    proxy đúng: request.url.scheme vẫn là http (fixture `client` mặc định
    http://testserver) trong khi Origin từ trình duyệt qua tunnel TLS-terminating
    là https -> phải bị reject (403), đúng hành vi bug đã gặp thực tế."""
    resp = client.post("/setup", data=_valid_form(),
                        headers={"origin": "https://testserver"})
    assert resp.status_code == 403
    assert "did not originate from the /setup" in resp.text


def test_is_same_origin_accepts_when_scheme_matches(tmp_path, monkeypatch):
    """Cùng Origin https nhưng request.url.scheme CŨNG là https (mô phỏng SAU
    KHI ProxyHeadersMiddleware rewrite đúng scheme, nhờ EDGE_FORWARDED_ALLOW_IPS
    đã được cấu hình trust đúng peer IP của proxy/tunnel) -> phải được chấp
    nhận, không còn 403 CSRF false-reject. Dùng TestClient riêng với base_url
    https:// để request.url.scheme là https (fixture `client` mặc định
    http://testserver, không thể ép scheme qua header)."""
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
    # EDGE_FORWARDED_ALLOW_IPS nằm trong RESTART_REQUIRED_KEYS -> số badge
    # "restart" phải đúng bằng số field trong tập đó (không thiếu, không thừa).
    assert resp.text.count(" restart</span>") == len(config.RESTART_REQUIRED_KEYS)


def test_setup_activity_returns_empty_rows_when_store_missing():
    """FastAPI() trần (không qua lifespan thật, giống pattern test agent=None
    ở setup_post) không có app.state.store -> phải trả rỗng an toàn, KHÔNG
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
    # ts lớn hơn (1001.0, kênh hum) phải đứng TRƯỚC (DESC theo ts).
    assert body["rows"][0]["ch"] == "hum"


def test_setup_get_activity_script_escapes_via_textcontent(client):
    """_ACTIVITY_SCRIPT phải được nhúng nguyên vẹn vào HTML /setup - đây là cơ
    chế escape client-side DUY NHẤT cho dữ liệu serial/ch/s đến từ thiết bị
    NGOÀI (node_agent) trước khi nối vào innerHTML, tránh XSS lưu trữ qua
    history. Test string-contains đơn giản là đủ, không cần headless browser
    - xem review 2026-09-17 (panel 'Live activity')."""
    resp = client.get("/setup")
    assert resp.status_code == 200
    assert "document.createElement('div')" in resp.text
    assert "d.textContent=String(s)" in resp.text


def test_setup_post_returns_clear_error_when_env_write_fails(client, monkeypatch):
    """Regression cho finding docker-reviewer 2026-09-17: container chạy
    non-root (uid 1000) có thể không ghi được .env bind-mount từ host (vd
    file thuộc sở hữu root/user khác trên host) - TRƯỚC KHI fix, OSError từ
    _write_env_file() không được bắt -> 500 mặc định của FastAPI (traceback
    thô, không rõ nguyên nhân là lỗi permission chứ không phải bug logic).
    Mô phỏng bằng monkeypatch _write_env_file để raise PermissionError (dạng
    con của OSError) thay vì chmod file thật - ổn định hơn vì test có thể
    chạy bởi root (bỏ qua permission bit)."""
    def _raise_permission_error(path, values):
        raise PermissionError("[Errno 13] Permission denied: '%s'" % path)

    monkeypatch.setattr(settings_api, "_write_env_file", _raise_permission_error)

    resp = client.post("/setup", data=_valid_form(),
                        headers={"origin": "http://testserver"})

    assert resp.status_code == 500
    assert "Could not write .env" in resp.text
    assert "Traceback" not in resp.text


def test_validate_rejects_invalid_forwarded_allow_ips(client, tmp_path):
    """Regression cho finding python-reviewer 2026-09-17: trước đây giá trị
    sai chính tả của EDGE_FORWARDED_ALLOW_IPS IM LẶNG không có tác dụng gì
    (uvicorn's _TrustedHosts fail-closed, không bao giờ crash, chỉ không bao
    giờ khớp client thật - không ai biết TẠI SAO trust proxy không hoạt động).
    Giờ phải bị từ chối NGAY lúc Save (400), KHÔNG được ghi xuống .env."""
    form = _valid_form()
    form["EDGE_FORWARDED_ALLOW_IPS"] = "not-an-ip, 10.0.0.0/8"

    resp = client.post("/setup", data=form)

    assert resp.status_code == 400
    assert "EDGE_FORWARDED_ALLOW_IPS" in resp.text
    assert "Invalid IP/CIDR" in resp.text
    env_path = tmp_path / ".env"
    assert not env_path.exists() or "not-an-ip" not in env_path.read_text()


def test_validate_rejects_invalid_mqtt_consumer_url_scheme(client, tmp_path):
    """Patch MQTT (merge từ production): EDGE_MQTT_CONSUMER_URL sai scheme
    phải bị từ chối ngay lúc Save - paho.mqtt.Client._split_url() (mqtt_consumer.py)
    chỉ biết strip 'mqtt://'/'tcp://', một URL http:// sẽ bị hiểu nhầm thành
    host 'http', im lặng không nối được broker nào cả."""
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
    """mqtt_consumer._split_url() chỉ biết strip "mqtt://"/"tcp://" và không
    gọi tls_set() ở đâu cả - "mqtts://"/"ssl://" sẽ bị parse SAI (host thành
    chuỗi "mqtts" thay vì hostname thật) mà không báo lỗi gì, nên phải bị
    chặn từ lúc validate - xem python-reviewer 2026-09-24."""
    form = _valid_form()
    form["EDGE_MQTT_CONSUMER_URL"] = "mqtts://broker.local:8883"

    resp = client.post("/setup", data=form)

    assert resp.status_code == 400
    assert "mqtt:// or tcp://" in resp.text


def test_validate_accepts_wildcard_and_valid_cidr(client):
    """"*" (wildcard - trust MỌI proxy, cảnh báo riêng trong docstring field,
    không phải lỗi validate) và danh sách IP/CIDR hợp lệ (có thể trộn IP đơn
    lẻ lẫn CIDR) đều phải được chấp nhận."""
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
    """Regression cho bug CSS Flexbox có sẵn (`.notice{display:flex}` chứa
    text-node xen `<b>` khiến vỡ thành nhiều cột) - toàn bộ nội dung text
    (kể cả `<b>restart</b>`) phải nằm trong DUY NHẤT 1 `<span>` để là 1 flex-
    item duy nhất, không tách riêng."""
    resp = client.get("/setup")
    assert resp.status_code == 200
    assert "<span>Saving applies most changes immediately." in resp.text
    assert "restarted (socket/storage opened once at startup).</span></p>" in resp.text


class _FakeStoreWithApiKey:
    """Store giả lập chỉ implement kv_get("api_key") - đủ cho test /setup và
    /setup/api_key đọc api_key, không cần Store SQLite thật."""

    def kv_get(self, key, default=None):
        return "testkey1234abcd" if key == "api_key" else default  # secret-allow: test fixture, không phải key thật


def test_mask_api_key_none_and_short_and_normal():
    """6 dấu chấm CỐ ĐỊNH (không tỉ lệ theo độ dài key thật, tránh lộ metadata
    độ dài) + 4 ký tự cuối - xem docstring _mask_api_key()."""
    assert settings_api._mask_api_key(None) is None
    assert settings_api._mask_api_key("") is None

    masked = settings_api._mask_api_key("abcd1234efgh")

    assert masked == "••••••efgh"
    assert masked.count("•") == 6
    assert masked.endswith("efgh")


def test_setup_get_shows_not_yet_received_when_no_api_key():
    """FastAPI() trần (không có app.state.store, giống pattern test activity
    đã có) -> phải hiện thông báo "chưa nhận được", KHÔNG có nút Copy (không
    có gì để copy)."""
    app = FastAPI()
    app.include_router(settings_api.router)
    test_client = TestClient(app)

    resp = test_client.get("/setup")

    assert resp.status_code == 200
    assert "Not yet received" in resp.text
    assert 'id="copy-api-key-btn"' not in resp.text


def test_setup_get_shows_masked_key_and_copy_button_when_present():
    """Assertion quan trọng nhất: full raw api_key KHÔNG BAO GIỜ được nhúng
    vào HTML /setup (chỉ masked) - /setup có thể truy cập qua domain public
    (tunnel), lộ full secret qua view-source/devtools sẽ rất nguy hiểm. Chỉ
    GET /setup/api_key (endpoint riêng, gọi lúc bấm Copy) mới được trả raw -
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
    """GET /setup/api_key là endpoint DUY NHẤT được phép trả raw value - thiết
    kế có chủ đích (gọi từ JS lúc bấm Copy), không phải thiếu sót."""
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
    """Regression cho bug thật: navigator.clipboard.writeText() CHỈ hoạt động
    trong secure context (HTTPS/localhost) - truy cập /setup qua LAN HTTP
    thường (vd http://192.168.5.190:8090/setup, threat-model gốc của project)
    khiến nút Copy "không làm gì" vì Clipboard API không tồn tại/bị chặn và
    `.catch(function(){})` cũ nuốt lỗi im lặng. Verify cả 2 nhánh (Clipboard
    API hiện đại VÀ fallback execCommand('copy')) đều có mặt nguyên vẹn trong
    HTML - string-contains đơn giản, không chạy JS thật (headless browser
    ngoài scope pytest, xem 'Scope đã KHÔNG cover' vòng trước)."""
    resp = client.get("/setup")
    assert resp.status_code == 200
    assert "navigator.clipboard && navigator.clipboard.writeText" in resp.text
    assert "document.execCommand('copy')" in resp.text
    assert "function fallbackCopy(text)" in resp.text


def test_setup_get_no_gate_when_token_empty(client):
    """Tương thích ngược: EDGE_SETUP_TOKEN mặc định rỗng (`config.settings.
    setup_token == ""` - xem conftest.py `_clean_edge_env`/singleton baseline)
    -> KHÔNG gate gì, deployment cũ chưa cấu hình token không được regression
    thành 401 - xem python-reviewer 2026-09-17 (finding lộ credential /setup/api_key)."""
    assert config.settings.setup_token == ""
    resp = client.get("/setup")
    assert resp.status_code == 200


def test_setup_requires_basic_auth_when_token_set(client):
    """`config.settings.setup_token` được `_restore_settings_singleton`
    (conftest.py, autouse) tự động khôi phục sau test này - không cần fixture
    riêng. Basic Auth: username bất kỳ, password phải khớp token qua
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
    """GET /setup/api_key là endpoint driver chính của finding (trả RAW
    credential) - phải có test riêng, không chỉ dựa vào test /setup."""
    config.settings.setup_token = "sekret"  # secret-allow: test fixture

    resp_no_auth = client.get("/setup/api_key")
    assert resp_no_auth.status_code == 401

    resp_wrong = client.get("/setup/api_key", auth=("anyuser", "wrong-password"))
    assert resp_wrong.status_code == 401

    resp_ok = client.get("/setup/api_key", auth=("anyuser", "sekret"))
    assert resp_ok.status_code == 200


def test_setup_post_requires_auth_when_token_set(client):
    """Gate auth phải chạy TRƯỚC _is_same_origin check - thiếu auth phải bị
    401 NGAY CẢ KHI Origin header đúng (không được lọt qua đến bước CSRF)."""
    config.settings.setup_token = "sekret"  # secret-allow: test fixture

    resp = client.post("/setup", data=_valid_form(),
                        headers={"origin": "http://testserver"})

    assert resp.status_code == 401


def test_mask_api_key_short_key_fully_masked():
    """Regression cho finding Minor cùng đợt review: key <=8 ký tự -
    str(key)[-4:] trên chuỗi ngắn sẽ trả về NGUYÊN VẸN cả chuỗi, làm 'mask' lộ
    100% key - giờ phải che TOÀN BỘ ("••••••"),
    không lộ bất kỳ ký tự nào của key thật."""
    masked = settings_api._mask_api_key("abcdefgh")  # 8 ky tu, dung nguong <=8

    assert masked == "••••••"
    for ch in "abcdefgh":
        assert ch not in masked


def test_setup_requires_basic_auth_rejects_non_ascii_password_without_crash(client):
    """Regression cho bug crash thật: secrets.compare_digest(str, str) RAISE
    TypeError khi 1 trong 2 chuỗi chứa ký tự non-ASCII - ai đó gõ đại password
    non-ASCII (không cần biết token thật) sẽ làm route trả về 500 thay vì 401
    (lỗi lộ ra qua stack trace, tệ hơn cả reject bình thường). Giờ phải encode
    utf-8 sang bytes trước khi so - assertion quan trọng nhất là 401, KHÔNG
    phải 500 - xem python-reviewer 2026-09-17."""
    config.settings.setup_token = "sekret"  # secret-allow: test fixture
    credentials = base64.b64encode("user:héllo".encode("utf-8")).decode("ascii")

    resp = client.get("/setup", headers={"Authorization": "Basic %s" % credentials})

    assert resp.status_code == 401


def test_summarize_pcm_request_for_each_endpoint():
    """Panel 'PCM requests' - đối xứng NGƯỢC CHIỀU với 'Live activity'
    (Odoo Main -> edge, không phải node_agent -> edge). Verify cả 5 endpoint
    + 2 giá trị cmd khác nhau cho /api/command (có trong _COMMAND_LABELS và
    không có, để bắt regression fallback về raw cmd)."""
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
    """Module-level deque của inbound_api.py dùng chung cả tiến trình pytest -
    dọn trước/sau để không rò rỉ sang test khác (cùng tinh thần
    tests/test_inbound_api.py)."""
    inbound_api._recent_requests.clear()
    try:
        inbound_api._log_request("/api/command", serial="EDGE1", ch="CH01", cmd="zero")
        inbound_api._log_request("/api/source/test", kind="modbus")

        resp = client.get("/setup/pcm_requests")

        assert resp.status_code == 200
        rows = resp.json()["rows"]
        assert len(rows) == 2
        # appendleft - request MỚI NHẤT (log sau cùng, /api/source/test) đứng đầu.
        assert rows[0]["endpoint"] == "/api/source/test"
        assert rows[0]["summary"] == "Connection test (modbus)"
        assert rows[1]["summary"] == "Zero on EDGE1 / CH01"
        assert all(row["age_s"] >= 0 for row in rows)
    finally:
        inbound_api._recent_requests.clear()


def test_setup_pcm_requests_requires_auth_when_token_set(client):
    """Route thứ 5 cũng phải được gate bởi EDGE_SETUP_TOKEN giống 4 route
    /setup/* còn lại - tránh sót consistency khi thêm route mới."""
    config.settings.setup_token = "sekret"  # secret-allow: test fixture

    resp_no_auth = client.get("/setup/pcm_requests")
    assert resp_no_auth.status_code == 401

    resp_ok = client.get("/setup/pcm_requests", auth=("anyuser", "sekret"))
    assert resp_ok.status_code == 200


def test_post_setup_accepts_matching_fingerprint(client, tmp_path):
    """Happy path: fingerprint lấy từ GET (khớp đúng nội dung .env hiện tại)
    được gửi kèm POST -> Save bình thường, không bị guard chặn."""
    env_path = tmp_path / ".env"
    env_path.write_text("EDGE_MAIN_URL=http://odoo-main.local:8069\n")

    resp_get = client.get("/setup")
    assert resp_get.status_code == 200
    fingerprint = _extract_fingerprint(resp_get.text)
    assert fingerprint and fingerprint != "missing"

    form = _valid_form()
    form["_env_fingerprint"] = fingerprint
    resp = client.post("/setup", data=form)

    assert resp.status_code == 200
    assert "Saved" in resp.text


def test_post_setup_rejects_stale_fingerprint_and_preserves_external_change(client, tmp_path):
    """Tái hiện đúng bug thật đã fix (xem docstring _env_fingerprint()): 1 tab
    /setup còn mở với fingerprint CŨ (stale_fingerprint) trong lúc .env bị sửa
    TRỰC TIẾP từ bên ngoài (mô phỏng SSH, hoặc tab khác đã Save trước). Tab cũ
    Save 1 field KHÔNG liên quan (EDGE_NAME) phải bị TỪ CHỐI (409), và quan
    trọng nhất: nội dung .env phải VẪN CÒN đúng giá trị đã sửa từ bên ngoài -
    KHÔNG được ghi đè bởi giá trị từ form của tab cũ."""
    env_path = tmp_path / ".env"
    env_path.write_text(
        "EDGE_MAIN_URL=http://odoo-main.local:8069\n"
        "EDGE_FORWARDED_ALLOW_IPS=127.0.0.1,::1\n"
    )

    resp_get = client.get("/setup")
    assert resp_get.status_code == 200
    stale_fingerprint = _extract_fingerprint(resp_get.text)

    # Sửa trực tiếp .env "từ bên ngoài" - vd qua SSH - TRONG LÚC tab /setup
    # trên vẫn còn mở với stale_fingerprint. Nội dung file đổi -> fingerprint
    # thật sự trên đĩa cũng đổi theo, khác hẳn stale_fingerprint.
    env_path.write_text(
        "EDGE_MAIN_URL=http://odoo-main.local:8069\n"
        "EDGE_FORWARDED_ALLOW_IPS=10.0.0.5/32\n"
    )
    external_content = env_path.read_text()

    form = _valid_form()
    form["_env_fingerprint"] = stale_fingerprint
    form["EDGE_NAME"] = "Changed From Stale Tab"

    resp = client.post("/setup", data=form)

    assert resp.status_code == 409
    assert "changed elsewhere" in resp.text.lower()
    assert "reopen /setup" in resp.text.lower()
    # Assertion CHÍNH chứng minh guard hoạt động: .env phải VẪN đúng nội dung
    # đã sửa bên ngoài (EDGE_FORWARDED_ALLOW_IPS=10.0.0.5/32), KHÔNG bị ghi đè
    # bởi giá trị POST gửi lên - đây chính là dữ liệu đã mất thật trong bug gốc.
    assert env_path.read_text() == external_content
    assert "Changed From Stale Tab" not in env_path.read_text()
    assert "10.0.0.5/32" in env_path.read_text()


def test_post_setup_conflict_page_resubmit_is_safe_noop_preserving_external_change(client, tmp_path):
    """Regression cho finding Critical python-reviewer 2026-09-25: nhánh 409
    TRƯỚC FIX gọi _render(values, ...) (data SUBMIT CŨ bị từ chối) - nhưng
    _render() luôn TỰ TÍNH fingerprint MỚI cho hidden field bất kể tham số
    `values` là gì, nên trang lỗi 409 vô tình mang theo "vé thông hành" mới
    KÈM THEO data cũ hiển thị trên form. Bấm Save LẦN 2 NGAY TRÊN CHÍNH TRANG
    LỖI (không cần reload /setup) sẽ đưa fingerprint mới + data CŨ đó qua
    được guard, ghi đè mất thay đổi ngoài luồng - tái diễn đúng bug gốc. Fix:
    đổi sang _render(_current_values(), ...), nhất quán với 403/500.

    Hành vi ĐÚNG sau fix: trang lỗi 409 hiển thị CURRENT VALUES thật sự trên
    đĩa (gồm cả thay đổi ngoài luồng) kèm fingerprint MỚI khớp file đó. Nếu
    Nam bấm Save lại NGAY trên trang lỗi mà KHÔNG sửa gì, browser gửi lên
    đúng những giá trị đang hiển thị (= _current_values()) - _write_env_file
    ghi lại ĐÚNG giá trị hiện tại, safe no-op, KHÔNG mất thay đổi ngoài luồng."""
    env_path = tmp_path / ".env"
    env_path.write_text(
        "EDGE_MAIN_URL=http://odoo-main.local:8069\n"
        "EDGE_FORWARDED_ALLOW_IPS=127.0.0.1,::1\n"
    )

    resp_get = client.get("/setup")
    assert resp_get.status_code == 200
    stale_fingerprint = _extract_fingerprint(resp_get.text)

    # Sửa trực tiếp .env "từ bên ngoài" - vd qua SSH - TRONG LÚC tab /setup
    # trên vẫn còn mở với stale_fingerprint.
    env_path.write_text(
        "EDGE_MAIN_URL=http://odoo-main.local:8069\n"
        "EDGE_FORWARDED_ALLOW_IPS=10.0.0.5/32\n"
    )
    external_content_before = env_path.read_text()

    form = _valid_form()
    form["_env_fingerprint"] = stale_fingerprint

    resp1 = client.post("/setup", data=form)

    assert resp1.status_code == 409
    assert env_path.read_text() == external_content_before

    # Fingerprint lay TU CHINH trang loi 409 nay - KHONG goi lai GET /setup
    # (dung dung kich ban "bam Save lai ngay tren trang loi").
    fingerprint_from_error_page = _extract_fingerprint(resp1.text)
    assert fingerprint_from_error_page != stale_fingerprint

    # Mô phỏng Nam bấm Save lại NGAY TRÊN TRANG LỖI mà KHÔNG sửa field nào -
    # browser thật sẽ gửi đúng CÁC GIÁ TRỊ ĐANG HIỂN THỊ trên form lỗi đó, ĐỌC
    # LẠI TỪ HTML (không hardcode _current_values() - buggy/fixed đều tính
    # fingerprint mới giống hệt nhau, chỉ khác ở VALUE hiển thị cho từng field,
    # nên phải scrape đúng HTML mới phân biệt được 2 trường hợp).
    resubmit_data = _extract_rendered_form_values(resp1.text)
    resubmit_data["_env_fingerprint"] = fingerprint_from_error_page

    resp2 = client.post("/setup", data=resubmit_data)

    # Guard cho qua (fingerprint khớp - không có gì đổi thêm giữa 2 request)
    # NHƯNG đây phải là SAFE NO-OP: giá trị ghi xuống CHÍNH LÀ giá trị hiện
    # tại (gồm cả thay đổi ngoài luồng), KHÔNG PHẢI data cũ bị từ chối ở
    # POST lần 1 - assertion CHÍNH chứng minh finding đã được fix đúng.
    assert resp2.status_code == 200
    assert "Saved" in resp2.text
    final_content = env_path.read_text()
    assert "10.0.0.5/32" in final_content
    assert "127.0.0.1,::1" not in final_content


def test_post_setup_without_fingerprint_field_skips_guard(client, tmp_path):
    """Backward-compat: client cũ (form render TRƯỚC khi tính năng này tồn tại,
    không có hidden field _env_fingerprint) vẫn phải Save được bình thường dù
    .env đã đổi từ bên ngoài - guard chỉ active khi có giá trị để đối chiếu
    (best-effort, cùng triết lý với _is_same_origin())."""
    env_path = tmp_path / ".env"
    env_path.write_text("EDGE_MAIN_URL=http://odoo-main.local:8069\n")

    client.get("/setup")  # mô phỏng tab cũ đã load, KHÔNG dùng fingerprint từ đây

    # .env đổi từ bên ngoài sau khi tab (giả lập) đã mở.
    env_path.write_text("EDGE_MAIN_URL=http://odoo-main.local:8069\nEDGE_NAME=changed-externally\n")

    form = _valid_form()
    assert "_env_fingerprint" not in form  # form "cũ", không có hidden field

    resp = client.post("/setup", data=form)

    assert resp.status_code == 200
    assert "Saved" in resp.text
