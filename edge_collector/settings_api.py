# -*- coding: utf-8 -*-
"""Trang web cấu hình EDGE_* (thay thế sửa file .env bằng tay):
    GET  /setup   form hiện giá trị đang có trong .env (hoặc mặc định nếu chưa có file)
    POST /setup   ghi lại .env tại chỗ (không tempfile+rename - xem _write_env_file,
                  giữ nguyên dòng/comment khác) RỒI hot-reload ngay singleton
                  `settings` + base_url của OdooClient đang chạy (xem
                  config.reload_settings()/OdooClient.refresh_base_url())

Cùng mức độ tin cậy LAN như inbound_api.py - mặc định nhắm LAN, nhưng có thể
đặt sau reverse-proxy/tunnel (vd truy cập qua domain public) NẾU cấu hình đúng
EDGE_FORWARDED_ALLOW_IPS (xem dưới). CHỈ 4 field (EDGE_LISTEN_HOST/PORT,
EDGE_STATE_DIR, EDGE_FORWARDED_ALLOW_IPS - xem config.RESTART_REQUIRED_KEYS)
vẫn cần KHỞI ĐỘNG LẠI edge_collector mới áp dụng (socket đã bind / SQLite đã
mở cố định / uvicorn đã đọc giá trị này lúc startup); 12 field còn lại (Main
URL/Edge code/Name/Platform/Base URL/Setup access token + 6 interval) áp dụng
NGAY sau Save, không cần restart - xem review 2026-09-17 (tính năng
hot-reload, phát sinh từ câu hỏi thực tế của Nam).

EDGE_SETUP_TOKEN: rỗng (mặc định) = KHÔNG gate gì (giữ nguyên threat-model
LAN-only cũ). Đặt 1 giá trị để yêu cầu HTTP Basic Auth (username bất kỳ,
password = token này) cho TOÀN BỘ /setup/GET/POST/activity/api_key - xem
_check_setup_auth(). Sinh ra vì GET /setup/api_key trả RAW credential (Odoo
api_key) không auth, và /setup giờ có thể truy cập qua domain public (xem
EDGE_FORWARDED_ALLOW_IPS ở trên) - ai có URL là lấy được key, dùng để mạo
danh edge này gọi thẳng Odoo từ bất kỳ đâu. STRONGLY RECOMMENDED đặt giá trị
này khi /setup được tunnel ra ngoài LAN - xem python-reviewer 2026-09-17.

EDGE_FORWARDED_ALLOW_IPS: IP/CIDR của reverse-proxy/tunnel được TIN để đọc
X-Forwarded-Proto/-Host, truyền thẳng cho uvicorn's ProxyHeadersMiddleware.
Mặc định "127.0.0.1,::1" (chỉ trust loopback, đúng y hệt uvicorn) - KHÔNG
đổi hành vi LAN-only hiện tại. Nếu /setup được truy cập qua 1 reverse-proxy/
tunnel TLS-terminating (Origin browser là https:// nhưng kết nối TCP tới
uvicorn vẫn là http://), _is_same_origin() bên dưới sẽ so sai scheme (Origin
https != request.url.scheme http) và reject nhầm request Save hợp lệ - đã
gặp thực tế 2026-09-17 (Nam tunnel /setup qua domain public). Fix: đặt field
này đúng IP/CIDR của proxy/tunnel đó (KHÔNG đặt "*" trừ khi đã chắn chắc
chắn không ai khác gửi thẳng tới cổng này được, vì "*" sẽ trust
X-Forwarded-Proto từ BẤT KỲ client nào, mở đường giả mạo Origin qua header).
"""
import base64
import html
import ipaddress
import math
import hashlib
import secrets
import time
from pathlib import Path
from typing import Dict, List
from urllib.parse import urlsplit

from dotenv.main import dotenv_values
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response

from . import inbound_api
from .config import DOTENV_PATH, RESTART_REQUIRED_KEYS, reload_settings, settings

router = APIRouter()

_ENV_PATH = DOTENV_PATH

# UI-facing strings đều bằng tiếng Anh theo yêu cầu Nam. Mỗi field: key (khớp
# .env), label, giá trị mặc định, hint, group ("connection"/"network"/"timing").
# Field nào cần restart mới áp dụng -> xem config.RESTART_REQUIRED_KEYS (định
# nghĩa 1 nơi DUY NHẤT, tránh 2 chỗ liệt kê lệch nhau).
_GROUPS = [
    ("connection", "Connection",
     "How this edge identifies itself and reaches Odoo Main."),
    ("network", "Network & storage",
     "Local networking and where this edge keeps its state."),
    ("timing", "Sync intervals",
     "How often this edge talks to Odoo."),
    ("mqtt", "MQTT broker",
     "The factory broker this edge reads from and sends node commands through."),
]

_FIELDS = [
    {"key": "EDGE_MAIN_URL", "label": "Odoo Main URL", "default": "http://localhost:8069",
     "hint": "Root URL of Odoo (required)", "group": "connection"},
    {"key": "EDGE_CODE", "label": "Edge code", "default": "",
     "hint": "Must match pcm.edge.code in Odoo - leave blank to auto-generate on first run",
     "group": "connection"},
    {"key": "EDGE_NAME", "label": "Display name", "default": "",
     "hint": "Name Odoo assigns the pcm.edge record on first self-registration",
     "group": "connection"},
    {"key": "EDGE_PLATFORM", "label": "Platform", "default": "other",
     "hint": "ubuntu | windows | other", "group": "connection"},
    {"key": "EDGE_BASE_URL", "label": "This edge's LAN address", "default": "",
     "hint": "e.g. http://10.10.1.50:8000 - lets Odoo/tablet call back into this edge",
     "group": "network"},
    {"key": "EDGE_LISTEN_HOST", "label": "Listen host", "default": "0.0.0.0",
     "hint": "Interface this edge listens on", "group": "network"},
    {"key": "EDGE_LISTEN_PORT", "label": "Listen port", "default": "8000",
     "hint": "Port this edge listens on", "group": "network"},
    {"key": "EDGE_STATE_DIR", "label": "State directory", "default": "./var",
     "hint": "SQLite outbox / history / api_key cache", "group": "network"},
    {"key": "EDGE_FORWARDED_ALLOW_IPS", "label": "Trusted reverse-proxy IPs",
     "default": "127.0.0.1,::1",
     "hint": "Comma-separated IPs/CIDRs allowed to set X-Forwarded-Proto/-Host "
             "- set to your reverse-proxy/tunnel's address if /setup is reached "
             "through one (fixes CSRF false-reject over HTTPS tunnels)",
     "group": "network"},
    {"key": "EDGE_SETUP_TOKEN", "label": "Setup access token", "default": "",
     "hint": "Leave blank = no access control (LAN-only threat model, previous "
             "behaviour). Set a secret here to require it (HTTP Basic Auth, any "
             "username) for every /setup page and API - strongly recommended if "
             "/setup is reached through a public domain/tunnel.",
     "group": "network", "input_type": "password"},
    {"key": "EDGE_HELLO_INTERVAL_S", "label": "Hello interval (s)", "default": "30",
     "hint": "Register / renew liveness with Odoo", "group": "timing"},
    {"key": "EDGE_HEARTBEAT_INTERVAL_S", "label": "Heartbeat interval (s)", "default": "30",
     "hint": "Per-device liveness ping", "group": "timing"},
    {"key": "EDGE_CONFIG_POLL_INTERVAL_S", "label": "Config poll interval (s)", "default": "30",
     "hint": "Pull pcm.source / pcm.device config", "group": "timing"},
    {"key": "EDGE_PRINT_POLL_INTERVAL_S", "label": "Print poll interval (s)", "default": "3",
     "hint": "Poll for queued print jobs", "group": "timing"},
    {"key": "EDGE_SUBMIT_INTERVAL_S", "label": "Data submit interval (s)", "default": "2",
     "hint": "Batch measurements to Odoo", "group": "timing"},
    {"key": "EDGE_CONFIG_DEBOUNCE_S", "label": "Config debounce (s)", "default": "10",
     "hint": "Delay before applying a config change", "group": "timing"},
    {"key": "EDGE_MQTT_CONSUMER", "label": "Enable MQTT consumer", "default": "false",
     "hint": "true | false - read measurements from the broker instead of waiting "
             "for nodes to POST them", "group": "mqtt"},
    {"key": "EDGE_MQTT_CONSUMER_URL", "label": "Broker address", "default": "mqtt://127.0.0.1:1883",
     "hint": "e.g. mqtt://192.168.5.190:1883 - use the LAN address, not 127.0.0.1: "
             "this service runs in a container and its loopback is not the host's",
     "group": "mqtt"},
    {"key": "EDGE_MQTT_CONSUMER_USER", "label": "Broker username", "default": "",
     "hint": "Matches the broker's own credentials (see broker/.env)", "group": "mqtt"},
    {"key": "EDGE_MQTT_CONSUMER_PASS", "label": "Broker password", "default": "",
     "hint": "Stored in this edge's .env. Rotating it also means reflashing every "
             "node, because nodes carry it in firmware",
     "group": "mqtt", "input_type": "password"},
    {"key": "EDGE_MQTT_CONSUMER_FORWARD", "label": "Push readings into Odoo", "default": "false",
     "hint": "true | false - MUST be true once nodes have HTTP switched off, "
             "otherwise readings stop at this edge. Keep false while a node still "
             "sends the same reading over both HTTP and MQTT, or Odoo records it twice",
     "group": "mqtt"},
    {"key": "EDGE_MQTT_CONSUMER_TOPIC", "label": "Measurement topic", "default": "fms/+/meas",
     "hint": "Wildcard pattern; + stands for the node serial", "group": "mqtt"},
    {"key": "EDGE_MQTT_CONSUMER_STATUS_TOPIC", "label": "Status topic", "default": "fms/+/status",
     "hint": "Carries online / Last Will, and the node's own 'I accept MQTT commands' flag",
     "group": "mqtt"},
    {"key": "EDGE_MQTT_CONSUMER_CLIENT_ID", "label": "Client id", "default": "",
     "hint": "Leave blank to derive it from the edge code. Must be fixed and unique: "
             "the durable session is keyed on it, and two processes sharing one id "
             "keep kicking each other off the broker", "group": "mqtt"},
]

_LABELS = {f["key"]: f["label"] for f in _FIELDS}
_INT_FIELDS = {"EDGE_LISTEN_PORT", "EDGE_HELLO_INTERVAL_S", "EDGE_HEARTBEAT_INTERVAL_S",
               "EDGE_CONFIG_POLL_INTERVAL_S", "EDGE_PRINT_POLL_INTERVAL_S",
               "EDGE_CONFIG_DEBOUNCE_S"}
_FLOAT_FIELDS = {"EDGE_SUBMIT_INTERVAL_S"}

_ICON_CHECK = ('<svg width="16" height="16" viewBox="0 0 20 20" fill="none" aria-hidden="true">'
               '<path d="M5 10.5L8.5 14L15 6.5" stroke="currentColor" stroke-width="2" '
               'stroke-linecap="round" stroke-linejoin="round"/></svg>')
_ICON_ALERT = ('<svg width="16" height="16" viewBox="0 0 20 20" fill="none" aria-hidden="true">'
               '<path d="M10 3L18 17H2L10 3Z" stroke="currentColor" stroke-width="1.6" '
               'stroke-linejoin="round"/><path d="M10 8v4" stroke="currentColor" stroke-width="1.6" '
               'stroke-linecap="round"/><circle cx="10" cy="14.2" r="0.9" fill="currentColor"/></svg>')
_ICON_RESTART = ('<svg width="14" height="14" viewBox="0 0 20 20" fill="none" aria-hidden="true">'
                 '<path d="M16 5v4h-4M4 15v-4h4" stroke="currentColor" stroke-width="1.8" '
                 'stroke-linecap="round" stroke-linejoin="round"/>'
                 '<path d="M5.5 8A6 6 0 0116 7.5M14.5 12A6 6 0 014 12.5" stroke="currentColor" '
                 'stroke-width="1.8" stroke-linecap="round"/></svg>')

_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

:root {
  --bg: #F8FAFC; --fg: #0F172A; --muted-fg: #475569;
  --card: #FFFFFF; --card-border: #E2E8F0;
  --input-bg: #FFFFFF; --input-border: #CBD5E1;
  --accent: #16A34A; --accent-fg: #FFFFFF;
  --ring: #2563EB; --ring-glow: rgba(37,99,235,.25);
  --danger: #DC2626; --danger-bg: #FEF2F2; --danger-border: #FCA5A5;
  --success-bg: #F0FDF4; --success-border: #86EFAC; --success-fg: #166534;
  --badge-bg: #F1F5F9; --badge-fg: #475569;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0F172A; --fg: #F8FAFC; --muted-fg: #94A3B8;
    --card: #1B2336; --card-border: #334155;
    --input-bg: #101828; --input-border: #334155;
    --accent: #22C55E; --accent-fg: #0F172A;
    --ring: #60A5FA; --ring-glow: rgba(96,165,250,.28);
    --danger: #F87171; --danger-bg: #2A1215; --danger-border: #7F1D1D;
    --success-bg: #0F2419; --success-border: #14532D; --success-fg: #86EFAC;
    --badge-bg: #1E293B; --badge-fg: #94A3B8;
  }
}
* { box-sizing: border-box; }
body {
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background: var(--bg); color: var(--fg);
  margin: 0; padding: 40px 20px 80px; line-height: 1.5;
}
.layout {
  max-width: 1360px; margin: 0 auto; display: grid;
  grid-template-columns: 760px minmax(220px, 1fr) minmax(220px, 1fr);
  gap: 24px; align-items: start;
}
@media (max-width: 1320px) { .layout { grid-template-columns: 1fr; max-width: 760px; } }
.wrap { max-width: none; margin: 0; }
h1 { font-size: 1.5rem; font-weight: 700; margin: 0 0 4px; }
.lede { color: var(--muted-fg); font-size: 0.9rem; margin: 0 0 12px; }
.notice {
  display: flex; align-items: center; gap: 6px; color: var(--badge-fg);
  background: var(--badge-bg); border-radius: 8px; padding: 8px 12px;
  font-size: 0.8rem; margin: 0 0 24px;
}
.banner {
  display: flex; gap: 10px; align-items: flex-start;
  border-radius: 10px; padding: 14px 16px; margin-bottom: 24px; font-size: 0.9rem;
}
.banner.error { background: var(--danger-bg); border: 1px solid var(--danger-border); color: var(--danger); }
.banner.success { background: var(--success-bg); border: 1px solid var(--success-border); color: var(--success-fg); }
.banner h2 { margin: 0 0 6px; font-size: 0.95rem; font-weight: 600; }
.banner ul { margin: 0; padding-left: 18px; }
.banner a { color: inherit; font-weight: 500; }
.banner:focus-visible { outline: 2px solid var(--ring); outline-offset: 2px; }
fieldset {
  border: 1px solid var(--card-border); border-radius: 12px; background: var(--card);
  padding: 22px 24px; margin: 0 0 22px; min-width: 0;
}
legend { padding: 0 6px; font-weight: 600; font-size: 0.95rem; }
.group-desc { color: var(--muted-fg); font-size: 0.82rem; margin: 0 0 18px; }
.field { display: grid; grid-template-columns: minmax(180px, 260px) 1fr; column-gap: 20px; row-gap: 4px; margin-bottom: 16px; }
.field:last-child { margin-bottom: 0; }
@media (max-width: 680px) { .field { grid-template-columns: 1fr; } }
label { font-size: 0.88rem; font-weight: 500; }
.hint { color: var(--muted-fg); font-size: 0.78rem; margin: 2px 0 0; grid-column: 1; }
@media (max-width: 680px) { .hint { grid-column: 1; } }
.badge {
  display: inline-flex; align-items: center; gap: 4px; margin-left: 6px;
  font-size: 0.65rem; font-weight: 600; text-transform: uppercase; letter-spacing: .03em;
  color: var(--badge-fg); background: var(--badge-bg); padding: 2px 7px; border-radius: 999px;
}
input[type=text], input[type=password] {
  width: 100%; font: inherit; color: var(--fg); background: var(--input-bg);
  border: 1px solid var(--input-border); border-radius: 8px; padding: 8px 11px;
  transition: border-color 150ms, box-shadow 150ms;
}
input[type=text]:focus-visible, input[type=password]:focus-visible {
  outline: none; border-color: var(--ring); box-shadow: 0 0 0 3px var(--ring-glow);
}
input[type=text].invalid, input[type=password].invalid { border-color: var(--danger); }
.field-error { color: var(--danger); font-size: 0.78rem; margin: 2px 0 0; grid-column: 2; }
@media (max-width: 680px) { .field-error { grid-column: 1; } }
.actions { margin-top: 4px; }
button[type=submit] {
  background: var(--accent); color: var(--accent-fg); border: none;
  padding: 10px 26px; border-radius: 8px; font: inherit; font-weight: 600; font-size: 0.92rem;
  cursor: pointer; transition: filter 150ms, transform 150ms;
}
button[type=submit]:hover { filter: brightness(1.08); }
button[type=submit]:active { transform: scale(.98); }
button[type=submit]:focus-visible { outline: 2px solid var(--ring); outline-offset: 2px; }
.activity {
  border: 1px solid var(--card-border); border-radius: 12px; background: var(--card);
  padding: 18px 20px; position: sticky; top: 20px; max-height: calc(100vh - 40px);
  overflow: auto; min-width: 0;
}
.activity h2 { margin: 0 0 4px; font-size: 0.95rem; font-weight: 600; }
.activity-list { list-style: none; margin: 12px 0 0; padding: 0; display: flex; flex-direction: column; gap: 8px; }
.activity-row { padding: 8px 10px; border-radius: 8px; background: var(--badge-bg); font-size: 0.8rem; }
.activity-row.a-bad { border: 1px solid var(--danger-border); }
.a-top { display: flex; justify-content: space-between; gap: 8px; font-weight: 500; }
.a-age { color: var(--muted-fg); font-size: 0.72rem; white-space: nowrap; }
.a-val { color: var(--muted-fg); margin-top: 2px; }
.activity-empty { color: var(--muted-fg); font-size: 0.82rem; padding: 8px 0; margin: 12px 0 0; }
.readonly-row { display: flex; gap: 8px; align-items: center; }
.readonly-value {
  font: inherit; color: var(--fg); background: var(--input-bg);
  border: 1px solid var(--input-border); border-radius: 8px; padding: 8px 11px;
  margin: 0; font-family: ui-monospace, Consolas, monospace; flex: 1; min-width: 0;
}
.copy-btn {
  background: var(--badge-bg); color: var(--fg); border: 1px solid var(--input-border);
  border-radius: 8px; padding: 8px 14px; font: inherit; font-size: 0.82rem; font-weight: 500;
  cursor: pointer; white-space: nowrap; transition: filter 150ms;
}
.copy-btn:hover { filter: brightness(1.1); }
.copy-btn:focus-visible { outline: 2px solid var(--ring); outline-offset: 2px; }
"""


# Poll riêng /setup/activity (không dùng WebSocket - project chưa có tiền lệ,
# polling JSON khớp pattern sẵn có /api/latest, /api/stats). esc() bắt buộc
# cho MỌI trường từ động node (serial/ch/giá trị chuỗi 's') trước khi nối vào
# innerHTML - đây là dữ liệu từ thiết bị ngoài (node_agent), không phải
# hằng-code, phải coi là KHÔNG đáng tin để tránh XSS lưu trữ qua history.
_ACTIVITY_SCRIPT = """<script>
(function(){
  var list = document.getElementById('activity-list');
  if (!list) return;
  function esc(s){ var d=document.createElement('div'); d.textContent=String(s); return d.innerHTML; }
  function fmtAge(s){
    if (s < 60) return s + 's ago';
    if (s < 3600) return Math.floor(s/60) + 'm ago';
    return Math.floor(s/3600) + 'h ago';
  }
  function render(rows){
    if (!rows.length) { list.innerHTML = '<li class="activity-empty">Waiting for data...</li>'; return; }
    list.innerHTML = rows.map(function(r){
      var bad = (r.q === 1 || r.q === 2) ? ' a-bad' : '';
      var val = (r.v === null || r.v === undefined || r.v === '') ? (r.s || '') : r.v;
      return '<li class="activity-row' + bad + '">'
        + '<div class="a-top"><span>' + esc(r.serial) + ' / ' + esc(r.ch) + '</span>'
        + '<span class="a-age">' + esc(fmtAge(r.age_s)) + '</span></div>'
        + '<div class="a-val">' + esc(val) + '</div></li>';
    }).join('');
  }
  function poll(){
    fetch('/setup/activity').then(function(r){ return r.json(); })
      .then(function(data){ render(data.rows || []); }).catch(function(){});
  }
  poll();
  setInterval(poll, 3000);
})();
</script>"""

# Panel "PCM requests" - CHIỀU NGƯỢC LẠI với Live activity (Odoo Main gọi
# XUỐNG edge này, xem inbound_api.py.recent_requests()/_log_request()).
# summary đã được build sẵn ở server (_summarize_pcm_request) - JS chỉ hiển
# thị, không tự suy đoán từng field khác nhau giữa các endpoint.
_PCM_REQUESTS_SCRIPT = """<script>
(function(){
  var list = document.getElementById('pcm-requests-list');
  if (!list) return;
  function esc(s){ var d=document.createElement('div'); d.textContent=String(s); return d.innerHTML; }
  function fmtAge(s){
    if (s < 60) return s + 's ago';
    if (s < 3600) return Math.floor(s/60) + 'm ago';
    return Math.floor(s/3600) + 'h ago';
  }
  function render(rows){
    if (!rows.length) { list.innerHTML = '<li class="activity-empty">Waiting for data...</li>'; return; }
    list.innerHTML = rows.map(function(r){
      return '<li class="activity-row">'
        + '<div class="a-top"><span>' + esc(r.endpoint) + '</span>'
        + '<span class="a-age">' + esc(fmtAge(r.age_s)) + '</span></div>'
        + '<div class="a-val">' + esc(r.summary) + '</div></li>';
    }).join('');
  }
  function poll(){
    fetch('/setup/pcm_requests').then(function(r){ return r.json(); })
      .then(function(data){ render(data.rows || []); }).catch(function(){});
  }
  poll();
  setInterval(poll, 3000);
})();
</script>"""

# Nút Copy KHÔNG bao giờ đọc giá trị từ DOM/HTML đã render (ở đó chỉ có bản
# mask) - luôn fetch /setup/api_key riêng lúc bấm, giữ đúng nguyên tắc "full
# secret không nằm sẵn trong response ban đầu" dù vẫn cho phép copy khi cần.
_API_KEY_SCRIPT = """<script>
(function(){
  var btn = document.getElementById('copy-api-key-btn');
  if (!btn) return;
  function showCopied(){
    var orig = btn.textContent;
    btn.textContent = 'Copied!';
    setTimeout(function(){ btn.textContent = orig; }, 1500);
  }
  function fallbackCopy(text){
    // navigator.clipboard.writeText CHỈ hoạt động trong secure context
    // (HTTPS hoặc localhost) - edge này thường truy cập qua LAN HTTP thường
    // (threat-model gốc), nên cần fallback này cho trường hợp đó, không chỉ
    // dựa vào Clipboard API hiện đại - xem bug báo cáo 2026-09-17 (nút Copy
    // "không hoạt động" khi truy cập qua http://192.168.x.x thường).
    var ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.focus();
    ta.select();
    var ok = false;
    try { ok = document.execCommand('copy'); } catch (e) { ok = false; }
    document.body.removeChild(ta);
    return ok;
  }
  btn.addEventListener('click', function(){
    fetch('/setup/api_key').then(function(r){ return r.json(); }).then(function(data){
      if (!data.api_key) return;
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(data.api_key).then(showCopied, function(){
          if (fallbackCopy(data.api_key)) showCopied();
        });
      } else if (fallbackCopy(data.api_key)) {
        showCopied();
      }
    }).catch(function(){});
  });
})();
</script>"""


def _current_values() -> dict:
    on_disk = dotenv_values(str(_ENV_PATH)) if _ENV_PATH.exists() else {}
    return {f["key"]: on_disk.get(f["key"]) or f["default"] for f in _FIELDS}


def _env_fingerprint(path: Path) -> str:
    """Optimistic-concurrency guard chống mất thay đổi khi có 2 nguồn ghi
    .env chồng nhau (vd sửa trực tiếp qua SSH trong lúc 1 tab /setup khác
    vẫn đang mở) - _write_env_file() ghi lại TOÀN BỘ giá trị lấy từ form,
    nên 1 tab cũ còn mở sẽ âm thầm GHI ĐÈ thay đổi từ bên ngoài ngay cả khi
    Nam chỉ đổi 1 field KHÔNG liên quan - đã tái hiện thật (EDGE_FORWARDED_
    ALLOW_IPS bị mất sau khi sửa qua SSH rồi Save 1 field khác từ tab cũ) -
    xem Fix-History 2026-09-25."""
    if not path.exists():
        return "missing"
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _format_env_line(key: str, value: str) -> str:
    """Quote logic khớp CHÍNH XÁC python-dotenv set_key(quote_mode='auto')
    (dotenv/main.py set_key()) - dùng lại để KHÔNG lặp lại bug '#'-truncation
    đã sửa trước đó (giá trị có khoảng trắng/'#' phải được quote)."""
    if value.isalnum():
        value_out = value
    else:
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        value_out = "'%s'" % escaped
    return "%s=%s\n" % (key, value_out)


def _write_env_file(path: Path, values: dict) -> None:
    """Ghi TRỰC TIẾP vào file đang có (truncate + write, KHÔNG tempfile+
    os.replace) - python-dotenv's set_key() dùng chiến lược atomic-rewrite
    (tempfile rồi os.replace) sẽ crash 'OSError: [Errno 16] Device or
    resource busy' khi .env là bind-mount 1 FILE riêng trong Docker (không
    thể rename() để vào đúng inode đang bị mount) - xem review 2026-09-17.
    Đổi lại: mất tính atomic (crash giữa chừng có thể để file dang dở), chấp
    nhận được cho 1 form cấu hình ít khi lưu, đổi lại chạy được cả venv lẫn
    Docker không cần đổi cấu trúc bind-mount.

    encoding="utf-8" tường minh ở cả đọc lẫn ghi - _current_values() (qua
    dotenv_values()) và cả `Settings` phía config.py đều giả định utf-8; nếu
    để mặc định sẽ rơi về locale.getpreferredencoding() của platform (vd
    ANSI codepage trên Windows - EDGE_PLATFORM=windows là target thật được
    khai báo trong _FIELDS), gây lệch encoding giữa ghi/đọc cho giá trị có
    dấu (vd EDGE_NAME tiếng Việt) - xem review 2026-09-17."""
    remaining = dict(values)
    replaced_keys = set()
    out_lines = []
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines(keepends=True):
            stripped = line.strip()
            candidate = None
            if stripped and not stripped.startswith("#") and "=" in stripped:
                candidate = stripped.split("=", 1)[0].strip()
            if candidate in replaced_keys:
                # Key TRÙNG LẶP đã có sẵn trong file và ĐÃ được ghi 1 lần rồi -
                # bỏ dòng này, không để 2 giá trị cho cùng 1 key tồn tại
                # (dotenv đọc lại theo kiểu "dòng sau đè dòng trước", sẽ làm
                # giá trị vừa Save bị vô hiệu âm thầm) - xem review 2026-09-17.
                continue
            if candidate is not None and candidate in remaining:
                out_lines.append(_format_env_line(candidate, remaining.pop(candidate)))
                replaced_keys.add(candidate)
            else:
                out_lines.append(line if line.endswith("\n") else line + "\n")
    for key, value in remaining.items():
        out_lines.append(_format_env_line(key, value))
    path.write_text("".join(out_lines), encoding="utf-8")


def _validate(values: dict) -> Dict[str, List[str]]:
    errors: Dict[str, List[str]] = {}

    def add(key: str, msg: str) -> None:
        errors.setdefault(key, []).append(msg)

    for key, value in values.items():
        if "\n" in value or "\r" in value:
            add(key, "Must not contain a newline")
    if not values.get("EDGE_MAIN_URL", "").startswith(("http://", "https://")):
        add("EDGE_MAIN_URL", "Must start with http:// or https://")
    url = values.get("EDGE_MQTT_CONSUMER_URL", "")
    # CHỈ 2 scheme này - mqtt_consumer._split_url() chỉ biết strip "mqtt://"/
    # "tcp://" và không gọi tls_set() ở đâu cả, nên "mqtts://"/"ssl://" sẽ bị
    # parse SAI (host thành chuỗi "mqtts" thay vì hostname thật) mà không báo
    # lỗi gì - xem python-reviewer 2026-09-24 (finding từ vòng merge patch).
    if url and not url.startswith(("mqtt://", "tcp://")):
        add("EDGE_MQTT_CONSUMER_URL", "Must start with mqtt:// or tcp:// (TLS not supported yet)")
    for key in ("EDGE_MQTT_CONSUMER", "EDGE_MQTT_CONSUMER_FORWARD"):
        v = (values.get(key) or "").strip().lower()
        if v and v not in ("true", "false", "1", "0", "yes", "no", "on", "off"):
            add(key, "Must be true or false")
    base_url = values.get("EDGE_BASE_URL", "")
    if base_url and not base_url.startswith(("http://", "https://")):
        add("EDGE_BASE_URL", "Must start with http:// or https:// (or be left blank)")
    if not values.get("EDGE_STATE_DIR", "").strip():
        add("EDGE_STATE_DIR", "Must not be blank")
    for key in _INT_FIELDS:
        raw = values.get(key, "")
        try:
            if int(raw) <= 0:
                add(key, "Must be a positive integer")
        except ValueError:
            add(key, "Must be an integer")
    for key in _FLOAT_FIELDS:
        raw = values.get(key, "")
        try:
            fval = float(raw)
            if not math.isfinite(fval) or fval <= 0:
                add(key, "Must be a finite positive number (not nan/inf)")
        except ValueError:
            add(key, "Must be a number")
    port = values.get("EDGE_LISTEN_PORT", "")
    if port.isdigit() and not (1 <= int(port) <= 65535):
        add("EDGE_LISTEN_PORT", "Must be in the range 1-65535")
    fwd_ips = values.get("EDGE_FORWARDED_ALLOW_IPS", "")
    if fwd_ips and fwd_ips != "*":
        # uvicorn's _TrustedHosts KHÔNG bao giờ crash trên giá trị sai (rơi
        # về "literal", không bao giờ khớp client thật - fail-closed an
        # toàn) nên trước đây sai chính tả ở đây sẽ IM LẶNG không có tác
        # dụng gì, không ai biết TẠI SAO. Validate sớm để báo lỗi ngay lúc
        # Save thay vì phải tự suy đoán sau khi restart - xem
        # python-reviewer 2026-09-17.
        for part in fwd_ips.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                # KHÔNG truyền strict=False - uvicorn's _TrustedHosts.__init__
                # (proxy_headers.py) gọi ipaddress.ip_network(host) MẶC ĐỊNH
                # strict=True. Nếu validate ở đây lỏng hơn (strict=False), 1
                # CIDR có host-bits-set (vd "10.0.0.5/24" thay vì đúng
                # "10.0.0.0/24") sẽ PASS validate nhưng uvicorn thật lại rớt
                # về ValueError -> coi là literal -> không bao giờ khớp
                # client thật -> Save "thành công" nhưng proxy KHÔNG được
                # trust, tái diễn chính triệu chứng CSRF false-reject ban đầu
                # mà finding này sinh ra để chặn - xem python-reviewer
                # 2026-09-17 (vòng 2, phát hiện bằng thực nghiệm).
                ipaddress.ip_network(part)
            except ValueError:
                add("EDGE_FORWARDED_ALLOW_IPS",
                    "Invalid IP/CIDR: %r (comma-separated IPs/CIDRs, or \"*\")" % part)
                break
    return errors


def _render_field(f: dict, values: dict, errors: Dict[str, List[str]]) -> str:
    key = f["key"]
    val = html.escape(str(values.get(key, "")))
    field_errors = errors.get(key, [])
    invalid_cls = " invalid" if field_errors else ""
    describedby = "%s-hint" % key
    error_html = ""
    if field_errors:
        describedby += " %s-error" % key
        error_html = '<p class="field-error" id="%s-error">%s</p>' % (
            key, html.escape("; ".join(field_errors)))
    badge = ('<span class="badge">%s restart</span>' % _ICON_RESTART
              if key in RESTART_REQUIRED_KEYS else "")
    input_type = f.get("input_type", "text")
    return (
        '<div class="field">'
        '<label for="%s">%s%s</label>'
        '<input type="%s" class="%s" id="%s" name="%s" value="%s" aria-describedby="%s" %s>'
        '<p class="hint" id="%s-hint">%s</p>'
        '%s'
        '</div>'
    ) % (key, html.escape(f["label"]), badge,
         input_type, invalid_cls.strip(), key, key, val, describedby,
         'aria-invalid="true"' if field_errors else "",
         key, html.escape(f["hint"]), error_html)


def _render(values: dict, errors: "Dict[str, List[str]]" = None, saved: bool = False,
            api_key_masked: str = None, env_fingerprint: str = None) -> str:
    errors = errors or {}
    if env_fingerprint is None:
        env_fingerprint = _env_fingerprint(_ENV_PATH)
    sections = []
    for gkey, gtitle, gdesc in _GROUPS:
        fields_html = "".join(_render_field(f, values, errors)
                               for f in _FIELDS if f["group"] == gkey)
        if gkey == "network":
            fields_html += _api_key_field_html(api_key_masked)
        sections.append(
            '<fieldset><legend>%s</legend><p class="group-desc">%s</p>%s</fieldset>'
            % (html.escape(gtitle), html.escape(gdesc), fields_html)
        )

    banner = ""
    if errors:
        items = []
        for key, msgs in errors.items():
            label = html.escape(_LABELS.get(key, "This form"))
            for msg in msgs:
                if key in _LABELS:
                    items.append('<li><a href="#%s">%s: %s</a></li>'
                                 % (key, label, html.escape(msg)))
                else:
                    items.append('<li>%s</li>' % html.escape(msg))
        banner = (
            '<div class="banner error" role="alert" tabindex="-1" id="error-summary">'
            '%s<div><h2>There is a problem</h2><ul>%s</ul></div></div>'
        ) % (_ICON_ALERT, "".join(items))
    elif saved:
        banner = (
            '<div class="banner success" role="status">%s'
            '<div>Saved and applied immediately. Fields marked <b>restart</b> still need '
            'edge_collector restarted to take effect.</div></div>'
        ) % _ICON_CHECK

    focus_script = (
        "<script>var s=document.getElementById('error-summary');if(s){s.focus();}</script>"
        if errors else ""
    )

    return """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PCM Edge Collector - Settings</title>
<style>%s</style>
</head>
<body>
<div class="layout">
<div class="wrap">
<h1>Edge Collector Settings</h1>
<p class="lede">Configure this edge without editing <code>.env</code> by hand.</p>
<p class="notice">%s <span>Saving applies most changes immediately. Fields marked <b>restart</b> need edge_collector restarted (socket/storage opened once at startup).</span></p>
%s
<form method="post" action="/setup" novalidate>
<input type="hidden" name="_env_fingerprint" value="%s">
%s
<div class="actions"><button type="submit">Save</button></div>
</form>
</div>
<aside class="activity" aria-label="Live activity">
<h2>Live activity</h2>
<p class="group-desc">Recent readings pushed by node_agent devices, updates every few seconds.</p>
<ul class="activity-list" id="activity-list"><li class="activity-empty">Waiting for data...</li></ul>
</aside>
<aside class="activity" aria-label="PCM requests">
<h2>PCM requests</h2>
<p class="group-desc">Recent calls from Odoo Main into this edge (commands, live reads, browse, tests).</p>
<ul class="activity-list" id="pcm-requests-list"><li class="activity-empty">Waiting for data...</li></ul>
</aside>
</div>
%s
%s
%s
%s
</body></html>""" % (_CSS, _ICON_RESTART, banner, html.escape(env_fingerprint), "".join(sections),
                     focus_script, _ACTIVITY_SCRIPT, _API_KEY_SCRIPT, _PCM_REQUESTS_SCRIPT)


def _is_same_origin(request: Request) -> bool:
    """Chặn CSRF: form POST từ trang KHÁC (kiểu simple-request, không bị CORS
    chặn gửi đi) vẫn có thể đổi EDGE_MAIN_URL của nạn nhân nếu không kiểm tra
    này - xem review 2026-09-17. Trình duyệt cũ không gửi Origin/Referer cho
    POST cùng gốc -> không có gì để đối chiếu thì cho qua (best-effort, không
    phải auth thật, dùng đúng mức với threat-model LAN hiện tại)."""
    origin = request.headers.get("origin") or request.headers.get("referer")
    if not origin:
        return True
    try:
        parsed = urlsplit(origin)
    except ValueError:
        return False
    return (parsed.scheme, parsed.netloc) == (request.url.scheme, request.url.netloc)


def _mask_api_key(key) -> str:
    """Che tất cả trừ 4 ký tự cuối (độ dài cố định 6 chấm, KHÔNG tỉ lệ theo
    độ dài key thật - tránh lộ luôn metadata độ dài). Dùng cho HIỂN THỊ TRÊN
    MÀN HÌNH; giá trị THẬT chỉ được trả qua GET /setup/api_key khi bấm nút
    Copy, KHÔNG bao giờ nhúng vào HTML ban đầu - /setup có thể truy cập qua
    domain public (tunnel, xem review 2026-09-17 vụ CSRF), giữ nguyên quyết
    định của Nam: mask trên màn hình đúng mức dù vẫn cho copy full value khi
    cần dùng ở nơi khác (vd dán vào Postman để debug)."""
    if not key:
        return None
    key = str(key)
    if len(key) <= 8:
        # Key thật quá ngắn (kiểu độ dài bất thường - key thật do Odoo sinh
        # thường là UUID/hex dài) - str(key)[-4:] trên chuỗi <=4 ký tự sẽ trả
        # về NGUYÊN VẸN cả chuỗi, làm "mask" lộ 100% key. Coi đây là dấu hiệu
        # bất thường, che toàn bộ thay vì lộ nốt - xem python-reviewer
        # 2026-09-17 (vòng review API key feature).
        return "••••••"
    return "••••••" + key[-4:]


def _api_key_field_html(masked: str) -> str:
    if masked:
        return (
            '<div class="field">'
            '<label>Odoo API key</label>'
            '<div class="readonly-row">'
            '<p class="readonly-value">%s</p>'
            '<button type="button" class="copy-btn" id="copy-api-key-btn">Copy</button>'
            '</div>'
            '<p class="hint">Learned from Odoo after the first successful hello - '
            'read-only, not stored in .env</p>'
            '</div>'
        ) % html.escape(masked)
    return (
        '<div class="field">'
        '<label>Odoo API key</label>'
        '<p class="readonly-value">Not yet received (waiting for first hello to Odoo)</p>'
        '<p class="hint">Learned from Odoo after the first successful hello - '
        'read-only, not stored in .env</p>'
        '</div>'
    )


def _current_api_key_masked(request: Request) -> str:
    store = getattr(request.app.state, "store", None)
    key = store.kv_get("api_key") if store is not None else None
    return _mask_api_key(key)


def _check_setup_auth(request: Request) -> "Response | None":
    """Gate HTTP Basic Auth cho TOÀN BỘ bề mặt /setup/* - CHỈ active khi Nam
    đã đặt EDGE_SETUP_TOKEN (mặc định rỗng = không gate, tương thích ngược
    với deployment cũ chưa cấu hình, giữ đúng threat-model LAN-only gốc).

    Sinh ra vì GET /setup/api_key trả RAW credential (không phải dữ liệu đo
    như /setup/activity) - ai có URL (vd qua tunnel công khai) là lấy được
    bằng 1 lệnh curl, dùng để mạo danh edge này gọi thẳng Odoo TỪ BẤT KỲ ĐÂU,
    vượt hẳn biên giới 'LAN trust' của toàn bộ /setup. Origin-check
    (_is_same_origin) KHÔNG chặn được vector này (chủ động cho qua khi
    THIẾU header Origin - dùng 1 curl/script thường không gửi Origin) - xem
    python-reviewer 2026-09-17. secrets.compare_digest để tránh timing
    attack độ dài token đúng."""
    token = settings.setup_token
    if not token:
        return None
    auth = request.headers.get("authorization", "")
    supplied = ""
    if auth.startswith("Basic "):
        try:
            decoded = base64.b64decode(auth[6:]).decode("utf-8", "replace")
            _, _, supplied = decoded.partition(":")
        except (ValueError, UnicodeDecodeError):
            supplied = ""
    # encode() sang bytes TRƯỚC khi so - secrets.compare_digest(str, str) RAISE
    # TypeError nếu 1 trong 2 chuỗi có ký tự non-ASCII (giới hạn riêng của
    # biến thể str-str, KHÔNG áp dụng cho bytes-bytes). Ai đó gõ đại 1 mật
    # khẩu chứa ký tự non-ASCII (không cần biết token thật) sẽ làm route
    # crash 500 thay vì 401 đúng thiết kế - xem python-reviewer 2026-09-17
    # (vòng verify auth gate, bắt bằng thực nghiệm TestClient thật).
    if supplied and secrets.compare_digest(supplied.encode("utf-8"), token.encode("utf-8")):
        return None
    return Response(status_code=401, headers={"WWW-Authenticate": 'Basic realm="edge setup"'})


@router.get("/setup", response_class=HTMLResponse)
async def setup_get(request: Request):
    denied = _check_setup_auth(request)
    if denied:
        return denied
    return _render(_current_values(), api_key_masked=_current_api_key_masked(request))


@router.get("/setup/api_key")
async def setup_api_key(request: Request):
    """Trả RAW api_key thật - CHỈ gọi khi Nam bấm nút Copy (xem
    _API_KEY_SCRIPT), không bao giờ tự động nhúng vào HTML/_render(). KHÁC
    /setup/activity về mức độ rủi ro (đó là dữ liệu đo, đây là 1 CREDENTIAL
    sống dùng để mạo danh edge gọi Odoo) - _check_setup_auth() là gate BẮT
    BUỘC cho endpoint này khi EDGE_SETUP_TOKEN đã được cấu hình, xem
    python-reviewer 2026-09-17 (không còn chỉ dựa vào "cùng threat-model
    LAN/tunnel" như trước)."""
    denied = _check_setup_auth(request)
    if denied:
        return denied
    store = getattr(request.app.state, "store", None)
    key = store.kv_get("api_key") if store is not None else None
    return {"api_key": key}


@router.get("/setup/activity")
async def setup_activity(request: Request):
    """Nguồn dữ liệu cho panel 'Live activity' - đọc lại Store.history (đã
    được EdgeAgent._on_value ghi sẵn mỗi lần node_agent đẩy đo lên, xem
    node_api.py/scheduler.py), KHÔNG mở kênh log riêng. store có thể None
    khi test dùng FastAPI() trần (không qua lifespan thật) - trả rỗng an toàn,
    giống pattern agent=None ở setup_post()."""
    denied = _check_setup_auth(request)
    if denied:
        return denied
    store = getattr(request.app.state, "store", None)
    rows = store.history_recent(limit=50) if store is not None else []
    now = time.time()
    return {
        "rows": [
            {
                "serial": r["serial"], "ch": r["ch"], "v": r["v"], "s": r["s"],
                "q": r["q"], "stable": r["stable"],
                "age_s": max(0, int(now - r["ts"])),
            }
            for r in rows
        ]
    }


_COMMAND_LABELS = {
    "zero": "Zero", "tare": "Tare", "read": "Read value", "write": "Write value",
    "restart": "Restart node", "reset_cycle": "Reset cycle",
}


def _summarize_pcm_request(r: dict) -> str:
    """Tóm tắt 1 dòng ngắn gọn, dễ đọc cho panel 'PCM requests' - đặt tên
    theo đúng label UI Odoo (pcm_base) để Nam thấy quen mắt, không phải raw
    endpoint path. Nguồn: hỏi session Odoo 2026-09-17 - mọi request đều do
    THAO TÁC NGƯỜI DÙNG kích hoạt (nút Zero/Tare/Restart, tablet worker,
    connection test, tag browser, workflow process...), KHÔNG có cron nào
    tự poll các endpoint này."""
    ep = r.get("endpoint")
    if ep == "/api/command":
        label = _COMMAND_LABELS.get(r.get("cmd"), r.get("cmd") or "?")
        return "%s on %s / %s" % (label, r.get("serial"), r.get("ch"))
    if ep == "/api/latest":
        return "Read live value %s / %s" % (r.get("serial"), r.get("ch"))
    if ep == "/api/browse":
        return "Browse tags on '%s'" % r.get("source")
    if ep == "/api/source/test":
        return "Connection test (%s)" % (r.get("kind") or "unknown kind")
    if ep == "/api/stats":
        return "Channel statistics %s / %s (last %sh)" % (
            r.get("serial"), r.get("ch"), r.get("hours"))
    return ep or "unknown"


@router.get("/setup/pcm_requests")
async def setup_pcm_requests(request: Request):
    """Nguồn dữ liệu cho panel 'PCM requests' - CHIỀU NGƯỢC LẠI với
    /setup/activity: đây là Odoo Main gọi XUỐNG edge này (inbound_api.py),
    không phải node_agent đẩy lên. Đọc ring-buffer trong-bộ-nhớ
    (inbound_api.recent_requests()), không persist SQLite - mất khi restart
    là chấp nhận được (telemetry hiển thị, khác history/outbox cần durable)."""
    denied = _check_setup_auth(request)
    if denied:
        return denied
    now = time.time()
    rows = inbound_api.recent_requests()
    return {
        "rows": [
            {"endpoint": r.get("endpoint"), "summary": _summarize_pcm_request(r),
             "age_s": max(0, int(now - r["ts"]))}
            for r in rows
        ]
    }


@router.post("/setup", response_class=HTMLResponse)
async def setup_post(request: Request):
    denied = _check_setup_auth(request)
    if denied:
        return denied
    if not _is_same_origin(request):
        return HTMLResponse(
            _render(_current_values(),
                    errors={"_form": ["Rejected: request did not originate from the /setup "
                                       "page (possible CSRF) - reopen /setup and save from "
                                       "that page"]},
                    api_key_masked=_current_api_key_masked(request)),
            status_code=403,
        )
    form = await request.form()
    values = {f["key"]: str(form.get(f["key"], "")).strip() for f in _FIELDS}
    submitted_fingerprint = str(form.get("_env_fingerprint", "")).strip()
    current_fingerprint = _env_fingerprint(_ENV_PATH)
    # Bỏ qua guard khi field rỗng (form cũ từ bản image TRƯỚC khi tính năng
    # này tồn tại, không có hidden field) - best-effort, giống triết lý
    # "không có gì để đối chiếu thì cho qua" của _is_same_origin() ở trên.
    if submitted_fingerprint and submitted_fingerprint != current_fingerprint:
        # _current_values() (KHÔNG PHẢI `values` vừa submit) - trang lỗi này
        # từ render() tính fingerprint MỚI (khớp file thật trên đĩa) cho hidden
        # field, nên NẾU vẫn hiển thị `values` (dữ liệu cũ bị từ chối) thì bấm
        # Save lại NGAY trên chính trang lỗi (không cần reload) sẽ qua được
        # guard và ghi đè mất thay đổi bên ngoài - tái diễn đúng bug gốc tính
        # năng này sinh ra để chặn. Nhất quán với 2 nhánh lỗi khác trong hàm
        # này (CSRF 403, OSError 500) đều đã dùng _current_values() - xem
        # python-reviewer 2026-09-25 (finding Critical, verify thực nghiệm).
        return HTMLResponse(
            _render(_current_values(),
                    errors={"_form": ["Config changed elsewhere since this page was loaded "
                                       "(e.g. edited via SSH, or saved from another /setup tab) "
                                       "- reopen /setup to see the latest values, then re-apply "
                                       "your change."]},
                    api_key_masked=_current_api_key_masked(request)),
            status_code=409,
        )
    errors = _validate(values)
    if errors:
        return HTMLResponse(
            _render(values, errors=errors, api_key_masked=_current_api_key_masked(request)),
            status_code=400)
    if not values["EDGE_CODE"]:
        # Để trống = "giữ nguyên" (đúng UX hint "leave blank to auto-generate
        # on first run") - PHẢI ghi giá trị ĐANG HIỆU LỰC THẬT (settings.edge_code,
        # có thể đã tự sinh từ lần startup trước) xuống .env, KHÔNG ghi rỗng.
        # Ghi rỗng sẽ khiến restart THẬT sau này (Settings.__init__) tự sinh 1
        # edge_code MỚI khác hẳn code đang chạy - edge mất khớp với pcm.edge.code
        # đã đăng ký bên Odoo, đứt kết nối im lặng - xem review 2026-09-17
        # (tính năng hot-reload).
        values["EDGE_CODE"] = settings.edge_code
    try:
        _write_env_file(_ENV_PATH, values)
    except OSError as exc:
        # Container chạy non-root (uid 1000, xem Dockerfile) - .env bind-mount
        # từ host có thể không writable bởi uid đó (vd tạo bởi root/user
        # khác). Không bắt sẽ rớt thành 500 không rõ nghĩa; bắt lại và trả
        # lỗi rõ ràng HƠN để Nam biết phải chỉnh permission host, KHÔNG phải
        # bug logic - xem docker-reviewer 2026-09-17.
        return HTMLResponse(
            _render(_current_values(),
                    errors={"_form": ["Could not write .env: %s - check that this file "
                                       "is writable by the container's uid 1000 (see "
                                       "README, section 'Running with Docker')" % exc]},
                    api_key_masked=_current_api_key_masked(request)),
            status_code=500,
        )
    reload_settings(_ENV_PATH)
    # EdgeAgent/OdooClient chỉ tồn tại khi chạy qua app.py thật (lifespan đã
    # gán app.state.agent) - test dùng FastAPI() trần nên không có, bỏ qua an
    # toàn. httpx.AsyncClient bake base_url lúc __init__ nên cần gọi tường
    # minh để EDGE_MAIN_URL mới có hiệu lực ngay (xem OdooClient.refresh_base_url).
    agent = getattr(request.app.state, "agent", None)
    if agent is not None:
        agent.odoo.refresh_base_url()
    return _render(values, saved=True, api_key_masked=_current_api_key_masked(request))
