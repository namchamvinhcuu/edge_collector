# -*- coding: utf-8 -*-
"""Trang web cau hinh EDGE_* (thay the sua file .env bang tay):
    GET  /setup   form hien gia tri dang co trong .env (hoac mac dinh neu chua co file)
    POST /setup   ghi lai .env tai cho (khong tempfile+rename - xem _write_env_file,
                  giu nguyen dong/comment khac) ROI hot-reload ngay singleton
                  `settings` + base_url cua OdooClient dang chay (xem
                  config.reload_settings()/OdooClient.refresh_base_url())

Cung muc do tin cay LAN nhu inbound_api.py - mac dinh nham LAN, nhung co the
dat sau reverse-proxy/tunnel (vd truy cap qua domain public) NEU cau hinh dung
EDGE_FORWARDED_ALLOW_IPS (xem duoi). CHI 4 field (EDGE_LISTEN_HOST/PORT,
EDGE_STATE_DIR, EDGE_FORWARDED_ALLOW_IPS - xem config.RESTART_REQUIRED_KEYS)
van can KHOI DONG LAI edge_collector moi ap dung (socket da bind / SQLite da
mo co dinh / uvicorn da doc gia tri nay luc startup); 12 field con lai (Main
URL/Edge code/Name/Platform/Base URL/Setup access token + 6 interval) ap dung
NGAY sau Save, khong can restart - xem review 2026-09-17 (tinh nang
hot-reload, phat sinh tu cau hoi thuc te cua Nam).

EDGE_SETUP_TOKEN: rong (mac dinh) = KHONG gate gi (giu nguyen threat-model
LAN-only cu). Dat 1 gia tri de yeu cau HTTP Basic Auth (username bat ky,
password = token nay) cho TOAN BO /setup/GET/POST/activity/api_key - xem
_check_setup_auth(). Sinh ra vi GET /setup/api_key tra RAW credential (Odoo
api_key) khong auth, va /setup gio co the truy cap qua domain public (xem
EDGE_FORWARDED_ALLOW_IPS o tren) - anh co URL la lay duoc key, dung de mao
danh edge nay goi thang Odoo tu bat ky dau. STRONGLY RECOMMENDED dat gia tri
nay khi /setup duoc tunnel ra ngoai LAN - xem python-reviewer 2026-09-17.

EDGE_FORWARDED_ALLOW_IPS: IP/CIDR cua reverse-proxy/tunnel duoc TIN de doc
X-Forwarded-Proto/-Host, truyen thang cho uvicorn's ProxyHeadersMiddleware.
Mac dinh "127.0.0.1,::1" (chi trust loopback, dung y het uvicorn) - KHONG
doi hanh vi LAN-only hien tai. Neu /setup duoc truy cap qua 1 reverse-proxy/
tunnel TLS-terminating (Origin browser la https:// nhung ket noi TCP toi
uvicorn van la http://), _is_same_origin() ben duoi se so sai scheme (Origin
https != request.url.scheme http) va reject nham request Save hop le - da
gap thuc te 2026-09-17 (Nam tunnel /setup qua domain public). Fix: dat field
nay dung IP/CIDR cua proxy/tunnel do (KHONG dat "*" tru khi da chan chac
chan khong ai khac gui thang toi cong nay duoc, vi "*" se trust
X-Forwarded-Proto tu BAT KY client nao, mo duong gia mao Origin qua header).
"""
import base64
import html
import ipaddress
import math
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

# UI-facing strings deu bang tieng Anh theo yeu cau Nam. Moi field: key (khop
# .env), label, gia tri mac dinh, hint, group ("connection"/"network"/"timing").
# Field nao can restart moi ap dung -> xem config.RESTART_REQUIRED_KEYS (dinh
# nghia 1 noi DUY NHAT, tranh 2 cho liet ke lech nhau).
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


# Poll rieng /setup/activity (khong dung WebSocket - project chua co tien le,
# polling JSON khop pattern san co /api/latest, /api/stats). esc() bat buoc
# cho MOI truong tu dong node (serial/ch/gia tri chuoi 's') truoc khi noi vao
# innerHTML - day la du lieu tu thiet bi ngoai (node_agent), khong phai
# hang-code, phai coi la KHONG dang tin de tranh XSS luu tru qua history.
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

# Panel "PCM requests" - CHIEU NGUOC LAI voi Live activity (Odoo Main goi
# XUONG edge nay, xem inbound_api.py.recent_requests()/_log_request()).
# summary da duoc build san o server (_summarize_pcm_request) - JS chi hien
# thi, khong tu suy doan tung field khac nhau giua cac endpoint.
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

# Nut Copy KHONG bao gio doc gia tri tu DOM/HTML da render (o do chi co ban
# mask) - luon fetch /setup/api_key rieng luc bam, giu dung nguyen tac "full
# secret khong nam san trong response ban dau" du van cho phep copy khi can.
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
    // navigator.clipboard.writeText CHI hoat dong trong secure context
    // (HTTPS hoac localhost) - edge nay thuong truy cap qua LAN HTTP thuong
    // (threat-model goc), nen can fallback nay cho truong hop do, khong chi
    // dua vao Clipboard API hien dai - xem bug bao cao 2026-09-17 (nut Copy
    // "khong hoat dong" khi truy cap qua http://192.168.x.x thuong).
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


def _format_env_line(key: str, value: str) -> str:
    """Quote logic khop CHINH XAC python-dotenv set_key(quote_mode='auto')
    (dotenv/main.py set_key()) - dung lai de KHONG lap lai bug '#'-truncation
    da sua truoc do (gia tri co khoang trang/'#' phai duoc quote)."""
    if value.isalnum():
        value_out = value
    else:
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        value_out = "'%s'" % escaped
    return "%s=%s\n" % (key, value_out)


def _write_env_file(path: Path, values: dict) -> None:
    """Ghi TRUC TIEP vao file dang co (truncate + write, KHONG tempfile+
    os.replace) - python-dotenv's set_key() dung chien luoc atomic-rewrite
    (tempfile roi os.replace) se crash 'OSError: [Errno 16] Device or
    resource busy' khi .env la bind-mount 1 FILE rieng trong Docker (khong
    the rename() de vao dung inode dang bi mount) - xem review 2026-09-17.
    Doi lai: mat tinh atomic (crash giua chung co the de file dang do), chap
    nhan duoc cho 1 form cau hinh it khi luu, doi lai chay duoc ca venv lan
    Docker khong can doi cau truc bind-mount.

    encoding="utf-8" tuong minh o ca doc lan ghi - _current_values() (qua
    dotenv_values()) va con `Settings` phia config.py deu gia dinh utf-8; neu
    de mac dinh se roi ve locale.getpreferredencoding() cua platform (vd
    ANSI codepage tren Windows - EDGE_PLATFORM=windows la target that duoc
    khai bao trong _FIELDS), gay lech encoding giua ghi/doc cho gia tri co
    dau (vd EDGE_NAME tieng Viet) - xem review 2026-09-17."""
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
                # Key TRUNG LAP da co san trong file va DA duoc ghi 1 lan roi -
                # bo dong nay, khong de 2 gia tri cho cung 1 key ton tai
                # (dotenv doc lai theo kieu "dong sau de dong truoc", se lam
                # gia tri vua Save bi vo hieu am tham) - xem review 2026-09-17.
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
    # CHI 2 scheme nay - mqtt_consumer._split_url() chi biet strip "mqtt://"/
    # "tcp://" va khong goi tls_set() o dau ca, nen "mqtts://"/"ssl://" se bi
    # parse SAI (host thanh chuoi "mqtts" thay vi hostname that) ma khong bao
    # loi gi - xem python-reviewer 2026-09-24 (finding tu vong merge patch).
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
        # uvicorn's _TrustedHosts KHONG bao gio crash tren gia tri sai (roi
        # ve "literal", khong bao gio khop client that - fail-closed an
        # toan) nen truoc day sai chinh ta o day se IM LANG khong co tac
        # dung gi, khong ai biet TAI SAO. Validate som de bao loi ngay luc
        # Save thay vi phai tu suy doan sau khi restart - xem
        # python-reviewer 2026-09-17.
        for part in fwd_ips.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                # KHONG truyen strict=False - uvicorn's _TrustedHosts.__init__
                # (proxy_headers.py) goi ipaddress.ip_network(host) MAC DINH
                # strict=True. Neu validate o day long hon (strict=False), 1
                # CIDR co host-bits-set (vd "10.0.0.5/24" thay vi dung
                # "10.0.0.0/24") se PASS validate nhung uvicorn that lai rot
                # ve ValueError -> coi la literal -> khong bao gio khop
                # client that -> Save "thanh cong" nhung proxy KHONG duoc
                # trust, tai dien chinh trieu chung CSRF false-reject ban dau
                # ma finding nay sinh ra de chan - xem python-reviewer
                # 2026-09-17 (vong 2, phat hien bang thuc nghiem).
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
            api_key_masked: str = None) -> str:
    errors = errors or {}
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
</body></html>""" % (_CSS, _ICON_RESTART, banner, "".join(sections), focus_script,
                     _ACTIVITY_SCRIPT, _API_KEY_SCRIPT, _PCM_REQUESTS_SCRIPT)


def _is_same_origin(request: Request) -> bool:
    """Chan CSRF: form POST tu trang KHAC (kieu simple-request, khong bi CORS
    chan gui di) van co the doi EDGE_MAIN_URL cua nan nhan neu khong kiem tra
    nay - xem review 2026-09-17. Trinh duyet cu khong gui Origin/Referer cho
    POST cung goc -> khong co gi de doi chieu thi cho qua (best-effort, khong
    phai auth that, dung dung muc voi threat-model LAN hien tai)."""
    origin = request.headers.get("origin") or request.headers.get("referer")
    if not origin:
        return True
    try:
        parsed = urlsplit(origin)
    except ValueError:
        return False
    return (parsed.scheme, parsed.netloc) == (request.url.scheme, request.url.netloc)


def _mask_api_key(key) -> str:
    """Che tat ca tru 4 ky tu cuoi (do dai co dinh 6 cham, KHONG ti le theo
    do dai key that - tranh lo luon metadata do dai). Dung cho HIEN THI TREN
    MAN HINH; gia tri THAT chi duoc tra qua GET /setup/api_key khi bam nut
    Copy, KHONG bao gio nhung vao HTML ban dau - /setup co the truy cap qua
    domain public (tunnel, xem review 2026-09-17 vu CSRF), giu nguyen quyet
    dinh cua Nam: mask tren man hinh dung muc du van cho copy full value khi
    can dung o noi khac (vd dan vao Postman de debug)."""
    if not key:
        return None
    key = str(key)
    if len(key) <= 8:
        # Key that qua ngan (kieu do dai bat thuong - key that do Odoo sinh
        # thuong la UUID/hex dai) - str(key)[-4:] tren chuoi <=4 ky tu se tra
        # ve NGUYEN VEN ca chuoi, lam "mask" lo 100% key. Coi day la dau hieu
        # bat thuong, che toan bo thay vi lo not - xem python-reviewer
        # 2026-09-17 (vong review API key feature).
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
    """Gate HTTP Basic Auth cho TOAN BO be mat /setup/* - CHI active khi Nam
    da dat EDGE_SETUP_TOKEN (mac dinh rong = khong gate, tuong thich nguoc
    voi deployment cu chua cau hinh, giu dung threat-model LAN-only goc).

    Sinh ra vi GET /setup/api_key tra RAW credential (khong phai du lieu do
    nhu /setup/activity) - ai co URL (vd qua tunnel cong khai) la lay duoc
    bang 1 lenh curl, dung de mao danh edge nay goi thang Odoo TU BAT KY DAU,
    vuot han bien gioi 'LAN trust' cua toan bo /setup. Origin-check
    (_is_same_origin) KHONG chan duoc vector nay (chu dong cho qua khi
    THIEU header Origin - dung 1 curl/script thuong khong gui Origin) - xem
    python-reviewer 2026-09-17. secrets.compare_digest de tranh timing
    attack do dai token dung."""
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
    # encode() sang bytes TRUOC khi so - secrets.compare_digest(str, str) RAISE
    # TypeError neu 1 trong 2 chuoi co ky tu non-ASCII (gioi han rieng cua
    # bien the str-str, KHONG ap dung cho bytes-bytes). Ai do go dai 1 mat
    # khau chua ky tu non-ASCII (khong can biet token that) se lam route
    # crash 500 thay vi 401 dung thiet ke - xem python-reviewer 2026-09-17
    # (vong verify auth gate, bat bang thuc nghiem TestClient that).
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
    """Tra RAW api_key that - CHI goi khi Nam bam nut Copy (xem
    _API_KEY_SCRIPT), khong bao gio tu dong nhung vao HTML/_render(). KHAC
    /setup/activity ve muc do rui ro (do la du lieu do, day la 1 CREDENTIAL
    song dung de mao danh edge goi Odoo) - _check_setup_auth() la gate BAT
    BUOC cho endpoint nay khi EDGE_SETUP_TOKEN da duoc cau hinh, xem
    python-reviewer 2026-09-17 (khong con chi dua vao "cung threat-model
    LAN/tunnel" nhu truoc)."""
    denied = _check_setup_auth(request)
    if denied:
        return denied
    store = getattr(request.app.state, "store", None)
    key = store.kv_get("api_key") if store is not None else None
    return {"api_key": key}


@router.get("/setup/activity")
async def setup_activity(request: Request):
    """Nguon du lieu cho panel 'Live activity' - doc lai Store.history (da
    duoc EdgeAgent._on_value ghi san moi lan node_agent day do len, xem
    node_api.py/scheduler.py), KHONG mo kenh log rieng. store co the None
    khi test dung FastAPI() tran (khong qua lifespan that) - tra rong an toan,
    giong pattern agent=None o setup_post()."""
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
    """Tom tat 1 dong ngan gon, de doc cho panel 'PCM requests' - dat ten
    theo dung label UI Odoo (pcm_base) de Nam thay quen mat, khong phai raw
    endpoint path. Nguon: hoi session Odoo 2026-09-17 - moi request deu do
    THAO TAC NGUOI DUNG kich hoat (nut Zero/Tare/Restart, tablet worker,
    connection test, tag browser, workflow process...), KHONG co cron nao
    tu poll cac endpoint nay."""
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
    """Nguon du lieu cho panel 'PCM requests' - CHIEU NGUOC LAI voi
    /setup/activity: day la Odoo Main goi XUONG edge nay (inbound_api.py),
    khong phai node_agent day len. Doc ring-buffer trong-bo-nho
    (inbound_api.recent_requests()), khong persist SQLite - mat khi restart
    la chap nhan duoc (telemetry hien thi, khac history/outbox can durable)."""
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
    errors = _validate(values)
    if errors:
        return HTMLResponse(
            _render(values, errors=errors, api_key_masked=_current_api_key_masked(request)),
            status_code=400)
    if not values["EDGE_CODE"]:
        # De trong = "giu nguyen" (dung UX hint "leave blank to auto-generate
        # on first run") - PHAI ghi gia tri DANG HIEU LUC THAT (settings.edge_code,
        # co the da tu sinh tu lan startup truoc) xuong .env, KHONG ghi rong.
        # Ghi rong se khien restart THAT sau nay (Settings.__init__) tu sinh 1
        # edge_code MOI khac han code dang chay - edge mat khop voi pcm.edge.code
        # da dang ky ben Odoo, dut ket noi im lang - xem review 2026-09-17
        # (tinh nang hot-reload).
        values["EDGE_CODE"] = settings.edge_code
    try:
        _write_env_file(_ENV_PATH, values)
    except OSError as exc:
        # Container chay non-root (uid 1000, xem Dockerfile) - .env bind-mount
        # tu host co the khong writable boi uid do (vd tao boi root/user
        # khac). Khong bat se rot thanh 500 khong ro nghia; bat lai va tra
        # loi ro rang HON de Nam biet phai chinh permission host, KHONG phai
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
    # EdgeAgent/OdooClient chi ton tai khi chay qua app.py that (lifespan da
    # gan app.state.agent) - test dung FastAPI() tran nen khong co, bo qua an
    # toan. httpx.AsyncClient bake base_url luc __init__ nen can goi tuong
    # minh de EDGE_MAIN_URL moi co hieu luc ngay (xem OdooClient.refresh_base_url).
    agent = getattr(request.app.state, "agent", None)
    if agent is not None:
        agent.odoo.refresh_base_url()
    return _render(values, saved=True, api_key_masked=_current_api_key_masked(request))
