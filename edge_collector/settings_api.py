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
mo co dinh / uvicorn da doc gia tri nay luc startup); 11 field con lai (Main
URL/Edge code/Name/Platform/Base URL + 6 interval) ap dung NGAY sau Save,
khong can restart - xem review 2026-09-17 (tinh nang hot-reload, phat sinh tu
cau hoi thuc te cua Nam).

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
import html
import ipaddress
import math
import time
from pathlib import Path
from typing import Dict, List
from urllib.parse import urlsplit

from dotenv.main import dotenv_values
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

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
.layout { max-width: 1120px; margin: 0 auto; display: grid; grid-template-columns: 760px 1fr; gap: 24px; align-items: start; }
@media (max-width: 1080px) { .layout { grid-template-columns: 1fr; max-width: 760px; } }
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
input[type=text] {
  width: 100%; font: inherit; color: var(--fg); background: var(--input-bg);
  border: 1px solid var(--input-border); border-radius: 8px; padding: 8px 11px;
  transition: border-color 150ms, box-shadow 150ms;
}
input[type=text]:focus-visible { outline: none; border-color: var(--ring); box-shadow: 0 0 0 3px var(--ring-glow); }
input[type=text].invalid { border-color: var(--danger); }
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
    return (
        '<div class="field">'
        '<label for="%s">%s%s</label>'
        '<input type="text" class="%s" id="%s" name="%s" value="%s" aria-describedby="%s" %s>'
        '<p class="hint" id="%s-hint">%s</p>'
        '%s'
        '</div>'
    ) % (key, html.escape(f["label"]), badge,
         invalid_cls.strip(), key, key, val, describedby,
         'aria-invalid="true"' if field_errors else "",
         key, html.escape(f["hint"]), error_html)


def _render(values: dict, errors: "Dict[str, List[str]]" = None, saved: bool = False) -> str:
    errors = errors or {}
    sections = []
    for gkey, gtitle, gdesc in _GROUPS:
        fields_html = "".join(_render_field(f, values, errors)
                               for f in _FIELDS if f["group"] == gkey)
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
<p class="notice">%s Saving applies most changes immediately. Fields marked <b>restart</b> need edge_collector restarted (socket/storage opened once at startup).</p>
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
</div>
%s
%s
</body></html>""" % (_CSS, _ICON_RESTART, banner, "".join(sections), focus_script, _ACTIVITY_SCRIPT)


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


@router.get("/setup", response_class=HTMLResponse)
async def setup_get():
    return _render(_current_values())


@router.get("/setup/activity")
async def setup_activity(request: Request):
    """Nguon du lieu cho panel 'Live activity' - doc lai Store.history (da
    duoc EdgeAgent._on_value ghi san moi lan node_agent day do len, xem
    node_api.py/scheduler.py), KHONG mo kenh log rieng. store co the None
    khi test dung FastAPI() tran (khong qua lifespan that) - tra rong an toan,
    giong pattern agent=None o setup_post()."""
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


@router.post("/setup", response_class=HTMLResponse)
async def setup_post(request: Request):
    if not _is_same_origin(request):
        return HTMLResponse(
            _render(_current_values(),
                    errors={"_form": ["Rejected: request did not originate from the /setup "
                                       "page (possible CSRF) - reopen /setup and save from "
                                       "that page"]}),
            status_code=403,
        )
    form = await request.form()
    values = {f["key"]: str(form.get(f["key"], "")).strip() for f in _FIELDS}
    errors = _validate(values)
    if errors:
        return HTMLResponse(_render(values, errors=errors), status_code=400)
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
                                       "README, section 'Running with Docker')" % exc]}),
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
    return _render(values, saved=True)
