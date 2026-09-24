# -*- coding: utf-8 -*-
"""Trang van hanh cua edge: trang thai 4 den + nhat ky goi tin MQTT.

Vi sao o DAY chu khong o Odoo: trang nay phai xem duoc dung luc Odoo hong.
No doc thang tu MqttConsumer trong bo nho cua chinh tien trinh nay, khong
goi Odoo, khong cham SQLite, khong them mot byte nao len duong day. Mot
trang chan doan ma chet cung voi thu no dang chan doan thi vo dung.

Hai o:

    /ops            trang HTML (mot file, khong CDN — nha may co the khong
                    co Internet, va mot trang chan doan khong duoc phu thuoc
                    vao mang ben ngoai)
    /ops/api/state  JSON, trinh duyet hoi moi giay

Cung cong HTTP Basic voi /setup (EDGE_SETUP_TOKEN). Khac /healthz — cai do
dang mo cong khai qua domain.
"""
import time

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from .settings_api import _check_setup_auth

router = APIRouter()

@router.get("/ops", response_class=HTMLResponse)
async def ops_page(request: Request):
    denied = _check_setup_auth(request)
    if denied is not None:
        return denied
    return HTMLResponse(PAGE)


@router.get("/ops/api/state")
async def ops_state(request: Request, since: int = 0):
    denied = _check_setup_auth(request)
    if denied is not None:
        return denied
    agent = request.app.state.agent
    c = agent.mqtt_consumer
    events = [e for e in list(c.traffic) if e["seq"] > since]
    return JSONResponse({
        "now": time.time(),
        "stats": c.stats,
        "lamps": c.lamps,
        "caps": c.caps,
        "events": events,
        "cursor": c.traffic[-1]["seq"] if c.traffic else since,
        "outbox": agent.store.outbox_count(),
    })


PAGE = """<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Đèn &amp; gói tin</title>
<style>
  :root {
    --bg:#f6f6f7; --panel:#ffffff; --line:#e3e3e6; --ink:#1c1c1f;
    --muted:#6d6d76; --up:#0a7c5a; --down:#9a5b00; --warn:#b4451a;
    --mono: ui-monospace, "SF Mono", "Cascadia Mono", Menlo, Consolas, monospace;
  }
  @media (prefers-color-scheme: dark) {
    :root:not([data-theme="light"]) {
      --bg:#0f1012; --panel:#17181c; --line:#2a2b31; --ink:#ececed;
      --muted:#8d8f99; --up:#3dd68c; --down:#f0b429; --warn:#ff8a5c;
    }
  }
  * { box-sizing:border-box; }
  html, body { margin:0; }
  body {
    background:var(--bg); color:var(--ink); padding:16px;
    padding-block:calc(16px + env(safe-area-inset-top,0px)) calc(16px + env(safe-area-inset-bottom,0px));
    font:15px/1.5 system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
  }
  .wrap { max-width:1000px; margin:0 auto; display:flex; flex-direction:column; gap:16px; }
  header { display:flex; flex-wrap:wrap; align-items:baseline; gap:12px; }
  h1 { font-size:20px; margin:0; letter-spacing:-.01em; }
  .dot { width:9px; height:9px; border-radius:50%; display:inline-block; }
  .sub { color:var(--muted); font-size:13px; }
  .tabs { display:flex; gap:4px; }
  .tabs button {
    background:none; border:1px solid transparent; color:var(--muted);
    padding:6px 12px; border-radius:7px; font:inherit; font-size:14px; cursor:pointer;
  }
  .tabs button[aria-selected="true"] {
    background:var(--panel); border-color:var(--line); color:var(--ink); font-weight:600;
  }
  .lamps { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:12px; }
  .lamp {
    background:var(--panel); border:1px solid var(--line); border-radius:11px;
    padding:14px; display:flex; flex-direction:column; gap:9px;
  }
  .bulb {
    width:40px; height:40px; border-radius:50%;
    border:2px solid var(--line); background:#2a2b31;
  }
  .lamp .name { font-weight:600; }
  .lamp .state { font-size:22px; font-weight:700; letter-spacing:-.02em; }
  .lamp .age { color:var(--muted); font-size:12px; font-variant-numeric:tabular-nums; }
  .cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(118px,1fr)); gap:10px; }
  .card {
    background:var(--panel); border:1px solid var(--line); border-radius:9px; padding:10px 12px;
  }
  .card b { display:block; font-size:19px; font-variant-numeric:tabular-nums; }
  .card span { color:var(--muted); font-size:12px; }
  .logbox { background:var(--panel); border:1px solid var(--line); border-radius:11px; overflow:auto; }
  table { border-collapse:collapse; width:100%; font-family:var(--mono); font-size:12.5px; }
  th, td { text-align:left; padding:6px 10px; border-bottom:1px solid var(--line); white-space:nowrap; }
  th { color:var(--muted); font-weight:600; position:sticky; top:0; background:var(--panel); }
  td.note { white-space:normal; color:var(--muted); }
  .up { color:var(--up); } .down { color:var(--down); }
  .hide { display:none; }
  .foot { color:var(--muted); font-size:12px; }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>Đèn &amp; gói tin</h1>
    <span class="sub"><span class="dot" id="brokerDot"></span> <span id="brokerTxt">đang nối…</span></span>
    <span class="sub" id="nodeTxt"></span>
  </header>

  <div class="tabs" role="tablist">
    <button role="tab" aria-selected="true"  onclick="tab('lamps')">4 đèn</button>
    <button role="tab" aria-selected="false" onclick="tab('log')">Gói tin</button>
  </div>

  <section id="tab-lamps">
    <div class="lamps" id="lamps"></div>
    <p class="foot">Đây là trạng thái firmware <b>báo</b> đã ghi ra chân GPIO,
       không phải cảm biến đọc ngược từ bóng đèn — không có dây phản hồi nào
       từ đèn về node.</p>
  </section>

  <section id="tab-log" class="hide">
    <div class="cards" id="cards"></div>
    <div class="logbox" style="margin-top:12px; max-height:60vh">
      <table>
        <thead><tr><th>Lúc</th><th>Chiều</th><th>Chủ đề</th><th>Byte</th><th>Nội dung</th></tr></thead>
        <tbody id="log"></tbody>
      </table>
    </div>
    <p class="foot">Mọi gói MQTT đi qua tiến trình này, mới nhất trước.
       Giữ 300 gói gần nhất trong bộ nhớ.</p>
  </section>
</div>

<script>
var cursor = 0, rows = [], shown = 'lamps';

function tab(which) {
  shown = which;
  document.getElementById('tab-lamps').className = which === 'lamps' ? '' : 'hide';
  document.getElementById('tab-log').className   = which === 'log'   ? '' : 'hide';
  var bs = document.querySelectorAll('.tabs button');
  bs[0].setAttribute('aria-selected', which === 'lamps');
  bs[1].setAttribute('aria-selected', which === 'log');
}

function ago(now, ts) {
  if (!ts) return 'chưa có số liệu';
  var d = Math.max(0, now - ts);
  if (d < 60) return d.toFixed(0) + ' giây trước';
  if (d < 3600) return (d / 60).toFixed(0) + ' phút trước';
  return (d / 3600).toFixed(1) + ' giờ trước';
}

function hhmmss(ts) {
  var d = new Date(ts * 1000);
  return d.toTimeString().slice(0, 8) + '.' +
         String(d.getMilliseconds()).padStart(3, '0');
}

var ORDER = ['relay_red', 'relay_yellow', 'relay_green', 'relay_blue', 'relay_15'];
var COLORS = {
  relay_red: '#e5484d', relay_yellow: '#f5d90a', relay_green: '#30a46c',
  relay_blue: '#3b82f6', relay_15: '#8b8d98'
};

function drawLamps(s) {
  var box = document.getElementById('lamps');
  var codes = ORDER.filter(function (c) { return s.lamps[c]; });
  Object.keys(s.lamps).forEach(function (c) {
    if (codes.indexOf(c) < 0) codes.push(c);
  });
  if (!codes.length) {
    box.innerHTML = '<p class="foot">Chưa thấy kênh đèn nào. Node chỉ báo ' +
                    'trạng thái khi có ai ghi vào — bấm một cái switch để nó lên tiếng.</p>';
    return;
  }
  box.innerHTML = codes.map(function (code) {
    var L = s.lamps[code], on = L.v > 0.5, col = COLORS[code] || '#8b8d98';
    return '<div class="lamp">' +
      '<div class="bulb" style="background:' + (on ? col : 'transparent') +
        ';border-color:' + col + ';box-shadow:' + (on ? '0 0 16px ' + col : 'none') + '"></div>' +
      '<div class="name">' + code + '</div>' +
      '<div class="state" style="color:' + (on ? col : 'var(--muted)') + '">' +
        (on ? 'SÁNG' : 'tắt') + '</div>' +
      '<div class="age">' + ago(s.now, L.ts) + '</div>' +
    '</div>';
  }).join('');
}

function drawCards(s) {
  var m = s.stats, c = [
    ['Gói nhận', m.messages], ['Bản ghi', m.items], ['Đẩy vào Odoo', m.forwarded],
    ['Lệnh gửi', m.cmd_sent], ['Chờ gửi lại (mất mạng)', m.cmd_sent_no_conn],
    ['Lệnh đã ack', m.cmd_acked],
    ['Gói hỏng', m.bad], ['Mốc giờ vô lý', m.ts_dropped], ['Outbox', s.outbox]
  ];
  document.getElementById('cards').innerHTML = c.map(function (x) {
    return '<div class="card"><b>' + x[1] + '</b><span>' + x[0] + '</span></div>';
  }).join('');
}

function drawLog() {
  document.getElementById('log').innerHTML = rows.slice(0, 300).map(function (e) {
    var up = e.dir === 'up';
    return '<tr>' +
      '<td>' + hhmmss(e.t) + '</td>' +
      '<td class="' + (up ? 'up' : 'down') + '">' + (up ? '\\u2191 node' : '\\u2193 edge') + '</td>' +
      '<td>' + e.topic + '</td>' +
      '<td style="text-align:right">' + e.bytes + '</td>' +
      '<td class="note">' + (e.note || '') + '</td>' +
    '</tr>';
  }).join('');
}

function poll() {
  fetch('/ops/api/state?since=' + cursor, { cache: 'no-store' })
    .then(function (r) { return r.json(); })
    .then(function (s) {
      cursor = s.cursor;
      var dot = document.getElementById('brokerDot');
      dot.style.background = s.stats.connected ? 'var(--up)' : 'var(--warn)';
      document.getElementById('brokerTxt').textContent =
        s.stats.connected ? 'broker đã nối' : 'MẤT KẾT NỐI BROKER';
      var names = Object.keys(s.stats.online);
      document.getElementById('nodeTxt').textContent = names.map(function (n) {
        return n + (s.stats.online[n] ? ' đang chạy' : ' ĐÃ TẮT') +
               (s.caps[n] ? ' · nhận lệnh qua MQTT' : '');
      }).join(' | ');

      if (s.events.length) {
        rows = s.events.reverse().concat(rows).slice(0, 300);
        if (shown === 'log') drawLog();
      }
      drawLamps(s);
      drawCards(s);
    })
    .catch(function () {
      document.getElementById('brokerDot').style.background = 'var(--warn)';
      document.getElementById('brokerTxt').textContent = 'không gọi được edge';
    })
    .then(function () { setTimeout(poll, 1000); });
}
poll();
</script>
</body>
</html>
"""
