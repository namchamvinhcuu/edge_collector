# PCM Edge Collector

Edge collector thật (FastAPI) cho `pcm_base` — cai duoc noi voi README goc cua
`pcm_ppd_pod` la con thieu ("아직 없는 것: 1. edge/collector (FastAPI)"). Chay tren
mini-PC trong xuong, dung DUNG hop dong `/pcm/api/v1/edge/*` mo ta trong
`pcm_base/controllers/ingest.py`.

## Kien truc

```
[PLC/OPC UA]  [Modbus TCP/RTU]  [Serial (can, caliper)]  [MQTT]  [sim]
      \             |                  |                  |      /
       \            |                  |                  |     /
        +-----  edge_collector/manager.py (SourceManager) -----+
                          |
              scheduler.py (EdgeAgent) -- outbox SQLite -- odoo_client.py
                          |                                        |
                  inbound_api.py (FastAPI)              /pcm/api/v1/edge/*
                  /api/command /api/latest                        |
                  /api/browse /api/source/test                    v
                  /api/stats  <-- Odoo goi vao          [Odoo Main / pcm_base]
```

- **edge -> Odoo** (`odoo_client.py`, `scheduler.py`): `hello` (30s) dang ky/song
  con, `edge/config` keo cau hinh (co debounce `EDGE_CONFIG_DEBOUNCE_S` giong
  bai hoc thuc te — sua config don dap khong lam driver restart lien tuc),
  `measurements` gui theo lo moi `EDGE_SUBMIT_INTERVAL_S` giay/serial (dedup
  bang `(serial, bid, seq)`, `bid` = boot_id co dinh, `seq` tang dan luu trong
  SQLite nen SONG SOT qua restart), `heartbeat` moi thiet bi, `print_jobs/next`
  + `ack` cho hang doi in.
- **Odoo -> edge** (`inbound_api.py`): `pcm_base/tools/edge_client.py` goi vao
  day khi nguoi dung bam nut tren man hinh (zero/tare, xem gia tri moi nhat,
  duyet tag, test ket noi, xem thong ke). Header `X-Edge-Code` duoc doi chieu
  nhung KHONG chan cung neu thieu — dung nguyen thiet ke goc (chi tin cay
  trong LAN). **Tu chan tuong lua cong nay, dung de lo ra Internet.**
- **node -> edge** (`node_api.py`): hop dong RIENG (khong thuoc pcm_base) cho
  thiet bi `kind=http_node` (Pi/PC bridge) tu day HTTP — xem `../node_agent/`
  (chuong trinh tham chieu chay tren node). Node khong the bi goi nguoc (chi
  outbound), nen lenh tu Odoo (`/api/command`) duoc XEP HANG cho node tu poll
  qua `GET /node/v1/commands` roi ACK lai — `manager.py` giu hang doi nay.
- **Offline-first**: moi lo do di qua `store.py` (SQLite `outbox`) truoc khi
  thu gui — mat mang toi Main chi lam cham, khong mat du lieu. Lich su gia tri
  cung nam o SQLite cuc bo (`history`), dung Odoo chi giu snapshot `last_*`
  (dung thiet ke PCM: "이력은 엣지 SQLite 에").

## Cai dat

Yeu cau **Python >= 3.10** (da smoke-test tren 3.12).

```bash
python -m venv .venv && . .venv/bin/activate   # hoac .venv\Scripts\activate tren Windows
pip install -r requirements.txt
cp .env.example .env      # roi sua EDGE_MAIN_URL, EDGE_CODE, EDGE_BASE_URL...
python -m edge_collector
```

### Cau hinh qua trinh duyet (thay vi sua .env bang tay)

Sau khi `edge_collector` da chay, mo `http://<dia-chi-edge>:<EDGE_LISTEN_PORT>/setup`
(vd `http://localhost:8000/setup`) de xem/sua toan bo bien trong `.env` bang form web
thay vi sua file tay. Trang ghi thang vao `.env` (`python-dotenv`, giu nguyen
comment/dong khac), validate URL/port/interval truoc khi luu. **Cac gia tri anh
huong port/interval/state_dir da nap vao tien trinh luc khoi dong — sua qua
`/setup` xong van phai KHOI DONG LAI `edge_collector` (`python -m edge_collector`)
moi ap dung**, trang chi ghi file, khong tu restart. Cung muc do tin cay LAN nhu
`inbound_api.py` — dung dua ra Internet.

### Chay bang Docker (thay vi tu tao venv)

```bash
cp .env.example .env      # sua EDGE_MAIN_URL, EDGE_CODE, EDGE_BASE_URL...
docker compose up -d --build
```

**Quan trong: phai `cp .env.example .env` TRUOC khi `docker compose up`** — neu
quen buoc nay, Docker se tu tao 1 THU MUC RONG ten `.env` (vi khong tim thay
file de bind-mount) thay vi bao loi ro rang; trieu chung se la loi kho hieu
(`IsADirectoryError`) khi app co mo `.env`. Gap phai truong hop nay: xoa thu
muc `.env/` rong do, tao lai dung 1 FILE `.env`, roi `docker compose up` lai.

`docker-compose.yml` mount `./.env` vao `/app/.env` trong container (BAT BUOC
mount file that, khong chi dung `-e`/`environment:` don le - trang `/setup`
doc gia tri hien thi TU FILE `.env`, khong doc bien moi truong runtime, nen
neu khong mount file thi `/setup` se hien mac dinh thay vi gia tri dang chay
that) va 1 volume `edge_data` cho `/data` (SQLite outbox/history - PHAI la
volume de khong mat du lieu offline-first khi container restart/recreate).
Sua cau hinh qua `http://localhost:8000/setup` roi `docker compose restart`
de ap dung (giong lai voi ban chay tay: chi ghi file, khong tu restart).

**Rieng field "State directory" tren `/setup` la NO-OP khi chay bang Docker** -
`Dockerfile` set san `ENV EDGE_STATE_DIR=/data`, va `python-dotenv` mac dinh
KHONG ghi de bien da co san trong environment (`override=False`) — nen du sua
"State directory" qua form roi restart, SQLite van luu vao `/data` (dung, giu
nguyen tinh nang offline-first), gia tri ban nhap KHONG co tac dung gi. Muon
doi noi luu that su, phai sua `EDGE_STATE_DIR`/volume trong `docker-compose.yml`
roi `docker compose up -d --build` lai.

Neu `pcm.source.kind=serial` (thiet bi that qua USB/RS232, khong phai `sim`),
can mount them cong vat ly - bo comment phan `devices:` trong
`docker-compose.yml` va sua dung `/dev/ttyUSBx` cua may.

**Publish port `8000:8000` di qua chain `DOCKER`/`DOCKER-USER` cua iptables,
KHONG di qua chain `INPUT` ma `ufw` quan ly** — neu mini-PC co nhieu NIC va
dang dua vao `ufw` de gioi han subnet duoc goi vao cong nay, publish port cua
Docker se BO QUA rule `ufw` do. Muon gioi han that, phai tu them rule vao
chain `DOCKER-USER` (vd `iptables -I DOCKER-USER -i <ten-NIC-huong-WAN> -p tcp
--dport 8000 -j DROP`), khong the chi dua vao `ufw` nhu khi chay bang venv
(venv-mode bind thang NIC, tuan theo `ufw` binh thuong).

Build tay khong qua compose:
```bash
docker build -t edge_collector .
docker run -d --name edge_collector -p 8000:8000 \
  -v "$(pwd)/.env:/app/.env" -v edge_data:/data edge_collector
```

### Cai tay bang venv (khong dung Docker)

**Venv tao tren mot may (Windows/Linux khac nhau) KHONG dung lai duoc tren
may khac** — `venv/` chua duong dan binary tuyet doi cua may goc. Doi may
(vd .env soan tren Windows, chay lai tren Linux dev) thi tao venv MOI, dung
xoa/ghi de venv cu (giu lai de doi chieu):

```bash
python3 -m venv venv_linux
./venv_linux/bin/pip install -r requirements.txt
./venv_linux/bin/python -m edge_collector
```

Ben Odoo: vao **PCM > Ket noi thiet bi > Edge**, tao (hoac de tu dang ky qua
`hello`) mot `pcm.edge` voi `code` **trung voi `EDGE_CODE`**, dat `base_url`
la dia chi LAN cua tien trinh nay (`EDGE_BASE_URL`, vd `http://10.10.1.50:8000`).
Bam **[Duyet]** — tu do edge moi nhan gia tri. Duyet nhanh qua Odoo shell
(dev, khong can mo UI):

```bash
$VENV_PY $ODOO_BIN shell -c odoo.conf -d <db> --no-http <<'EOF'
edge = env['pcm.edge'].sudo().search([('code','=','EDGE-1')], limit=1)
edge.action_approve()
env.cr.commit()
EOF
```

### Test tren 1 may (dev local, khong can thiet bi/edge that)

Da verify THAT (khong phai doan) theo trinh tu nay — chay Odoo dev + edge
+ node CUNG mot may:

1. `EDGE_LISTEN_PORT` mac dinh **8000** — kiem tra port TRONG truoc khi chay
   (`ss -ltnp | grep :8000` hoac `curl localhost:8000/healthz`), doi sang
   port khac (vd `8090`) neu da bi chiem boi project khac tren cung may.
   Nho doi luon `EDGE_BASE_URL` (nguoi/service khac goi nguoc vao) va bien
   `NODE_EDGE_URL` ben `../node_agent/.env` cho khop port moi.
2. `EDGE_MAIN_URL` tro ve `http://localhost:<http_port_cua_Odoo_dev>` (vd
   `18013`) thay vi may LAN that.
3. Edge/device **MOI hoan toan** (code/serial chua tung dang ky) se tu tao
   ban ghi `state=new` qua `hello()`/`measurements()` — **chi hoat dung
   dung tu Odoo 2026-09-16 tro di**: ban `pcm_base` cu hon co bug crash
   HTTP 500 khi tu dang ky lan dau (`message_post()` trong route
   `auth='none'` doc `env.user` rong — xem
   `.obsidian-vault/Fix-History/2026-09-16-ingest-auth-none-message-post-expected-singleton.md`
   va skill dung chung `Skill-Odoo-Auth-None-Route-Needs-With-User` trong
   `.odoo-skill/`). Neu gap 500 o `hello()`/`measurements()` voi edge/device
   MOI → kiem tra `pcm_base` da co fix nay chua truoc khi nghi ngo cau
   hinh.
4. State cu (`EDGE_STATE_DIR`, mac dinh `./var`) giu `api_key` da hoc tu
   lan dang ky TRUOC — neu doi sang Odoo Main KHAC (vd tu LAN sang dev
   local) ma DB do chua biet key nay, `hello`/`measurements` se bi tu choi
   cho toi chu ky `hello` ke tiep (toi da 1 vong `EDGE_HELLO_INTERVAL_S`).
   Muon sach hoan toan: doi `EDGE_STATE_DIR` sang thu muc moi (vd `./var_dev`)
   thay vi xoa `var/` cu (giu lai de doi chieu neu can quay lai Main cu).
5. Da verify end-to-end that: `node_agent` (kenh `mode: sim`) → edge_collector
   (venv Linux, port doi) → Odoo dev that — `hello`/`config`/`source_status`/
   `measurements`/`print_jobs/next` deu tra 200, gia tri toi `pcm.channel`
   dung realtime (xem chi tiet trong Fix-History o tren).

## Cau hinh nguon (khong sua code)

Toan bo `pcm.source` / `pcm.device` / `pcm.channel` / `pcm.serial.profile` /
`pcm.printer` khai bao BEN ODOO (man hinh PCM). Edge tu keo ve qua
`GET /pcm/api/v1/edge/config` va tu khoi dong dung driver theo `kind`:

| `pcm.source.kind` | Driver | Thu vien | Ghi chu |
|---|---|---|---|
| `sim` | `drivers/sim.py` | - | Tao song gia (test duong ong, khong lien quan `pcm.simulator`) |
| `serial` | `drivers/serial_ascii.py` | `pyserial`, `pymodbus` | Theo `pcm.serial.profile` (`link=ascii` hoac `link=modbus`) |
| `modbus_tcp` / `modbus_rtu` | `drivers/modbus.py` | `pymodbus` (async) | Dia chi kieu Modicon `HR40001`/`IR30001` hoac so nguyen tran |
| `opcua` | `drivers/opcua.py` | `asyncua` | Subscribe theo `channel.source_tag`; xem GIOI HAN ben duoi |
| `mqtt` | `drivers/mqtt.py` | `paho-mqtt` | Tu suy ma kenh tu topic neu payload khong co khoa `ch` |
| `edge` / `http_node` | (khong co driver) | - | Nguon tu bao cao (chinh edge nay / node tu day HTTP) |

## Gioi han da biet (can bo sung truoc khi dua vao san xuat that)

1. **OPC UA Sign/Sign&Encrypt**: `pcm.source.cert_id` (chung chi) khong duoc
   `pcm_source._as_config()` gui ve edge — hien tai `opcua.py` chi ket noi
   duoc `security_mode=none` hoac userpass tren keonh khong ma hoa. Muon dung
   thuc, can them duong API rieng de edge tai chung chi (hoac dinh kem trong
   `edge_config()`).
2. **Camera** (`channel.stream_url/capture_url`): nam o node (Pi), khong phai
   o edge — collector nay KHONG serve snapshot/stream, chi la ha tang do/ghi
   gia tri. Xem `04_Camera_Integration_Guide...docx` cho phan node rieng.
3. **May in `usb://`**: `printer.py` chi gui duoc `tcp://host:port` (Zebra/
   ESC-POS co card mang). May in USB cam thang vao edge can driver rieng theo
   HDH (chua lam).
4. **MQTT auto-detect topic**: khi payload khong phai JSON co khoa `ch`, ma
   kenh duoc doan tu duoi topic — kiem tra lai cho dung quy uoc cua he thong
   MQTT thuc te truoc khi dung.
5. Da smoke-test toan bo vong doi voi driver `sim` (start/apply_config/
   emit/outbox/tat ca route inbound) VA voi `node_agent` that qua HTTP that
   (khong mock) — bao gom ca vong lenh Odoo→edge→node→ack. **Da verify THAT
   voi Odoo Main that (khong con la gia lap)**: chay `pcmppdpod_dev` local +
   edge_collector + node_agent cung may, tu dang ky edge/device MOI, duyet
   qua Odoo shell, gia tri sim toi `pcm.channel` dung — xem muc "Test tren
   1 may" o tren. CHUA test voi PLC/Modbus/OPC UA/MQTT that (khong co thiet
   bi that de kiem trong moi truong nay). Kiem tra ky truoc khi noi vao
   thiet bi that, dac biet lenh GHI (`command`/`write`).

## Cau truc thu muc

```
edge_collector/
  config.py        # doc .env
  store.py          # SQLite: kv, seq, outbox, history
  odoo_client.py     # goi VAO Odoo (/pcm/api/v1/edge/*)
  manager.py         # nap config, dieu phoi driver theo pcm.source
  scheduler.py        # EdgeAgent: hello/config/flush/sender/heartbeat/print/gc loop
  inbound_api.py       # Odoo -> edge (/api/command,/api/latest,/api/browse,/api/source/test,/api/stats)
  node_api.py          # node -> edge (/node/v1/*), rieng cua edge_collector
  settings_api.py      # trang web /setup, sua .env qua trinh duyet
  printer.py           # gui ZPL/ESC-POS qua tcp://
  app.py               # FastAPI app + lifespan
  drivers/
    base.py sim.py serial_ascii.py modbus.py opcua.py mqtt.py
```

Xem `../node_agent/README.md` cho chuong trinh chay tren Pi/PC-bridge dung
hop dong `/node/v1/*` noi tren.
