# PCM Edge Collector

A small FastAPI service that runs on a mini-PC on the shop floor and bridges
industrial data sources (PLC/OPC UA, Modbus, serial scales/calipers, MQTT, or
a built-in simulator) to an Odoo "Main" server, following the `pcm_base`
edge/device/channel contract (`/pcm/api/v1/edge/*`).

It is offline-first: every reading goes through a local SQLite outbox before
it is sent, and a network outage to Main only delays delivery — it never
drops data.

## Architecture

```
[PLC/OPC UA]  [Modbus TCP/RTU]  [Serial (scale, caliper)]  [MQTT]  [sim]
      \             |                  |                    |      /
       \            |                  |                    |     /
        +-----  edge_collector/manager.py (SourceManager)  -----+
                          |
              scheduler.py (EdgeAgent) -- outbox (SQLite) -- odoo_client.py
                          |                                        |
                  inbound_api.py (FastAPI)                /pcm/api/v1/edge/*
                  /api/command  /api/latest                        |
                  /api/browse   /api/source/test                   v
                  /api/stats    <-- Main calls IN          [Odoo Main / pcm_base]
                          ^
                          |
                  node_api.py (FastAPI, /node/v1/*)
                          |
              [Node / Pi-bridge / PC-bridge, HTTP device]
```

- **edge → Main** (`odoo_client.py`, `scheduler.py`): `hello` (liveness +
  self-registration), `edge/config` (pulls source/device/channel config,
  debounced so a config edit doesn't restart drivers on every keystroke),
  `measurements` (batched, deduplicated by `(serial, boot_id, seq)` so a
  retried batch is never double-counted), `heartbeat` per device, and a
  print-job queue (`print_jobs/next` + `ack`).
- **Main → edge** (`inbound_api.py`): synchronous calls Main makes back into
  this edge when an operator clicks a button on screen — zero/tare a scale,
  read the latest value, browse OPC UA/Modbus tags, test a source
  connection, or fetch local stats. **This port is LAN-trust only — put it
  behind a firewall, never expose it to the public Internet.**
- **node → edge** (`node_api.py`): a separate, edge-defined contract for
  devices that report over plain HTTP (a Raspberry Pi or PC acting as a
  bridge for sensors it reads locally). A node never talks to Main directly
  — it only talks to its edge, and it can only be *polled*, never called
  back into, so commands issued from Main are queued here until the node
  polls for them. See [Node contract](#node-contract-nodev1) below if you
  want to write your own client for this API — no reference implementation
  ships in this repository.
- **Offline-first**: every reading passes through `store.py`'s SQLite
  `outbox` before a send is attempted; losing the connection to Main only
  slows delivery down, it never loses data. Recent history is also kept
  locally (`history` table, used by `/api/latest` and `/api/stats`) — Main
  only ever holds the latest snapshot per channel.

## Requirements

- Python **3.10+** (tested on 3.12), or Docker.
- An Odoo instance implementing the `pcm_base` edge contract described in
  [Odoo/Main API contract](#odoomain-api-contract) — this repository does
  not include that Odoo module.

## Quick start (venv)

```bash
git clone https://github.com/namchamvinhcuu/edge_collector.git
cd edge_collector
python -m venv .venv && . .venv/bin/activate   # .venv\Scripts\activate on Windows
pip install -r requirements.txt
cp .env.example .env      # then edit EDGE_MAIN_URL, EDGE_CODE, EDGE_BASE_URL...
python -m edge_collector
```

The service listens on `EDGE_LISTEN_PORT` (default `8000`).

### Configuring through the browser instead of hand-editing `.env`

Once `edge_collector` is running, open
`http://<edge-address>:<EDGE_LISTEN_PORT>/setup` (e.g.
`http://localhost:8000/setup`) to view and edit every `.env` value from a web
form instead of editing the file by hand. The page writes straight to `.env`
(preserving comments/other lines) and validates URLs/ports/intervals before
saving.

Most fields are applied **immediately** on save (no restart needed) —
**Listen host**, **Listen port**, and **State directory** are the exception
(they require restarting `edge_collector`, since the listening socket and
the local SQLite store are opened once at startup); the page marks those
three fields clearly.

## Running with Docker

```bash
cp .env.example .env      # edit EDGE_MAIN_URL, EDGE_CODE, EDGE_BASE_URL...
docker compose up -d --build
```

**You must `cp .env.example .env` before `docker compose up`** — otherwise
Docker will silently create an empty *directory* named `.env` (since there
is nothing to bind-mount), and the app will fail with a confusing
`IsADirectoryError`. If that happens: remove the empty `.env/` directory,
create a real `.env` file, then run `docker compose up` again.

**The container runs as non-root uid 1000 ("edge")** — since `/setup` writes
straight back to the bind-mounted `.env` file, that file must be writable by
uid 1000 on the host: `chown 1000:1000 .env` is the reliable fix regardless
of which user created the file (`chmod 664` only helps if your host user
already happens to be uid/gid 1000 - true by luck on a single-user Linux
desktop, not guaranteed on a server or CI-provisioned box). If it isn't
writable, saving from `/setup` will show a clear "Could not write .env" error
banner (instead of a silent failure) — fix the permission and save again.

`docker-compose.yml` mounts `./.env` into `/app/.env` inside the container
(mounting the real file matters — the `/setup` page reads directly from the
file on disk, not from the container's environment variables) and a named
volume `edge_data` for `/data` (the SQLite outbox/history — must be a
volume so offline-first data survives a container restart/recreate).
Configure through `http://localhost:8000/setup`, then
`docker compose restart` to apply anything not covered by hot-reload
(see above) - **except `EDGE_LISTEN_PORT`: changing that one needs
`docker compose up -d` (recreate), not `restart`**, since the container's
port mapping and the `EDGE_LISTEN_PORT` value the HEALTHCHECK reads are both
fixed at container-creation time from `.env`, not re-read by an in-place
`restart`.

If a source has `pcm.source.kind=serial` (a real device over USB/RS-232,
not `sim`), you also need to pass through the physical port — uncomment the
`devices:` section in `docker-compose.yml` and point it at your
`/dev/ttyUSBx`.

**Publishing a port with Docker (`8000:8000`) goes through the
`DOCKER`/`DOCKER-USER` iptables chains, not the `INPUT` chain that `ufw`
manages** — if your mini-PC has multiple NICs and you rely on `ufw` to
restrict which subnet can reach this port, Docker's port publishing will
silently bypass that rule. To actually restrict it, add a rule to the
`DOCKER-USER` chain yourself (e.g.
`iptables -I DOCKER-USER -i <your-WAN-facing-NIC> -p tcp --dport 8000 -j DROP`)
— unlike running via venv, where the process binds the NIC directly and
`ufw` applies normally.

Building/running without compose:
```bash
docker build -t edge_collector .
docker run -d --name edge_collector -p 8000:8000 \
  -v "$(pwd)/.env:/app/.env" -v edge_data:/data edge_collector
```

## Registering an edge in Odoo

On Main, create (or let it self-register on first `hello`) a `pcm.edge`
record whose `code` matches `EDGE_CODE`, with `base_url` set to this
process's LAN address (`EDGE_BASE_URL`, e.g. `http://10.10.1.50:8000`).
A newly self-registered edge starts in a "pending approval" state and won't
have its measurements accepted until approved on the Main side.

## Configuring sources (no code changes needed)

All of `pcm.source` / `pcm.device` / `pcm.channel` / `pcm.serial.profile` /
`pcm.printer` are declared on the Odoo side. The edge pulls them via
`GET /pcm/api/v1/edge/config` and starts the matching driver by `kind`:

| `pcm.source.kind` | Driver | Library | Notes |
|---|---|---|---|
| `sim` | `drivers/sim.py` | - | Generates fake waveforms (pipeline testing) |
| `serial` | `drivers/serial_ascii.py` | `pyserial`, `pymodbus` | Driven by `pcm.serial.profile` (`link=ascii` or `link=modbus`) |
| `modbus_tcp` / `modbus_rtu` | `drivers/modbus.py` | `pymodbus` (async) | Modicon-style addresses (`HR40001`/`IR30001`) or plain integers |
| `opcua` | `drivers/opcua.py` | `asyncua` | Subscribes by `channel.source_tag`; see known limitations below |
| `mqtt` | `drivers/mqtt.py` | `paho-mqtt` | Guesses the channel from the topic if the payload has no `ch` key |
| `edge` / `http_node` | (no driver) | - | Self-reported source (this edge itself, or a node pushing over HTTP) |

## Odoo/Main API contract

If you want to implement your own "Main" server instead of using `pcm_base`,
this is the exact contract `odoo_client.py` speaks. All requests carry
`X-Edge-Code: <EDGE_CODE>`, plus `X-API-Key: <key>` once one has been
issued (every response is a JSON object; on error, `edge_collector` treats
any non-2xx or malformed body as `{"ok": false, "error": "..."}`).

| Method & path | Purpose | Request body (JSON) |
|---|---|---|
| `POST /pcm/api/v1/edge/hello` | Liveness + self-registration/key issuance | `{code, name, platform, base_url, version, lag, forward_state, mqtt_connected, config_version, api_key: null on first call}` → response includes `api_key` once granted |
| `POST /pcm/api/v1/edge/config` | Pull source/device/channel config | `{config_version}` → response `{ok, config: {...}}` |
| `POST /pcm/api/v1/edge/source_status` | Report per-source health | `{sources: [...], mqtt_connected}` |
| `POST /pcm/api/v1/measurements` | Push a batch of readings | `{serial, items: [{ch, v, s, q, stable, ts}], bid, seq}` — dedupe key is `(serial, bid, seq)` |
| `POST /pcm/api/v1/heartbeat` | Per-device liveness | `{serial, ...device-reported fields}` |
| `GET /pcm/api/v1/print_jobs/next` | Poll for a queued print job | — |
| `POST /pcm/api/v1/print_jobs/ack` | Acknowledge a print job | `{id, ok, detail}` |

### Node contract (`/node/v1/*`)

This is a **separate, edge-defined** contract (not part of `pcm_base`) for
devices that report over plain HTTP instead of running a driver directly on
the edge — typically a Raspberry Pi or a PC acting as a bridge. It is
served by this same process (`node_api.py`). Every request needs
`X-Device-Serial`; `X-API-Key` is only required on data routes once the
edge has actually been told a key for that serial by Main (a node learns
its key via `/hello`, the one route that never rejects a *missing* key —
only an explicitly wrong one, so a brand-new device can bootstrap).

| Method & path | Purpose | Request body (JSON) |
|---|---|---|
| `POST /node/v1/hello` | Learn/refresh the API key for this serial | — → `{ok, known, api_key, server_time_ms}` |
| `POST /node/v1/measurements` | Push readings (same shape edge→Main uses) | `{items: [{ch, v, s, q, ts, stable}], bid, seq}` |
| `POST /node/v1/heartbeat` | Liveness | `{fw, ip, uptime_s, rssi, buffered, ...}` |
| `GET /node/v1/commands` | Poll for a queued command (zero/tare/...) | — |
| `POST /node/v1/commands/ack` | Acknowledge a command | `{id, ok, detail}` |
| `GET /node/v1/config` | Optional: fetch this device's channel labels/units | — |

A node can never be called back into — it can only poll — so any command
issued from Main for a node-backed device is queued on the edge
(`manager.py`) until the node's next `GET /node/v1/commands`.

## Known limitations (before going to production)

1. **OPC UA Sign/Sign&Encrypt**: `pcm.source.cert_id` (a certificate) is not
   forwarded to the edge by the Main side's `pcm_source._as_config()` —
   `opcua.py` currently only connects with `security_mode=none` or
   userpass over an unencrypted channel. Real certificate-based security
   needs a dedicated endpoint for the edge to fetch the cert (or embedding
   it in `edge_config()`).
2. **Camera** (`channel.stream_url`/`capture_url`) lives on the node (a Pi),
   not on the edge — this collector does not serve snapshots/streams, it
   only handles measurement I/O.
3. **USB printers**: `printer.py` only sends to `tcp://host:port`
   (network-attached Zebra/ESC-POS printers). A USB-attached printer needs
   its own OS-specific driver (not implemented).
4. **MQTT auto-detected topics**: when a payload isn't JSON with a `ch`
   key, the channel is guessed from the topic's last path segment — double
   check this matches your real MQTT naming convention before relying on
   it.
5. The `sim` driver's full lifecycle (start/apply_config/emit/outbox/every
   inbound route) and the node contract have been exercised end-to-end
   against a real Odoo instance. PLC/Modbus/OPC UA/MQTT against **real**
   hardware have not — test carefully before wiring up real equipment,
   especially any write command (`command`/`write`).

## Project layout

```
edge_collector/
  config.py          # reads .env, exposes the `settings` singleton (hot-reloadable)
  store.py           # SQLite: kv, seq, outbox, history
  odoo_client.py      # calls OUT to Main (/pcm/api/v1/edge/*)
  manager.py          # loads config, drives sources by pcm.source.kind
  scheduler.py         # EdgeAgent: hello/config/flush/sender/heartbeat/print/gc loops
  inbound_api.py        # Main -> edge (/api/command,/api/latest,/api/browse,/api/source/test,/api/stats)
  node_api.py           # node -> edge (/node/v1/*), edge-defined contract
  settings_api.py       # the /setup web page for editing .env from a browser
  printer.py            # sends ZPL/ESC-POS over tcp://
  app.py                # FastAPI app + lifespan
  drivers/
    base.py sim.py serial_ascii.py modbus.py opcua.py mqtt.py
tests/                  # pytest suite (settings_api, config hot-reload)
```

## Configuration reference

All values below are set in `.env` (copy from `.env.example`) or through
the `/setup` web page.

| Variable | Default | Notes |
|---|---|---|
| `EDGE_MAIN_URL` | `http://localhost:8069` | Root URL of Odoo Main |
| `EDGE_CODE` | auto-generated | Must match `pcm.edge.code` on Main; leave blank to auto-generate on first run |
| `EDGE_NAME` | (empty) | Display name Main assigns on first self-registration |
| `EDGE_PLATFORM` | `other` | `ubuntu` \| `windows` \| `other` |
| `EDGE_BASE_URL` | (empty) | This edge's own LAN address, so Main/a tablet can call back into it |
| `EDGE_LISTEN_HOST` | `0.0.0.0` | Interface this edge listens on *(requires restart)* |
| `EDGE_LISTEN_PORT` | `8000` | Port this edge listens on *(requires restart)* |
| `EDGE_STATE_DIR` | `./var` | SQLite outbox/history/api-key cache *(requires restart)* |
| `EDGE_HELLO_INTERVAL_S` | `30` | Liveness/registration interval |
| `EDGE_HEARTBEAT_INTERVAL_S` | `30` | Per-device heartbeat interval |
| `EDGE_CONFIG_POLL_INTERVAL_S` | `30` | How often to pull source/device/channel config |
| `EDGE_PRINT_POLL_INTERVAL_S` | `3` | Print-job queue poll interval |
| `EDGE_SUBMIT_INTERVAL_S` | `2` | How often buffered readings are batched into the outbox |
| `EDGE_CONFIG_DEBOUNCE_S` | `10` | Delay before applying a config change (avoids restarting drivers on every edit) |

## Running the tests

```bash
pip install -r requirements-dev.txt
pytest
```

## License

No license file is included yet — all rights reserved by default until one
is added. Open an issue if you'd like to use this under a specific license.
