# -*- coding: utf-8 -*-
"""SQLite cuc bo cua edge — lich su do (Odoo chi giu snapshot last_*, xem
pcm_channel.py) + hang doi gui offline (outbox) khi mat mang toi Main.

Mot ket noi dung chung, khoa bang threading.Lock: luu luong cua mot edge (vai
chuc kenh, vai giay/mau) khong dang de doi sang connection-pool/async driver.
"""
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Iterable, Optional

_SCHEMA = """
CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS seq_counter (
    serial TEXT PRIMARY KEY,
    seq INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    serial TEXT NOT NULL,
    bid TEXT NOT NULL,
    seq INTEGER NOT NULL,
    payload TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_outbox_serial ON outbox(serial, id);
CREATE TABLE IF NOT EXISTS history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    serial TEXT NOT NULL,
    ch TEXT NOT NULL,
    ts REAL NOT NULL,
    v REAL,
    s TEXT,
    q INTEGER NOT NULL DEFAULT 0,
    stable INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_history_lookup ON history(serial, ch, ts);
"""


class Store:
    def __init__(self, path: Path):
        self._lock = threading.Lock()
        self._cx = sqlite3.connect(str(path), check_same_thread=False)
        self._cx.executescript(_SCHEMA)
        self._cx.commit()

    # ------------------------------------------------------------------
    # kv — api_key, config_rev da ap dung, boot_id, config cache...
    # ------------------------------------------------------------------
    def kv_get(self, key: str, default=None):
        with self._lock:
            row = self._cx.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        if not row:
            return default
        try:
            return json.loads(row[0])
        except ValueError:
            return row[0]

    def kv_set(self, key: str, value) -> None:
        raw = value if isinstance(value, str) else json.dumps(value)
        with self._lock:
            self._cx.execute(
                "INSERT INTO kv(key, value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, raw),
            )
            self._cx.commit()

    # ------------------------------------------------------------------
    # seq — moi serial mot boi dem rieng, dung lam (bid, seq) chong trung
    # o Odoo (pcm.device._seen_batch, PCM-04).
    # ------------------------------------------------------------------
    def next_seq(self, serial: str) -> int:
        with self._lock:
            cur = self._cx.execute(
                "INSERT INTO seq_counter(serial, seq) VALUES(?, 1) "
                "ON CONFLICT(serial) DO UPDATE SET seq = seq_counter.seq + 1 "
                "RETURNING seq",
                (serial,),
            )
            seq = cur.fetchone()[0]
            self._cx.commit()
        return seq

    # ------------------------------------------------------------------
    # outbox — durable trước khi thử gửi, xoá sau khi Main ACK.
    # ------------------------------------------------------------------
    def outbox_push(self, serial: str, bid: str, seq: int, payload: dict) -> int:
        with self._lock:
            cur = self._cx.execute(
                "INSERT INTO outbox(serial, bid, seq, payload, created_at) VALUES(?,?,?,?,?)",
                (serial, bid, seq, json.dumps(payload), time.time()),
            )
            self._cx.commit()
            return cur.lastrowid

    def outbox_oldest(self, serial: str) -> Optional[dict]:
        with self._lock:
            row = self._cx.execute(
                "SELECT id, bid, seq, payload FROM outbox WHERE serial=? ORDER BY id ASC LIMIT 1",
                (serial,),
            ).fetchone()
        if not row:
            return None
        return {"id": row[0], "bid": row[1], "seq": row[2], "payload": json.loads(row[3])}

    def outbox_serials(self) -> list:
        with self._lock:
            rows = self._cx.execute("SELECT DISTINCT serial FROM outbox").fetchall()
        return [r[0] for r in rows]

    def outbox_delete(self, row_id: int) -> None:
        with self._lock:
            self._cx.execute("DELETE FROM outbox WHERE id=?", (row_id,))
            self._cx.commit()

    def outbox_count(self) -> int:
        with self._lock:
            return self._cx.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]

    # ------------------------------------------------------------------
    # history — phuc vu /api/latest va /api/stats (Odoo -> edge, dong bo).
    # ------------------------------------------------------------------
    def history_insert_many(self, rows: Iterable[tuple]) -> None:
        rows = list(rows)
        if not rows:
            return
        with self._lock:
            self._cx.executemany(
                "INSERT INTO history(serial, ch, ts, v, s, q, stable) VALUES(?,?,?,?,?,?,?)",
                rows,
            )
            self._cx.commit()

    def history_latest(self, serial: str, ch: str) -> Optional[dict]:
        with self._lock:
            row = self._cx.execute(
                "SELECT ts, v, s, q, stable FROM history WHERE serial=? AND ch=? "
                "ORDER BY ts DESC LIMIT 1",
                (serial, ch),
            ).fetchone()
        if not row:
            return None
        return {"ts": row[0], "v": row[1], "s": row[2], "q": row[3], "stable": bool(row[4])}

    def history_stats(self, serial: str, ch: str, since_ts: float) -> dict:
        # q=8 (mo phong, xem pcm_simulator.py) khong tinh vao thong ke - gia tri
        # 'hop le nhung gia' khong duoc lam nhieu ty le loi/trung binh thuc.
        with self._lock:
            row = self._cx.execute(
                "SELECT COUNT(*), "
                "       SUM(CASE WHEN q IN (1, 2) THEN 1 ELSE 0 END), "
                "       AVG(v), MIN(v), MAX(v) "
                "FROM history WHERE serial=? AND ch=? AND ts >= ? AND q != 8",
                (serial, ch, since_ts),
            ).fetchone()
            last_bad = self._cx.execute(
                "SELECT ts FROM history WHERE serial=? AND ch=? AND q IN (1, 2) "
                "ORDER BY ts DESC LIMIT 1",
                (serial, ch),
            ).fetchone()
        samples = row[0] or 0
        return {
            "samples": samples,
            "bad_quality": row[1] or 0,
            "avg": row[2], "min": row[3], "max": row[4],
            "last_stale_at": last_bad[0] if last_bad else None,
        }

    def history_gc(self, older_than_ts: float) -> int:
        with self._lock:
            cur = self._cx.execute("DELETE FROM history WHERE ts < ?", (older_than_ts,))
            self._cx.commit()
            return cur.rowcount
