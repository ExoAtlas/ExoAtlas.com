"""
Daily Space-Track GP (TLE) data fetcher & loader.

What this script does
---------------------
1) Logs into Space-Track and pulls newest GP elset per object (class/gp).
2) Ingests rows into Postgres via Cloud SQL Auth Proxy on localhost:5432.
   - Upsert by (norad_cat_id, epoch).
   - Stores both OMM-style fields and raw TLE lines.
3) Exports a full-table CSV snapshot to GCS for downstream consumption.

Environment variables expected
------------------------------
DB_HOST                default 127.0.0.1 (Cloud SQL Auth Proxy local listener)
DB_PORT                default 5432
DB_USER                (required)
DB_PASSWORD            (preferred) or DB_PASS (legacy fallback)
DB_NAME                (required)

ST_USERNAME            (required)  Space-Track login
ST_PASSWORD            (required)

GCS_BUCKET_NAME        (required)
GCS_OBJECT_NAME        optional    default workflow/spacetrack_full.csv

Optional knobs
--------------
UPSERT_BATCH_SIZE      default 2000
EXPORT_CHUNK_ROWS      default 50000
SKIP_GCS_EXPORT        if "1"/"true"/"yes", skip exporting CSV
REQUEST_TIMEOUT        connect/read timeout tuple (fixed in code)
"""

from __future__ import annotations

import os
import json
import time
import random
import typing as t
from datetime import datetime, timezone

import requests
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from google.cloud import storage

# ---------------------- Configuration ----------------------

BASE = "https://www.space-track.org"
S = requests.Session()
REQUEST_TIMEOUT = (10, 120)  # (connect, read) seconds

# DB (mirror MPC fetcher style; proxy handles TLS → sslmode=disable)
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD") or os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# Space-Track creds
SPACE_TRACK_USER = os.getenv("ST_USERNAME")
SPACE_TRACK_PASS = os.getenv("ST_PASSWORD")

# GCS export
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_OBJECT_NAME = os.getenv("GCS_OBJECT_NAME", "workflow/spacetrack_full.csv")

# Batch/perf knobs
UPSERT_BATCH_SIZE = int(os.getenv("UPSERT_BATCH_SIZE", "2000"))
EXPORT_CHUNK_ROWS = int(os.getenv("EXPORT_CHUNK_ROWS", "50000"))
SKIP_GCS_EXPORT = os.getenv("SKIP_GCS_EXPORT", "").lower() in {"1", "true", "yes"}

# ---------------------- Helpers ----------------------------

def log(msg: str) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{now}] {msg}", flush=True)

def _assert_required_env() -> None:
    missing = []
    for k in ["DB_USER", "DB_NAME", "ST_USERNAME", "ST_PASSWORD"]:
        if not os.getenv(k):
            missing.append(k)
    if not DB_PASSWORD:
        missing.append("DB_PASSWORD (or legacy DB_PASS)")
    if not GCS_BUCKET_NAME:
        missing.append("GCS_BUCKET_NAME")
    if missing:
        raise SystemExit(f"Missing required environment variable(s): {', '.join(missing)}")

def _safe_get(url: str, max_attempts: int = 5) -> requests.Response:
    for attempt in range(max_attempts):
        r = S.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code in (429, 500, 502, 503, 504):
            wait = 2 ** attempt + random.random()
            log(f"Transient {r.status_code} from ST; retrying in {wait:.1f}s")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r
    raise RuntimeError(f"Failed GET after {max_attempts} attempts: {url}")

# ---------------------- Space-Track ------------------------

def st_login() -> None:
    log("Logging into Space-Track…")
    r = S.post(
        f"{BASE}/ajaxauth/login",
        data={"identity": SPACE_TRACK_USER, "password": SPACE_TRACK_PASS},
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()

def fetch_gp_chunk(norad_min: int, norad_max: int) -> list[dict]:
    """Newest elset per object in a NORAD range, JSON (includes TLE_LINE1/2)."""
    url = (
        f"{BASE}/basicspacedata/query/class/gp/"
        f"NORAD_CAT_ID/{norad_min}--{norad_max}/"
        f"orderby/NORAD_CAT_ID asc/limit/200000/format/json"
    )
    r = _safe_get(url)
    return r.json()

def fetch_gp_all() -> list[dict]:
    ranges = [
        (1, 99999),
        (100000, 199999),
        (200000, 299999),
        (300000, 399999),  # future-proof
    ]
    out: list[dict] = []
    for a, b in ranges:
        log(f"Fetching GP range {a}–{b}…")
        out.extend(fetch_gp_chunk(a, b))
        time.sleep(1)  # be polite; well under 30/min
    log(f"Fetched {len(out)} records from Space-Track.")
    return out

# ---------------------- Database ---------------------------

def get_db_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        sslmode="disable",            # via Cloud SQL Auth Proxy (mTLS)
        connect_timeout=20,
        application_name="spacetrack_data_fetcher",
    )
    conn.autocommit = False
    return conn

def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS gp_catalog (
              norad_cat_id      INT NOT NULL,
              object_name       TEXT,
              object_id         TEXT,               -- e.g., 1998-067A
              epoch             TIMESTAMPTZ,
              mean_motion       DOUBLE PRECISION,
              eccentricity      DOUBLE PRECISION,
              inclination       DOUBLE PRECISION,
              ra_of_asc_node    DOUBLE PRECISION,
              arg_of_pericenter DOUBLE PRECISION,
              mean_anomaly      DOUBLE PRECISION,
              bstar             DOUBLE PRECISION,
              tle_line0         TEXT,
              tle_line1         TEXT,
              tle_line2         TEXT,
              creation_date     TIMESTAMPTZ,
              gp_id             INT,
              inserted_at       TIMESTAMPTZ DEFAULT NOW(),
              PRIMARY KEY (norad_cat_id, epoch)
            );
            CREATE INDEX IF NOT EXISTS gp_epoch_idx ON gp_catalog(epoch DESC);
            """
        )
    conn.commit()

def upsert_gp(conn, rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = """
    INSERT INTO gp_catalog
      (norad_cat_id, object_name, object_id, epoch, mean_motion, eccentricity,
       inclination, ra_of_asc_node, arg_of_pericenter, mean_anomaly, bstar,
       tle_line0, tle_line1, tle_line2, creation_date, gp_id)
    VALUES
      (%(NORAD_CAT_ID)s, %(OBJECT_NAME)s, %(OBJECT_ID)s, %(EPOCH)s, %(MEAN_MOTION)s, %(ECCENTRICITY)s,
       %(INCLINATION)s, %(RA_OF_ASC_NODE)s, %(ARG_OF_PERICENTER)s, %(MEAN_ANOMALY)s, %(BSTAR)s,
       %(TLE_LINE0)s, %(TLE_LINE1)s, %(TLE_LINE2)s, %(CREATION_DATE)s, %(GP_ID)s)
    ON CONFLICT (norad_cat_id, epoch) DO UPDATE
      SET object_name       = EXCLUDED.object_name,
          object_id         = EXCLUDED.object_id,
          mean_motion       = EXCLUDED.mean_motion,
          eccentricity      = EXCLUDED.eccentricity,
          inclination       = EXCLUDED.inclination,
          ra_of_asc_node    = EXCLUDED.ra_of_asc_node,
          arg_of_pericenter = EXCLUDED.arg_of_pericenter,
          mean_anomaly      = EXCLUDED.mean_anomaly,
          bstar             = EXCLUDED.bstar,
          tle_line0         = EXCLUDED.tle_line0,
          tle_line1         = EXCLUDED.tle_line1,
          tle_line2         = EXCLUDED.tle_line2,
          creation_date     = EXCLUDED.creation_date;
    """
    total = 0
    with conn.cursor() as cur:
        # batch insert/upsert
        for i in range(0, len(rows), UPSERT_BATCH_SIZE):
            batch = rows[i : i + UPSERT_BATCH_SIZE]
            execute_batch(cur, sql, batch, page_size=len(batch))
            total += len(batch)
    conn.commit()
    return total

def export_full_csv(conn, bucket_name: str, object_name: str) -> None:
    """Export the full gp_catalog table to GCS as CSV (chunked), like MPC."""
    log(f"Exporting snapshot to gs://{bucket_name}/{object_name} …")
    sql = """
        SELECT
          norad_cat_id, object_name, object_id, epoch,
          mean_motion, eccentricity, inclination,
          ra_of_asc_node, arg_of_pericenter, mean_anomaly, bstar,
          tle_line0, tle_line1, tle_line2, creation_date, gp_id
        FROM gp_catalog
        ORDER BY norad_cat_id, epoch
    """
    chunks = pd.read_sql_query(sql, conn, chunksize=EXPORT_CHUNK_ROWS)
    tmp_path = os.path.join("/tmp", f"spacetrack_export_{int(time.time())}.csv")
    first = True
    for df in chunks:
        df.to_csv(tmp_path, index=False, mode="w" if first else "a", header=first)
        first = False
    if first:
        # no rows case
        cols = [
            "norad_cat_id","object_name","object_id","epoch",
            "mean_motion","eccentricity","inclination",
            "ra_of_asc_node","arg_of_pericenter","mean_anomaly","bstar",
            "tle_line0","tle_line1","tle_line2","creation_date","gp_id",
        ]
        pd.DataFrame(columns=cols).to_csv(tmp_path, index=False)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(tmp_path)
    log("GCS upload complete.")

# ---------------------- Main -------------------------------

def main() -> None:
    _assert_required_env()
    log("Starting Space-Track data fetcher…")
    st_login()

    raw = fetch_gp_all()

    # Trim to exactly what we store; skip incomplete rows just in case
    rows: list[dict] = []
    for d in raw:
        if not (d.get("NORAD_CAT_ID") and d.get("EPOCH") and d.get("TLE_LINE1") and d.get("TLE_LINE2")):
            continue
        rows.append({
            "NORAD_CAT_ID": d.get("NORAD_CAT_ID"),
            "OBJECT_NAME": d.get("OBJECT_NAME"),
            "OBJECT_ID": d.get("OBJECT_ID"),
            "EPOCH": d.get("EPOCH"),
            "MEAN_MOTION": d.get("MEAN_MOTION"),
            "ECCENTRICITY": d.get("ECCENTRICITY"),
            "INCLINATION": d.get("INCLINATION"),
            "RA_OF_ASC_NODE": d.get("RA_OF_ASC_NODE"),
            "ARG_OF_PERICENTER": d.get("ARG_OF_PERICENTER"),
            "MEAN_ANOMALY": d.get("MEAN_ANOMALY"),
            "BSTAR": d.get("BSTAR"),
            "TLE_LINE0": d.get("TLE_LINE0"),
            "TLE_LINE1": d.get("TLE_LINE1"),
            "TLE_LINE2": d.get("TLE_LINE2"),
            "CREATION_DATE": d.get("CREATION_DATE"),
            "GP_ID": d.get("GP_ID"),
        })

    conn = get_db_connection()
    log("Connected to DB.")
    try:
        ensure_tables(conn)
        n = upsert_gp(conn, rows)
        log(f"Ingested {n} rows into gp_catalog.")

        if SKIP_GCS_EXPORT:
            log("Skipping GCS export (SKIP_GCS_EXPORT is set).")
        else:
            export_full_csv(conn, GCS_BUCKET_NAME, GCS_OBJECT_NAME)

        conn.commit()
        log("All done.")
    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Interrupted.")
        raise SystemExit(130)
    except Exception as e:
        log(f"FATAL: {e.__class__.__name__}: {e}")
        raise
