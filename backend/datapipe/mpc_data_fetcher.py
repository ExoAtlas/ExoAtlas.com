"""
Daily Minor Planet Center (MPC) data fetcher & loader.

What this script does
---------------------
1) Downloads the MPCORB catalog (gzip) from MPC.
2) Streams + decompresses in-memory (no large temp files).
3) Ingests rows into Postgres via Cloud SQL Auth Proxy on localhost:5432.
   - Upsert by packed designation (serves as stable identifier for both numbered
     and unnumbered objects).
   - Stores the full raw line so you can re-parse later if MPC format changes.
4) Exports a CSV snapshot to GCS for downstream consumption.

Environment variables expected
------------------------------
DB_HOST                default 127.0.0.1 (Cloud SQL Auth Proxy local listener)
DB_PORT                default 5432
DB_USER                (required)
DB_PASSWORD            (required)
DB_NAME                (required)
GCS_BUCKET_NAME        (required)
GCS_OBJECT_NAME        optional    default workflow/mpcorb_full.csv

Optional test/tuning env vars
-----------------------------
MAX_ROWS_INGEST        optional    default 0 (no limit). If >0, stop after N rows.
SKIP_GCS_EXPORT        optional    if "1"/"true"/"yes", skip exporting CSV to GCS.
UPSERT_BATCH_SIZE      optional    default 5000
EXPORT_CHUNK_ROWS      optional    default 200000

Notes
-----
- On GitHub-hosted runners, enable a PUBLIC IP on the Cloud SQL instance and use
  the Cloud SQL Auth Proxy (secure mTLS). Private IP requires the runner to be
  on your VPC.
- With the proxy, set sslmode=disable for the DB client; the proxy handles TLS.
"""

from __future__ import annotations

import io
import os
import sys
import time
import gzip
import typing as t
from datetime import datetime, timezone

import requests
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from google.cloud import storage

# ---------------------- Configuration ----------------------

MPCORB_URL = "https://minorplanetcenter.net/iau/MPCORB/MPCORB.DAT.gz"

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME", "exoatlas")

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_OBJECT_NAME = os.getenv("GCS_OBJECT_NAME", "workflow/mpcorb_full.csv")

# Batch sizes / perf
UPSERT_BATCH_SIZE = int(os.getenv("UPSERT_BATCH_SIZE", "2000"))
EXPORT_CHUNK_ROWS = int(os.getenv("EXPORT_CHUNK_ROWS", "50000"))

# Test mode limiter (0 = no limit)
MAX_ROWS_INGEST = int(os.getenv("MAX_ROWS_INGEST", "0"))

# Skip GCS export?
SKIP_GCS_EXPORT = os.getenv("SKIP_GCS_EXPORT", "").lower() in {"1", "true", "yes"}

REQUEST_TIMEOUT = (10, 120)  # (connect, read) seconds

# ---------------------- Helpers ----------------------------

def log(msg: str) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{now}] {msg}", flush=True)

def get_db_connection():
    if not DB_PASSWORD:
        raise RuntimeError("DB_PASSWORD is not set")

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        sslmode="disable",     # proxy handles TLS
        connect_timeout=20,
        application_name="mpc_data_fetcher",
    )
    conn.autocommit = False
    return conn

def ensure_table(conn) -> None:
    """
    Ensure table exists and carries the full orbital element set.
    Safe to run repeatedly.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS mpc_objects_raw (
        packed_desig           TEXT PRIMARY KEY,
        raw_line               TEXT NOT NULL,
        h_mag                  REAL NULL,
        g_slope                REAL NULL,
        epoch_mjd              INTEGER NULL,
        mean_anomaly_deg       REAL NULL,   -- M
        arg_perihelion_deg     REAL NULL,   -- ω
        long_asc_node_deg      REAL NULL,   -- Ω
        inclination_deg        REAL NULL,   -- i
        eccentricity           REAL NULL,   -- e
        mean_daily_motion_deg  REAL NULL,   -- n  (deg/day)
        semimajor_axis_au      REAL NULL,   -- a  (au)
        last_updated           TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    alters = [
        "ALTER TABLE mpc_objects_raw ADD COLUMN IF NOT EXISTS h_mag REAL NULL;",
        "ALTER TABLE mpc_objects_raw ADD COLUMN IF NOT EXISTS g_slope REAL NULL;",
        "ALTER TABLE mpc_objects_raw ADD COLUMN IF NOT EXISTS epoch_mjd INTEGER NULL;",
        "ALTER TABLE mpc_objects_raw ADD COLUMN IF NOT EXISTS mean_anomaly_deg REAL NULL;",
        "ALTER TABLE mpc_objects_raw ADD COLUMN IF NOT EXISTS arg_perihelion_deg REAL NULL;",
        "ALTER TABLE mpc_objects_raw ADD COLUMN IF NOT EXISTS long_asc_node_deg REAL NULL;",
        "ALTER TABLE mpc_objects_raw ADD COLUMN IF NOT EXISTS inclination_deg REAL NULL;",
        "ALTER TABLE mpc_objects_raw ADD COLUMN IF NOT EXISTS eccentricity REAL NULL;",
        "ALTER TABLE mpc_objects_raw ADD COLUMN IF NOT EXISTS mean_daily_motion_deg REAL NULL;",
        "ALTER TABLE mpc_objects_raw ADD COLUMN IF NOT EXISTS semimajor_axis_au REAL NULL;",
        "ALTER TABLE mpc_objects_raw ADD COLUMN IF NOT EXISTS last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW();",
    ]
    with conn.cursor() as cur:
        cur.execute(ddl)
        for stmt in alters:
            cur.execute(stmt)
    conn.commit()

def extract_packed_designation(line: str) -> str | None:
    """
    Extract the packed designation field from the fixed-width record.
    In MPCORB it's near the right edge; 1-based columns ~167-194.
    """
    if not line or len(line) < 170:
        return None
    desig = line[166:194].strip()  # 0-based slice
    return desig or None

def try_parse_float(slice_text: str) -> t.Optional[float]:
    try:
        return float(slice_text.strip())
    except Exception:
        return None

def try_parse_int(slice_text: str) -> t.Optional[int]:
    try:
        return int(slice_text.strip())
    except Exception:
        return None

def extract_basic_fields(line: str) -> tuple[str | None, t.Optional[float], t.Optional[float]]:
    """
    H ~ cols 9-13 (1-based), G ~ 15-19 (best-effort).
    """
    h = None
    g = None
    if len(line) >= 19:
        h = try_parse_float(line[8:13])
        g = try_parse_float(line[14:19])
    return extract_packed_designation(line), h, g

def extract_orbital_elements(line: str) -> dict[str, t.Optional[float | int]]:
    """
    Parse the classic MPCORB one-line orbital element block using
    tolerant fixed-width slices. These positions match the public
    MPCORB layout (best-effort, robust to spacing):

        epoch (MJD)          ~ cols 21-25  -> [20:25]
        mean anomaly M       ~ cols 27-35  -> [26:35]
        arg perihelion ω     ~ cols 38-46  -> [37:46]
        long. asc. node Ω    ~ cols 49-57  -> [48:57]
        inclination i        ~ cols 60-68  -> [59:68]
        eccentricity e       ~ cols 71-79  -> [70:79]
        mean daily motion n  ~ cols 81-91  -> [80:91] (deg/day)
        semimajor axis a     ~ cols 93-103 -> [92:103] (au)

    Returns a dict with None for any field that fails to parse.
    """
    return {
        "epoch_mjd":             try_parse_int(line[20:25]) if len(line) >= 25 else None,
        "mean_anomaly_deg":      try_parse_float(line[26:35]) if len(line) >= 35 else None,
        "arg_perihelion_deg":    try_parse_float(line[37:46]) if len(line) >= 46 else None,
        "long_asc_node_deg":     try_parse_float(line[48:57]) if len(line) >= 57 else None,
        "inclination_deg":       try_parse_float(line[59:68]) if len(line) >= 68 else None,
        "eccentricity":          try_parse_float(line[70:79]) if len(line) >= 79 else None,
        "mean_daily_motion_deg": try_parse_float(line[80:91]) if len(line) >= 91 else None,
        "semimajor_axis_au":     try_parse_float(line[92:103]) if len(line) >= 103 else None,
    }

def stream_download_and_decompress(url: str) -> t.Iterator[str]:
    """Stream download the gz file and yield decoded lines."""
    log(f"Downloading {url} ...")
    with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
        resp.raise_for_status()
        compressed = io.BytesIO()
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                compressed.write(chunk)
        compressed.seek(0)
    log("Decompressing stream...")
    with gzip.GzipFile(fileobj=compressed, mode="rb") as gz:
        for raw in gz:
            yield raw.decode("utf-8", errors="replace")

def is_data_line(line: str) -> bool:
    """Skip headers and comments."""
    if not line:
        return False
    s = line.strip("\r\n")
    if not s:
        return False
    if s.startswith("#") or s.startswith("---"):
        return False
    return len(s) > 40

def upsert_rows(conn, rows: list[tuple]) -> None:
    """
    rows: [
      (packed, raw, h, g, epoch_mjd, M, w, Omega, i, e, n, a),
      ...
    ]
    """
    if not rows:
        return
    sql = """
    INSERT INTO mpc_objects_raw (
        packed_desig, raw_line, h_mag, g_slope,
        epoch_mjd, mean_anomaly_deg, arg_perihelion_deg,
        long_asc_node_deg, inclination_deg, eccentricity,
        mean_daily_motion_deg, semimajor_axis_au, last_updated
    )
    VALUES (
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, NOW()
    )
    ON CONFLICT (packed_desig) DO UPDATE
      SET raw_line              = EXCLUDED.raw_line,
          h_mag                 = EXCLUDED.h_mag,
          g_slope               = EXCLUDED.g_slope,
          epoch_mjd             = EXCLUDED.epoch_mjd,
          mean_anomaly_deg      = EXCLUDED.mean_anomaly_deg,
          arg_perihelion_deg    = EXCLUDED.arg_perihelion_deg,
          long_asc_node_deg     = EXCLUDED.long_asc_node_deg,
          inclination_deg       = EXCLUDED.inclination_deg,
          eccentricity          = EXCLUDED.eccentricity,
          mean_daily_motion_deg = EXCLUDED.mean_daily_motion_deg,
          semimajor_axis_au     = EXCLUDED.semimajor_axis_au,
          last_updated          = NOW();
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, rows, page_size=UPSERT_BATCH_SIZE)
    conn.commit()

def export_to_gcs(conn, bucket_name: str, object_name: str) -> None:
    """Export the full table to GCS as CSV (chunked)."""
    log(f"Exporting snapshot to gs://{bucket_name}/{object_name} ...")
    sql = """
        SELECT
          packed_desig, h_mag, g_slope,
          epoch_mjd, mean_anomaly_deg, arg_perihelion_deg,
          long_asc_node_deg, inclination_deg, eccentricity,
          mean_daily_motion_deg, semimajor_axis_au
        FROM mpc_objects_raw
        ORDER BY packed_desig
    """
    chunks = pd.read_sql_query(sql, conn, chunksize=EXPORT_CHUNK_ROWS)
    tmp_path = os.path.join("/tmp", f"mpc_export_{int(time.time())}.csv")
    first = True
    for df in chunks:
        df.to_csv(tmp_path, index=False, mode="w" if first else "a", header=first)
        first = False
    if first:
        # no rows
        cols = ["packed_desig","h_mag","g_slope","epoch_mjd","mean_anomaly_deg",
                "arg_perihelion_deg","long_asc_node_deg","inclination_deg",
                "eccentricity","mean_daily_motion_deg","semimajor_axis_au"]
        pd.DataFrame(columns=cols).to_csv(tmp_path, index=False)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(tmp_path)
    log("GCS upload complete.")

def main() -> None:
    if not GCS_BUCKET_NAME:
        raise RuntimeError("GCS_BUCKET_NAME is required")

    if MAX_ROWS_INGEST > 0:
        log(f"TEST MODE: will stop after {MAX_ROWS_INGEST} rows.")

    log("Starting MPC data fetcher...")

    conn = get_db_connection()
    log("Connected to DB.")

    try:
        ensure_table(conn)

        buffer: list[tuple] = []
        total = 0
        reached_limit = False

        for line in stream_download_and_decompress(MPCORB_URL):
            if not is_data_line(line):
                continue

            packed, h, g = extract_basic_fields(line)
            if not packed:
                continue

            elems = extract_orbital_elements(line)

            # Apply test limit before buffering
            if MAX_ROWS_INGEST and total >= MAX_ROWS_INGEST:
                reached_limit = True
                break

            buffer.append((
                packed, line.rstrip("\r\n"), h, g,
                elems["epoch_mjd"], elems["mean_anomaly_deg"], elems["arg_perihelion_deg"],
                elems["long_asc_node_deg"], elems["inclination_deg"], elems["eccentricity"],
                elems["mean_daily_motion_deg"], elems["semimajor_axis_au"]
            ))

            # If adding this batch would cross the limit, only take what's needed
            if MAX_ROWS_INGEST and (total + len(buffer)) >= MAX_ROWS_INGEST:
                need = MAX_ROWS_INGEST - total
                if need > 0:
                    upsert_rows(conn, buffer[:need])
                    total += need
                    log(f"Ingested {total} rows...")
                buffer = []
                reached_limit = True
                break

            if len(buffer) >= UPSERT_BATCH_SIZE:
                upsert_rows(conn, buffer)
                total += len(buffer)
                log(f"Ingested {total} rows...")
                buffer.clear()

        # Final flush
        if not reached_limit and buffer:
            upsert_rows(conn, buffer)
            total += len(buffer)
            buffer.clear()

        log(f"Ingestion complete. Total rows processed: {total}")

        if SKIP_GCS_EXPORT:
            log("Skipping GCS export (SKIP_GCS_EXPORT is set).")
        else:
            export_to_gcs(conn, GCS_BUCKET_NAME, GCS_OBJECT_NAME)

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
        sys.exit(130)
    except Exception as e:
        log(f"FATAL: {e.__class__.__name__}: {e}")
        raise
