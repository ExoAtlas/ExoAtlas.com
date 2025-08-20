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
4) Exports a simple CSV snapshot to GCS for downstream consumption.

Environment variables expected
------------------------------
DB_HOST                default 127.0.0.1 (Cloud SQL Auth Proxy local listener)
DB_PORT                default 5432
DB_USER                (required)
DB_PASSWORD            (required)
DB_NAME                (required)
GCS_BUCKET_NAME        (required)
GCS_OBJECT_NAME        optional    default mpc/mpcorb_latest.csv

Notes
-----
- When running on GitHub-hosted runners, you must enable a PUBLIC IP on the Cloud SQL
  instance and use the Cloud SQL Auth Proxy (secure mTLS) as done in the GitHub Action.
  Private IP requires the runner to be on your VPC, which is not the case.
- With the proxy, set sslmode=disable for the DB client; the proxy handles encryption.

Author: ExoAtlas (C) 2025
"""

from __future__ import annotations

import io
import os
import sys
import time
import gzip
import math
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
GCS_OBJECT_NAME = os.getenv("GCS_OBJECT_NAME", "mpc/mpcorb_latest.csv")

# Batch sizes
UPSERT_BATCH_SIZE = int(os.getenv("UPSERT_BATCH_SIZE", "5000"))
EXPORT_CHUNK_ROWS = int(os.getenv("EXPORT_CHUNK_ROWS", "200000"))

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
    """Create a simple raw table keyed by packed designation."""
    ddl = """
    CREATE TABLE IF NOT EXISTS mpc_objects_raw (
        packed_desig   TEXT PRIMARY KEY,
        raw_line       TEXT NOT NULL,
        h_mag          REAL NULL,
        g_slope        REAL NULL,
        last_updated   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def extract_packed_designation(line: str) -> str | None:
    """
    Extract the 'packed designation' field from MPCORB fixed-width line.
    According to MPCORB.DAT layout, the packed designation is near the
    right side of the line. The commonly used slice is columns 167-194 (1-based).
    We'll use a tolerant approach and then .strip().

    If the line is too short or looks like a header, return None.
    """
    if not line or len(line) < 170:
        return None
    # Python is 0-based -> 166:194
    desig = line[166:194].strip()
    return desig or None


def try_parse_float(slice_text: str) -> t.Optional[float]:
    try:
        return float(slice_text.strip())
    except Exception:
        return None


def extract_basic_fields(line: str) -> tuple[str | None, t.Optional[float], t.Optional[float]]:
    """
    Grab a few convenient numeric fields (H, G) using typical MPCORB fixed columns.
    These are *best-effort*; the raw_line is authoritative.
    MPC documents show:
      - H ~ cols 9-13 (1-based)
      - G ~ cols 15-19 (1-based)
    We'll be defensive with bounds.
    """
    h = None
    g = None
    if len(line) >= 19:
        # Convert to 0-based slices
        h = try_parse_float(line[8:13])
        g = try_parse_float(line[14:19])
    return extract_packed_designation(line), h, g


def stream_download_and_decompress(url: str) -> t.Iterator[str]:
    """
    Stream download the gz file and yield decoded lines (str) one by one
    without writing large intermediate files.
    """
    log(f"Downloading {url} ...")
    with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
        resp.raise_for_status()
        # Accumulate compressed bytes in chunks into a BytesIO and feed GzipFile,
        # but to keep memory bounded, we'll feed the raw socket to GzipFile via an
        # incremental buffer.
        # Simplest robust method: download into a BytesIO then decompress. MPCORB gz
        # is ~50–80MB, fits in runner memory comfortably.
        compressed = io.BytesIO()
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                compressed.write(chunk)
        compressed.seek(0)

    log("Decompressing stream...")
    with gzip.GzipFile(fileobj=compressed, mode="rb") as gz:
        # Decode line-by-line; MPC uses ASCII
        for raw in gz:
            yield raw.decode("utf-8", errors="replace")


def is_data_line(line: str) -> bool:
    """Skip headers and comments."""
    if not line:
        return False
    s = line.strip("\r\n")
    if not s:
        return False
    # Header/comment lines often start with '#', or have '-----' delimiters.
    if s.startswith("#") or s.startswith("---"):
        return False
    # Extremely short lines are not valid data rows.
    return len(s) > 40


def upsert_rows(conn, rows: list[tuple[str, str, t.Optional[float], t.Optional[float]]]) -> None:
    """
    rows: [(packed_desig, raw_line, h, g), ...]
    """
    if not rows:
        return
    sql = """
    INSERT INTO mpc_objects_raw (packed_desig, raw_line, h_mag, g_slope, last_updated)
    VALUES (%s, %s, %s, %s, NOW())
    ON CONFLICT (packed_desig) DO UPDATE
      SET raw_line = EXCLUDED.raw_line,
          h_mag    = EXCLUDED.h_mag,
          g_slope  = EXCLUDED.g_slope,
          last_updated = NOW();
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, rows, page_size=UPSERT_BATCH_SIZE)
    conn.commit()


def export_to_gcs(conn, bucket_name: str, object_name: str) -> None:
    """
    Dump a lightweight CSV (packed_desig, h_mag, g_slope) to GCS from the DB.
    Uses chunked reads to avoid high memory.
    """
    log(f"Exporting snapshot to gs://{bucket_name}/{object_name} ...")

    # Read in chunks from DB
    sql = "SELECT packed_desig, h_mag, g_slope FROM mpc_objects_raw ORDER BY packed_desig"
    chunks = pd.read_sql_query(sql, conn, chunksize=EXPORT_CHUNK_ROWS)

    # Write to a temporary local CSV
    tmp_path = os.path.join("/tmp", f"mpc_export_{int(time.time())}.csv")
    first = True
    for df in chunks:
        mode = "w" if first else "a"
        header = first
        df.to_csv(tmp_path, index=False, mode=mode, header=header)
        first = False

    if first:
        # No rows case: create an empty CSV with headers
        pd.DataFrame(columns=["packed_desig", "h_mag", "g_slope"]).to_csv(tmp_path, index=False)

    # Upload via ADC (set by github auth action)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(tmp_path)
    log("GCS upload complete.")


def main() -> None:
    if not GCS_BUCKET_NAME:
        raise RuntimeError("GCS_BUCKET_NAME is required")

    log("Starting MPC data fetcher...")

    # Connect DB
    conn = get_db_connection()
    log("Connected to DB.")

    try:
        ensure_table(conn)

        # Download + ingest
        rows_buffer: list[tuple[str, str, t.Optional[float], t.Optional[float]]] = []
        total = 0
        for line in stream_download_and_decompress(MPCORB_URL):
            if not is_data_line(line):
                continue
            packed, h, g = extract_basic_fields(line)
            if not packed:
                continue
            rows_buffer.append((packed, line.rstrip("\r\n"), h, g))
            if len(rows_buffer) >= UPSERT_BATCH_SIZE:
                upsert_rows(conn, rows_buffer)
                total += len(rows_buffer)
                log(f"Ingested {total} rows...")
                rows_buffer.clear()

        if rows_buffer:
            upsert_rows(conn, rows_buffer)
            total += len(rows_buffer)
            rows_buffer.clear()

        log(f"Ingestion complete. Total rows processed: {total}")

        # Export -> GCS
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
