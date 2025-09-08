# ExoAtlas asteroid catalog fetcher & publisher
#
# - Downloads MPCORB.DAT.gz from Minor Planet Center
# - Parses ONLY numbered objects (skips provisional-only designations)
# - Stores compact orbital elements in Postgres table "asteroid_catalog"
# - Exports a CSV to GCS
# - Builds JSON shards for R2 grouped by orbital regime (semi-major axis and perihelion)
#   with small, web-friendly file sizes.
#
# Fields kept (DB/CSV/JSON):
#     id (int)          -- MPC number
#     name (str)        -- common name without the leading "(####) "
#     H, G (float)
#     epoch_mjd (float) -- epoch of elements in Modified Julian Date (JD - 2400000.5)
#     M, w, Omega, i, e, n, a (float)  [degrees for angles, a in AU, n in deg/day]
#
# Environment variables:
#     DB_HOST (default: 127.0.0.1)
#     DB_PORT (default: 5432)
#     DB_USER
#     DB_PASSWORD
#     DB_NAME
#
#     GCS_BUCKET_NAME             -- e.g. "exoatlas-prod"
#     SKIP_GCS_EXPORT             -- "1" to skip CSV upload, default "0"
#     GCS_CSV_OBJECT_NAME         -- default: "asteroids/asteroid_catalog.csv"
#
#     R2_ENDPOINT                 -- e.g. "https://<accountid>.r2.cloudflarestorage.com"
#     R2_BUCKET                   -- bucket name
#     R2_ACCESS_KEY_ID
#     R2_SECRET_ACCESS_KEY
#     R2_SHARD_SIZE               -- approx objects per JSON file (default: 25000)
#     R2_BASE_PREFIX              -- default: "asteroids/by_regime"
#
#     MPCORB_URL                  -- override MPC file URL (default stable)
#     MAX_ROWS_INGEST             -- 0 (all) or limit for testing
#     UPSERT_BATCH_SIZE           -- default: 1000

from __future__ import annotations

import gzip
import io
import json
import os
import re
import sys
import time
import typing as t
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
import requests
from psycopg2.extras import execute_batch

# ---- Cloud exports (optional) ------------------------------------------------

# GCS
from google.cloud import storage

# R2 (S3-compatible)
import boto3
from botocore.client import Config

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

# Canonical MPC URL
MPCORB_URL = "https://www.minorplanetcenter.net/iau/MPCORB/MPCORB.DAT.gz"

# DB
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
TABLE_NAME = os.getenv("TABLE_NAME", "asteroid_catalog")  # canonical name

# GCS (optional)
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_OBJECT_NAME = os.getenv("GCS_OBJECT_NAME", "workflow/asteroids_numbered.csv")

# R2 (optional; if any missing, JSON upload to R2 is skipped)
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_PREFIX = os.getenv("R2_PREFIX", "asteroids/")
R2_MAX_JSON_RECORDS = int(os.getenv("R2_MAX_JSON_RECORDS", "50000"))

# Ingestion/export tuning
UPSERT_BATCH_SIZE = int(os.getenv("UPSERT_BATCH_SIZE", "1000"))
EXPORT_CHUNK_ROWS = int(os.getenv("EXPORT_CHUNK_ROWS", "50000"))

# Limit rows in test runs (0 = unlimited)
MAX_ROWS_INGEST = int(os.getenv("MAX_ROWS_INGEST", "0"))

# Skip GCS export?
SKIP_GCS_EXPORT = os.getenv("SKIP_GCS_EXPORT", "").lower() in {"1", "true", "yes"}

REQUEST_TIMEOUT = (10, 120)  # (connect, read)
TMP_DIR = Path("/tmp")

# ------------------------------------------------------------------------------
# Logging helpers
# ------------------------------------------------------------------------------

def log(msg: str) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{now}] {msg}", flush=True)

# ------------------------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------------------------

def get_db_connection():
    if not DB_PASSWORD:
        raise RuntimeError("DB_PASSWORD is not set")
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        sslmode="disable",            # proxy handles TLS in CI
        connect_timeout=20,
        application_name="mpc_data_fetcher",
    )
    conn.autocommit = False
    return conn

def table_exists(conn, table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema='public' AND table_name=%s
            LIMIT 1
            """,
            (table_name,),
        )
        return cur.fetchone() is not None

def ensure_schema(conn) -> None:
    """
    Compact, numbered-only table:

      asteroid_catalog (
        id INT PRIMARY KEY,
        name TEXT NOT NULL,
        raw_line TEXT NOT NULL,
        H REAL, G REAL,
        epoch_mjd INT,
        M REAL, w REAL, Omega REAL, i REAL, e REAL, n REAL, a REAL,
        last_updated TIMESTAMPTZ DEFAULT NOW()
      )

    If an older table named 'mpc_objects' exists, rename it to 'asteroid_catalog'.
    If a very old 'mpc_objects_raw' exists, migrate numbered rows once, then drop it.
    """
    with conn.cursor() as cur:
        if not table_exists(conn, TABLE_NAME):
            if table_exists(conn, "mpc_objects"):
                log("Renaming existing table mpc_objects → asteroid_catalog ...")
                cur.execute("ALTER TABLE mpc_objects RENAME TO asteroid_catalog;")
            else:
                log("Creating table asteroid_catalog ...")
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        raw_line TEXT NOT NULL,
                        H REAL,
                        G REAL,
                        epoch_mjd INTEGER,
                        M REAL,
                        w REAL,
                        Omega REAL,
                        i REAL,
                        e REAL,
                        n REAL,
                        a REAL,
                        last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )

        # One-time migration from very old raw table, if present.
        if table_exists(conn, "mpc_objects_raw"):
            log("Legacy mpc_objects_raw detected: migrating numbered rows → asteroid_catalog ...")
            cur.execute(
                f"""
                INSERT INTO {TABLE_NAME}
                    (id, name, raw_line, H, G, epoch_mjd, M, w, Omega, i, e, n, a, last_updated)
                SELECT
                    mp_number AS id,
                    COALESCE(NULLIF(name, ''), NULLIF(prov_desig, ''), NULLIF(packed_desig, '')) AS name,
                    raw_line,
                    h_mag AS H, g_slope AS G,
                    epoch_mjd, mean_anomaly_deg AS M, arg_perihelion_deg AS w,
                    long_asc_node_deg AS Omega, inclination_deg AS i, eccentricity AS e,
                    mean_daily_motion_deg AS n, semimajor_axis_au AS a,
                    NOW()
                FROM mpc_objects_raw
                WHERE mp_number IS NOT NULL
                ON CONFLICT (id) DO NOTHING;
                """
            )
            log("Dropping legacy table mpc_objects_raw ...")
            cur.execute("DROP TABLE IF EXISTS mpc_objects_raw;")

    conn.commit()

# ------------------------------------------------------------------------------
# Parsing helpers
# ------------------------------------------------------------------------------

def try_parse_float(s: str) -> t.Optional[float]:
    try:
        return float(s.strip())
    except Exception:
        return None

def try_parse_int(s: str) -> t.Optional[int]:
    try:
        return int(s.strip())
    except Exception:
        return None

def extract_readable_designation(line: str) -> str | None:
    """Readable designation is typically at MPCORB 0-based [166:194]."""
    if not line or len(line) < 170:
        return None
    return (line[166:194].strip() or None)

_TRAILING_NUM_NAME_RE = re.compile(r"\((\d+)\)\s+([^\r\n]+)$")
_NUMBERED_RE = re.compile(r"^\(?(?P<num>\d+)\)?\s+(?P<name>.+)$")
_PROV_RE = re.compile(r"^\d{4}\s+[A-Z]{1,2}\d{0,4}[A-Z]?$")

def derive_designation_text(line: str) -> str | None:
    d = extract_readable_designation(line)
    if d:
        return " ".join(d.split())
    m = _TRAILING_NUM_NAME_RE.search(line.strip())
    if m:
        return f"({m.group(1)}) {m.group(2).strip()}"
    return None

def parse_designation(readable: str | None) -> tuple[t.Optional[int], t.Optional[str]]:
    """
    Returns (id, name) for numbered objects; (None, None) for provisional-only.
    """
    s = " ".join((readable or "").split())
    if not s:
        return (None, None)

    m = _NUMBERED_RE.match(s)
    if m and m.group("num").isdigit():
        mpn = int(m.group("num"))
        nm = m.group("name").strip()
        if nm.startswith(")") and len(nm) > 1:
            nm = nm[1:].strip()
        return (mpn, nm)

    # purely provisional → skip
    if _PROV_RE.match(s):
        return (None, None)

    return (None, None)

def extract_orbital_elements(line: str) -> dict[str, t.Optional[float | int]]:
    # Tolerant fixed-width slices (approximate MPCORB layout)
    return {
        "epoch_mjd": try_parse_int(line[20:25]) if len(line) >= 25 else None,
        "M":         try_parse_float(line[26:35]) if len(line) >= 35 else None,
        "w":         try_parse_float(line[37:46]) if len(line) >= 46 else None,
        "Omega":     try_parse_float(line[48:57]) if len(line) >= 57 else None,
        "i":         try_parse_float(line[59:68]) if len(line) >= 68 else None,
        "e":         try_parse_float(line[70:79]) if len(line) >= 79 else None,
        "n":         try_parse_float(line[80:91]) if len(line) >= 91 else None,
        "a":         try_parse_float(line[92:103]) if len(line) >= 103 else None,
    }

def is_data_line(line: str) -> bool:
    s = (line or "").strip("\r\n")
    if not s or s.startswith("#") or s.startswith("---"):
        return False
    return len(s) > 40

# ------------------------------------------------------------------------------
# DB write & export
# ------------------------------------------------------------------------------

UPSERT_SQL_SINGLE = f"""
INSERT INTO {TABLE_NAME} (
    id, name, raw_line, H, G, epoch_mjd, M, w, Omega, i, e, n, a, last_updated
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
)
ON CONFLICT (id) DO UPDATE SET
    name         = EXCLUDED.name,
    raw_line     = EXCLUDED.raw_line,
    H            = EXCLUDED.H,
    G            = EXCLUDED.G,
    epoch_mjd    = EXCLUDED.epoch_mjd,
    M            = EXCLUDED.M,
    w            = EXCLUDED.w,
    Omega        = EXCLUDED.Omega,
    i            = EXCLUDED.i,
    e            = EXCLUDED.e,
    n            = EXCLUDED.n,
    a            = EXCLUDED.a,
    last_updated = NOW();
"""

def dedup_by_id(rows: list[tuple]) -> list[tuple]:
    """Keep the last tuple for each id within the batch."""
    if not rows:
        return rows
    latest: dict[int, tuple] = {}
    for r in rows:
        latest[r[0]] = r  # r[0] = id
    return list(latest.values())

def upsert_rows(conn, rows: list[tuple]) -> int:
    """
    rows: (id, name, raw_line, H, G, epoch_mjd, M, w, Omega, i, e, n, a)
    Returns number of rows attempted (after in-batch dedup).
    """
    if not rows:
        return 0

    values = dedup_by_id(rows)

    try:
        with conn.cursor() as cur:
            # Fast path: executes one statement per row under the hood
            execute_batch(cur, UPSERT_SQL_SINGLE, values, page_size=UPSERT_BATCH_SIZE)
        conn.commit()
        return len(values)

    except psycopg2.Error as e:
        # Extremely defensive: if something in this batch still trips ON CONFLICT
        # semantics, fall back to true row-by-row execution.
        conn.rollback()
        log(f"Batch upsert failed with {e.__class__.__name__}; retrying row-by-row ...")
        with conn.cursor() as cur:
            for v in values:
                cur.execute(UPSERT_SQL_SINGLE, v)
        conn.commit()
        return len(values)

def export_to_gcs(conn, bucket_name: str, object_name: str) -> None:
    """Export numbered-only snapshot to GCS as CSV."""
    log(f"Exporting snapshot to gs://{bucket_name}/{object_name} ...")
    sql = f"""
        SELECT
          id,
          name,
          H, G,
          epoch_mjd, M, w, Omega, i, e, n, a
        FROM {TABLE_NAME}
        ORDER BY id
    """
    chunks = pd.read_sql_query(sql, conn, chunksize=EXPORT_CHUNK_ROWS)
    tmp_path = os.path.join("/tmp", f"mpc_export_numbered_{int(time.time())}.csv")
    first = True
    for df in chunks:
        df.to_csv(tmp_path, index=False, mode="w" if first else "a", header=first)
        first = False
    if first:  # no data
        cols = ["id","name","H","G","epoch_mjd","M","w","Omega","i","e","n","a"]
        pd.DataFrame(columns=cols).to_csv(tmp_path, index=False)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(tmp_path)
    log("GCS upload complete.")

def _r2_client():
    session = boto3.session.Session()
    return session.client(
        service_name="s3",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        endpoint_url=R2_ENDPOINT,
        config=Config(signature_version="s3v4"),
    )

def r2_upload_file(local_path: Path, key: str) -> None:
    s3 = _r2_client()
    extra = {"CacheControl": "public, max-age=86400"}
    if key.endswith(".json"):
        extra["ContentType"] = "application/json; charset=utf-8"
    elif key.endswith(".csv"):
        extra["ContentType"] = "text/csv; charset=utf-8"
    s3.upload_file(str(local_path), R2_BUCKET, key, ExtraArgs=extra)

def export_json_shards_and_manifest(conn) -> None:
    """
    Build compact JSON shards (numbered only) and upload to R2:
      - {prefix}/numbered-0001.json, numbered-0002.json, ...
      - {prefix}/index.json manifest
    """
    if not all([R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET]):
        log("R2 env not fully set; skipping R2 shard upload.")
        return

    log("Building JSON shards for R2 (numbered only) ...")
    sql = f"""
        SELECT
          id,
          name,
          H, G,
          epoch_mjd, M, w, Omega, i, e, n, a
        FROM {TABLE_NAME}
        ORDER BY id
    """

    manifest = {
        "version": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "categories": {"numbered": []},
        "totals": {"numbered": 0}
    }

    shard_idx = 1
    shard_buf: list[dict] = []

    def flush():
        nonlocal shard_idx, shard_buf
        if not shard_buf:
            return
        key_name = f"numbered-{shard_idx:04d}.json"
        key = f"{R2_PREFIX}{key_name}"
        p = TMP_DIR / key_name
        p.write_text(json.dumps(shard_buf, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
        r2_upload_file(p, key)
        manifest["categories"]["numbered"].append({"key": key, "count": len(shard_buf)})
        manifest["totals"]["numbered"] += len(shard_buf)
        shard_buf.clear()
        shard_idx += 1
        try:
            p.unlink(missing_ok=True)
        except Exception:
            pass

    with conn.cursor(name="asteroid_numbered_cur") as cur:
        cur.itersize = 10000
        cur.execute(sql)
        rows = 0
        for row in cur:
            out = {
                "id": row[0],      # int
                "name": row[1],    # str
                "H": row[2], "G": row[3],
                "epoch_mjd": row[4], "M": row[5], "w": row[6],
                "Omega": row[7], "i": row[8], "e": row[9],
                "n": row[10], "a": row[11],
            }
            shard_buf.append(out)
            rows += 1
            if len(shard_buf) >= R2_MAX_JSON_RECORDS:
                flush()
            if MAX_ROWS_INGEST and rows >= MAX_ROWS_INGEST:
                break
        flush()

    man_key = f"{R2_PREFIX}index.json"
    man_path = TMP_DIR / "index.json"
    man_path.write_text(json.dumps(manifest, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    r2_upload_file(man_path, man_key)
    log(f"R2 shard upload complete. Manifest: {man_key}")

# ------------------------------------------------------------------------------
# Download / Decompress
# ------------------------------------------------------------------------------

def stream_download_and_decompress(url: str) -> t.Iterator[str]:
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

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main() -> None:
    if not GCS_BUCKET_NAME:
        raise RuntimeError("GCS_BUCKET_NAME is required")

    if MAX_ROWS_INGEST > 0:
        log(f"TEST MODE: will stop after {MAX_ROWS_INGEST} rows.")

    log("Starting MPC data fetcher (schema-compact, numbered-only)...")

    conn = get_db_connection()
    log("Connected to DB.")

    try:
        ensure_schema(conn)

        batch: list[tuple] = []
        total = 0
        reached_limit = False

        for line in stream_download_and_decompress(MPCORB_URL):
            if not is_data_line(line):
                continue

            readable = derive_designation_text(line)
            obj_id, obj_name = parse_designation(readable)

            # ONLY ingest numbered objects
            if obj_id is None or not obj_name:
                continue

            # photometry slices (rough)
            H = try_parse_float(line[8:13]) if len(line) >= 13 else None
            G = try_parse_float(line[14:19]) if len(line) >= 19 else None
            elems = extract_orbital_elements(line)

            batch.append((
                obj_id, obj_name, line.rstrip("\r\n"),
                H, G, elems["epoch_mjd"], elems["M"], elems["w"], elems["Omega"],
                elems["i"], elems["e"], elems["n"], elems["a"],
            ))

            if len(batch) >= UPSERT_BATCH_SIZE:
                total += upsert_rows(conn, batch)
                log(f"Upserted {total} rows...")
                batch.clear()

            if MAX_ROWS_INGEST and total >= MAX_ROWS_INGEST:
                reached_limit = True
                break

        if not reached_limit and batch:
            total += upsert_rows(conn, batch)
            batch.clear()

        log(f"Ingestion complete. Total numbered rows: {total}")

        if SKIP_GCS_EXPORT:
            log("Skipping GCS CSV export (SKIP_GCS_EXPORT is set).")
        else:
            export_to_gcs(conn, GCS_BUCKET_NAME, GCS_OBJECT_NAME)

        export_json_shards_and_manifest(conn)

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
