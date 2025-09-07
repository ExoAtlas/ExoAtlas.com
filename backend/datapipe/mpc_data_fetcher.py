"""
Daily Minor Planet Center (MPC) data fetcher & loader — robust ID/Name model + R2 JSON.

What this script does
---------------------
1) Downloads MPCORB (gzip) and parses one-line elements.
2) Parses the right-hand "readable designation" into a stable, app-friendly model:
   - If numbered: "<number> <IAU name>"   → mp_number=<int>, name=<IAU name>, object_id=<number>, prov_desig=NULL
   - If provisional only: "YYYY CODE..."   → mp_number=NULL, name=<provisional>, object_id=<provisional>, prov_desig=<provisional>
   (We also store the original right-edge text in 'packed_desig' for debugging/migration.)
3) Upserts into Postgres (via Cloud SQL Auth Proxy) keyed by object_id.
4) Exports CSV to GCS (includes id/name/number/provisional).
5) Writes compact JSON and (optionally) uploads to Cloudflare R2 (S3-compatible) if R2_* envs are set.

Environment variables expected
------------------------------
DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME             (Postgres)
GCS_BUCKET_NAME (required), GCS_OBJECT_NAME (optional)      (GCS CSV)
MAX_ROWS_INGEST, SKIP_GCS_EXPORT, UPSERT_BATCH_SIZE, etc.   (optional)

# Cloudflare R2 (S3-compatible) — all required to enable R2 upload
R2_ENDPOINT            e.g. https://<accountid>.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID
R2_SECRET_ACCESS_KEY
R2_BUCKET              e.g. exoatlas-public
R2_JSON_KEY            optional; default mpcorb.json

# Output JSON filename (local /tmp before upload)
OUT_JSON_NAME          optional; default mpcorb.json
"""

from __future__ import annotations

import io
import os
import sys
import time
import json
import gzip
import re
import typing as t
from datetime import datetime, timezone
from pathlib import Path

import requests
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from google.cloud import storage

# ---- R2 (S3-compat) ----
import boto3
from botocore.client import Config

# ---------------------- Configuration ----------------------

MPCORB_URL = "https://minorplanetcenter.net/iau/MPCORB/MPCORB.DAT.gz"

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_OBJECT_NAME = os.getenv("GCS_OBJECT_NAME", "workflow/mpcorb_full.csv")

# R2 envs (optional; if any missing, JSON upload to R2 is skipped)
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_JSON_KEY = os.getenv("R2_JSON_KEY", "mpcorb.json")

OUT_JSON_NAME = os.getenv("OUT_JSON_NAME", "asteroids.json")
TMP_JSON_PATH = Path("/tmp") / OUT_JSON_NAME

# Batch sizes / perf
UPSERT_BATCH_SIZE = int(os.getenv("UPSERT_BATCH_SIZE", "2000"))
EXPORT_CHUNK_ROWS = int(os.getenv("EXPORT_CHUNK_ROWS", "50000"))

# Test mode limiter (0 = no limit)
MAX_ROWS_INGEST = int(os.getenv("MAX_ROWS_INGEST", "0"))

# Skip GCS export?
SKIP_GCS_EXPORT = os.getenv("SKIP_GCS_EXPORT", "").lower() in {"1", "true", "yes"}

REQUEST_TIMEOUT = (10, 120)  # (connect, read) seconds

# ---------------------- Logging ----------------------------

def log(msg: str) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{now}] {msg}", flush=True)

# ---------------------- DB --------------------------------

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

def column_exists(conn, table: str, column: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND column_name = %s
            LIMIT 1
        """, (table, column))
        return cur.fetchone() is not None

def ensure_table(conn) -> None:
    """
    Create/upgrade schema and safely enforce NOT NULL on `name`
    AFTER backfilling legacy rows.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS mpc_objects_raw (
        object_id              TEXT PRIMARY KEY,
        mp_number              INTEGER NULL,
        prov_desig             TEXT NULL,
        name                   TEXT NULL,          -- set NOT NULL later after backfill
        packed_desig           TEXT NULL,
        raw_line               TEXT NOT NULL,
        h_mag                  REAL NULL,
        g_slope                REAL NULL,
        epoch_mjd              INTEGER NULL,
        mean_anomaly_deg       REAL NULL,
        arg_perihelion_deg     REAL NULL,
        long_asc_node_deg      REAL NULL,
        inclination_deg        REAL NULL,
        eccentricity           REAL NULL,
        mean_daily_motion_deg  REAL NULL,
        semimajor_axis_au      REAL NULL,
        last_updated           TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    alters = [
        "ALTER TABLE mpc_objects_raw ADD COLUMN IF NOT EXISTS mp_number INTEGER NULL;",
        "ALTER TABLE mpc_objects_raw ADD COLUMN IF NOT EXISTS prov_desig TEXT NULL;",
        "ALTER TABLE mpc_objects_raw ADD COLUMN IF NOT EXISTS name TEXT NULL;",
        "ALTER TABLE mpc_objects_raw ADD COLUMN IF NOT EXISTS packed_desig TEXT NULL;",
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

        # --- Backfill `name` from legacy `obj_name` if that column exists ---
        if column_exists(conn, "mpc_objects_raw", "obj_name"):
            cur.execute("""
                UPDATE mpc_objects_raw
                SET name = COALESCE(name, NULLIF(btrim(obj_name), ''))
                WHERE name IS NULL OR btrim(name) = '';
            """)

        # --- Backfill remaining NULL/blank names from prov_desig / packed_desig / object_id ---
        cur.execute("""
            UPDATE mpc_objects_raw
            SET name = COALESCE(
                NULLIF(btrim(name), ''),
                NULLIF(btrim(prov_desig), ''),
                NULLIF(btrim(packed_desig), ''),
                NULLIF(btrim(object_id), '')
            )
            WHERE name IS NULL OR btrim(name) = '';
        """)

        # Check if any NULL/blank remain; only then enforce NOT NULL
        cur.execute("SELECT COUNT(*) FROM mpc_objects_raw WHERE name IS NULL OR btrim(name) = ''")
        remaining = cur.fetchone()[0]

        if remaining == 0:
            cur.execute("ALTER TABLE mpc_objects_raw ALTER COLUMN name SET NOT NULL;")
        else:
            # Don’t fail the run; finish upgrading but leave it nullable. We’ll fill as new data ingests.
            # If you want to see how many, they’re logged:
            print(f"[schema] Warning: {remaining} rows still missing `name`; keeping column nullable this run.", flush=True)

    conn.commit()

# ---------------------- Parsing ----------------------------

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

def extract_packed_designation(line: str) -> str | None:
    """Right-edge 'readable designation' (1-based ~167–194)."""
    if not line or len(line) < 170:
        return None
    return line[166:194].strip() or None

def extract_basic_fields(line: str) -> tuple[str | None, str, t.Optional[float], t.Optional[float]]:
    """
    H ~ cols 9-13 (1-based), G ~ 15-19. Also returns the whole raw line and packed_desig.
    """
    h = g = None
    if len(line) >= 19:
        h = try_parse_float(line[8:13])
        g = try_parse_float(line[14:19])
    return extract_packed_designation(line), line.rstrip("\r\n"), h, g

def extract_orbital_elements(line: str) -> dict[str, t.Optional[float | int]]:
    """Tolerant fixed-width slices based on public MPCORB layout."""
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

# Provisional-only regex (e.g., "1998 KD3", "2004 MN4", "2010 AB", "2000 SG344", "2015 P5")
_PROV_RE = re.compile(r"^(?P<year>\d{4})\s+(?P<code>[A-Z]{1,2}\d{0,4}[A-Z]?)$")

def parse_readable_designation(readable: str) -> dict[str, t.Optional[str]]:
    """
    Returns:
      mp_number (str) or None
      prov_desig (str) or None
      name (str) not None
      object_id (str) not None
    """
    s = " ".join((readable or "").split())
    if not s:
        return dict(mp_number=None, prov_desig=None, name=None, object_id=None)

    # Numbered + name: "<digits> <rest>"
    first, *rest = s.split(" ", 1)
    if first.isdigit() and rest:
        mp_number = first
        name = rest[0]  # rest of string as IAU name
        return {
            "mp_number": mp_number,
            "prov_desig": None,
            "name": name,
            "object_id": mp_number,
        }

    # Provisional only: "YYYY CODE..."
    if _PROV_RE.match(s):
        return {
            "mp_number": None,
            "prov_desig": s,
            "name": s,
            "object_id": s,
        }

    # Fallback: treat entire string as a display name and key
    return {
        "mp_number": None,
        "prov_desig": None,
        "name": s,
        "object_id": s,
    }

# ---------------------- IO: download/decompress ------------

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

# ---------------------- DB upsert/export -------------------

def upsert_rows(conn, rows: list[tuple]) -> None:
    """
    rows: [
      (object_id, mp_number, prov_desig, name, packed_desig,
       raw, h, g, epoch, M, w, Omega, i, e, n, a),
      ...
    ]
    """
    if not rows:
        return
    sql = """
    INSERT INTO mpc_objects_raw (
        object_id, mp_number, prov_desig, name, packed_desig,
        raw_line, h_mag, g_slope,
        epoch_mjd, mean_anomaly_deg, arg_perihelion_deg,
        long_asc_node_deg, inclination_deg, eccentricity,
        mean_daily_motion_deg, semimajor_axis_au, last_updated
    )
    VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, NOW()
    )
    ON CONFLICT (object_id) DO UPDATE
      SET mp_number             = EXCLUDED.mp_number,
          prov_desig            = EXCLUDED.prov_desig,
          name                  = EXCLUDED.name,
          packed_desig          = EXCLUDED.packed_desig,
          raw_line              = EXCLUDED.raw_line,
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
    """Export a snapshot to GCS as CSV (chunked)."""
    log(f"Exporting snapshot to gs://{bucket_name}/{object_name} ...")
    sql = """
        SELECT
          object_id, name, mp_number, prov_desig,
          h_mag, g_slope,
          epoch_mjd, mean_anomaly_deg, arg_perihelion_deg,
          long_asc_node_deg, inclination_deg, eccentricity,
          mean_daily_motion_deg, semimajor_axis_au
        FROM mpc_objects_raw
        ORDER BY object_id
    """
    chunks = pd.read_sql_query(sql, conn, chunksize=EXPORT_CHUNK_ROWS)
    tmp_path = os.path.join("/tmp", f"mpc_export_{int(time.time())}.csv")
    first = True
    for df in chunks:
        df.to_csv(tmp_path, index=False, mode="w" if first else "a", header=first)
        first = False
    if first:
        cols = [
            "object_id","name","mp_number","prov_desig",
            "h_mag","g_slope","epoch_mjd","mean_anomaly_deg","arg_perihelion_deg",
            "long_asc_node_deg","inclination_deg","eccentricity",
            "mean_daily_motion_deg","semimajor_axis_au"
        ]
        pd.DataFrame(columns=cols).to_csv(tmp_path, index=False)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(tmp_path)
    log("GCS upload complete.")

def r2_upload_json(json_path: Path, bucket: str, key: str) -> str:
    session = boto3.session.Session()
    s3 = session.client(
        service_name="s3",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        endpoint_url=R2_ENDPOINT,
        config=Config(signature_version="s3v4"),
    )
    extra = {
        "ContentType": "application/json; charset=utf-8",
        "CacheControl": "public, max-age=86400",  # 1 day; tune per your cache strategy
    }
    s3.upload_file(str(json_path), bucket, key, ExtraArgs=extra)
    return f"{bucket}/{key}"

def export_json_and_maybe_upload(conn) -> None:
    """
    Emit compact JSON list for your web app:
      id (=object_id), name, number (=mp_number), provisional (=prov_desig), + key orbital fields.
    """
    log("Building JSON snapshot ...")
    sql = """
        SELECT
          object_id, name, mp_number, prov_desig,
          h_mag, g_slope,
          epoch_mjd, mean_anomaly_deg, arg_perihelion_deg,
          long_asc_node_deg, inclination_deg, eccentricity,
          mean_daily_motion_deg, semimajor_axis_au
        FROM mpc_objects_raw
        ORDER BY object_id
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

    # Convert to your app schema
    out = []
    for r in rows:
        rec = dict(zip(cols, r))
        out.append({
            "id": rec["object_id"],
            "name": rec["name"],
            "number": rec["mp_number"],
            "provisional": rec["prov_desig"],
            "H": rec["h_mag"],
            "G": rec["g_slope"],
            "epoch_mjd": rec["epoch_mjd"],
            "M": rec["mean_anomaly_deg"],
            "w": rec["arg_perihelion_deg"],
            "Omega": rec["long_asc_node_deg"],
            "i": rec["inclination_deg"],
            "e": rec["eccentricity"],
            "n": rec["mean_daily_motion_deg"],
            "a": rec["semimajor_axis_au"],
        })

    TMP_JSON_PATH.write_text(json.dumps(out, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    log(f"Wrote JSON → {TMP_JSON_PATH}")

    if all([R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET, R2_JSON_KEY]):
        try:
            obj = r2_upload_json(TMP_JSON_PATH, R2_BUCKET, R2_JSON_KEY)
            log(f"R2 upload complete: {obj}")
        except Exception as e:
            log(f"R2 upload failed: {e.__class__.__name__}: {e}")
    else:
        log("R2 env not fully set; skipping R2 upload.")

# ---------------------- Main -------------------------------

def main() -> None:
    if not GCS_BUCKET_NAME:
        raise RuntimeError("GCS_BUCKET_NAME is required")

    if MAX_ROWS_INGEST > 0:
        log(f"TEST MODE: will stop after {MAX_ROWS_INGEST} rows.")

    log("Starting MPC data fetcher (robust ID/Name)...")

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

            packed_desig, raw, h, g = extract_basic_fields(line)
            parsed = parse_readable_designation(packed_desig or "")
            object_id  = parsed["object_id"]
            name       = parsed["name"]
            mp_number  = int(parsed["mp_number"]) if parsed["mp_number"] else None
            prov_desig = parsed["prov_desig"]

            if not object_id or not name:
                # skip malformed rows
                continue

            elems = extract_orbital_elements(line)

            if MAX_ROWS_INGEST and total >= MAX_ROWS_INGEST:
                reached_limit = True
                break

            buffer.append((
                object_id, mp_number, prov_desig, name, packed_desig,
                raw, h, g,
                elems["epoch_mjd"], elems["mean_anomaly_deg"], elems["arg_perihelion_deg"],
                elems["long_asc_node_deg"], elems["inclination_deg"], elems["eccentricity"],
                elems["mean_daily_motion_deg"], elems["semimajor_axis_au"]
            ))

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

        if not reached_limit and buffer:
            upsert_rows(conn, buffer)
            total += len(buffer)
            buffer.clear()

        log(f"Ingestion complete. Total rows processed: {total}")

        if SKIP_GCS_EXPORT:
            log("Skipping GCS CSV export (SKIP_GCS_EXPORT is set).")
        else:
            export_to_gcs(conn, GCS_BUCKET_NAME, GCS_OBJECT_NAME)

        # Always build local JSON; upload to R2 only if env is set
        export_json_and_maybe_upload(conn)

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
