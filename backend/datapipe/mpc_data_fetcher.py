# mpc_data_fetcher.py

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

# ---- R2 (S3-compatible) ----
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
# fixed spelling: default CSV object key
GCS_OBJECT_NAME = os.getenv("GCS_OBJECT_NAME", "workflow/asteroids.csv")

# R2 envs (optional; if any missing, JSON upload to R2 is skipped)
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")

# Folder/prefix where shards will be written on R2, e.g. "asteroids/"
R2_PREFIX = os.getenv("R2_PREFIX", "asteroids/")
# Max records per shard file
R2_MAX_JSON_RECORDS = int(os.getenv("R2_MAX_JSON_RECORDS", "50000"))

OUT_JSON_NAME = os.getenv("OUT_JSON_NAME", "asteroids.json")
TMP_DIR = Path("/tmp")
TMP_JSON_PATH = TMP_DIR / OUT_JSON_NAME

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
    Create/upgrade schema. Safe on fresh or legacy installs.
    """
    # Create table if missing
    ddl = """
    CREATE TABLE IF NOT EXISTS mpc_objects_raw (
        object_id              TEXT,
        mp_number              INTEGER NULL,
        prov_desig             TEXT NULL,
        name                   TEXT NULL,
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
    with conn.cursor() as cur:
        cur.execute(ddl)

        # Add columns if a very old table exists
        alters = [
            "ALTER TABLE mpc_objects_raw ADD COLUMN IF NOT EXISTS object_id TEXT;",
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
        for stmt in alters:
            cur.execute(stmt)

        # Create a unique index on object_id to support upsert even if PK wasn't declared initially
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes
                    WHERE schemaname='public' AND indexname='mpc_objects_raw_object_id_key'
                ) THEN
                    CREATE UNIQUE INDEX mpc_objects_raw_object_id_key ON mpc_objects_raw (object_id);
                END IF;
            END$$;
        """)

        # Make sure name is at least filled from other designations if blank (no reference to object_id here)
        cur.execute("""
            UPDATE mpc_objects_raw
            SET name = COALESCE(NULLIF(btrim(name), ''), NULLIF(btrim(prov_desig), ''), NULLIF(btrim(packed_desig), ''))
            WHERE name IS NULL OR btrim(name) = '';
        """)

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
    """Right-edge 'readable designation' (MPCORB fixed-width ~167–194, 1-based)."""
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

# Provisional-only regex (e.g., "1998 KD3", "2004 MN4", "2010 AB", "2000 SG344")
_PROV_RE = re.compile(r"^(?P<year>\d{4})\s+(?P<code>[A-Z]{1,2}\d{0,4}[A-Z]?)$")

# Variants like "(100000) Astronautica"  or "100000 Astronautica"
_NUMBERED_RE = re.compile(r"^\(?(?P<num>\d+)\)?\s+(?P<name>.+)$")

def parse_readable_designation(readable: str) -> dict[str, t.Optional[str | int]]:
    """
    Rules you asked for:
      - Numbered asteroid "(100000) Astronautica" → id=100000, name="Astronautica", number=100000, provisional=None
      - Provisional-only "1998 KD3" → id=None, name="1998 KD3", number=None, provisional="1998 KD3"
    """
    s = " ".join((readable or "").split())
    if not s:
        return dict(mp_number=None, prov_desig=None, name=None, object_id=None)

    # Numbered + name (with or without parentheses around the number)
    m = _NUMBERED_RE.match(s)
    if m and m.group("num").isdigit():
        mp_number = int(m.group("num"))
        name = m.group("name").strip()
        # If someone accidentally included parentheses around name, normalize
        if name.startswith(")") and len(name) > 1:
            name = name[1:].strip()
        return {
            "mp_number": mp_number,
            "prov_desig": None,
            "name": name,
            "object_id": mp_number,   # <-- numeric ID
        }

    # Provisional only
    if _PROV_RE.match(s):
        return {
            "mp_number": None,
            "prov_desig": s,
            "name": s,
            "object_id": None,        # <-- per your requirement: NULL id for provisional-only
        }

    # Fallback: treat entire string as a display name; keep id None (not numbered)
    return {
        "mp_number": None,
        "prov_desig": None,
        "name": s,
        "object_id": None,
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
        ORDER BY COALESCE(mp_number, 2147483647), name
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
    extra = {
        "CacheControl": "public, max-age=86400",
    }
    # ContentType by extension
    if key.endswith(".json"):
        extra["ContentType"] = "application/json; charset=utf-8"
    elif key.endswith(".csv"):
        extra["ContentType"] = "text/csv; charset=utf-8"
    s3.upload_file(str(local_path), R2_BUCKET, key, ExtraArgs=extra)

def export_json_shards_and_manifest(conn) -> None:
    """
    Build compact JSON shards and upload:
      - {prefix}/numbered-0001.json, numbered-0002.json, ...
      - {prefix}/provisional-0001.json, ...
      - {prefix}/index.json  (manifest)
    """
    if not all([R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET]):
        log("R2 env not fully set; skipping R2 shard upload.")
        return

    log("Building JSON shards for R2 ...")
    # Stream with server-side cursor to keep memory low
    sql = """
        SELECT
          object_id, name, mp_number, prov_desig,
          h_mag, g_slope,
          epoch_mjd, mean_anomaly_deg, arg_perihelion_deg,
          long_asc_node_deg, inclination_deg, eccentricity,
          mean_daily_motion_deg, semimajor_axis_au
        FROM mpc_objects_raw
        ORDER BY COALESCE(mp_number, 2147483647), name
    """
    manifest = {
        "version": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "categories": {
            "numbered": [],
            "provisional": [],
        }
    }

    def cat_for(rec) -> str:
        return "numbered" if rec["mp_number"] is not None else "provisional"

    counts = {"numbered": 0, "provisional": 0}
    shard_idx = {"numbered": 1, "provisional": 1}
    shard_buf: dict[str, list] = {"numbered": [], "provisional": []}

    def flush(category: str):
        nonlocal shard_idx, shard_buf, manifest
        if not shard_buf[category]:
            return
        seq = shard_idx[category]
        key = f"{R2_PREFIX}{category}-{seq:04d}.json"
        p = TMP_DIR / f"{category}-{seq:04d}.json"
        p.write_text(json.dumps(shard_buf[category], separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
        r2_upload_file(p, key)
        manifest["categories"][category].append({"key": key, "count": len(shard_buf[category])})
        shard_buf[category].clear()
        shard_idx[category] += 1
        try:
            p.unlink(missing_ok=True)
        except Exception:
            pass

    with conn.cursor(name="mpc_cur") as cur:  # named cursor = server-side
        cur.itersize = 10000
        cur.execute(sql)
        rows_yielded = 0
        for row in cur:
            rec = {
                "object_id": row[0],
                "name": row[1],
                "mp_number": row[2],
                "prov_desig": row[3],
                "H": row[4],
                "G": row[5],
                "epoch_mjd": row[6],
                "M": row[7],
                "w": row[8],
                "Omega": row[9],
                "i": row[10],
                "e": row[11],
                "n": row[12],
                "a": row[13],
            }
            # Map to app schema with your strict ID rule:
            out = {
                "id": rec["mp_number"] if rec["mp_number"] is not None else None,
                "name": rec["name"],
                "number": rec["mp_number"],
                "provisional": rec["prov_desig"],
                "H": rec["H"],
                "G": rec["G"],
                "epoch_mjd": rec["epoch_mjd"],
                "M": rec["M"],
                "w": rec["w"],
                "Omega": rec["Omega"],
                "i": rec["i"],
                "e": rec["e"],
                "n": rec["n"],
                "a": rec["a"],
            }

            category = cat_for(rec)
            shard_buf[category].append(out)
            counts[category] += 1
            rows_yielded += 1

            if len(shard_buf[category]) >= R2_MAX_JSON_RECORDS:
                flush(category)

            if MAX_ROWS_INGEST and rows_yielded >= MAX_ROWS_INGEST:
                break

        # Flush any remaining buffers
        flush("numbered")
        flush("provisional")

    # Write manifest
    man_key = f"{R2_PREFIX}index.json"
    man_path = TMP_DIR / "index.json"
    man_path.write_text(json.dumps({
        **manifest,
        "totals": counts
    }, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    r2_upload_file(man_path, man_key)
    log(f"R2 shard upload complete. Manifest: {man_key}")

def export_single_json_locally(conn) -> None:
    """
    (Optional) If you still want a single local JSON file for diagnostics.
    """
    log("Building single JSON snapshot (local only) ...")
    sql = """
        SELECT
          object_id, name, mp_number, prov_desig,
          h_mag, g_slope,
          epoch_mjd, mean_anomaly_deg, arg_perihelion_deg,
          long_asc_node_deg, inclination_deg, eccentricity,
          mean_daily_motion_deg, semimajor_axis_au
        FROM mpc_objects_raw
        ORDER BY COALESCE(mp_number, 2147483647), name
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

    out = []
    for r in rows:
        rec = dict(zip(cols, r))
        out.append({
            "id": rec["mp_number"] if rec["mp_number"] is not None else None,
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
    log(f"Wrote local JSON → {TMP_JSON_PATH}")

# ---------------------- Main -------------------------------

def main() -> None:
    if not GCS_BUCKET_NAME:
        raise RuntimeError("GCS_BUCKET_NAME is required")

    if MAX_ROWS_INGEST > 0:
        log(f"TEST MODE: will stop after {MAX_ROWS_INGEST} rows.")

    log("Starting MPC data fetcher (robust ID/Name + shard output)...")

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

            object_id  = parsed["object_id"]       # may be None for provisional-only
            name       = parsed["name"]
            mp_number  = parsed["mp_number"] if parsed["mp_number"] is not None else None
            prov_desig = parsed["prov_desig"]

            # We always store a stable key in DB; for provisional-only rows,
            # use prov_desig as the key to keep rows distinct.
            db_object_id = str(object_id) if object_id is not None else (prov_desig or name)
            if not db_object_id or not name:
                continue  # skip malformed

            elems = extract_orbital_elements(line)

            if MAX_ROWS_INGEST and total >= MAX_ROWS_INGEST:
                reached_limit = True
                break

            buffer.append((
                db_object_id, mp_number, prov_desig, name, packed_desig,
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

        # Always write shards to R2 if configured (compact + categorized)
        export_json_shards_and_manifest(conn)

        # Optional: keep a single local JSON for quick inspection
        # export_single_json_locally(conn)

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
