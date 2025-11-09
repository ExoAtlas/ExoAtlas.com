# ExoAtlas asteroid catalog fetcher & publisher
#
# - Downloads MPCORB.DAT.gz from MPC
# - Parses ONLY numbered objects
# - Writes a compact CSV locally
# - Builds compact JSON shards + index manifest
# - Uploads CSV + JSON to Cloudflare R2 (S3-compatible)
#
# Fields kept (CSV/JSON):
#     id (int)          -- MPC number
#     name (str)        -- common name without the leading "(####) "
#     H, G (float)
#     epoch_mjd (float) -- epoch of elements in Modified Julian Date (JD - 2400000.5)
#     M, w, Omega, i, e, n, a (float)  [degrees for angles, a in AU, n in deg/day]
#
# Environment variables:
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

import csv
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
import requests

# R2 (S3-compatible)
import boto3
from botocore.client import Config

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

# Canonical MPC URL
MPCORB_URL = "https://www.minorplanetcenter.net/iau/MPCORB/MPCORB.DAT.gz"

# R2 (optional; if any missing, JSON upload to R2 is skipped)
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_PREFIX = os.getenv("R2_PREFIX", "asteroids/")
R2_MAX_JSON_RECORDS = int(os.getenv("R2_MAX_JSON_RECORDS", "20000"))

# Ingestion/export tuning
UPSERT_BATCH_SIZE = int(os.getenv("UPSERT_BATCH_SIZE", "1000"))
EXPORT_CHUNK_ROWS = int(os.getenv("EXPORT_CHUNK_ROWS", "20000"))

# Limit rows in test runs (0 = unlimited)
MAX_ROWS_INGEST = int(os.getenv("MAX_ROWS_INGEST", "0"))

REQUEST_TIMEOUT = (10, 120)  # (connect, read)
TMP_DIR = Path("/tmp")

# ------------------------------------------------------------------------------
# Logging helpers
# ------------------------------------------------------------------------------

def log(msg: str) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{now}] {msg}", flush=True)

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
# R2 helpers
# ------------------------------------------------------------------------------

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
    if MAX_ROWS_INGEST > 0:
        log(f"TEST MODE: will stop after {MAX_ROWS_INGEST} rows.")

    log("Starting MPC data fetcher (schema-compact, numbered-only)...")



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
