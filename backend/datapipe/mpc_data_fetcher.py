# ExoAtlas asteroid catalog → R2 uploader (no DB, no GCP)
#
# - Downloads MPCORB.DAT.gz from MPC
# - Parses ONLY numbered objects
# - Writes a compact CSV locally
# - Builds compact JSON shards + index manifest
# - Uploads CSV + JSON to Cloudflare R2 (S3-compatible)
#
# Fields (CSV/JSON):
#   id (int), name (str), H, G, epoch_mjd, M, w, Omega, i, e, n, a
#
# Env vars:
#   MPCORB_URL                (default: stable MPC URL)
#   MAX_ROWS_INGEST           (0 = all; limit for testing)
#
#   # Cloudflare R2 (required to upload)
#   R2_ENDPOINT               e.g. "https://<accountid>.r2.cloudflarestorage.com"
#   R2_BUCKET                 bucket name
#   R2_ACCESS_KEY_ID
#   R2_SECRET_ACCESS_KEY
#   R2_PREFIX                 (default: "asteroids/")  # folder/prefix for JSON
#   R2_MAX_JSON_RECORDS       (default: "20000")       # objects per JSON shard
#   R2_CSV_KEY                (default: "<R2_PREFIX>asteroid_catalog.csv")
#
# Notes:
# - Memory-friendly: CSV writes incrementally; JSON shards flush at N records.
# - No Postgres, no Google Cloud.

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
import boto3
from botocore.client import Config

# ---------- Configuration ----------
MPCORB_URL = os.getenv(
    "MPCORB_URL",
    "https://www.minorplanetcenter.net/iau/MPCORB/MPCORB.DAT.gz",
)

# R2
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_PREFIX = os.getenv("R2_PREFIX", "asteroids/")
R2_MAX_JSON_RECORDS = int(os.getenv("R2_MAX_JSON_RECORDS", "20000"))
R2_CSV_KEY = os.getenv("R2_CSV_KEY", f"{R2_PREFIX}asteroid_catalog.csv")

# Limits
MAX_ROWS_INGEST = int(os.getenv("MAX_ROWS_INGEST", "0"))

REQUEST_TIMEOUT = (10, 120)  # (connect, read)
TMP_DIR = Path("/tmp")
TMP_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Logging ----------
def log(msg: str) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{now}] {msg}", flush=True)

# ---------- Parsing helpers ----------
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
    """Readable designation typically at MPCORB 0-based [166:194]."""
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

# ---------- R2 helpers ----------
def _r2_client():
    if not all([R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET]):
        raise RuntimeError("R2 configuration missing (endpoint, keys, or bucket).")
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
    log(f"Uploaded to r2://{R2_BUCKET}/{key}")

# ---------- Download ----------
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

# ---------- Main ----------
def main() -> None:
    if MAX_ROWS_INGEST > 0:
        log(f"TEST MODE: will stop after {MAX_ROWS_INGEST} numbered rows.")

    # Prep CSV temp file
    csv_tmp = TMP_DIR / f"asteroid_catalog_{int(time.time())}.csv"
    csv_headers = ["id","name","H","G","epoch_mjd","M","w","Omega","i","e","n","a"]
    csv_file = csv_tmp.open("w", newline="", encoding="utf-8")
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(csv_headers)

    # JSON shard state
    manifest = {
        "version": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "categories": {"numbered": []},
        "totals": {"numbered": 0},
    }
    shard_idx = 1
    shard_buf: list[dict] = []

    def flush_shard():
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

    total_numbered = 0

    try:
        for line in stream_download_and_decompress(MPCORB_URL):
            if not is_data_line(line):
                continue

            readable = derive_designation_text(line)
            obj_id, obj_name = parse_designation(readable)

            # ONLY numbered
            if obj_id is None or not obj_name:
                continue

            # Photometry & elements
            H = try_parse_float(line[8:13]) if len(line) >= 13 else None
            G = try_parse_float(line[14:19]) if len(line) >= 19 else None
            elems = extract_orbital_elements(line)

            # CSV row (incremental)
            csv_writer.writerow([
                obj_id, obj_name, H, G,
                elems["epoch_mjd"], elems["M"], elems["w"], elems["Omega"],
                elems["i"], elems["e"], elems["n"], elems["a"],
            ])

            # JSON shard buffer
            shard_buf.append({
                "id": obj_id,
                "name": obj_name,
                "H": H, "G": G,
                "epoch_mjd": elems["epoch_mjd"], "M": elems["M"], "w": elems["w"],
                "Omega": elems["Omega"], "i": elems["i"], "e": elems["e"],
                "n": elems["n"], "a": elems["a"],
            })

            total_numbered += 1
            if len(shard_buf) >= R2_MAX_JSON_RECORDS:
                flush_shard()

            if MAX_ROWS_INGEST and total_numbered >= MAX_ROWS_INGEST:
                break

        # Final flush
        flush_shard()

    finally:
        try:
            csv_file.close()
        except Exception:
            pass

    # Upload manifest + CSV
    man_key = f"{R2_PREFIX}index.json"
    man_path = TMP_DIR / "index.json"
    man_path.write_text(json.dumps(manifest, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    r2_upload_file(man_path, man_key)
    r2_upload_file(csv_tmp, R2_CSV_KEY)

    log(f"Done. Numbered rows processed: {total_numbered}")
    log(f"Manifest: r2://{R2_BUCKET}/{man_key}")
    log(f"CSV:      r2://{R2_BUCKET}/{R2_CSV_KEY}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Interrupted.")
        sys.exit(130)
    except Exception as e:
        log(f"FATAL: {e.__class__.__name__}: {e}")
        raise
