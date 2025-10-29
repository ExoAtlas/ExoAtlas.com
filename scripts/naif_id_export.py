"""
NAIF/SPK Object ID exporter:
- Pulls recent small-body objects (asteroids + optional comets) from JPL SBDB query API
- Produces rows: object_id (NAIF/SPK ID), name
- Uploads CSV to GCS and JSON to Cloudflare R2 (S3-compatible)

Env vars:
  SBDB_LIMIT (int)
  DAYS_LOOKBACK (int)
  INCLUDE_COMETS ("true"/"false")

  # GCS
  GCS_BUCKET, GCS_OBJECT, GCS_CACHE_CONTROL, GOOGLE_CLOUD_PROJECT

  # R2
  CF_R2_ACCOUNT_ID, CF_R2_ACCESS_KEY_ID, CF_R2_SECRET_ACCESS_KEY
  R2_BUCKET, R2_OBJECT, R2_CACHE_CONTROL
"""

import csv
import io
import json
import os
import sys
import time
import typing as t
from datetime import datetime, timedelta, timezone

import requests
from dateutil.parser import isoparse
from google.cloud import storage
import boto3

SBDB_QUERY_URL = "https://ssd-api.jpl.nasa.gov/sbdb_query.api"

def env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "y", "on")

def _get_sbdb(params: dict) -> dict:
    """GET with basic retries and helpful error text on failure."""
    for attempt in range(3):
        r = requests.get(SBDB_QUERY_URL, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        # Surface API error body to logs to diagnose quickly
        msg = f"SBDB {r.status_code} for {r.url}\nBody: {r.text[:4000]}"
        if r.status_code in (429, 500, 502, 503, 504) and attempt < 2:
            time.sleep(2 * (attempt + 1))
            continue
        raise requests.HTTPError(msg)

def fetch_sbdb(limit: int, days_lookback: int, include_comets: bool) -> t.List[dict]:
    """
    Queries SBDB for numbered small bodies updated recently.
    We derive NAIF/SPK IDs per NAIF convention:
      asteroid: 2000000 + number
      comet:    1000000 + sequence (best-effort)
    """
    since = (datetime.now(timezone.utc) - timedelta(days=days_lookback)).date().isoformat()
    rows: t.List[dict] = []

    # ---- Asteroids ----
    # Use last_obs only; 'updated' causes 400s on sbdb_query.api in some cases.
    params_ast = {
        "fields": "number,full_name,last_obs",
        "where": f"number>0 and last_obs>='{since}'",
        "limit": str(limit),
        "order": "-number",
    }
    data = _get_sbdb(params_ast)
    for rec in data.get("data", []):
        number, full_name, last_obs = rec
        try:
            number = int(number)
        except Exception:
            continue
        spkid = 2000000 + number
        rows.append({"object_id": spkid, "name": str(full_name).strip()})

    # ---- Comets (optional) ----
    if include_comets:
        params_cmt = {
            "fields": "pdes,full_name,last_obs",
            "where": f"comet=1 and last_obs>='{since}'",
            "limit": str(limit),
            "order": "-last_obs",
        }
        dc = _get_sbdb(params_cmt)
        for rec in dc.get("data", []):
            pdes, full_name, last_obs = rec
            seq = abs(hash(pdes)) % 900000  # bounded synthetic sequence
            spkid = 1000000 + seq
            rows.append({"object_id": spkid, "name": str(full_name).strip()})

    # Deduplicate by object_id
    out = {}
    for rrow in rows:
        out[rrow["object_id"]] = rrow["name"]
    return [{"object_id": k, "name": v} for k, v in out.items()]

def write_csv_bytes(rows: t.List[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["object_id", "name"])
    for r in sorted(rows, key=lambda x: x["object_id"]):
        writer.writerow([r["object_id"], r["name"]])
    return buf.getvalue().encode("utf-8")

def write_json_bytes(rows: t.List[dict]) -> bytes:
    # compact, stable ordering by object_id
    payload = [{"object_id": int(r["object_id"]), "name": r["name"]} for r in sorted(rows, key=lambda x: x["object_id"])]
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

def upload_gcs(blob_bytes: bytes, bucket: str, object_name: str, cache_control: str):
    client = storage.Client()  # uses ADC from WIF
    bkt = client.bucket(bucket)
    blob = bkt.blob(object_name)
    blob.cache_control = cache_control
    # Choose content type by extension
    if object_name.endswith(".csv"):
        content_type = "text/csv; charset=utf-8"
    else:
        content_type = "application/octet-stream"
    blob.upload_from_string(blob_bytes, content_type=content_type)
    print(f"[GCS] Uploaded gs://{bucket}/{object_name} ({len(blob_bytes)} bytes)")

def upload_r2(blob_bytes: bytes, account_id: str, access_key_id: str, secret: str,
              bucket: str, object_name: str, cache_control: str, content_type: str):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret,
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        region_name="auto",
    )
    s3.put_object(
        Bucket=bucket,
        Key=object_name,
        Body=blob_bytes,
        ContentType=content_type,
        CacheControl=cache_control,
    )
    print(f"[R2] Uploaded r2://{bucket}/{object_name} ({len(blob_bytes)} bytes)")

def main():
    limit = int(os.getenv("SBDB_LIMIT", "10000"))
    days_lookback = int(os.getenv("DAYS_LOOKBACK", "7"))
    include_comets = env_bool("INCLUDE_COMETS", True)

    # Fetch
    print(f"Fetching SBDB: limit={limit}, days_lookback={days_lookback}, include_comets={include_comets}")
    rows = fetch_sbdb(limit, days_lookback, include_comets)
    if not rows:
        print("No rows returned; exiting without uploads.", file=sys.stderr)
        sys.exit(1)
    print(f"Fetched {len(rows)} rows.")

    # Serialize
    csv_bytes = write_csv_bytes(rows)
    json_bytes = write_json_bytes(rows)

    # Upload GCS (CSV)
    gcs_bucket = os.getenv("GCS_BUCKET")
    gcs_object = os.getenv("GCS_OBJECT", "catalogs/naif/naif_ids.csv")
    gcs_cc = os.getenv("GCS_CACHE_CONTROL", "public, max-age=3600")
    if not gcs_bucket:
        print("GCS_BUCKET not set; skipping GCS upload.", file=sys.stderr)
    else:
        upload_gcs(csv_bytes, gcs_bucket, gcs_object, gcs_cc)

    # Upload R2 (JSON)
    r2_bucket = os.getenv("R2_BUCKET")
    r2_object = os.getenv("R2_OBJECT", "catalogs/naif/naif_ids.json")
    r2_cc = os.getenv("R2_CACHE_CONTROL", "public, max-age=3600")
    r2_account = os.getenv("CF_R2_ACCOUNT_ID")
    r2_key = os.getenv("CF_R2_ACCESS_KEY_ID")
    r2_secret = os.getenv("CF_R2_SECRET_ACCESS_KEY")

    if not all([r2_bucket, r2_object, r2_account, r2_key, r2_secret]):
        print("R2 env not fully set; skipping R2 upload.", file=sys.stderr)
    else:
        upload_r2(json_bytes, r2_account, r2_key, r2_secret, r2_bucket, r2_object, r2_cc, "application/json")

if __name__ == "__main__":
    main()
