"""
NAIF/SPK Object ID exporter:
- Pulls small-body objects (asteroids + optional comets) from JPL SBDB Query API
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
from google.cloud import storage
import boto3

SBDB_QUERY_URL = "https://ssd-api.jpl.nasa.gov/sbdb_query.api"


# --------------------------- helpers ---------------------------

def env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "y", "on")


def _http_get(params: dict) -> dict:
    """
    GET with small retry loop and helpful error message on failure.
    We avoid using unsupported 'where' filters that trigger 400s.
    """
    last_err: t.Optional[Exception] = None
    for attempt in range(3):
        try:
            r = requests.get(SBDB_QUERY_URL, params=params, timeout=60)
            if r.status_code == 200:
                return r.json()
            # Surface response body for diagnosis
            body = r.text[:4000]
            # Retry only on transient server/rate errors
            if r.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                time.sleep(2 * (attempt + 1))
                continue
            raise requests.HTTPError(f"SBDB {r.status_code} for {r.url}\nBody: {body}")
        except Exception as e:
            last_err = e
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
    # Final failure
    raise last_err or RuntimeError("SBDB request failed")


def _parse_last_obs(s: t.Optional[str]) -> t.Optional[datetime]:
    """SBDB returns last_obs like 'YYYY-MM-DD'. Be tolerant if it's missing."""
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return None


# ------------------------- main fetch --------------------------

def fetch_sbdb(limit: int, days_lookback: int, include_comets: bool) -> t.List[dict]:
    """
    Pulls objects from SBDB, then filters by last_obs locally.
    NAIF/SPK ID mapping used:
      - asteroid (numbered): 2000000 + number
      - comet (no simple integer): synthetic 1000000 + hash(pdes) % 900000 (best-effort)
    """
    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days_lookback)).date()

    rows: t.List[dict] = []

    # ---- Asteroids (numbered) ----
    # We purposely do NOT include a 'where' filter—it's what caused SBDB 400s.
    # Ask for a large limit and filter locally by last_obs >= cutoff.
    ast_params = {
        "fields": "number,full_name,last_obs",
        "limit": str(limit),
        # We'll avoid order hints for maximum compatibility.
        # "order": "-last_obs"  # uncomment if you find it consistently supported
    }
    data_ast = _http_get(ast_params)

    for rec in data_ast.get("data", []):
        # shape: [number, full_name, last_obs]
        if len(rec) < 3:
            continue
        number, full_name, last_obs = rec
        try:
            num = int(number)
        except Exception:
            continue
        dt = _parse_last_obs(last_obs)
        if dt and dt.date() < cutoff_date:
            continue
        spkid = 2000000 + num
        rows.append({"object_id": spkid, "name": str(full_name).strip()})

    # ---- Comets (optional) ----
    if include_comets:
        # Try a comet-only query first; if the API 400s on where, fallback to global + local filter.
        comet_params_primary = {
            "fields": "pdes,full_name,last_obs",
            "limit": str(limit),
            "where": "comet=1",
        }
        try:
            data_cmt = _http_get(comet_params_primary)
        except Exception:
            # Fallback: fetch mixed list, then filter locally where 'comet' field == 1 if present.
            comet_params_fallback = {
                "fields": "pdes,full_name,last_obs,comet",
                "limit": str(limit),
            }
            data_cmt = _http_get(comet_params_fallback)

        for rec in data_cmt.get("data", []):
            # Possible shapes:
            #  - [pdes, full_name, last_obs]
            #  - [pdes, full_name, last_obs, comet]
            if len(rec) < 3:
                continue
            pdes, full_name, last_obs = rec[0], rec[1], rec[2]
            # If comet flag present, keep only comets
            if len(rec) >= 4:
                comet_flag = rec[3]
                try:
                    if int(comet_flag) != 1:
                        continue
                except Exception:
                    # If not parseable, ignore and accept record
                    pass

            dt = _parse_last_obs(last_obs)
            if dt and dt.date() < cutoff_date:
                continue

            # bounded synthetic ID for comets without a simple integer mapping
            seq = abs(hash(pdes)) % 900000
            spkid = 1000000 + seq
            rows.append({"object_id": spkid, "name": str(full_name).strip()})

    # Deduplicate by object_id (keep latest name encountered)
    dedup: dict[int, str] = {}
    for r in rows:
        dedup[int(r["object_id"])] = r["name"]
    out = [{"object_id": k, "name": v} for k, v in dedup.items()]
    return out


# ----------------------- serialization ------------------------

def write_csv_bytes(rows: t.List[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["object_id", "name"])
    for r in sorted(rows, key=lambda x: int(x["object_id"])):
        writer.writerow([int(r["object_id"]), r["name"]])
    return buf.getvalue().encode("utf-8")


def write_json_bytes(rows: t.List[dict]) -> bytes:
    payload = [{"object_id": int(r["object_id"]), "name": r["name"]}
               for r in sorted(rows, key=lambda x: int(x["object_id"]))]
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


# ------------------------- uploads ----------------------------

def upload_gcs(blob_bytes: bytes, bucket: str, object_name: str, cache_control: str):
    client = storage.Client()  # uses ADC from WIF
    bkt = client.bucket(bucket)
    blob = bkt.blob(object_name)
    blob.cache_control = cache_control
    content_type = "text/csv; charset=utf-8" if object_name.endswith(".csv") else "application/octet-stream"
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


# --------------------------- CLI ------------------------------

def main():
    limit = int(os.getenv("SBDB_LIMIT", "10000"))
    days_lookback = int(os.getenv("DAYS_LOOKBACK", "7"))
    include_comets = env_bool("INCLUDE_COMETS", True)

    print(f"Fetching SBDB: limit={limit}, days_lookback={days_lookback}, include_comets={include_comets}")
    rows = fetch_sbdb(limit, days_lookback, include_comets)
    if not rows:
        print("No rows returned; exiting without uploads.", file=sys.stderr)
        sys.exit(1)
    print(f"Fetched {len(rows)} rows.")

    csv_bytes = write_csv_bytes(rows)
    json_bytes = write_json_bytes(rows)

    # Upload GCS (CSV)
    gcs_bucket = os.getenv("GCS_BUCKET")
    gcs_object = os.getenv("GCS_OBJECT", "workflow/naif_ids.csv")
    gcs_cc = os.getenv("GCS_CACHE_CONTROL", "public, max-age=3600, s-maxage=86400, immutable")
    if not gcs_bucket:
        print("GCS_BUCKET not set; skipping GCS upload.", file=sys.stderr)
    else:
        upload_gcs(csv_bytes, gcs_bucket, gcs_object, gcs_cc)

    # Upload R2 (JSON)
    r2_bucket = os.getenv("R2_BUCKET")
    r2_object = os.getenv("R2_OBJECT", "naif/naif_ids.json")
    r2_cc = os.getenv("R2_CACHE_CONTROL", "public, max-age=3600, s-maxage=86400, immutable")
    r2_account = os.getenv("CF_R2_ACCOUNT_ID")
    r2_key = os.getenv("CF_R2_ACCESS_KEY_ID")
    r2_secret = os.getenv("CF_R2_SECRET_ACCESS_KEY")

    if not all([r2_bucket, r2_object, r2_account, r2_key, r2_secret]):
        print("R2 env not fully set; skipping R2 upload.", file=sys.stderr)
    else:
        upload_r2(json_bytes, r2_account, r2_key, r2_secret, r2_bucket, r2_object, r2_cc, "application/json")


if __name__ == "__main__":
    main()
