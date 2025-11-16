"""
Daily Space-Track GP (TLE) data fetcher & CSV/JSON uploader to Cloudflare R2.

What this script does
---------------------
1) Logs into Space-Track and pulls newest GP elset per object (class/gp).
2) Writes a CSV and JSON snapshot of all rows to /tmp.
3) Uploads both files to a Cloudflare R2 bucket (S3-compatible).

Environment variables expected
------------------------------
ST_USERNAME            (required)  Space-Track login
ST_PASSWORD            (required)

R2_ENDPOINT            (required)
R2_BUCKET_PRIVATE            (required)
R2_ACCESS_KEY_ID_PRIVATE     (required)
R2_SECRET_ACCESS_KEY_PRIVATE (required)
R2_CSV_OBJECT_NAME     optional    default workflow/spacetrack_full.csv
R2_JSON_OBJECT_NAME    optional    default workflow/spacetrack_full.json

Optional knobs
--------------
REQUEST_TIMEOUT        connect/read timeout tuple (fixed in code)
"""

from __future__ import annotations

import os
import csv
import json
import time
import random
from datetime import datetime, timezone
from typing import List, Dict

import requests
import boto3

# ---------------------- Configuration ----------------------

BASE = "https://www.space-track.org"
S = requests.Session()
REQUEST_TIMEOUT = (10, 120)  # (connect, read) seconds

# Space-Track creds
SPACE_TRACK_USER = os.getenv("ST_USERNAME")
SPACE_TRACK_PASS = os.getenv("ST_PASSWORD")

# Cloudflare R2 (S3-compatible) config
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY_ID_PRIVATE = os.getenv("R2_ACCESS_KEY_ID_PRIVATE")
R2_SECRET_ACCESS_KEY_PRIVATE = os.getenv("R2_SECRET_ACCESS_KEY_PRIVATE")
R2_BUCKET_PRIVATE = os.getenv("R2_BUCKET_PRIVATE")
R2_CSV_OBJECT_NAME = os.getenv("R2_CSV_OBJECT_NAME", "spacetrack_catalog.csv")
R2_JSON_OBJECT_NAME = os.getenv("R2_JSON_OBJECT_NAME", "spacetrack_catalog.json")


# ---------------------- Helpers ----------------------------

def log(msg: str) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{now}] {msg}", flush=True)


def _assert_required_env() -> None:
    missing = []
    for k in [
        "ST_USERNAME",
        "ST_PASSWORD",
        "R2_ENDPOINT",
        "R2_ACCESS_KEY_ID_PRIVATE",
        "R2_SECRET_ACCESS_KEY_PRIVATE",
        "R2_BUCKET_PRIVATE",
    ]:
        if not os.getenv(k):
            missing.append(k)

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


def fetch_gp_chunk(norad_min: int, norad_max: int) -> List[Dict]:
    """Newest elset per object in a NORAD range, JSON (includes TLE_LINE1/2)."""
    url = (
        f"{BASE}/basicspacedata/query/class/gp/"
        f"NORAD_CAT_ID/{norad_min}--{norad_max}/"
        f"orderby/NORAD_CAT_ID asc/limit/200000/format/json"
    )
    r = _safe_get(url)
    return r.json()


def fetch_gp_all() -> List[Dict]:
    ranges = [
        (1, 99999),
        (100000, 199999),
        (200000, 299999),
        (300000, 399999),  # future-proof
    ]
    out: List[Dict] = []
    for a, b in ranges:
        log(f"Fetching GP range {a}–{b}…")
        out.extend(fetch_gp_chunk(a, b))
        time.sleep(1)  # be polite; well under 30/min
    log(f"Fetched {len(out)} records from Space-Track.")
    return out


# ---------------------- Local export -----------------------

FIELDNAMES = [
    "NORAD_CAT_ID",
    "OBJECT_NAME",
    "OBJECT_ID",
    "EPOCH",
    "MEAN_MOTION",
    "ECCENTRICITY",
    "INCLINATION",
    "RA_OF_ASC_NODE",
    "ARG_OF_PERICENTER",
    "MEAN_ANOMALY",
    "BSTAR",
    "TLE_LINE0",
    "TLE_LINE1",
    "TLE_LINE2",
    "CREATION_DATE",
    "GP_ID",
]


def write_rows_to_csv(rows: List[Dict], path: str) -> None:
    """
    Write rows to CSV. Columns mirror what used to be stored in gp_catalog.
    """
    log(f"Writing {len(rows)} rows to CSV at {path}…")
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "NORAD_CAT_ID": row.get("NORAD_CAT_ID"),
                "OBJECT_NAME": row.get("OBJECT_NAME"),
                "OBJECT_ID": row.get("OBJECT_ID"),
                "EPOCH": row.get("EPOCH"),
                "MEAN_MOTION": row.get("MEAN_MOTION"),
                "ECCENTRICITY": row.get("ECCENTRICITY"),
                "INCLINATION": row.get("INCLINATION"),
                "RA_OF_ASC_NODE": row.get("RA_OF_ASC_NODE"),
                "ARG_OF_PERICENTER": row.get("ARG_OF_PERICENTER"),
                "MEAN_ANOMALY": row.get("MEAN_ANOMALY"),
                "BSTAR": row.get("BSTAR"),
                "TLE_LINE0": row.get("TLE_LINE0"),
                "TLE_LINE1": row.get("TLE_LINE1"),
                "TLE_LINE2": row.get("TLE_LINE2"),
                "CREATION_DATE": row.get("CREATION_DATE"),
                "GP_ID": row.get("GP_ID"),
            })


def write_rows_to_json(rows: List[Dict], path: str) -> None:
    """
    Write rows to JSON (list of objects with the same fields as the CSV).
    """
    log(f"Writing {len(rows)} rows to JSON at {path}…")

    # Normalize to only the expected fields to keep JSON tight and consistent.
    normalized = []
    for row in rows:
        normalized.append({
            "NORAD_CAT_ID": row.get("NORAD_CAT_ID"),
            "OBJECT_NAME": row.get("OBJECT_NAME"),
            "OBJECT_ID": row.get("OBJECT_ID"),
            "EPOCH": row.get("EPOCH"),
            "MEAN_MOTION": row.get("MEAN_MOTION"),
            "ECCENTRICITY": row.get("ECCENTRICITY"),
            "INCLINATION": row.get("INCLINATION"),
            "RA_OF_ASC_NODE": row.get("RA_OF_ASC_NODE"),
            "ARG_OF_PERICENTER": row.get("ARG_OF_PERICENTER"),
            "MEAN_ANOMALY": row.get("MEAN_ANOMALY"),
            "BSTAR": row.get("BSTAR"),
            "TLE_LINE0": row.get("TLE_LINE0"),
            "TLE_LINE1": row.get("TLE_LINE1"),
            "TLE_LINE2": row.get("TLE_LINE2"),
            "CREATION_DATE": row.get("CREATION_DATE"),
            "GP_ID": row.get("GP_ID"),
        })

    with open(path, "w") as f:
        json.dump(normalized, f, separators=(",", ":"), ensure_ascii=False)


# ---------------------- R2 Upload --------------------------

def get_r2_client():
    session = boto3.session.Session()
    return session.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID_PRIVATE,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY_PRIVATE,
        region_name="auto",
    )


def upload_to_r2(local_path: str, object_name: str, content_type: str) -> None:
    log(f"Uploading {local_path} to R2 bucket '{R2_PRIVATE_BUCKET_NAME}' as '{object_name}'…")
    s3 = get_r2_client()

    with open(local_path, "rb") as f:
        s3.upload_fileobj(
            f,
            R2_PRIVATE_BUCKET_NAME,
            object_name,
            ExtraArgs={"ContentType": content_type},
        )

    log(f"R2 upload complete for {object_name}.")


# ---------------------- Main -------------------------------

def main() -> None:
    _assert_required_env()
    log("Starting Space-Track data fetcher (R2 CSV + JSON)…")
    st_login()

    raw = fetch_gp_all()

    # Trim to exactly what we export; skip incomplete rows just in case
    rows: List[Dict] = []
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

    log(f"Prepared {len(rows)} cleaned rows for CSV/JSON export.")

    ts = int(time.time())
    csv_path = os.path.join("/tmp", f"spacetrack_export_{ts}.csv")
    json_path = os.path.join("/tmp", f"spacetrack_export_{ts}.json")

    write_rows_to_csv(rows, csv_path)
    write_rows_to_json(rows, json_path)

    upload_to_r2(csv_path, R2_CSV_OBJECT_NAME, "text/csv")
    upload_to_r2(json_path, R2_JSON_OBJECT_NAME, "application/json")

    log("All done.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Interrupted.")
        raise SystemExit(130)
    except Exception as e:
        log(f"FATAL: {e.__class__.__name__}: {e}")
        raise
