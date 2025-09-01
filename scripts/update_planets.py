"""
Daily JPL Horizons planet fetcher & loader.

What this script does
---------------------
1) Queries the JPL Horizons API (text mode) for heliocentric **osculating elements**
   for the 8 major planets at one epoch (today → today+1 day, step=1d).
   - Extracted fields per planet: a, e, i, Ω, ω, ν, epoch.
   - Adds static physical constants bundled in this file (radius r, μ, rotation params).
2) Normalizes records and writes a **CSV** to /tmp/<OUT_CSV_NAME>.
3) **Upserts** the rows into PostgreSQL (via Cloud SQL Auth Proxy on 127.0.0.1:5432)
   into table **public.planet_catalog** using `objnum` as the primary key.
   - Stored columns: objnum, category, name, naif_id, a, e, i, omega_big (Ω), omega_small (ω),
     nu_true (ν), epoch, radius_km, mu_km3s2, rot_rate, rot_lat, rot_lon.
4) Uploads the CSV to **Google Cloud Storage** (bucket/object configurable).
5) Produces a compact **JSON** array in /tmp/<OUT_JSON_NAME> and uploads it to
   **Cloudflare R2** (S3-compatible). Both GCS and R2 uploads set Cache-Control
   for reasonable edge caching.

Environment variables expected
------------------------------
# PostgreSQL (choose either DATABASE_URL or discrete PG* vars)
PGHOST                 default 127.0.0.1  (Cloud SQL Auth Proxy local listener)
PGPORT                 default 5432
PGUSER                 (required if no DATABASE_URL)
PGPASSWORD             (required if no DATABASE_URL)
PGDATABASE             (required if no DATABASE_URL)
DATABASE_URL           optional  e.g. postgres://user:pass@host:5432/dbname?sslmode=disable
                       (When using the proxy, ensure sslmode=disable.)

# Google Cloud Storage (uses Application Default Credentials via OIDC or SA key)
GCS_BUCKET_NAME        (required)
GCS_OBJECT_NAME        optional  default workflow/planets.csv

# Cloudflare R2 (S3-compatible)
R2_ENDPOINT            (required) 
R2_ACCESS_KEY_ID       (required)
R2_SECRET_ACCESS_KEY   (required)
R2_BUCKET              (required)
R2_JSON_KEY            optional  default planets.json
                       (If any R2 var is missing, the R2 upload is skipped.)

# Output filenames (written to /tmp/)
OUT_CSV_NAME           optional  default planets.csv
OUT_JSON_NAME          optional  default planets.json

Operational notes
-----------------
- Expect the **Cloud SQL Auth Proxy** to be started by the workflow before this script runs.
  With the proxy on localhost, the DB client should use sslmode=disable (the proxy handles TLS).
- GCS auth: use **GitHub OIDC → google-github-actions/auth** so the GCS client picks up
  short-lived credentials via ADC; no long-lived keys needed.
- Horizons etiquette: the script sleeps ~1s between requests to be polite.
- Failure handling: DB/GCS/R2 steps log errors and continue so one failure doesn’t hide others.
- Idempotent loads: UPSERT ensures repeated runs won’t duplicate rows.
- Security: keep DB creds and R2 keys in GitHub Secrets; avoid echoing envs in logs.

Change log (high level)
-----------------------
- Writes CSV to GCS instead of committing artifacts to the repo.
- Uses table **public.planet_catalog** (replaces previous table name).
- Removed report file writing; logs are printed to stdout for CI visibility.
"""

import os, sys, re, json, time, csv
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional

import requests
import psycopg2
from psycopg2.extras import execute_values
from google.cloud import storage
import boto3
from botocore.client import Config

API_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"

PLANET_DATA = {
    "000100000": {"id": "199", "name": "Mercury",  "r": "2439.7", "μ": "22031.8685", "R_rate": "0.0000687", "R_lat": "61.45", "R_lon": "281.01"},
    "000200000": {"id": "299", "name": "Venus",    "r": "6051.8", "μ": "324858.592", "R_rate": "-0.0000148","R_lat": "67.16", "R_lon": "272.76"},
    "000300000": {"id": "399", "name": "Earth",    "r": "6371.0", "μ": "398600.4355","R_rate": "0.004178",  "R_lat": "90.00", "R_lon": "0.0"},
    "000400000": {"id": "499", "name": "Mars",     "r": "3389.5", "μ": "42828.3758", "R_rate": "0.004058",  "R_lat": "52.85", "R_lon": "317.68"},
    "000500000": {"id": "599", "name": "Jupiter",  "r": "69911",  "μ": "126686531.9","R_rate": "0.01021",   "R_lat": "84.28", "R_lon": "268.06"},
    "000600000": {"id": "699", "name": "Saturn",   "r": "58232",  "μ": "37931206.2", "R_rate": "0.00949",   "R_lat": "83.54", "R_lon": "40.59"},
    "000700000": {"id": "799", "name": "Uranus",   "r": "25362",  "μ": "5793951.2",  "R_rate": "-0.00585",  "R_lat": "15.18", "R_lon": "257.31"},
    "000800000": {"id": "899", "name": "Neptune",  "r": "24622",  "μ": "6835099.3",  "R_rate": "0.00624",   "R_lat": "43.46", "R_lon": "299.33"}
}

# Local JSON (still written so your web app can build locally if needed)
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
LOCAL_JSON_PATH = PROJECT_ROOT / "public" / "data" / "json" / "planets.json"

# One daily epoch window
START_TIME_DT = datetime.now(timezone.utc)
STOP_TIME_DT  = START_TIME_DT + timedelta(days=1)
START_TIME_STR = START_TIME_DT.strftime("%Y-%m-%d")
STOP_TIME_STR  = STOP_TIME_DT.strftime("%Y-%m-%d")

# ---- ENV (set by GitHub Actions) ----
# Cloud SQL (via proxy)
PGHOST = os.environ.get("PGHOST", "127.0.0.1")
PGPORT = int(os.environ.get("PGPORT", "5432"))
PGUSER = os.environ.get("PGUSER")
PGPASSWORD = os.environ.get("PGPASSWORD")
PGDATABASE = os.environ.get("PGDATABASE")
DATABASE_URL = os.environ.get("DATABASE_URL")  # optional override

# GCS
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "")
GCS_OBJECT_NAME = os.environ.get("GCS_OBJECT_NAME", "workflow/planets.csv")

# R2 (S3-compatible)
R2_ENDPOINT = os.environ.get("R2_ENDPOINT")  # https://<accountid>.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.environ.get("R2_BUCKET")
R2_JSON_KEY = os.environ.get("R2_JSON_KEY", "planets.json")

# Output filenames
OUT_CSV_NAME = os.environ.get("OUT_CSV_NAME", "planets.csv")
OUT_JSON_NAME = os.environ.get("OUT_JSON_NAME", "planets.json")
TMP_CSV_PATH = Path("/tmp") / OUT_CSV_NAME
TMP_JSON_PATH = Path("/tmp") / OUT_JSON_NAME

TABLE_NAME = "public.planet_catalog"   # <-- renamed table

# ------------------------------ Helpers ------------------------------

def format_value(value_str, decimal_places=4):
    if value_str is None:
        return None
    try:
        if "+-" in value_str:
            value_str = value_str.split("+-")[0]
        formatted_str = f"{float(value_str):.{decimal_places}f}".rstrip("0").rstrip(".")
        return formatted_str if formatted_str else "0"
    except (ValueError, TypeError):
        return value_str

def parse_horizons_elements(response_text: str) -> Optional[Dict[str, Any]]:
    m = re.search(r"\$\$SOE(.*)\$\$EOE", response_text, re.DOTALL)
    if not m:
        return None
    data_block = m.group(1)
    lines = data_block.strip().split("\n")
    values = {}
    for line in lines[1:5]:
        pairs = re.findall(r"(\S+)\s*=\s*(\S+)", line)
        for key, value in pairs:
            values[key.strip()] = value.strip()
    return {
        "a":     format_value(values.get("A"), 0),
        "e":     format_value(values.get("EC"), 6),
        "i":     format_value(values.get("IN"), 4),
        "Ω":     format_value(values.get("OM"), 4),
        "ω":     format_value(values.get("W"), 4),
        "ν":     format_value(values.get("TA"), 4),
        "epoch": lines[0].strip().split("=")[0].strip()
    }

def horizons_fetch_planets() -> List[Dict[str, Any]]:
    headers = {"User-Agent": "ExoAtlas-PlanetData-Bot/1.0 (+https://exoatlas.com/contact)"}
    out = []
    for objnum, info in PLANET_DATA.items():
        pid, name = info["id"], info["name"]
        print(f"[fetch] {name} ({pid})")
        try:
            params = {
                "format": "text",
                "COMMAND": f"'{pid}'",
                "OBJ_DATA": "NO",
                "MAKE_EPHEM": "YES",
                "EPHEM_TYPE": "ELEMENTS",
                "CENTER": "@sun",
                "START_TIME": START_TIME_STR,
                "STOP_TIME": STOP_TIME_STR,
                "STEP_SIZE": "1d",
                "OUT_UNITS": "KM-S",
                "REF_PLANE": "ECLIPTIC",
            }
            r = requests.get(API_URL, params=params, headers=headers, timeout=60)
            r.raise_for_status()
            el = parse_horizons_elements(r.text)
            if el:
                out.append({
                    "objnum": objnum,
                    "category": "Planet",
                    "name": name,
                    "naif_id": pid,
                    "a": el.get("a"),
                    "e": el.get("e"),
                    "i": el.get("i"),
                    "Ω": el.get("Ω"),
                    "ω": el.get("ω"),
                    "ν": el.get("ν"),
                    "epoch": el.get("epoch"),
                    "r": info.get("r"),
                    "μ": info.get("μ"),
                    "R_rate": info.get("R_rate"),
                    "R_lat": info.get("R_lat"),
                    "R_lon": info.get("R_lon"),
                })
        except Exception as e:
            print(f"[warn] {name}: {e}")
        time.sleep(1)  # polite to Horizons
    return out

def write_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["objnum","category","name","naif_id","a","e","i","Ω","ω","ν","epoch","r","μ","R_rate","R_lat","R_lon"]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})

def _num(x):
    try:
        if x is None or x == "":
            return None
        return float(str(x).replace(",", ""))
    except Exception:
        return None

def pg_connect():
    if DATABASE_URL:
        return psycopg2.connect(DATABASE_URL, connect_timeout=10)
    # proxy on localhost: disable SSL for proxy connections
    return psycopg2.connect(
        host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE,
        connect_timeout=10, sslmode="disable"
    )

def pg_upsert(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
      objnum      text PRIMARY KEY,
      category    text NOT NULL,
      name        text NOT NULL,
      naif_id     text NOT NULL,
      a           numeric,
      e           numeric,
      i           numeric,
      omega_big   numeric, -- Ω
      omega_small numeric, -- ω
      nu_true     numeric, -- ν
      epoch       text,
      radius_km   numeric,
      mu_km3s2    numeric,
      rot_rate    numeric,
      rot_lat     numeric,
      rot_lon     numeric,
      updated_at  timestamptz NOT NULL DEFAULT now()
    );
    """
    tuples = []
    for r in rows:
        tuples.append((
            r.get("objnum"), r.get("category"), r.get("name"), r.get("naif_id"),
            _num(r.get("a")), _num(r.get("e")), _num(r.get("i")),
            _num(r.get("Ω")), _num(r.get("ω")), _num(r.get("ν")),
            r.get("epoch"),
            _num(r.get("r")), _num(r.get("μ")),
            _num(r.get("R_rate")), _num(r.get("R_lat")), _num(r.get("R_lon")),
        ))
    insert_sql = f"""
    INSERT INTO {TABLE_NAME}
    (objnum,category,name,naif_id,a,e,i,omega_big,omega_small,nu_true,epoch,radius_km,mu_km3s2,rot_rate,rot_lat,rot_lon)
    VALUES %s
    ON CONFLICT (objnum) DO UPDATE SET
      category=EXCLUDED.category,
      name=EXCLUDED.name,
      naif_id=EXCLUDED.naif_id,
      a=EXCLUDED.a,
      e=EXCLUDED.e,
      i=EXCLUDED.i,
      omega_big=EXCLUDED.omega_big,
      omega_small=EXCLUDED.omega_small,
      nu_true=EXCLUDED.nu_true,
      epoch=EXCLUDED.epoch,
      radius_km=EXCLUDED.radius_km,
      mu_km3s2=EXCLUDED.mu_km3s2,
      rot_rate=EXCLUDED.rot_rate,
      rot_lat=EXCLUDED.rot_lat,
      rot_lon=EXCLUDED.rot_lon,
      updated_at=now();
    """
    conn = pg_connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(ddl)
            execute_values(cur, insert_sql, tuples, page_size=50)
        return len(tuples)
    finally:
        conn.close()

def gcs_upload(local_path: Path, bucket_name: str, object_name: str) -> str:
    client = storage.Client()  # uses ADC from OIDC
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.cache_control = "public, max-age=86400"  # 1 day
    blob.upload_from_filename(str(local_path), content_type="text/csv; charset=utf-8")
    return f"gs://{bucket_name}/{object_name}"

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
        "CacheControl": "public, max-age=86400",
    }
    s3.upload_file(str(json_path), bucket, key, ExtraArgs=extra)
    return f"{bucket}/{key}"

def main() -> int:
    print(f"Run UTC: {datetime.now(timezone.utc).isoformat()}")
    print(f"Window: {START_TIME_STR} → {STOP_TIME_STR}")

    # 1) Fetch from Horizons
    rows = horizons_fetch_planets()
    if len(rows) != len(PLANET_DATA):
        print(f"[warn] fetched {len(rows)}/{len(PLANET_DATA)} planets")

    # 2) CSV → /tmp
    write_csv(rows, TMP_CSV_PATH)
    print(f"[ok] CSV written: {TMP_CSV_PATH} ({len(rows)} rows)")

    # 3) UPSERT to Cloud SQL (via proxy)
    try:
        n = pg_upsert(rows)
        print(f"[ok] Postgres upserted: {n} rows into {TABLE_NAME}")
    except Exception as e:
        print(f"[err] Postgres upsert failed: {e}")

    # 4) Upload CSV to GCS
    try:
        gcs_uri = gcs_upload(TMP_CSV_PATH, GCS_BUCKET_NAME, GCS_OBJECT_NAME or OUT_CSV_NAME)
        print(f"[ok] GCS uploaded: {gcs_uri}")
    except Exception as e:
        print(f"[err] GCS upload failed: {e}")

    # 5) JSON → local + R2
    try:
        TMP_JSON_PATH.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
        if all([R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET, R2_JSON_KEY]):
            obj = r2_upload_json(TMP_JSON_PATH, R2_BUCKET, R2_JSON_KEY)
            print(f"[ok] R2 uploaded: {obj}")
    except Exception as e:
        print(f"[err] R2 upload failed: {e}")

    print("[done] planets pipeline finished")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
