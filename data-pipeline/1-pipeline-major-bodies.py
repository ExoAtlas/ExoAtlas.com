"""
NASA Horizons → ExoAtlas JSON Converter (Planets + Dwarf Planets)
-----------------------------------------------------------------
Fetches GEOMETRIC osculating elements (Ecliptic of J2000, TDB, KM-S, degrees)
from NASA/JPL Horizons for a fixed catalog of NAIF IDs and writes:
  1) CSV → /tmp → Cloudflare R2
  2) JSON → /tmp → Cloudflare R2

CENTER: Solar System Barycenter (@0) to match your parent mapping.

Units (from Horizons header):
  - Distances: km
  - Angles: degrees
  - Mean motion N: degrees/second (convert to deg/day if needed)
  - Period PR: seconds
  - Epoch (JDTDB/Tp): Julian Day (TDB)
"""

import os, sys, re, json, time, csv
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional

import requests
import boto3
from botocore.client import Config

API_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"

# ---------------------------------------------------------------------
# Catalog to fetch (your list). Parent is the Solar System Barycenter.
# type -> used for 'category' in DB/JSON.
# ---------------------------------------------------------------------
BODY_CATALOG = [
    {"id": "10",     "name": "Sun (Sol)", "type": "Sun"},
    {"id": "199",    "name": "Mercury",   "type": "Planet"},
    {"id": "299",    "name": "Venus",     "type": "Planet"},
    {"id": "399",    "name": "Earth",     "type": "Planet"},
    {"id": "499",    "name": "Mars",      "type": "Planet"},
    {"id": "599",    "name": "Jupiter",   "type": "Planet"},
    {"id": "699",    "name": "Saturn",    "type": "Planet"},
    {"id": "799",    "name": "Uranus",    "type": "Planet"},
    {"id": "899",    "name": "Neptune",   "type": "Planet"},
    {"id": "134340", "name": "Pluto",     "type": "Dwarf Planet"},
    {"id": "136199", "name": "Eris",      "type": "Dwarf Planet"},
    {"id": "136108", "name": "Haumea",    "type": "Dwarf Planet"},
    {"id": "136472", "name": "Makemake",  "type": "Dwarf Planet"},
    {"id": "225088", "name": "Gonggong",  "type": "Dwarf Planet"},
    {"id": "50000",  "name": "Quaoar",    "type": "Dwarf Planet"},
    {"id": "90482",  "name": "Orcus",     "type": "Dwarf Planet"},
    {"id": "90377",  "name": "Sedna",     "type": "Dwarf Planet"},
    {"id": "120347", "name": "Salacia",   "type": "Dwarf Planet"},
    {"id": "307261", "name": "Máni",      "type": "Dwarf Planet"},
    {"id": "55565",  "name": "Aya",       "type": "Dwarf Planet"},
    {"id": "174567", "name": "Varda",     "type": "Dwarf Planet"},
    {"id": "532037", "name": "Chiminigagua","type": "Dwarf Planet"},
    {"id": "208996", "name": "Achlys",    "type": "Dwarf Planet"},
    {"id": "28978",  "name": "Ixion",     "type": "Dwarf Planet"},
]

# Optional physical constants for some bodies (mean radius km, mu km^3/s^2, crude rotation)
# These are "self" GMs for the body (not the Sun's). Missing values default to None.
PHYS_DEFAULTS = {
    # Sun + major planets
    "10":     {"r": 695700.0,   "mu": 132712440018.0, "R_rate": None,      "R_lat": 63.87,  "R_lon": 286.13},
    "199":    {"r": 2439.7,     "mu": 22031.86855,    "R_rate": 6.1385e-3, "R_lat": 61.45,  "R_lon": 281.01},
    "299":    {"r": 6051.84,    "mu": 324858.592,     "R_rate": -2.9924e-7,"R_lat": 67.16,  "R_lon": 272.76},
    "399":    {"r": 6371.0084,  "mu": 398600.435436,  "R_rate": 0.004178,  "R_lat": 90.0,   "R_lon": 0.0},
    "499":    {"r": 3389.5,     "mu": 42828.375214,   "R_rate": 0.004058,  "R_lat": 52.86,  "R_lon": 317.68},
    "599":    {"r": 69911.0,    "mu": 126686531.9,    "R_rate": 0.01021,   "R_lat": 84.28,  "R_lon": 268.06},
    "699":    {"r": 58232.0,    "mu": 37931206.0,     "R_rate": 0.00949,   "R_lat": 83.54,  "R_lon": 40.59},
    "799":    {"r": 25362.0,    "mu": 5793951.0,      "R_rate": -0.00585,  "R_lat": 15.18,  "R_lon": 257.31},
    "899":    {"r": 24622.0,    "mu": 6835099.0,      "R_rate": 0.00624,   "R_lat": 43.46,  "R_lon": 299.33},
    # Dwarfs: leave None unless you want to hardcode authoritative values
}

# Local JSON (optional local write)
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
LOCAL_JSON_PATH = PROJECT_ROOT / "public" / "data" / "json" / "major-bodies.json"

# One daily epoch window (TDB-aligned timestamp not required; Horizons uses TDB internally)
START_TIME_DT = datetime.now(timezone.utc)
STOP_TIME_DT  = START_TIME_DT + timedelta(days=1)
START_TIME_STR = START_TIME_DT.strftime("%Y-%m-%d")
STOP_TIME_STR  = STOP_TIME_DT.strftime("%Y-%m-%d")

# ---- ENV (set by GitHub Actions) ----
# R2 (S3-compatible)
R2_ENDPOINT = os.environ.get("R2_ENDPOINT")  # https://<accountid>.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.environ.get("R2_BUCKET")
R2_JSON_KEY = os.environ.get("R2_JSON_KEY", "major-bodies.json")
R2_CACHE_CONTROL = os.environ.get("R2_CACHE_CONTROL", "public, max-age=86400") 

# Output filenames
OUT_CSV_NAME = os.environ.get("OUT_CSV_NAME", "major-bodies.csv")
OUT_JSON_NAME = os.environ.get("OUT_JSON_NAME", "major-bodies.json")
TMP_CSV_PATH = Path("/tmp") / OUT_CSV_NAME
TMP_JSON_PATH = Path("/tmp") / OUT_JSON_NAME


# ------------------------------ Helpers ------------------------------

def format_value(value_str, decimal_places=6):
    """Format numeric strings (possibly with '+-' sigmas) into trimmed decimals."""
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
    """
    Parses the Horizons ELEMENTS block:
      JDTDB
      EC QR IN
      OM W Tp
      N MA TA
      A AD PR
    Returns dict with a,e,i,Ω,ω,ν, epoch JDTDB string; you can add N/PR if needed.
    """
    m = re.search(r"\$\$SOE(.*)\$\$EOE", response_text, re.DOTALL)
    if not m:
        return None
    data_block = m.group(1).strip()
    lines = [ln for ln in data_block.splitlines() if ln.strip()]
    # Line 0 is: "<JDTDB> = A.D. <iso TDB>"
    epoch_jd = lines[0].split("=")[0].strip()
    values = {}
    for line in lines[1:5]:
        for key, value in re.findall(r"([A-Za-z]+)\s*=\s*([+\-0-9.E]+)", line):
            values[key.strip()] = value.strip()
    return {
        "a":     format_value(values.get("A"), 6),
        "e":     format_value(values.get("EC"), 9),
        "i":     format_value(values.get("IN"), 6),
        "Ω":     format_value(values.get("OM"), 6),
        "ω":     format_value(values.get("W"), 6),
        "ν":     format_value(values.get("TA"), 6),
        "epoch": epoch_jd,  # JDTDB as string
        # Optional extras if you later want them:
        # "N_deg_per_s": format_value(values.get("N"), 12),
        # "PR_s":        format_value(values.get("PR"), 3),
    }

def get_phys_defaults(naif_id: str) -> Dict[str, Optional[float]]:
    d = PHYS_DEFAULTS.get(naif_id, {})
    return {
        "r": d.get("r"),
        "mu": d.get("mu"),
        "R_rate": d.get("R_rate"),
        "R_lat": d.get("R_lat"),
        "R_lon": d.get("R_lon"),
    }

def horizons_fetch_catalog() -> List[Dict[str, Any]]:
    headers = {"User-Agent": "ExoAtlas-PlanetData-Bot/1.0 (+https://exoatlas.com/contact/)"}
    out = []
    for entry in BODY_CATALOG:
        pid, name, category = entry["id"], entry["name"], entry["type"]
        print(f"[fetch] {name} ({pid})")
        try:
            params = {
                "format": "text",
                "COMMAND": f"'{pid}'",
                "OBJ_DATA": "NO",
                "MAKE_EPHEM": "YES",
                "EPHEM_TYPE": "ELEMENTS",
                "CENTER": "@0",            # Solar System Barycenter
                "START_TIME": START_TIME_STR,
                "STOP_TIME": STOP_TIME_STR,
                "STEP_SIZE": "1d",
                "OUT_UNITS": "KM-S",
                "REF_PLANE": "ECLIPTIC",
            }
            r = requests.get(API_URL, params=params, headers=headers, timeout=60)
            r.raise_for_status()
            el = parse_horizons_elements(r.text)
            if not el:
                raise RuntimeError("Elements block not found")
            phys = get_phys_defaults(pid)
            out.append({
                "objnum": pid,              # use NAIF id as stable key
                "category": category,       # Sun / Planet / Dwarf Planet
                "name": name,
                "naif_id": pid,
                "a": el.get("a"),
                "e": el.get("e"),
                "i": el.get("i"),
                "Ω": el.get("Ω"),
                "ω": el.get("ω"),
                "ν": el.get("ν"),
                "epoch": el.get("epoch"),   # JDTDB
                "r": phys.get("r"),
                "μ": phys.get("mu"),
                "R_rate": phys.get("R_rate"),
                "R_lat": phys.get("R_lat"),
                "R_lon": phys.get("R_lon"),
            })
        except Exception as e:
            print(f"[warn] {name}: {e}")
        time.sleep(1)  # polite
    return out

def write_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["objnum","category","name","naif_id","a","e","i","Ω","ω","ν","epoch","r","μ","R_rate","R_lat","R_lon"]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})

def r2_upload_json(json_path: Path, bucket: str, key: str) -> str:
    session = boto3.session.Session()
    endpoint = (R2_ENDPOINT or "").rstrip("/")
    s3 = session.client(
        service_name="s3",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        endpoint_url=endpoint if endpoint else None, 
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )
    extra = {
        "ContentType": "application/json; charset=utf-8",
        "CacheControl": R2_CACHE_CONTROL,
    }
    s3.upload_file(str(json_path), bucket, key, ExtraArgs=extra)
    return f"{bucket}/{key}"

def main() -> int:
    print(f"Run UTC: {datetime.now(timezone.utc).isoformat()}")
    print(f"Window: {START_TIME_STR} → {STOP_TIME_STR} (CENTER=@0 / SSB)")

    # 1) Fetch from Horizons
    rows = horizons_fetch_catalog()
    if len(rows) != len(BODY_CATALOG):
        print(f"[warn] fetched {len(rows)}/{len(BODY_CATALOG)} bodies")

    # 2) CSV → /tmp
    write_csv(rows, TMP_CSV_PATH)
    print(f"[ok] CSV written: {TMP_CSV_PATH} ({len(rows)} rows)")

    # 3) JSON → local + R2
    try:
        TMP_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
        TMP_JSON_PATH.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
        if all([R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET, R2_JSON_KEY]):
            obj = r2_upload_json(TMP_JSON_PATH, R2_BUCKET, R2_JSON_KEY)
            print(f"[ok] R2 uploaded: {obj}")
        # Optional: also write to repo-local for dev preview
        try:
            LOCAL_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
            LOCAL_JSON_PATH.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"[ok] Local JSON written: {LOCAL_JSON_PATH}")
        except Exception:
            pass
    except Exception as e:
        print(f"[err] R2/local JSON step failed: {e}")

    print("[done] bodies pipeline finished")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

