"""
Fetch osculating orbital elements for planetary satellites (moons) from JPL Horizons.

What this script does
---------------------
1) Discovers the list of natural satellites for a given planet by scraping the
   SSD "Planetary Satellite Physical Parameters" pages (contains NAIF IDs).
2) For each moon, queries JPL Horizons API (text mode) for **osculating elements**
   at a single epoch (today -> today+1 day, step=1d) with the planet as CENTER.
3) Writes a per-planet CSV/JSON to supplied paths.

Usage
-----
python fetch_moons.py --planet jupiter --out-json data/moons/moons_jupiter.json --out-csv data/moons/moons_jupiter.csv

Environment (optional)
----------------------
HORIZONS_USER_AGENT   Custom UA string (default set below)

Notes
-----
- Elements are requested in the **planet-centric frame** (CENTER='@<planet NAIF>').
- REF_PLANE='EQUATOR' for satellites to align with common satellite conventions.
- One-epoch snapshot: EPHEM_TYPE='ELEMENTS', START=utc today, STOP=today+1d, STEP=1d.
- Be polite to Horizons: ~1s sleep between requests.

References
----------
- Horizons API parameter doc (format/COMMAND/EPHEM_TYPE/CENTER/etc.).  # see cite
- SSD planetary satellites pages (lists of satellites and NAIF IDs).    # see cite
- Horizons Lookup API (fallback NAIF resolution).                        # see cite
"""

from __future__ import annotations
import os, re, csv, json, time, argparse, sys
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup  # pip install beautifulsoup4

# ---------- Config ----------
API_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"
LOOKUP_URL = "https://ssd-api.jpl.nasa.gov/horizons.api"  # using 'format=json' with COMMAND as needed
HORIZONS_USER_AGENT = os.environ.get(
    "HORIZONS_USER_AGENT",
    "ExoAtlas-MoonsData-Bot/1.0 (+https://exoatlas.com/contact)"
)

SSD_SATS_ROOT = "https://ssd.jpl.nasa.gov/sats/phys_par/"

PLANET_META = {
    # planet: (planet_naif_id, ssd_subpage)
    # NAIF IDs for planet system barycenters are x99; we'll use planet body ID (same for center here).
    "mercury": ("199", None),  # no known moons; still supported
    "venus":   ("299", None),  # no known moons
    "earth":   ("399", "saturn.html".replace("saturn","earth")),  # we’ll compute exact URL below
    "mars":    ("499", None),  # has its own section on main table page
    "jupiter": ("599", None),
    "saturn":  ("699", None),
    "uranus":  ("799", None),
    "neptune": ("899", None),
}

# Mapping planet -> anchor id used in the consolidated phys_par table (one page)
SSD_TABLE_ANCHORS = {
    "earth":   "Earth",
    "mars":    "Mars",
    "jupiter": "Jupiter",
    "saturn":  "Saturn",
    "uranus":  "Uranus",
    "neptune": "Neptune",
}

# ---------- Data containers ----------
@dataclass
class Moon:
    planet: str
    name: str
    naif_id: Optional[str]  # may be None; we'll try to resolve
    alt_names: List[str]

# ---------- Helpers ----------
def today_range_utc() -> Tuple[str, str]:
    start_dt = datetime.now(timezone.utc)
    stop_dt = start_dt + timedelta(days=1)
    return start_dt.strftime("%Y-%m-%d"), stop_dt.strftime("%Y-%m-%d")

def get_soup(url: str) -> BeautifulSoup:
    headers = {"User-Agent": HORIZONS_USER_AGENT}
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def parse_moons_from_ssd_table(planet: str) -> List[Moon]:
    """
    Scrape https://ssd.jpl.nasa.gov/sats/phys_par/ for the planet section.
    The table includes a 'NAIF ID' column for most satellites.
    """
    soup = get_soup(SSD_SATS_ROOT)
    anchor = SSD_TABLE_ANCHORS.get(planet.lower())
    if not anchor:
        return []
    # Find the section header
    h = soup.find(id=anchor)
    if not h:
        return []
    # The table is the next <table> after the header
    tbl = h.find_next("table")
    if not tbl:
        return []

    # Find the column indices by header row
    header_row = tbl.find("tr")
    headers = [th.get_text(strip=True) for th in header_row.find_all(["th","td"])]
    idx_name = headers.index("Name") if "Name" in headers else 0
    idx_naif = headers.index("NAIF ID") if "NAIF ID" in headers else -1
    idx_alt  = headers.index("Other Designation") if "Other Designation" in headers else -1

    moons: List[Moon] = []
    for tr in tbl.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if not tds:
            continue
        name = tds[idx_name].get_text(strip=True)
        if not name:
            continue
        naif_id = tds[idx_naif].get_text(strip=True) if idx_naif != -1 else None
        naif_id = naif_id or None
        alt = tds[idx_alt].get_text(strip=True) if idx_alt != -1 else ""
        alt_names = [a.strip() for a in re.split(r"[;,/]", alt) if a.strip()] if alt else []
        moons.append(Moon(planet=planet, name=name, naif_id=naif_id, alt_names=alt_names))

    # Filter out rows that might be summaries
    return [m for m in moons if m.name.lower() not in {"name","total"}]

def horizons_lookup_naif(name: str) -> Optional[str]:
    """
    Use Horizons API (JSON mode) to resolve an object's NAIF ID by name.
    We try COMMAND='NAME' with format=json and parse the metadata block.
    """
    params = {
        "format": "json",
        "COMMAND": f"'{name}'",
        "OBJ_DATA": "YES",
        "MAKE_EPHEM": "NO",
    }
    headers = {"User-Agent": HORIZONS_USER_AGENT}
    try:
        r = requests.get(API_URL, params=params, headers=headers, timeout=60)
        r.raise_for_status()
        data = r.json()
        # The JSON payload from horizons.api includes meta->target->id or similar;
        # fallback: parse the raw text if needed.
        # Try common spots:
        meta = data.get("result", "") if isinstance(data.get("result"), str) else json.dumps(data)
        # Look for 'Target body ID: NNN'
        m = re.search(r"Target body ID:\s*([-\d]+)", meta)
        if m:
            return m.group(1)
        # Or 'Target primary ID / name:' lines
        m2 = re.search(r"Target\s+primary\s+ID:\s*([-\d]+)", meta)
        if m2:
            return m2.group(1)
    except Exception:
        return None
    return None

def parse_horizons_elements_text(block: str) -> Optional[Dict[str, str]]:
    """
    Extract A, EC, IN, OM, W, TA values between $$SOE ... $$EOE from text mode output.
    """
    m = re.search(r"\$\$SOE(.*)\$\$EOE", block, re.DOTALL)
    if not m:
        return None
    lines = [ln.strip() for ln in m.group(1).strip().splitlines()]
    # first line has epoch like '245... = A.D. 2025-Oct-28 00:00:00.0000 TDB'
    epoch_text = lines[0].split("=")[0].strip()
    # next lines have key=value pairs
    values = {}
    for ln in lines[1:6]:
        for k, v in re.findall(r"([A-Z]+)\s*=\s*([^\s]+)", ln):
            values[k] = v
    def fmt(val: Optional[str], places: int) -> Optional[str]:
        if not val: return None
        if "+-" in val:
            val = val.split("+-")[0]
        try:
            s = f"{float(val):.{places}f}"
            s = s.rstrip("0").rstrip(".")
            return s or "0"
        except Exception:
            return val
    return {
        "a":  fmt(values.get("A"), 2),
        "e":  fmt(values.get("EC"), 6),
        "i":  fmt(values.get("IN"), 4),
        "Ω":  fmt(values.get("OM"), 4),
        "ω":  fmt(values.get("W"), 4),
        "ν":  fmt(values.get("TA"), 4),
        "epoch": epoch_text,
    }

def fetch_elements_for_moon(planet_naif: str, moon_id: str, user_agent: str) -> Optional[Dict[str,str]]:
    """
    Query Horizons ELEMENTS for one moon using planet-centric CENTER.
    """
    start, stop = today_range_utc()
    params = {
        "format": "text",
        "COMMAND": f"'{moon_id}'",
        "OBJ_DATA": "NO",
        "MAKE_EPHEM": "YES",
        "EPHEM_TYPE": "ELEMENTS",
        "CENTER": f"@{planet_naif}",   # planet-centric
        "START_TIME": start,
        "STOP_TIME": stop,
        "STEP_SIZE": "1d",
        "OUT_UNITS": "KM-S",
        "REF_PLANE": "EQUATOR",        # satellites: equatorial plane convention
        "ELEM_LABELS": "NO",
    }
    headers = {"User-Agent": user_agent}
    r = requests.get(API_URL, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return parse_horizons_elements_text(r.text)

def ensure_naif_id(moon: Moon) -> Moon:
    if moon.naif_id:
        return moon
    # Try alternate names first (often designations like S/2000 J 11 etc.)
    candidates = [moon.name] + moon.alt_names
    for label in candidates:
        naif = horizons_lookup_naif(label)
        if naif:
            return Moon(planet=moon.planet, name=moon.name, naif_id=naif, alt_names=moon.alt_names)
    return moon  # may remain None; we will skip if unresolved

def normalize_row(planet: str, moon_name: str, moon_id: str, el: Dict[str,str]) -> Dict[str,str]:
    return {
        "planet": planet.capitalize(),
        "name": moon_name,
        "naif_id": moon_id,
        "a": el.get("a"),
        "e": el.get("e"),
        "i_deg": el.get("i"),
        "raan_deg": el.get("Ω"),
        "argp_deg": el.get("ω"),
        "nu_deg": el.get("ν"),
        "epoch": el.get("epoch"),
        "elements_type": "osculating",
        "frame": "EQUATOR",
        "center_naif": planet_naif_id(planet),
        "source": "JPL Horizons",
        "query_time_utc": datetime.now(timezone.utc).isoformat(),
    }

def planet_naif_id(planet: str) -> str:
    return PLANET_META[planet][0]

def write_csv(rows: List[Dict[str,str]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        # still write a header-only file
        headers = ["planet","name","naif_id","a","e","i_deg","raan_deg","argp_deg","nu_deg","epoch","elements_type","frame","center_naif","source","query_time_utc"]
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
        return
    headers = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def write_json(rows: List[Dict[str,str]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")

# ---------- Main ----------
def run_for_planet(planet: str, out_json: Path, out_csv: Path) -> int:
    planet = planet.lower().strip()
    if planet not in PLANET_META:
        raise SystemExit(f"Unknown planet '{planet}'. Valid: {', '.join(PLANET_META)}")

    p_naif = planet_naif_id(planet)
    # Early exit for planets with no moons
    if planet in ("mercury","venus"):
        rows: List[Dict[str,str]] = []
        write_csv(rows, out_csv)
        write_json(rows, out_json)
        print(f"[ok] {planet}: 0 moons (no data needed)")
        return 0

    moons = parse_moons_from_ssd_table(planet)
    if not moons:
        print(f"[warn] Could not parse SSD table for {planet}; no rows found.")
        rows: List[Dict[str,str]] = []
        write_csv(rows, out_csv)
        write_json(rows, out_json)
        return 0

    # Resolve missing NAIFs if any
    resolved: List[Moon] = []
    for m in moons:
        mm = ensure_naif_id(m)
        if not mm.naif_id:
            print(f"[skip] NAIF unresolved for {m.name} ({planet})")
        else:
            resolved.append(mm)

    rows: List[Dict[str,str]] = []
    for m in resolved:
        try:
            el = fetch_elements_for_moon(p_naif, m.naif_id, HORIZONS_USER_AGENT)
            if el:
                rows.append(normalize_row(planet, m.name, m.naif_id, el))
            else:
                print(f"[warn] No elements parsed for {m.name} ({m.naif_id})")
        except Exception as e:
            print(f"[warn] Horizons error for {m.name} ({m.naif_id}): {e}")
        time.sleep(1.0)

    write_csv(rows, out_csv)
    write_json(rows, out_json)
    print(f"[ok] {planet}: wrote {len(rows)} rows → {out_csv.name}, {out_json.name}")
    return len(rows)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--planet", required=True, help="mercury|venus|earth|mars|jupiter|saturn|uranus|neptune")
    ap.add_argument("--out-json", required=True, help="Output JSON file path")
    ap.add_argument("--out-csv", required=True, help="Output CSV file path")
    args = ap.parse_args()

    out_json = Path(args.out_json)
    out_csv  = Path(args.out_csv)
    return run_for_planet(args.planet, out_json, out_csv)

if __name__ == "__main__":
    sys.exit(main())
