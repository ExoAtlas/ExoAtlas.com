import requests
import json
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

# --- Configuration ---
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

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
OUTPUT_FILE_PATH = PROJECT_ROOT / "public" / "data" / "json" / "planets.json"
REPORT_FILE_PATH = SCRIPT_DIR / "reports" / "update_planets_report.txt"

START_TIME_DT = datetime.now(timezone.utc)
STOP_TIME_DT = START_TIME_DT + timedelta(days=1)
START_TIME_STR = START_TIME_DT.strftime("%Y-%m-%d")
STOP_TIME_STR = STOP_TIME_DT.strftime("%Y-%m-%d")

def format_value(value_str, decimal_places=4):
    if value_str is None: return None
    try:
        if '+-' in value_str:
            value_str = value_str.split('+-')[0]
        formatted_str = f"{float(value_str):.{decimal_places}f}".rstrip('0').rstrip('.')
        return formatted_str if formatted_str else "0"
    except (ValueError, TypeError): return value_str

def parse_horizons_elements(response_text):
    match = re.search(r'\$\$SOE(.*)\$\$EOE', response_text, re.DOTALL)
    if not match: return None
        
    data_block = match.group(1)
    lines = data_block.strip().split('\n')
    values = {}
    
    for line in lines[1:5]:
        pairs = re.findall(r'(\S+)\s*=\s*(\S+)', line)
        for key, value in pairs: values[key.strip()] = value.strip()

    data = {}
    data['a'] = format_value(values.get('A'), 0)
    data['e'] = format_value(values.get('EC'), 6)
    data['i'] = format_value(values.get('IN'), 4)
    data['Ω'] = format_value(values.get('OM'), 4)
    data['ω'] = format_value(values.get('W'), 4)
    data['ν'] = format_value(values.get('TA'), 4)
    data['epoch'] = lines[0].strip().split('=')[0].strip()
    return data

def get_all_planet_data():
    all_planets_data = []
    
    for objnum, info in PLANET_DATA.items():
        planet_name = info['name']
        planet_id = info['id']
        print(f"Fetching orbital elements for {planet_name} ({planet_id})...")
        
        try:
            params = {
                "format": "text", "COMMAND": f"'{planet_id}'", "OBJ_DATA": "NO",
                "MAKE_EPHEM": "YES", "EPHEM_TYPE": "ELEMENTS", "CENTER": "@sun",
                "START_TIME": START_TIME_STR, "STOP_TIME": STOP_TIME_STR, "STEP_SIZE": "1d",
                "OUT_UNITS": "KM-S", "REF_PLANE": "ECLIPTIC"
            }
            response = requests.get(API_URL, params=params)
            response.raise_for_status()
            parsed_elements = parse_horizons_elements(response.text)
            
            if parsed_elements:
                final_planet_obj = {
                    "objnum": objnum, "category": "Planet", "name": planet_name,
                    "a": parsed_elements.get("a"), "i": parsed_elements.get("i"),
                    "e": parsed_elements.get("e"), "Ω": parsed_elements.get("Ω"),
                    "ω": parsed_elements.get("ω"), "ν": parsed_elements.get("ν"),
                    "epoch": parsed_elements.get("epoch"),
                    "r": info.get("r"), "μ": info.get("μ"), "R_rate": info.get("R_rate"),
                    "R_lat": info.get("R_lat"), "R_lon": info.get("R_lon")
                }
                all_planets_data.append(final_planet_obj)
            
        except Exception as e:
            print(f"  - An error occurred while processing {planet_name}: {e}")

    return all_planets_data

if __name__ == "__main__":
    # --- Report Generation Setup ---
    REPORT_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    original_stdout = sys.stdout
    
    final_data = None
    try:
        with open(REPORT_FILE_PATH, 'w', encoding='utf-8') as report_file:
            sys.stdout = report_file
            print(f"Starting planetary data update...")
            final_data = get_all_planet_data()
            
            if final_data and len(final_data) == len(PLANET_DATA):
                OUTPUT_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
                with open(OUTPUT_FILE_PATH, 'w', encoding='utf-8') as f:
                    json.dump(final_data, f, indent=2, ensure_ascii=False)
                print(f"\nSuccessfully updated {OUTPUT_FILE_PATH}")
            else:
                print("\nCould not fetch valid data for all planets. File was not updated.")
    finally:
        sys.stdout = original_stdout

    # --- Final Console and Report Message ---
    final_message = ""
    if final_data and len(final_data) == len(PLANET_DATA):
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        final_message = f"planets.json last updated {update_time}"
    else:
        final_message = "Script finished with errors. See report for details."
        
    print(final_message)
    with open(REPORT_FILE_PATH, 'a', encoding='utf-8') as report_file:
        report_file.write(f"\n{final_message}\n")
