# scripts/update_planets.py

import requests
import json
import re
from datetime import datetime, timezone

# --- Configuration ---
API_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"
PLANET_IDS = {
    "001": "199", "002": "299", "003": "399", "004": "499",
    "005": "599", "006": "699", "007": "799", "008": "899"
}
OUTPUT_FILE_PATH = "public/data/planets.json"
EPOCH = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def parse_horizons_response(response_text, obj_data):
    """Parses the text block from the HORIZONS API response."""
    data = {}
    
    # Helper to find values using regular expressions
    def find_value(pattern):
        match = re.search(pattern, response_text, re.MULTILINE)
        return match.group(1).strip() if match else None

    # Extract orbital elements
    data['a'] = str(round(float(find_value(r"a\s*=\s*([0-9]+\.[0-9]+)")) * 149597870.7)) # Convert AU to km
    data['e'] = find_value(r"e\s*=\s*([0-9]+\.[0-9]+)")
    data['i'] = find_value(r"i\s*=\s*([0-9]+\.[0-9]+)")
    data['Ω'] = find_value(r"OM\s*=\s*([0-9]+\.[0-9]+)")
    data['ω'] = find_value(r"w\s*=\s*([0-9]+\.[0-9]+)")
    data['ν'] = find_value(r"TA\s*=\s*([0-9]+\.[0-9]+)")
    data['epoch'] = find_value(r"JD\s*([0-9]+\.[0-9]+)")

    # Extract physical properties from the pre-fetched object data
    data['name'] = obj_data.get('name', 'Unknown').split(' ')[0]
    data['r'] = obj_data.get('radius', None)
    data['μ'] = obj_data.get('gm', None)
    
    # Rotational data (can be complex, simplified here)
    data['R_rate'] = obj_data.get('rot_per', None) # Placeholder, needs conversion
    data['R_lat'] = obj_data.get('pole_dec', None)
    data['R_lon'] = obj_data.get('pole_ra', None)

    return data


def get_planet_data():
    """Fetches and processes data for all planets."""
    all_planets = []
    
    for objnum, planet_id in PLANET_IDS.items():
        print(f"Fetching data for {planet_id}...")
        
        # First, get physical data since it's not in the orbital elements response
        obj_params = {"format": "json", "COMMAND": f"'{planet_id}'"}
        obj_response = requests.get(API_URL, params=obj_params).json()
        
        # Now, get the orbital elements for the current epoch
        elements_params = {
            "format": "text",
            "COMMAND": f"'{planet_id}'",
            "OBJ_DATA": "NO",
            "MAKE_EPHEM": "YES",
            "EPHEM_TYPE": "ELEMENTS",
            "CENTER": "@sun",
            "START_TIME": EPOCH,
            "STOP_TIME": EPOCH,
            "STEP_SIZE": "1d"
        }
        elements_response = requests.get(API_URL, params=elements_params)
        
        if elements_response.status_code == 200:
            parsed_data = parse_horizons_response(elements_response.text, obj_response)
            parsed_data.update({
                "objnum": objnum,
                "category": "Planet"
            })
            all_planets.append(parsed_data)
        else:
            print(f"Failed to retrieve data for planet {planet_id}")
            
    return all_planets


if __name__ == "__main__":
    print(f"Starting planetary data update for epoch {EPOCH}...")
    final_data = get_planet_data()
    
    if final_data:
        with open(OUTPUT_FILE_PATH, 'w') as f:
            json.dump(final_data, f, indent=2)
        print(f"Successfully updated {OUTPUT_FILE_PATH}")
    else:
        print("No data was fetched. File not updated.")
