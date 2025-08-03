import requests
import json
import os
import logging
from datetime import datetime, timezone

# --- Configuration ---
JSON_FILE_PATH = 'public/data/json/satellites.json'
REPORT_FILE_PATH = 'update_satellites_report.txt'
SPACETRACK_URL = 'https://www.space-track.org'
LOGIN_URL = f"{SPACETRACK_URL}/ajaxauth/login"
QUERY_URL = f"{SPACETRACK_URL}/basicspacedata/query/class/gp/decay_date/null-val/orderby/norad_cat_id/format/json"

# --- Set up Logging ---
# This will create a report file and also print to the console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(REPORT_FILE_PATH, mode='w'), # Overwrites the file each run
        logging.StreamHandler() # Prints to console
    ]
)

def main_update_logic():
    # --- Get Credentials from GitHub Secrets ---
    identity = os.environ.get('SPACETRACK_USER')
    password = os.environ.get('SPACETRACK_PASSWORD')
    if not identity or not password:
        raise Exception("Space-Track credentials are not set in GitHub Secrets.")

    # 1. Log in to Space-Track and create a session
    with requests.Session() as session:
        logging.info("Logging into Space-Track.org...")
        auth_payload = {'identity': identity, 'password': password}
        response = session.post(LOGIN_URL, data=auth_payload, timeout=(5, 15))
        response.raise_for_status()
        if "logged in" not in response.text.lower():
            raise Exception("Space-Track login failed. Check credentials.")
        logging.info("Login successful.")

        # 2. Fetch the latest satellite data
        logging.info("Fetching latest GP data...")
        data_response = session.get(QUERY_URL, timeout=(5, 60))
        data_response.raise_for_status()
        latest_data = data_response.json()
        logging.info(f"Fetched {len(latest_data)} active satellite records.")

    # 3. Read your existing data
    logging.info(f"Reading existing data from {JSON_FILE_PATH}...")
    with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
        existing_data = json.load(f)

    existing_map = {sat['NORAD_CAT_ID']: sat for sat in existing_data}
    
    # 4. Compare and update
    updates_found = 0
    for new_sat in latest_data:
        norad_id = new_sat['NORAD_CAT_ID']
        if norad_id in existing_map:
            fields_to_update = ['EPOCH', 'MEAN_MOTION', 'ECCENTRICITY', 'INCLINATION', 
                                'RA_OF_ASC_NODE', 'ARG_OF_PERICENTER', 'MEAN_ANOMALY', 
                                'EPHEMERIS_TYPE', 'CLASSIFICATION_TYPE', 'REV_AT_EPOCH', 
                                'BSTAR', 'MEAN_MOTION_DOT', 'MEAN_MOTION_DDOT', 
                                'TLE_LINE0', 'TLE_LINE1', 'TLE_LINE2']
            
            for field in fields_to_update:
                if field in new_sat:
                    existing_map[norad_id][field] = new_sat[field]
            updates_found += 1
            
    logging.info(f"Updated TLE data for {updates_found} satellites.")

    # 5. Write the updated data back to the file
    logging.info(f"Writing updated data back to {JSON_FILE_PATH}...")
    with open(JSON_FILE_PATH, 'w', encoding='utf-8') as f:
        json.dump(list(existing_map.values()), f, indent=4)

    # Return the number of updates for the final success message
    return updates_found

if __name__ == "__main__":
    try:
        logging.info("Starting satellite data update script...")
        updates = main_update_logic()
        # Final success message in the report
        logging.info(f"Script completed successfully. Updated {updates} satellite records.")
    except Exception as e:
        # Final error message in the report
        logging.error(f"Script failed with an error: {e}")
        # Re-raise the exception to make sure the GitHub Action fails as well
        raise
