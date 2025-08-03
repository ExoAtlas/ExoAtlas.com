import requests
import json
import os

# --- Configuration ---
JSON_FILE_PATH = 'public/data/json/satellites.json'
SPACETRACK_URL = 'https://www.space-track.org'
LOGIN_URL = f"{SPACETRACK_URL}/ajaxauth/login"
QUERY_URL = f"{SPACETRACK_URL}/basicspacedata/query/class/gp/decay_date/null-val/orderby/norad_cat_id/format/json"

# --- Get Credentials from GitHub Secrets ---
identity = os.environ.get('SPACETRACK_USER')
password = os.environ.get('SPACETRACK_PASSWORD')

if not identity or not password:
    raise Exception("Space-Track credentials are not set in GitHub Secrets.")

# --- Main Script Logic ---
def update_satellite_data():
    print("Starting satellite data update...")

    try:
        # 1. Log in to Space-Track and create a session
        with requests.Session() as session:
            print("Logging into Space-Track.org...")
            auth_payload = {'identity': identity, 'password': password}
            # Add a timeout for login (5s connect, 15s read)
            response = session.post(LOGIN_URL, data=auth_payload, timeout=(5, 15))
            
            response.raise_for_status() # Raise an error for bad status codes (4xx or 5xx)
            if "logged in" not in response.text.lower():
                raise Exception("Space-Track login failed. Check credentials.")
            print("Login successful.")

            # 2. Fetch the latest satellite data
            print("Fetching latest GP data...")
            # Add a longer read timeout for the data query
            data_response = session.get(QUERY_URL, timeout=(5, 60))
            
            data_response.raise_for_status()
            latest_data = data_response.json()
            print(f"Fetched {len(latest_data)} active satellite records.")

    except requests.exceptions.Timeout:
        # This block will run if a timeout occurs
        raise Exception("Request to Space-Track timed out. Aborting update.")
    except requests.exceptions.RequestException as e:
        # This block will catch other network-related errors
        raise Exception(f"A network error occurred: {e}")

    # 3. Read your existing data
    print(f"Reading existing data from {JSON_FILE_PATH}...")
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
            
    print(f"Updated TLE data for {updates_found} satellites.")

    # 5. Write the updated data back to the file
    print(f"Writing updated data back to {JSON_FILE_PATH}...")
    with open(JSON_FILE_PATH, 'w', encoding='utf-8') as f:
        json.dump(list(existing_map.values()), f, indent=4)

    print("Update complete.")

if __name__ == "__main__":
    update_satellite_data()
