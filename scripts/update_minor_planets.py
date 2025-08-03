import requests
import json
import logging
import gzip
import os
from datetime import datetime, timezone

# --- Configuration ---
REPORT_DIR = "scripts/reports"
REPORT_FILE_PATH = os.path.join(REPORT_DIR, "update_minor_planets_report.txt")
# Using the user-confirmed URL
MPCORB_URL = "https://minorplanetcenter.net/iau/MPCORB/MPCORB.DAT.gz"
DAT_FILE_PATH = "MPCORB.dat"
JSON_OUTPUT_PATH = "public/data/json/minor_planets.json"

# --- Create Report Directory if it doesn't exist ---
os.makedirs(REPORT_DIR, exist_ok=True)

# --- Set up Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(REPORT_FILE_PATH, mode='w'),
        logging.StreamHandler()
    ]
)

def download_and_decompress():
    """Downloads and decompresses the MPCORB.dat file."""
    try:
        logging.info(f"Downloading data from {MPCORB_URL}...")
        response = requests.get(MPCORB_URL, stream=True, timeout=300) # 5 min timeout
        response.raise_for_status()
        with gzip.open(response.raw, 'rb') as f_in:
            with open(DAT_FILE_PATH, 'wb') as f_out:
                f_out.writelines(f_in)
        logging.info("Download and decompression complete.")
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to download file: {e}")

def parse_mpcorb_dat():
    """Parses the fixed-width MPCORB.dat file."""
    logging.info(f"Parsing {DAT_FILE_PATH}...")
    asteroids = []
    skipped_lines = 0
    with open(DAT_FILE_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            if line.startswith('#') or line.strip() == '' or line.startswith('-----'):
                continue
            
            # --- Robust Parsing Block ---
            try:
                asteroids.append({
                    "designation": line[0:7].strip(),
                    "name": line[166:194].strip() or "N/A",
                    "magnitude": float(line[8:13].strip()),
                    "epoch": line[20:25].strip(),
                    "mean_anomaly": float(line[26:35].strip()),
                    "arg_of_perihelion": float(line[37:46].strip()),
                    "asc_node": float(line[48:57].strip()),
                    "inclination": float(line[59:68].strip()),
                    "eccentricity": float(line[70:79].strip()),
                    "mean_daily_motion": float(line[80:91].strip()),
                    "semimajor_axis": float(line[92:103].strip()),
                })
            except ValueError:
                skipped_lines += 1
                logging.warning(f"Skipping malformed line: Could not parse number. Content: {line.strip()}")

    if skipped_lines > 0:
        logging.warning(f"Total lines skipped due to formatting errors: {skipped_lines}")
    logging.info(f"Successfully parsed {len(asteroids)} asteroid records.")
    return asteroids

def write_to_json(data):
    """Writes the parsed data to a JSON file."""
    logging.info(f"Writing data to {JSON_OUTPUT_PATH}...")
    try:
        with open(JSON_OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        logging.info("JSON file created successfully.")
    except IOError as e:
        raise Exception(f"Failed to write JSON file: {e}")

if __name__ == "__main__":
    try:
        logging.info("Starting weekly minor planet update...")
        download_and_decompress()
        parsed_data = parse_mpcorb_dat()
        if not parsed_data:
            raise Exception("Parsing resulted in no data. Check MPCORB.dat format.")
        write_to_json(parsed_data)
        logging.info(f"SUCCESS: Update complete. Total objects processed: {len(parsed_data)}")
    except Exception as e:
        logging.error(f"FAILURE: Script failed with an error: {e}")
        raise # Re-raise exception to fail the workflow
