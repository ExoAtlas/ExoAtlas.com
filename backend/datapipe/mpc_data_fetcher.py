import os
import requests
import psycopg2
from psycopg2.extras import execute_batch
import json
import logging

# Configure logging to print messages to the console
logging.basicConfig(level=logging.INFO)

# URL for the MPCORB.DAT file
MPCORB_URL = "https://minorplanetcenter.net/iau/MPCORB/MPCORB.DAT"

# Database connection details from environment variables
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_NAME = os.environ.get("DB_NAME")
# Use localhost and port 5432 because the Cloud SQL Auth Proxy creates a local tunnel
DB_HOST = "127.0.0.1"
DB_PORT = 5432

# Define the database table schema
TABLE_NAME = "minor_planet_orbital_elements"
TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS {} (
    name VARCHAR(20) PRIMARY KEY,
    h_abs_mag NUMERIC,
    g_slope_param NUMERIC,
    epoch VARCHAR(5),
    mean_anomaly NUMERIC,
    arg_perihelion NUMERIC,
    long_asc_node NUMERIC,
    inclination NUMERIC,
    eccentricity NUMERIC,
    mean_daily_motion NUMERIC,
    semimajor_axis NUMERIC,
    orbit_type VARCHAR(10),
    last_observation VARCHAR(15),
    arc_length INTEGER,
    exoatlas_object_id VARCHAR(9) UNIQUE
);
""".format(TABLE_NAME)

def generate_exoatlas_id(minor_planet_number):
    """Generates the 9-character ExoAtlas Object ID for an asteroid."""
    if minor_planet_number is not None:
        padded_number = str(minor_planet_number).zfill(8)
        return f"3{padded_number}"
    return None

def safe_float(val):
    """Safely converts a string to a float, returns None on error."""
    try:
        return float(val.strip())
    except (ValueError, IndexError):
        return None

def safe_int(val):
    """Safely converts a string to an int, returns None on error."""
    try:
        return int(val.strip())
    except (ValueError, IndexError):
        return None

def fetch_and_parse_data(url):
    """
    Fetches the data from the MPCORB.DAT file and parses it,
    with robust error handling for malformed lines.
    """
    logging.info(f"Fetching data from {url}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        return []

    lines = response.iter_lines(decode_unicode=True)
    data = []
    
    # Skip header lines
    for line in lines:
        if not line or len(line) < 170:
            continue
        if line.strip().startswith("---"):
            break
    
    for line in lines:
        if not line or len(line) < 170:
            continue
        
        try:
            name = line[0:7].strip()
            
            # Using the new safe conversion functions
            minor_planet_number = safe_int(line[159:165])
            exoatlas_id = generate_exoatlas_id(minor_planet_number)
            
            h_abs_mag = safe_float(line[8:13])
            g_slope_param = safe_float(line[14:19])
            epoch = line[20:25].strip()
            mean_anomaly = safe_float(line[26:35])
            arg_perihelion = safe_float(line[37:46])
            long_asc_node = safe_float(line[48:57])
            inclination = safe_float(line[59:68])
            eccentricity = safe_float(line[70:79])
            mean_daily_motion = safe_float(line[80:91])
            semimajor_axis = safe_float(line[92:103])
            orbit_type = line[166:170].strip()
            last_observation = line[105:114].strip()
            arc_length = safe_int(line[115:119])
            
            # Append the parsed data to the list
            data.append((name, h_abs_mag, g_slope_param, epoch, mean_anomaly, arg_perihelion,
                         long_asc_node, inclination, eccentricity, mean_daily_motion,
                         semimajor_axis, orbit_type, last_observation, arc_length, exoatlas_id))
        except (IndexError, ValueError) as e:
            logging.warning(f"Skipping malformed line: {line}. Error: {e}")
            continue

    logging.info(f"Successfully parsed {len(data)} lines of data.")
    return data

def insert_or_update_data_into_db(data):
    """
    Connects to the database and uses an UPSERT statement.
    Uses robust error handling with a guarded cleanup block.
    """
    conn = None
    
    if not all([DB_USER, DB_PASSWORD, DB_NAME]):
        logging.error("Database environment variables are not set. Cannot connect to the database.")
        return

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        
        with conn.cursor() as cur:
            # Create the table if it doesn't exist
            cur.execute(TABLE_SCHEMA)
            
            # The UPSERT query now includes the exoatlas_object_id column
            upsert_query = f"""
            INSERT INTO {TABLE_NAME} (
                name, h_abs_mag, g_slope_param, epoch, mean_anomaly, arg_perihelion,
                long_asc_node, inclination, eccentricity, mean_daily_motion,
                semimajor_axis, orbit_type, last_observation, arc_length, exoatlas_object_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (name) DO UPDATE SET
                h_abs_mag = EXCLUDED.h_abs_mag,
                g_slope_param = EXCLUDED.g_slope_param,
                epoch = EXCLUDED.epoch,
                mean_anomaly = EXCLUDED.mean_anomaly,
                arg_perihelion = EXCLUDED.arg_perihelion,
                long_asc_node = EXCLUDED.long_asc_node,
                inclination = EXCLUDED.inclination,
                eccentricity = EXCLUDED.eccentricity,
                mean_daily_motion = EXCLUDED.mean_daily_motion,
                semimajor_axis = EXCLUDED.semimajor_axis,
                orbit_type = EXCLUDED.orbit_type,
                last_observation = EXCLUDED.last_observation,
                arc_length = EXCLUDED.arc_length,
                exoatlas_object_id = EXCLUDED.exoatlas_object_id;
            """
            
            execute_batch(cur, upsert_query, data)
            
        conn.commit()
        logging.info(f"Successfully processed {len(data)} records.")

    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            
if __name__ == "__main__":
    orbital_elements = fetch_and_parse_data(MPCORB_URL)
    if orbital_elements:
        insert_or_update_data_into_db(orbital_elements)
