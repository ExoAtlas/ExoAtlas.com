import os
import requests
import psycopg2
from psycopg2.extras import execute_batch
import json
import logging
import pandas as pd
from google.cloud import storage

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

MPCORB_URL = "https://minorplanetcenter.net/iau/MPCORB/MPCORB.DAT"

# DB settings (proxy listens on localhost)
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_NAME = os.environ.get("DB_NAME")
DB_HOST = "127.0.0.1"
DB_PORT = 5432

TABLE_NAME = "minor_planet_orbital_elements"
TABLE_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    name VARCHAR(20) PRIMARY KEY,
    h_abs_mag NUMERIC,
    g_slope_param NUMERIC,
    epoch VARCHAR(8),
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
"""

def generate_exoatlas_id(minor_planet_number: int | None) -> str | None:
    if minor_planet_number is None:
        return None
    return f"3{str(minor_planet_number).zfill(8)}"

def safe_float(val: str | None):
    try:
        return float(val.strip())
    except Exception:
        return None

def safe_int(val: str | None):
    try:
        return int(val.strip())
    except Exception:
        return None

def fetch_and_parse_data(url: str):
    """
    One-pass streaming parser. Skips short/comment lines and parses fixed-width fields.
    """
    logging.info(f"Fetching data from {url}...")
    try:
        resp = requests.get(url, stream=True, timeout=(15, 180))
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching MPC file: {e}")
        return []

    data = []
    # Many header lines are short or start with '#'. Real data lines are long (>=160 chars).
    for line in resp.iter_lines(decode_unicode=True):
        if not line:
            continue
        if line.startswith("#"):
            continue
        if len(line) < 160:
            continue

        try:
            name = line[0:7].strip()
            minor_planet_number = safe_int(line[159:165])
            exoatlas_id = generate_exoatlas_id(minor_planet_number)

            h_abs_mag        = safe_float(line[8:13])
            g_slope_param    = safe_float(line[14:19])
            epoch            = line[20:28].strip()        # widen to 8 for safety
            mean_anomaly     = safe_float(line[26:35])
            arg_perihelion   = safe_float(line[37:46])
            long_asc_node    = safe_float(line[48:57])
            inclination      = safe_float(line[59:68])
            eccentricity     = safe_float(line[70:79])
            mean_daily_motion= safe_float(line[80:91])
            semimajor_axis   = safe_float(line[92:103])
            orbit_type       = line[166:170].strip()
            last_observation = line[105:114].strip()
            arc_length       = safe_int(line[115:119])

            data.append((
                name, h_abs_mag, g_slope_param, epoch, mean_anomaly, arg_perihelion,
                long_asc_node, inclination, eccentricity, mean_daily_motion,
                semimajor_axis, orbit_type, last_observation, arc_length, exoatlas_id
            ))
        except Exception as e:
            logging.warning(f"Skipping malformed line: {e}")
            continue

    logging.info(f"Parsed {len(data):,} records.")
    return data

def insert_or_update_data_into_db(rows):
    if not all([DB_USER, DB_PASSWORD, DB_NAME]):
        logging.error("DB env vars (DB_USER/DB_PASSWORD/DB_NAME) are not set.")
        return

    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        with conn, conn.cursor() as cur:
            cur.execute(TABLE_SCHEMA)
            upsert = f"""
                INSERT INTO {TABLE_NAME} (
                    name, h_abs_mag, g_slope_param, epoch, mean_anomaly, arg_perihelion,
                    long_asc_node, inclination, eccentricity, mean_daily_motion,
                    semimajor_axis, orbit_type, last_observation, arc_length, exoatlas_object_id
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
            execute_batch(cur, upsert, rows, page_size=1000)
        logging.info(f"Upserted {len(rows):,} rows.")
    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

def generate_json_and_upload(bucket_name: str | None):
    if not bucket_name:
        logging.info("GCS bucket not provided; skipping JSON export.")
        return

    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        # Use read_sql with a query (works with psycopg2; no SQLAlchemy needed)
        df = pd.read_sql(f'SELECT * FROM {TABLE_NAME}', conn)
        json_data = df.to_json(orient='records')

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob("minor_planet_data.json")
        blob.upload_from_string(json_data, content_type="application/json")
        logging.info(f"Uploaded minor_planet_data.json to gs://{bucket_name}")
    except Exception as e:
        logging.error(f"GCS export error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    elements = fetch_and_parse_data(MPCORB_URL)
    if elements:
        insert_or_update_data_into_db(elements)
        generate_json_and_upload(os.environ.get("GCS_BUCKET_NAME"))
    else:
        logging.warning("No data parsed; skipping DB and GCS steps.")
