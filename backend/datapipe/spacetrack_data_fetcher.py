import os, json, datetime as dt, math, time
import requests
import psycopg2
from psycopg2.extras import execute_batch
from google.cloud import storage

SPACE_TRACK_USER = os.environ["ST_USERNAME"]
SPACE_TRACK_PASS = os.environ["ST_PASSWORD"]
GCP_PROJECT      = os.environ["GOOGLE_CLOUD_PROJECT"]
GCS_BUCKET       = os.environ["GCS_BUCKET"]
EXPORT_PREFIX    = os.environ.get("EXPORT_PREFIX", "space-track/")
DB_HOST          = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT          = int(os.environ.get("DB_PORT", "5432"))
DB_NAME          = os.environ["DB_NAME"]
DB_USER          = os.environ["DB_USER"]
DB_PASS          = os.environ["DB_PASS"]

BASE = "https://www.space-track.org"
S = requests.Session()

# -------- Space-Track auth --------
def st_login():
    r = S.post(f"{BASE}/ajaxauth/login", data={"identity": SPACE_TRACK_USER, "password": SPACE_TRACK_PASS}, timeout=60)
    r.raise_for_status()

# -------- Alpha-5 helpers (only needed if parsing TLE text) --------
# Alpha-5 replaces the first character of a 5-char satnum with A..Z (I/O omitted) for IDs > 99,999.
# e.g., 'A1234' => 10*10000 + 1234 = 101234. JSON NORAD_CAT_ID already numeric; this is for TLE parsing.
_A5_MAP = {c: i for i, c in enumerate("0123456789ABCDEFGHJKLMNPQRSTUVWXYZ")}
def decode_alpha5_satnum(a5: str) -> int:
    """Decode 5-char Alpha-5 to integer. Safe for numeric-only too."""
    if len(a5) != 5:
        raise ValueError("Alpha-5 satnum must be 5 chars")
    n = 0
    for ch in a5:
        if ch.upper() not in _A5_MAP:
            raise ValueError(f"Invalid Alpha-5 character: {ch}")
        n = n * 36 + _A5_MAP[ch.upper()]
    return n

# -------- Data fetch using class/gp --------
def fetch_gp_chunk(norad_min: int, norad_max: int) -> list[dict]:
    """Newest elset per object in a NORAD range, JSON (includes TLE_LINE1/2)."""
    # Order and big limit to cut round-trips. Respect ST throttling in your schedule.
    url = (f"{BASE}/basicspacedata/query/class/gp/"
           f"NORAD_CAT_ID/{norad_min}--{norad_max}/"
           f"orderby/NORAD_CAT_ID asc/limit/200000/format/json")
    r = S.get(url, timeout=600)
    r.raise_for_status()
    return r.json()

def fetch_gp_all() -> list[dict]:
    """
    Pull entire public catalog newest elset using a few NORAD ranges.
    Adjust ranges as catalog grows; this avoids server timeouts and throttling pain.
    """
    ranges = [
        (1,  99999),
        (100000, 199999),
        (200000, 299999),
        (300000, 399999),   # future-proof
    ]
    out = []
    for a, b in ranges:
        chunk = fetch_gp_chunk(a, b)
        out.extend(chunk)
        time.sleep(1)  # be polite; keep well under 30/min guideline
    return out

# -------- DB plumbing --------
def connect_db():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, sslmode="require"
    )

def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        create table if not exists gp_catalog (
          norad_cat_id   int not null,
          object_name    text,
          object_id      text,               -- e.g., 1998-067A
          epoch          timestamptz,
          mean_motion    double precision,
          eccentricity   double precision,
          inclination    double precision,
          ra_of_asc_node double precision,
          arg_of_pericenter double precision,
          mean_anomaly   double precision,
          bstar          double precision,
          tle_line0      text,
          tle_line1      text,
          tle_line2      text,
          creation_date  timestamptz,
          gp_id          int,
          inserted_at    timestamptz default now(),
          primary key (norad_cat_id, epoch)
        );
        create index if not exists gp_epoch_idx on gp_catalog(epoch desc);
        """)
    conn.commit()

def upsert_gp(conn, rows):
    sql = """
    insert into gp_catalog
      (norad_cat_id, object_name, object_id, epoch, mean_motion, eccentricity,
       inclination, ra_of_asc_node, arg_of_pericenter, mean_anomaly, bstar,
       tle_line0, tle_line1, tle_line2, creation_date, gp_id)
    values
      (%(NORAD_CAT_ID)s, %(OBJECT_NAME)s, %(OBJECT_ID)s, %(EPOCH)s, %(MEAN_MOTION)s, %(ECCENTRICITY)s,
       %(INCLINATION)s, %(RA_OF_ASC_NODE)s, %(ARG_OF_PERICENTER)s, %(MEAN_ANOMALY)s, %(BSTAR)s,
       %(TLE_LINE0)s, %(TLE_LINE1)s, %(TLE_LINE2)s, %(CREATION_DATE)s, %(GP_ID)s)
    on conflict (norad_cat_id, epoch) do update
      set object_name = excluded.object_name,
          object_id = excluded.object_id,
          mean_motion = excluded.mean_motion,
          eccentricity = excluded.eccentricity,
          inclination = excluded.inclination,
          ra_of_asc_node = excluded.ra_of_asc_node,
          arg_of_pericenter = excluded.arg_of_pericenter,
          mean_anomaly = excluded.mean_anomaly,
          bstar = excluded.bstar,
          tle_line0 = excluded.tle_line0,
          tle_line1 = excluded.tle_line1,
          tle_line2 = excluded.tle_line2,
          creation_date = excluded.creation_date;
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, rows, page_size=2000)
    conn.commit()

# -------- Export to GCS (private) --------
def export_recent_json(conn, hours=24):
    with conn.cursor() as cur:
        cur.execute("""
            select norad_cat_id, object_name, object_id, epoch, tle_line1, tle_line2
            from gp_catalog
            where epoch >= now() - interval %s
            order by epoch desc
        """, (f"{hours} hours",))
        recs = cur.fetchall()
    items = [{
        "norad_cat_id": r[0],
        "object_name": r[1],
        "object_id": r[2],
        "epoch": (r[3].isoformat() if r[3] else None),
        "line1": r[4],
        "line2": r[5],
    } for r in recs]
    return json.dumps({
        "generated_utc": dt.datetime.utcnow().isoformat() + "Z",
        "count": len(items),
        "items": items
    })

def upload_to_gcs(text, bucket_name, object_name):
    client = storage.Client(project=GCP_PROJECT)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.cache_control = "public, max-age=3600"
    blob.upload_from_string(text, content_type="application/json")

# -------- Main --------
def main():
    st_login()

    # Pull newest elset per object across catalog using gp
    raw = fetch_gp_all()

    # Normalize/trim keys to exactly what we store
    rows = []
    for d in raw:
        rows.append({
            "NORAD_CAT_ID": d.get("NORAD_CAT_ID"),
            "OBJECT_NAME": d.get("OBJECT_NAME"),
            "OBJECT_ID": d.get("OBJECT_ID"),
            "EPOCH": d.get("EPOCH"),
            "MEAN_MOTION": d.get("MEAN_MOTION"),
            "ECCENTRICITY": d.get("ECCENTRICITY"),
            "INCLINATION": d.get("INCLINATION"),
            "RA_OF_ASC_NODE": d.get("RA_OF_ASC_NODE"),
            "ARG_OF_PERICENTER": d.get("ARG_OF_PERICENTER"),
            "MEAN_ANOMALY": d.get("MEAN_ANOMALY"),
            "BSTAR": d.get("BSTAR"),
            "TLE_LINE0": d.get("TLE_LINE0"),
            "TLE_LINE1": d.get("TLE_LINE1"),
            "TLE_LINE2": d.get("TLE_LINE2"),
            "CREATION_DATE": d.get("CREATION_DATE"),
            "GP_ID": d.get("GP_ID"),
        })

    conn = connect_db()
    ensure_tables(conn)
    if rows:
        upsert_gp(conn, rows)

    # Export last N hours for the website
    hours = int(os.environ.get("EXPORT_HOURS", "24"))
    text = export_recent_json(conn, hours=hours)
    stamp = dt.datetime.utcnow().strftime("%Y-%m-%d")
    object_name = f"{EXPORT_PREFIX}gp_recent_{stamp}.json"
    upload_to_gcs(text, GCS_BUCKET, object_name)
    print(f"Uploaded gs://{GCS_BUCKET}/{object_name} (ingested {len(rows)}).")

if __name__ == "__main__":
    main()
