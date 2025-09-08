"""
ExoAtlas asteroid catalog fetcher & publisher

- Downloads MPCORB.DAT.gz from Minor Planet Center
- Parses ONLY numbered objects (skips provisional-only designations)
- Stores compact orbital elements in Postgres table "asteroid_catalog"
- Exports a CSV to GCS
- Builds JSON shards for R2 grouped by orbital regime.

Kept fields (DB/CSV/JSON):
    id (int), name (str), H, G, epoch_mjd, M, w, Omega, i, e, n, a

Environment variables:
    DB_HOST (default: 127.0.0.1), DB_PORT (default: 5432), DB_USER, DB_PASSWORD, DB_NAME
    GCS_BUCKET_NAME, SKIP_GCS_EXPORT=0/1, GCS_CSV_OBJECT_NAME (default: asteroids/asteroid_catalog.csv)
    R2_ENDPOINT, R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY
    R2_SHARD_SIZE (default: 25000), R2_BASE_PREFIX (default: asteroids/by_regime)
    MPCORB_URL (override source), MAX_ROWS_INGEST (0 = all), UPSERT_BATCH_SIZE (default: 1000)
"""
from __future__ import annotations

import os, io, gzip, json, datetime as dt
from typing import Iterator, Tuple, Optional, Dict, Any, List

import requests
import psycopg2
from psycopg2.extras import execute_values
from google.cloud import storage as gcs
import boto3

# ---------------------------- Config ----------------------------

MPCORB_URL = os.getenv(
    "MPCORB_URL",
    "https://minorplanetcenter.net/Extended/MPCORB/MPCORB.DAT.gz",
)

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_CSV_OBJECT_NAME = os.getenv("GCS_CSV_OBJECT_NAME", "asteroids/asteroid_catalog.csv")
SKIP_GCS_EXPORT = os.getenv("SKIP_GCS_EXPORT", "0") == "1"

R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_SHARD_SIZE = int(os.getenv("R2_SHARD_SIZE", "25000"))
R2_BASE_PREFIX = os.getenv("R2_BASE_PREFIX", "asteroids/by_regime")

MAX_ROWS_INGEST = int(os.getenv("MAX_ROWS_INGEST", "0"))
UPSERT_BATCH_SIZE = int(os.getenv("UPSERT_BATCH_SIZE", "1000"))

# ---------------------------- Utils ----------------------------

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] {msg}", flush=True)

def try_float(s: str) -> Optional[float]:
    s = s.strip()
    if not s: return None
    try:
        return float(s)
    except Exception:
        return None

def jd_from_ymd(year: int, month: int, day: int) -> float:
    # Fliegel–Van Flandern algorithm → JD at 0h UT (i.e., noon-based JD - 0.5)
    a = (14 - month)//12
    y = year + 4800 - a
    m = month + 12*a - 3
    jd = day + ((153*m + 2)//5) + 365*y + y//4 - y//100 + y//400 - 32045
    return float(jd) - 0.5

def mjd_from_packed_epoch(packed: str) -> Optional[float]:
    """Decode MPC 5-char packed epoch (e.g., 'K134I') to MJD."""
    s = (packed or "").strip().upper()
    if len(s) != 5:
        return None
    century_map = {"I": 1800, "J": 1900, "K": 2000, "L": 2100}
    try:
        c0, c1, c2, c3, c4 = s
        if c0 not in century_map: return None
        year = century_map[c0] + int(c1 + c2)
        def dec(ch: str) -> int:
            if ch.isdigit(): return int(ch)
            return ord(ch) - ord("A") + 10
        month = dec(c3)
        day = dec(c4)
        if not (1 <= month <= 12 and 1 <= day <= 31): return None
        jd = jd_from_ymd(year, month, day)
        return float(f"{jd - 2400000.5:.5f}")
    except Exception:
        return None

# ---------------------------- MPC parsing ----------------------------

def parse_id_and_name(line: str) -> Tuple[Optional[int], Optional[str]]:
    """
    Pull numbered ID and clean name from the fixed 'name' field (~cols 166-194).
    Examples:
      "(1) Ceres"  -> (1, "Ceres")
      "433 Eros"   -> (433, "Eros")
      "2014 OD436" -> (None, "2014 OD436")  # provisional-only, later skipped
    """
    tail = line[166:194].strip()
    if not tail:
        return None, None
    if tail.startswith("("):
        try:
            close = tail.find(")")
            num = int(tail[1:close])
            nm = tail[close+1:].lstrip() or None
            return num, nm
        except Exception:
            pass
    parts = tail.split()
    if parts and parts[0].isdigit():
        num = int(parts[0])
        nm = " ".join(parts[1:]) if len(parts) > 1 else None
        return num, nm
    return None, tail or None

def extract_orbital_elements(line: str) -> Dict[str, Optional[float]]:
    H   = try_float(line[8:13])
    G   = try_float(line[14:19])
    epk = line[20:25].strip()
    M   = try_float(line[26:35])
    w   = try_float(line[37:46])
    Om  = try_float(line[49:58])
    inc = try_float(line[60:68])
    ecc = try_float(line[70:79])
    n   = try_float(line[80:91])
    a   = try_float(line[92:103])
    epoch_mjd = mjd_from_packed_epoch(epk)
    return {"H":H,"G":G,"epoch_mjd":epoch_mjd,"M":M,"w":w,"Omega":Om,"i":inc,"e":ecc,"n":n,"a":a}

def is_data_line(line: str) -> bool:
    return line and len(line) > 120 and (line[0].isalnum() or line[0] == " ")

def stream_mpc_lines(url: str) -> Iterator[str]:
    log(f"Downloading MPCORB: {url}")
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        gzf = gzip.GzipFile(fileobj=r.raw)
        for bline in gzf:
            try:
                yield bline.decode("ascii", errors="ignore").rstrip("\r\n")
            except Exception:
                continue

# ---------------------------- DB ----------------------------

DDL = """
CREATE TABLE IF NOT EXISTS asteroid_catalog (
    id         BIGINT PRIMARY KEY,
    name       TEXT NOT NULL,
    H          DOUBLE PRECISION,
    G          DOUBLE PRECISION,
    epoch_mjd  DOUBLE PRECISION,
    M          DOUBLE PRECISION,
    w          DOUBLE PRECISION,
    Omega      DOUBLE PRECISION,
    i          DOUBLE PRECISION,
    e          DOUBLE PRECISION,
    n          DOUBLE PRECISION,
    a          DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

UPSERT_SQL = """
INSERT INTO asteroid_catalog
(id, name, H, G, epoch_mjd, M, w, Omega, i, e, n, a)
VALUES %s
ON CONFLICT (id) DO UPDATE SET
    name=EXCLUDED.name,
    H=EXCLUDED.H, G=EXCLUDED.G, epoch_mjd=EXCLUDED.epoch_mjd,
    M=EXCLUDED.M, w=EXCLUDED.w, Omega=EXCLUDED.Omega,
    i=EXCLUDED.i, e=EXCLUDED.e, n=EXCLUDED.n, a=EXCLUDED.a,
    updated_at=NOW();
"""

def get_db_conn():
    if not all([DB_USER, DB_PASSWORD, DB_NAME]):
        raise RuntimeError("DB_USER, DB_PASSWORD, DB_NAME must be set")
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME,
        connect_timeout=15,
    )
    # keep autocommit OFF; we control commits so server-side cursors work
    conn.autocommit = False
    return conn

def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    log('Ensured table "asteroid_catalog".')

# ---------------------------- Export: GCS CSV ----------------------------

CSV_HEADERS = ["id","name","H","G","epoch_mjd","M","w","Omega","i","e","n","a"]

def export_csv_to_gcs(conn) -> None:
    if SKIP_GCS_EXPORT:
        log("Skipping GCS CSV export per SKIP_GCS_EXPORT=1")
        return
    if not GCS_BUCKET_NAME:
        log("GCS_BUCKET_NAME not set; skipping CSV export.")
        return

    log("Exporting CSV to GCS...")
    with conn.cursor(name="csv_stream") as cur:
        cur.itersize = 50000
        cur.execute("SELECT id,name,H,G,epoch_mjd,M,w,Omega,i,e,n,a FROM asteroid_catalog ORDER BY id;")

        buf = io.StringIO()
        buf.write(",".join(CSV_HEADERS) + "\n")
        count = 0
        for row in cur:
            vals = []
            for v in row:
                if v is None:
                    vals.append("")
                elif isinstance(v, str):
                    vv = v.replace('"','""')
                    if "," in vv or '"' in vv:
                        vv = f'"{vv}"'
                    vals.append(vv)
                else:
                    vals.append(str(v))
            buf.write(",".join(vals) + "\n")
            count += 1
            if count % 200000 == 0:
                _upload_csv_to_gcs(buf.getvalue())
                buf.seek(0); buf.truncate(0)
                log(f"  Flushed {count} rows to GCS (chunk).")

        _upload_csv_to_gcs(buf.getvalue())
        log(f"GCS CSV export complete, rows={count} -> gs://{GCS_BUCKET_NAME}/{GCS_CSV_OBJECT_NAME}")

    # Close the read transaction
    conn.rollback()

def _upload_csv_to_gcs(data: str) -> None:
    client = gcs.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(GCS_CSV_OBJECT_NAME)
    blob.cache_control = "public, max-age=3600, immutable"
    blob.content_type = "text/csv"
    blob.upload_from_string(data)

# ---------------------------- Export: R2 JSON shards ----------------------------

def classify_regime(a: Optional[float], e: Optional[float]) -> str:
    if a is None:
        return "unknown"
    q = a*(1 - (e or 0.0))
    if q is not None and q <= 1.3: return "neo"
    if a < 2.0: return "inner-asteroids"
    if 2.0 <= a < 2.5: return "main-inner"
    if 2.5 <= a < 2.82: return "main-middle"
    if 2.82 <= a < 3.3: return "main-outer"
    if 3.3 <= a < 3.7: return "cybele"
    if 3.7 <= a < 4.2: return "hilda"
    if 4.8 <= a <= 5.5: return "jupiter-trojan"
    if 5.5 < a <= 30.0: return "centaur"
    if a > 30.0: return "tno"
    return "other"

def build_r2_shards(conn) -> None:
    if not (R2_ENDPOINT and R2_BUCKET and R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY):
        log("R2 credentials not fully set; skipping JSON shard upload.")
        return

    log("Building JSON shards for R2 by orbital regime...")

    s3 = boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="us-east-1",
    )

    buffers: Dict[str, List[Dict[str, Any]]] = {}
    counters: Dict[str, int] = {}

    def flush(regime: str) -> None:
        arr = buffers.get(regime, [])
        if not arr: return
        idx = counters.get(regime, 0) + 1
        counters[regime] = idx
        key = f"{R2_BASE_PREFIX}/{regime}/{regime}-{idx:04d}.json"
        body = json.dumps(arr, separators=(",", ":"), ensure_ascii=False)
        s3.put_object(
            Bucket=R2_BUCKET, Key=key, Body=body.encode("utf-8"),
            ContentType="application/json",
            CacheControl="public, max-age=31536000, immutable",
        )
        buffers[regime] = []
        log(f"  Uploaded {regime} shard #{idx:04d} with {len(arr)} objects -> r2://{R2_BUCKET}/{key}")

    with conn.cursor(name="r2_stream") as cur:
        cur.itersize = 50000
        cur.execute("SELECT id,name,H,G,epoch_mjd,M,w,Omega,i,e,n,a FROM asteroid_catalog ORDER BY id;")
        total = 0
        for row in cur:
            (rid, name, H, G, epoch_mjd, M, w, Om, inc, ecc, n, a) = row
            regime = classify_regime(a, ecc)
            item = {"id":rid,"name":name,"H":H,"G":G,"epoch_mjd":epoch_mjd,
                    "M":M,"w":w,"Omega":Om,"i":inc,"e":ecc,"n":n,"a":a}
            buf = buffers.setdefault(regime, [])
            buf.append(item)
            if len(buf) >= R2_SHARD_SIZE:
                flush(regime)
            total += 1

    for regime in list(buffers.keys()):
        flush(regime)

    manifest = {reg: {"count": counters.get(reg, 0), "shard_size": R2_SHARD_SIZE}
                for reg in sorted(counters.keys())}
    man_key = f"{R2_BASE_PREFIX}/manifest.json"
    s3.put_object(
        Bucket=R2_BUCKET, Key=man_key,
        Body=json.dumps(manifest, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json", CacheControl="public, max-age=300",
    )
    # Close read transaction
    conn.rollback()
    log(f"R2 shard build complete. Regimes: {manifest}")

# ---------------------------- Ingest pipeline ----------------------------

def ingest(conn) -> None:
    ensure_table(conn)

    batch: List[tuple] = []
    inserted = 0
    for line in stream_mpc_lines(MPCORB_URL):
        if not is_data_line(line):
            continue
        obj_id, obj_name = parse_id_and_name(line)
        if obj_id is None or not obj_name:
            continue
        els = extract_orbital_elements(line)
        batch.append((
            obj_id, obj_name,
            els["H"], els["G"], els["epoch_mjd"],
            els["M"], els["w"], els["Omega"], els["i"],
            els["e"], els["n"], els["a"],
        ))
        if len(batch) >= UPSERT_BATCH_SIZE:
            with conn.cursor() as cur:
                execute_values(cur, UPSERT_SQL, batch, page_size=UPSERT_BATCH_SIZE)
            conn.commit()
            inserted += len(batch)
            log(f"Upserted {inserted} rows...")
            batch.clear()
        if MAX_ROWS_INGEST and (inserted + len(batch)) >= MAX_ROWS_INGEST:
            break

    if batch:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, batch, page_size=UPSERT_BATCH_SIZE)
        conn.commit()
        inserted += len(batch)
        batch.clear()

    log(f"Ingest complete. Total upserted: {inserted}")

# ---------------------------- Main ----------------------------

def main():
    log("Starting asteroid catalog updater...")
    conn = get_db_conn()
    try:
        ingest(conn)
        export_csv_to_gcs(conn)
        build_r2_shards(conn)
    finally:
        conn.close()
    log("All done.")

if __name__ == "__main__":
    main()
