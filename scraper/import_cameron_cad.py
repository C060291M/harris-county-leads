import psycopg2, logging, time, os, openpyxl
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)
DB = os.environ["DATABASE_URL"]

def get_conn():
    for i in range(5):
        try: return psycopg2.connect(DB, connect_timeout=30)
        except Exception as e:
            log.warning(f"Connect {i+1} failed: {e}"); time.sleep(5)
    raise Exception("No connection")

def insert_batch(batch):
    for i in range(5):
        try:
            conn = get_conn(); cur = conn.cursor()
            cur.executemany("""INSERT INTO cameron_cad
                (prop_id,owner_name,situs_address,situs_city,situs_zip,appraised_val,yr_built,living_area)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(prop_id) DO UPDATE SET
                situs_address=EXCLUDED.situs_address,
                living_area=EXCLUDED.living_area,
                appraised_val=EXCLUDED.appraised_val""", batch)
            conn.commit(); conn.close(); return
        except Exception as e:
            log.warning(f"Insert {i+1} failed: {e}"); time.sleep(10)

conn = get_conn(); cur = conn.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS cameron_cad(
    prop_id TEXT PRIMARY KEY, owner_name TEXT, situs_address TEXT,
    situs_city TEXT, situs_zip TEXT, appraised_val INTEGER,
    yr_built INTEGER, living_area INTEGER)""")
conn.commit(); conn.close()

path = "cameron_cad/cameron-2026-GCC-preliminary-export-20260423.xlsx"
log.info("Loading Excel...")
wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
ws = wb.active
headers = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]
idx = {h: i for i, h in enumerate(headers)}

batch = []; total = 0
for row in ws.iter_rows(min_row=2, values_only=True):
    prop_type = row[idx["propType"]]
    if prop_type not in ("R", "MH"): continue
    prop_id = str(row[idx["pID"]])
    owner = str(row[idx["name"]] or "")
    num = str(row[idx["situsNum"]] or "")
    pre = str(row[idx["situsPrefix"]] or "")
    st = str(row[idx["situsStreet"]] or "")
    suf = str(row[idx["situsSuffix"]] or "")
    addr = " ".join(p for p in [num, pre, st, suf] if p.strip()).strip()
    city = str(row[idx["situsCity"]] or "")
    zip_ = str(row[idx["situsZip"]] or "")
    try: appval = int(row[idx["appraisedValue"]] or 0) or None
    except: appval = None
    try: yr = int(row[idx["yrBuilt"]] or 0) or None
    except: yr = None
    try: sqft = int(row[idx["imprvMainArea"]] or 0) or None
    except: sqft = None
    batch.append((prop_id, owner, addr, city, zip_, appval, yr, sqft))
    if len(batch) >= 500:
        insert_batch(batch); total += len(batch)
        log.info(f"Inserted {total} rows..."); batch = []

if batch: insert_batch(batch); total += len(batch)
log.info(f"Done! {total} Cameron properties imported")
wb.close()
