import psycopg2, glob, logging, time, os
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)
DB = os.environ["DATABASE_URL"]
INFO_FIELDS = {"prop_id":(1,12),"prop_type_cd":(13,17),"owner_name":(609,678),"situs_prefx":(1040,1049),"situs_street":(1050,1099),"situs_suffix":(1100,1109),"situs_city":(1110,1139),"situs_zip":(1140,1149),"appraised_val":(1916,1930)}
IMPRV_FIELDS = {"prop_id":(1,12),"yr_built":(105,108),"living_area":(113,125)}
def parse_fixed(line,fields):
    return {name:line[s-1:e].strip() for name,(s,e) in fields.items()}
def get_conn():
    for i in range(5):
        try: return psycopg2.connect(DB,connect_timeout=30)
        except Exception as e:
            logger.warning(f"Connect {i+1} failed: {e}"); time.sleep(5)
    raise Exception("No connection")
def insert_batch(batch):
    for i in range(5):
        try:
            conn=get_conn(); cur=conn.cursor()
            cur.executemany("INSERT INTO nueces_cad(prop_id,owner_name,situs_address,situs_city,situs_zip,appraised_val,yr_built,living_area) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(prop_id) DO UPDATE SET situs_address=EXCLUDED.situs_address,living_area=EXCLUDED.living_area,appraised_val=EXCLUDED.appraised_val",batch)
            conn.commit(); conn.close(); return
        except Exception as e:
            logger.warning(f"Insert {i+1} failed: {e}"); time.sleep(10)
nueces_dir="nueces_cad"
info_files=glob.glob(f"{nueces_dir}/*APPRAISAL_INFO.TXT")
imprv_files=glob.glob(f"{nueces_dir}/*APPRAISAL_IMPROVEMENT_DETAIL.TXT")
if not info_files: raise Exception("INFO not found")
logger.info("Reading improvement data...")
imprv={}
if imprv_files:
    with open(imprv_files[0],"r",encoding="latin-1",errors="ignore") as f:
        for line in f:
            if len(line)<125: continue
            r=parse_fixed(line,IMPRV_FIELDS); pid=r["prop_id"]
            if pid not in imprv:
                try: imprv[pid]={"yr_built":int(r["yr_built"]) if r["yr_built"].isdigit() else None,"living_area":int(float(r["living_area"])) if r["living_area"] else None}
                except: pass
logger.info(f"Loaded {len(imprv)} improvement records")
conn=get_conn(); cur=conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS nueces_cad(prop_id TEXT PRIMARY KEY,owner_name TEXT,situs_address TEXT,situs_city TEXT,situs_zip TEXT,appraised_val INTEGER,yr_built INTEGER,living_area INTEGER)")
conn.commit(); conn.close()
logger.info("Parsing APPRAISAL_INFO...")
batch=[]; total=0
with open(info_files[0],"r",encoding="latin-1",errors="ignore") as f:
    for line in f:
        if len(line)<1930: continue
        r=parse_fixed(line,INFO_FIELDS)
        if r["prop_type_cd"].strip() not in ("R","RE","A"): continue
        pid=r["prop_id"]; imp=imprv.get(pid,{})
        addr=" ".join(p for p in [r["situs_prefx"],r["situs_street"],r["situs_suffix"]] if p).strip()
        try: appval=int(r["appraised_val"]) if r["appraised_val"].isdigit() else None
        except: appval=None
        batch.append((pid,r["owner_name"],addr,r["situs_city"],r["situs_zip"],appval,imp.get("yr_built"),imp.get("living_area")))
        if len(batch)>=500:
            insert_batch(batch); total+=len(batch); logger.info(f"Inserted {total} rows..."); batch=[]
if batch: insert_batch(batch); total+=len(batch)
logger.info(f"Done! Total {total} Nueces properties imported")
