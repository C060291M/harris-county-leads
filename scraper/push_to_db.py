import json, glob, psycopg2, os, sys, time
from datetime import datetime

DB = os.environ.get("DATABASE_URL","")
if not DB:
    print("No DATABASE_URL")
    sys.exit(0)

SQL = ("INSERT INTO lead_records "
       "(doc_num,owner,cat,cat_label,doc_type,filed,amount,legal,clerk_url,"
       "prop_address,prop_city,prop_state,prop_zip,mail_address,mail_city,"
       "mail_state,mail_zip,score,flags,county) "
       "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
       "ON CONFLICT (doc_num) DO UPDATE SET "
       "score=EXCLUDED.score,flags=EXCLUDED.flags,"
       "prop_address=COALESCE(NULLIF(EXCLUDED.prop_address,''),lead_records.prop_address),"
       "county=EXCLUDED.county")
LOG_SQL = ("INSERT INTO scraper_runs (job_name, county, run_at, leads_pushed, status, error_msg) "
           "VALUES (%s,%s,%s,%s,%s,%s)")

pattern = os.environ.get("JSON_GLOB", "dashboard/*_records.json")
all_files = glob.glob(pattern)

# Only push files modified in the last 2 hours (current run)
cutoff = time.time() - 7200
files = [f for f in all_files if os.path.getmtime(f) > cutoff]

# Also pick up Harris records.json if fresh
harris_file = "dashboard/records.json"
if os.path.exists(harris_file) and harris_file not in files:
    if os.path.getmtime(harris_file) > cutoff:
        files.append(harris_file)

if not files:
    print(f"No fresh JSON files found (checked {len(all_files)} total, none modified in last 2h)")
    sys.exit(0)

print(f"Pushing {len(files)} fresh files (skipping {len(all_files)-len(files)} stale)")

for f in files:
    try:
        payload = json.load(open(f))
        recs = payload.get("records", payload) if isinstance(payload, dict) else payload
        job_name = os.path.basename(f).replace("_records.json","")
        run_at = datetime.utcnow()
        county_counts = {}
        for r in recs:
            c = r.get("county","unknown")
            county_counts[c] = county_counts.get(c, 0) + 1
        conn = psycopg2.connect(DB, connect_timeout=30)
        cur = conn.cursor()
        ins = 0
        for r in recs:
            try:
                cur.execute(SQL, (
                    r.get("doc_num",""), r.get("owner",""), r.get("cat",""),
                    r.get("cat_label",""), r.get("doc_type",""), r.get("filed"),
                    r.get("amount"), r.get("legal",""), r.get("clerk_url",""),
                    r.get("prop_address",""), r.get("prop_city",""),
                    r.get("prop_state","TX"), r.get("prop_zip",""),
                    r.get("mail_address",""), r.get("mail_city",""),
                    r.get("mail_state","TX"), r.get("mail_zip",""),
                    r.get("score",0), json.dumps(r.get("flags",[])), r.get("county","")
                ))
                ins += 1
            except: pass
        for county, count in county_counts.items():
            try:
                cur.execute(LOG_SQL, (job_name, county, run_at, count, "success", None))
            except: pass
        conn.commit()
        conn.close()
        print(f"{f}: {ins} inserted across {list(county_counts.keys())}")
    except Exception as e:
        print(f"Error {f}: {e}")
        try:
            conn2 = psycopg2.connect(DB, connect_timeout=30)
            cur2 = conn2.cursor()
            cur2.execute(LOG_SQL, (os.path.basename(f), "unknown", datetime.utcnow(), 0, "error", str(e)))
            conn2.commit()
            conn2.close()
        except: pass
