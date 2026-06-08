import json, glob, psycopg2, os, sys

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
       "prop_address=COALESCE(EXCLUDED.prop_address,lead_records.prop_address),"
       "county=EXCLUDED.county")

pattern = os.environ.get("JSON_GLOB", "dashboard/*_records.json")
for f in glob.glob(pattern):
    try:
        payload = json.load(open(f))
        recs = payload.get("records", payload) if isinstance(payload, dict) else payload
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
        conn.commit()
        conn.close()
        print(f"{f}: {ins} inserted")
    except Exception as e:
        print(f"Error {f}: {e}")
