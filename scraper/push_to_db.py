import json, glob, psycopg2, os, sys, re
from datetime import datetime, timezone
from psycopg2.extras import execute_values

DB = os.environ.get("DATABASE_URL","")
if not DB:
    print("ERROR: DATABASE_URL not set"); sys.exit(1)

DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), "..", "dashboard")
JSON_GLOB = os.environ.get("JSON_GLOB", "")

def clean(val):
    if not isinstance(val, str): return str(val).strip() if val is not None else None
    val = val.replace("\ufeff","").replace("\u00a0"," ").replace("\u200b","")
    bullet = re.compile(r'[\u2022\u00b7]\s*')
    if bullet.search(val):
        val = bullet.split(val)[0]
    return re.sub(r'\s+',' ',val).strip() or None

def extract_doc_type(val):
    if not isinstance(val, str): return None
    parts = re.split(r'[\u2022\u00b7]\s*', val, maxsplit=1)
    return re.sub(r'\s+',' ',parts[1]).strip() if len(parts)==2 else None

def parse_amount(val):
    if not val: return None
    try: return float(str(val).replace("$","").replace(",","").strip()) or None
    except: return None

def parse_date(val):
    if not val: return None
    val = str(val).strip().split("T")[0]
    for fmt in ("%Y-%m-%d","%m/%d/%Y","%m-%d-%Y","%Y/%m/%d"):
        try: return datetime.strptime(val,fmt).strftime("%Y-%m-%d")
        except: pass
    return val[:10]

def normalize(raw, source_county=None):
    r = {k.lower().strip():v for k,v in raw.items()}
    out = {}

    FIELD_MAP = {
        "doc_num":   ["doc_num","document_number","instrument_number","inst_num","recordnumber"],
        "owner":     ["owner","grantor","grantor_name","owner_name","grantorname","party1name"],
        "prop_address":["prop_address","property_address","address","situs_address","site_address"],
        "filed":     ["filed","recorded_date","recording_date","file_date","filedate","recordeddate"],
        "doc_type":  ["doc_type","document_type","doctype","instrument_type","type","record_type"],
        "amount":    ["amount","consideration","sale_price","saleprice","price"],
        "county":    ["county","county_name"],
        "clerk_url": ["clerk_url","url","document_url","doc_url","link"],
        "cat":       ["cat"],
        "cat_label": ["cat_label"],
        "beds":      ["beds","bedrooms"],
        "full_baths":["full_baths","bathrooms","baths"],
        "sqft":      ["sqft","square_feet","living_area"],
        "yr_built":  ["yr_built","year_built","yearbuilt"],
    }

    for canonical, aliases in FIELD_MAP.items():
        for alias in aliases:
            v = r.get(alias)
            if v not in (None,"","N/A","n/a","null","NULL"):
                out[canonical] = str(v).strip()
                break

    # Clean doc_num — strip embedded bullet+doc_type
    if out.get("doc_num"):
        raw_dn = out["doc_num"]
        out["doc_num"] = clean(raw_dn)
        if not out.get("doc_type"):
            recovered = extract_doc_type(raw_dn)
            if recovered: out["doc_type"] = recovered

    # Fix duplicate doc_num==doc_type (Tyler bug)
    if out.get("doc_num") and out.get("doc_type") and out["doc_num"] == out["doc_type"]:
        raw_val = out["doc_num"]
        out["doc_num"] = clean(raw_val)
        recovered = extract_doc_type(raw_val)
        if recovered: out["doc_type"] = recovered
        else: out.pop("doc_type",None)

    if not out.get("doc_num"): return None

    if not out.get("county") and source_county:
        out["county"] = source_county.lower().replace(" county","").replace(" ","_")
    if out.get("county"):
        out["county"] = out["county"].lower().replace(" county","").replace(" ","_").strip()

    if out.get("filed"): out["filed"] = parse_date(out["filed"])
    if out.get("amount"): out["amount"] = parse_amount(out["amount"])
    out["scraped_at"] = datetime.now(timezone.utc).isoformat()
    return out

COLS = ["doc_num","owner","prop_address","filed","doc_type","amount",
        "county","clerk_url","beds","full_baths","sqft","yr_built",
        "cat","cat_label","scraped_at"]

UPSERT = """
INSERT INTO lead_records ({cols})
VALUES %s
ON CONFLICT (doc_num) DO UPDATE SET
    owner=COALESCE(EXCLUDED.owner,lead_records.owner),
    prop_address=COALESCE(EXCLUDED.prop_address,lead_records.prop_address),
    filed=COALESCE(EXCLUDED.filed,lead_records.filed),
    doc_type=COALESCE(EXCLUDED.doc_type,lead_records.doc_type),
    amount=COALESCE(EXCLUDED.amount,lead_records.amount),
    county=COALESCE(EXCLUDED.county,lead_records.county),
    clerk_url=COALESCE(EXCLUDED.clerk_url,lead_records.clerk_url),
    beds=COALESCE(EXCLUDED.beds,lead_records.beds),
    full_baths=COALESCE(EXCLUDED.full_baths,lead_records.full_baths),
    sqft=COALESCE(EXCLUDED.sqft,lead_records.sqft),
    yr_built=COALESCE(EXCLUDED.yr_built,lead_records.yr_built),
    cat=COALESCE(EXCLUDED.cat,lead_records.cat),
    cat_label=COALESCE(EXCLUDED.cat_label,lead_records.cat_label),
    scraped_at=EXCLUDED.scraped_at
WHERE lead_records.scraped_at < NOW() - INTERVAL '2 hours' OR lead_records.scraped_at IS NULL
""".format(cols=",".join(COLS))

def process_file(path, conn):
    county = os.path.basename(path).replace("_records.json","").replace("pubsearch_","")
    try:
        data = json.loads(open(path, encoding="utf-8-sig").read())
        raws = data.get("records") or data.get("leads") or (data if isinstance(data,list) else [])
        rows = []
        seen_doc_nums = set()
        for raw in raws:
            rec = normalize(raw, source_county=county)
            if rec:
                doc_num = rec.get("doc_num")
                if doc_num and doc_num in seen_doc_nums:
                    continue  # skip duplicates within same batch
                if doc_num:
                    seen_doc_nums.add(doc_num)
                rows.append(tuple(rec.get(c) for c in COLS))
        if rows:
            with conn.cursor() as cur:
                execute_values(cur, UPSERT, rows, page_size=500)
            conn.commit()
        print(f"  {os.path.basename(path)}: {len(rows)} upserted")
        return len(rows)
    except Exception as e:
        conn.rollback()  # CRITICAL: reset aborted transaction so next file can proceed
        print(f"  ERROR {os.path.basename(path)}: {e}")
        return 0

def main():
    if JSON_GLOB:
        pattern = os.path.join(os.path.dirname(__file__), "..", JSON_GLOB)
        files = glob.glob(pattern)
    else:
        files = glob.glob(os.path.join(DASHBOARD_DIR, "*.json"))

    if not files:
        print("No JSON files found"); return

    conn = psycopg2.connect(DB)
    total = sum(process_file(f, conn) for f in sorted(files))
    conn.close()
    print(f"TOTAL: {total} records upserted from {len(files)} files")

if __name__=="__main__":
    main()
