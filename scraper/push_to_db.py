import json, glob, psycopg2, os, sys, re
from datetime import datetime, timezone
from psycopg2.extras import execute_values

DB = os.environ.get("DATABASE_URL","")
if not DB:
    print("ERROR: DATABASE_URL not set"); sys.exit(1)

DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), "..", "dashboard")
JSON_GLOB = os.environ.get("JSON_GLOB", "")

# ============================================================
# Owner / co-owner / lien-holder classifier
# ============================================================
_OWNERCLS_PLACEHOLDER_VALUES = {
    "public", "the public", "ex parte", "see instrument", "unknown",
    "n/a", "na", "none", "the",
}

_OWNERCLS_INSTITUTIONAL_PATTERNS = [
    r"\bbank\b", r"\bn\.?\s?a\.?\b", r"\bmortgage\b", r"\bcredit\b",
    r"\bllc\b", r"\binc\b", r"\bl\s?l\s?c\b", r"\bl\s?p\b", r"\bcorp\b",
    r"\bcorporation\b", r"\bcompany\b", r"\bco\.?\b", r"\bassociation\b",
    r"\bhoa\b", r"\bhomeowners\b", r"\bcommunity\b", r"\bimprovement\b",
    r"\bfunding\b", r"\bfinance\b", r"\bfinancial\b", r"\binvestments?\b",
    r"\bcapital\b", r"\bproperties\b", r"\bhomes?\s+of\s+texas\b",
    r"\bhome\s?builders?\b", r"\bservicing\b", r"\bservices\b",
    r"\btrustee\b", r"\bnominee\b", r"\bmers\b", r"\bfannie\s?mae\b",
    r"\bfreddie\s?mac\b", r"\bhud\b", r"\bhousing\s+and\s+urban\b",
    r"\bhousing\b", r"\bcommission\b", r"\bdepartment\b", r"\bagency\b",
    r"\bauthority\b", r"\binsurance\b", r"\bwater\s+supply\b",
    r"\bcounty\b", r"\bcity\s+of\b", r"\bstate\s+of\s?texa?s?\b",
    r"\bi\s?s\s?d\b", r"\bm\s?u\s?d\b", r"\bschool\s+district\b",
    r"\bunited\s+states?\b", r"\bu\.?\s?s\.?\s?a?\.?\b(?!\w)",
    r"\bworkforce\b", r"\brailroad\b", r"\btexas\s+department\b",
    r"\btax\s+solutions?\b", r"\brecovery\b", r"\bmanagement\b",
    r"\bstaffing\b", r"\bpowersports?\b", r"\bpipeline\b", r"\bresorts?\b",
    r"\bgroup\b", r"\bfund\b", r"\bseries\b", r"\bsociety\b",
    r"\blaw\s+office\b",
    r"\bestates\b", r"\bacres\b", r"\baddition\b", r"\bsubdivision\b",
    r"\bcrossing\b", r"\bmeadows\b", r"\bplaza\b", r"\bvillas?\b",
    r"\blanding\b", r"\btrails?\b", r"\bboating\b",
    r"\bcollege\b", r"\bhospital\b", r"\bministry\b", r"\bconstruction\b",
    r"\bconst\b", r"\bsupply\b", r"\bdistrict\b", r"\bemergency\s+serv",
    r"\bclinic\b", r"\bfoundation\b", r"\bbuilders?\b", r"\brealty\b",
    r"\btitle\s+company\b", r"\bengineering\b", r"\bcontractors?\b",
]
_OWNERCLS_INSTITUTIONAL_RE = re.compile("|".join(_OWNERCLS_INSTITUTIONAL_PATTERNS), re.IGNORECASE)
_OWNERCLS_CHURCH_RE = re.compile(r"\bchurch\b", re.IGNORECASE)

def _ownercls_clean_name(name):
    if not name:
        return name
    name = re.sub(r"\s+(Temp\s+)?[A-Z]?\s?OPR?\d{5,}.*$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\s+\d{6,}.*$", "", name)
    return name.strip()

def _ownercls_split_parties(field):
    if not field:
        return []
    return [p.strip() for p in field.split("/") if p.strip()]

def _ownercls_is_placeholder(name):
    return name.strip().lower() in _OWNERCLS_PLACEHOLDER_VALUES

def _ownercls_has_smashed_bank(word):
    w = word.lower()
    return w.endswith("bank") and w != "bank" and not w.endswith("banks")

def _ownercls_has_institutional_church(name):
    m = _OWNERCLS_CHURCH_RE.search(name)
    if not m:
        return False
    first_word = name.strip().split()[0].lower() if name.strip() else ""
    return first_word != "church"

def _ownercls_is_institutional(name):
    if _ownercls_is_placeholder(name):
        return None
    if _OWNERCLS_INSTITUTIONAL_RE.search(name):
        return True
    if _ownercls_has_institutional_church(name):
        return True
    for word in re.findall(r"[A-Za-z]+", name):
        if _ownercls_has_smashed_bank(word):
            return True
    return False

def _ownercls_is_suspicious_single_word(name):
    words = name.split()
    return len(words) == 1 and len(words[0]) > 4

def _ownercls_classify_field(field):
    field = _ownercls_clean_name(field)
    parts = _ownercls_split_parties(field)
    if not parts:
        return (False, [], False, True, [])
    real_parts = [p for p in parts if not _ownercls_is_placeholder(p)]
    if not real_parts:
        return (False, [], False, True, [])
    person_parts, institutional_parts, suspicious_parts = [], [], []
    for p in real_parts:
        if _ownercls_is_institutional(p):
            institutional_parts.append(p)
        elif _ownercls_is_suspicious_single_word(p):
            suspicious_parts.append(p)
        else:
            person_parts.append(p)
    has_person = len(person_parts) > 0
    is_pure_institutional = len(person_parts) == 0 and len(institutional_parts) > 0
    return (has_person, person_parts, is_pure_institutional, False, suspicious_parts)

def classify_owner_pair(owner, grantee):
    """Given raw owner/grantee strings, return homeowner/co_owner/lienholder/confidence."""
    owner = owner or ""
    grantee = grantee or ""
    o_has, o_persons, o_inst, _, _ = _ownercls_classify_field(owner)
    g_has, g_persons, g_inst, _, _ = _ownercls_classify_field(grantee)
    if o_has and g_has:
        return {"homeowner_name": " / ".join(o_persons), "co_owner_name": " / ".join(g_persons), "lienholder_name": None, "classification_confidence": "medium"}
    if o_has and not g_has:
        return {"homeowner_name": " / ".join(o_persons), "co_owner_name": None, "lienholder_name": grantee if g_inst else None, "classification_confidence": "high"}
    if g_has and not o_has:
        return {"homeowner_name": " / ".join(g_persons), "co_owner_name": None, "lienholder_name": owner if o_inst else None, "classification_confidence": "high"}
    if o_inst and g_inst:
        return {"homeowner_name": None, "co_owner_name": None, "lienholder_name": f"{owner} -> {grantee}", "classification_confidence": "high"}
    return {"homeowner_name": None, "co_owner_name": None, "lienholder_name": None, "classification_confidence": "low"}


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
        "grantee":   ["grantee","grantee_name","party2name"],
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
    
    # Reject resolved/release records — not distress leads
    RESOLVED_CATS = {"RELLP","REL","RELP","RELJ","RELN","SATLIEN","SATJUD","DISCH","WITHD","CANCEL","VOID"}
    RESOLVED_KEYWORDS = ("release","satisfaction","discharge","withdrawal","cancellation","vacated","dismissed")
    cat = (out.get("cat") or "").upper()
    doc_type = (out.get("doc_type") or "").lower()
    cat_label = (out.get("cat_label") or "").lower()
    if cat in RESOLVED_CATS:
        return None
    if any(k in doc_type for k in RESOLVED_KEYWORDS):
        return None
    if any(k in cat_label for k in RESOLVED_KEYWORDS):
        return None

    if not out.get("county") and source_county:
        out["county"] = source_county.lower().replace(" county","").replace(" ","_")
    if out.get("county"):
        out["county"] = out["county"].lower().replace(" county","").replace(" ","_").strip()

    if out.get("filed"): 
        out["filed"] = parse_date(out["filed"])
        # Reject records older than Jan 2025 or with no date
        if not out["filed"]:
            return None  # No date = unusable record
        if out["filed"] < "2025-01-01":
            return None
    if out.get("amount"): out["amount"] = parse_amount(out["amount"])
    out["scraped_at"] = datetime.now(timezone.utc).isoformat()

    if out.get("grantee"):
        cls = classify_owner_pair(out.get("owner"), out.get("grantee"))
        out.update(cls)

    return out

COLS = ["doc_num","owner","grantee","prop_address","filed","doc_type","amount",
        "county","clerk_url","beds","full_baths","sqft","yr_built",
        "cat","cat_label","scraped_at",
        "homeowner_name","co_owner_name","lienholder_name","classification_confidence"]

UPSERT = """
INSERT INTO lead_records ({cols})
VALUES %s
ON CONFLICT (doc_num) DO UPDATE SET
    owner=COALESCE(EXCLUDED.owner,lead_records.owner),
    grantee=COALESCE(EXCLUDED.grantee,lead_records.grantee),
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
    homeowner_name=COALESCE(EXCLUDED.homeowner_name,lead_records.homeowner_name),
    co_owner_name=COALESCE(EXCLUDED.co_owner_name,lead_records.co_owner_name),
    lienholder_name=COALESCE(EXCLUDED.lienholder_name,lead_records.lienholder_name),
    classification_confidence=COALESCE(EXCLUDED.classification_confidence,lead_records.classification_confidence),
    scraped_at=EXCLUDED.scraped_at

""".format(cols=",".join(COLS))

def process_file(path, conn):
    county = os.path.basename(path).replace("_records.json","").replace("pubsearch_","")
    # Special case: records.json = harris
    if county in ("records", "records.json", ""):
        county = "harris"
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


